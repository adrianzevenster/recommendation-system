"""TTL-cached loaders for ranking weights, the active experiment, and the ANN index.

All caches use double-checked locking: the stale value is served without
acquiring the lock; only the refresh path holds it.
"""
import hashlib
import json
import threading
import time

from sqlalchemy import desc, select

from common.ann import deserialize_index
from common.config import settings
from common.eval import DEFAULT_WEIGHTS, WEIGHT_KEYS
from common.models import Experiment, RankingWeights

from .runtime import _ANN_CACHE_TTL_SECONDS, _CACHE_TTL_SECONDS, logger

# Keyed by (primary, segment). Weights cache.
_weights_cache: dict[tuple, dict] = {}
_weights_lock = threading.Lock()

# ANN index cache — double-checked locking, same pattern as weights cache
_ann_cache: dict = {"index": None, "id_list": [], "ts": 0.0}
_ann_lock = threading.Lock()

# Active-experiment cache
_exp_cache: dict = {"data": None, "ts": 0.0}
_exp_lock = threading.Lock()


def _get_s3_client():
    import boto3
    from botocore.config import Config as BotoCoreConfig
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=BotoCoreConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _load_ann_index():
    """Load FAISS index from MinIO with TTL-based refresh. Returns (index, id_list)."""
    now = time.time()
    # Fast path — return cached index if still fresh
    if _ann_cache["index"] is not None and now - _ann_cache["ts"] < _ANN_CACHE_TTL_SECONDS:
        return _ann_cache["index"], _ann_cache["id_list"]
    with _ann_lock:
        # Re-check after acquiring lock
        if _ann_cache["index"] is not None and now - _ann_cache["ts"] < _ANN_CACHE_TTL_SECONDS:
            return _ann_cache["index"], _ann_cache["id_list"]
        try:
            client = _get_s3_client()
            # Resolve latest version via manifest
            resp = client.get_object(Bucket="models", Key="latest.json")
            manifest = json.loads(resp["Body"].read())
            version = manifest["version"]

            idx_resp = client.get_object(Bucket="models", Key=f"trainer/{version}/ann_index.faiss")
            index = deserialize_index(idx_resp["Body"].read())

            ids_resp = client.get_object(Bucket="models", Key=f"trainer/{version}/ann_index_ids.json")
            id_list = json.loads(ids_resp["Body"].read())

            _ann_cache["index"] = index
            _ann_cache["id_list"] = id_list
            _ann_cache["ts"] = now
            logger.info("Loaded ANN index", extra={"version": version, "items": len(id_list)})
        except Exception as exc:
            logger.debug("ANN index not available", extra={"error": str(exc)})
    return _ann_cache["index"], _ann_cache["id_list"]


def _load_ranking_weights(
    session,
    segment: str | None = None,
    primary: str | None = None,
) -> list[float]:
    """Return ranking weights using priority: primary → segment → global → defaults.

    `primary` is intended for device-type keys (e.g. ``"device:mobile"``).
    Uses double-checked locking so stale data is served without acquiring the
    lock; only the refresh path holds it.
    """
    now = time.monotonic()
    cache_key = (primary, segment)
    entry = _weights_cache.get(cache_key)
    if entry and now - entry["ts"] <= _CACHE_TTL_SECONDS:
        return entry["data"]

    with _weights_lock:
        entry = _weights_cache.get(cache_key)
        if entry and now - entry["ts"] <= _CACHE_TTL_SECONDS:
            return entry["data"]

        # Try each segment in priority order, stop on first DB hit
        row = None
        for seg in filter(None, [primary, segment]):
            row = session.execute(
                select(RankingWeights)
                .where(RankingWeights.segment == seg)
                .order_by(desc(RankingWeights.created_at))
            ).scalars().first()
            if row:
                break

        # Final fallback: global weights (segment IS NULL)
        if not row:
            row = session.execute(
                select(RankingWeights)
                .where(RankingWeights.segment.is_(None))
                .order_by(desc(RankingWeights.created_at))
            ).scalars().first()

        data = (
            [row.collaborative, row.content, row.session,
             row.trending, row.freshness, row.genre_bonus]
            if row else DEFAULT_WEIGHTS[:]
        )
        _weights_cache[cache_key] = {"data": data, "ts": now}

    return _weights_cache[cache_key]["data"]


def _load_active_experiment(session):
    """Return the currently active Experiment row, or None."""
    now = time.monotonic()
    # Fast path
    if now - _exp_cache["ts"] <= _CACHE_TTL_SECONDS:
        return _exp_cache["data"]

    with _exp_lock:
        if now - _exp_cache["ts"] <= _CACHE_TTL_SECONDS:
            return _exp_cache["data"]
        _exp_cache["data"] = session.execute(
            select(Experiment)
            .where(Experiment.is_active.is_(True))
            .order_by(desc(Experiment.created_at))
        ).scalars().first()
        _exp_cache["ts"] = now

    return _exp_cache["data"]


def _experiment_assignment(user_id: str, experiment: Experiment) -> tuple[list[float], str]:
    """Deterministically assign user to control or variant.

    Returns (weights_to_use, variant_name). weights_to_use is None for control.
    """
    bucket = int(hashlib.md5(f"{user_id}:{experiment.name}".encode()).hexdigest(), 16) % 100
    if bucket < experiment.traffic_pct:
        raw = json.loads(experiment.variant_weights or "{}")
        if len(raw) == len(WEIGHT_KEYS) and all(k in raw for k in WEIGHT_KEYS):
            return [raw[k] for k in WEIGHT_KEYS], "variant"
    return None, "control"
