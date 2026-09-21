import hashlib
import io
import json
import logging
import math
import signal
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

import boto3
import joblib
import numpy as np
from botocore.config import Config as BotocoreConfig
from prometheus_client import start_http_server
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler, normalize as sklearn_normalize
from sqlalchemy import delete, desc, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from common.config import settings
from common.db import SessionLocal, engine
from common.eval import (
    DEFAULT_WEIGHTS,
    ENGAGEMENT_EVENTS,
    WEIGHT_KEYS,
    _normalize,
    build_feature_matrix,
    evaluate_model,
    interaction_weight,
    item_freshness,
    temporal_split,
    watch_time_gain,
)
from common.logging_utils import configure_logging
from common.metrics import (
    ARTIFACTS_SAVED,
    ATTRIBUTED_WATCH_TIME_SECONDS,
    AVG_ATTRIBUTED_WATCH_SECONDS,
    EVAL_COVERAGE,
    EVAL_COVERAGE_DELTA,
    EVAL_HIT_RATE_AT_10,
    EVAL_MRR_AT_10,
    EVAL_NDCG_AT_10,
    EVAL_WATCH_TIME_NDCG_AT_10,
    MODELS_TRAINED,
    RETENTION_7DAY,
    TRAINING_DATA_QUALITY_FAILURES,
    WEIGHT_ROLLBACKS,
)
from common.models import (
    Base,
    Experiment,
    Interaction,
    Item,
    ItemNeighbor,
    ModelEvaluation,
    ModelVersion,
    RankingWeights,
    TrendingItem,
    User,
)
from common.redis_client import get_redis
from common.telemetry import setup_tracing

SERVICE_NAME = "trainer"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)

_stop = threading.Event()


def _handle_signal(signum, frame):
    logger.info("Signal received — stopping after current training run", extra={"signal": signum})
    _stop.set()
tracer = setup_tracing(SERVICE_NAME)

_MATURE_ORDER = {"G": 0, "PG": 1, "PG-13": 2, "R": 3}
_PRECOMPUTE_TOP_K = 50
_PRECOMPUTE_TTL_SECONDS = 7200  # 2 hours — refreshed on each training run
_MIN_SEGMENT_INTERACTIONS = 100  # minimum interactions to train a per-region LR model


def serve_metrics():
    start_http_server(settings.metrics_port)


def wait_for_postgres() -> None:
    for _ in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:
            logger.info("Waiting for Postgres", extra={"error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Postgres unavailable")


# ---------------------------------------------------------------------------
# Signal builders
# ---------------------------------------------------------------------------

def build_collaborative_neighbors(interactions: list[Interaction], top_k: int = 8):
    """Co-occurrence based item-item collaborative filtering."""
    user_items = defaultdict(list)
    for interaction in interactions:
        if interaction.event_type not in {"play_start", "watch_progress", "complete", "watchlist_add", "click"}:
            continue
        user_items[interaction.user_id].append(interaction.item_id)

    co_counts = defaultdict(Counter)
    item_counts = Counter()

    for items in user_items.values():
        unique_items = list(dict.fromkeys(items))
        for item in unique_items:
            item_counts[item] += 1
        for i in range(len(unique_items)):
            for j in range(i + 1, len(unique_items)):
                a, b = unique_items[i], unique_items[j]
                co_counts[a][b] += 1
                co_counts[b][a] += 1

    neighbors = []
    for source_item, related in co_counts.items():
        scored = []
        for neighbor, count in related.items():
            denom = math.sqrt(item_counts[source_item] * item_counts[neighbor]) or 1.0
            score = count / denom
            scored.append((neighbor, score))
        for neighbor, score in sorted(scored, key=lambda x: x[1], reverse=True)[:top_k]:
            neighbors.append((source_item, neighbor, float(round(score, 6)), "collaborative"))
    return neighbors


def build_mf_neighbors(
    interactions: list[Interaction],
    n_components: int = 50,
    top_k: int = 8,
) -> list[tuple]:
    """SVD matrix factorization → item-item similarity for enhanced collaborative signal.

    Complements co-occurrence CF by capturing latent factor structure that
    co-occurrence misses (e.g. items with sparse but high-quality overlap).
    Results are blended with co-occurrence scores in run_training_once().
    """
    engagement_ixs = [ix for ix in interactions if ix.event_type in ENGAGEMENT_EVENTS]
    if not engagement_ixs:
        return []

    user_ids = list(dict.fromkeys(ix.user_id for ix in engagement_ixs))
    item_ids = list(dict.fromkeys(ix.item_id for ix in engagement_ixs))

    if len(user_ids) < 5 or len(item_ids) < 5:
        return []

    user_idx = {uid: i for i, uid in enumerate(user_ids)}
    item_idx = {iid: i for i, iid in enumerate(item_ids)}

    # Accumulate max interaction weight per (user, item) pair
    score_map: dict[tuple[int, int], float] = {}
    for ix in engagement_ixs:
        u = user_idx[ix.user_id]
        it = item_idx[ix.item_id]
        w = interaction_weight(ix.event_type, ix.completion_pct)
        key = (u, it)
        score_map[key] = max(score_map.get(key, 0.0), w)

    if not score_map:
        return []

    rows_list = [k[0] for k in score_map]
    cols_list = [k[1] for k in score_map]
    data_list = list(score_map.values())

    matrix = csr_matrix(
        (data_list, (rows_list, cols_list)),
        shape=(len(user_ids), len(item_ids)),
    )

    n_comp = min(n_components, min(matrix.shape) - 1)
    if n_comp < 2:
        return []

    svd = TruncatedSVD(n_components=n_comp, random_state=42)
    item_factors = svd.fit_transform(matrix.T)  # shape: (n_items, n_components)
    item_factors = sklearn_normalize(item_factors, norm="l2")

    k = min(top_k + 1, len(item_ids))
    nn = NearestNeighbors(n_neighbors=k, metric="cosine", algorithm="brute")
    nn.fit(item_factors)
    distances, indices = nn.kneighbors(item_factors)

    neighbors = []
    for i, iid in enumerate(item_ids):
        for rank, j in enumerate(indices[i]):
            if int(j) == i:
                continue
            sim = float(round(1.0 - float(distances[i][rank]), 6))
            if sim > 0.0:
                neighbors.append((iid, item_ids[int(j)], sim, "mf"))

    return neighbors


def build_content_neighbors(items: list[Item], top_k: int = 8):
    """TF-IDF content similarity via k-NN. Returns (neighbors, vectorizer)."""
    if not items:
        return [], None
    corpus = [
        f"{item.title} {item.genres.replace(',', ' ')} {item.actors} {item.director} {item.synopsis}"
        for item in items
    ]
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(corpus)

    k = min(top_k + 1, len(items))  # +1 because each item is its own nearest neighbour
    nn = NearestNeighbors(n_neighbors=k, metric="cosine", algorithm="brute")
    nn.fit(matrix)
    distances, indices = nn.kneighbors(matrix)

    neighbors = []
    for i, item in enumerate(items):
        for rank, j in enumerate(indices[i]):
            if int(j) == i:
                continue
            sim = float(round(1.0 - float(distances[i][rank]), 6))
            neighbors.append((item.item_id, items[int(j)].item_id, sim, "content"))
    return neighbors, vectorizer


def build_trending(interactions: list[Interaction]):
    """Build regional trending scores with exponential time-decay within the 24h window."""
    region_scores = defaultdict(Counter)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    for interaction in interactions:
        evt_ts = interaction.event_ts.replace(tzinfo=timezone.utc)
        if evt_ts < cutoff:
            continue
        hours_ago = (now - evt_ts).total_seconds() / 3600.0
        decay = math.exp(-0.1 * hours_ago)
        score = interaction_weight(
            interaction.event_type,
            interaction.completion_pct,
            watch_seconds=interaction.watch_seconds,
        ) * decay
        region_scores[interaction.region][interaction.item_id] += score

    trending = []
    for region, scores in region_scores.items():
        for item_id, score in scores.most_common(10):
            trending.append((region, item_id, float(round(score, 6))))
    return trending


# ---------------------------------------------------------------------------
# Business metric computation
# ---------------------------------------------------------------------------

def _compute_business_metrics(session) -> dict:
    """Compute 7-day retention and attributed watch-time from the interaction table.

    Retention: fraction of users who engaged in the prior week (7-14 days ago)
    that also engaged in the current week (0-7 days ago).  This is a lagging
    indicator — a regression here precedes subscription churn by 1-2 weeks.

    Attributed watch-time: average watch_seconds for plays that came from a
    recommendation (Interaction.position > 0).  This is the primary business
    signal that the recommendation quality directly drives.
    """
    from sqlalchemy import distinct, func

    now = _utcnow()
    week_ago = now - timedelta(days=7)
    two_weeks_ago = now - timedelta(days=14)
    eng = list(ENGAGEMENT_EVENTS)

    # Prior-week cohort (subquery to avoid IN with potentially large list)
    prior_subq = (
        select(Interaction.user_id.distinct().label("user_id"))
        .where(Interaction.event_ts.between(two_weeks_ago, week_ago))
        .where(Interaction.event_type.in_(eng))
        .subquery()
    )

    prior_count = session.execute(
        select(func.count()).select_from(prior_subq)
    ).scalar() or 0

    returned_count = 0
    if prior_count:
        returned_count = session.execute(
            select(func.count(distinct(Interaction.user_id)))
            .where(Interaction.event_ts >= week_ago)
            .where(Interaction.event_type.in_(eng))
            .where(Interaction.user_id.in_(select(prior_subq.c.user_id)))
        ).scalar() or 0

    retention_7d = round(returned_count / prior_count, 4) if prior_count else 0.0

    # Attributed watch-time: plays that originated from a recommendation (position > 0)
    avg_watch_s = session.execute(
        select(func.avg(Interaction.watch_seconds))
        .where(Interaction.event_ts >= week_ago)
        .where(Interaction.position > 0)
        .where(Interaction.event_type.in_(["play_start", "watch_progress", "complete"]))
    ).scalar()
    avg_watch_s = round(float(avg_watch_s or 0.0), 2)

    # Total attributed watch-time for the Prometheus counter increment
    total_watch_s = session.execute(
        select(func.sum(Interaction.watch_seconds))
        .where(Interaction.event_ts >= week_ago)
        .where(Interaction.position > 0)
        .where(Interaction.event_type.in_(["play_start", "watch_progress", "complete"]))
    ).scalar() or 0

    return {
        "retention_7d": retention_7d,
        "avg_attributed_watch_s": avg_watch_s,
        "total_attributed_watch_s": int(total_watch_s),
        "prior_week_users": prior_count,
        "returned_users": returned_count,
    }


# ---------------------------------------------------------------------------
# Upsert helpers — prevent serve-side blackout during neighbor refresh
# ---------------------------------------------------------------------------

# psycopg3 enforces a 65535-parameter cap per query.
# ItemNeighbor has 5 bound params; TrendingItem has 4.
_UPSERT_BATCH = 10_000


def _upsert_neighbors(session, neighbors: list, run_ts: datetime) -> None:
    """Insert/update neighbor rows in chunks; prune stale rows after."""
    if not neighbors:
        session.execute(delete(ItemNeighbor))
        return
    data = [
        {
            "source_item_id": src,
            "neighbor_item_id": nbr,
            "score": score,
            "algorithm": algo,
            "updated_at": run_ts,
        }
        for src, nbr, score, algo in neighbors
    ]
    for i in range(0, len(data), _UPSERT_BATCH):
        batch = data[i : i + _UPSERT_BATCH]
        stmt = pg_insert(ItemNeighbor).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_item_neighbor",
            set_={"score": stmt.excluded.score, "updated_at": stmt.excluded.updated_at},
        )
        session.execute(stmt)
    session.execute(delete(ItemNeighbor).where(ItemNeighbor.updated_at < run_ts))


def _upsert_trending(session, trending: list, run_ts: datetime) -> None:
    """Insert/update trending rows in chunks; prune stale rows after."""
    if not trending:
        session.execute(delete(TrendingItem))
        return
    data = [
        {"region": region, "item_id": item_id, "score": score, "updated_at": run_ts}
        for region, item_id, score in trending
    ]
    for i in range(0, len(data), _UPSERT_BATCH):
        batch = data[i : i + _UPSERT_BATCH]
        stmt = pg_insert(TrendingItem).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_trending_region_item",
            set_={"score": stmt.excluded.score, "updated_at": stmt.excluded.updated_at},
        )
        session.execute(stmt)
    session.execute(delete(TrendingItem).where(TrendingItem.updated_at < run_ts))


# ---------------------------------------------------------------------------
# Data quality gate
# ---------------------------------------------------------------------------

def _validate_training_data(interactions: list, items: list) -> None:
    """Raise ValueError if the training data looks corrupt or insufficient."""
    if len(interactions) < 50:
        raise ValueError(f"too_few_interactions:{len(interactions)}")
    if len(items) < 5:
        raise ValueError(f"too_few_items:{len(items)}")
    engagement_count = sum(1 for ix in interactions if ix.event_type in ENGAGEMENT_EVENTS)
    if engagement_count / len(interactions) < 0.05:
        raise ValueError(f"low_engagement_fraction:{engagement_count}/{len(interactions)}")
    future_cutoff = _utcnow() + timedelta(minutes=5)
    future_count = sum(1 for ix in interactions if ix.event_ts > future_cutoff)
    if future_count / len(interactions) > 0.01:
        raise ValueError(f"future_timestamps:{future_count}/{len(interactions)}")


# ---------------------------------------------------------------------------
# MinIO / S3 model artifact helpers
# ---------------------------------------------------------------------------

def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=BotocoreConfig(signature_version="s3v4"),
    )


def _save_model_artifacts(version: str, vectorizer, scaler, lr_model) -> None:
    try:
        client = _get_s3_client()
        try:
            client.create_bucket(Bucket="models")
        except Exception:
            pass

        artifacts = {"vectorizer": vectorizer, "scaler": scaler, "lr_model": lr_model}
        for name, artifact in artifacts.items():
            if artifact is None:
                continue
            buf = io.BytesIO()
            joblib.dump(artifact, buf)
            size = buf.tell()
            buf.seek(0)
            key = f"trainer/{version}/{name}.joblib"
            client.put_object(Bucket="models", Key=key, Body=buf, ContentLength=size)

        # Write a stable latest.json manifest so loaders never have to sort version strings
        manifest = json.dumps({"version": version, "saved_at": _utcnow().isoformat()}).encode()
        client.put_object(
            Bucket="models", Key="latest.json",
            Body=manifest, ContentLength=len(manifest),
        )

        ARTIFACTS_SAVED.inc()
        logger.info("Saved model artifacts to MinIO", extra={"version": version})
    except Exception as exc:
        logger.warning("Failed to save model artifacts", extra={"error": str(exc)})


def _load_latest_model_artifacts() -> tuple | None:
    try:
        client = _get_s3_client()

        # Prefer the manifest over lexicographic sort — it's the authoritative pointer
        try:
            resp = client.get_object(Bucket="models", Key="latest.json")
            manifest = json.loads(resp["Body"].read())
            latest = manifest["version"]
        except Exception:
            # Fall back to lexicographic sort for backward compatibility
            response = client.list_objects_v2(Bucket="models", Prefix="trainer/")
            objects = response.get("Contents", [])
            if not objects:
                return None
            versions: set[str] = set()
            for obj in objects:
                parts = obj["Key"].split("/")
                if len(parts) >= 3:
                    versions.add(parts[1])
            if not versions:
                return None
            latest = sorted(versions)[-1]

        result = {}
        for name in ("vectorizer", "scaler", "lr_model"):
            key = f"trainer/{latest}/{name}.joblib"
            try:
                resp = client.get_object(Bucket="models", Key=key)
                buf = io.BytesIO(resp["Body"].read())
                result[name] = joblib.load(buf)
            except Exception:
                result[name] = None

        return result.get("vectorizer"), result.get("scaler"), result.get("lr_model")
    except Exception as exc:
        logger.warning("Failed to load model artifacts", extra={"error": str(exc)})
        return None


# ---------------------------------------------------------------------------
# LR weight learning (shared for global and per-segment models)
# ---------------------------------------------------------------------------

def _learn_lr_weights(
    train_ixs: list,
    items: list,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
) -> tuple[list[float], int, StandardScaler | None, LogisticRegression | None]:
    """Train logistic regression to learn ranking weights.

    Returns (weights, sample_count, scaler, lr_model).
    Falls back to DEFAULT_WEIGHTS if insufficient data.
    """
    X, y, sample_weights = build_feature_matrix(train_ixs, items, collab_map, content_map, trending_map)
    if len(X) < 50 or sum(y) < 10:
        return DEFAULT_WEIGHTS[:], len(X), None, None

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(np.array(X, dtype=float))
    lr = LogisticRegression(max_iter=200, C=1.0, class_weight="balanced", solver="lbfgs")
    lr.fit(X_scaled, y, sample_weight=np.array(sample_weights))
    shifted = [max(float(c), 0.0) for c in lr.coef_[0]]
    total = sum(shifted) or 1.0
    weights = [round(c / total, 6) for c in shifted]
    return weights, len(X), scaler, lr


# ---------------------------------------------------------------------------
# Per-user recommendation pre-computation
# ---------------------------------------------------------------------------

def _score_user_candidates_offline(
    user_region: str,
    user_maturity: str,
    recent_items: list[str],
    item_map: dict,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
    weights: list[float],
    genre_affinity: dict[str, float] | None = None,
    catalog_max_year: int | None = None,
) -> list[dict]:
    """Score candidates for one user from in-memory maps. No DB or Redis access."""
    genre_affinity = genre_affinity or {}
    collab: Counter = Counter()
    content: Counter = Counter()
    for src in recent_items[-10:]:
        for n, s in collab_map.get(src, {}).items():
            collab[n] += s
        for n, s in content_map.get(src, {}).items():
            content[n] += s

    norm_collab = _normalize(dict(collab))
    norm_content = _normalize(dict(content))

    sess: dict = {}
    w = 1.0
    for item_id in reversed(recent_items[-5:]):
        sess[item_id] = sess.get(item_id, 0.0) + w
        w *= 0.7
    norm_sess = _normalize(sess)

    norm_trending = _normalize(dict(trending_map.get(user_region, {})))

    candidates = set(norm_collab) | set(norm_content) | set(norm_sess) | set(norm_trending)

    scored = []
    for item_id in candidates:
        item = item_map.get(item_id)
        if item is None or not item.is_active:
            continue
        if user_region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if _MATURE_ORDER.get(item.maturity_rating, 0) > _MATURE_ORDER.get(user_maturity, 2):
            continue

        freshness = item_freshness(item.release_year, reference_year=catalog_max_year)
        genre_bonus = min(
            sum(float(genre_affinity.get(g.strip(), 0.0)) for g in item.genres.split(",")) / 10.0,
            1.0,
        )
        components = {
            "collaborative": round(norm_collab.get(item_id, 0.0), 4),
            "content": round(norm_content.get(item_id, 0.0), 4),
            "session": round(norm_sess.get(item_id, 0.0), 4),
            "trending": round(norm_trending.get(item_id, 0.0), 4),
            "freshness": round(freshness, 4),
            "genre_bonus": round(genre_bonus, 4),
        }
        score = (
            weights[0] * components["collaborative"]
            + weights[1] * components["content"]
            + weights[2] * components["session"]
            + weights[3] * components["trending"]
            + weights[4] * components["freshness"]
            + weights[5] * components["genre_bonus"]
        )
        reason = max(WEIGHT_KEYS, key=lambda k: weights[WEIGHT_KEYS.index(k)] * components.get(k, 0.0))
        scored.append({
            "item_id": item_id,
            "score": round(score, 4),
            "reason": reason,
            "components": components,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:_PRECOMPUTE_TOP_K]


def _precompute_user_recommendations(
    session,
    item_map: dict,
    collab_map: dict,
    content_map: dict,
    trending_map: dict,
    global_weights: list[float],
    segment_weights_map: dict[str | None, list[float]] | None = None,
) -> int:
    """Pre-compute and Redis-cache per-user top-K lists.

    Uses per-segment weights for each user's region when available,
    falling back to global_weights. Also precomputes per-experiment
    variant caches for users who fall in the variant bucket.

    Returns number of users cached.
    """
    redis = get_redis()
    segment_weights_map = segment_weights_map or {}

    cutoff = _utcnow() - timedelta(days=30)
    active_user_ids = session.execute(
        select(Interaction.user_id).where(Interaction.event_ts >= cutoff).distinct()
    ).scalars().all()

    if not active_user_ids:
        return 0

    users = session.execute(
        select(User).where(User.user_id.in_(active_user_ids))
    ).scalars().all()

    # Batch-fetch recent items and genre affinities for all users
    pipe = redis.pipeline()
    for user in users:
        pipe.zrevrange(f"recent:{user.user_id}", 0, 9)
    recent_per_user = pipe.execute()

    pipe = redis.pipeline()
    for user in users:
        pipe.hgetall(f"genre_affinity:{user.user_id}")
    genre_affinity_per_user = pipe.execute()

    catalog_max_year = max((i.release_year for i in item_map.values()), default=None)

    # Load active experiments for per-experiment variant precompute
    active_exps = session.execute(
        select(Experiment).where(Experiment.is_active.is_(True))
    ).scalars().all()

    # Score and cache base (non-experiment) recommendations
    write_pipe = redis.pipeline()
    cached_count = 0
    per_user_data = []
    for user, recent_items, raw_affinity in zip(users, recent_per_user, genre_affinity_per_user):
        user_weights = segment_weights_map.get(user.region, global_weights)
        genre_aff = {k: float(v) for k, v in raw_affinity.items()}
        recs = _score_user_candidates_offline(
            user.region, user.maturity_rating, recent_items, item_map,
            collab_map, content_map, trending_map, user_weights,
            genre_affinity=genre_aff, catalog_max_year=catalog_max_year,
        )
        if recs:
            write_pipe.setex(
                f"precomputed:{user.user_id}",
                _PRECOMPUTE_TTL_SECONDS,
                json.dumps(recs),
            )
            cached_count += 1
        per_user_data.append((user, recent_items, genre_aff))
    write_pipe.execute()

    # Per-experiment variant caches: only for users who hash into the variant bucket
    for exp in active_exps:
        raw = json.loads(exp.variant_weights or "{}")
        if len(raw) != len(WEIGHT_KEYS) or not all(k in raw for k in WEIGHT_KEYS):
            continue
        exp_weights = [raw[k] for k in WEIGHT_KEYS]
        exp_pipe = redis.pipeline()
        for user, recent_items, genre_aff in per_user_data:
            bucket = int(hashlib.md5(f"{user.user_id}:{exp.name}".encode()).hexdigest(), 16) % 100
            if bucket >= exp.traffic_pct:
                continue  # control group — served from the base precomputed key
            recs = _score_user_candidates_offline(
                user.region, user.maturity_rating, recent_items, item_map,
                collab_map, content_map, trending_map, exp_weights,
                genre_affinity=genre_aff, catalog_max_year=catalog_max_year,
            )
            if recs:
                exp_pipe.setex(
                    f"precomputed:{user.user_id}:exp:{exp.name}",
                    _PRECOMPUTE_TTL_SECONDS,
                    json.dumps(recs),
                )
        exp_pipe.execute()

    logger.info("Pre-computed recommendations", extra={
        "users_cached": cached_count,
        "active_experiments": len(active_exps),
    })
    return cached_count


# ---------------------------------------------------------------------------
# Training entry point
# ---------------------------------------------------------------------------

def run_training_once() -> None:
    with tracer.start_as_current_span("training_run"):
        # Locals that need to survive past the first session block
        item_map: dict = {}
        collab_map: dict = {}
        content_map: dict = {}
        trending_map: dict = {}
        serving_weights = DEFAULT_WEIGHTS[:]
        segment_weights_map: dict = {}

        with SessionLocal() as session:
            items = session.execute(select(Item).where(Item.is_active.is_(True))).scalars().all()
            lookback_cutoff = _utcnow() - timedelta(days=settings.training_lookback_days)
            interactions = session.execute(
                select(Interaction).where(Interaction.event_ts >= lookback_cutoff)
            ).scalars().all()

            _validate_training_data(interactions, items)

            # Split BEFORE building neighbour graphs to prevent eval data from
            # leaking into the collab signal used during evaluation.
            train_ixs, eval_ixs = temporal_split(interactions)

            # --- Build signal models from train split only ---
            collaborative_cooc = build_collaborative_neighbors(train_ixs)
            collaborative_mf = build_mf_neighbors(train_ixs)
            content, content_vectorizer = build_content_neighbors(items)
            # Trending is operational (time-windowed), not eval-sensitive — use all interactions
            trending = build_trending(interactions)

            # Blend co-occurrence and MF collaborative signals by averaging shared pairs
            cooc_map_raw: dict[tuple, float] = {(s, n): sc for s, n, sc, _ in collaborative_cooc}
            mf_map_raw: dict[tuple, float] = {(s, n): sc for s, n, sc, _ in collaborative_mf}
            all_pairs = set(cooc_map_raw) | set(mf_map_raw)
            collaborative = []
            for (src, nbr) in all_pairs:
                scores = [s for s in [cooc_map_raw.get((src, nbr)), mf_map_raw.get((src, nbr))] if s is not None]
                avg = round(sum(scores) / len(scores), 6)
                collaborative.append((src, nbr, avg, "collaborative"))

            # --- Upsert neighbours/trending (zero-blackout swap) ---
            run_ts = _utcnow()
            _upsert_neighbors(session, collaborative + content, run_ts)
            _upsert_trending(session, trending, run_ts)

            # --- Build in-memory lookup maps ---
            for src, nbr, score, _ in collaborative:
                collab_map.setdefault(src, {})[nbr] = score
            for src, nbr, score, _ in content:
                content_map.setdefault(src, {})[nbr] = score
            for region, item_id, score in trending:
                trending_map.setdefault(region, {})[item_id] = score
            item_map = {item.item_id: item for item in items}

            # --- Learn global ranking weights via logistic regression ---
            weights, sample_count, scaler, lr = _learn_lr_weights(
                train_ixs, items, collab_map, content_map, trending_map
            )
            if sample_count >= 50:
                logger.info(
                    "LR weights learned",
                    extra={"sample_count": sample_count, "weights": dict(zip(WEIGHT_KEYS, weights))},
                )
            else:
                logger.info(
                    "Insufficient data for LR weight learning, using defaults",
                    extra={"sample_count": sample_count},
                )

            # --- Per-segment (per-region) weight learning ---
            region_ixs_map: dict[str, list] = defaultdict(list)
            for ix in train_ixs:
                region_ixs_map[ix.region].append(ix)

            for region, region_train_ixs in region_ixs_map.items():
                if len(region_train_ixs) < _MIN_SEGMENT_INTERACTIONS:
                    continue
                seg_weights, seg_count, _, _ = _learn_lr_weights(
                    region_train_ixs, items, collab_map, content_map, trending_map
                )
                if seg_count >= 50:
                    segment_weights_map[region] = seg_weights
                    logger.info(
                        "Per-segment LR weights learned",
                        extra={"region": region, "sample_count": seg_count,
                               "weights": dict(zip(WEIGHT_KEYS, seg_weights))},
                    )

            # --- Offline evaluation ---
            eval_result = evaluate_model(
                eval_ixs, train_ixs, collab_map, content_map, trending_map, item_map, weights
            )

            # --- Business metrics (trailing 7-day window, uses same session) ---
            business_metrics = _compute_business_metrics(session)

            # --- Regression guard: only deploy weights when NDCG hasn't dropped ---
            prev_eval = session.execute(
                select(ModelEvaluation).order_by(desc(ModelEvaluation.created_at))
            ).scalars().first()

            EVAL_COVERAGE_DELTA.set(
                eval_result["coverage"] - prev_eval.coverage if prev_eval else 0.0
            )

            deploy_weights = True
            serving_weights = weights[:]
            if prev_eval and prev_eval.ndcg_at_10 > 0.0:
                if eval_result["ndcg_at_k"] < prev_eval.ndcg_at_10 * 0.95:
                    logger.warning(
                        "NDCG regressed vs previous run — keeping existing weights",
                        extra={
                            "new_ndcg": round(eval_result["ndcg_at_k"], 4),
                            "prev_ndcg": round(prev_eval.ndcg_at_10, 4),
                        },
                    )
                    deploy_weights = False
                    WEIGHT_ROLLBACKS.inc()
                    prev_row = session.execute(
                        select(RankingWeights)
                        .where(RankingWeights.segment.is_(None))
                        .order_by(desc(RankingWeights.created_at))
                    ).scalars().first()
                    if prev_row:
                        serving_weights = [
                            prev_row.collaborative, prev_row.content, prev_row.session,
                            prev_row.trending, prev_row.freshness, prev_row.genre_bonus,
                        ]
                    else:
                        serving_weights = DEFAULT_WEIGHTS[:]

            # --- Persist version metadata, weights, and eval results ---
            version = f"hybrid-{_utcnow().strftime('%Y%m%d%H%M%S')}"
            session.add(ModelVersion(version=version, description="co-occurrence+mf+tfidf+lr-weights"))
            if deploy_weights:
                # Global weights (segment=None)
                session.add(
                    RankingWeights(
                        model_version=version,
                        segment=None,
                        collaborative=weights[0],
                        content=weights[1],
                        session=weights[2],
                        trending=weights[3],
                        freshness=weights[4],
                        genre_bonus=weights[5],
                        sample_count=sample_count,
                    )
                )
                # Per-segment weights
                for region, seg_w in segment_weights_map.items():
                    region_count = len(region_ixs_map.get(region, []))
                    session.add(
                        RankingWeights(
                            model_version=version,
                            segment=region,
                            collaborative=seg_w[0],
                            content=seg_w[1],
                            session=seg_w[2],
                            trending=seg_w[3],
                            freshness=seg_w[4],
                            genre_bonus=seg_w[5],
                            sample_count=region_count,
                        )
                    )

            session.add(
                ModelEvaluation(
                    model_version=version,
                    ndcg_at_10=eval_result["ndcg_at_k"],
                    hit_rate_at_10=eval_result["hit_rate_at_k"],
                    mrr_at_10=eval_result["mrr_at_k"],
                    watch_time_ndcg_at_10=eval_result["watch_time_ndcg_at_k"],
                    coverage=eval_result["coverage"],
                    test_user_count=eval_result["test_user_count"],
                    retention_7d=business_metrics["retention_7d"],
                    avg_attributed_watch_s=business_metrics["avg_attributed_watch_s"],
                )
            )
            # Commit all DB changes before the (potentially slow) Redis precompute
            session.commit()

        # --- Save model artifacts to object storage (non-critical, outside session) ---
        _save_model_artifacts(version, content_vectorizer, scaler, lr)

        # --- Update Prometheus gauges ---
        EVAL_NDCG_AT_10.set(eval_result["ndcg_at_k"])
        EVAL_HIT_RATE_AT_10.set(eval_result["hit_rate_at_k"])
        EVAL_MRR_AT_10.set(eval_result["mrr_at_k"])
        EVAL_WATCH_TIME_NDCG_AT_10.set(eval_result["watch_time_ndcg_at_k"])
        EVAL_COVERAGE.set(eval_result["coverage"])
        RETENTION_7DAY.set(business_metrics["retention_7d"])
        AVG_ATTRIBUTED_WATCH_SECONDS.set(business_metrics["avg_attributed_watch_s"])
        if business_metrics["total_attributed_watch_s"] > 0:
            ATTRIBUTED_WATCH_TIME_SECONDS.inc(business_metrics["total_attributed_watch_s"])
        MODELS_TRAINED.inc()

        # --- Pre-compute per-user top-K for low-latency serving (fresh session) ---
        with SessionLocal() as precompute_session:
            _precompute_user_recommendations(
                precompute_session, item_map, collab_map, content_map, trending_map,
                serving_weights, segment_weights_map=segment_weights_map,
            )

        logger.info(
            "Completed training run",
            extra={
                "version": version,
                "collaborative_edges": len(collaborative),
                "content_edges": len(content),
                "trending_rows": len(trending),
                "mf_edges": len(collaborative_mf),
                "ndcg_at_10": round(eval_result["ndcg_at_k"], 4),
                "hit_rate_at_10": round(eval_result["hit_rate_at_k"], 4),
                "mrr_at_10": round(eval_result["mrr_at_k"], 4),
                "watch_time_ndcg_at_10": round(eval_result["watch_time_ndcg_at_k"], 4),
                "coverage": round(eval_result["coverage"], 4),
                "retention_7d": business_metrics["retention_7d"],
                "avg_attributed_watch_s": business_metrics["avg_attributed_watch_s"],
                "prior_week_users": business_metrics["prior_week_users"],
                "returned_users": business_metrics["returned_users"],
                "weights_deployed": deploy_weights,
                "segments_trained": list(segment_weights_map.keys()),
                "serving_weights": dict(zip(WEIGHT_KEYS, serving_weights)),
            },
        )


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_postgres()
    Base.metadata.create_all(bind=engine)
    backoff = 60
    while not _stop.is_set():
        try:
            run_training_once()
            backoff = 60
        except ValueError as exc:
            TRAINING_DATA_QUALITY_FAILURES.labels(reason=str(exc)[:64]).inc()
            logger.error("Training aborted: data quality failure", extra={"reason": str(exc)})
            backoff = min(backoff * 2, 3600)
            logger.info("Retrying after backoff", extra={"seconds": backoff})
        except Exception as exc:
            logger.exception("Training failed", extra={"error": str(exc)})
            backoff = min(backoff * 2, 3600)
            logger.info("Retrying after backoff", extra={"seconds": backoff})
        _stop.wait(timeout=backoff)


if __name__ == "__main__":
    main()
