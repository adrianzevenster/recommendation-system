"""Ranking-weight learning (LR), offline candidate scoring, and per-user precompute."""
import hashlib
import json
from collections import Counter
from datetime import timedelta

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select

from common.eval import (
    DEFAULT_WEIGHTS,
    WEIGHT_KEYS,
    _normalize,
    build_feature_matrix,
    item_freshness,
)
from common.models import Experiment, Interaction, User
from common.redis_client import get_redis

from .runtime import (
    _MATURE_ORDER,
    _PRECOMPUTE_TOP_K,
    _PRECOMPUTE_TTL_SECONDS,
    _utcnow,
    logger,
)


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
