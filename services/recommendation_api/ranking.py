"""Candidate generation, scoring, MMR diversity re-ranking, and cache serving."""
from collections import Counter
from typing import Any

from sqlalchemy import desc, select

from common.ann import query_index
from common.eval import WEIGHT_KEYS, item_freshness
from common.metrics import CANDIDATES_GENERATED
from common.models import Item, ItemNeighbor, ModelVersion, TrendingItem, User

from .caches import _load_ann_index
from .runtime import MATURE_ORDER, redis_client


def normalize_scores(score_map: dict[str, float]) -> dict[str, float]:
    if not score_map:
        return {}
    max_score = max(score_map.values()) or 1.0
    return {k: v / max_score for k, v in score_map.items()}


def get_latest_model_version(session) -> str:
    version = session.execute(
        select(ModelVersion).order_by(desc(ModelVersion.created_at))
    ).scalars().first()
    return version.version if version else "untrained"


def fetch_candidate_scores(session, recent_items: list[str], region: str):
    """Fetch collaborative, content, and trending scores.

    Collaborative signal is sourced from the FAISS ANN index when available
    (sub-millisecond, scales to millions of items).  Falls back to the DB
    neighbour table when the index hasn't been built yet (e.g. first run).
    """
    collab: Counter = Counter()
    content: Counter = Counter()
    trending: Counter = Counter()

    if recent_items:
        # --- Collaborative: prefer ANN index over DB lookup ---
        ann_index, ann_id_list = _load_ann_index()
        if ann_index is not None and ann_id_list:
            ann_scores = query_index(ann_index, ann_id_list, recent_items, k=100)
            collab.update(ann_scores)
        else:
            # DB fallback for collaborative signal
            for row in session.execute(
                select(ItemNeighbor).where(
                    ItemNeighbor.source_item_id.in_(recent_items),
                    ItemNeighbor.algorithm == "collaborative",
                )
            ).scalars().all():
                collab[row.neighbor_item_id] += row.score

        # Content signal stays in DB (TF-IDF vectors aren't in the ANN index)
        for row in session.execute(
            select(ItemNeighbor).where(
                ItemNeighbor.source_item_id.in_(recent_items),
                ItemNeighbor.algorithm == "content",
            )
        ).scalars().all():
            content[row.neighbor_item_id] += row.score

    for row in session.execute(
        select(TrendingItem).where(TrendingItem.region == region)
    ).scalars().all():
        trending[row.item_id] = row.score

    return normalize_scores(dict(collab)), normalize_scores(dict(content)), normalize_scores(dict(trending))


def fetch_session_candidates(recent_items: list[str]) -> dict[str, float]:
    session_scores: dict[str, float] = {}
    weight = 1.0
    for item_id in reversed(recent_items[-5:]):
        session_scores[item_id] = session_scores.get(item_id, 0.0) + weight
        weight *= 0.7
    return normalize_scores(session_scores)


def _genre_jaccard(genres_a: str, genres_b: str) -> float:
    """Jaccard similarity between two comma-separated genre strings."""
    a = {g.strip() for g in genres_a.split(",")}
    b = {g.strip() for g in genres_b.split(",")}
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def _mmr_rerank(
    scored: list[tuple],
    limit: int,
    lambda_mmr: float = 0.7,
) -> list[tuple]:
    """
    Maximal Marginal Relevance re-ranking.

    Balances relevance (the pre-computed score) against diversity (genre
    dissimilarity to already-selected items).  lambda_mmr=1.0 is pure
    relevance; lambda_mmr=0.0 is pure diversity.
    """
    if not scored:
        return []

    selected: list[tuple] = []
    remaining = list(scored)

    while remaining and len(selected) < limit:
        if not selected:
            selected.append(remaining.pop(0))
            continue

        best_mmr = -float("inf")
        best_idx = 0
        for idx, (item, score, reason, components) in enumerate(remaining):
            max_sim = max(
                _genre_jaccard(item.genres, s[0].genres) for s in selected
            )
            mmr_score = lambda_mmr * score - (1.0 - lambda_mmr) * max_sim
            if mmr_score > best_mmr:
                best_mmr = mmr_score
                best_idx = idx

        selected.append(remaining.pop(best_idx))

    return selected


def rank_candidates(
    session,
    user: User,
    collab: dict[str, float],
    content: dict[str, float],
    session_scores: dict[str, float],
    trending: dict[str, float],
    watched: set[str],
    limit: int,
    weights: list[float],
) -> list[dict[str, Any]]:
    genre_affinity = {
        k: float(v)
        for k, v in redis_client.hgetall(f"genre_affinity:{user.user_id}").items()
    }

    pop_raw = dict(redis_client.zrange(f"popular:{user.region}", 0, -1, withscores=True))
    pop_max = max(pop_raw.values(), default=1.0) or 1.0

    candidate_ids = set(collab) | set(content) | set(session_scores) | set(trending) | set(pop_raw)
    CANDIDATES_GENERATED.labels(source="merged").observe(len(candidate_ids))

    items = (
        session.execute(select(Item).where(Item.item_id.in_(candidate_ids))).scalars().all()
        if candidate_ids else []
    )
    item_map = {item.item_id: item for item in items}
    catalog_max_year = max((i.release_year for i in items), default=None)
    scored = []

    for item_id in candidate_ids:
        if item_id in watched:
            continue
        item = item_map.get(item_id)
        if item is None or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue

        genre_bonus = min(
            sum(genre_affinity.get(g.strip(), 0.0) for g in item.genres.split(",")) / 10.0,
            1.0,
        )
        freshness = item_freshness(item.release_year, reference_year=catalog_max_year)

        rt_pop = (pop_raw.get(item_id, 0.0) / pop_max) * (1.0 + 0.5 * genre_bonus)
        blended_trending = 0.6 * trending.get(item_id, 0.0) + 0.4 * min(rt_pop, 1.0)

        w = weights
        score = (
            w[0] * collab.get(item_id, 0.0)
            + w[1] * content.get(item_id, 0.0)
            + w[2] * session_scores.get(item_id, 0.0)
            + w[3] * blended_trending
            + w[4] * freshness
            + w[5] * genre_bonus
        )
        components = {
            "collaborative": collab.get(item_id, 0.0),
            "content": content.get(item_id, 0.0),
            "session": session_scores.get(item_id, 0.0),
            "trending": round(blended_trending, 4),
            "freshness": freshness,
            "genre_bonus": genre_bonus,
        }
        reason = max(WEIGHT_KEYS, key=lambda k: weights[WEIGHT_KEYS.index(k)] * components.get(k, 0.0))
        scored.append((item, score, reason, components))

    scored.sort(key=lambda x: x[1], reverse=True)

    reranked = _mmr_rerank(scored[: limit * 3], limit=limit, lambda_mmr=0.7)

    return [
        {
            "item_id": item.item_id,
            "title": item.title,
            "genres": item.genres.split(","),
            "score": round(score, 4),
            "reason": reason,
            "components": {k: round(v, 4) for k, v in components.items()},
        }
        for item, score, reason, components in reranked
    ]


def _serve_from_cache(
    session,
    user: User,
    cached_recs: list[dict],
    watched: set[str],
    limit: int,
    offset: int,
) -> list[dict]:
    """Serve from Redis pre-computed list, applying watched/maturity/region filters."""
    candidates = [r for r in cached_recs if r["item_id"] not in watched]
    window = candidates[offset : offset + limit]
    if not window:
        return []

    item_ids = [r["item_id"] for r in window]
    item_details = {
        i.item_id: i
        for i in session.execute(select(Item).where(Item.item_id.in_(item_ids))).scalars().all()
    }

    results = []
    for rec in window:
        item = item_details.get(rec["item_id"])
        if item is None or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue
        results.append({
            "item_id": item.item_id,
            "title": item.title,
            "genres": item.genres.split(","),
            "score": rec.get("score", 0.0),
            "reason": rec.get("reason", "precomputed"),
            "components": rec.get("components", {}),
        })
    return results
