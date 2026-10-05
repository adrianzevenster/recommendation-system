"""Cold-start recommendation path for users with little or no watch history."""
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, select

from common.eval import item_freshness
from common.models import Item, TrendingItem, User

from .runtime import MATURE_ORDER, redis_client


def _build_cold_start_response(session, user: User, limit: int) -> list[dict[str, Any]]:
    """Serve cold-start recommendations blending trending, new items, and genre affinity."""
    # Genre affinity from Redis — may be empty for brand-new users
    genre_affinity = {
        k: float(v)
        for k, v in redis_client.hgetall(f"genre_affinity:{user.user_id}").items()
    }

    # Regional trending items
    trending_rows = session.execute(
        select(TrendingItem)
        .where(TrendingItem.region == user.region)
        .order_by(desc(TrendingItem.score))
        .limit(limit * 3)
    ).scalars().all()
    trending_scores = {r.item_id: r.score for r in trending_rows}

    # New items added in the last 30 days — injected to surface recent catalog additions
    cutoff_30d = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    new_items = session.execute(
        select(Item)
        .where(Item.is_active.is_(True), Item.created_at >= cutoff_30d)
        .limit(limit * 2)
    ).scalars().all()

    # Merge candidates preserving insertion order (trending first, then new items)
    candidate_ids = list(dict.fromkeys(
        [r.item_id for r in trending_rows] + [i.item_id for i in new_items]
    ))

    if not candidate_ids:
        raw_items = session.execute(
            select(Item)
            .where(Item.is_active.is_(True))
            .order_by(desc(Item.release_year))
            .limit(limit * 3)
        ).scalars().all()
        item_map = {i.item_id: i for i in raw_items}
        candidate_ids = [i.item_id for i in raw_items]
    else:
        fetched = session.execute(
            select(Item).where(Item.item_id.in_(candidate_ids))
        ).scalars().all()
        item_map = {i.item_id: i for i in fetched}

    catalog_max_year = max((i.release_year for i in item_map.values()), default=None)
    scored = []

    for item_id in candidate_ids:
        item = item_map.get(item_id)
        if not item or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue

        trend_score = trending_scores.get(item_id, 0.0)
        freshness = item_freshness(item.release_year, reference_year=catalog_max_year)
        genre_bonus = min(
            sum(genre_affinity.get(g.strip(), 0.0) for g in item.genres.split(",")) / 10.0,
            1.0,
        )
        # Blend: trending dominates; genre affinity and freshness break ties for new users
        blended = round(0.5 * trend_score + 0.3 * genre_bonus + 0.2 * freshness, 4)
        components = {
            "trending": round(trend_score, 4),
            "freshness": round(freshness, 4),
            "genre_bonus": round(genre_bonus, 4),
            "collaborative": 0.0,
            "content": 0.0,
            "session": 0.0,
        }
        reason = max(
            ("trending", trend_score * 0.5),
            ("genre_bonus", genre_bonus * 0.3),
            ("freshness", freshness * 0.2),
            key=lambda t: t[1],
        )[0]
        scored.append({
            "item_id": item.item_id,
            "title": item.title,
            "genres": item.genres.split(","),
            "score": blended,
            "reason": reason,
            "components": components,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:limit]
