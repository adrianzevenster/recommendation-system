"""
Backfill Redis online features from Postgres interaction history.

Run this whenever Redis is cold (fresh container, eviction, etc.) to restore
the real-time feature keys that the stream processor normally maintains.
Reads each user's N most recent interactions and writes:
  recent:{user_id}       — ZSET  item_id → event_ts timestamp
  watched:{user_id}      — SET   items where completion_pct >= 5% or event_type=complete
  genre_affinity:{user_id} — HASH genre → cumulative weight
  popular:{region}       — ZSET  item_id → cumulative engagement score

Usage:
    python scripts/backfill_redis.py [--limit 20] [--lookback-days 365]
"""
import argparse
import logging
import sys
import time
from collections import defaultdict

sys.path.insert(0, ".")

from sqlalchemy import select, text
from common.db import SessionLocal, engine
from common.models import Interaction, Item, User
from common.redis_client import get_redis

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_EVENT_WEIGHT = {
    "impression": 0.1,
    "click": 0.3,
    "play_start": 0.8,
    "watch_progress": 1.2,
    "complete": 2.0,
    "watchlist_add": 0.5,
}
_RECENT_LIMIT = 20
_GENRE_TTL = 86400 * 30
_POP_TTL = 86400 * 7


def backfill(lookback_days: int = 365, recent_limit: int = _RECENT_LIMIT) -> None:
    redis = get_redis()

    with SessionLocal() as session:
        logger.info("Loading items for genre lookup...")
        items = session.execute(select(Item)).scalars().all()
        item_genres: dict[str, str] = {i.item_id: i.genres for i in items}
        logger.info(f"Loaded {len(items)} items")

        logger.info(f"Loading interactions (last {lookback_days} days)...")
        rows = session.execute(
            text(
                f"SELECT user_id, item_id, event_type, completion_pct, watch_seconds, "
                f"       region, extract(epoch from event_ts) as ts "
                f"FROM interactions "
                f"WHERE event_ts >= NOW() - INTERVAL '{lookback_days} days' "
                f"ORDER BY user_id, event_ts"
            )
        ).fetchall()
        logger.info(f"Loaded {len(rows)} interactions for {len({r.user_id for r in rows})} users")

    # Group by user — keep all rows to build affinity/popularity,
    # but only write the N most recent to the recent sorted set.
    user_rows: dict[str, list] = defaultdict(list)
    region_scores: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in rows:
        user_rows[r.user_id].append(r)
        weight = _EVENT_WEIGHT.get(r.event_type, 0.1)
        region_scores[r.region][r.item_id] += weight

    logger.info("Writing to Redis...")
    pipe = redis.pipeline()
    written = 0

    for user_id, ixs in user_rows.items():
        recent_key = f"recent:{user_id}"
        watched_key = f"watched:{user_id}"
        genre_key = f"genre_affinity:{user_id}"

        # recent sorted set — most recent N by event timestamp
        pipe.delete(recent_key)
        for r in ixs[-recent_limit:]:
            pipe.zadd(recent_key, {r.item_id: float(r.ts)})

        # watched set
        for r in ixs:
            if r.event_type == "complete" or (r.completion_pct or 0) >= 5.0 or (r.watch_seconds or 0) >= 120:
                pipe.sadd(watched_key, r.item_id)

        # genre affinity hash
        pipe.delete(genre_key)
        genre_totals: dict[str, float] = defaultdict(float)
        for r in ixs:
            genres = item_genres.get(r.item_id, "")
            if not genres:
                continue
            weight = _EVENT_WEIGHT.get(r.event_type, 0.1)
            for g in genres.split(","):
                genre_totals[g.strip()] += weight
        if genre_totals:
            pipe.hset(genre_key, mapping={k: str(round(v, 4)) for k, v in genre_totals.items()})
        pipe.expire(genre_key, _GENRE_TTL)

        written += 1
        if written % 500 == 0:
            pipe.execute()
            pipe = redis.pipeline()
            logger.info(f"  {written}/{len(user_rows)} users")

    # popularity sorted sets per region
    for region, scores in region_scores.items():
        pop_key = f"popular:{region}"
        pipe.delete(pop_key)
        for item_id, score in scores.items():
            pipe.zadd(pop_key, {item_id: round(score, 4)})
        pipe.expire(pop_key, _POP_TTL)

    pipe.execute()
    logger.info(f"Done — backfilled {written} users, {len(region_scores)} regions")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--limit", type=int, default=_RECENT_LIMIT,
                        help="Max recent items per user in Redis sorted set")
    args = parser.parse_args()
    backfill(lookback_days=args.lookback_days, recent_limit=args.limit)
