"""Neighbor/trending upserts (zero-blackout swap) and the data-quality gate."""
from datetime import datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from common.eval import ENGAGEMENT_EVENTS
from common.models import ItemNeighbor, TrendingItem

from .runtime import _utcnow

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
    # Distribution shift guard: very low completion rates indicate a broken event pipeline
    play_starts = sum(1 for ix in interactions if ix.event_type == "play_start")
    completes = sum(1 for ix in interactions if ix.event_type == "complete")
    if play_starts >= 20 and completes / play_starts < 0.03:
        raise ValueError(f"completion_rate_too_low:{completes}/{play_starts}")
