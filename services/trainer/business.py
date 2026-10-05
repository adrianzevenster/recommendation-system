"""Business-metric computation: 7-day retention and attributed watch-time."""
from datetime import timedelta

from sqlalchemy import select

from common.eval import ENGAGEMENT_EVENTS
from common.models import Interaction

from .runtime import _utcnow


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
