"""A/B experiment auto-conclusion via two-proportion z-test."""
import hashlib
import math

from sqlalchemy import func, select

from common.models import Experiment, Interaction

from .runtime import _utcnow, logger


def _auto_conclude_experiments(session) -> None:
    """Conclude A/B experiments that have run past their max_duration_days.

    Runs a two-proportion z-test on engagements vs impressions.  If significant
    (|z| >= 1.96) the winning side is recorded; otherwise the experiment is
    marked inconclusive.  Only committed here — caller is responsible for the
    surrounding session lifecycle.
    """
    now = _utcnow()
    active_exps = session.execute(
        select(Experiment).where(Experiment.is_active.is_(True))
    ).scalars().all()

    _STRONG_ENGAGEMENT = frozenset({"play_start", "complete", "watchlist_add"})

    for exp in active_exps:
        elapsed_days = (now - exp.created_at).days
        if elapsed_days < exp.max_duration_days:
            continue

        # Aggregate per (user_id, event_type) since the experiment started
        agg_rows = session.execute(
            select(
                Interaction.user_id,
                Interaction.event_type,
                func.count().label("cnt"),
            )
            .where(
                Interaction.event_ts >= exp.created_at,
                Interaction.event_type.in_(["impression"] + list(_STRONG_ENGAGEMENT)),
            )
            .group_by(Interaction.user_id, Interaction.event_type)
        ).all()

        ctrl = {"impressions": 0, "engagements": 0}
        var_ = {"impressions": 0, "engagements": 0}

        for row in agg_rows:
            bucket = int(hashlib.md5(f"{row.user_id}:{exp.name}".encode()).hexdigest(), 16) % 100
            side = var_ if bucket < exp.traffic_pct else ctrl
            if row.event_type == "impression":
                side["impressions"] += row.cnt
            elif row.event_type in _STRONG_ENGAGEMENT:
                side["engagements"] += row.cnt

        n1, k1 = ctrl["impressions"], ctrl["engagements"]
        n2, k2 = var_["impressions"], var_["engagements"]

        is_significant = False
        winning_variant = "inconclusive"
        if n1 > 0 and n2 > 0:
            p1, p2 = k1 / n1, k2 / n2
            p_pool = (k1 + k2) / (n1 + n2)
            if 0.0 < p_pool < 1.0:
                se = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / n1 + 1.0 / n2))
                if se > 0.0:
                    z = (p2 - p1) / se
                    if abs(z) >= 1.96:
                        is_significant = True
                        winning_variant = "variant" if z > 0 else "control"

        exp.is_active = False
        exp.concluded_at = now
        exp.winning_variant = winning_variant
        exp.conclusion_reason = "significant" if is_significant else "max_duration"

        logger.info(
            "Auto-concluded experiment",
            extra={
                "experiment": exp.name,
                "elapsed_days": elapsed_days,
                "winning_variant": winning_variant,
                "conclusion_reason": exp.conclusion_reason,
                "ctrl_impressions": n1,
                "var_impressions": n2,
            },
        )

    session.commit()
