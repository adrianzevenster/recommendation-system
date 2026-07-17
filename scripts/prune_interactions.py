"""
Data retention script — delete interaction rows older than RETENTION_DAYS.

Usage:
    python scripts/prune_interactions.py [--days 90] [--dry-run]

Run as a cron job or k8s CronJob.  Safe to run while the system is live;
it uses a single bounded DELETE so it never holds a long table lock.
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, select, text

# Allow running from repo root without installing the package
sys.path.insert(0, ".")

from common.db import SessionLocal, engine
from common.models import Interaction

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_RETENTION_DAYS = 90
_BATCH_SIZE = 10_000


def prune(retention_days: int = _DEFAULT_RETENTION_DAYS, dry_run: bool = False) -> int:
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=retention_days)

    with SessionLocal() as session:
        total_before = session.execute(
            select(func.count()).select_from(Interaction)
        ).scalar_one()

        eligible = session.execute(
            select(func.count()).select_from(Interaction)
            .where(Interaction.event_ts < cutoff)
        ).scalar_one()

        logger.info(
            "Pruning interactions",
            extra={
                "retention_days": retention_days,
                "cutoff": cutoff.isoformat(),
                "total_rows": total_before,
                "eligible_for_deletion": eligible,
                "dry_run": dry_run,
            },
        )

        if dry_run:
            logger.info("Dry run — no rows deleted")
            return 0

        # Delete in batches to avoid long table locks
        deleted_total = 0
        while True:
            # Fetch a batch of event_ids to delete
            batch_ids = session.execute(
                select(Interaction.event_id)
                .where(Interaction.event_ts < cutoff)
                .limit(_BATCH_SIZE)
            ).scalars().all()

            if not batch_ids:
                break

            result = session.execute(
                delete(Interaction).where(Interaction.event_id.in_(batch_ids))
            )
            session.commit()
            deleted_total += result.rowcount
            logger.info("Deleted batch", extra={"batch": result.rowcount, "total": deleted_total})

        logger.info("Pruning complete", extra={"deleted": deleted_total})
        return deleted_total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prune old interaction rows")
    parser.add_argument(
        "--days",
        type=int,
        default=_DEFAULT_RETENTION_DAYS,
        help="Delete interactions older than this many days (default: 90)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would be deleted without deleting them",
    )
    args = parser.parse_args()
    prune(retention_days=args.days, dry_run=args.dry_run)
