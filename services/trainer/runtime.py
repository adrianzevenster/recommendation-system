"""Shared process-wide singletons, lifecycle helpers, and tuning constants.

Imported by every other module in this package so logging, the tracer, and
the stop-event are initialised exactly once and logic modules never import
``app`` (which would be circular).
"""
import logging
import threading
import time
from datetime import datetime, timezone

from prometheus_client import start_http_server
from sqlalchemy import text

from common.config import settings
from common.db import engine
from common.logging_utils import configure_logging
from common.telemetry import setup_tracing

SERVICE_NAME = "trainer"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)

_stop = threading.Event()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
