"""Shared process-wide singletons and cross-module constants for the API.

Imported by every other module in this package so the Redis client, logger,
and tuning constants are created exactly once and there is no circular
dependency back onto ``app``.
"""
import collections
import logging

from common.logging_utils import configure_logging
from common.redis_client import get_redis

SERVICE_NAME = "recommendation-api"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
redis_client = get_redis()

MATURE_ORDER = {"G": 0, "PG": 1, "PG-13": 2, "R": 3}

_ONLINE_ENGAGEMENT_EVENTS = frozenset({"play_start", "complete", "watchlist_add"})
_engagement_window: collections.deque = collections.deque(maxlen=1000)

_COLD_START_THRESHOLD = 3
_CACHE_TTL_SECONDS = 60.0
# Items watched more than 180 days ago are eligible for re-recommendation
_WATCHED_WINDOW_DAYS = 180
# ANN index is refreshed at most every 5 minutes (warm cache) to amortise MinIO latency
_ANN_CACHE_TTL_SECONDS = 300.0
