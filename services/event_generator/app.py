import json
import logging
import random
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Producer
from prometheus_client import start_http_server
from sqlalchemy import select

from common.config import settings
from common.db import SessionLocal
from common.logging_utils import configure_logging
from common.metrics import ACTIVE_USERS_GAUGE, EVENTS_GENERATED
from common.models import Item, User
from common.telemetry import setup_tracing

SERVICE_NAME = "event-generator"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
tracer = setup_tracing(SERVICE_NAME)

EVENT_TYPES = [
    ("impression", 0.40),
    ("click", 0.18),
    ("play_start", 0.20),
    ("watch_progress", 0.14),
    ("complete", 0.06),
    ("watchlist_add", 0.02),
]

GENRE_WEIGHTS = {
    "u001": {"thriller": 3, "crime": 3, "mystery": 2, "sci-fi": 1},
    "u002": {"action": 3, "adventure": 2, "sci-fi": 2},
    "u003": {"comedy": 3, "drama": 2, "food": 1},
    "u004": {"romance": 3, "drama": 3, "comedy": 1},
    "u005": {"history": 2, "drama": 2, "documentary": 2},
    "u006": {"sci-fi": 3, "thriller": 2, "action": 2},
    "u007": {"crime": 3, "mystery": 2, "thriller": 2},
    "u008": {"comedy": 3, "food": 2, "documentary": 2},
    "u009": {"sports": 3, "drama": 2, "action": 1},
    "u010": {"history": 2, "documentary": 2, "drama": 1},
}


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})


def load_seed_data():
    with SessionLocal() as session:
        users = session.execute(select(User)).scalars().all()
        items = session.execute(select(Item)).scalars().all()
    return users, items


def choose_item_for_user(user_id: str, items: list[Item]) -> Item:
    weights = []
    user_genres = GENRE_WEIGHTS.get(user_id, {})
    for item in items:
        score = 1.0
        for genre in item.genres.split(","):
            score += user_genres.get(genre.strip(), 0)
        score += max(0, item.release_year - 2022) * 0.3
        weights.append(score)
    return random.choices(items, weights=weights, k=1)[0]


def make_event(user: User, item: Item) -> dict:
    event_type = random.choices([e[0] for e in EVENT_TYPES], weights=[e[1] for e in EVENT_TYPES], k=1)[0]
    watch_seconds = 0
    completion_pct = 0.0
    if event_type in {"watch_progress", "complete", "play_start"}:
        runtime = 45 * 60 if item.item_type == "series" else 110 * 60
        if event_type == "play_start":
            watch_seconds = random.randint(30, 600)
        elif event_type == "watch_progress":
            watch_seconds = random.randint(600, runtime - 60)
        else:
            watch_seconds = runtime
        completion_pct = min(100.0, watch_seconds / runtime * 100)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user.user_id,
        "item_id": item.item_id,
        "event_type": event_type,
        "watch_seconds": watch_seconds,
        "completion_pct": round(completion_pct, 2),
        "region": user.region,
        "device_type": random.choice(["web", "mobile", "tv"]),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def delivery_callback(err, msg):
    if err:
        logger.error("Failed to deliver event", extra={"error": str(err)})


def serve_metrics():
    start_http_server(settings.metrics_port)


def main() -> None:
    threading.Thread(target=serve_metrics, daemon=True).start()
    producer = build_producer()
    users, items = load_seed_data()
    logger.info("Loaded seed entities", extra={"users": len(users), "items": len(items)})

    while True:
        active = random.randint(4, min(8, len(users)))
        active_users = random.sample(users, active)
        ACTIVE_USERS_GAUGE.set(active)

        with tracer.start_as_current_span("generate_batch"):
            for user in active_users:
                for _ in range(random.randint(1, 3)):
                    item = choose_item_for_user(user.user_id, items)
                    event = make_event(user, item)
                    producer.produce(
                        settings.kafka_topic_events,
                        key=user.user_id,
                        value=json.dumps(event).encode("utf-8"),
                        on_delivery=delivery_callback,
                    )
                    EVENTS_GENERATED.labels(event_type=event["event_type"]).inc()
                    logger.info("Generated event", extra=event)

            producer.flush(timeout=3)

        time.sleep(2)


if __name__ == "__main__":
    main()
