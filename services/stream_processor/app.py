import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime

import boto3
from botocore.client import Config as BotoConfig
from confluent_kafka import Consumer
from prometheus_client import start_http_server
from sqlalchemy import select, text

from common.config import settings
from common.db import SessionLocal, engine
from common.logging_utils import configure_logging
from common.metrics import EVENTS_CONSUMED
from common.models import Base, Interaction, Item
from common.redis_client import get_redis
from common.telemetry import setup_tracing

SERVICE_NAME = "stream-processor"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
tracer = setup_tracing(SERVICE_NAME)
redis_client = get_redis()


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def wait_for_dependencies() -> None:
    for _ in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            redis_client.ping()
            logger.info("Dependencies ready")
            return
        except Exception as exc:
            logger.info("Waiting for dependencies", extra={"error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Dependencies unavailable")


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=settings.minio_bucket)
    except Exception:
        s3.create_bucket(Bucket=settings.minio_bucket)


def save_raw_event(s3, event: dict) -> None:
    ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))
    key = (
        f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}/"
        f"{event['user_id']}/{event['event_id']}.json"
    )
    s3.put_object(Bucket=settings.minio_bucket, Key=key, Body=json.dumps(event).encode("utf-8"))


def persist_interaction(session, event: dict) -> bool:
    existing = session.execute(
        select(Interaction).where(Interaction.event_id == event["event_id"])
    ).scalar_one_or_none()

    if existing is not None:
        return False

    interaction = Interaction(
        event_id=event["event_id"],
        user_id=event["user_id"],
        item_id=event["item_id"],
        event_type=event["event_type"],
        event_ts=datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00")),
        watch_seconds=int(event.get("watch_seconds") or 0),
        completion_pct=float(event.get("completion_pct") or 0.0),
        region=event.get("region"),
        device_type=event.get("device_type"),
    )
    session.add(interaction)
    return True


def update_online_features(session, event: dict) -> None:
    item = session.get(Item, event["item_id"])
    if item is None:
        return

    recent_key = f"recent:{event['user_id']}"
    watched_key = f"watched:{event['user_id']}"
    genre_key = f"genre_affinity:{event['user_id']}"
    popularity_key = f"popular:{event['region']}"

    event_ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00")).timestamp()
    redis_client.zadd(recent_key, {event["item_id"]: event_ts})
    redis_client.zremrangebyrank(recent_key, 0, -21)

    event_type = event["event_type"]
    watch_seconds = int(event.get("watch_seconds") or 0)
    completion_pct = float(event.get("completion_pct") or 0.0)

    if event_type in {"play_start", "watch_progress", "complete"}:
        if completion_pct >= 5.0 or watch_seconds >= 120 or event_type == "complete":
            redis_client.sadd(watched_key, event["item_id"])

    increment = {
        "impression": 0.1,
        "click": 0.3,
        "play_start": 0.8,
        "watch_progress": 1.2,
        "complete": 2.0,
        "watchlist_add": 0.5,
    }.get(event_type, 0.1)

    for genre in item.genres.split(","):
        redis_client.hincrbyfloat(genre_key, genre.strip(), increment)

    redis_client.zincrby(popularity_key, increment, event["item_id"])


def serve_metrics():
    start_http_server(settings.metrics_port)


def main() -> None:
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_dependencies()
    Base.metadata.create_all(bind=engine)
    consumer = make_consumer()
    consumer.subscribe([settings.kafka_topic_events])
    s3 = make_s3_client()
    ensure_bucket(s3)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka consumer error", extra={"error": str(msg.error())})
                continue

            with tracer.start_as_current_span("process_event"):
                event = json.loads(msg.value().decode("utf-8"))
                save_raw_event(s3, event)
                with SessionLocal() as session:
                    inserted = persist_interaction(session, event)
                    update_online_features(session, event)
                    session.commit()
                if inserted:
                    EVENTS_CONSUMED.labels(event_type=event["event_type"]).inc()
                    logger.info("Processed event", extra=event)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()