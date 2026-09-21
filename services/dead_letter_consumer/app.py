"""Dead-letter queue consumer.

Reads failed events from the dead-letter Kafka topic, archives them to
MinIO for inspection and manual replay, and emits Prometheus metrics.
Events that cannot be parsed or archived are counted separately so that
silent failures are visible in alerting.
"""
import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone

import boto3
from botocore.client import Config as BotoConfig
from confluent_kafka import Consumer
from prometheus_client import start_http_server
from sqlalchemy import text

from common.config import settings
from common.db import engine
from common.logging_utils import configure_logging
from common.metrics import DLQ_EVENTS_ARCHIVED, DLQ_REPLAY_FAILURES
from common.telemetry import setup_tracing

SERVICE_NAME = "dead-letter-consumer"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
tracer = setup_tracing(SERVICE_NAME)

_stop = threading.Event()
_DLQ_BUCKET = "dead-letter"


def _handle_signal(signum, frame):
    logger.info("Signal received — draining then stopping", extra={"signal": signum})
    _stop.set()


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"{settings.kafka_group_id}-dlq",
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


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=_DLQ_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=_DLQ_BUCKET)


def archive_event(s3, raw_bytes: bytes) -> str:
    """Archive raw event bytes to MinIO. Returns the S3 key written."""
    now = datetime.now(timezone.utc)
    try:
        event = json.loads(raw_bytes.decode("utf-8", errors="replace"))
        event_type = event.get("event_type", "unknown")
        event_id = event.get("event_id", "no-id")
    except Exception:
        event_type = "parse-error"
        event_id = "no-id"

    key = (
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"{event_type}/{event_id}.json"
    )
    s3.put_object(Bucket=_DLQ_BUCKET, Key=key, Body=raw_bytes)
    return key


def wait_for_dependencies() -> None:
    for _ in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Dependencies ready")
            return
        except Exception as exc:
            logger.info("Waiting for dependencies", extra={"error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Dependencies unavailable")


def serve_metrics():
    start_http_server(settings.metrics_port)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_dependencies()

    consumer = make_consumer()
    consumer.subscribe([settings.kafka_topic_dead_letter])
    s3 = make_s3_client()
    ensure_bucket(s3)

    logger.info("Dead-letter consumer started", extra={"topic": settings.kafka_topic_dead_letter})

    try:
        while not _stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka consumer error", extra={"error": str(msg.error())})
                continue

            with tracer.start_as_current_span("archive_dead_letter"):
                raw = msg.value()
                try:
                    key = archive_event(s3, raw)
                    # Parse for structured logging and metrics
                    try:
                        event = json.loads(raw.decode("utf-8", errors="replace"))
                        event_type = event.get("event_type", "unknown")
                    except Exception:
                        event_type = "parse-error"

                    DLQ_EVENTS_ARCHIVED.labels(event_type=event_type).inc()
                    logger.warning(
                        "Dead-letter event archived",
                        extra={
                            "event_type": event_type,
                            "s3_key": key,
                            "raw_size": len(raw),
                        },
                    )
                except Exception as exc:
                    DLQ_REPLAY_FAILURES.inc()
                    logger.error(
                        "Failed to archive dead-letter event",
                        extra={
                            "error": str(exc),
                            "raw": raw.decode("utf-8", errors="replace")[:200],
                        },
                    )
    finally:
        consumer.close()
        logger.info("Dead-letter consumer shut down")


if __name__ == "__main__":
    main()
