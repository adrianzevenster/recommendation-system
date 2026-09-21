"""Unit tests for services/dead_letter_consumer/app.py.

All Kafka, S3, and database calls are mocked — no live infrastructure needed.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw(event_type="click", event_id="e1") -> bytes:
    return json.dumps({"event_type": event_type, "event_id": event_id}).encode()


# ---------------------------------------------------------------------------
# _handle_signal
# ---------------------------------------------------------------------------

class TestHandleSignal:
    def test_sets_stop_event(self):
        from services.dead_letter_consumer.app import _handle_signal, _stop
        _stop.clear()
        _handle_signal(15, None)
        assert _stop.is_set()
        _stop.clear()  # restore for other tests


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------

class TestEnsureBucket:
    def test_does_not_create_bucket_when_it_exists(self):
        from services.dead_letter_consumer.app import ensure_bucket
        s3 = MagicMock()
        s3.head_bucket.return_value = {}
        ensure_bucket(s3)
        s3.create_bucket.assert_not_called()

    def test_creates_bucket_when_head_raises(self):
        from services.dead_letter_consumer.app import ensure_bucket
        s3 = MagicMock()
        s3.head_bucket.side_effect = Exception("NoSuchBucket")
        ensure_bucket(s3)
        s3.create_bucket.assert_called_once_with(Bucket="dead-letter")


# ---------------------------------------------------------------------------
# archive_event
# ---------------------------------------------------------------------------

class TestArchiveEvent:
    def test_returns_s3_key_string(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        key = archive_event(s3, _raw())
        assert isinstance(key, str)
        assert key.endswith(".json")

    def test_key_includes_event_type_and_id(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        key = archive_event(s3, _raw(event_type="complete", event_id="evt-99"))
        assert "complete" in key
        assert "evt-99" in key

    def test_key_includes_date_partitions(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        key = archive_event(s3, _raw())
        now = datetime.now(timezone.utc)
        assert f"year={now.year}" in key
        assert "month=" in key
        assert "day=" in key
        assert "hour=" in key

    def test_puts_object_to_dead_letter_bucket(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        archive_event(s3, _raw())
        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "dead-letter"

    def test_invalid_json_uses_parse_error_type(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        key = archive_event(s3, b"not valid json {{{{")
        assert "parse-error" in key

    def test_missing_event_type_defaults_to_unknown(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        raw = json.dumps({"event_id": "e42"}).encode()
        key = archive_event(s3, raw)
        assert "unknown" in key

    def test_missing_event_id_defaults_to_no_id(self):
        from services.dead_letter_consumer.app import archive_event
        s3 = MagicMock()
        raw = json.dumps({"event_type": "click"}).encode()
        key = archive_event(s3, raw)
        assert "no-id" in key


# ---------------------------------------------------------------------------
# make_consumer
# ---------------------------------------------------------------------------

class TestMakeConsumer:
    def test_returns_consumer_instance(self):
        from services.dead_letter_consumer.app import make_consumer
        mock_consumer = MagicMock()
        with patch("services.dead_letter_consumer.app.Consumer", return_value=mock_consumer):
            result = make_consumer()
        assert result is mock_consumer

    def test_consumer_uses_dlq_group_id(self):
        from services.dead_letter_consumer.app import make_consumer
        from common.config import settings
        with patch("services.dead_letter_consumer.app.Consumer") as mock_cls:
            make_consumer()
        config = mock_cls.call_args[0][0]
        assert "-dlq" in config["group.id"]


# ---------------------------------------------------------------------------
# make_s3_client
# ---------------------------------------------------------------------------

class TestMakeS3Client:
    def test_returns_boto3_client(self):
        from services.dead_letter_consumer.app import make_s3_client
        mock_client = MagicMock()
        with patch("services.dead_letter_consumer.app.boto3.client", return_value=mock_client):
            result = make_s3_client()
        assert result is mock_client

    def test_uses_minio_endpoint(self):
        from services.dead_letter_consumer.app import make_s3_client
        from common.config import settings
        with patch("services.dead_letter_consumer.app.boto3.client") as mock_boto:
            make_s3_client()
        call_kwargs = mock_boto.call_args.kwargs
        assert call_kwargs["endpoint_url"] == settings.minio_endpoint


# ---------------------------------------------------------------------------
# wait_for_dependencies
# ---------------------------------------------------------------------------

class TestWaitForDependencies:
    def test_returns_immediately_when_postgres_ready(self):
        from services.dead_letter_consumer.app import wait_for_dependencies
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        with patch("services.dead_letter_consumer.app.engine") as mock_engine:
            mock_engine.connect.return_value = mock_conn
            wait_for_dependencies()  # should not raise
        mock_conn.execute.assert_called_once()

    def test_raises_runtime_error_after_max_attempts(self):
        from services.dead_letter_consumer.app import wait_for_dependencies
        with patch("services.dead_letter_consumer.app.engine") as mock_engine, \
             patch("services.dead_letter_consumer.app.time.sleep"):
            mock_engine.connect.side_effect = Exception("connection refused")
            with pytest.raises(RuntimeError, match="unavailable"):
                wait_for_dependencies()
