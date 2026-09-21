import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.stream_processor.app import (
    ensure_bucket,
    persist_interaction,
    save_raw_event,
    update_online_features,
)


class TestSaveRawEventKey:
    def test_key_is_partitioned_by_datetime_components(self):
        s3 = MagicMock()
        event = {
            "event_id": "evt-abc",
            "user_id": "u001",
            "item_id": "m001",
            "event_type": "click",
            "event_ts": "2025-06-15T10:30:00Z",
        }
        save_raw_event(s3, event)
        key = s3.put_object.call_args.kwargs["Key"]
        assert "year=2025" in key
        assert "month=06" in key
        assert "day=15" in key
        assert "hour=10" in key

    def test_key_includes_user_id_and_event_id(self):
        s3 = MagicMock()
        event = {
            "event_id": "evt-xyz",
            "user_id": "u007",
            "event_ts": "2025-01-01T00:00:00Z",
        }
        save_raw_event(s3, event)
        key = s3.put_object.call_args.kwargs["Key"]
        assert "u007" in key
        assert "evt-xyz" in key

    def test_body_is_json_encoded_event(self):
        s3 = MagicMock()
        event = {"event_id": "e1", "user_id": "u1", "event_ts": "2025-03-01T12:00:00Z"}
        save_raw_event(s3, event)
        body = s3.put_object.call_args.kwargs["Body"]
        assert json.loads(body) == event

    def test_uses_correct_bucket(self):
        s3 = MagicMock()
        event = {"event_id": "e1", "user_id": "u1", "event_ts": "2025-03-01T12:00:00Z"}
        save_raw_event(s3, event)
        from common.config import settings
        assert s3.put_object.call_args.kwargs["Bucket"] == settings.minio_bucket


class TestPersistInteraction:
    def test_new_event_is_inserted_and_returns_true(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        event = {
            "event_id": "e1",
            "user_id": "u1",
            "item_id": "m1",
            "event_type": "click",
            "event_ts": "2025-01-01T00:00:00Z",
            "watch_seconds": 0,
            "completion_pct": 0.0,
            "region": "US",
            "device_type": "web",
        }
        result = persist_interaction(session, event)
        assert result is True
        session.add.assert_called_once()

    def test_duplicate_event_is_skipped_and_returns_false(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = MagicMock()
        event = {
            "event_id": "e1",
            "user_id": "u1",
            "item_id": "m1",
            "event_type": "click",
            "event_ts": "2025-01-01T00:00:00Z",
        }
        result = persist_interaction(session, event)
        assert result is False
        session.add.assert_not_called()

    def test_inserted_interaction_has_correct_fields(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        event = {
            "event_id": "e99",
            "user_id": "u5",
            "item_id": "m7",
            "event_type": "complete",
            "event_ts": "2025-06-01T08:00:00Z",
            "watch_seconds": 6600,
            "completion_pct": 100.0,
            "region": "ZA",
            "device_type": "tv",
        }
        persist_interaction(session, event)
        added = session.add.call_args[0][0]
        assert added.event_id == "e99"
        assert added.user_id == "u5"
        assert added.item_id == "m7"
        assert added.event_type == "complete"
        assert added.region == "ZA"


class TestUpdateOnlineFeatures:
    def _make_event(self, event_type="complete", completion_pct=100.0, watch_seconds=6600):
        return {
            "user_id": "u1",
            "item_id": "m1",
            "event_type": event_type,
            "event_ts": "2025-01-01T00:00:00Z",
            "watch_seconds": watch_seconds,
            "completion_pct": completion_pct,
            "region": "US",
        }

    def test_unknown_item_is_a_noop(self):
        session = MagicMock()
        session.get.return_value = None
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event())
            mock_redis.zadd.assert_not_called()

    def test_recent_sorted_set_updated(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi,thriller")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event())
            recent_calls = [c for c in mock_redis.zadd.call_args_list if c[0][0] == "recent:u1"]
            assert len(recent_calls) == 1

    def test_complete_event_adds_to_watched_zset(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event(event_type="complete", completion_pct=100.0))
            # watched: is now a ZSET — assert zadd was called with the watched key
            watched_zadd_calls = [
                c for c in mock_redis.zadd.call_args_list
                if c[0][0] == "watched:u1"
            ]
            assert len(watched_zadd_calls) == 1

    def test_impression_does_not_add_to_watched_zset(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event(event_type="impression",
                                                              completion_pct=0.0, watch_seconds=0))
            watched_zadd_calls = [
                c for c in mock_redis.zadd.call_args_list
                if c[0][0] == "watched:u1"
            ]
            assert len(watched_zadd_calls) == 0

    def test_genre_affinity_updated_for_each_genre(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi,thriller")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event())
            calls = [c[0][1] for c in mock_redis.hincrbyfloat.call_args_list]
            assert "sci-fi" in calls
            assert "thriller" in calls

    def test_play_start_with_low_completion_does_not_add_to_watched(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="drama")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            event = self._make_event(event_type="play_start", completion_pct=1.0, watch_seconds=10)
            update_online_features(session, event)
            watched_calls = [c for c in mock_redis.zadd.call_args_list if c[0][0] == "watched:u1"]
            assert len(watched_calls) == 0

    def test_watch_progress_with_high_completion_adds_to_watched(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="drama")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            event = self._make_event(event_type="watch_progress", completion_pct=80.0, watch_seconds=3000)
            update_online_features(session, event)
            watched_calls = [c for c in mock_redis.zadd.call_args_list if c[0][0] == "watched:u1"]
            assert len(watched_calls) == 1


# ---------------------------------------------------------------------------
# _handle_signal
# ---------------------------------------------------------------------------

class TestHandleSignal:
    def test_sets_stop_event(self):
        from services.stream_processor.app import _handle_signal, _stop
        _stop.clear()
        _handle_signal(15, None)
        assert _stop.is_set()
        _stop.clear()


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------

class TestEnsureBucket:
    def test_does_not_create_bucket_when_head_succeeds(self):
        s3 = MagicMock()
        s3.head_bucket.return_value = {}
        ensure_bucket(s3)
        s3.create_bucket.assert_not_called()

    def test_creates_bucket_when_head_raises(self):
        from common.config import settings
        s3 = MagicMock()
        s3.head_bucket.side_effect = Exception("NoSuchBucket")
        ensure_bucket(s3)
        s3.create_bucket.assert_called_once_with(Bucket=settings.minio_bucket)


# ---------------------------------------------------------------------------
# make_consumer / make_s3_client / make_producer
# ---------------------------------------------------------------------------

class TestFactoryFunctions:
    def test_make_consumer_returns_consumer(self):
        from services.stream_processor.app import make_consumer
        mock_consumer = MagicMock()
        with patch("services.stream_processor.app.Consumer", return_value=mock_consumer):
            result = make_consumer()
        assert result is mock_consumer

    def test_make_consumer_uses_kafka_settings(self):
        from services.stream_processor.app import make_consumer
        from common.config import settings
        with patch("services.stream_processor.app.Consumer") as mock_cls:
            make_consumer()
        config = mock_cls.call_args[0][0]
        assert config["bootstrap.servers"] == settings.kafka_bootstrap_servers

    def test_make_s3_client_returns_client(self):
        from services.stream_processor.app import make_s3_client
        mock_client = MagicMock()
        with patch("services.stream_processor.app.boto3.client", return_value=mock_client):
            result = make_s3_client()
        assert result is mock_client

    def test_make_producer_returns_producer(self):
        from services.stream_processor.app import make_producer
        mock_producer = MagicMock()
        with patch("confluent_kafka.Producer", return_value=mock_producer):
            result = make_producer()
        assert result is mock_producer
