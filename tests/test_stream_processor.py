import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.stream_processor.app import (
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
            mock_redis.zadd.assert_called_once()
            key = mock_redis.zadd.call_args[0][0]
            assert key == "recent:u1"

    def test_complete_event_adds_to_watched_set(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event(event_type="complete", completion_pct=100.0))
            mock_redis.sadd.assert_called_once()
            assert "watched:u1" in mock_redis.sadd.call_args[0]

    def test_impression_does_not_add_to_watched_set(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event(event_type="impression",
                                                              completion_pct=0.0, watch_seconds=0))
            mock_redis.sadd.assert_not_called()

    def test_genre_affinity_updated_for_each_genre(self):
        session = MagicMock()
        session.get.return_value = SimpleNamespace(item_id="m1", genres="sci-fi,thriller")
        with patch("services.stream_processor.app.redis_client") as mock_redis:
            update_online_features(session, self._make_event())
            calls = [c[0][1] for c in mock_redis.hincrbyfloat.call_args_list]
            assert "sci-fi" in calls
            assert "thriller" in calls
