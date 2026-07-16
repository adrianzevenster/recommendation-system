"""
Integration tests — use SQLite in-memory + fakeredis to verify full event
processing flows without any external infrastructure.
"""
from datetime import datetime, timezone
from unittest.mock import patch

import fakeredis
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.models import Base, Interaction, Item, User


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def rdb():
    return fakeredis.FakeRedis(decode_responses=True)


def _seed_item(session, item_id="m1", genres="sci-fi,thriller", regions="GLOBAL"):
    item = Item(
        item_id=item_id,
        title=f"Movie {item_id}",
        item_type="movie",
        genres=genres,
        actors="",
        director="",
        synopsis="",
        release_year=2024,
        maturity_rating="PG-13",
        available_regions=regions,
    )
    session.add(item)
    session.commit()
    return item


def _event(event_type, item_id="m1", user_id="u1", region="US",
           watch_seconds=0, completion_pct=0.0,
           event_id="evt-1", ts="2025-06-01T10:00:00Z"):
    return {
        "event_id": event_id,
        "user_id": user_id,
        "item_id": item_id,
        "event_type": event_type,
        "watch_seconds": watch_seconds,
        "completion_pct": completion_pct,
        "region": region,
        "device_type": "web",
        "event_ts": ts,
    }


# ---------------------------------------------------------------------------
# Stream processor: online feature updates
# ---------------------------------------------------------------------------

class TestOnlineFeatureUpdate:
    def _process(self, db, rdb, event):
        with patch("services.stream_processor.app.redis_client", rdb):
            from services.stream_processor.app import update_online_features
            update_online_features(db, event)

    def test_complete_event_updates_recent_sorted_set(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        assert "m1" in rdb.zrange("recent:u1", 0, -1)

    def test_complete_event_adds_to_watched_set(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        assert rdb.sismember("watched:u1", "m1")

    def test_impression_does_not_add_to_watched_set(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("impression"))
        assert not rdb.sismember("watched:u1", "m1")
        assert "m1" in rdb.zrange("recent:u1", 0, -1)

    def test_genre_affinity_incremented_for_each_genre(self, db, rdb):
        _seed_item(db, genres="sci-fi,thriller")
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        affinity = rdb.hgetall("genre_affinity:u1")
        assert "sci-fi" in affinity
        assert "thriller" in affinity
        assert float(affinity["sci-fi"]) > 0

    def test_regional_popularity_incremented(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        assert rdb.zscore("popular:US", "m1") is not None

    def test_genre_affinity_key_has_ttl(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        assert rdb.ttl("genre_affinity:u1") > 0

    def test_popularity_key_has_ttl(self, db, rdb):
        _seed_item(db)
        self._process(db, rdb, _event("complete", watch_seconds=6600, completion_pct=100.0))
        assert rdb.ttl("popular:US") > 0

    def test_unknown_item_is_noop(self, db, rdb):
        # no item seeded — nothing should be written
        self._process(db, rdb, _event("complete", item_id="ghost"))
        assert rdb.zrange("recent:u1", 0, -1) == []

    def test_partial_watch_adds_to_watched(self, db, rdb):
        _seed_item(db)
        # 10% completion, 300 seconds (≥ 120 threshold)
        self._process(db, rdb, _event("watch_progress", watch_seconds=300, completion_pct=10.0))
        assert rdb.sismember("watched:u1", "m1")

    def test_recent_set_is_capped_at_twenty(self, db, rdb):
        for i in range(25):
            _seed_item(db, item_id=f"item{i}")
            ev = _event("impression", item_id=f"item{i}", event_id=f"e{i}",
                        ts=f"2025-06-{(i % 28) + 1:02d}T10:{i % 60:02d}:00Z")
            self._process(db, rdb, ev)
        assert rdb.zcard("recent:u1") <= 20


# ---------------------------------------------------------------------------
# Stream processor: interaction persistence
# ---------------------------------------------------------------------------

class TestInteractionPersistence:
    def test_new_event_is_persisted(self, db):
        _seed_item(db)
        from services.stream_processor.app import persist_interaction
        ev = _event("click")
        result = persist_interaction(db, ev)
        db.commit()
        assert result is True
        assert db.get(Interaction, "evt-1") is not None

    def test_duplicate_event_is_rejected(self, db):
        _seed_item(db)
        from services.stream_processor.app import persist_interaction
        ev = _event("click")
        persist_interaction(db, ev)
        db.commit()
        result = persist_interaction(db, ev)
        assert result is False

    def test_persisted_interaction_has_correct_fields(self, db):
        _seed_item(db)
        from services.stream_processor.app import persist_interaction
        ev = _event("complete", watch_seconds=6600, completion_pct=100.0, region="ZA")
        persist_interaction(db, ev)
        db.commit()
        ix = db.get(Interaction, "evt-1")
        assert ix.event_type == "complete"
        assert ix.completion_pct == 100.0
        assert ix.region == "ZA"


# ---------------------------------------------------------------------------
# Trainer: temporal split
# ---------------------------------------------------------------------------

class TestTemporalSplit:
    def test_split_preserves_total_count(self):
        from datetime import timedelta
        from services.trainer.app import temporal_split
        now = datetime.now(timezone.utc)
        ixs = [
            _make_ix(i, now + timedelta(hours=i))
            for i in range(100)
        ]
        train, test = temporal_split(ixs, eval_fraction=0.2)
        assert len(train) + len(test) == 100

    def test_split_respects_eval_fraction(self):
        from datetime import timedelta
        from services.trainer.app import temporal_split
        now = datetime.now(timezone.utc)
        ixs = [_make_ix(i, now + timedelta(hours=i)) for i in range(100)]
        train, test = temporal_split(ixs, eval_fraction=0.2)
        assert len(test) == 20
        assert len(train) == 80

    def test_train_timestamps_precede_test(self):
        from datetime import timedelta
        from services.trainer.app import temporal_split
        now = datetime.now(timezone.utc)
        ixs = [_make_ix(i, now + timedelta(hours=i)) for i in range(10)]
        train, test = temporal_split(ixs, eval_fraction=0.3)
        assert max(ix.event_ts for ix in train) <= min(ix.event_ts for ix in test)

    def test_empty_interactions_returns_empty_splits(self):
        from services.trainer.app import temporal_split
        train, test = temporal_split([])
        assert train == [] and test == []


def _make_ix(i, ts):
    return type("Ix", (), {"event_ts": ts, "id": i})()
