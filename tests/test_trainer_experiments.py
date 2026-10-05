"""Unit tests for trainer A/B experiment auto-conclusion (_auto_conclude_experiments)."""
import hashlib
from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.models import Base, Experiment, Interaction, _utcnow
from services.trainer.app import _auto_conclude_experiments


@pytest.fixture
def mem_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _side(user_id: str, exp_name: str, traffic_pct: int) -> str:
    """Mirror the trainer's bucketing so tests can force engagement per side."""
    bucket = int(hashlib.md5(f"{user_id}:{exp_name}".encode()).hexdigest(), 16) % 100
    return "variant" if bucket < traffic_pct else "control"


def _add_interactions(session, user_id, started_at, impressions, engagements, eng_type="play_start"):
    ts = started_at + timedelta(hours=1)
    rows = [
        Interaction(event_id=f"{user_id}-imp-{i}", user_id=user_id, item_id="m1",
                    event_type="impression", region="US", event_ts=ts)
        for i in range(impressions)
    ]
    rows += [
        Interaction(event_id=f"{user_id}-eng-{i}", user_id=user_id, item_id="m1",
                    event_type=eng_type, region="US", event_ts=ts)
        for i in range(engagements)
    ]
    session.add_all(rows)


def _make_experiment(session, name, *, age_days, max_duration_days=14, traffic_pct=50):
    created = _utcnow() - timedelta(days=age_days)
    exp = Experiment(name=name, traffic_pct=traffic_pct, max_duration_days=max_duration_days,
                     is_active=True, created_at=created)
    session.add(exp)
    session.commit()
    return exp, created


class TestAutoConcludeExperiments:
    def test_young_experiment_is_not_concluded(self, mem_db):
        _make_experiment(mem_db, "young", age_days=5)
        _auto_conclude_experiments(mem_db)

        refreshed = mem_db.get(Experiment, "young")
        assert refreshed.is_active is True
        assert refreshed.concluded_at is None
        assert refreshed.winning_variant is None

    def test_expired_with_clear_variant_lift_is_significant(self, mem_db):
        _, created = _make_experiment(mem_db, "winner", age_days=20)
        # Variant engages at 0.5, control at 0.1 — a large, clearly significant gap.
        for i in range(60):
            uid = f"u{i:03d}"
            if _side(uid, "winner", 50) == "variant":
                _add_interactions(mem_db, uid, created, impressions=10, engagements=5)
            else:
                _add_interactions(mem_db, uid, created, impressions=10, engagements=1)
        mem_db.commit()

        _auto_conclude_experiments(mem_db)

        refreshed = mem_db.get(Experiment, "winner")
        assert refreshed.is_active is False
        assert refreshed.concluded_at is not None
        assert refreshed.winning_variant == "variant"
        assert refreshed.conclusion_reason == "significant"

    def test_expired_with_no_difference_is_inconclusive(self, mem_db):
        _, created = _make_experiment(mem_db, "flat", age_days=20)
        # Identical engagement rate on both sides => z == 0, not significant.
        for i in range(60):
            _add_interactions(mem_db, f"u{i:03d}", created, impressions=10, engagements=2)
        mem_db.commit()

        _auto_conclude_experiments(mem_db)

        refreshed = mem_db.get(Experiment, "flat")
        assert refreshed.is_active is False
        assert refreshed.winning_variant == "inconclusive"
        assert refreshed.conclusion_reason == "max_duration"

    def test_expired_with_no_interactions_is_inconclusive(self, mem_db):
        _make_experiment(mem_db, "empty", age_days=20)
        _auto_conclude_experiments(mem_db)

        refreshed = mem_db.get(Experiment, "empty")
        assert refreshed.is_active is False
        assert refreshed.winning_variant == "inconclusive"
        assert refreshed.conclusion_reason == "max_duration"
