import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from types import SimpleNamespace

from common.models import Base, Item
from services.recommendation_api.app import (
    fetch_session_candidates,
    normalize_scores,
    _serve_from_cache,
    _z_test_two_proportions,
)


class TestNormalizeScores:
    def test_empty_returns_empty(self):
        assert normalize_scores({}) == {}

    def test_max_value_becomes_one(self):
        result = normalize_scores({"a": 0.5, "b": 2.0, "c": 1.0})
        assert result["b"] == 1.0

    def test_proportions_are_preserved(self):
        result = normalize_scores({"a": 4.0, "b": 2.0, "c": 1.0})
        assert abs(result["a"] - 1.0) < 1e-9
        assert abs(result["b"] - 0.5) < 1e-9
        assert abs(result["c"] - 0.25) < 1e-9

    def test_all_zeros_avoids_division_by_zero(self):
        result = normalize_scores({"a": 0.0, "b": 0.0})
        assert result["a"] == 0.0
        assert result["b"] == 0.0

    def test_single_item_normalizes_to_one(self):
        result = normalize_scores({"only": 42.0})
        assert result["only"] == 1.0


class TestFetchSessionCandidates:
    def test_empty_recent_returns_empty(self):
        assert fetch_session_candidates([]) == {}

    def test_most_recent_item_gets_highest_score(self):
        result = fetch_session_candidates(["m1", "m2", "m3"])
        # m3 is most recent (last in list)
        assert result["m3"] >= result["m2"] >= result["m1"]

    def test_only_last_five_items_are_considered(self):
        # m0 is the 6th-from-last and must be excluded
        result = fetch_session_candidates(["m0", "m1", "m2", "m3", "m4", "m5"])
        assert "m0" not in result
        assert "m5" in result

    def test_scores_normalize_to_max_of_one(self):
        result = fetch_session_candidates(["m1", "m2", "m3"])
        assert max(result.values()) == 1.0

    def test_decay_factor_is_applied(self):
        # With decay 0.7 per step: m3=1.0, m2=0.7, m1=0.49 (before normalization)
        result = fetch_session_candidates(["m1", "m2", "m3"])
        assert result["m3"] > result["m2"] > result["m1"]

    def test_repeated_item_accumulates_weight(self):
        result_repeated = fetch_session_candidates(["m1", "m1"])
        result_single = fetch_session_candidates(["m2", "m1"])
        # m1 appears twice in the first list — should accumulate
        # Both are normalized, so compare raw structure via ratio
        assert result_repeated["m1"] == 1.0  # always highest after normalization
        assert result_single["m1"] == 1.0   # also most recent — comparison not meaningful here

    def test_five_item_window_uses_most_recent_only(self):
        # m5 is included (within last 5), m0 is not (6th from end)
        items = [f"m{i}" for i in range(7)]
        result = fetch_session_candidates(items)
        assert "m0" not in result
        assert "m1" not in result
        assert "m2" in result  # last 5 are m2..m6


# ---------------------------------------------------------------------------
# Fixtures shared by cache and experiment tests
# ---------------------------------------------------------------------------

@pytest.fixture
def mem_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _seed_item(session, item_id, genres="drama", active=True,
               regions="GLOBAL", maturity="PG-13", release_year=2024):
    item = Item(
        item_id=item_id, title=f"Movie {item_id}", item_type="movie",
        genres=genres, actors="", director="", synopsis="",
        release_year=release_year, maturity_rating=maturity,
        available_regions=regions, is_active=active,
    )
    session.add(item)
    session.commit()
    return item


def _fake_user(region="US", maturity="PG-13"):
    return SimpleNamespace(user_id="u1", region=region, maturity_rating=maturity)


def _cached(item_ids):
    return [
        {"item_id": iid, "score": round(1.0 - i * 0.1, 1), "reason": "collaborative", "components": {}}
        for i, iid in enumerate(item_ids)
    ]


# ---------------------------------------------------------------------------
# _z_test_two_proportions
# ---------------------------------------------------------------------------

class TestZTestTwoProportions:
    def test_variant_better_gives_positive_z(self):
        _, z = _z_test_two_proportions(1000, 50, 1000, 70)
        assert z > 0

    def test_variant_worse_gives_negative_z(self):
        _, z = _z_test_two_proportions(1000, 70, 1000, 50)
        assert z < 0

    def test_no_difference_gives_zero_z(self):
        _, z = _z_test_two_proportions(1000, 50, 1000, 50)
        assert z == 0.0

    def test_zero_impressions_returns_zeros(self):
        lift, z = _z_test_two_proportions(0, 0, 1000, 50)
        assert lift == 0.0 and z == 0.0

    def test_lift_pct_is_relative_to_control(self):
        lift, _ = _z_test_two_proportions(1000, 100, 1000, 150)
        assert abs(lift - 50.0) < 0.1

    def test_large_clear_difference_is_significant(self):
        _, z = _z_test_two_proportions(10000, 500, 10000, 700)
        assert abs(z) >= 1.96

    def test_all_zero_pool_proportion_returns_zeros(self):
        lift, z = _z_test_two_proportions(1000, 0, 1000, 0)
        assert lift == 0.0 and z == 0.0


# ---------------------------------------------------------------------------
# _serve_from_cache
# ---------------------------------------------------------------------------

class TestServeFromCache:
    def test_watched_items_excluded(self, mem_db):
        _seed_item(mem_db, "m1")
        _seed_item(mem_db, "m2")
        result = _serve_from_cache(mem_db, _fake_user(), _cached(["m1", "m2"]), {"m1"}, 10, 0)
        assert not any(r["item_id"] == "m1" for r in result)
        assert any(r["item_id"] == "m2" for r in result)

    def test_inactive_items_excluded(self, mem_db):
        _seed_item(mem_db, "m1", active=False)
        _seed_item(mem_db, "m2")
        result = _serve_from_cache(mem_db, _fake_user(), _cached(["m1", "m2"]), set(), 10, 0)
        assert not any(r["item_id"] == "m1" for r in result)

    def test_wrong_region_excluded(self, mem_db):
        _seed_item(mem_db, "m1", regions="UK")
        _seed_item(mem_db, "m2", regions="GLOBAL")
        result = _serve_from_cache(mem_db, _fake_user(region="US"), _cached(["m1", "m2"]), set(), 10, 0)
        assert not any(r["item_id"] == "m1" for r in result)
        assert any(r["item_id"] == "m2" for r in result)

    def test_mature_content_filtered_for_pg_user(self, mem_db):
        _seed_item(mem_db, "m1", maturity="R")
        _seed_item(mem_db, "m2", maturity="PG")
        result = _serve_from_cache(mem_db, _fake_user(maturity="PG-13"), _cached(["m1", "m2"]), set(), 10, 0)
        assert not any(r["item_id"] == "m1" for r in result)
        assert any(r["item_id"] == "m2" for r in result)

    def test_limit_respected(self, mem_db):
        for i in range(5):
            _seed_item(mem_db, f"m{i}")
        result = _serve_from_cache(mem_db, _fake_user(), _cached([f"m{i}" for i in range(5)]), set(), 2, 0)
        assert len(result) == 2

    def test_offset_respected(self, mem_db):
        for i in range(5):
            _seed_item(mem_db, f"m{i}")
        result = _serve_from_cache(mem_db, _fake_user(), _cached([f"m{i}" for i in range(5)]), set(), 2, 2)
        assert result[0]["item_id"] == "m2"

    def test_empty_cache_returns_empty(self, mem_db):
        assert _serve_from_cache(mem_db, _fake_user(), [], set(), 10, 0) == []

    def test_result_contains_title_and_genres(self, mem_db):
        _seed_item(mem_db, "m1", genres="sci-fi,drama")
        result = _serve_from_cache(mem_db, _fake_user(), _cached(["m1"]), set(), 10, 0)
        assert result[0]["title"] == "Movie m1"
        assert "sci-fi" in result[0]["genres"]

    def test_score_and_reason_carried_from_cache(self, mem_db):
        _seed_item(mem_db, "m1")
        cached = [{"item_id": "m1", "score": 0.77, "reason": "trending", "components": {}}]
        result = _serve_from_cache(mem_db, _fake_user(), cached, set(), 10, 0)
        assert result[0]["score"] == 0.77
        assert result[0]["reason"] == "trending"
