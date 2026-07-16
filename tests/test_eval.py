"""
Unit tests for common/eval.py.

All functions are pure (no DB, no Redis) so they run without any mocking.
"""
import math
from types import SimpleNamespace

import pytest

from common.eval import (
    DEFAULT_WEIGHTS,
    build_feature_matrix,
    build_score_lookups,
    dcg_at_k,
    evaluate_model,
    hit_rate_at_k,
    ndcg_at_k,
    rank_candidates_offline,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _item(item_id, genres="drama", release_year=2024, regions="GLOBAL", active=True):
    return SimpleNamespace(
        item_id=item_id,
        genres=genres,
        release_year=release_year,
        available_regions=regions,
        is_active=active,
        actors="",
        director="",
        synopsis="",
    )


def _ix(user_id, item_id, event_type, region="US", completion_pct=0.0):
    from datetime import datetime, timezone
    return SimpleNamespace(
        user_id=user_id,
        item_id=item_id,
        event_type=event_type,
        region=region,
        completion_pct=completion_pct,
        event_ts=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# DCG / NDCG
# ---------------------------------------------------------------------------

class TestDcgAtK:
    def test_relevant_item_at_position_one(self):
        assert dcg_at_k({"a"}, ["a", "b", "c"], 3) == pytest.approx(1.0 / math.log2(2))

    def test_relevant_item_at_position_two(self):
        assert dcg_at_k({"a"}, ["b", "a", "c"], 3) == pytest.approx(1.0 / math.log2(3))

    def test_no_relevant_items(self):
        assert dcg_at_k({"z"}, ["a", "b", "c"], 3) == 0.0

    def test_multiple_relevant_items_accumulate(self):
        score = dcg_at_k({"a", "b"}, ["a", "b", "c"], 3)
        assert score == pytest.approx(1.0 / math.log2(2) + 1.0 / math.log2(3))

    def test_item_beyond_k_not_counted(self):
        assert dcg_at_k({"c"}, ["a", "b", "c"], 2) == 0.0


class TestNdcgAtK:
    def test_empty_relevant_set_is_zero(self):
        assert ndcg_at_k(set(), ["a", "b"], 10) == 0.0

    def test_perfect_ranking_is_one(self):
        assert ndcg_at_k({"a"}, ["a", "b", "c"], 10) == pytest.approx(1.0)

    def test_worst_ranking_within_k_is_low(self):
        # Relevant item at position k; ideal has it at position 1
        score = ndcg_at_k({"c"}, ["a", "b", "c"], 3)
        assert 0.0 < score < 1.0

    def test_no_overlap_is_zero(self):
        assert ndcg_at_k({"z"}, ["a", "b", "c"], 3) == 0.0

    def test_known_value(self):
        # ideal DCG = 1/log2(2); actual DCG = 1/log2(3)
        expected = (1.0 / math.log2(3)) / (1.0 / math.log2(2))
        assert ndcg_at_k({"a"}, ["b", "a"], 2) == pytest.approx(expected)


class TestHitRateAtK:
    def test_relevant_item_in_top_k_is_one(self):
        assert hit_rate_at_k({"b"}, ["a", "b", "c"], 3) == 1.0

    def test_relevant_item_outside_k_is_zero(self):
        assert hit_rate_at_k({"c"}, ["a", "b", "c"], 2) == 0.0

    def test_empty_relevant_set_is_zero(self):
        assert hit_rate_at_k(set(), ["a", "b"], 5) == 0.0

    def test_no_overlap_is_zero(self):
        assert hit_rate_at_k({"z"}, ["a", "b", "c"], 10) == 0.0


# ---------------------------------------------------------------------------
# build_score_lookups
# ---------------------------------------------------------------------------

class TestBuildScoreLookups:
    def test_collab_rows_indexed_by_source(self):
        rows = [
            SimpleNamespace(source_item_id="m1", neighbor_item_id="m2", score=0.8),
            SimpleNamespace(source_item_id="m1", neighbor_item_id="m3", score=0.5),
        ]
        collab, _, _ = build_score_lookups(rows, [], [])
        assert collab["m1"]["m2"] == pytest.approx(0.8)
        assert collab["m1"]["m3"] == pytest.approx(0.5)

    def test_trending_rows_indexed_by_region(self):
        rows = [SimpleNamespace(region="US", item_id="m1", score=1.5)]
        _, _, trending = build_score_lookups([], [], rows)
        assert trending["US"]["m1"] == pytest.approx(1.5)

    def test_empty_inputs_return_empty_dicts(self):
        c, ct, t = build_score_lookups([], [], [])
        assert c == {} and ct == {} and t == {}


# ---------------------------------------------------------------------------
# rank_candidates_offline
# ---------------------------------------------------------------------------

class TestRankCandidatesOffline:
    def _setup(self):
        items = [
            _item("m1", genres="sci-fi", release_year=2025),
            _item("m2", genres="drama", release_year=2020),
            _item("m3", genres="comedy", release_year=2024),
        ]
        item_map = {i.item_id: i for i in items}
        # m1 is a collaborative neighbor of m_seed, m2 is a content neighbor
        collab_map = {"m_seed": {"m1": 1.0, "m3": 0.5}}
        content_map = {"m_seed": {"m2": 1.0}}
        trending_map = {"US": {"m3": 1.0}}
        return item_map, collab_map, content_map, trending_map

    def test_returns_at_most_k_items(self):
        item_map, collab, content, trending = self._setup()
        result = rank_candidates_offline(["m_seed"], "US", collab, content, trending, item_map, DEFAULT_WEIGHTS, k=2)
        assert len(result) <= 2

    def test_inactive_items_excluded(self):
        items = [_item("m1", active=False), _item("m2")]
        item_map = {i.item_id: i for i in items}
        collab = {"seed": {"m1": 1.0, "m2": 0.5}}
        result = rank_candidates_offline(["seed"], "US", collab, {}, {}, item_map, DEFAULT_WEIGHTS)
        assert "m1" not in result

    def test_region_restricted_items_excluded(self):
        items = [_item("m1", regions="UK"), _item("m2", regions="GLOBAL")]
        item_map = {i.item_id: i for i in items}
        collab = {"seed": {"m1": 1.0, "m2": 0.8}}
        result = rank_candidates_offline(["seed"], "US", collab, {}, {}, item_map, DEFAULT_WEIGHTS)
        assert "m1" not in result
        assert "m2" in result

    def test_higher_weighted_signal_wins(self):
        # m1 has high collab (weight 0.35), m2 has high content (weight 0.25)
        items = [_item("m1"), _item("m2")]
        item_map = {i.item_id: i for i in items}
        collab = {"seed": {"m1": 1.0}}
        content = {"seed": {"m2": 1.0}}
        result = rank_candidates_offline(["seed"], "US", collab, content, {}, item_map, DEFAULT_WEIGHTS, k=2)
        assert result[0] == "m1"

    def test_empty_history_returns_only_trending(self):
        items = [_item("m1"), _item("m2")]
        item_map = {i.item_id: i for i in items}
        trending = {"US": {"m1": 1.0}}
        result = rank_candidates_offline([], "US", {}, {}, trending, item_map, DEFAULT_WEIGHTS)
        assert "m1" in result


# ---------------------------------------------------------------------------
# evaluate_model
# ---------------------------------------------------------------------------

class TestEvaluateModel:
    def _make_scenario(self):
        """
        Two users; m1/m2 as train items, m_test as a test positive.
        The neighbor tables make m_test a top recommendation for both users.
        """
        items = [_item("m1"), _item("m2"), _item("m_test")]
        item_map = {i.item_id: i for i in items}

        train_ixs = [
            _ix("u1", "m1", "complete"),
            _ix("u2", "m2", "complete"),
        ]
        test_ixs = [
            _ix("u1", "m_test", "complete"),
            _ix("u2", "m_test", "complete"),
        ]
        collab_map = {"m1": {"m_test": 1.0}, "m2": {"m_test": 1.0}}
        content_map: dict = {}
        trending_map: dict = {}
        return test_ixs, train_ixs, collab_map, content_map, trending_map, item_map

    def test_perfect_model_returns_high_ndcg(self):
        test_ixs, train_ixs, collab, content, trending, item_map = self._make_scenario()
        result = evaluate_model(test_ixs, train_ixs, collab, content, trending, item_map, DEFAULT_WEIGHTS, k=10)
        assert result["ndcg_at_k"] > 0.5
        assert result["hit_rate_at_k"] == 1.0

    def test_no_overlap_returns_zero_metrics(self):
        items = [_item("m1"), _item("m2")]
        item_map = {i.item_id: i for i in items}
        train_ixs = [_ix("u1", "m1", "complete")]
        test_ixs = [_ix("u1", "m2", "complete")]  # m2 is never recommended
        result = evaluate_model(test_ixs, train_ixs, {}, {}, {}, item_map, DEFAULT_WEIGHTS, k=10)
        assert result["ndcg_at_k"] == 0.0
        assert result["hit_rate_at_k"] == 0.0

    def test_coverage_is_fraction_of_catalog(self):
        test_ixs, train_ixs, collab, content, trending, item_map = self._make_scenario()
        result = evaluate_model(test_ixs, train_ixs, collab, content, trending, item_map, DEFAULT_WEIGHTS, k=10)
        # 1 unique recommended item (m_test) out of 3 in catalog
        assert 0.0 < result["coverage"] <= 1.0

    def test_test_user_count_matches_users_with_positives(self):
        test_ixs, train_ixs, collab, content, trending, item_map = self._make_scenario()
        result = evaluate_model(test_ixs, train_ixs, collab, content, trending, item_map, DEFAULT_WEIGHTS)
        assert result["test_user_count"] == 2

    def test_empty_test_set_returns_zero_metrics(self):
        items = [_item("m1")]
        item_map = {i.item_id: i for i in items}
        result = evaluate_model([], [], {}, {}, {}, item_map, DEFAULT_WEIGHTS)
        assert result["ndcg_at_k"] == 0.0
        assert result["test_user_count"] == 0


# ---------------------------------------------------------------------------
# build_feature_matrix
# ---------------------------------------------------------------------------

class TestBuildFeatureMatrix:
    def test_returns_x_y_with_matching_lengths(self):
        items = [_item("m1", genres="sci-fi"), _item("m2", genres="drama"), _item("m3", genres="sci-fi")]
        train_ixs = [
            _ix("u1", "m1", "complete", completion_pct=100.0),
            _ix("u1", "m2", "complete", completion_pct=100.0),
            _ix("u1", "m3", "impression"),
        ]
        collab = {"m1": {"m3": 0.8}, "m2": {"m3": 0.6}}
        X, y = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert len(X) == len(y)

    def test_positive_label_for_engagement_events(self):
        items = [_item("m1"), _item("m2")]
        # u1 plays m1 (positive) and u1 also has m2 as a neighbor candidate
        train_ixs = [
            _ix("u1", "m1", "complete", completion_pct=100.0),
            _ix("u1", "m2", "impression"),
        ]
        collab = {"m1": {"m2": 1.0}}
        X, y = build_feature_matrix(train_ixs, items, collab, {}, {})
        # m2 is a candidate derived from m1; u1 only impressed on m2 → label 0
        if X:
            assert 0 in y

    def test_each_feature_vector_has_six_dimensions(self):
        items = [_item("m1"), _item("m2"), _item("m3")]
        train_ixs = [
            _ix("u1", "m1", "complete", completion_pct=100.0),
            _ix("u1", "m2", "complete", completion_pct=80.0),
        ]
        collab = {"m1": {"m3": 0.9}}
        X, y = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert all(len(row) == 6 for row in X)

    def test_users_with_fewer_than_two_played_items_are_skipped(self):
        items = [_item("m1"), _item("m2")]
        # u1 only has one played item → skipped
        train_ixs = [_ix("u1", "m1", "complete", completion_pct=100.0)]
        collab = {"m1": {"m2": 0.9}}
        X, y = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert X == [] and y == []
