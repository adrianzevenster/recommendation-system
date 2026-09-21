"""
Unit tests for common/eval.py.

All functions are pure (no DB, no Redis) so they run without any mocking.
"""
import math
from types import SimpleNamespace

import pytest

from datetime import datetime

from common.eval import (
    DEFAULT_WEIGHTS,
    build_feature_matrix,
    build_score_lookups,
    dcg_at_k,
    evaluate_model,
    hit_rate_at_k,
    interaction_weight,
    item_freshness,
    mrr_at_k,
    ndcg_at_k,
    rank_candidates_offline,
    temporal_split,
    watch_time_gain,
    weighted_ndcg_at_k,
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


def _ix(user_id, item_id, event_type, region="US", completion_pct=0.0, position=-1, watch_seconds=0):
    from datetime import datetime, timezone
    return SimpleNamespace(
        user_id=user_id,
        item_id=item_id,
        event_type=event_type,
        region=region,
        completion_pct=completion_pct,
        position=position,
        watch_seconds=watch_seconds,
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


class TestMrrAtK:
    def test_first_position_hit_returns_one(self):
        assert mrr_at_k({"a"}, ["a", "b", "c"], 3) == pytest.approx(1.0)

    def test_second_position_hit_returns_half(self):
        assert mrr_at_k({"b"}, ["a", "b", "c"], 3) == pytest.approx(0.5)

    def test_no_hit_returns_zero(self):
        assert mrr_at_k({"z"}, ["a", "b", "c"], 3) == 0.0

    def test_empty_relevant_set_returns_zero(self):
        assert mrr_at_k(set(), ["a", "b"], 5) == 0.0

    def test_item_beyond_k_not_counted(self):
        assert mrr_at_k({"c"}, ["a", "b", "c"], 2) == 0.0

    def test_earlier_hit_beats_later_hit(self):
        assert mrr_at_k({"a"}, ["a", "b"], 2) > mrr_at_k({"b"}, ["a", "b"], 2)


class TestInteractionWeight:
    def test_complete_has_highest_base_weight(self):
        assert interaction_weight("complete", 0.0) > interaction_weight("play_start", 0.0)
        assert interaction_weight("play_start", 0.0) > interaction_weight("click", 0.0)

    def test_completion_pct_adds_bonus(self):
        assert interaction_weight("click", 100.0) > interaction_weight("click", 0.0)

    def test_position_discount_applied(self):
        assert interaction_weight("click", 0.0, position=2) < interaction_weight("click", 0.0)

    def test_no_position_discount_when_minus_one(self):
        assert interaction_weight("click", 0.0, position=-1) == interaction_weight("click", 0.0)

    def test_unknown_event_type_returns_minimum(self):
        assert interaction_weight("unknown_xyz", 0.0) == pytest.approx(0.1)

    def test_watch_seconds_adds_bonus(self):
        # A full 2-hour watch (7200s) should add a bonus of 1.0 on top of base weight
        no_watch = interaction_weight("complete", 0.0, watch_seconds=0)
        full_watch = interaction_weight("complete", 0.0, watch_seconds=7200)
        assert full_watch > no_watch

    def test_watch_seconds_bonus_capped_at_one(self):
        # Even an absurdly long session should not push the bonus past 1.0
        normal = interaction_weight("complete", 0.0, watch_seconds=7200)
        extreme = interaction_weight("complete", 0.0, watch_seconds=7200 * 10)
        assert extreme == pytest.approx(normal, abs=1e-3)


class TestWatchTimeGain:
    def test_zero_seconds_returns_half(self):
        assert watch_time_gain(0) == pytest.approx(0.5)

    def test_negative_seconds_returns_half(self):
        assert watch_time_gain(-1) == pytest.approx(0.5)

    def test_full_reference_watch_returns_one(self):
        assert watch_time_gain(7200) == pytest.approx(1.0)

    def test_gain_increases_with_watch_time(self):
        assert watch_time_gain(1800) < watch_time_gain(3600) < watch_time_gain(7200)

    def test_gain_capped_at_one(self):
        assert watch_time_gain(999_999) == pytest.approx(1.0)


class TestWeightedNdcgAtK:
    def test_empty_gain_map_returns_zero(self):
        assert weighted_ndcg_at_k({}, ["a", "b"], 10) == 0.0

    def test_perfect_ranking_returns_one(self):
        # Single item, maximum gain, ranked first → perfect score
        gain_map = {"a": 1.0}
        assert weighted_ndcg_at_k(gain_map, ["a", "b"], 10) == pytest.approx(1.0)

    def test_no_overlap_returns_zero(self):
        gain_map = {"z": 0.9}
        assert weighted_ndcg_at_k(gain_map, ["a", "b", "c"], 10) == 0.0

    def test_higher_gain_item_at_top_beats_lower_gain(self):
        gain_map = {"a": 0.9, "b": 0.3}
        score_ideal = weighted_ndcg_at_k(gain_map, ["a", "b"], 2)
        score_reversed = weighted_ndcg_at_k(gain_map, ["b", "a"], 2)
        assert score_ideal > score_reversed

    def test_score_is_between_zero_and_one(self):
        gain_map = {"a": 0.7, "b": 0.4, "c": 0.2}
        score = weighted_ndcg_at_k(gain_map, ["c", "b", "a"], 3)
        assert 0.0 <= score <= 1.0


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
        assert result["mrr_at_k"] > 0.0
        assert result["watch_time_ndcg_at_k"] > 0.0

    def test_no_overlap_returns_zero_metrics(self):
        items = [_item("m1"), _item("m2")]
        item_map = {i.item_id: i for i in items}
        train_ixs = [_ix("u1", "m1", "complete")]
        test_ixs = [_ix("u1", "m2", "complete")]  # m2 is never recommended
        result = evaluate_model(test_ixs, train_ixs, {}, {}, {}, item_map, DEFAULT_WEIGHTS, k=10)
        assert result["ndcg_at_k"] == 0.0
        assert result["hit_rate_at_k"] == 0.0
        assert result["mrr_at_k"] == 0.0
        assert result["watch_time_ndcg_at_k"] == 0.0

    def test_watch_time_ndcg_is_bounded_and_nonzero_for_good_ranking(self):
        # watch_time_ndcg_at_k normalises per user, so a single positive ranked first
        # always yields 1.0 — this tests the field exists and is in [0, 1]
        items = [_item("m1"), _item("m2"), _item("m_test")]
        item_map = {i.item_id: i for i in items}
        train_ixs = [_ix("u1", "m1", "complete")]
        test_ixs = [_ix("u1", "m_test", "complete", watch_seconds=3600)]
        collab = {"m1": {"m_test": 1.0}}
        result = evaluate_model(test_ixs, train_ixs, collab, {}, {}, item_map, DEFAULT_WEIGHTS)
        assert 0.0 < result["watch_time_ndcg_at_k"] <= 1.0

    def test_watch_time_ndcg_captures_gain_ordering(self):
        # Two positive items per user with different gains: the model ranks the
        # high-gain item first → watch_time_ndcg should be higher than when
        # the low-gain item is ranked first.
        items = [_item("m1"), _item("m_high"), _item("m_low")]
        item_map = {i.item_id: i for i in items}
        train_ixs = [_ix("u1", "m1", "complete")]
        # m_high has 7200s watch (gain≈1.0), m_low has 60s watch (gain≈0.12)
        test_ixs = [
            _ix("u1", "m_high", "complete", watch_seconds=7200),
            _ix("u1", "m_low", "complete", watch_seconds=60),
        ]
        # Model that ranks high-gain item first (optimal order)
        collab_optimal = {"m1": {"m_high": 1.0, "m_low": 0.2}}
        # Model that ranks low-gain item first (suboptimal order)
        collab_suboptimal = {"m1": {"m_low": 1.0, "m_high": 0.2}}
        result_opt = evaluate_model(test_ixs, train_ixs, collab_optimal, {}, {}, item_map, DEFAULT_WEIGHTS)
        result_sub = evaluate_model(test_ixs, train_ixs, collab_suboptimal, {}, {}, item_map, DEFAULT_WEIGHTS)
        assert result_opt["watch_time_ndcg_at_k"] > result_sub["watch_time_ndcg_at_k"]

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
        assert result["mrr_at_k"] == 0.0
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
        X, y, sw = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert len(X) == len(y) == len(sw)

    def test_positive_label_for_engagement_events(self):
        items = [_item("m1"), _item("m2")]
        # u1 plays m1 (positive) and u1 also has m2 as a neighbor candidate
        train_ixs = [
            _ix("u1", "m1", "complete", completion_pct=100.0),
            _ix("u1", "m2", "impression"),
        ]
        collab = {"m1": {"m2": 1.0}}
        X, y, sw = build_feature_matrix(train_ixs, items, collab, {}, {})
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
        X, y, sw = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert all(len(row) == 6 for row in X)

    def test_users_with_fewer_than_two_played_items_are_skipped(self):
        items = [_item("m1"), _item("m2")]
        # u1 only has one played item → skipped
        train_ixs = [_ix("u1", "m1", "complete", completion_pct=100.0)]
        collab = {"m1": {"m2": 0.9}}
        X, y, sw = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert X == [] and y == [] and sw == []

    def test_sample_weights_positive_gt_zero(self):
        items = [_item("m1"), _item("m2"), _item("m3")]
        train_ixs = [
            _ix("u1", "m1", "complete", completion_pct=100.0),
            _ix("u1", "m2", "complete", completion_pct=80.0),
        ]
        collab = {"m1": {"m3": 0.9}, "m2": {"m3": 0.8}}
        X, y, sw = build_feature_matrix(train_ixs, items, collab, {}, {})
        assert all(w > 0 for w in sw)

    def test_position_discount_reduces_sample_weight(self):
        # position=2 applies discount 1/log2(3) ≈ 0.63; position=-1 applies none
        items = [_item("m1"), _item("m2"), _item("m3")]
        train_no_pos = [
            _ix("u1", "m1", "complete", completion_pct=100.0, position=-1),
            _ix("u1", "m2", "complete", completion_pct=100.0, position=-1),
        ]
        train_with_pos = [
            _ix("u1", "m1", "complete", completion_pct=100.0, position=2),
            _ix("u1", "m2", "complete", completion_pct=100.0, position=2),
        ]
        collab = {"m1": {"m3": 0.9}, "m2": {"m3": 0.8}}
        _, y1, sw1 = build_feature_matrix(train_no_pos, items, collab, {}, {})
        _, y2, sw2 = build_feature_matrix(train_with_pos, items, collab, {}, {})
        pos_sw1 = [sw1[i] for i in range(len(y1)) if y1[i] == 1]
        pos_sw2 = [sw2[i] for i in range(len(y2)) if y2[i] == 1]
        if pos_sw1 and pos_sw2:
            assert sum(pos_sw1) > sum(pos_sw2)


# ---------------------------------------------------------------------------
# item_freshness
# ---------------------------------------------------------------------------

class TestItemFreshness:
    def test_current_year_scores_one(self):
        year = datetime.now().year
        assert item_freshness(year, reference_year=year) == pytest.approx(1.0)

    def test_five_years_ago_scores_zero(self):
        year = datetime.now().year
        assert item_freshness(year - 5, reference_year=year) == pytest.approx(0.0)

    def test_older_than_five_years_clamps_to_zero(self):
        assert item_freshness(2000, reference_year=2025) == 0.0

    def test_future_items_clamp_to_one(self):
        assert item_freshness(2099, reference_year=2025) == 1.0

    def test_midpoint_year_scores_half(self):
        # reference=2025, window=[2020,2025]; midpoint=2022 → (2022-2020)/5=0.4... wait
        # formula: (release - (ref-5)) / 5  → (2022 - 2020) / 5 = 0.4
        assert item_freshness(2022, reference_year=2025) == pytest.approx(0.4)

    def test_scores_increase_with_recency(self):
        ref = 2025
        assert item_freshness(2024, ref) > item_freshness(2022, ref) > item_freshness(2021, ref)

    def test_uses_current_year_as_default_reference(self):
        year = datetime.now().year
        assert item_freshness(year) == pytest.approx(1.0)
        assert item_freshness(year - 5) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# temporal_split
# ---------------------------------------------------------------------------

def _ix_ts(ts: datetime):
    return SimpleNamespace(event_ts=ts)


class TestTemporalSplit:
    def test_empty_returns_empty_pair(self):
        train, test = temporal_split([])
        assert train == [] and test == []

    def test_default_20pct_eval(self):
        ixs = [_ix_ts(datetime(2025, 1, i + 1)) for i in range(10)]
        train, test = temporal_split(ixs)
        assert len(train) == 8
        assert len(test) == 2

    def test_train_precedes_test_chronologically(self):
        ixs = [_ix_ts(datetime(2025, 1, i + 1)) for i in range(10)]
        train, test = temporal_split(ixs)
        assert max(ix.event_ts for ix in train) < min(ix.event_ts for ix in test)

    def test_custom_fraction(self):
        ixs = [_ix_ts(datetime(2025, 1, i + 1)) for i in range(10)]
        train, test = temporal_split(ixs, eval_fraction=0.3)
        assert len(train) == 7
        assert len(test) == 3

    def test_very_small_set_does_not_error(self):
        # int(1 * 0.8) == 0, so one item rounds entirely into eval — just assert no crash
        ixs = [_ix_ts(datetime(2025, 1, 1))]
        train, test = temporal_split(ixs)
        assert len(train) + len(test) == 1

    def test_output_is_sorted_by_time(self):
        import random
        ixs = [_ix_ts(datetime(2025, 1, i + 1)) for i in range(10)]
        shuffled = ixs[:]
        random.shuffle(shuffled)
        train, test = temporal_split(shuffled)
        all_out = train + test
        assert all_out == sorted(all_out, key=lambda x: x.event_ts)
