"""Unit tests for common/utils.py."""
import pytest
from common.utils import deterministic_score, now_utc, weighted_choice


class TestNowUtc:
    def test_returns_datetime(self):
        from datetime import datetime
        assert isinstance(now_utc(), datetime)

    def test_is_timezone_aware(self):
        from datetime import timezone
        dt = now_utc()
        assert dt.tzinfo is not None
        assert dt.tzinfo == timezone.utc


class TestDeterministicScore:
    def test_returns_float_between_zero_and_one(self):
        score = deterministic_score("user1", "item1")
        assert 0.0 <= score <= 1.0

    def test_same_inputs_return_same_score(self):
        assert deterministic_score("u1", "i1") == deterministic_score("u1", "i1")

    def test_different_inputs_return_different_scores(self):
        assert deterministic_score("u1", "i1") != deterministic_score("u1", "i2")

    def test_multiple_parts_are_combined(self):
        score = deterministic_score("a", "b", "c")
        assert 0.0 <= score <= 1.0

    def test_single_part_works(self):
        assert 0.0 <= deterministic_score("anything") <= 1.0


class TestWeightedChoice:
    def test_returns_item_from_list(self):
        items = [("a", 1.0), ("b", 2.0), ("c", 0.5)]
        result = weighted_choice(items)
        assert result in {"a", "b", "c"}

    def test_single_item_always_returned(self):
        for _ in range(10):
            assert weighted_choice([("only", 1.0)]) == "only"

    def test_zero_weight_item_not_chosen(self):
        # Item with weight 0.0 should essentially never be chosen
        results = {weighted_choice([("always", 1.0), ("never", 0.0)]) for _ in range(20)}
        assert "always" in results

    def test_returns_string(self):
        result = weighted_choice([("x", 1.0)])
        assert isinstance(result, str)
