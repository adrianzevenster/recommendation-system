from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from services.trainer.app import (
    build_collaborative_neighbors,
    build_content_neighbors,
    build_trending,
    interaction_weight,
)


def _interaction(**kwargs):
    defaults = dict(
        event_id="e1",
        user_id="u1",
        item_id="m1",
        event_type="click",
        completion_pct=0.0,
        position=-1,
        watch_seconds=0,
        event_ts=datetime.now(timezone.utc),
        region="US",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _item(**kwargs):
    defaults = dict(
        item_id="m1",
        title="Test Movie",
        genres="sci-fi,thriller",
        actors="Actor One",
        director="Dir One",
        synopsis="A test film about space.",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestInteractionWeight:
    def test_complete_has_highest_base_weight(self):
        assert interaction_weight("complete", 0.0) > interaction_weight("play_start", 0.0)
        assert interaction_weight("play_start", 0.0) > interaction_weight("click", 0.0)
        assert interaction_weight("click", 0.0) > interaction_weight("impression", 0.0)

    def test_completion_pct_adds_bonus(self):
        assert interaction_weight("click", 100.0) > interaction_weight("click", 0.0)

    def test_completion_pct_bonus_is_one_at_100_pct(self):
        w0 = interaction_weight("impression", 0.0)
        w100 = interaction_weight("impression", 100.0)
        assert abs((w100 - w0) - 1.0) < 1e-9

    def test_unknown_event_type_returns_minimum(self):
        assert interaction_weight("unknown_xyz", 0.0) == 0.1

    def test_position_minus_one_applies_no_discount(self):
        assert interaction_weight("click", 0.0, position=-1) == interaction_weight("click", 0.0)

    def test_position_two_applies_discount(self):
        # position=2 gives discount 1/log2(3) ≈ 0.63; position=-1 gives no discount
        w_no_pos = interaction_weight("click", 0.0)
        w_pos2 = interaction_weight("click", 0.0, position=2)
        assert w_pos2 < w_no_pos

    def test_higher_position_means_lower_weight(self):
        w1 = interaction_weight("complete", 0.0, position=1)
        w5 = interaction_weight("complete", 0.0, position=5)
        assert w1 > w5


class TestBuildCollaborativeNeighbors:
    def test_co_viewed_items_become_neighbors(self):
        interactions = [
            _interaction(user_id="u1", item_id="m1", event_type="play_start"),
            _interaction(user_id="u1", item_id="m2", event_type="play_start"),
        ]
        results = build_collaborative_neighbors(interactions)
        assert len(results) == 2
        pairs = {(r[0], r[1]) for r in results}
        assert ("m1", "m2") in pairs
        assert ("m2", "m1") in pairs

    def test_impression_events_excluded(self):
        interactions = [
            _interaction(user_id="u1", item_id="m1", event_type="impression"),
            _interaction(user_id="u1", item_id="m2", event_type="impression"),
        ]
        assert build_collaborative_neighbors(interactions) == []

    def test_top_k_limits_neighbors_per_source(self):
        # u1 watches 6 items; m1 can have at most top_k=3 neighbors
        interactions = [
            _interaction(user_id="u1", item_id=f"m{i}", event_type="complete")
            for i in range(1, 7)
        ]
        results = build_collaborative_neighbors(interactions, top_k=3)
        m1_neighbors = [r for r in results if r[0] == "m1"]
        assert len(m1_neighbors) <= 3

    def test_scores_are_positive_and_at_most_one(self):
        interactions = [
            _interaction(user_id="u1", item_id="m1", event_type="play_start"),
            _interaction(user_id="u1", item_id="m2", event_type="play_start"),
        ]
        results = build_collaborative_neighbors(interactions)
        for _, _, score, _ in results:
            assert 0.0 < score <= 1.0

    def test_algorithm_label_is_collaborative(self):
        interactions = [
            _interaction(user_id="u1", item_id="m1", event_type="complete"),
            _interaction(user_id="u1", item_id="m2", event_type="complete"),
        ]
        results = build_collaborative_neighbors(interactions)
        assert all(r[3] == "collaborative" for r in results)

    def test_multiple_users_increase_co_occurrence_score(self):
        # m1 and m2 co-viewed by 3 users; m1 and m3 by only 1 — m2 should score higher
        interactions = [
            _interaction(user_id="u1", item_id="m1", event_type="complete"),
            _interaction(user_id="u1", item_id="m2", event_type="complete"),
            _interaction(user_id="u2", item_id="m1", event_type="complete"),
            _interaction(user_id="u2", item_id="m2", event_type="complete"),
            _interaction(user_id="u3", item_id="m1", event_type="complete"),
            _interaction(user_id="u3", item_id="m2", event_type="complete"),
            _interaction(user_id="u4", item_id="m1", event_type="complete"),
            _interaction(user_id="u4", item_id="m3", event_type="complete"),
        ]
        results = build_collaborative_neighbors(interactions, top_k=8)
        m1_scores = {r[1]: r[2] for r in results if r[0] == "m1"}
        assert m1_scores["m2"] > m1_scores["m3"]


class TestBuildContentNeighbors:
    def test_empty_items_returns_empty(self):
        neighbors, vec = build_content_neighbors([])
        assert neighbors == [] and vec is None

    def test_returns_neighbors_and_vectorizer(self):
        items = [
            _item(item_id="m1", title="Alpha drama", genres="drama", actors="A", director="D", synopsis="Story"),
            _item(item_id="m2", title="Beta drama", genres="drama", actors="B", director="E", synopsis="Tale"),
        ]
        neighbors, vec = build_content_neighbors(items)
        assert vec is not None

    def test_self_never_appears_as_own_neighbor(self):
        items = [
            _item(item_id="m1", title="Alpha drama", genres="drama", actors="A", director="D", synopsis="Story"),
            _item(item_id="m2", title="Beta drama", genres="drama", actors="B", director="E", synopsis="Tale"),
        ]
        results, _ = build_content_neighbors(items)
        for source, neighbor, _, _ in results:
            assert source != neighbor

    def test_genre_similar_items_are_neighbors(self):
        items = [
            _item(item_id="m1", title="Sci-fi Thriller", genres="sci-fi,thriller", synopsis="Space crime"),
            _item(item_id="m2", title="Sci-fi Adventure", genres="sci-fi,adventure", synopsis="Space travel"),
            _item(item_id="m3", title="Romance Drama", genres="romance,drama", synopsis="Love story"),
        ]
        results, _ = build_content_neighbors(items, top_k=2)
        m1_neighbors = {r[1] for r in results if r[0] == "m1"}
        assert "m2" in m1_neighbors

    def test_algorithm_label_is_content(self):
        items = [
            _item(item_id="m1", title="One", genres="drama"),
            _item(item_id="m2", title="Two", genres="drama"),
        ]
        results, _ = build_content_neighbors(items)
        assert all(r[3] == "content" for r in results)

    def test_top_k_limits_neighbors_per_item(self):
        items = [_item(item_id=f"m{i}", title=f"Movie {i}", genres="action") for i in range(6)]
        results, _ = build_content_neighbors(items, top_k=3)
        m0_neighbors = [r for r in results if r[0] == "m0"]
        assert len(m0_neighbors) <= 3


class TestBuildTrending:
    def test_recent_interactions_appear_in_results(self):
        now = datetime.now(timezone.utc)
        interactions = [
            _interaction(item_id="m1", region="US", event_type="complete",
                         completion_pct=100.0, event_ts=now - timedelta(hours=1)),
        ]
        results = build_trending(interactions)
        assert any(r[0] == "US" and r[1] == "m1" for r in results)

    def test_interactions_older_than_24h_excluded(self):
        old = datetime.now(timezone.utc) - timedelta(hours=25)
        interactions = [
            _interaction(item_id="m1", region="US", event_type="complete",
                         completion_pct=100.0, event_ts=old),
        ]
        assert build_trending(interactions) == []

    def test_results_capped_at_ten_per_region(self):
        now = datetime.now(timezone.utc)
        interactions = [
            _interaction(item_id=f"m{i}", region="ZA", event_type="click",
                         completion_pct=0.0, event_ts=now)
            for i in range(20)
        ]
        results = build_trending(interactions)
        za_results = [r for r in results if r[0] == "ZA"]
        assert len(za_results) <= 10

    def test_regions_are_bucketed_separately(self):
        now = datetime.now(timezone.utc)
        interactions = [
            _interaction(item_id="m1", region="US", event_type="click",
                         completion_pct=0.0, event_ts=now),
            _interaction(item_id="m2", region="UK", event_type="click",
                         completion_pct=0.0, event_ts=now),
        ]
        results = build_trending(interactions)
        regions = {r[0] for r in results}
        assert "US" in regions
        assert "UK" in regions

    def test_scores_are_positive(self):
        now = datetime.now(timezone.utc)
        interactions = [
            _interaction(item_id="m1", region="ZA", event_type="complete",
                         completion_pct=100.0, event_ts=now),
        ]
        results = build_trending(interactions)
        assert all(r[2] > 0 for r in results)

    def test_complete_events_score_higher_than_impressions(self):
        now = datetime.now(timezone.utc)
        interactions = [
            _interaction(item_id="m1", region="US", event_type="complete",
                         completion_pct=100.0, event_ts=now),
            _interaction(item_id="m2", region="US", event_type="impression",
                         completion_pct=0.0, event_ts=now),
        ]
        results = build_trending(interactions)
        scores = {r[1]: r[2] for r in results if r[0] == "US"}
        assert scores["m1"] > scores["m2"]


# ---------------------------------------------------------------------------
# _score_user_candidates_offline
# ---------------------------------------------------------------------------

from services.trainer.app import _score_user_candidates_offline

_WEIGHTS = [0.35, 0.25, 0.20, 0.10, 0.05, 0.05]


def _candidate_item(item_id, genres="drama", release_year=2024, regions="GLOBAL",
                    active=True, maturity="PG-13"):
    return SimpleNamespace(
        item_id=item_id, genres=genres, release_year=release_year,
        available_regions=regions, is_active=active, maturity_rating=maturity,
    )


class TestScoreUserCandidatesOffline:
    def _base_maps(self):
        item_map = {
            "m1": _candidate_item("m1", genres="sci-fi"),
            "m2": _candidate_item("m2", genres="drama"),
            "m3": _candidate_item("m3", genres="comedy"),
        }
        collab_map = {"seed": {"m1": 1.0}}
        content_map = {"seed": {"m2": 1.0}}
        trending_map = {"US": {"m3": 1.0}}
        return item_map, collab_map, content_map, trending_map

    def test_returns_scored_dicts_with_required_keys(self):
        item_map, collab, content, trending = self._base_maps()
        result = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, collab, content, trending, _WEIGHTS
        )
        assert all({"item_id", "score", "reason", "components"} <= set(r.keys()) for r in result)

    def test_inactive_items_excluded(self):
        item_map = {
            "m1": _candidate_item("m1", active=False),
            "m2": _candidate_item("m2"),
        }
        result = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, {"seed": {"m1": 1.0, "m2": 0.5}},
            {}, {}, _WEIGHTS
        )
        assert not any(r["item_id"] == "m1" for r in result)

    def test_wrong_region_excluded(self):
        item_map = {
            "m1": _candidate_item("m1", regions="UK"),
            "m2": _candidate_item("m2", regions="GLOBAL"),
        }
        result = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, {"seed": {"m1": 1.0, "m2": 0.8}},
            {}, {}, _WEIGHTS
        )
        assert not any(r["item_id"] == "m1" for r in result)
        assert any(r["item_id"] == "m2" for r in result)

    def test_mature_content_excluded_for_pg_user(self):
        item_map = {
            "m1": _candidate_item("m1", maturity="R"),
            "m2": _candidate_item("m2", maturity="PG"),
        }
        result = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, {"seed": {"m1": 1.0, "m2": 0.9}},
            {}, {}, _WEIGHTS
        )
        assert not any(r["item_id"] == "m1" for r in result)
        assert any(r["item_id"] == "m2" for r in result)

    def test_results_sorted_descending_by_score(self):
        item_map, collab, content, trending = self._base_maps()
        result = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, collab, content, trending, _WEIGHTS
        )
        scores = [r["score"] for r in result]
        assert scores == sorted(scores, reverse=True)

    def test_empty_history_uses_trending_signal(self):
        item_map = {"m1": _candidate_item("m1")}
        result = _score_user_candidates_offline(
            "US", "PG-13", [], item_map, {}, {}, {"US": {"m1": 1.0}}, _WEIGHTS
        )
        assert any(r["item_id"] == "m1" for r in result)

    def test_genre_affinity_increases_score_for_matching_genre(self):
        item_map = {
            "m1": _candidate_item("m1", genres="sci-fi"),
            "m2": _candidate_item("m2", genres="drama"),
        }
        collab = {"seed": {"m1": 1.0, "m2": 1.0}}
        weights = [0.35, 0.25, 0.20, 0.10, 0.05, 0.05]

        base = _score_user_candidates_offline("US", "PG-13", ["seed"], item_map, collab, {}, {}, weights)
        with_affinity = _score_user_candidates_offline(
            "US", "PG-13", ["seed"], item_map, collab, {}, {}, weights,
            genre_affinity={"sci-fi": 10.0},
        )
        base_m1 = next(r["score"] for r in base if r["item_id"] == "m1")
        boosted_m1 = next(r["score"] for r in with_affinity if r["item_id"] == "m1")
        assert boosted_m1 > base_m1

    def test_genre_affinity_stored_in_components(self):
        item_map = {"m1": _candidate_item("m1", genres="sci-fi")}
        result = _score_user_candidates_offline(
            "US", "PG-13", [], item_map, {}, {}, {"US": {"m1": 1.0}}, _WEIGHTS,
            genre_affinity={"sci-fi": 10.0},
        )
        assert result[0]["components"]["genre_bonus"] > 0.0
