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
        assert build_content_neighbors([]) == []

    def test_self_never_appears_as_own_neighbor(self):
        items = [
            _item(item_id="m1", title="Alpha drama", genres="drama", actors="A", director="D", synopsis="Story"),
            _item(item_id="m2", title="Beta drama", genres="drama", actors="B", director="E", synopsis="Tale"),
        ]
        results = build_content_neighbors(items)
        for source, neighbor, _, _ in results:
            assert source != neighbor

    def test_genre_similar_items_are_neighbors(self):
        items = [
            _item(item_id="m1", title="Sci-fi Thriller", genres="sci-fi,thriller", synopsis="Space crime"),
            _item(item_id="m2", title="Sci-fi Adventure", genres="sci-fi,adventure", synopsis="Space travel"),
            _item(item_id="m3", title="Romance Drama", genres="romance,drama", synopsis="Love story"),
        ]
        results = build_content_neighbors(items, top_k=2)
        m1_neighbors = {r[1] for r in results if r[0] == "m1"}
        assert "m2" in m1_neighbors

    def test_algorithm_label_is_content(self):
        items = [
            _item(item_id="m1", title="One", genres="drama"),
            _item(item_id="m2", title="Two", genres="drama"),
        ]
        results = build_content_neighbors(items)
        assert all(r[3] == "content" for r in results)

    def test_top_k_limits_neighbors_per_item(self):
        items = [_item(item_id=f"m{i}", title=f"Movie {i}", genres="action") for i in range(6)]
        results = build_content_neighbors(items, top_k=3)
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
