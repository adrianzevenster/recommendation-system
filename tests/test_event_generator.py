from types import SimpleNamespace

from services.event_generator.app import choose_item_for_user, make_event


def _user(user_id="u001", region="US"):
    return SimpleNamespace(user_id=user_id, region=region)


def _item(item_id="m001", genres="sci-fi,thriller", item_type="movie", release_year=2024):
    return SimpleNamespace(
        item_id=item_id,
        genres=genres,
        item_type=item_type,
        release_year=release_year,
    )


class TestMakeEvent:
    def test_required_fields_are_present(self):
        event = make_event(_user(), _item())
        for field in ("event_id", "user_id", "item_id", "event_type",
                      "watch_seconds", "completion_pct", "region", "device_type", "event_ts"):
            assert field in event

    def test_user_and_item_ids_are_set_correctly(self):
        event = make_event(_user(user_id="u003", region="ZA"), _item(item_id="m007"))
        assert event["user_id"] == "u003"
        assert event["item_id"] == "m007"
        assert event["region"] == "ZA"

    def test_event_type_is_valid(self):
        valid = {"impression", "click", "play_start", "watch_progress", "complete", "watchlist_add"}
        for _ in range(50):
            event = make_event(_user(), _item())
            assert event["event_type"] in valid

    def test_device_type_is_valid(self):
        valid = {"web", "mobile", "tv"}
        for _ in range(30):
            event = make_event(_user(), _item())
            assert event["device_type"] in valid

    def test_impression_has_zero_watch_seconds(self):
        for _ in range(300):
            event = make_event(_user(), _item())
            if event["event_type"] == "impression":
                assert event["watch_seconds"] == 0
                assert event["completion_pct"] == 0.0
                return
        # If we never hit an impression in 300 tries (probability ~(0.6)^300 ≈ 0), something is wrong
        assert False, "never generated an impression event"

    def test_complete_event_uses_full_movie_runtime(self):
        for _ in range(500):
            event = make_event(_user(), _item(item_type="movie"))
            if event["event_type"] == "complete":
                assert event["watch_seconds"] == 110 * 60
                assert event["completion_pct"] == 100.0
                return

    def test_complete_event_uses_episode_runtime_for_series(self):
        for _ in range(500):
            event = make_event(_user(), _item(item_type="series"))
            if event["event_type"] == "complete":
                assert event["watch_seconds"] == 45 * 60
                assert event["completion_pct"] == 100.0
                return

    def test_completion_pct_bounded_between_zero_and_one_hundred(self):
        for _ in range(100):
            event = make_event(_user(), _item())
            assert 0.0 <= event["completion_pct"] <= 100.0


class TestChooseItemForUser:
    def test_returns_an_item_from_the_list(self):
        items = [_item(item_id=f"m{i}") for i in range(5)]
        result = choose_item_for_user("u001", items)
        assert result in items

    def test_genre_weighted_user_prefers_matching_genres(self):
        # u001 strongly prefers thriller/crime (weight 3 each)
        thriller_item = _item(item_id="thriller", genres="thriller,crime")
        comedy_item = _item(item_id="comedy", genres="comedy,romance")
        items = [thriller_item, comedy_item]

        choices = [choose_item_for_user("u001", items) for _ in range(300)]
        thriller_count = sum(1 for c in choices if c.item_id == "thriller")
        # Thriller has weight ~7 vs comedy ~1; expect ~87.5% thriller selections
        assert thriller_count > 200

    def test_unknown_user_uses_uniform_weights(self):
        items = [_item(item_id=f"m{i}", genres="drama") for i in range(4)]
        # Should not raise for an unrecognized user_id
        result = choose_item_for_user("u_unknown_99", items)
        assert result in items

    def test_recent_releases_get_score_boost(self):
        # Newer release_year should give a higher base score
        old_item = _item(item_id="old", genres="drama", release_year=2020)
        new_item = _item(item_id="new", genres="drama", release_year=2026)
        items = [old_item, new_item]

        choices = [choose_item_for_user("u_unknown", items) for _ in range(300)]
        new_count = sum(1 for c in choices if c.item_id == "new")
        # new_item gets 0.3*(2026-2022)=1.2 extra vs 0.3*(2020-2022)=0 (capped at 0)
        assert new_count > 150
