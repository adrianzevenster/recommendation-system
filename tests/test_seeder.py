"""Unit tests for services/seeder/app.py.

Tests cover pure parsing helpers and DB-agnostic utilities.
Functions that require a live DB or Redis are tested with mocks.
"""
import io
import zipfile
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_ml1m_zip(
    movies: list[str] | None = None,
    users: list[str] | None = None,
    ratings: list[str] | None = None,
) -> bytes:
    """Build a minimal in-memory MovieLens 1M ZIP for parser tests."""
    movies = movies or ["1::Toy Story (1995)::Animation|Children's|Comedy"]
    users = users or ["1::F::1::10::48067"]
    ratings = ratings or ["1::1::5::978300760"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ml-1m/movies.dat", "\n".join(movies) + "\n")
        zf.writestr("ml-1m/users.dat", "\n".join(users) + "\n")
        zf.writestr("ml-1m/ratings.dat", "\n".join(ratings) + "\n")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _parse_title_year
# ---------------------------------------------------------------------------

class TestParseTitleYear:
    def _call(self, raw):
        from services.seeder.app import _parse_title_year
        return _parse_title_year(raw)

    def test_extracts_title_and_year_from_standard_format(self):
        title, year = self._call("Toy Story (1995)")
        assert title == "Toy Story"
        assert year == 1995

    def test_strips_whitespace_around_title(self):
        title, year = self._call("  The Matrix  (1999)  ")
        assert title == "The Matrix"
        assert year == 1999

    def test_returns_raw_and_2000_when_no_year(self):
        title, year = self._call("No Year Here")
        assert title == "No Year Here"
        assert year == 2000

    def test_handles_title_with_commas(self):
        title, year = self._call("Lock, Stock and Two Smoking Barrels (1998)")
        assert year == 1998
        assert "Lock" in title

    def test_handles_title_with_parentheses_in_name(self):
        # Year must be at the end — parentheses in the middle should be part of title
        title, year = self._call("Se7en (1995)")
        assert year == 1995


# ---------------------------------------------------------------------------
# _genres_csv
# ---------------------------------------------------------------------------

class TestGenresCsv:
    def _call(self, raw):
        from services.seeder.app import _genres_csv
        return _genres_csv(raw)

    def test_replaces_pipe_with_comma(self):
        assert self._call("Action|Comedy") == "action,comedy"

    def test_lowercases_genres(self):
        assert "action" in self._call("Action|Drama")

    def test_single_genre_has_no_comma(self):
        result = self._call("Drama")
        assert result == "drama"
        assert "," not in result

    def test_strips_whitespace_around_genres(self):
        result = self._call(" Action | Drama ")
        assert result == "action,drama"


# ---------------------------------------------------------------------------
# _maturity
# ---------------------------------------------------------------------------

class TestMaturity:
    def _call(self, genres_csv):
        from services.seeder.app import _maturity
        return _maturity(genres_csv)

    def test_animation_is_rated_g(self):
        assert self._call("animation,comedy") == "G"

    def test_childrens_is_rated_g(self):
        assert self._call("children's,adventure") == "G"

    def test_horror_is_rated_r(self):
        assert self._call("horror,thriller") == "R"

    def test_comedy_is_rated_pg(self):
        assert self._call("comedy,drama") == "PG"

    def test_romance_is_rated_pg(self):
        assert self._call("romance") == "PG"

    def test_neutral_genre_is_pg13(self):
        assert self._call("action,thriller") == "PG-13"

    def test_horror_takes_priority_over_comedy(self):
        assert self._call("comedy,horror") == "R"


# ---------------------------------------------------------------------------
# _rating_to_event
# ---------------------------------------------------------------------------

class TestRatingToEvent:
    def _call(self, rating):
        from services.seeder.app import _rating_to_event
        return _rating_to_event(rating)

    def test_rating_5_is_complete(self):
        event_type, completion_pct, _ = self._call(5)
        assert event_type == "complete"
        assert completion_pct == 100.0

    def test_rating_4_is_watch_progress(self):
        event_type, completion_pct, _ = self._call(4)
        assert event_type == "watch_progress"
        assert completion_pct == 75.0

    def test_rating_3_is_click(self):
        event_type, _, _ = self._call(3)
        assert event_type == "click"

    def test_rating_1_is_impression(self):
        event_type, completion_pct, watch_seconds = self._call(1)
        assert event_type == "impression"
        assert completion_pct == 0.0
        assert watch_seconds == 0

    def test_rating_2_is_impression(self):
        event_type, _, _ = self._call(2)
        assert event_type == "impression"

    def test_watch_seconds_are_nonzero_for_high_ratings(self):
        _, _, ws = self._call(5)
        assert ws > 0
        _, _, ws4 = self._call(4)
        assert ws4 > 0

    def test_complete_has_more_watch_seconds_than_progress(self):
        _, _, ws5 = self._call(5)
        _, _, ws4 = self._call(4)
        assert ws5 >= ws4


# ---------------------------------------------------------------------------
# _parse_ml1m
# ---------------------------------------------------------------------------

class TestParseML1M:
    def test_returns_three_lists(self):
        from services.seeder.app import _parse_ml1m
        items, users, interactions = _parse_ml1m(_make_fake_ml1m_zip())
        assert isinstance(items, list)
        assert isinstance(users, list)
        assert isinstance(interactions, list)

    def test_parses_movie_record(self):
        from services.seeder.app import _parse_ml1m
        items, _, _ = _parse_ml1m(_make_fake_ml1m_zip())
        assert len(items) == 1
        assert items[0]["item_id"] == "m1"
        assert items[0]["title"] == "Toy Story"
        assert items[0]["release_year"] == 1995

    def test_item_genres_are_lowercased_csv(self):
        from services.seeder.app import _parse_ml1m
        items, _, _ = _parse_ml1m(_make_fake_ml1m_zip())
        assert "animation" in items[0]["genres"]

    def test_parses_user_record(self):
        from services.seeder.app import _parse_ml1m
        _, users, _ = _parse_ml1m(_make_fake_ml1m_zip())
        assert len(users) == 1
        assert users[0]["user_id"] == "u1"
        assert users[0]["region"] in {"US", "UK", "ZA"}

    def test_parses_rating_record(self):
        from services.seeder.app import _parse_ml1m
        _, _, interactions = _parse_ml1m(_make_fake_ml1m_zip())
        assert len(interactions) == 1
        ix = interactions[0]
        assert ix["user_id"] == "u1"
        assert ix["item_id"] == "m1"
        assert ix["event_type"] == "complete"  # rating=5 → complete

    def test_multiple_ratings_produce_multiple_interactions(self):
        from services.seeder.app import _parse_ml1m
        ratings = [
            "1::1::5::978300760",
            "1::1::4::978300761",
            "1::1::3::978300762",
            "1::1::1::978300763",
        ]
        _, _, interactions = _parse_ml1m(_make_fake_ml1m_zip(ratings=ratings))
        assert len(interactions) == 4

    def test_items_include_required_fields(self):
        from services.seeder.app import _parse_ml1m
        items, _, _ = _parse_ml1m(_make_fake_ml1m_zip())
        required = {"item_id", "title", "item_type", "genres", "release_year", "maturity_rating"}
        assert required.issubset(set(items[0].keys()))

    def test_interactions_include_watch_seconds(self):
        from services.seeder.app import _parse_ml1m
        _, _, interactions = _parse_ml1m(_make_fake_ml1m_zip())
        assert "watch_seconds" in interactions[0]

    def test_empty_or_malformed_lines_are_skipped(self):
        from services.seeder.app import _parse_ml1m
        movies = [
            "",                      # empty line — should be skipped
            "bad_line_no_separator", # no :: separator — skipped
            "2::Alien (1979)::Sci-Fi|Horror",  # valid
        ]
        items, _, _ = _parse_ml1m(_make_fake_ml1m_zip(movies=movies))
        assert len(items) == 1
        assert items[0]["item_id"] == "m2"

    def test_multiple_movies_parsed(self):
        from services.seeder.app import _parse_ml1m
        movies = [
            "1::Toy Story (1995)::Animation|Children's|Comedy",
            "2::GoodFellas (1990)::Crime|Drama",
            "3::Pulp Fiction (1994)::Crime|Drama|Thriller",
        ]
        items, _, _ = _parse_ml1m(_make_fake_ml1m_zip(movies=movies))
        assert len(items) == 3


# ---------------------------------------------------------------------------
# _bulk_insert
# ---------------------------------------------------------------------------

class TestBulkInsert:
    def test_executes_for_each_batch(self):
        from services.seeder.app import _bulk_insert, _BATCH_SIZE
        from common.models import User
        session = MagicMock()
        records = [{"user_id": f"u{i}"} for i in range(_BATCH_SIZE + 5)]
        _bulk_insert(session, User, records, "users")
        assert session.execute.call_count == 2
        assert session.commit.call_count == 2

    def test_single_batch_for_small_set(self):
        from services.seeder.app import _bulk_insert
        from common.models import Item
        session = MagicMock()
        records = [{"item_id": "m1"}]
        _bulk_insert(session, Item, records, "items")
        session.execute.assert_called_once()
        session.commit.assert_called_once()

    def test_empty_records_does_not_execute(self):
        from services.seeder.app import _bulk_insert
        from common.models import User
        session = MagicMock()
        _bulk_insert(session, User, [], "users")
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# _wipe_all
# ---------------------------------------------------------------------------

class TestWipeAll:
    def test_executes_delete_for_each_model(self):
        from services.seeder.app import _wipe_all
        session = MagicMock()
        _wipe_all(session)
        # 9 models are wiped — check at least one execute per model
        assert session.execute.call_count >= 9
        session.commit.assert_called_once()
