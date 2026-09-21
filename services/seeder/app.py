"""
Seeder — bootstraps Postgres with the MovieLens 1M dataset.

On first run (or when fewer than 100 items are present) it downloads
ml-1m.zip, parses movies / users / ratings, and bulk-inserts everything.
Subsequent runs are a no-op.  Falls back to a small hardcoded catalog
when the download fails (e.g. CI without network access).

After seeding Postgres, the seeder also backfills Redis so the
recommendation API sees personalized signals immediately (rather than
treating every user as cold-start because Redis is empty).
"""
import io
import logging
import re
import time
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import delete, insert, select, text

from common.db import SessionLocal, engine
from common.logging_utils import configure_logging
from common.models import (
    Base,
    Experiment,
    Interaction,
    Item,
    ItemNeighbor,
    ModelEvaluation,
    ModelVersion,
    RankingWeights,
    TrendingItem,
    User,
)
from common.redis_client import get_redis

configure_logging("seeder")
logger = logging.getLogger(__name__)

_EVENT_WEIGHT = {
    "impression": 0.1,
    "click": 0.3,
    "play_start": 0.8,
    "watch_progress": 1.2,
    "complete": 2.0,
    "watchlist_add": 0.5,
}
_REDIS_RECENT_LIMIT = 20
_GENRE_TTL = 86400 * 30
_POP_TTL = 86400 * 7

ML1M_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
_BATCH_SIZE = 10_000
# Threshold: if we have fewer items than this we treat the DB as "not yet seeded"
_REAL_CATALOG_MIN = 100

_YEAR_RE = re.compile(r"^(.+?)\s*\((\d{4})\)\s*$")
_REGIONS = ["US", "UK", "ZA"]


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_title_year(raw: str) -> tuple[str, int]:
    m = _YEAR_RE.match(raw.strip())
    if m:
        return m.group(1).strip(), int(m.group(2))
    return raw.strip(), 2000


def _genres_csv(raw: str) -> str:
    return ",".join(g.strip().lower() for g in raw.split("|"))


def _maturity(genres_csv: str) -> str:
    genres = {g.strip() for g in genres_csv.split(",")}
    if genres & {"animation", "children's", "children"}:
        return "G"
    if genres & {"horror"}:
        return "R"
    if "romance" in genres or "comedy" in genres:
        return "PG"
    return "PG-13"


def _rating_to_event(rating: int) -> tuple[str, float, int]:
    """Map 1-5 star rating to (event_type, completion_pct, watch_seconds)."""
    if rating >= 5:
        return "complete", 100.0, 5400
    elif rating == 4:
        return "watch_progress", 75.0, 4050
    elif rating == 3:
        return "click", 20.0, 600
    else:
        return "impression", 0.0, 0


# ---------------------------------------------------------------------------
# Download + parse MovieLens 1M
# ---------------------------------------------------------------------------

def _download_ml1m() -> bytes:
    logger.info("Downloading MovieLens 1M", extra={"url": ML1M_URL})
    req = urllib.request.Request(ML1M_URL, headers={"User-Agent": "recsys-seeder/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    logger.info("Download complete", extra={"mb": round(len(data) / 1_048_576, 1)})
    return data


def _parse_ml1m(zip_bytes: bytes) -> tuple[list[dict], list[dict], list[dict]]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    items: list[dict] = []
    users: list[dict] = []
    interactions: list[dict] = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:

        # ---- movies.dat ----
        with zf.open("ml-1m/movies.dat") as fh:
            for raw in io.TextIOWrapper(fh, encoding="latin-1"):
                raw = raw.strip()
                if not raw:
                    continue
                parts = raw.split("::")
                if len(parts) < 3:
                    continue
                movie_id, raw_title, raw_genres = parts[0], parts[1], parts[2]
                title, year = _parse_title_year(raw_title)
                genres = _genres_csv(raw_genres)
                items.append({
                    "item_id":          f"m{movie_id}",
                    "title":            title,
                    "item_type":        "movie",
                    "genres":           genres,
                    "actors":           "",
                    "director":         "",
                    "synopsis":         "",
                    "language":         "en",
                    "release_year":     year,
                    "maturity_rating":  _maturity(genres),
                    "available_regions": "GLOBAL",
                    "is_active":        True,
                    "created_at":       now,
                })

        # ---- users.dat ----
        with zf.open("ml-1m/users.dat") as fh:
            for raw in io.TextIOWrapper(fh, encoding="latin-1"):
                raw = raw.strip()
                if not raw:
                    continue
                parts = raw.split("::")
                if len(parts) < 2:
                    continue
                user_id = parts[0]
                users.append({
                    "user_id":            f"u{user_id}",
                    "region":             _REGIONS[int(user_id) % len(_REGIONS)],
                    "preferred_language": "en",
                    "maturity_rating":    "PG-13",
                    "created_at":         now,
                })

        # ---- ratings.dat ----
        with zf.open("ml-1m/ratings.dat") as fh:
            for raw in io.TextIOWrapper(fh, encoding="latin-1"):
                raw = raw.strip()
                if not raw:
                    continue
                parts = raw.split("::")
                if len(parts) < 4:
                    continue
                user_id, movie_id, rating, ts = (
                    parts[0], parts[1], int(parts[2]), int(parts[3])
                )
                event_type, completion_pct, watch_seconds = _rating_to_event(rating)
                event_ts = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
                interactions.append({
                    "event_id":      f"ml_{user_id}_{movie_id}_{ts}",
                    "user_id":       f"u{user_id}",
                    "item_id":       f"m{movie_id}",
                    "event_type":    event_type,
                    "watch_seconds": watch_seconds,
                    "completion_pct": completion_pct,
                    "region":        _REGIONS[int(user_id) % len(_REGIONS)],
                    "device_type":   "web",
                    "event_ts":      event_ts,
                    "ingestion_ts":  now,
                })

    logger.info(
        "Parsed MovieLens 1M",
        extra={
            "items":        len(items),
            "users":        len(users),
            "interactions": len(interactions),
        },
    )
    return items, users, interactions


# ---------------------------------------------------------------------------
# Bulk insert
# ---------------------------------------------------------------------------

def _bulk_insert(session, model, records: list[dict], label: str) -> None:
    total = len(records)
    for start in range(0, total, _BATCH_SIZE):
        batch = records[start : start + _BATCH_SIZE]
        session.execute(insert(model), batch)
        session.commit()
        done = min(start + _BATCH_SIZE, total)
        if done % 50_000 == 0 or done == total:
            logger.info(f"Inserted {label}", extra={"done": done, "total": total})


# ---------------------------------------------------------------------------
# Fallback hardcoded catalog (used when download fails)
# ---------------------------------------------------------------------------

_FALLBACK_CATALOG = [
    {"item_id": "m001", "title": "Shadow Protocol",   "item_type": "movie",   "genres": "thriller,crime",      "actors": "Ava Cross, Liam Hart",   "director": "Noah Kent",      "synopsis": "An analyst uncovers a conspiracy spanning intelligence agencies.", "language": "en", "release_year": 2024, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m002", "title": "Neon District",     "item_type": "series",  "genres": "sci-fi,action",       "actors": "Mila Ray, Omar Stone",    "director": "Zane Yu",        "synopsis": "Detectives pursue synthetic criminals in a vertical megacity.",    "language": "en", "release_year": 2025, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m003", "title": "Kitchen Kings",     "item_type": "series",  "genres": "comedy,drama",        "actors": "Nia Banks, Theo Reed",    "director": "Sara Cole",      "synopsis": "A struggling restaurant becomes the center of a family power struggle.", "language": "en", "release_year": 2023, "maturity_rating": "PG",   "available_regions": "GLOBAL"},
    {"item_id": "m004", "title": "Savannah Unit",     "item_type": "movie",   "genres": "action,adventure",    "actors": "Daniel Khoza, Jada Mills","director": "Priya Ndlovu",   "synopsis": "Elite responders cross borders to stop an ecological disaster.",   "language": "en", "release_year": 2022, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m005", "title": "Velvet Court",      "item_type": "series",  "genres": "romance,drama",       "actors": "Ana Flores, Marcus Webb", "director": "Helen Moore",    "synopsis": "Rival lawyers fall in love while battling a corporate dynasty.",   "language": "en", "release_year": 2024, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m006", "title": "Orbit Nine",        "item_type": "movie",   "genres": "sci-fi,thriller",     "actors": "Ravi Cole, Elena North",  "director": "Mina Park",      "synopsis": "A damaged station AI may be the crew's only hope of survival.",   "language": "en", "release_year": 2026, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m007", "title": "Harbor Files",      "item_type": "series",  "genres": "crime,mystery",       "actors": "Tessa Gray, Joel Prince", "director": "Nina Vargas",    "synopsis": "Cold cases reopen when a port city digitizes decades of records.", "language": "en", "release_year": 2021, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m008", "title": "Laugh Track Hotel", "item_type": "series",  "genres": "comedy",              "actors": "Ivy Price, Ben Moss",     "director": "Luca Dean",      "synopsis": "Hotel staff navigate chaos when their workplace becomes a reality show.", "language": "en", "release_year": 2020, "maturity_rating": "PG",   "available_regions": "GLOBAL"},
    {"item_id": "m009", "title": "The Last Pitch",    "item_type": "movie",   "genres": "sports,drama",        "actors": "Chris Dale, Faith Morgan","director": "Eli Turner",     "synopsis": "An aging coach assembles one final team from rejected talent.",    "language": "en", "release_year": 2025, "maturity_rating": "PG",   "available_regions": "GLOBAL"},
    {"item_id": "m010", "title": "Cipher Garden",     "item_type": "movie",   "genres": "mystery,thriller",    "actors": "Ari Vale, Sora Chen",     "director": "Imani Ford",     "synopsis": "A botanist finds hidden messages encoded in genetically edited flowers.", "language": "en", "release_year": 2026, "maturity_rating": "PG-13", "available_regions": "GLOBAL"},
    {"item_id": "m011", "title": "Gold Reef Stories", "item_type": "series",  "genres": "history,drama",       "actors": "Nomsa Dube, Peter Hall",  "director": "Kabelo Sithole", "synopsis": "Three families build fortunes and rivalries in a mining boomtown.", "language": "en", "release_year": 2023, "maturity_rating": "PG",   "available_regions": "GLOBAL"},
    {"item_id": "m012", "title": "Planet Bites",      "item_type": "series",  "genres": "documentary,food",    "actors": "Leah Singh",              "director": "Marco Bell",     "synopsis": "A chef travels the world to document the science behind iconic dishes.", "language": "en", "release_year": 2022, "maturity_rating": "G",    "available_regions": "GLOBAL"},
]

_FALLBACK_USERS = [
    {"user_id": f"u{i:03d}", "region": region, "preferred_language": "en", "maturity_rating": maturity}
    for i, (region, maturity) in enumerate(
        [("UK","PG-13"),("ZA","PG-13"),("US","PG"),("UK","PG"),("ZA","PG"),
         ("US","PG-13"),("ZA","PG-13"),("UK","PG-13"),("US","PG"),("ZA","PG")],
        start=1,
    )
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _wait_for_postgres(max_attempts: int = 30) -> None:
    for attempt in range(max_attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Postgres ready")
            return
        except Exception as exc:
            logger.info("Waiting for Postgres", extra={"attempt": attempt + 1, "error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Postgres not available")


def _wipe_all(session) -> None:
    """Clear all tables so we can reseed cleanly."""
    for model in [
        Interaction, ItemNeighbor, TrendingItem, ModelEvaluation,
        RankingWeights, ModelVersion, Experiment, Item, User,
    ]:
        session.execute(delete(model))
    session.commit()
    logger.info("Wiped existing data for reseed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _wait_for_postgres()
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as session:
        item_count = session.execute(
            select(Item).limit(1)
        ).scalars().first()

        if item_count is not None:
            total = session.execute(text("SELECT COUNT(*) FROM items")).scalar()
            if total >= _REAL_CATALOG_MIN:
                logger.info("Already seeded with real data — skipping", extra={"items": total})
                _backfill_redis()
                return
            logger.info(
                "Found small fallback catalog — wiping to reseed with MovieLens 1M",
                extra={"items": total},
            )
            _wipe_all(session)

    # Try to download and parse MovieLens 1M
    try:
        zip_bytes = _download_ml1m()
        items, users, interactions = _parse_ml1m(zip_bytes)
        use_fallback = False
    except Exception as exc:
        logger.warning(
            "MovieLens download failed — using fallback catalog",
            extra={"error": str(exc)},
        )
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        items = [{**c, "is_active": True, "created_at": now} for c in _FALLBACK_CATALOG]
        users = [{**u, "created_at": now} for u in _FALLBACK_USERS]
        interactions = []
        use_fallback = True

    with SessionLocal() as session:
        logger.info("Inserting users", extra={"count": len(users)})
        _bulk_insert(session, User, users, "users")

        logger.info("Inserting items", extra={"count": len(items)})
        _bulk_insert(session, Item, items, "items")

        if interactions:
            logger.info("Inserting interactions", extra={"count": len(interactions)})
            _bulk_insert(session, Interaction, interactions, "interactions")

    source = "fallback" if use_fallback else "MovieLens 1M"
    logger.info(
        "Seeding complete",
        extra={
            "source":       source,
            "items":        len(items),
            "users":        len(users),
            "interactions": len(interactions),
        },
    )

    _backfill_redis()


def _backfill_redis() -> None:
    """
    Populate Redis online-feature keys from the Postgres interaction history.
    Without this, every user appears as cold-start on a fresh deploy because
    the stream processor hasn't had a chance to write anything to Redis yet.
    Skips if Redis already has recent-item keys (i.e. not cold).
    """
    redis = get_redis()
    if redis.keys("recent:*"):
        logger.info("Redis already has recent-item keys — skipping backfill")
        return

    with SessionLocal() as session:
        items = session.execute(select(Item)).scalars().all()
        item_genres: dict[str, str] = {i.item_id: i.genres for i in items}

        rows = session.execute(
            text(
                "SELECT user_id, item_id, event_type, completion_pct, watch_seconds, "
                "       region, extract(epoch from event_ts) as ts "
                "FROM interactions ORDER BY user_id, event_ts"
            )
        ).fetchall()

    if not rows:
        logger.info("No interactions to backfill into Redis — skipping")
        return

    logger.info("Backfilling Redis", extra={"interactions": len(rows)})

    user_rows: dict[str, list] = defaultdict(list)
    region_scores: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in rows:
        user_rows[r.user_id].append(r)
        weight = _EVENT_WEIGHT.get(r.event_type, 0.1)
        region_scores[r.region][r.item_id] += weight

    pipe = redis.pipeline()
    written = 0

    for user_id, ixs in user_rows.items():
        recent_key = f"recent:{user_id}"
        watched_key = f"watched:{user_id}"
        genre_key = f"genre_affinity:{user_id}"

        pipe.delete(recent_key)
        for r in ixs[-_REDIS_RECENT_LIMIT:]:
            pipe.zadd(recent_key, {r.item_id: float(r.ts)})

        for r in ixs:
            if r.event_type == "complete" or (r.completion_pct or 0) >= 5.0 or (r.watch_seconds or 0) >= 120:
                pipe.sadd(watched_key, r.item_id)

        pipe.delete(genre_key)
        genre_totals: dict[str, float] = defaultdict(float)
        for r in ixs:
            genres = item_genres.get(r.item_id, "")
            if not genres:
                continue
            w = _EVENT_WEIGHT.get(r.event_type, 0.1)
            for g in genres.split(","):
                genre_totals[g.strip()] += w
        if genre_totals:
            pipe.hset(genre_key, mapping={k: str(round(v, 4)) for k, v in genre_totals.items()})
        pipe.expire(genre_key, _GENRE_TTL)

        written += 1
        if written % 500 == 0:
            pipe.execute()
            pipe = redis.pipeline()
            logger.info("Redis backfill progress", extra={"users": written, "total": len(user_rows)})

    for region, scores in region_scores.items():
        pop_key = f"popular:{region}"
        pipe.delete(pop_key)
        for item_id, score in scores.items():
            pipe.zadd(pop_key, {item_id: round(score, 4)})
        pipe.expire(pop_key, _POP_TTL)

    pipe.execute()
    logger.info("Redis backfill complete", extra={"users": written, "regions": len(region_scores)})


if __name__ == "__main__":
    main()
