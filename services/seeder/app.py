import logging
import time
from sqlalchemy import select, text

from common.db import SessionLocal, engine
from common.logging_utils import configure_logging
from common.models import Base, Item, User

configure_logging("seeder")
logger = logging.getLogger(__name__)


CATALOG = [
    {
        "item_id": "m001",
        "title": "Shadow Protocol",
        "item_type": "movie",
        "genres": "thriller,crime",
        "actors": "Ava Cross, Liam Hart",
        "director": "Noah Kent",
        "synopsis": "An analyst uncovers a conspiracy spanning intelligence agencies.",
        "language": "en",
        "release_year": 2024,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m002",
        "title": "Neon District",
        "item_type": "series",
        "genres": "sci-fi,action",
        "actors": "Mila Ray, Omar Stone",
        "director": "Zane Yu",
        "synopsis": "Detectives pursue synthetic criminals in a vertical megacity.",
        "language": "en",
        "release_year": 2025,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m003",
        "title": "Kitchen Kings",
        "item_type": "series",
        "genres": "comedy,drama",
        "actors": "Nia Banks, Theo Reed",
        "director": "Sara Cole",
        "synopsis": "A struggling restaurant becomes the center of a family power struggle.",
        "language": "en",
        "release_year": 2023,
        "maturity_rating": "PG",
        "available_regions": "GLOBAL,UK,ZA",
    },
    {
        "item_id": "m004",
        "title": "Savannah Unit",
        "item_type": "movie",
        "genres": "action,adventure",
        "actors": "Daniel Khoza, Jada Mills",
        "director": "Priya Ndlovu",
        "synopsis": "Elite responders cross borders to stop an ecological disaster.",
        "language": "en",
        "release_year": 2022,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,ZA,US",
    },
    {
        "item_id": "m005",
        "title": "Velvet Court",
        "item_type": "series",
        "genres": "romance,drama",
        "actors": "Ana Flores, Marcus Webb",
        "director": "Helen Moore",
        "synopsis": "Rival lawyers fall in love while battling a corporate dynasty.",
        "language": "en",
        "release_year": 2024,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m006",
        "title": "Orbit Nine",
        "item_type": "movie",
        "genres": "sci-fi,thriller",
        "actors": "Ravi Cole, Elena North",
        "director": "Mina Park",
        "synopsis": "A damaged station AI may be the crew's only hope of survival.",
        "language": "en",
        "release_year": 2026,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m007",
        "title": "Harbor Files",
        "item_type": "series",
        "genres": "crime,mystery",
        "actors": "Tessa Gray, Joel Prince",
        "director": "Nina Vargas",
        "synopsis": "Cold cases reopen when a port city digitizes decades of records.",
        "language": "en",
        "release_year": 2021,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA",
    },
    {
        "item_id": "m008",
        "title": "Laugh Track Hotel",
        "item_type": "series",
        "genres": "comedy",
        "actors": "Ivy Price, Ben Moss",
        "director": "Luca Dean",
        "synopsis": "Hotel staff navigate chaos when their workplace becomes a reality show.",
        "language": "en",
        "release_year": 2020,
        "maturity_rating": "PG",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m009",
        "title": "The Last Pitch",
        "item_type": "movie",
        "genres": "sports,drama",
        "actors": "Chris Dale, Faith Morgan",
        "director": "Eli Turner",
        "synopsis": "An aging coach assembles one final team from rejected talent.",
        "language": "en",
        "release_year": 2025,
        "maturity_rating": "PG",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m010",
        "title": "Cipher Garden",
        "item_type": "movie",
        "genres": "mystery,thriller",
        "actors": "Ari Vale, Sora Chen",
        "director": "Imani Ford",
        "synopsis": "A botanist finds hidden messages encoded in genetically edited flowers.",
        "language": "en",
        "release_year": 2026,
        "maturity_rating": "PG-13",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
    {
        "item_id": "m011",
        "title": "Gold Reef Stories",
        "item_type": "series",
        "genres": "history,drama",
        "actors": "Nomsa Dube, Peter Hall",
        "director": "Kabelo Sithole",
        "synopsis": "Three families build fortunes and rivalries in a mining boomtown.",
        "language": "en",
        "release_year": 2023,
        "maturity_rating": "PG",
        "available_regions": "GLOBAL,ZA,UK",
    },
    {
        "item_id": "m012",
        "title": "Planet Bites",
        "item_type": "series",
        "genres": "documentary,food",
        "actors": "Leah Singh",
        "director": "Marco Bell",
        "synopsis": "A chef travels the world to document the science behind iconic dishes.",
        "language": "en",
        "release_year": 2022,
        "maturity_rating": "G",
        "available_regions": "GLOBAL,UK,ZA,US",
    },
]

USERS = [
    {"user_id": f"u{i:03d}", "region": region, "preferred_language": "en", "maturity_rating": maturity}
    for i, (region, maturity) in enumerate(
        [
            ("UK", "PG-13"),
            ("ZA", "PG-13"),
            ("US", "PG"),
            ("UK", "PG"),
            ("ZA", "PG"),
            ("US", "PG-13"),
            ("ZA", "PG-13"),
            ("UK", "PG-13"),
            ("US", "PG"),
            ("ZA", "PG"),
        ],
        start=1,
    )
]


def wait_for_postgres(max_attempts: int = 30) -> None:
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


def main() -> None:
    wait_for_postgres()
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as session:
        existing_users = session.execute(select(User)).scalars().first()
        if not existing_users:
            session.add_all([User(**user) for user in USERS])
            logger.info("Seeded users", extra={"count": len(USERS)})

        existing_items = session.execute(select(Item)).scalars().first()
        if not existing_items:
            session.add_all([Item(**item) for item in CATALOG])
            logger.info("Seeded catalog", extra={"count": len(CATALOG)})

        session.commit()


if __name__ == "__main__":
    main()
