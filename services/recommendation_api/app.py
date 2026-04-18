import logging
import time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import desc, select

from common.config import settings
from common.db import SessionLocal, engine
from common.logging_utils import configure_logging
from common.metrics import CANDIDATES_GENERATED, RECOMMENDATION_REQUESTS, REQUEST_LATENCY
from common.models import Base, Item, ItemNeighbor, ModelVersion, TrendingItem, User
from common.redis_client import get_redis
from common.telemetry import instrument_fastapi

SERVICE_NAME = "recommendation-api"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
redis_client = get_redis()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Recommendation API", lifespan=lifespan)
instrument_fastapi(app, SERVICE_NAME)


MATURE_ORDER = {"G": 0, "PG": 1, "PG-13": 2, "R": 3}


def normalize_scores(score_map: dict[str, float]) -> dict[str, float]:
    if not score_map:
        return {}
    max_score = max(score_map.values()) or 1.0
    return {k: v / max_score for k, v in score_map.items()}


def get_latest_model_version(session) -> str:
    version = session.execute(select(ModelVersion).order_by(desc(ModelVersion.created_at))).scalars().first()
    return version.version if version else "untrained"


def fetch_candidate_scores(session, recent_items: list[str], region: str):
    collab = Counter()
    content = Counter()
    trending = Counter()

    if recent_items:
        collab_rows = session.execute(
            select(ItemNeighbor).where(
                ItemNeighbor.source_item_id.in_(recent_items), ItemNeighbor.algorithm == "collaborative"
            )
        ).scalars().all()
        for row in collab_rows:
            collab[row.neighbor_item_id] += row.score

        content_rows = session.execute(
            select(ItemNeighbor).where(
                ItemNeighbor.source_item_id.in_(recent_items), ItemNeighbor.algorithm == "content"
            )
        ).scalars().all()
        for row in content_rows:
            content[row.neighbor_item_id] += row.score

    trending_rows = session.execute(select(TrendingItem).where(TrendingItem.region == region)).scalars().all()
    for row in trending_rows:
        trending[row.item_id] = row.score

    return normalize_scores(dict(collab)), normalize_scores(dict(content)), normalize_scores(dict(trending))


def fetch_session_candidates(recent_items: list[str]) -> dict[str, float]:
    # Session signal is a simple recency boost derived from the last 5 titles.
    session_scores = {}
    weight = 1.0
    for item_id in reversed(recent_items[-5:]):
        session_scores[item_id] = session_scores.get(item_id, 0.0) + weight
        weight *= 0.7
    return normalize_scores(session_scores)


def rank_candidates(
    session,
    user: User,
    collab: dict[str, float],
    content: dict[str, float],
    session_scores: dict[str, float],
    trending: dict[str, float],
    watched: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    genre_affinity = redis_client.hgetall(f"genre_affinity:{user.user_id}")
    genre_affinity = {k: float(v) for k, v in genre_affinity.items()}

    candidate_ids = set(collab) | set(content) | set(session_scores) | set(trending)
    CANDIDATES_GENERATED.labels(source="merged").observe(len(candidate_ids))

    items = session.execute(select(Item).where(Item.item_id.in_(candidate_ids))).scalars().all() if candidate_ids else []
    item_map = {item.item_id: item for item in items}
    scored = []

    for item_id in candidate_ids:
        if item_id in watched:
            continue
        item = item_map.get(item_id)
        if item is None or not item.is_active:
            continue
        if user.region not in item.available_regions and "GLOBAL" not in item.available_regions:
            continue
        if MATURE_ORDER.get(item.maturity_rating, 0) > MATURE_ORDER.get(user.maturity_rating, 2):
            continue

        genre_bonus = sum(genre_affinity.get(g.strip(), 0.0) for g in item.genres.split(","))
        genre_bonus = min(genre_bonus / 10.0, 1.0)
        freshness = min(max((item.release_year - 2020) / 6.0, 0.0), 1.0)

        score = (
            0.35 * collab.get(item_id, 0.0)
            + 0.25 * content.get(item_id, 0.0)
            + 0.20 * session_scores.get(item_id, 0.0)
            + 0.10 * trending.get(item_id, 0.0)
            + 0.05 * freshness
            + 0.05 * genre_bonus
        )
        components = {
            "collaborative": collab.get(item_id, 0.0),
            "content": content.get(item_id, 0.0),
            "session": session_scores.get(item_id, 0.0),
            "trending": trending.get(item_id, 0.0),
            "freshness": freshness,
            "genre_bonus": genre_bonus,
        }
        reason = max(components, key=components.get)
        scored.append((item, score, reason, components))

    scored.sort(key=lambda x: x[1], reverse=True)

    # Diversity guardrail: no more than two of the same leading genre in top-N.
    results = []
    genre_counts = defaultdict(int)
    for item, score, reason, components in scored:
        lead_genre = item.genres.split(",")[0].strip()
        if genre_counts[lead_genre] >= 2:
            continue
        genre_counts[lead_genre] += 1
        results.append(
            {
                "item_id": item.item_id,
                "title": item.title,
                "genres": item.genres.split(","),
                "score": round(score, 4),
                "reason": reason,
                "components": {k: round(v, 4) for k, v in components.items()},
            }
        )
        if len(results) >= limit:
            break
    return results


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.get("/recommendations/{user_id}")
def recommendations(
    user_id: str,
    context: str = Query(default="home"),
    limit: int = Query(default=settings.recommendation_limit_default, ge=1, le=25),
):
    started = time.perf_counter()
    RECOMMENDATION_REQUESTS.labels(context=context).inc()

    with SessionLocal() as session:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Unknown user")

        recent_items = redis_client.zrevrange(f"recent:{user_id}", 0, 9)
        watched = set(redis_client.smembers(f"watched:{user_id}"))
        collab, content, trending = fetch_candidate_scores(session, recent_items, user.region)
        session_scores = fetch_session_candidates(recent_items)
        rows = rank_candidates(session, user, collab, content, session_scores, trending, watched, limit)
        model_version = get_latest_model_version(session)

    elapsed = time.perf_counter() - started
    REQUEST_LATENCY.labels(endpoint="/recommendations/{user_id}").observe(elapsed)

    return {
        "user_id": user_id,
        "context": context,
        "model_version": model_version,
        "recent_items": recent_items,
        "recommendations": rows,
        "latency_ms": round(elapsed * 1000, 2),
    }
