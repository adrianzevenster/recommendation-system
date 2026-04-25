import logging
import math
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
from prometheus_client import start_http_server
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import delete, select, text

from common.config import settings
from common.db import SessionLocal, engine
from common.logging_utils import configure_logging
from common.metrics import MODELS_TRAINED
from common.models import Base, Interaction, Item, ItemNeighbor, ModelVersion, TrendingItem
from common.telemetry import setup_tracing

SERVICE_NAME = "trainer"
configure_logging(SERVICE_NAME)
logger = logging.getLogger(__name__)
tracer = setup_tracing(SERVICE_NAME)


def serve_metrics():
    start_http_server(settings.metrics_port)


def wait_for_postgres() -> None:
    for _ in range(30):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:
            logger.info("Waiting for Postgres", extra={"error": str(exc)})
            time.sleep(2)
    raise RuntimeError("Postgres unavailable")


def interaction_weight(event_type: str, completion_pct: float) -> float:
    base = {
        "impression": 0.2,
        "click": 0.5,
        "play_start": 1.0,
        "watch_progress": 1.5,
        "complete": 2.2,
        "watchlist_add": 0.8,
    }.get(event_type, 0.1)
    return base + (completion_pct / 100.0)


def build_collaborative_neighbors(interactions: list[Interaction], top_k: int = 8):
    user_items = defaultdict(list)
    for interaction in interactions:
        if interaction.event_type not in {"play_start", "watch_progress", "complete", "watchlist_add", "click"}:
            continue
        user_items[interaction.user_id].append(interaction.item_id)

    co_counts = defaultdict(Counter)
    item_counts = Counter()

    for items in user_items.values():
        unique_items = list(dict.fromkeys(items))
        for item in unique_items:
            item_counts[item] += 1
        for i in range(len(unique_items)):
            for j in range(i + 1, len(unique_items)):
                a, b = unique_items[i], unique_items[j]
                co_counts[a][b] += 1
                co_counts[b][a] += 1

    neighbors = []
    for source_item, related in co_counts.items():
        scored = []
        for neighbor, count in related.items():
            denom = math.sqrt(item_counts[source_item] * item_counts[neighbor]) or 1.0
            score = count / denom
            scored.append((neighbor, score))
        for neighbor, score in sorted(scored, key=lambda x: x[1], reverse=True)[:top_k]:
            neighbors.append((source_item, neighbor, float(round(score, 6)), "collaborative"))
    return neighbors


def build_content_neighbors(items: list[Item], top_k: int = 8):
    if not items:
        return []
    corpus = [
        f"{item.title} {item.genres.replace(',', ' ')} {item.actors} {item.director} {item.synopsis}"
        for item in items
    ]
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(corpus)
    sim = cosine_similarity(matrix)

    neighbors = []
    for i, item in enumerate(items):
        scores = []
        for j, other in enumerate(items):
            if i == j:
                continue
            scores.append((other.item_id, float(sim[i][j])))
        for neighbor, score in sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]:
            neighbors.append((item.item_id, neighbor, float(round(score, 6)), "content"))
    return neighbors


def build_trending(interactions: list[Interaction]):
    region_scores = defaultdict(Counter)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    for interaction in interactions:
        if interaction.event_ts.replace(tzinfo=timezone.utc) < cutoff:
            continue
        score = interaction_weight(interaction.event_type, interaction.completion_pct)
        region_scores[interaction.region][interaction.item_id] += score

    trending = []
    for region, scores in region_scores.items():
        for item_id, score in scores.most_common(10):
            trending.append((region, item_id, float(round(score, 6))))
    return trending


def run_training_once():
    with tracer.start_as_current_span("training_run"):
        with SessionLocal() as session:
            items = session.execute(select(Item).where(Item.is_active.is_(True))).scalars().all()
            interactions = session.execute(select(Interaction)).scalars().all()

            collaborative = build_collaborative_neighbors(interactions)
            content = build_content_neighbors(items)
            trending = build_trending(interactions)

            session.execute(delete(ItemNeighbor))
            session.execute(delete(TrendingItem))

            session.add_all(
                [
                    ItemNeighbor(
                        source_item_id=source,
                        neighbor_item_id=neighbor,
                        score=score,
                        algorithm=algo,
                    )
                    for source, neighbor, score, algo in collaborative + content
                ]
            )
            session.add_all(
                [TrendingItem(region=region, item_id=item_id, score=score) for region, item_id, score in trending]
            )
            version = f"hybrid-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            session.add(ModelVersion(version=version, description="co-occurrence + tfidf hybrid"))
            session.commit()

            MODELS_TRAINED.inc()
            logger.info(
                "Completed training run",
                extra={
                    "version": version,
                    "collaborative_edges": len(collaborative),
                    "content_edges": len(content),
                    "trending_rows": len(trending),
                },
            )


def main() -> None:
    threading.Thread(target=serve_metrics, daemon=True).start()
    wait_for_postgres()
    Base.metadata.create_all(bind=engine)
    while True:
        try:
            run_training_once()
        except Exception as exc:
            logger.exception("Training failed", extra={"error": str(exc)})
        time.sleep(60)


if __name__ == "__main__":
    main()
