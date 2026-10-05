"""Candidate-signal builders: collaborative (co-occurrence + MF), content, trending."""
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize as sklearn_normalize

from common.eval import ENGAGEMENT_EVENTS, interaction_weight
from common.models import Interaction, Item


def build_collaborative_neighbors(interactions: list[Interaction], top_k: int = 8):
    """Co-occurrence based item-item collaborative filtering."""
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


def build_mf_neighbors(
    interactions: list[Interaction],
    n_components: int = 50,
    top_k: int = 8,
) -> tuple[list[tuple], dict[str, np.ndarray]]:
    """SVD matrix factorization → item-item similarity for enhanced collaborative signal.

    Complements co-occurrence CF by capturing latent factor structure that
    co-occurrence misses (e.g. items with sparse but high-quality overlap).
    Results are blended with co-occurrence scores in run_training_once().

    Returns (neighbors, item_embeddings) where item_embeddings maps item_id
    to its L2-normalised SVD factor — used to build the FAISS ANN index.
    """
    engagement_ixs = [ix for ix in interactions if ix.event_type in ENGAGEMENT_EVENTS]
    if not engagement_ixs:
        return [], {}

    user_ids = list(dict.fromkeys(ix.user_id for ix in engagement_ixs))
    item_ids = list(dict.fromkeys(ix.item_id for ix in engagement_ixs))

    if len(user_ids) < 5 or len(item_ids) < 5:
        return [], {}

    user_idx = {uid: i for i, uid in enumerate(user_ids)}
    item_idx = {iid: i for i, iid in enumerate(item_ids)}

    # Accumulate max interaction weight per (user, item) pair
    score_map: dict[tuple[int, int], float] = {}
    for ix in engagement_ixs:
        u = user_idx[ix.user_id]
        it = item_idx[ix.item_id]
        w = interaction_weight(ix.event_type, ix.completion_pct)
        key = (u, it)
        score_map[key] = max(score_map.get(key, 0.0), w)

    if not score_map:
        return [], {}

    rows_list = [k[0] for k in score_map]
    cols_list = [k[1] for k in score_map]
    data_list = list(score_map.values())

    matrix = csr_matrix(
        (data_list, (rows_list, cols_list)),
        shape=(len(user_ids), len(item_ids)),
    )

    n_comp = min(n_components, min(matrix.shape) - 1)
    if n_comp < 2:
        return [], {}

    svd = TruncatedSVD(n_components=n_comp, random_state=42)
    item_factors = svd.fit_transform(matrix.T)  # shape: (n_items, n_components)
    item_factors = sklearn_normalize(item_factors, norm="l2")

    k = min(top_k + 1, len(item_ids))
    nn = NearestNeighbors(n_neighbors=k, metric="cosine", algorithm="brute")
    nn.fit(item_factors)
    distances, indices = nn.kneighbors(item_factors)

    neighbors = []
    for i, iid in enumerate(item_ids):
        for rank, j in enumerate(indices[i]):
            if int(j) == i:
                continue
            sim = float(round(1.0 - float(distances[i][rank]), 6))
            if sim > 0.0:
                neighbors.append((iid, item_ids[int(j)], sim, "mf"))

    # Return embeddings dict alongside neighbors for ANN index construction
    embeddings = {item_ids[i]: item_factors[i] for i in range(len(item_ids))}
    return neighbors, embeddings


def build_content_neighbors(items: list[Item], top_k: int = 8):
    """TF-IDF content similarity via k-NN. Returns (neighbors, vectorizer)."""
    if not items:
        return [], None
    corpus = [
        f"{item.title} {item.genres.replace(',', ' ')} {item.actors} {item.director} {item.synopsis}"
        for item in items
    ]
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(corpus)

    k = min(top_k + 1, len(items))  # +1 because each item is its own nearest neighbour
    nn = NearestNeighbors(n_neighbors=k, metric="cosine", algorithm="brute")
    nn.fit(matrix)
    distances, indices = nn.kneighbors(matrix)

    neighbors = []
    for i, item in enumerate(items):
        for rank, j in enumerate(indices[i]):
            if int(j) == i:
                continue
            sim = float(round(1.0 - float(distances[i][rank]), 6))
            neighbors.append((item.item_id, items[int(j)].item_id, sim, "content"))
    return neighbors, vectorizer


def build_trending(interactions: list[Interaction]):
    """Build regional trending scores with exponential time-decay within the 24h window."""
    region_scores = defaultdict(Counter)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    for interaction in interactions:
        evt_ts = interaction.event_ts.replace(tzinfo=timezone.utc)
        if evt_ts < cutoff:
            continue
        hours_ago = (now - evt_ts).total_seconds() / 3600.0
        decay = math.exp(-0.1 * hours_ago)
        score = interaction_weight(
            interaction.event_type,
            interaction.completion_pct,
            watch_seconds=interaction.watch_seconds,
        ) * decay
        region_scores[interaction.region][interaction.item_id] += score

    trending = []
    for region, scores in region_scores.items():
        for item_id, score in scores.most_common(10):
            trending.append((region, item_id, float(round(score, 6))))
    return trending
