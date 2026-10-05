"""Approximate Nearest Neighbour index utilities (FAISS-backed).

The index stores L2-normalised item embeddings produced by TruncatedSVD.
Inner-product search on normalised vectors equals cosine similarity, so
IndexFlatIP gives exact cosine nearest-neighbours with no approximation
error at the embedding size we use (50-d).

For very large catalogs (>500 k items) swap IndexFlatIP for IndexIVFFlat
or IndexHNSWFlat — the query() contract is identical.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import faiss as _faiss_type

logger = logging.getLogger(__name__)


def build_ann_index(
    item_embeddings: dict[str, np.ndarray],
) -> tuple["_faiss_type.Index", list[str]]:
    """Build a FAISS IndexFlatIP from a dict of item_id → L2-normalised vector.

    Returns (index, id_list) where id_list[i] is the item_id at FAISS row i.
    Returns (None, []) when there are fewer than 5 items.
    """
    import faiss

    if len(item_embeddings) < 5:
        return None, []

    id_list = list(item_embeddings.keys())
    vectors = np.stack([item_embeddings[iid] for iid in id_list]).astype(np.float32)

    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(vectors)
    logger.info("Built ANN index", extra={"items": len(id_list), "dim": dim})
    return index, id_list


def serialize_index(index: "_faiss_type.Index") -> bytes:
    """Serialize a FAISS index to a bytes object for storage."""
    import faiss
    arr = faiss.serialize_index(index)
    return arr.tobytes()


def deserialize_index(data: bytes) -> "_faiss_type.Index":
    """Deserialize a FAISS index from raw bytes."""
    import faiss
    arr = np.frombuffer(data, dtype=np.uint8)
    return faiss.deserialize_index(arr)


def query_index(
    index: "_faiss_type.Index",
    id_list: list[str],
    query_item_ids: list[str],
    k: int = 100,
) -> dict[str, float]:
    """Return up to k candidate item_ids (and cosine scores) similar to the query set.

    query_item_ids: items representing the user's recent history.  Their
    embeddings are averaged to form a single query vector.

    Items in query_item_ids are excluded from the results so the user is
    not recommended something they just watched.
    """
    if index is None or not id_list or not query_item_ids:
        return {}

    id_to_idx = {iid: i for i, iid in enumerate(id_list)}
    query_vecs: list[np.ndarray] = []
    for iid in query_item_ids[-10:]:  # cap at 10 seed items
        idx = id_to_idx.get(iid)
        if idx is not None:
            vec = np.empty(index.d, dtype=np.float32)
            index.reconstruct(idx, vec)
            query_vecs.append(vec)

    if not query_vecs:
        return {}

    # Mean of seed embeddings, then re-normalise to stay on the unit sphere
    query_mat = np.mean(query_vecs, axis=0).astype(np.float32)
    norm = np.linalg.norm(query_mat)
    if norm > 0:
        query_mat /= norm
    query_mat = query_mat.reshape(1, -1)

    fetch_k = min(k + len(query_item_ids), index.ntotal)
    scores, indices = index.search(query_mat, fetch_k)

    exclude = set(query_item_ids)
    result: dict[str, float] = {}
    for score, idx in zip(scores[0], indices[0]):
        if idx < 0:
            continue
        iid = id_list[int(idx)]
        if iid not in exclude:
            result[iid] = float(score)
        if len(result) >= k:
            break

    return result
