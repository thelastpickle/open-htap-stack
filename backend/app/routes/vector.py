"""Vector search routes — Cassandra 5 SAI over a float vector column.

Each asset carries a snippet of prose.  Indexing embeds that snippet into
``demo.drone_text_embeddings``; searching embeds the query, asks Cassandra for
the nearest neighbours and scores each with ``similarity_cosine``, then point-reads
the matching assets for their current position.  So one search exercises the
analytical index and the transactional path together, which is the argument the
whole stack is making.

Embeddings come from an OpenAI-compatible endpoint when a key is configured.
Without one the backend uses a local hashing embedder: no network, no key, and
still genuinely lexical, so the demo ranks by real similarity rather than
dressing noise up as a result.
"""
import asyncio
import hashlib
import math
import re
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.config import settings
from app.db.cassandra_client import cassandra_client
from app.models import VectorSearchRequest, VectorSearchResponse

router = APIRouter(prefix="/api/vector", tags=["vector"])

# Must match the column definition the ingest sink creates.
EMBEDDING_DIMS = 1536
# Concurrent embed+write pairs during a bulk index.
INDEX_CONCURRENCY = 8
_TOKEN_RE = re.compile(r"[a-z0-9]+")


# ──────────────────────── Embeddings ────────────────────────


def local_embedding(text: str) -> List[float]:
    """A deterministic hashing embedder ("hashing trick").

    Each token is hashed to a dimension and accumulated, then the vector is
    L2-normalised.  Texts that share vocabulary land near each other, which is
    what makes an offline demo of cosine similarity meaningful.  It is
    deterministic across processes — Python's ``hash()`` is not, being salted per
    interpreter, so a vector written by one run would not match a query embedded
    by the next.
    """
    vector = [0.0] * EMBEDDING_DIMS
    tokens = _TOKEN_RE.findall(text.lower())
    for token in tokens:
        if len(token) < 3:
            continue
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        index = int.from_bytes(digest[:4], "big") % EMBEDDING_DIMS
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign

    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0.0:
        # An empty or all-stopword text: a fixed unit vector keeps the column
        # valid and keeps such rows at a constant, low similarity.
        vector[0] = 1.0
        return vector
    return [v / norm for v in vector]


async def get_embedding(text: str) -> List[float]:
    """Embed text remotely when configured, locally otherwise."""
    if not settings.openai_api_key:
        return local_embedding(text)
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                f"{settings.openai_base_url.rstrip('/')}/embeddings",
                headers={
                    "Authorization": f"Bearer {settings.openai_api_key}",
                    "Content-Type": "application/json",
                },
                json={"model": settings.embedding_model, "input": text},
            )
            resp.raise_for_status()
            vector = resp.json()["data"][0]["embedding"]
    except Exception as e:
        print(f"[vector] embedding endpoint failed, using local embedder: {e}")
        return local_embedding(text)

    if len(vector) != EMBEDDING_DIMS:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Embedding model returned {len(vector)} dimensions but the "
                f"payload_vector column holds {EMBEDDING_DIMS}"
            ),
        )
    return vector


# ──────────────────────── Search ────────────────────────


@router.post("/search", response_model=VectorSearchResponse)
async def vector_search(req: VectorSearchRequest) -> VectorSearchResponse:
    if not cassandra_client.connected:
        cassandra_client.connect()
    if not cassandra_client.connected:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")

    limit = max(1, min(req.limit, 50))
    query_vector = await get_embedding(req.query)

    def search() -> List[Dict[str, Any]]:
        """ANN over the embeddings, then a point read per hit for live state."""
        hits = cassandra_client.execute_query(
            "SELECT entity_id, text_payload, similarity_cosine(payload_vector, %s) AS similarity "
            f"FROM drone_text_embeddings ORDER BY payload_vector ANN OF %s LIMIT {limit}",
            (query_vector, query_vector),
        )
        results = []
        for hit in hits:
            current = cassandra_client.get_drone_detail(str(hit["entity_id"])) or {}
            results.append({
                "entity_id": hit["entity_id"],
                "text_payload": hit.get("text_payload"),
                "similarity": hit.get("similarity"),
                "observer_id": current.get("observer_id"),
                "latitude": current.get("latitude"),
                "longitude": current.get("longitude"),
                "altitude_m": current.get("altitude_m"),
                "is_flying": current.get("is_flying"),
            })
        return results

    start = time.perf_counter()
    try:
        rows = await asyncio.to_thread(search)
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Vector search failed: {e}. Build the embeddings first — nothing is "
                "indexed until then."
            ),
        )
    return VectorSearchResponse(
        results=rows, query_time_ms=round((time.perf_counter() - start) * 1000, 1)
    )


# ──────────────────────── Indexing ────────────────────────


@router.post("/index-all")
async def index_all(background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """Embed every asset's text snippet, in the background."""
    if not cassandra_client.connected:
        cassandra_client.connect()
    if not cassandra_client.connected:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")
    background_tasks.add_task(_run_indexing)
    return {
        "status": "started",
        "embedder": "remote" if settings.openai_api_key else "local",
        "message": "Indexing started; results appear as rows are embedded.",
    }


async def _run_indexing() -> None:
    """Embed each asset's current snippet into drone_text_embeddings.

    Text and vector are written together, so a stored embedding always matches
    the snippet stored beside it.
    """
    try:
        rows = await asyncio.to_thread(
            cassandra_client.execute_query,
            "SELECT entity_id, text_payload FROM drone_latest_status",
        )
    except Exception as e:
        print(f"[vector] could not read the rows to index: {e}")
        return

    pending = [r for r in rows if (r.get("text_payload") or "").strip()]
    print(
        f"[vector] indexing {len(pending)} of {len(rows)} assets "
        f"({len(rows) - len(pending)} carry no text)"
    )
    if not pending:
        return

    semaphore = asyncio.Semaphore(INDEX_CONCURRENCY)
    indexed = 0

    async def index_one(row: Dict[str, Any]) -> None:
        nonlocal indexed
        async with semaphore:
            text = row["text_payload"]
            try:
                vector = await get_embedding(text)
                await asyncio.to_thread(
                    cassandra_client.execute_query,
                    "INSERT INTO drone_text_embeddings "
                    "(entity_id, text_payload, payload_vector, updated_at) "
                    "VALUES (%s, %s, %s, toTimestamp(now()))",
                    (row["entity_id"], text, vector),
                )
                indexed += 1
            except Exception as e:
                print(f"[vector] indexing {row.get('entity_id')} failed: {e}")

    await asyncio.gather(*(index_one(r) for r in pending))
    print(f"[vector] indexed {indexed} of {len(pending)} assets")


def probe_vector(dimensions: int = EMBEDDING_DIMS) -> Optional[List[float]]:
    """A fixed unit vector, for latency probes against the ANN index."""
    vector = [0.0] * dimensions
    vector[0] = 1.0
    return vector
