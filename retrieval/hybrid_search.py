"""
Hybrid retrieval: semantic (ChromaDB) + BM25 keyword → RRF merge.

Pipeline:
  1. Semantic search via ChromaDB (existing)
  2. BM25 keyword search on chunk corpus
  3. RRF (Reciprocal Rank Fusion) merge + dedup

Lazy-loading: BM25 index built on first use, cached thereafter.
Call rebuild_hybrid_index() after re-ingesting BIRD data.
"""

import re
import threading

_RRF_K = 60

# ── Lazy singletons ──
_bm25_index = None
_bm25_corpus_meta: list[dict] = []  # [{id, document, db_id, table_name, chunk_type, source_path}]
_bm25_lock = threading.Lock()


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9_]+", text.lower())


def _build_bm25():
    global _bm25_index, _bm25_corpus_meta

    from rank_bm25 import BM25Okapi
    from retrieval.rag_retrieve import get_bird_collection

    col = get_bird_collection()
    data = col.get(include=["documents", "metadatas"])

    corpus_tok: list[list[str]] = []
    meta_list: list[dict] = []

    for chunk_id, doc, meta in zip(data["ids"],
                                    data["documents"] or [],
                                    data["metadatas"] or []):
        corpus_tok.append(_tokenize(doc or ""))
        meta_list.append({
            "id": chunk_id,
            "document": doc or "",
            "db_id": meta.get("db_id", ""),
            "table_name": meta.get("table_name", ""),
            "chunk_type": meta.get("chunk_type", "domain"),
            "source_path": meta.get("source_path", ""),
        })

    _bm25_index = BM25Okapi(corpus_tok)
    _bm25_corpus_meta = meta_list


def _get_bm25():
    global _bm25_index
    if _bm25_index is None:
        with _bm25_lock:
            if _bm25_index is None:
                _build_bm25()
    return _bm25_index, _bm25_corpus_meta


def rebuild_hybrid_index():
    """Force rebuild of BM25 index. Call after re-ingesting BIRD data."""
    global _bm25_index, _bm25_corpus_meta
    with _bm25_lock:
        _bm25_index = None
        _bm25_corpus_meta = []


def _bm25_search(question: str, db_id: str, k: int) -> list[dict]:
    """BM25 keyword search filtered by db_id. Returns same shape as retrieve_bird."""
    bm25, meta = _get_bm25()

    tokenized = _tokenize(question)
    if not tokenized or bm25 is None or len(meta) == 0:
        return []

    scores = bm25.get_scores(tokenized)

    scored = [(scores[i], meta[i]) for i in range(len(meta))
              if meta[i]["db_id"] == db_id]
    scored.sort(key=lambda x: x[0], reverse=True)

    results: list[dict] = []
    seen: set[tuple] = set()
    for score, m in scored:
        key = (m["db_id"], m["table_name"], m["chunk_type"])
        if key in seen:
            continue
        seen.add(key)
        results.append({
            "content": m["document"],
            "source": m["source_path"],
            "metadata": {
                "db_id": m["db_id"],
                "table_name": m["table_name"],
                "chunk_type": m["chunk_type"],
            },
        })
        if len(results) >= k:
            break

    return results


def _rrf_merge(*result_lists: list[dict], k: int = _RRF_K) -> list[dict]:
    """Reciprocal Rank Fusion: merge multiple ranked lists, dedup by (db_id, table, type)."""

    def _key(item: dict) -> str:
        m = item.get("metadata", {})
        return f"{m.get('db_id','')}::{m.get('table_name','')}::{m.get('chunk_type','')}"

    scores: dict[str, float] = {}
    items: dict[str, dict] = {}

    for results in result_lists:
        for rank, item in enumerate(results):
            rk = _key(item)
            scores[rk] = scores.get(rk, 0) + 1.0 / (k + rank + 1)
            if rk not in items:
                items[rk] = item

    merged = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [items[rk] for rk, _ in merged]


def hybrid_retrieve(question: str, db_id: str, k: int = 8) -> list[dict]:
    """
    Hybrid retrieval: semantic + BM25 → RRF merge.

    Same return shape as rag_retrieve.retrieve_bird():
      [{content: str, source: str, metadata: {db_id, table_name, chunk_type}}]
    """
    from retrieval.rag_retrieve import retrieve_bird

    # Path 1: Semantic (2x for richer RRF pool)
    semantic_results = retrieve_bird(question, db_id, k=k * 2)

    # Path 2: BM25 keyword (2x for richer RRF pool)
    bm25_results = _bm25_search(question, db_id, k=k * 2)

    if not bm25_results:
        return semantic_results[:k]
    if not semantic_results:
        return bm25_results[:k]

    merged = _rrf_merge(semantic_results, bm25_results)
    return merged[:k]
