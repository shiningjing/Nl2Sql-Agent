"""Redis cache layer — LLM semantic cache.

Stores (question embedding, SQL, exec result) in Redis.
Lookup computes cosine similarity of incoming question against cached embeddings.
On hit (>0.95), re-executes cached SQL as safety verification.
On miss or exec failure, returns None — caller falls through to normal pipeline.

Graceful degradation: if Redis is unreachable, all reads return None (cache miss),
all writes are no-ops. The system operates normally without caching.
"""
import json
import logging
import math
import uuid

import redis

from retrieval.embedder import get_embedder

_log = logging.getLogger("nl2sql.cache")

# ── Lazy singletons ──────────────────────────────────────────────────────────

_redis: redis.Redis | None = None
_redis_checked: bool = False

# ── Constants ─────────────────────────────────────────────────────────────────

_CACHE_PREFIX = "semcache"
_CACHE_KEYS_SET = f"{_CACHE_PREFIX}:keys"
_CACHE_TTL = 7200  # 2 hours
_CACHE_SIM_THRESHOLD = 0.95
_MAX_CACHED_ROWS = 50  # truncate exec_result data to control memory

_CANDIDATE_URLS = [
    "redis://127.0.0.1:6379/0",   # local Redis or Docker port mapping
    "redis://redis:6379/0",       # Docker Compose internal
]


# ── Connection ────────────────────────────────────────────────────────────────

def _try_connect(url: str) -> redis.Redis | None:
    """Attempt a single Redis connection. Returns client or None."""
    try:
        client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
        )
        client.ping()
        return client
    except (redis.ConnectionError, redis.TimeoutError):
        return None


def get_redis() -> redis.Redis | None:
    """Lazy-init Redis connection with local-first fallback.

    Tries 127.0.0.1:6379 first (local / Docker port mapping),
    then redis:6379 (Docker Compose internal DNS).
    Config.REDIS_URL overrides if explicitly set to non-default.
    Returns None if all attempts fail (graceful degradation).
    """
    global _redis, _redis_checked
    if _redis is not None:
        return _redis
    if _redis_checked:
        return None

    from storage.config import Config

    # Explicit env override takes priority
    env_url = Config.REDIS_URL
    is_default = env_url == "redis://localhost:6379/0"
    if not is_default:
        _redis = _try_connect(env_url)
        if _redis:
            _redis_checked = True
            _log.info(json.dumps({"event": "redis_connect", "status": "ok",
                                   "url": env_url.split("@")[-1] if "@" in env_url else env_url}))
            return _redis

    # Local-first auto-detect
    for url in _CANDIDATE_URLS:
        _redis = _try_connect(url)
        if _redis:
            _redis_checked = True
            _log.info(json.dumps({"event": "redis_connect", "status": "ok",
                                   "url": url.split("@")[-1] if "@" in url else url}))
            return _redis

    _redis_checked = True
    _log.warning(json.dumps({"event": "redis_connect", "status": "unavailable",
                               "tried": _CANDIDATE_URLS}))
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _cleanup_stale(r: redis.Redis, cache_id: str) -> None:
    """Remove a stale cache entry from the key set."""
    r.srem(_CACHE_KEYS_SET, cache_id)


# ── LLM Semantic Cache ───────────────────────────────────────────────────────

def cache_get_llm(question: str, threshold: float = _CACHE_SIM_THRESHOLD) -> dict | None:
    """Check semantic cache for a question.

    Returns dict with keys: question, sql, exec_result, similarity — or None on miss.
    """
    r = get_redis()
    if r is None:
        return None

    keys = r.smembers(_CACHE_KEYS_SET)
    if not keys:
        _log.info(json.dumps({"event": "cache_miss", "reason": "empty",
                               "question": question[:80]}))
        return None

    embedder = get_embedder()
    q_embedding = embedder.encode(question).tolist()

    best_sim = 0.0
    best_entry = None

    for cache_id in keys:
        raw = r.get(f"{_CACHE_PREFIX}:{cache_id}")
        if raw is None:
            _cleanup_stale(r, cache_id)
            continue

        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            _cleanup_stale(r, cache_id)
            continue

        stored_emb = entry.get("embedding", [])
        sim = _cosine_similarity(q_embedding, stored_emb)
        if sim > threshold and sim > best_sim:
            best_sim = sim
            best_entry = entry

    if best_entry is None:
        _log.info(json.dumps({"event": "cache_miss", "reason": "no_match",
                               "question": question[:80],
                               "best_similarity": round(best_sim, 4)}))
        return None

    _log.info(json.dumps({"event": "cache_hit", "question": question[:80],
                           "similarity": round(best_sim, 4)}))
    return {
        "question": best_entry["question"],
        "sql": best_entry["sql"],
        "exec_result": best_entry["exec_result"],
        "similarity": best_sim,
    }


def cache_set_llm(question: str, sql: str, exec_result: dict) -> None:
    """Store a question→SQL→result mapping in the semantic cache (TTL 2h)."""
    r = get_redis()
    if r is None:
        return

    embedder = get_embedder()
    embedding = embedder.encode(question).tolist()

    # Truncate exec_result data to bound cache memory
    cached_result = {**exec_result}
    if cached_result.get("data") and len(cached_result["data"]) > _MAX_CACHED_ROWS:
        cached_result["data"] = cached_result["data"][:_MAX_CACHED_ROWS]
        cached_result["row_count"] = exec_result.get("row_count", 0)  # keep real count

    cache_id = str(uuid.uuid4())
    entry = {
        "question": question,
        "sql": sql,
        "exec_result": cached_result,
        "embedding": embedding,
    }

    r.setex(
        f"{_CACHE_PREFIX}:{cache_id}",
        _CACHE_TTL,
        json.dumps(entry, ensure_ascii=False, default=str),
    )
    r.sadd(_CACHE_KEYS_SET, cache_id)
    _log.info(json.dumps({"event": "cache_store", "question": question[:80],
                           "cache_id": cache_id}))
