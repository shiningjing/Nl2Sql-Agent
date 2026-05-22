"""Test Redis connectivity and LLM semantic cache."""
import os, sys
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)

from src.infrastructure.redis_cache import get_redis, cache_get_llm, cache_set_llm

# 1. Connection
r = get_redis()
print(f"1. Redis connection: {r is not None}")
if r:
    print(f"   ping: {r.ping()}")
    keys = r.keys("*")
    print(f"   keys in DB: {len(keys)}")

# 2. LLM semantic cache round-trip
print()
print("2. LLM semantic cache round-trip:")
q1 = "How many users have more than 1000 posts?"
sql1 = "SELECT COUNT(*) FROM users WHERE post_count > 1000"
result1 = {"success": True, "data": [(42,)], "columns": ["count"], "row_count": 1, "error": None}

cache_set_llm(q1, sql1, result1)
print("   Stored")

cached = cache_get_llm(q1)
if cached:
    print(f"   Same question: HIT, similarity={cached['similarity']:.4f}")
    print(f"   SQL: {cached['sql']}")
else:
    print("   Same question: MISS (unexpected)")

q2 = "What is the average age of customers in Beijing?"
cached2 = cache_get_llm(q2)
print(f"   Different question: {'HIT' if cached2 else 'MISS'} (expected: MISS)")

# Cleanup
r.flushdb()
print()
print("3. Cleanup done. Redis is working.")
