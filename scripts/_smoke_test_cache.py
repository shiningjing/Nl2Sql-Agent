"""Smoke test: verify gold cache works in exec_match."""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.filterwarnings("ignore")

from src.eval.bird_loader import load_bird_dev, get_database_url
from src.eval.metrics import exec_match

samples = load_bird_dev()

# Pick a fast sample (not #701)
s_test = next(s for s in samples if s.question_id == "89")  # financial
db_url = get_database_url(s_test)
cache = json.load(open(
    os.path.join("reports", ".gold_cache", f"{s_test.db_id}.json"),
    "r", encoding="utf-8",
))

# A plausible generated SQL (we don't need a real gen, just testing the comparison)
# For simplicity just use the gold SQL itself → should result in EX match
gen_sql = s_test.gold_sql

print(f"Testing #{s_test.question_id} [{s_test.db_id}]")
print(f"Cache entry: norm={'OK' if cache.get(s_test.question_id, {}).get('norm') else 'MISSING'}")
print()

# Test with cache
print("WITH cache:")
t0 = time.time()
r1 = exec_match(gen_sql, s_test.gold_sql, database_url=db_url,
                gold_cache=cache.get(s_test.question_id))
dt1 = time.time() - t0
print(f"  {dt1:.3f}s EX={r1['ex']} | {r1['detail']}")
print(f"  gold_time_ms={r1['gold_time_ms']:.0f} gen_time_ms={r1['gen_time_ms']:.0f}")

# Test without cache
print()
print("WITHOUT cache:")
t0 = time.time()
r2 = exec_match(gen_sql, s_test.gold_sql, database_url=db_url)
dt2 = time.time() - t0
print(f"  {dt2:.3f}s EX={r2['ex']} | {r2['detail']}")
print(f"  gold_time_ms={r2['gold_time_ms']:.0f} gen_time_ms={r2['gen_time_ms']:.0f}")

# Verify results match
print()
if r1["ex"] == r2["ex"]:
    print("PASS: cache and live results match")
else:
    print("FAIL: mismatch!")

# Also verify #701 cache works (skip live comparison)
s701 = next(s for s in samples if s.question_id == "701")
cache_701 = json.load(open(
    os.path.join("reports", ".gold_cache", f"{s701.db_id}.json"),
    "r", encoding="utf-8",
))
gen_701 = ("WITH influential_user AS(SELECT Id FROM users ORDER BY Reputation DESC LIMIT 1)"
           ",user_posts AS(SELECT Score FROM posts WHERE OwnerUserId=(SELECT Id FROM influential_user))"
           "SELECT ROUND(100.0*SUM(CASE WHEN Score>50 THEN 1 ELSE 0 END)/COUNT(*),2)AS pct FROM user_posts LIMIT 200")

print()
print(f"Testing #701 with cache (gen vs cached gold):")
t0 = time.time()
r701 = exec_match(gen_701, s701.gold_sql, database_url=get_database_url(s701),
                  gold_cache=cache_701.get("701"))
dt = time.time() - t0
print(f"  {dt:.3f}s EX={r701['ex']} | {r701['detail']}")
print(f"  gold_time_ms={r701['gold_time_ms']:.0f} (cached), gen_time_ms={r701['gen_time_ms']:.0f}")

print()
print("All smoke tests done.")
