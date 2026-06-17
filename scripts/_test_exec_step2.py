"""Isolation: load_bird_dev then execute_sql with gen and gold."""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.filterwarnings("ignore")

from evaluation.bird_loader import get_database_url, load_bird_dev

samples = load_bird_dev()
s701 = next(s for s in samples if s.question_id == "701")
url = get_database_url(s701)
print(f"URL: {url}")

gen = (
    "WITH influential_user AS("
    "SELECT Id FROM users ORDER BY Reputation DESC LIMIT 1"
    "),user_posts AS("
    "SELECT Score FROM posts WHERE OwnerUserId=(SELECT Id FROM influential_user)"
    ")SELECT ROUND(100.0*SUM(CASE WHEN Score>50 THEN 1 ELSE 0 END)/COUNT(*),2)AS pct FROM user_posts LIMIT 200"
)

# Only import execute_sql now (after bird_loader is done)
print("Importing execute_sql...")
from tools.sql_executor import execute_sql
print("  done")

print("Exec gen_sql...")
t0 = time.time()
r = execute_sql(gen, database_url=url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']}")

if not r['success']:
    print(f"  error={r['error'][:200]}")

print("Exec gold_sql...")
t0 = time.time()
r = execute_sql(s701.gold_sql, database_url=url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']}")

if not r['success']:
    print(f"  error={r['error'][:200]}")

print("Done.")
