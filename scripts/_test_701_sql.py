"""Quick test: execute #701 gen_sql and gold_sql via execute_sql."""
import os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings
warnings.filterwarnings("ignore")

from src.eval.bird_loader import get_database_url, load_bird_dev
from nl2sql.execute import execute_sql

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

print("Exec gen_sql...")
t0 = time.time()
r = execute_sql(gen, database_url=url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']} err={r['error']}")

print("Exec gold_sql...")
t0 = time.time()
r = execute_sql(s701.gold_sql, database_url=url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']} err={r['error']}")

print("Done.")
