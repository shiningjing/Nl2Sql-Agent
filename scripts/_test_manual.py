"""Manual BIRD loading + execute_sql, NO src module imports."""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.filterwarnings("ignore")

print("Reading BIRD JSON...")
BIRD_JSON = "data/bird/mini_dev_data/minidev/MINIDEV/mini_dev_sqlite.json"
with open(BIRD_JSON, "r", encoding="utf-8") as f:
    raw = json.load(f)
print(f"  loaded {len(raw)} items")

s701 = None
for item in raw:
    if str(item["question_id"]) == "701":
        db_path = os.path.join("data/bird/mini_dev_data/minidev/MINIDEV/dev_databases",
                               item["db_id"], item["db_id"] + ".sqlite")
        db_url = "sqlite:///" + db_path.replace("\\", "/")
        s701 = {"sql": item["SQL"], "url": db_url}
        break

print(f"URL: {db_url}")
print(f"Path exists: {os.path.exists(db_path)}")
print()

print("Importing execute_sql...")
from tools.sql_executor import execute_sql
print("  done")

gen = "WITH influential_user AS(SELECT Id FROM users ORDER BY Reputation DESC LIMIT 1),user_posts AS(SELECT Score FROM posts WHERE OwnerUserId=(SELECT Id FROM influential_user))SELECT ROUND(100.0*SUM(CASE WHEN Score>50 THEN 1 ELSE 0 END)/COUNT(*),2)AS pct FROM user_posts LIMIT 200"

print("Exec gen_sql...")
t0 = time.time()
r = execute_sql(gen, database_url=db_url)
print(f"  {time.time()-t0:.3f}s ok={r['success']} data={r['data']}")
if not r['success']:
    print(f"  error={(r.get('error') or '')[:200]}")

print("Exec gold_sql...")
t0 = time.time()
r = execute_sql(s701["sql"], database_url=db_url)
print(f"  {time.time()-t0:.3f}s ok={r['success']} data={r['data']}")
if not r['success']:
    print(f"  error={(r.get('error') or '')[:200]}")

print("Done.")
