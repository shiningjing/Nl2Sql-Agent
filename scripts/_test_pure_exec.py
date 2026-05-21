"""Pure isolation: NO src module imports. Parse BIRD JSON manually, test execute_sql."""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.filterwarnings("ignore")

# ── Manual BIRD loading (no src.eval imports) ──
BIRD_JSON = "data/bird/mini_dev_data/minidev/MINIDEV/mini_dev_sqlite.json"
DB_DIR = "data/bird/mini_dev_data/minidev/MINIDEV/dev_databases"

with open(BIRD_JSON, "r", encoding="utf-8") as f:
    raw = json.load(f)

s701 = None
for item in raw:
    if str(item["question_id"]) == "701":
        db_id = item["db_id"]
        db_path = os.path.join(DB_DIR, db_id, f"{db_id}.sqlite")
        # Convert backslash to forward slash for SQLAlchemy URL
        db_url = "sqlite:///" + db_path.replace("\\", "/")
        s701 = {
            "question": item["question"],
            "gold_sql": item["SQL"],
            "db_url": db_url,
        }
        break

print(f"DB path: {db_path}")
print(f"DB URL:  {db_url}")
print(f"DB exists: {os.path.exists(db_path)}")
print(f"Question: {s701['question'][:100]}")
print()

# ── Now import and test execute_sql ──
print("Importing execute_sql...")
from nl2sql.execute import execute_sql
print("  done")
print()

gen = (
    "WITH influential_user AS("
    "SELECT Id FROM users ORDER BY Reputation DESC LIMIT 1"
    "),user_posts AS("
    "SELECT Score FROM posts WHERE OwnerUserId=(SELECT Id FROM influential_user)"
    ")SELECT ROUND(100.0*SUM(CASE WHEN Score>50 THEN 1 ELSE 0 END)/COUNT(*),2)AS pct FROM user_posts LIMIT 200"
)

print("Exec gen_sql...")
t0 = time.time()
r = execute_sql(gen, database_url=db_url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']}")
if not r['success']:
    print(f"  error={(r.get('error') or '')[:200]}")

print("Exec gold_sql...")
t0 = time.time()
r = execute_sql(s701["gold_sql"], database_url=db_url)
dt = time.time() - t0
print(f"  {dt:.3f}s ok={r['success']} data={r['data']}")
if not r['success']:
    print(f"  error={(r.get('error') or '')[:200]}")

print("Done.")
