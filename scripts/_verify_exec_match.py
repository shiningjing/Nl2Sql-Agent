"""Larger-scale exec_match verification: same-SQL and wrong-SQL on 100 random samples."""
import sys, os, json, random, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()

from src.eval.bird_loader import load_bird_dev, get_database_url
from src.eval.metrics import exec_match

random.seed(42)
samples = load_bird_dev()
random.shuffle(samples)
test_samples = samples[:100]

cache_dir = os.path.join("reports", ".gold_cache")
gold_cache_map = {}
for fname in os.listdir(cache_dir):
    if fname.endswith(".json"):
        with open(os.path.join(cache_dir, fname), encoding="utf-8") as f:
            gold_cache_map.update(json.load(f))

same_ok = 0; same_fail = 0; same_timeout = 0; same_error = 0
wrong_ok = 0; wrong_fail = 0; wrong_timeout = 0; wrong_error = 0
false_pass = []  # wrong SQL passed EX
false_fail = []  # same SQL failed EX
timeouts = []    # timeouts during exec_match

for i, s in enumerate(test_samples):
    qid = s.question_id
    db_url = get_database_url(s)
    gold_entry = gold_cache_map.get(qid)

    # Skip if gold cache unavailable
    if gold_entry is None:
        continue

    # --- Same-SQL test ---
    r_same = exec_match(s.gold_sql, s.gold_sql, database_url=db_url, gold_cache=gold_entry)
    if "timed out" in r_same["detail"]:
        same_timeout += 1
        timeouts.append((qid, "same", r_same["detail"]))
    elif r_same["ex"]:
        same_ok += 1
    else:
        same_fail += 1
        false_fail.append((qid, r_same["detail"]))

    # --- Wrong-SQL test (use a trivial wrong query) ---
    # Pick a wrong SQL that should definitely not match
    wrong_sql = "SELECT 1 AS x"
    r_wrong = exec_match(s.gold_sql, wrong_sql, database_url=db_url, gold_cache=gold_entry)
    if "timed out" in r_wrong["detail"]:
        wrong_timeout += 1
    elif r_wrong["ex"]:
        wrong_ok += 1
        false_pass.append((qid, s.db_id, r_wrong["detail"]))
    elif "gold unavailable" in r_wrong["detail"] or "gold SQL failed" in r_wrong["detail"]:
        wrong_error += 1
    else:
        wrong_fail += 1

    if (i + 1) % 20 == 0:
        print(f"  ... {i+1}/{len(test_samples)} done", flush=True)

print()
print(f"=== exec_match verification ({len(test_samples)} samples) ===")
print(f"Same-SQL:  OK={same_ok}  FAIL={same_fail}  TIMEOUT={same_timeout}  (expect all OK except slow gold SQL)")
print(f"Wrong-SQL: OK={wrong_ok}  FAIL={wrong_fail}  TIMEOUT={wrong_timeout}  ERROR={wrong_error}  (expect all FAIL except coincidental matches)")

if false_pass:
    print(f"\nFalse PASS ({len(false_pass)}):")
    for qid, db_id, detail in false_pass:
        print(f"  #{qid} [{db_id}]: {detail}")

if false_fail:
    print(f"\nFalse FAIL ({len(false_fail)}):")
    for qid, detail in false_fail:
        print(f"  #{qid}: {detail}")

if timeouts:
    print(f"\nTimeouts ({len(timeouts)}):")
    for qid, phase, detail in timeouts:
        print(f"  #{qid} [{phase}]: {detail}")
