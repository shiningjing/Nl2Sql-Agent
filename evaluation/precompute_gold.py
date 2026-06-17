"""Pre-compute gold SQL results for all BIRD dev samples.

Usage:
  python scripts/_precompute_gold.py [--timeout 30] [--workers 4]

Output: reports/.gold_cache/<db_id>.json
  {question_id: {norm: list[tuple], gold_time_ms: float, gold_rows: int}}
"""

import argparse, json, os, sys, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.bird_loader import load_bird_dev, get_database_url
from evaluation.metrics import normalize_rows
from tools.sql_executor import execute_sql


def run_gold(sample, timeout_s: int) -> dict:
    """Execute gold SQL and return normalized result, or error on timeout."""
    database_url = get_database_url(sample)

    def _exec():
        t0 = time.time()
        r = execute_sql(sample.gold_sql, database_url=database_url)
        gold_time = (time.time() - t0) * 1000
        if not r["success"]:
            # Fallback: raw sqlite3 for SQLs with LIKE patterns that SQLAlchemy
            # misparses as bind params (e.g., '_:%:__.___')
            err = r["error"]
            if "bind parameter" in err.lower():
                import sqlite3
                db_path = database_url.replace("sqlite:///", "")
                # Convert forward slashes back to OS-native on Windows
                if os.name == "nt":
                    db_path = db_path.replace("/", "\\")
                t0b = time.time()
                try:
                    conn = sqlite3.connect(db_path)
                    cur = conn.execute(sample.gold_sql)
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description] if cur.description else []
                    conn.close()
                    gold_time = (time.time() - t0b) * 1000
                    norm = normalize_rows(rows, cols)
                    return {"status": "ok", "norm": norm, "gold_time_ms": gold_time,
                            "gold_rows": len(rows)}
                except Exception as e2:
                    return {"status": "error", "error": f"{err[:80]}; sqlite3 fallback: {e2}"[:200],
                            "gold_time_ms": gold_time, "gold_rows": 0}
            return {"status": "error", "error": err[:200],
                    "gold_time_ms": gold_time, "gold_rows": 0}
        norm = normalize_rows(r["data"], r["columns"])
        return {"status": "ok", "norm": norm, "gold_time_ms": gold_time,
                "gold_rows": r["row_count"]}

    pool = ThreadPoolExecutor(max_workers=1)
    try:
        fut = pool.submit(_exec)
        return fut.result(timeout=timeout_s)
    except FutureTimeoutError:
        return {"status": "timeout", "error": f"gold SQL timeout ({timeout_s}s)",
                "gold_time_ms": timeout_s * 1000, "gold_rows": 0}
    finally:
        pool.shutdown(wait=False)  # don't block on stuck threads


def main():
    parser = argparse.ArgumentParser(description="Precompute BIRD gold SQL results")
    parser.add_argument("--timeout", type=int, default=30,
                        help="Per-sample gold SQL timeout in seconds (default: 30)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel workers (default: 4)")
    args = parser.parse_args()

    samples = load_bird_dev()
    by_db: dict[str, list] = defaultdict(list)
    for s in samples:
        by_db[s.db_id].append(s)

    cache_dir = os.path.join("reports", ".gold_cache")
    os.makedirs(cache_dir, exist_ok=True)

    total = len(samples)
    ok = 0
    errors = 0
    timeouts = 0

    for db_id, db_samples in sorted(by_db.items()):
        cache_path = os.path.join(cache_dir, f"{db_id}.json")

        # Load existing cache if any
        cache = {}
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            already = set(cache.keys()) & {s.question_id for s in db_samples}
            if already:
                print(f"[{db_id}] {len(already)} already cached, skipping")
                # Don't count already cached in totals; we continue to compute the rest
                already_cached = len(already)

        print(f"[{db_id}] {len(db_samples)} samples with {args.workers} workers...")

        pending = [s for s in db_samples if s.question_id not in cache]
        if not pending:
            continue

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            fut_map = {pool.submit(run_gold, s, args.timeout): s for s in pending}
            for fut in as_completed(fut_map):
                s = fut_map[fut]
                try:
                    result = fut.result(timeout=args.timeout + 5)
                except FutureTimeoutError:
                    result = {"status": "timeout", "error": "worker timeout",
                              "gold_time_ms": (args.timeout + 5) * 1000, "gold_rows": 0}

                if result["status"] == "ok":
                    cache[s.question_id] = {
                        "norm": result["norm"],
                        "gold_time_ms": result["gold_time_ms"],
                        "gold_rows": result["gold_rows"],
                    }
                    ok += 1
                    print(f"  #{s.question_id} [{s.difficulty}] OK "
                          f"({result['gold_time_ms']:.0f}ms, {result['gold_rows']} rows)")
                else:
                    status = result["status"]
                    err = result.get("error", "")[:80]
                    if status == "timeout":
                        timeouts += 1
                        # Cache the timeout as well so we don't retry every time
                        cache[s.question_id] = {
                            "norm": None,
                            "gold_time_ms": result["gold_time_ms"],
                            "gold_rows": 0,
                            "_timeout": True,
                            "_error": err,
                        }
                    else:
                        errors += 1
                        cache[s.question_id] = {
                            "norm": None,
                            "gold_time_ms": result["gold_time_ms"],
                            "gold_rows": 0,
                            "_error": err,
                        }
                    print(f"  #{s.question_id} [{s.difficulty}] {status.upper()}: {err}")

        # Save after each DB
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, default=str)
        print(f"  → Saved {len(cache)} entries to {cache_path}")

    print()
    print(f"Total: {total} | OK: {ok} | Timeout: {timeouts} | Error: {errors}")
    print(f"Cache: {cache_dir}/")


if __name__ == "__main__":
    main()
