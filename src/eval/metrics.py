"""Shared evaluation metrics — EX, VES, gold loading, row normalization.

Used by both scripts/eval.py (CLI) and src/eval/task_manager.py (async API).
"""
import json
import math
import time

from nl2sql.execute import execute_sql


def load_gold(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def normalize_rows(rows: list, columns: list) -> list[tuple]:
    """Sort and round rows for stable comparison."""
    if not rows:
        return []
    normalized = []
    for row in rows:
        norm = []
        for v in row:
            if isinstance(v, (int, float)):
                norm.append(round(float(v), 6))
            elif v is None:
                norm.append("\x00NULL\x00")  # sentinel for stable sorting
            else:
                norm.append(str(v))
        normalized.append(tuple(norm))
    # Sort with string-based key to avoid str-vs-float TypeError on mixed-type columns
    return sorted(normalized, key=lambda t: tuple(str(x) for x in t))


def exec_match(sql_a: str, sql_b: str, database_url: str | None = None,
               gold_cache: dict | None = None) -> dict:
    """Execute two SQLs and compare normalized results.

    Args:
        sql_a: gold SQL
        sql_b: generated SQL
        database_url: SQLAlchemy connection URL
        gold_cache: precomputed gold result dict with keys:
            norm (list[tuple] | None), gold_time_ms, gold_rows

    Returns {ex, detail, gold_time_ms, gen_time_ms, gold_rows, gen_rows}.
    If gold_cache has norm=None (precompute timeout/error), marks as unevaluable.
    """
    if gold_cache is not None:
        norm_a = gold_cache.get("norm")
        if norm_a is not None:
            norm_a = [tuple(row) for row in norm_a]  # JSON deserializes tuples → lists
        gold_time = gold_cache.get("gold_time_ms", 0)
        gold_rows = gold_cache.get("gold_rows", 0)
        if norm_a is None:
            return {"ex": False, "detail": f"gold unavailable: {gold_cache.get('_error', '?')[:80]}",
                    "gold_time_ms": gold_time, "gen_time_ms": 0,
                    "gold_rows": 0, "gen_rows": 0}
    else:
        t0 = time.time()
        r_a = execute_sql(sql_a, database_url=database_url)
        gold_time = (time.time() - t0) * 1000
        if not r_a["success"]:
            return {"ex": False, "detail": f"gold SQL failed: {r_a['error'][:80]}",
                    "gold_time_ms": gold_time, "gen_time_ms": 0,
                    "gold_rows": 0, "gen_rows": 0}
        norm_a = normalize_rows(r_a["data"], r_a["columns"])
        gold_rows = r_a["row_count"]

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
    t0 = time.time()
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(execute_sql, sql_b, database_url=database_url)
        r_b = future.result(timeout=60)
        gen_time = (time.time() - t0) * 1000
    except FutureTimeoutError:
        gen_time = (time.time() - t0) * 1000
        return {"ex": False, "detail": "generated SQL timed out after 60s",
                "gold_time_ms": gold_time, "gen_time_ms": gen_time,
                "gold_rows": gold_rows, "gen_rows": 0}
    except Exception as e:
        gen_time = (time.time() - t0) * 1000
        return {"ex": False, "detail": f"generated SQL error: {str(e)[:80]}",
                "gold_time_ms": gold_time, "gen_time_ms": gen_time,
                "gold_rows": gold_rows, "gen_rows": 0}
    finally:
        pool.shutdown(wait=False)

    if not r_b["success"]:
        return {"ex": False, "detail": f"generated SQL failed: {r_b['error'][:80]}",
                "gold_time_ms": gold_time, "gen_time_ms": gen_time,
                "gold_rows": gold_rows, "gen_rows": 0}

    norm_b = normalize_rows(r_b["data"], r_b["columns"])

    if norm_a == norm_b:
        return {"ex": True, "detail": f"match ({gold_rows} rows)",
                "gold_time_ms": gold_time, "gen_time_ms": gen_time,
                "gold_rows": gold_rows, "gen_rows": r_b["row_count"]}
    else:
        return {"ex": False, "detail": f"mismatch: gold={gold_rows} rows, gen={r_b['row_count']} rows",
                "gold_time_ms": gold_time, "gen_time_ms": gen_time,
                "gold_rows": gold_rows, "gen_rows": r_b["row_count"]}


def ves_score(ex: bool, gold_time_ms: float, gen_time_ms: float) -> float:
    """BIRD VES: ex * sqrt(gold_time / gen_time). 0 if EX=0."""
    if not ex or gold_time_ms <= 0 or gen_time_ms <= 0:
        return 0.0
    return math.sqrt(gold_time_ms / gen_time_ms)
