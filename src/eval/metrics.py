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


def extract_tables_from_rag_chunks(rag_chunks: list[dict]) -> set[str]:
    """Extract unique table names from RAG chunk metadata."""
    tables = set()
    for c in rag_chunks:
        if isinstance(c.get("metadata"), dict):
            tn = c["metadata"].get("table_name", "")
            if tn:
                tables.add(tn.lower())
    return tables


def compute_rag_recall(rag_chunks: list[dict], gold_sql: str,
                       dialect: str = "sqlite") -> dict | None:
    """Compute RAG table recall = |tables_in_gold ∩ tables_in_rag| / |tables_in_gold|.

    Returns None if RAG is off (empty chunks) or gold SQL has no tables.
    """
    from src.guardrails.ast_validator import extract_table_names

    if not rag_chunks:
        return None

    gold_tables = extract_table_names(gold_sql, dialect=dialect)
    if not gold_tables:
        return None

    rag_tables = extract_tables_from_rag_chunks(rag_chunks)
    hit = gold_tables & rag_tables
    missed = gold_tables - rag_tables

    return {
        "recall": round(len(hit) / len(gold_tables), 4),
        "gold_tables": sorted(gold_tables),
        "retrieved_tables": sorted(rag_tables),
        "hit_tables": sorted(hit),
        "missed_tables": sorted(missed),
    }


def ves_score(ex: bool, gold_time_ms: float, gen_time_ms: float) -> float:
    """BIRD VES: ex * sqrt(gold_time / gen_time). 0 if EX=0."""
    if not ex or gold_time_ms <= 0 or gen_time_ms <= 0:
        return 0.0
    return math.sqrt(gold_time_ms / gen_time_ms)
