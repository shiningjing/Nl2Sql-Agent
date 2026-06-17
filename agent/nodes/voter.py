"""Voter node — execute SQL candidates in parallel, with LLM vote as timeout/offline fallback."""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from agent.nodes.executor import run_sql
from agent.state import AgentState

MAX_VOTER_WORKERS = 3


def _normalize(rows: list, columns: list) -> str:
    """Sort + round rows into a stable hashable representation."""
    if not rows:
        return "EMPTY"
    norm = []
    for row in rows:
        vals = []
        for v in row:
            if isinstance(v, (int, float)):
                vals.append(round(float(v), 6))
            elif v is None:
                vals.append("\x00NULL\x00")
            else:
                vals.append(str(v))
        norm.append(tuple(vals))
    return str(sorted(norm))


def _llm_vote(question: str, schema_text: str, candidates: list[str], tlog=None) -> str | None:
    """Ask LLM to pick the best SQL candidate. Returns winning SQL or None."""
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    from langchain_core.messages import SystemMessage, HumanMessage
    from storage.config import Config
    import re

    parts = [f"## QUESTION\n{question}"]
    if schema_text:
        # Keep schema concise — first 3000 chars covers relevant tables
        parts.append(f"## SCHEMA\n{schema_text[:3000]}")
    parts.append("## CANDIDATES")
    for i, sql in enumerate(candidates):
        parts.append(f"### Candidate {i}\n```sql\n{sql}\n```")
    parts.append(
        "\nCompare the candidates. Return ONLY the integer index of the best SQL. "
        "Best = logically correct, answers the question precisely, handles edge cases. "
        "Reply with a single digit."
    )

    if tlog:
        tlog.node_enter("voter_llm", {"candidate_count": len(candidates)})

    from agent.llm_factory import get_llm

    chat = get_llm(temperature=0, max_tokens=10, request_timeout=15, max_retries=0)

    try:
        _t0 = time.time()
        response = chat.invoke([
            SystemMessage(content="You are a SQL reviewer. Compare candidates and pick the best one. Reply with a single integer."),
            HumanMessage(content="\n\n".join(parts)),
        ])
        _duration = round(time.time() - _t0, 3)
        tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
        if tlog:
            tlog.llm_call(Config.LLM_CHAT_MODEL, tu, _duration, node="voter")
            tlog.node_exit("voter_llm", {"raw": response.content.strip()[:60]})
        match = re.search(r"\d+", response.content)
        if match:
            idx = int(match.group())
            if 0 <= idx < len(candidates):
                return candidates[idx]
    except Exception as e:
        if tlog:
            tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300], node="voter")
            tlog.node_exit("voter_llm", {"error": str(e)[:120]})

    return candidates[0]  # fallback to first (lowest temp)


def voter_node(state: AgentState) -> dict:
    """Execute candidates in parallel, vote by result hash. LLM vote as timeout/offline fallback."""
    t0 = time.time()
    sql = state.get("sql", "")
    candidate_sqls = state.get("candidate_sqls", [])
    sqls = candidate_sqls if candidate_sqls else [sql]
    database_url = state.get("database_url")
    schema_text = state.get("schema_text", "")
    question = state.get("question", "")

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("voter", {"candidate_count": len(sqls), "has_db": bool(database_url)})

    # ── No database → LLM vote ──
    if not database_url:
        winner = _llm_vote(question, schema_text, sqls, tlog)
        node_latency = dict(state.get("node_latency", {}))
        node_latency["voter"] = round(time.time() - t0, 3)
        return {
            "sql": winner or sql,
            "exec_result": {"success": True, "error": "", "data": [],
                            "columns": [], "row_count": 0, "_voted_by": "llm_nodb"},
            "node_latency": node_latency,
        }

    # ── Execute candidates (single or parallel) ──
    results: list[dict] = []  # {sql, result, hash, success}

    if len(sqls) <= 1:
        for s in sqls:
            r = run_sql(s, database_url=database_url)
            results.append({
                "sql": s,
                "success": r["success"],
                "result": r,
                "hash": _normalize(r.get("data", []), r.get("columns", [])) if r["success"] else None,
            })
            if tlog:
                tlog.sql_exec(r["success"], r.get("row_count", 0),
                               r.get("_elapsed_ms", 0) / 1000, r.get("error", ""))
    else:
        with ThreadPoolExecutor(max_workers=min(MAX_VOTER_WORKERS, len(sqls))) as pool:
            fut_map = {pool.submit(run_sql, s, database_url=database_url): s for s in sqls}
            for fut in as_completed(fut_map):
                s = fut_map[fut]
                try:
                    r = fut.result(timeout=0)
                except Exception:
                    r = {"success": False, "error": "voter future error",
                         "data": None, "columns": None, "row_count": 0}
                results.append({
                    "sql": s,
                    "success": r["success"],
                    "result": r,
                    "hash": _normalize(r.get("data", []), r.get("columns", [])) if r["success"] else None,
                })
                if tlog:
                    tlog.sql_exec(r["success"], r.get("row_count", 0),
                                   r.get("_elapsed_ms", 0) / 1000, r.get("error", ""))
        results.sort(key=lambda r: sqls.index(r["sql"]))

    successful = [r for r in results if r["success"]]

    # ── All timed out / failed → LLM vote fallback ──
    if not successful:
        reason = "timeout" if any("timed out" in r["result"].get("error", "") for r in results) else "exec_fail"
        if tlog:
            tlog.node_exit("voter", {"winner": "llm_fallback", "reason": reason})
        winner = _llm_vote(question, schema_text, sqls, tlog)
        node_latency = dict(state.get("node_latency", {}))
        node_latency["voter"] = round(time.time() - t0, 3)
        return {
            "sql": winner or sql,
            "exec_result": {"success": True, "error": "", "data": [],
                            "columns": [], "row_count": 0, "_voted_by": f"llm_{reason}"},
            "node_latency": node_latency,
        }

    # Single candidate → no vote needed
    if len(successful) == 1:
        if tlog:
            tlog.node_exit("voter", {"winner": "single", "successful_count": 1})
        node_latency = dict(state.get("node_latency", {}))
        node_latency["voter"] = round(time.time() - t0, 3)
        return {
            "sql": successful[0]["sql"],
            "exec_result": {**successful[0]["result"], "_sql": successful[0]["sql"]},
            "node_latency": node_latency,
        }

    # Vote: group by result hash
    from collections import Counter
    hash_counts = Counter(r["hash"] for r in successful)
    most_common = hash_counts.most_common()

    if most_common[0][1] >= 2:
        winner_hash = most_common[0][0]
        for r in successful:
            if r["hash"] == winner_hash:
                if tlog:
                    tlog.node_exit("voter", {"winner": "majority", "successful_count": len(successful),
                                             "hash_count": most_common[0][1]})
                node_latency = dict(state.get("node_latency", {}))
                node_latency["voter"] = round(time.time() - t0, 3)
                return {
                    "sql": r["sql"],
                    "exec_result": {**r["result"], "_sql": r["sql"]},
                    "node_latency": node_latency,
                }
    else:
        best = min(successful, key=lambda r: r["result"].get("row_count", float("inf")))
        if tlog:
            tlog.node_exit("voter", {"winner": "tiebreak", "successful_count": len(successful)})
        node_latency = dict(state.get("node_latency", {}))
        node_latency["voter"] = round(time.time() - t0, 3)
        return {
            "sql": best["sql"],
            "exec_result": {**best["result"], "_sql": best["sql"]},
            "node_latency": node_latency,
        }

    return {}  # unreachable
