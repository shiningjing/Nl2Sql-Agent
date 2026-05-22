"""Query page — SSE streaming client, result rendering, chat interface."""
import json
import streamlit as st
import pandas as pd

from nl2sql.config import Config

API_BASE = __import__("os").getenv("API_BASE", f"http://127.0.0.1:{Config.API_PORT}")


def _llm_payload() -> dict:
    return {
        "model": st.session_state.get("llm_model") or None,
        "api_key": st.session_state.get("llm_api_key") or None,
        "base_url": st.session_state.get("llm_base_url") or None,
    }


def call_api_stream(question: str, db_id: str, database_url: str) -> dict:
    import httpx

    payload = {
        "question": question,
        "db_id": db_id,
        "database_url": database_url,
        "rag_schema": True,
        "rag_domain": True,
        "multi_candidate": st.session_state.get("multi_candidate", True),
        "rag_k": st.session_state.get("rag_k", 8),
        "rag_column_prune": st.session_state.get("rag_column_prune", False),
        "rag_hybrid": True,
        "rag_fk_expand": st.session_state.get("rag_fk_expand", True),
        "fewshot_enabled": st.session_state.get("fewshot_enabled", True),
        "use_cache": False,
        "llm": _llm_payload(),
    }

    progress = st.empty()
    node_list = []
    sql_preview = ""
    token_buffer = ""

    try:
        with httpx.stream("POST", f"{API_BASE}/api/v1/query/full/stream",
                          json=payload, timeout=180) as resp:
            current_event = None
            for line in resp.iter_lines():
                if line.startswith("event:"):
                    current_event = line[6:].strip()
                elif line.startswith("data:") and current_event:
                    data_str = line[5:].strip()
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    if current_event == "token":
                        token_buffer += data.get("text", "")
                        with progress.container():
                            st.caption(" -> ".join(node_list + ["generating..."]))
                            st.code(token_buffer, language="sql")

                    elif current_event == "node_complete":
                        node = data.get("node", "?")
                        summary = data.get("summary", {})
                        if node == "_post_refiner":
                            pass
                        else:
                            node_list.append(node)
                            with progress.container():
                                st.caption(" -> ".join(node_list))
                                if node == "generator":
                                    sql_preview = summary.get("sql_preview", "")
                                    token_buffer = ""
                                    if sql_preview:
                                        st.code(sql_preview, language="sql")
                                elif node == "guard":
                                    st.caption(f"Guard: {'PASS' if summary.get('guard_pass') else 'FAIL'}")
                                elif node == "voter":
                                    st.caption(f"Voter: {'OK' if summary.get('exec_success') else 'FAIL'} | {summary.get('row_count', 0)} rows")
                                elif node == "semantic_check":
                                    st.caption(f"Semantic: {'PASS' if summary.get('semantic_pass') else 'FAIL'}")

                    elif current_event == "complete":
                        progress.empty()
                        return {
                            "sql": data.get("sql", ""),
                            "exec_result": data.get("exec_result"),
                            "token_usage": data.get("token_usage", {}),
                            "elapsed_ms": 0,
                            "rag_chunks": [],
                            "node_timings": data.get("node_timings", {}),
                            "raw_response": "",
                        }
                    elif current_event == "error":
                        progress.empty()
                        return _error_result(data.get("error", "stream error"))

                    current_event = None

        progress.empty()
        return _error_result("stream ended without completion")
    except Exception as e:
        progress.empty()
        return _error_result(f"Stream error: {e}")


def _error_result(msg: str) -> dict:
    return {"sql": "", "exec_result": {"success": False, "error": msg},
            "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [],
            "node_timings": {}, "raw_response": ""}


# ── Result rendering ──

def render_result(result: dict):
    sql = result.get("sql", "")
    exec_result = result.get("exec_result")
    token_usage = result.get("token_usage", {})
    node_timings = result.get("node_timings", {})
    rag_chunks = result.get("rag_chunks", [])

    tab1, tab2, tab3, tab4 = st.tabs(["SQL", "Results", "Trace", "RAG"])

    with tab1:
        if sql:
            st.code(sql, language="sql")
        else:
            st.error("SQL generation failed.")
            raw = result.get("raw_response", "")
            if raw:
                with st.expander("Raw LLM response"):
                    st.text(raw[:1000])

    with tab2:
        if exec_result and exec_result.get("success"):
            st.success(f"{exec_result.get('row_count', 0)} rows returned")
            data = exec_result.get("data", [])
            cols = exec_result.get("columns", [])
            if data:
                df = pd.DataFrame(data, columns=cols)
                st.dataframe(df, use_container_width=True)
        elif exec_result:
            st.error(f"Execution failed: {exec_result.get('error', 'Unknown')}")

    with tab3:
        c1, c2, c3 = st.columns(3)
        prompt_tok = token_usage.get("prompt", 0)
        comp_tok = token_usage.get("completion", 0)
        total_tok = token_usage.get("total", 0) or (prompt_tok + comp_tok)
        c1.metric("Prompt Tokens", f"{prompt_tok:,}")
        c2.metric("Completion Tokens", f"{comp_tok:,}")
        c3.metric("Total Tokens", f"{total_tok:,}")

        elapsed = result.get("elapsed_ms", 0)
        if elapsed:
            st.metric("Total Time", f"{elapsed/1000:.1f}s" if elapsed > 1000 else f"{elapsed:.0f}ms")

        if node_timings:
            st.caption("Node timings:")
            for node, dur in sorted(node_timings.items(), key=lambda x: -x[1]):
                st.text(f"  {node}: {dur:.2f}s")

    with tab4:
        if rag_chunks:
            schema_count = sum(1 for c in rag_chunks if c.get("chunk_type") == "schema")
            domain_count = len(rag_chunks) - schema_count
            st.caption(f"Schema chunks: {schema_count}, Domain chunks: {domain_count}")
            for c in rag_chunks:
                ctype = c.get("chunk_type", "?")
                src = c.get("source", "?")
                with st.expander(f"[{ctype}] {src}"):
                    st.text(c.get("preview", "")[:500])
        else:
            st.caption("(RAG context not loaded in streaming mode)")


def _snapshot_result(result: dict) -> dict:
    exec_result = result.get("exec_result")
    return {
        "sql": result.get("sql", ""),
        "exec_result": exec_result,
        "token_usage": result.get("token_usage", {}),
        "rag_chunks": [
            {"source": c.get("source", str(c.get("id", "?"))),
             "chunk_type": c.get("metadata", {}).get("chunk_type", "?"),
             "preview": (c.get("content", "") or "")[:200]}
            for c in result.get("rag_chunks", [])
        ],
        "node_timings": result.get("node_timings", {}),
        "raw_response": (result.get("raw_response") or "")[:500] if not result.get("sql") else "",
    }


# ── Query page renderer ──

def get_current_db():
    from src.ui.settings import _get_databases
    db_list = _get_databases()
    db_options = {d.display_name: d for d in db_list}
    name = st.session_state.get("selected_db_name")
    if name and name in db_options:
        return db_options[name]
    first = list(db_options.keys())[0]
    st.session_state.selected_db_name = first
    return db_options[first]


def render_query():
    from datetime import datetime

    # Init session
    if "history" not in st.session_state:
        st.session_state.history = []
    if "selected_idx" not in st.session_state:
        st.session_state.selected_idx = None
    if "next_idx" not in st.session_state:
        st.session_state.next_idx = 0

    db = get_current_db()
    st.title("NL2SQL Agent — BIRD")

    # History browsing
    if st.session_state.selected_idx is not None:
        hist = next((h for h in st.session_state.history if h["idx"] == st.session_state.selected_idx), None)
        if hist:
            st.caption(f"Browsing history — {hist['time']}  [{hist.get('db_name', '?')}]")
            st.chat_message("user").write(hist["question"])
            with st.chat_message("assistant"):
                render_result(hist["result"])
            st.divider()
            if st.button("Back to latest"):
                st.session_state.selected_idx = None
                st.rerun()

    # Chat input
    question = st.chat_input(f"Ask a data question about {db.display_name}...")

    if "pending_question" in st.session_state and st.session_state.pending_question:
        question = st.session_state.pending_question
        st.session_state.pending_question = None

    if question:
        st.session_state.selected_idx = None

        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Running pipeline..."):
                result = call_api_stream(question, db.db_id, db.database_url)

            render_result(result)

            sql = result.get("sql", "")
            exec_result = result.get("exec_result")
            status = "ok" if (exec_result and exec_result.get("success")) else "fail"

            idx = st.session_state.next_idx
            st.session_state.next_idx += 1
            st.session_state.history.append({
                "idx": idx,
                "question": question,
                "sql": sql,
                "result": _snapshot_result(result),
                "time": datetime.now().strftime("%H:%M:%S"),
                "status": status,
                "db_name": st.session_state.selected_db_name,
                "db_id": db.db_id,
            })
