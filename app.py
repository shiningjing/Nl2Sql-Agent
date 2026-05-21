"""NL2SQL Agent — BIRD Multi-Database Demo (Full Graph only)."""
import os
import json
import streamlit as st
import pandas as pd
from datetime import datetime
from nl2sql.config import Config

API_BASE = os.getenv("API_BASE", f"http://127.0.0.1:{Config.API_PORT}")
USE_STREAMING = True  # always use streaming

st.set_page_config(page_title="NL2SQL Agent — BIRD", page_icon=":bird:", layout="wide")

# ── Custom CSS ──
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 4px 4px 0 0; padding: 8px 16px;
    }
    [data-testid="stMetricValue"] { font-size: 1.2rem; }
</style>
""", unsafe_allow_html=True)

# ── Session state ──
if "history" not in st.session_state:
    st.session_state.history = []
if "selected_idx" not in st.session_state:
    st.session_state.selected_idx = None
if "next_idx" not in st.session_state:
    st.session_state.next_idx = 0


# ── Cached helpers ──

@st.cache_data(ttl=3600)
def _get_databases():
    from nl2sql.db_registry import list_databases
    return list_databases()


@st.cache_data(ttl=3600)
def _get_sample_questions(db_id: str, n: int = 5) -> list[str]:
    from src.eval.bird_loader import load_bird_dev
    samples = load_bird_dev()
    db_questions = [(s.question, s.difficulty) for s in samples if s.db_id == db_id]
    db_questions.sort(key=lambda x: {"simple": 0, "moderate": 1, "challenging": 2}.get(x[1], 9))
    return [q for q, _ in db_questions[:n]]


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


# ── Sidebar ──
with st.sidebar:
    st.header("Database")

    db_list = _get_databases()
    db_options = {d.display_name: d for d in db_list}

    if "selected_db_name" not in st.session_state:
        st.session_state.selected_db_name = list(db_options.keys())[0]

    selected_name = st.selectbox(
        "Select Database",
        options=list(db_options.keys()),
        index=list(db_options.keys()).index(st.session_state.selected_db_name)
        if st.session_state.selected_db_name in db_options else 0,
    )
    st.session_state.selected_db_name = selected_name
    db = db_options[selected_name]

    with st.container(border=True):
        st.caption(f"**Domain:** {db.domain}")
        st.caption(f"**Tables:** {db.table_count}")
        st.caption(f"**ID:** `{db.db_id}`")

    st.divider()

    # ── Sample Questions ──
    st.subheader("Sample Questions")
    try:
        samples = _get_sample_questions(db.db_id, n=5)
        for i, q in enumerate(samples):
            if st.button(f"{q[:80]}{'...' if len(q) > 80 else ''}",
                         key=f"sample_{db.db_id}_{i}", use_container_width=True):
                st.session_state.pending_question = q
                st.rerun()
    except Exception:
        st.caption("(sample questions unavailable)")

    st.divider()

    # ── Pipeline Config ──
    st.subheader("Pipeline")

    multi_candidate = st.toggle("Multi-Candidate", value=True,
                                help="Generate SQL with 3 temperatures, vote by execution result")
    rag_k = st.slider("RAG chunks", 2, 20, 8,
                      help="Number of schema + domain chunks to retrieve")
    rag_column_prune = st.toggle("Column Prune", value=False,
                                 help="Drop low-relevance columns from DDL to reduce prompt size")
    fewshot_enabled = st.toggle("Few-Shot", value=True,
                                help="Retrieve similar Q-SQL pairs from the same database")
    rag_fk_expand = st.toggle("FK Expand", value=True,
                              help="Include 1-hop foreign-key neighbor tables in schema")

    st.divider()

    st.caption(f"LLM: {Config.LLM_CHAT_MODEL} | Streaming: {'ON' if USE_STREAMING else 'OFF'}")

    # ── History ──
    st.divider()
    st.subheader("History")

    if st.button("Clear History", use_container_width=True):
        st.session_state.history = []
        st.session_state.selected_idx = None
        st.session_state.next_idx = 0
        st.rerun()

    if st.session_state.history:
        for h in reversed(st.session_state.history):
            icon = ":white_check_mark:" if h["status"] == "ok" else ":x:"
            db_tag = f"[{h.get('db_name', '?')}]"
            label = f"{icon} {db_tag} {h['question'][:35]}{'...' if len(h['question']) > 35 else ''}"
            if st.button(label, key=f"hist_{h['idx']}", use_container_width=True):
                st.session_state.selected_idx = h["idx"]
                st.rerun()

        if st.session_state.selected_idx is not None:
            sel = next((h for h in st.session_state.history if h["idx"] == st.session_state.selected_idx), None)
            if sel:
                st.caption(f"Viewing: [{sel.get('db_name','?')}] {sel['question'][:50]}")
    else:
        st.caption("No queries yet.")


# ── SSE streaming client ──

def _call_api_stream(question: str, db_id: str, database_url: str) -> dict:
    import httpx
    payload = {
        "question": question,
        "db_id": db_id,
        "database_url": database_url,
        "rag_schema": True,
        "rag_domain": True,
        "multi_candidate": multi_candidate,
        "rag_k": rag_k,
        "rag_column_prune": rag_column_prune,
        "rag_hybrid": True,
        "rag_fk_expand": rag_fk_expand,
        "fewshot_enabled": fewshot_enabled,
        "use_cache": False,
    }

    progress = st.empty()
    node_list = []
    sql_preview = ""
    token_buffer = ""  # accumulates streaming tokens for live display

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
                            st.caption(" → ".join(node_list + ["generating..."]))
                            st.code(token_buffer, language="sql")

                    elif current_event == "node_complete":
                        node = data.get("node", "?")
                        summary = data.get("summary", {})
                        if node == "_post_refiner":
                            pass  # skip internal counter node
                        else:
                            node_list.append(node)
                            with progress.container():
                                st.caption(" → ".join(node_list))
                                if node == "generator":
                                    sql_preview = summary.get("sql_preview", "")
                                    token_buffer = ""  # reset token buffer; show extracted SQL
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
                        exec_result = data.get("exec_result")
                        return {
                            "sql": data.get("sql", ""),
                            "exec_result": exec_result,
                            "token_usage": data.get("token_usage", {}),
                            "elapsed_ms": 0,
                            "rag_chunks": [],
                            "node_timings": data.get("node_timings", {}),
                            "raw_response": "",
                        }
                    elif current_event == "error":
                        progress.empty()
                        return {"sql": "", "exec_result": {"success": False, "error": data.get("error", "stream error")},
                                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": ""}

                    current_event = None

        progress.empty()
        return {"sql": sql_preview, "exec_result": {"success": False, "error": "stream ended without completion"},
                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": ""}
    except Exception as e:
        progress.empty()
        return {"sql": "", "exec_result": {"success": False, "error": f"Stream error: {e}"},
                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": str(e)}


# ── Result rendering ──

def render_result(result: dict, question: str):
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
            st.caption("(RAG context not loaded in streaming mode — open a non-streaming query to see)")


# ── Main area ──
st.title("NL2SQL Agent — BIRD")

# History browsing
if st.session_state.selected_idx is not None:
    hist = next((h for h in st.session_state.history if h["idx"] == st.session_state.selected_idx), None)
    if hist:
        st.caption(f"Browsing history — {hist['time']}  [{hist.get('db_name', '?')}]")
        st.chat_message("user").write(hist["question"])
        with st.chat_message("assistant"):
            render_result(hist["result"], hist["question"])
        st.divider()
        if st.button("Back to latest"):
            st.session_state.selected_idx = None
            st.rerun()

# Chat input
question = st.chat_input("Ask a data question about the selected database...")

# Handle pending question (from sample click)
if "pending_question" in st.session_state and st.session_state.pending_question:
    question = st.session_state.pending_question
    st.session_state.pending_question = None

if question:
    st.session_state.selected_idx = None

    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Running pipeline..."):
            result = _call_api_stream(question, db.db_id, db.database_url)

        render_result(result, question)

        # Save to history
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
            "db_name": selected_name,
            "db_id": db.db_id,
        })
