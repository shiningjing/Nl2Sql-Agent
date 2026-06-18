"""NL2SQL Agent — BIRD Multi-Database Demo (Full Graph only)."""
import os
import json
import streamlit as st
import pandas as pd
from datetime import datetime
from storage.config import Config

API_BASE = os.getenv("API_BASE", f"http://127.0.0.1:{Config.API_PORT}")
USE_STREAMING = True

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
if "page" not in st.session_state:
    st.session_state.page = "query"

# Settings defaults
if "selected_db_name" not in st.session_state:
    st.session_state.selected_db_name = None
if "multi_candidate" not in st.session_state:
    st.session_state.multi_candidate = True
if "rag_k" not in st.session_state:
    st.session_state.rag_k = 8
if "rag_column_prune" not in st.session_state:
    st.session_state.rag_column_prune = False
if "fewshot_enabled" not in st.session_state:
    st.session_state.fewshot_enabled = True
if "rag_fk_expand" not in st.session_state:
    st.session_state.rag_fk_expand = True
if "llm_model" not in st.session_state:
    st.session_state.llm_model = Config.LLM_CHAT_MODEL
if "llm_base_url" not in st.session_state:
    st.session_state.llm_base_url = Config.LLM_BASE_URL
if "llm_api_key" not in st.session_state:
    st.session_state.llm_api_key = Config.LLM_API_KEY


# ── Cached helpers ──

@st.cache_data(ttl=3600)
def _get_databases():
    from storage.db_registry import list_databases
    return list_databases()


@st.cache_data(ttl=3600)
def _load_bird_json():
    import json, os
    root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "bird", "mini_dev_data", "minidev", "MINIDEV")
    with open(os.path.join(root, "mini_dev_sqlite.json"), "r", encoding="utf-8") as f:
        return json.load(f)


def _get_sample_questions(db_id: str, n: int = 5) -> list[str]:
    items = [it for it in _load_bird_json() if it["db_id"] == db_id]
    items.sort(key=lambda x: {"simple": 0, "moderate": 1, "challenging": 2}.get(x.get("difficulty"), 9))
    return [it["question"] for it in items[:n]]


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


# ── Async task SSE streaming client ──

def _call_api_async(question: str, db_id: str, database_url: str) -> dict:
    """Submit async task → stream SSE from /task/{id}/stream."""
    import httpx

    llm_payload = {
        "question": question,
        "db_id": db_id,
        "database_url": database_url,
        "rag_schema": True,
        "rag_domain": True,
        "multi_candidate": st.session_state.multi_candidate,
        "rag_k": st.session_state.rag_k,
        "rag_column_prune": st.session_state.rag_column_prune,
        "rag_hybrid": True,
        "rag_fk_expand": st.session_state.rag_fk_expand,
        "fewshot_enabled": st.session_state.fewshot_enabled,
        "llm": {
            "model": st.session_state.llm_model or None,
            "api_key": st.session_state.llm_api_key or None,
            "base_url": st.session_state.llm_base_url or None,
        },
    }

    # Step 1: submit task
    try:
        submit_resp = httpx.post(f"{API_BASE}/api/v1/task/submit", json=llm_payload, timeout=10)
        if submit_resp.status_code != 202:
            return {"sql": "", "exec_result": {"success": False, "error": f"Task submission failed ({submit_resp.status_code})"},
                    "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": ""}
        task_id = submit_resp.json()["task_id"]
    except Exception as e:
        return {"sql": "", "exec_result": {"success": False, "error": f"Task submission error: {e}"},
                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": str(e)}

    # Step 2: stream SSE
    progress = st.empty()
    node_list = []
    sql_preview = ""
    token_buffer = ""
    seen_nodes = set()

    try:
        with httpx.stream("GET", f"{API_BASE}/api/v1/task/{task_id}/stream", timeout=360) as resp:
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

                    elif current_event == "node_done":
                        node = data.get("node", "?")
                        if node not in seen_nodes:
                            seen_nodes.add(node)
                            node_list.append(node)
                            with progress.container():
                                st.caption(" -> ".join(node_list))

                    elif current_event == "status":
                        sql_preview = data.get("sql_preview", "") or sql_preview
                        node = data.get("node", "")
                        if node and node not in seen_nodes:
                            seen_nodes.add(node)
                            node_list.append(node)
                        if data.get("status") in ("RUNNING",) and not token_buffer:
                            pass  # waiting for LLM to start streaming

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

                    elif current_event in ("error", "timeout"):
                        progress.empty()
                        return {"sql": sql_preview, "exec_result": {"success": False, "error": data.get("error", "task failed")},
                                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": ""}

                    current_event = None

        progress.empty()
        return {"sql": sql_preview, "exec_result": {"success": False, "error": "stream ended without completion"},
                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": ""}
    except Exception as e:
        progress.empty()
        return {"sql": "", "exec_result": {"success": False, "error": f"Stream error: {e}"},
                "token_usage": {}, "elapsed_ms": 0, "rag_chunks": [], "node_timings": {}, "raw_response": str(e)}


# ── Waterfall chart ──

def _render_waterfall(node_timings: dict):
    """Interactive Plotly waterfall chart of pipeline node timings."""
    import plotly.graph_objects as go

    order = ["router", "schema_retriever", "decomposer", "fewshot_selector",
             "generator", "guard", "voter", "semantic_check", "executor", "refiner"]
    sorted_nodes = [(n, node_timings.get(n, 0.0)) for n in order if n in node_timings]
    for n, d in node_timings.items():
        if n not in order:
            sorted_nodes.append((n, d))

    if not sorted_nodes:
        return

    labels = [n for n, _ in sorted_nodes]
    values = [d for _, d in sorted_nodes]
    total = sum(values)

    # Color coding
    color_map = {"generator": "#FF9800", "voter": "#4CAF50",
                 "refiner": "#E91E63", "router": "#607D8B"}
    bar_colors = [color_map.get(n, "#2196F3") for n in labels]

    # Build waterfall via go.Bar with per-bar color (Waterfall trace lacks marker.color)
    hover_texts = [
        f"<b>{n}</b><br>Duration: {v:.2f}s<br>Share: {v/total*100:.1f}%<br>Cumulative: {sum(values[:i+1]):.2f}s"
        for i, (n, v) in enumerate(zip(labels, values))
    ]
    base = [sum(values[:i]) for i in range(len(values))]

    fig = go.Figure(go.Bar(
        name="Pipeline",
        x=labels,
        y=values,
        base=base,
        text=[f"{v:.2f}s" for v in values],
        textposition="outside",
        hovertext=hover_texts,
        hoverinfo="text",
        marker={"color": bar_colors, "line": {"color": "#fff", "width": 1}},
    ))

    fig.update_layout(
        title={"text": "Pipeline Node Timings (Waterfall)", "font": {"size": 16}},
        xaxis_title="Node",
        yaxis_title="Seconds",
        showlegend=False,
        height=400,
        margin={"t": 40, "b": 60, "l": 60, "r": 20},
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)


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
            _render_waterfall(node_timings)

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


# ═══════════════════════════════════════════════════════════════════════════════
# Settings page
# ═══════════════════════════════════════════════════════════════════════════════

def render_settings():
    st.title("Settings")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Pipeline")
        st.session_state.multi_candidate = st.toggle(
            "Multi-Candidate", value=st.session_state.multi_candidate,
            help="Generate SQL with 3 temperatures, vote by execution result")
        st.session_state.rag_k = st.slider(
            "RAG chunks", 2, 20, st.session_state.rag_k,
            help="Number of schema + domain chunks to retrieve")
        st.session_state.rag_column_prune = st.toggle(
            "Column Prune", value=st.session_state.rag_column_prune,
            help="Drop low-relevance columns from DDL to reduce prompt size")
        st.session_state.fewshot_enabled = st.toggle(
            "Few-Shot", value=st.session_state.fewshot_enabled,
            help="Retrieve similar Q-SQL pairs from the same database")
        st.session_state.rag_fk_expand = st.toggle(
            "FK Expand", value=st.session_state.rag_fk_expand,
            help="Include 1-hop foreign-key neighbor tables in schema")

    with col2:
        st.subheader("LLM Configuration")

        # Load preset list from Config
        preset_names = [p["name"] for p in Config.LLM_PRESETS]

        # Detect which preset matches current model + base_url
        current_model = st.session_state.llm_model
        current_base = st.session_state.llm_base_url
        default_idx = len(preset_names) - 1  # "Custom"
        for i, p in enumerate(Config.LLM_PRESETS):
            if p["name"] != "Custom" and p["model"] == current_model and p["base_url"] == current_base:
                default_idx = i
                break

        # Track previous preset to only auto-fill on actual change
        if "llm_preset" not in st.session_state:
            st.session_state.llm_preset = preset_names[default_idx]

        selected_preset = st.selectbox(
            "Provider", options=preset_names, index=default_idx,
            help="切换提供商自动填充模型名、API 地址和密钥")

        # Only auto-fill when user actually switches preset
        if selected_preset != st.session_state.llm_preset:
            st.session_state.llm_preset = selected_preset
            if selected_preset != "Custom":
                preset = Config.LLM_PRESETS[default_idx]
                st.session_state.llm_model = preset["model"]
                st.session_state.llm_base_url = preset["base_url"]
                # Load key from JSON, fall back to current key (from .env or manual input)
                if preset.get("key_field"):
                    keys = Config.load_llm_keys()
                    json_key = keys.get(preset["key_field"], "")
                    if json_key:
                        st.session_state.llm_api_key = json_key

        st.session_state.llm_model = st.text_input(
            "Model", value=st.session_state.llm_model,
            help="模型名，传递给 API")
        st.session_state.llm_base_url = st.text_input(
            "API Base URL", value=st.session_state.llm_base_url,
            help="例如 https://api.deepseek.com/v1")
        st.session_state.llm_api_key = st.text_input(
            "API Key", value=st.session_state.llm_api_key,
            type="password", help="优先用 llm_keys.json，为空则用 .env 兜底")

        st.divider()
        st.caption(f"Config 兜底: {Config.LLM_CHAT_MODEL} @ {Config.LLM_BASE_URL}")


# ═══════════════════════════════════════════════════════════════════════════════
# Query page
# ═══════════════════════════════════════════════════════════════════════════════

def get_current_db():
    db_list = _get_databases()
    db_options = {d.display_name: d for d in db_list}
    name = st.session_state.selected_db_name
    if name and name in db_options:
        return db_options[name]
    first = list(db_options.keys())[0]
    st.session_state.selected_db_name = first
    return db_options[first]


def render_query():
    db = get_current_db()

    # Ensure DB is initialized
    if st.session_state.selected_db_name is None:
        st.session_state.selected_db_name = db.display_name

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
    question = st.chat_input(f"Ask a data question about {db.display_name}...")

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
                result = _call_api_async(question, db.db_id, db.database_url)

            # Save to history first — so rendering errors don't lose the result
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

            try:
                render_result(result, question)
            except Exception as e:
                st.error(f"Render error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar — History + page toggle
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    # Page toggle button
    if st.session_state.page == "query":
        if st.button("Settings", use_container_width=True, type="secondary"):
            st.session_state.page = "settings"
            st.rerun()
    else:
        if st.button("Back to Query", use_container_width=True, type="primary"):
            st.session_state.page = "query"
            st.rerun()

    st.divider()

    # Database selector
    st.subheader("Database")
    db_list = _get_databases()
    db_options = {}  # display_label -> DbInfo
    for d in db_list:
        label = d.display_name
        if not d.online:
            label += " (offline)"
        db_options[label] = d

    if st.session_state.selected_db_name is None:
        st.session_state.selected_db_name = list(db_options.values())[0].display_name

    # Build label list matching by display_name
    db_labels = list(db_options.keys())
    current_display = st.session_state.selected_db_name
    default_idx = 0
    for i, (label, info) in enumerate(db_options.items()):
        if info.display_name == current_display:
            default_idx = i
            break

    selected_label = st.selectbox(
        "Select Database",
        options=db_labels, index=default_idx,
        label_visibility="collapsed",
    )
    db = db_options[selected_label]
    st.session_state.selected_db_name = db.display_name
    with st.container(border=True):
        st.caption(f"Domain: {db.domain}")
        st.caption(f"Dialect: {db.dialect} | Tables: {db.table_count}")
        if not db.online:
            st.caption(":red_circle: Offline — start Docker or check connection")

    st.divider()

    # Sample questions
    st.subheader("Sample Questions")
    try:
        samples = _get_sample_questions(db.db_id, n=5)
        for i, q in enumerate(samples):
            if st.button(f"{q[:80]}{'...' if len(q) > 80 else ''}",
                         key=f"sample_{db.db_id}_{i}", use_container_width=True):
                st.session_state.pending_question = q
                st.session_state.page = "query"
                st.rerun()
    except Exception:
        st.caption("(sample questions unavailable)")

    st.divider()

    # LLM badge
    st.caption(f"Model: **{st.session_state.llm_model}**")

    st.divider()

    # History
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


# ═══════════════════════════════════════════════════════════════════════════════
# Page router
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state.page == "settings":
    render_settings()
else:
    render_query()
