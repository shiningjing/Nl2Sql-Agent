"""Schema Retriever node — BIRD RAG retrieval + prompt context building."""
from retrieval.rag_retrieve import retrieve_bird, build_prompt_context
from retrieval.schema import get_schema_summary, get_sample_rows, _get_cached_schema_info
from agent.state import AgentState
from retrieval.fk_expand import expand_tables, expand_schema_text, _parse_matched_tables
from retrieval.column_prune import prune_columns, build_compact_ddl
from observability.logger import TraceLogger


def _build_table_catalog(database_url: str | None = None) -> str:
    if not database_url:
        return ""
    info = _get_cached_schema_info(database_url)
    lines = []
    for t_lower, t_info in info.items():
        cols = [c["name"] for c in t_info["columns"]]
        lines.append(f"  {t_info['actual_name']}({', '.join(cols)})")
    return "Tables:\n" + "\n".join(lines)


def _build_sample_rows(database_url: str | None = None, tables: set[str] | None = None) -> str:
    if not database_url:
        return ""
    info = _get_cached_schema_info(database_url)
    all_tables = list(info.keys())
    target = [t for t in all_tables if tables is None or t.lower() in tables]
    sample_blocks = []
    for t in target:
        actual_name = info[t]["actual_name"]
        sample_blocks.append(get_sample_rows(actual_name, n=3, database_url=database_url))
    return "\n\n".join(sample_blocks)


def schema_retriever_node(state: AgentState) -> dict:
    """RAG retrieval -> schema_text + notes_text + sample_rows_text.

    Also initializes TraceLogger with a new trace_id on first entry — all
    downstream nodes read tlog from state to emit structured log events.
    """
    # ── Trace init (Router may have already created it) ──
    tlog = state.get("tlog")
    if not tlog:
        trace_id = state.get("trace_id", "")
        tlog = TraceLogger(trace_id)
    trace_id = tlog.trace_id

    question = state["question"]
    database_url = state.get("database_url")
    db_id = state.get("db_id", "")
    tlog.node_enter("schema_retriever", {"question": question[:80]})

    # Knowledge ablation (experiment 2): evidence source provides pre-built domain notes
    domain_override = state.get("_domain_notes_override", "")

    rag_schema = state.get("rag_schema", True)
    rag_domain = state.get("rag_domain", True)
    skip_schema = state.get("skip_schema", False)
    k = state.get("rag_k", 8)
    use_fk_expand = state.get("rag_fk_expand", True)
    complexity = state.get("complexity", "simple")
    use_column_prune = state.get("rag_column_prune", False) and complexity != "simple"

    schema_text = ""
    notes_text = ""
    rag_chunks: list[dict] = []

    # BIRD RAG: require db_id to filter bird_minidev collection to correct database
    if rag_schema or rag_domain:
        if not db_id:
            raise ValueError("rag_schema/rag_domain requested but no db_id provided — schema_retriever needs db_id to query bird_minidev")
        rag_chunks = retrieve_bird(question, db_id, k=k)

    if rag_chunks:
        ctx = build_prompt_context(rag_chunks)
        if rag_schema:
            schema_text = ctx["schema_text"]
        if rag_domain:
            notes_text = ctx["notes_text"]

    # Knowledge ablation: evidence source overrides domain notes
    if domain_override:
        notes_text = domain_override

    if not schema_text and not skip_schema:
        schema_text = get_schema_summary(database_url)

    _table_set: set[str] | None = None  # tracked for sample_rows pruning

    if skip_schema:
        # R0_Naked: no schema at all — skip pruning, FK expand, catalog
        pass
    elif use_column_prune:
        # Column pruning: rebuild compact DDL from the full table set
        matched = _parse_matched_tables(rag_chunks)
        table_set = expand_tables(matched) if use_fk_expand and matched else matched
        _table_set = {t.lower() for t in table_set} if table_set else None
        if table_set:
            pruned = prune_columns(question, table_set, database_url=database_url)
            schema_text = build_compact_ddl(table_set, pruned, database_url=database_url)
    else:
        # No column pruning: use RAG chunk DDL + FK expansion + catalog
        if use_fk_expand:
            schema_text = expand_schema_text(schema_text, rag_chunks, database_url=database_url)
        schema_text = schema_text + "\n\n" + _build_table_catalog(database_url)

    _sample_rows = state.get("sample_rows", True)
    sample_rows_text = "" if (skip_schema or not _sample_rows) else _build_sample_rows(database_url, tables=_table_set)

    tlog.node_exit("schema_retriever", {
        "schema_len": len(schema_text),
        "notes_len": len(notes_text),
        "chunk_count": len(rag_chunks),
    })

    return {
        "rag_chunks": rag_chunks,
        "schema_text": schema_text,
        "notes_text": notes_text,
        "sample_rows_text": sample_rows_text,
        "trace_id": trace_id,
        "tlog": tlog,
    }
