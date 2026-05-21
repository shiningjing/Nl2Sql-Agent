"""Few-shot Selector node — retrieves similar (Q, SQL) examples for Generator prompt."""
from src.agent.state import AgentState
from src.retrieval.fewshot_retrieve import retrieve_fewshot, retrieve_fewshot_for_db, format_fewshot


def fewshot_selector_node(state: AgentState) -> dict:
    """Retrieve top-K similar Q-SQL examples and format for prompt injection."""
    question = state.get("question", "")
    enabled = state.get("fewshot_enabled", False)
    db_id = state.get("db_id", "")
    complexity = state.get("complexity", "simple")
    k = 1 if complexity == "simple" else 3

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("fewshot_selector", {"enabled": enabled, "db_id": db_id, "k": k})

    if not enabled:
        if tlog:
            tlog.node_exit("fewshot_selector", {"example_count": 0, "skipped": True})
        return {"fewshot_text": ""}

    if db_id:
        items = retrieve_fewshot_for_db(question, db_id, k=k)
    else:
        items = retrieve_fewshot(question, k=k)
    fewshot_text = format_fewshot(items) if items else ""
    fewshot_hits = [item["source"] for item in items]

    if tlog:
        tlog.node_exit("fewshot_selector", {"example_count": len(items), "hits": fewshot_hits})

    return {"fewshot_text": fewshot_text, "fewshot_hits": fewshot_hits}
