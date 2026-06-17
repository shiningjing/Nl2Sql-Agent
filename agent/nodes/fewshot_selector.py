"""Few-shot Selector node — retrieves similar (Q, SQL) examples for Generator prompt."""
import time
from agent.state import AgentState
from retrieval.fewshot_retrieve import retrieve_fewshot, retrieve_fewshot_for_db, format_fewshot
from agent.generator_llm import get_dialect_from_url


def fewshot_selector_node(state: AgentState) -> dict:
    """Retrieve top-K similar Q-SQL examples and format for prompt injection.

    Lookup order: db_id first (BIRD databases), then dialect (mysql/postgresql).
    Falls back to generic retrieval if neither matches.
    """
    t0 = time.time()
    question = state.get("question", "")
    enabled = state.get("fewshot_enabled", False)
    db_id = state.get("db_id", "")
    database_url = state.get("database_url", "")
    complexity = state.get("complexity", "simple")
    k = 1 if complexity == "simple" else 3

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("fewshot_selector", {"enabled": enabled, "db_id": db_id, "k": k})

    if not enabled:
        if tlog:
            tlog.node_exit("fewshot_selector", {"example_count": 0, "skipped": True})
        return {"fewshot_text": ""}

    items = []
    if db_id:
        items = retrieve_fewshot_for_db(question, db_id, k=k)
    if not items and database_url:
        dialect = get_dialect_from_url(database_url)
        if dialect and dialect != "sqlite":
            items = retrieve_fewshot_for_db(question, dialect, k=k)
    if not items:
        items = retrieve_fewshot(question, k=k)

    fewshot_text = format_fewshot(items) if items else ""
    fewshot_hits = [item["source"] for item in items] if items else []

    if tlog:
        tlog.node_exit("fewshot_selector", {"example_count": len(items), "hits": fewshot_hits})

    node_latency = dict(state.get("node_latency", {}))
    node_latency["fewshot_selector"] = round(time.time() - t0, 3)

    return {"fewshot_text": fewshot_text, "fewshot_hits": fewshot_hits, "node_latency": node_latency}
