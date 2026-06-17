"""Full LangGraph pipeline: Generator → Guard → Voter → END (with Refiner loop)."""
from langgraph.graph import StateGraph, END, START
from agent.state import AgentState
from agent.nodes.schema_retriever import schema_retriever_node
from agent.nodes.router import router_node
from agent.nodes.decomposer import decomposer_node
from agent.nodes.generator import generator_node
from agent.nodes.guard import guard_node
from agent.nodes.voter import voter_node
from agent.nodes.semantic_check import semantic_check_node
from agent.nodes.refiner import refiner_node
from agent.nodes.fewshot_selector import fewshot_selector_node


# ── Routing ─────────────────────────────────────────────────────────────────

def _route_after_schema(state: AgentState) -> str:
    """Simple → fewshot. Complex + decomposer_enabled → decomposer."""
    if state.get("complexity") == "complex" and state.get("decomposer_enabled"):
        return "decomposer"
    return "fewshot_selector"


def _route_after_guard(state: AgentState) -> str:
    """Guard passed → Voter for execution validation. Failed → Refiner for fix."""
    if state.get("guard_pass"):
        return "voter"
    return "refiner"


def _route_after_voter(state: AgentState) -> str:
    """Voter found a winner → SemanticCheck. All candidates failed → Refiner."""
    exec_result = state.get("exec_result", {})
    if exec_result and exec_result.get("success"):
        return "semantic_check"
    return "refiner"


def _route_after_semantic(state: AgentState) -> str:
    """Semantic check passed → END. Failed → Refiner with feedback."""
    if state.get("semantic_pass", True):
        return END
    return "refiner"


def _route_after_refiner(state: AgentState) -> str:
    """Refiner formats error → back to Generator for retry. Or END if max retries."""
    retry = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 2)
    if retry > max_retries:
        return END
    return "generator"


# ── Post-node updates ──────────────────────────────────────────────────────

def _after_refiner(state: AgentState) -> dict:
    return {"retry_count": state.get("retry_count", 0) + 1}


# ── Graph builder ───────────────────────────────────────────────────────────

def create_full_graph():
    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("schema_retriever", schema_retriever_node)
    graph.add_node("router", router_node)
    graph.add_node("decomposer", decomposer_node)
    graph.add_node("generator", generator_node)
    graph.add_node("guard", guard_node)
    graph.add_node("voter", voter_node)
    graph.add_node("semantic_check", semantic_check_node)
    graph.add_node("refiner", refiner_node)
    graph.add_node("fewshot_selector", fewshot_selector_node)
    graph.add_node("_post_refiner", _after_refiner)

    # Edges — forward path: Router first (only needs question), then Schema
    graph.add_edge(START, "router")
    graph.add_edge("router", "schema_retriever")

    # After schema_retriever: simple → fewshot (1 shot),
    # complex with multi-intent → decomposer → fewshot (3 shots),
    # complex without multi-intent → fewshot (3 shots, skip decomposer)
    graph.add_conditional_edges(
        "schema_retriever", _route_after_schema,
        {"decomposer": "decomposer", "fewshot_selector": "fewshot_selector"},
    )
    graph.add_edge("decomposer", "fewshot_selector")
    graph.add_edge("fewshot_selector", "generator")

    # Generator → Guard → Voter / Refiner
    graph.add_edge("generator", "guard")

    graph.add_conditional_edges(
        "guard", _route_after_guard,
        {"voter": "voter", "refiner": "refiner"},
    )

    # Voter → SemanticCheck / Refiner
    graph.add_conditional_edges(
        "voter", _route_after_voter,
        {"semantic_check": "semantic_check", "refiner": "refiner"},
    )

    # SemanticCheck → END / Refiner
    graph.add_conditional_edges(
        "semantic_check", _route_after_semantic,
        {END: END, "refiner": "refiner"},
    )

    # Refiner → Generator (retry loop) / END (max retries)
    graph.add_edge("refiner", "_post_refiner")
    graph.add_conditional_edges(
        "_post_refiner", _route_after_refiner,
        {"generator": "generator", END: END},
    )

    return graph.compile()
