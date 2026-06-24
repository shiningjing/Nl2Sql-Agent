"""Feedback graph — human-in-the-loop correction reusing the self-correction loop.

Graph: START → Refiner → Generator → Guard → Voter → SemCheck → (loop / END)

This graph REUSES existing node implementations (refiner_node, generator_node,
guard_node, voter_node, semantic_check_node). No new nodes.

Compared to the full graph, this skips Router, Schema Retriever, Decomposer,
and Fewshot Selector — the question and schema haven't changed between turns.

The initial state MUST already contain: question, schema_text, notes_text,
database_url, db_id, fewshot_text — loaded from Redis task state by the Worker.
"""

from langgraph.graph import StateGraph, END, START
from agent.state import AgentState
from agent.nodes.generator import generator_node
from agent.nodes.guard import guard_node
from agent.nodes.voter import voter_node
from agent.nodes.semantic_check import semantic_check_node
from agent.nodes.refiner import refiner_node


def _route_after_guard(state: AgentState) -> str:
    if state.get("guard_pass"):
        return "voter"
    return "refiner"


def _route_after_voter(state: AgentState) -> str:
    exec_result = state.get("exec_result", {})
    if exec_result and exec_result.get("success"):
        return "semantic_check"
    return "refiner"


def _route_after_semantic(state: AgentState) -> str:
    if state.get("semantic_pass", True):
        return END
    return "refiner"


def _route_after_refiner(state: AgentState) -> str:
    retry = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 2)
    if retry > max_retries:
        return END
    return "generator"


def _after_refiner(state: AgentState) -> dict:
    return {"retry_count": state.get("retry_count", 0) + 1}


def create_feedback_graph():
    """Build the feedback correction graph reusing existing nodes.

    Entry: Refiner (formats user_feedback into CORRECTION FEEDBACK)
    Exit:  END after SemCheck passes or max retries exhausted.
    """
    graph = StateGraph(AgentState)

    graph.add_node("generator", generator_node)
    graph.add_node("guard", guard_node)
    graph.add_node("voter", voter_node)
    graph.add_node("semantic_check", semantic_check_node)
    graph.add_node("refiner", refiner_node)
    graph.add_node("_post_refiner", _after_refiner)

    # START → Refiner (formats user_feedback, then routes to Generator)
    graph.add_edge(START, "refiner")
    graph.add_edge("refiner", "_post_refiner")
    graph.add_conditional_edges(
        "_post_refiner", _route_after_refiner,
        {"generator": "generator", END: END},
    )

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

    return graph.compile()
