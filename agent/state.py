"""Shared AgentState — used by all nodes in the LangGraph pipeline."""
from typing import TypedDict, NotRequired


class AgentState(TypedDict):
    # ── Input ──
    question: str

    # ── RAG (schema_retriever) ──
    rag_schema: NotRequired[bool]
    rag_domain: NotRequired[bool]
    skip_schema: NotRequired[bool]
    rag_hybrid: NotRequired[bool]
    rag_fk_expand: NotRequired[bool]
    rag_column_prune: NotRequired[bool]
    rag_k: NotRequired[int]
    rag_chunks: NotRequired[list[dict]]
    schema_text: NotRequired[str]
    notes_text: NotRequired[str]
    sample_rows: NotRequired[bool]
    sample_rows_text: NotRequired[str]
    # Override for BIRD knowledge ablation: evidence source bypasses RAG domain
    _domain_notes_override: NotRequired[str]

    # ── Generator ──
    sql: NotRequired[str]
    raw_response: NotRequired[str]
    multi_candidate: NotRequired[bool]
    candidate_sqls: NotRequired[list[str]]

    # ── Guard ──
    guard_pass: NotRequired[bool]
    guard_issues: NotRequired[list[dict]]

    # ── Reviewer (legacy, kept for W5 evaluation) ──
    reviewer_on: NotRequired[bool]
    review_rounds: NotRequired[list[dict]]
    review_round_count: NotRequired[int]
    skip_reviewer: NotRequired[bool]

    # ── Executor ──
    exec_result: NotRequired[dict | None]
    exec_attempts: NotRequired[list[dict]]

    # ── Self-Correction control ──
    retry_count: NotRequired[int]
    max_retries: NotRequired[int]
    last_error: NotRequired[str]
    last_sql: NotRequired[str]
    rag_k_expanded: NotRequired[int | None]

    # ── Tracing ──
    token_usage: NotRequired[dict]

    # ── Full version reserved ──
    db_id: NotRequired[str]
    database_url: NotRequired[str]
    trace_id: NotRequired[str]
    complexity: NotRequired[str]
    decomposer_enabled: NotRequired[bool]
    sub_questions: NotRequired[list[dict]]
    chosen_sql: NotRequired[str]

    # ── W5: Semantic check ──
    semantic_pass: NotRequired[bool]
    semantic_feedback: NotRequired[str]
    _sem_reject_count: NotRequired[int]
    _sem_last_rejected_sql: NotRequired[str]

    # ── W5: AST guardrail ──
    ast_pass: NotRequired[bool]
    ast_issues: NotRequired[list[dict]]

    # ── W5: Router debug ──
    router_score: NotRequired[int]
    router_score_detail: NotRequired[str]
    router_method: NotRequired[str]
    router_llm_raw: NotRequired[str]

    # ── W6: Few-shot ──
    fewshot_text: NotRequired[str]
    fewshot_enabled: NotRequired[bool]
    fewshot_hits: NotRequired[list[str]]

    # ── W7: Human-Feedback Conversation ──
    user_feedback: NotRequired[str]              # raw user guidance for this correction turn
    conversation_turns: NotRequired[list[dict]]   # [{turn, user_feedback, sql, exec_result}]
    is_feedback_round: NotRequired[bool]          # True = skip Router/Schema/Decomposer/Fewshot

    # ── W5: Trace ──
    tlog: NotRequired[object]  # TraceLogger instance (not serialized)

    # ── W1: Observability ──
    node_latency: NotRequired[dict[str, float]]  # per-node elapsed seconds
    repair_history: NotRequired[list[dict]]  # [{attempt, error_source, error_type, failed_sql, fix_strategy}]
