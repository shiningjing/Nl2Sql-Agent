"""Pydantic models for FastAPI request/response schemas."""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class LLMConfig(BaseModel):
    """Optional per-request LLM overrides. Falls back to env vars if not set."""
    model: str | None = Field(default=None, description="Model name, e.g. deepseek-v4-pro")
    api_key: str | None = Field(default=None, description="API key")
    base_url: str | None = Field(default=None, description="API base URL, e.g. https://api.deepseek.com/v1")


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    rag_schema: bool = Field(default=True)
    rag_domain: bool = Field(default=True)
    reviewer_on: bool = Field(default=False)
    k: int = Field(default=8, ge=1, le=50)
    use_cache: bool = Field(default=True)
    database_url: str | None = Field(default=None)
    db_id: str | None = Field(default=None, description="BIRD database ID for RAG filtering")
    llm: LLMConfig | None = Field(default=None, description="Optional LLM overrides")


class QueryFullRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    rag_schema: bool = Field(default=True)
    rag_domain: bool = Field(default=True)
    multi_candidate: bool = Field(default=True)
    rag_k: int = Field(default=8, ge=1, le=50)
    rag_column_prune: bool = Field(default=False)
    rag_hybrid: bool = Field(default=True)
    rag_fk_expand: bool = Field(default=True)
    fewshot_enabled: bool = Field(default=True)
    use_cache: bool = Field(default=True)
    database_url: str | None = Field(default=None)
    db_id: str | None = Field(default=None, description="BIRD database ID for RAG filtering")
    llm: LLMConfig | None = Field(default=None, description="Optional LLM overrides")


class ExecResult(BaseModel):
    success: bool
    data: list[Any] | None = None
    columns: list[str] | None = None
    row_count: int = 0
    error: str | None = None


class QueryResponse(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    question: str
    sql: str
    exec_result: ExecResult | None = None
    token_usage: dict[str, int] = Field(default_factory=dict)
    cache_hit: bool = False
    elapsed_ms: float = 0.0


class HealthResponse(BaseModel):
    status: str  # "healthy" | "degraded"
    version: str = "0.2.0"
    db: str  # "ok" | "error"
    redis: str  # "ok" | "unavailable"
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class ErrorResponse(BaseModel):
    request_id: str
    error: str
    detail: str = ""


# ── Eval endpoints ──────────────────────────────────────────────────────────

class EvalConfigItem(BaseModel):
    name: str
    rag_schema: bool = True
    rag_domain: bool = False
    reviewer_on: bool = False
    use_full_graph: bool = False
    multi_candidate: bool = True
    k: int = 8
    rag_column_prune: bool = False
    fewshot_enabled: bool = False


class EvalStartRequest(BaseModel):
    experiment: str = Field(default="ablation", min_length=1, max_length=100)
    gold_path: str = Field(default="eval/gold.jsonl", max_length=500)
    configs: list[EvalConfigItem] = Field(..., min_length=1, max_length=20)
    database_url: str | None = Field(default=None)
    max_workers: int = Field(default=4, ge=1, le=16)


class EvalStartResponse(BaseModel):
    task_id: str
    status: str
    experiment: str
    total_configs: int
    total_samples: int


class EvalConfigProgress(BaseModel):
    name: str
    total: int
    completed: int
    passed: int


class EvalStatusResponse(BaseModel):
    task_id: str
    status: str
    experiment: str
    configs: list[EvalConfigProgress]
    current_config: str
    total_configs: int
    total_samples: int
    overall_completed: int
    overall_passed: int
    progress_pct: float
    created_at: str
    started_at: str
    completed_at: str
    report_dir: str
    error: str
    database_url: str


class EvalTaskListItem(BaseModel):
    task_id: str
    status: str
    experiment: str
    overall_completed: int
    total_samples: int
    overall_passed: int
    created_at: str
    completed_at: str


# ── Async task endpoints (W3) ────────────────────────────────────────────────

class TaskSubmitRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    rag_schema: bool = True
    rag_domain: bool = True
    multi_candidate: bool = True
    rag_k: int = Field(default=8, ge=1, le=50)
    rag_column_prune: bool = False
    rag_hybrid: bool = True
    rag_fk_expand: bool = True
    fewshot_enabled: bool = True
    database_url: str | None = None
    db_id: str | None = None
    llm: LLMConfig | None = None
    idempotency_key: str | None = Field(default=None, max_length=128,
        description="Client-generated dedup key; same key within 5 min returns existing task_id")


class TaskSubmitResponse(BaseModel):
    task_id: str
    status: str  # "PENDING"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str  # PENDING | RUNNING | SUCCESS | FAILED | TIMEOUT | CANCELLED
    question: str = ""
    db_id: str = ""
    progress: int = 0
    node: str | None = None
    sql: str | None = None
    exec_result: dict | None = None
    token_usage: dict = Field(default_factory=dict)
    node_timings: dict = Field(default_factory=dict)
    error: str | None = None
    retry_count: int = 0
    created_at: str = ""
    updated_at: str = ""


class TaskCancelResponse(BaseModel):
    task_id: str
    status: str  # "cancelled" | "not_found"
