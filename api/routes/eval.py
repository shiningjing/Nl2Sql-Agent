"""Eval endpoints — async evaluation with background execution and progress polling."""
import os

from fastapi import APIRouter, HTTPException

from api.models import (
    EvalStartRequest,
    EvalStartResponse,
    EvalStatusResponse,
    EvalTaskListItem,
)
from evaluation.task_manager import (
    start_eval_task,
    get_task_status,
    list_tasks,
    cancel_task,
    is_task_running,
)

router = APIRouter()

# Resolve gold_path relative to project root
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(_PROJECT_ROOT, path)


@router.post("/eval/start", response_model=EvalStartResponse, status_code=202)
def eval_start(req: EvalStartRequest):
    """Start a background evaluation task. Returns task_id for polling."""
    if is_task_running():
        raise HTTPException(status_code=409, detail="An evaluation task is already running")

    configs = []
    for c in req.configs:
        configs.append({
            "name": c.name,
            "rag_schema": c.rag_schema,
            "rag_domain": c.rag_domain,
            "reviewer_on": c.reviewer_on,
            "use_full_graph": c.use_full_graph,
            "multi_candidate": c.multi_candidate,
            "k": c.k,
            "rag_column_prune": c.rag_column_prune,
            "fewshot_enabled": c.fewshot_enabled,
        })

    try:
        task_id = start_eval_task(
            gold_path=_resolve_path(req.gold_path),
            configs=configs,
            experiment=req.experiment,
            database_url=req.database_url,
            max_workers=req.max_workers,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))

    status = get_task_status(task_id)
    return EvalStartResponse(
        task_id=task_id,
        status=status.status if status else "pending",
        experiment=req.experiment,
        total_configs=len(configs),
        total_samples=status.total_samples if status else 0,
    )


@router.get("/eval/status/{task_id}", response_model=EvalStatusResponse)
def eval_status(task_id: str):
    """Poll evaluation progress. Returns per-config completion counts and overall progress."""
    p = get_task_status(task_id)
    if p is None:
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    total = p.total_samples
    pct = (p.overall_completed / total * 100) if total > 0 else 0.0

    configs = [
        {
            "name": c["name"],
            "total": c["total"],
            "completed": c["completed"],
            "passed": c["passed"],
        }
        for c in p.configs
    ]

    return EvalStatusResponse(
        task_id=p.task_id,
        status=p.status,
        experiment=p.experiment,
        configs=configs,
        current_config=p.current_config,
        total_configs=p.total_configs,
        total_samples=p.total_samples,
        overall_completed=p.overall_completed,
        overall_passed=p.overall_passed,
        progress_pct=round(pct, 1),
        created_at=p.created_at,
        started_at=p.started_at,
        completed_at=p.completed_at,
        report_dir=p.report_dir,
        error=p.error,
        database_url=p.database_url,
    )


@router.get("/eval/tasks", response_model=list[EvalTaskListItem])
def eval_list_tasks():
    """List all evaluation tasks (latest first)."""
    tasks = list_tasks()
    return [
        EvalTaskListItem(
            task_id=t["task_id"],
            status=t["status"],
            experiment=t["experiment"],
            overall_completed=t["overall_completed"],
            total_samples=t["total_samples"],
            overall_passed=t["overall_passed"],
            created_at=t["created_at"],
            completed_at=t.get("completed_at", ""),
        )
        for t in tasks
    ]


@router.delete("/eval/cancel/{task_id}", status_code=200)
def eval_cancel(task_id: str):
    """Cancel a running evaluation task."""
    ok = cancel_task(task_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Task not running: {task_id}")
    return {"task_id": task_id, "status": "cancelled"}
