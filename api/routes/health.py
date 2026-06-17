"""Health, schema, and database discovery endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.models import HealthResponse
from storage.redis_cache import get_redis
from storage.config import Config
from retrieval.schema import get_schema_summary, _get_cached_schema_info

router = APIRouter()


class DbInfoResponse(BaseModel):
    db_id: str
    display_name: str
    database_url: str
    domain: str
    table_count: int


@router.get("/health", response_model=HealthResponse)
def health_check():
    """Health check: DB connectivity + Redis status."""
    db_status = "ok"
    redis_status = "ok"

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(engine.dialect.statement_compiler(engine.dialect, "SELECT 1"))
    except Exception:
        db_status = "error"

    r = get_redis()
    if r is None:
        redis_status = "unavailable"

    overall = "healthy" if db_status == "ok" else "degraded"

    return HealthResponse(
        status=overall,
        db=db_status,
        redis=redis_status,
    )


@router.get("/schema")
def get_schema():
    """Return the full database DDL and table catalog."""
    try:
        ddl = get_schema_summary()
        info = _get_cached_schema_info(Config.DATABASE_URL if hasattr(Config, "DATABASE_URL") else "")
        tables = []
        for t_lower, t_info in info.items():
            cols = [c["name"] for c in t_info["columns"]]
            tables.append({"name": t_info["actual_name"], "columns": cols})
        return {"ddl": ddl, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/databases", response_model=list[DbInfoResponse])
def list_databases_endpoint():
    """Return all available BIRD databases with metadata."""
    from storage.db_registry import list_databases
    return [
        DbInfoResponse(
            db_id=info.db_id,
            display_name=info.display_name,
            database_url=info.database_url,
            domain=info.domain,
            table_count=info.table_count,
        )
        for info in list_databases()
    ]
