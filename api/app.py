"""FastAPI application factory with lifespan warmup and CORS middleware."""
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.middleware import RequestLoggingMiddleware, RateLimitMiddleware
from api.routes.health import router as health_router
from api.routes.query import router as query_router
from api.routes.eval import router as eval_router
from api.routes.task import router as task_router

_log = logging.getLogger("nl2sql.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warmup: pre-load embedding model in background, verify Redis on startup."""
    # Pre-load embedding model in background thread (non-blocking)
    def _warm_embedding():
        try:
            from retrieval.embedder import get_embedder
            get_embedder()
            _log.info("Embedding model loaded")
        except Exception as e:
            _log.warning(f"Embedding model load failed: {e}")

    threading.Thread(target=_warm_embedding, daemon=True).start()

    # Verify Redis (fast — 2s timeout)
    from storage.redis_cache import get_redis
    r = get_redis()
    if r:
        _log.info("Redis connected")
    else:
        _log.warning("Redis unavailable — running without cache")

    yield  # app runs here

    _log.info("Shutdown complete")


def create_app() -> FastAPI:
    app = FastAPI(
        title="NL2SQL Agent API",
        description="Natural Language → SQL with RAG, Multi-Candidate Generation, and Self-Correction",
        version="0.2.0",
        lifespan=lifespan,
    )

    # Middleware order: logging (outer) → rate limit → routes
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(RateLimitMiddleware)

    # CORS — allow Streamlit and other local dev origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(health_router, prefix="/api/v1", tags=["Health"])
    app.include_router(query_router, prefix="/api/v1", tags=["Query"])
    app.include_router(eval_router, prefix="/api/v1", tags=["Eval"])
    app.include_router(task_router, prefix="/api/v1", tags=["Task"])

    return app


# Module-level app instance for `uvicorn src.api.app:app`
app = create_app()
