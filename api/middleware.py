"""FastAPI middleware — request logging and rate limiting."""
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from storage.redis_cache import get_redis
from observability.logger import TraceLogger

_RATE_LIMIT_WINDOW = 60        # 1 minute
_RATE_LIMIT_MAX = 10           # 10 requests per window per IP
_RATE_LIMIT_PREFIX = "rate_limit"


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log each request with TraceLogger — structured JSON, unified format."""

    async def dispatch(self, request: Request, call_next):
        trace_id = request.headers.get("X-Trace-ID", uuid.uuid4().hex[:12])
        request.state.trace_id = trace_id
        tlog = TraceLogger(trace_id)

        client_ip = request.client.host if request.client else ""
        tlog.api_request("begin", method=request.method,
                         path=request.url.path, client_ip=client_ip)

        start = time.time()
        response = await call_next(request)
        elapsed_ms = (time.time() - start) * 1000

        tlog.api_request("end", method=request.method, path=request.url.path,
                         status=response.status_code, elapsed_ms=elapsed_ms)

        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Elapsed-Ms"] = f"{elapsed_ms:.1f}"
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-IP sliding-window rate limiter backed by Redis.

    Falls back to allow-all when Redis is unavailable.
    """

    async def dispatch(self, request: Request, call_next):
        r = get_redis()
        if r is None:
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"
        key = f"{_RATE_LIMIT_PREFIX}:{client_ip}"

        current = r.incr(key)
        if current == 1:
            r.expire(key, _RATE_LIMIT_WINDOW)

        if current > _RATE_LIMIT_MAX:
            trace_id = getattr(request.state, "trace_id", "?")
            TraceLogger(trace_id)._emit("rate_limit_hit", {
                "client_ip": client_ip,
                "current": current,
            })
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "detail": f"Max {_RATE_LIMIT_MAX} requests per {_RATE_LIMIT_WINDOW}s per IP",
                },
            )

        return await call_next(request)
