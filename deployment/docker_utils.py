"""Docker lifecycle utilities — ensure DB containers are cleaned up on exit."""
import atexit
import os
import subprocess
import time

_CLEANUP_REGISTERED = False
_PROJECT_ROOT: str | None = None


def _get_project_root() -> str:
    global _PROJECT_ROOT
    if _PROJECT_ROOT:
        return _PROJECT_ROOT
    candidates = [
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        os.getcwd(),
    ]
    for c in candidates:
        if os.path.exists(os.path.join(c, "docker-compose.yml")):
            _PROJECT_ROOT = c
            return c
    return candidates[0]


def _compose_file() -> str:
    return os.path.join(_get_project_root(), "docker-compose.yml")


def start_dbs() -> bool:
    """Start PostgreSQL and MySQL containers. Returns True if successful."""
    f = _compose_file()
    r = subprocess.run(
        ["docker", "compose", "-f", f, "up", "-d", "postgres", "mysql"],
        capture_output=True, encoding="utf-8", errors="replace",
    )
    return r.returncode == 0


def stop_dbs() -> bool:
    """Stop PostgreSQL and MySQL containers. Returns True if successful."""
    f = _compose_file()
    r = subprocess.run(
        ["docker", "compose", "-f", f, "stop", "postgres", "mysql"],
        capture_output=True, encoding="utf-8", errors="replace",
    )
    return r.returncode == 0


def register_cleanup() -> None:
    """Register atexit handler to stop Docker services on normal or error exit."""
    global _CLEANUP_REGISTERED
    if not _CLEANUP_REGISTERED:
        atexit.register(stop_dbs)
        _CLEANUP_REGISTERED = True


def ensure_dbs(wait_healthy: bool = True, timeout: int = 60) -> dict[str, bool]:
    """Start PG/MySQL containers, register auto-cleanup, optionally wait for health.

    Returns {postgres: healthy, mysql: healthy}.
    """
    start_dbs()
    register_cleanup()

    if not wait_healthy:
        return {"postgres": True, "mysql": True}

    f = _compose_file()
    deadline = time.time() + timeout
    status = {"postgres": False, "mysql": False}

    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "compose", "-f", f, "ps", "--format", "json"],
            capture_output=True, encoding="utf-8", errors="replace",
        )
        if not r.stdout:
            time.sleep(2)
            continue
        for line in r.stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                import json
                svc = json.loads(line)
                name = svc.get("Service", "")
                if name in status:
                    status[name] = svc.get("Health") == "healthy"
            except json.JSONDecodeError:
                continue
        if all(status.values()):
            return status
        time.sleep(2)

    return status
