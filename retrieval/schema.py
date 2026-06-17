import threading
from functools import lru_cache

from sqlalchemy import create_engine, event, inspect, table, select, text
from sqlalchemy.exc import NoSuchTableError
from storage.config import Config


@lru_cache(maxsize=64)
def get_engine(database_url: str | None = None):
    url = database_url or Config.DATABASE_URL
    engine = create_engine(url, echo=False)

    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _set_wal(dbapi_connection, connection_record):
            dbapi_connection.execute("PRAGMA journal_mode=WAL")

    return engine


# ── Schema metadata cache (per database_url, process-lifetime) ────────────────
# {database_url: {table_name_lower: {
#   "columns": [...], "pk_cols": set, "fk_cols": set, "fks": [...], "actual_name": str
# }}}
_schema_info_cache: dict[str, dict] = {}
_schema_cache_lock = threading.Lock()


def _get_cached_schema_info(database_url: str) -> dict:
    """Return per-table schema metadata from cache, reflecting once per database.

    All schema consumers (get_schema_summary, build_ddl_for_tables, _build_graph,
    prune_columns, build_compact_ddl, _build_sample_rows) route through this
    single entry point so that per-DB PRAGMA reflection happens exactly once
    under ThreadPoolExecutor concurrency.
    """
    if database_url not in _schema_info_cache:
        with _schema_cache_lock:
            if database_url not in _schema_info_cache:
                engine = get_engine(database_url)
                insp = inspect(engine)
                info: dict[str, dict] = {}
                for t in insp.get_table_names():
                    cols = insp.get_columns(t)
                    pk_cols = set(
                        insp.get_pk_constraint(t).get("constrained_columns", [])
                    )
                    fks = insp.get_foreign_keys(t)
                    fk_cols = set()
                    for fk in fks:
                        fk_cols.update(fk["constrained_columns"])
                    info[t.lower()] = {
                        "columns": cols,
                        "pk_cols": pk_cols,
                        "fk_cols": fk_cols,
                        "fks": fks,
                        "actual_name": t,
                    }
                _schema_info_cache[database_url] = info
    return _schema_info_cache[database_url]


# ── Schema consumers ──────────────────────────────────────────────────────────


def get_schema_summary(database_url: str | None = None) -> str:
    """Build a compressed DDL block for the LLM prompt."""
    if not database_url:
        return ""
    from agent.generator_llm import get_dialect_from_url
    info = _get_cached_schema_info(database_url)
    dialect = get_dialect_from_url(database_url)
    lines = [f"-- SQL Dialect: {dialect}\n"]

    for t_lower, t_info in info.items():
        actual_name = t_info["actual_name"]
        pk_cols = t_info["pk_cols"]
        fks = t_info["fks"]

        members = []
        for c in t_info["columns"]:
            parts = [c["name"], str(c["type"])]
            if not c.get("nullable", True):
                parts.append("NOT NULL")
            if c["name"] in pk_cols:
                parts.append("PRIMARY KEY")
            members.append("  " + " ".join(parts))

        if fks:
            for fk in fks:
                src = ", ".join(fk["constrained_columns"])
                tgt = fk["referred_table"]
                tgt_cols = ", ".join(fk["referred_columns"])
                members.append(
                    f"  FOREIGN KEY ({src}) REFERENCES {tgt}({tgt_cols})"
                )

        lines.append(f"CREATE TABLE {actual_name} (")
        lines.append(",\n".join(members))
        lines.append(");\n")

    return "\n".join(lines)


def get_sample_rows(table_name: str, n: int = 3, database_url: str | None = None) -> str:
    """Return sample rows for a table to help LLM understand data."""
    engine = get_engine(database_url)
    t = table(table_name)
    stmt = select("*").select_from(t).limit(n)
    with engine.connect() as conn:
        result = conn.execute(stmt)
        rows = result.fetchall()
        if not rows:
            return f"-- {table_name}: no rows"
        cols = result.keys()
    header = " | ".join(cols)
    sep = "-" * len(header)
    data = "\n".join(" | ".join(str(v) for v in row) for row in rows)
    return f"-- Sample rows from {table_name}:\n{header}\n{sep}\n{data}"
