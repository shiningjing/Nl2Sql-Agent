"""FK graph expansion — include JOIN-path tables in schema context."""
import threading
from collections import defaultdict

from retrieval.schema import _get_cached_schema_info, get_sample_rows
from storage.config import Config
from sqlalchemy.exc import NoSuchTableError


def _get_dialect(database_url: str) -> str:
    for prefix, d in [("postgresql", "postgresql"), ("mysql", "mysql")]:
        if database_url and database_url.startswith(prefix):
            return d
    return "sqlite"


_graph_cache: dict | None = None
_graph_db_url: str | None = None
_graph_lock = threading.Lock()


def _build_graph(database_url: str | None = None) -> dict[str, dict]:
    """Build FK graph from cached schema info.

    Returns {table_lower: {references: set, referenced_by: set}}.
    All FK data comes from _get_cached_schema_info — zero additional PRAGMA.
    """
    global _graph_cache, _graph_db_url
    db = database_url or Config.DATABASE_URL
    if _graph_cache is not None and _graph_db_url == db:
        return _graph_cache

    with _graph_lock:
        if _graph_cache is not None and _graph_db_url == db:
            return _graph_cache

        info = _get_cached_schema_info(db)
        graph: dict = defaultdict(lambda: {"references": set(), "referenced_by": set()})

        for t_lower, t_info in info.items():
            for fk in t_info["fks"]:
                tgt = fk["referred_table"].lower()
                if tgt not in info:
                    continue
                graph[t_lower]["references"].add(tgt)
                graph[tgt]["referenced_by"].add(t_lower)

        _graph_cache = dict(graph)
        _graph_db_url = db
        return _graph_cache


def expand_tables(selected: list[str], database_url: str | None = None) -> list[str]:
    """Expand selected table list 1-hop along FK edges (both directions)."""
    graph = _build_graph(database_url)
    expanded = set(selected)
    for t in selected:
        key = t.lower()
        if key in graph:
            expanded |= graph[key]["references"]
            expanded |= graph[key]["referenced_by"]
    return list(expanded)


def build_ddl_for_tables(table_names: list[str], database_url: str | None = None) -> str:
    """Generate compact DDL block for a specific set of tables.

    Reads column metadata from _get_cached_schema_info — zero additional PRAGMA.
    """
    if not table_names or not database_url:
        return ""

    info = _get_cached_schema_info(database_url)
    dialect = _get_dialect(database_url)
    lines = [f"-- SQL Dialect: {dialect}\n"]

    for t in table_names:
        t_info = info.get(t.lower())
        if t_info is None:
            continue

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
        lines.append(");")

        # Append sample rows (SELECT read, not PRAGMA)
        samples = get_sample_rows(actual_name, n=3, database_url=database_url)
        lines.append(samples + "\n")

    return "\n".join(lines)


def _parse_matched_tables(rag_chunks: list[dict]) -> list[str]:
    """Extract table names from RAG-retrieved schema chunks."""
    tables = []
    for c in rag_chunks:
        if isinstance(c.get("metadata"), dict):
            tn = c["metadata"].get("table_name", "")
            if tn and tn not in tables:
                tables.append(tn)
    return tables


def expand_schema_text(schema_text: str, rag_chunks: list[dict],
                       database_url: str | None = None) -> str:
    """Take existing schema_text and append DDL for FK-expanded tables not yet in it."""
    matched = _parse_matched_tables(rag_chunks)
    if not matched:
        return schema_text

    expanded = expand_tables(matched, database_url=database_url)
    missing = [t for t in expanded if t not in matched]

    if not missing:
        return schema_text

    extra_ddl = build_ddl_for_tables(missing, database_url=database_url)
    return schema_text + "\n\n-- FK-expanded tables (1-hop JOIN paths)\n" + extra_ddl
