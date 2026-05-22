"""Column-level pruning — keep only question-relevant columns + mandatory PK/FK."""
import re

from nl2sql.schema import _get_cached_schema_info


def _get_dialect(database_url: str) -> str:
    for prefix, d in [("postgresql", "postgresql"), ("mysql", "mysql")]:
        if database_url and database_url.startswith(prefix):
            return d
    return "sqlite"


# ── Tokenization & scoring ────────────────────────────────────────────────────

def _tokenize(text: str) -> set[str]:
    """Tokenize into lowercased words + Chinese char bigrams."""
    tokens = set(re.findall(r"[a-zA-Z0-9_]+", text.lower()))
    for seg in re.findall(r"[一-鿿]+", text):
        tokens.update(seg[i : i + 2] for i in range(len(seg) - 1))
        tokens.update(seg)
    return tokens


def _score_column(col_name: str, table_name: str, question_tokens: set[str]) -> int:
    """Keyword-overlap score between column identity and question tokens."""
    col_tokens = _tokenize(f"{table_name} {col_name}")
    return len(question_tokens & col_tokens)


def _resolve_actual_name(schema_info: dict, table_name: str) -> str | None:
    """Resolve table name case-insensitively against cached schema info."""
    if table_name.lower() in schema_info:
        return schema_info[table_name.lower()]["actual_name"]
    return None


# ── Pruning ───────────────────────────────────────────────────────────────────

def prune_columns(
    question: str, table_names: list[str], max_cols_per_table: int = 10,
    max_cols_threshold: int = 10,
    database_url: str | None = None,
) -> dict[str, list[str]]:
    """Return {table: [selected_column_names]} with relevant cols + PK + FK.

    Tables with ≤ max_cols_threshold columns are kept in full (no pruning).
    Larger tables keep PK + FK + top-N scored columns (N = max_cols_per_table).
    """
    schema_info = _get_cached_schema_info(database_url) if database_url else {}
    qt = _tokenize(question)
    result: dict[str, list[str]] = {}

    for t in table_names:
        actual_t = _resolve_actual_name(schema_info, t)
        if actual_t is None:
            result[t] = []
            continue
        info = schema_info[t.lower()]
        cols = info["columns"]
        pk_cols = info["pk_cols"]
        fk_cols = info["fk_cols"]
        mandatory = pk_cols | fk_cols

        # Small tables: keep all columns, no pruning needed
        if len(cols) <= max_cols_threshold:
            result[t] = [c["name"] for c in cols]
            continue

        # Score non-mandatory columns
        scored = []
        for c in cols:
            name = c["name"]
            if name in mandatory:
                continue
            scored.append((_score_column(name, t, qt), name))

        scored.sort(key=lambda x: x[0], reverse=True)

        # Take top-N by score, plus all mandatory columns
        budget = max(0, max_cols_per_table - len(mandatory))
        kept = sorted(mandatory) + [name for _, name in scored[:budget]]
        result[t] = kept

    return result


def build_compact_ddl(
    table_names: list[str], pruned: dict[str, list[str]],
    database_url: str | None = None,
) -> str:
    """Generate DDL with only selected columns per table."""
    schema_info = _get_cached_schema_info(database_url) if database_url else {}
    dialect = _get_dialect(database_url)
    lines = [f"-- SQL Dialect: {dialect}"]

    for t in table_names:
        actual_t = _resolve_actual_name(schema_info, t)
        if actual_t is None:
            continue
        info = schema_info[t.lower()]
        all_cols = {c["name"]: c for c in info["columns"]}
        pk_cols = info["pk_cols"]
        fks = info["fks"]
        kept = pruned.get(t, list(all_cols.keys()))

        members = []
        for name in kept:
            c = all_cols[name]
            parts = [name, str(c["type"])]
            if not c.get("nullable", True):
                parts.append("NOT NULL")
            if name in pk_cols:
                parts.append("PRIMARY KEY")
            members.append("  " + " ".join(parts))

        # Include FK declarations only if both src and tgt columns are kept
        if fks:
            for fk in fks:
                src_cols = fk["constrained_columns"]
                tgt = fk["referred_table"]
                tgt_cols = fk["referred_columns"]
                if all(c in kept for c in src_cols):
                    members.append(
                        f"  FOREIGN KEY ({', '.join(src_cols)}) "
                        f"REFERENCES {tgt}({', '.join(tgt_cols)})"
                    )

        lines.append(f"CREATE TABLE {t} (")
        lines.append(",\n".join(members))
        lines.append(");")

    return "\n".join(lines)
