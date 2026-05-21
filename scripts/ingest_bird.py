"""Build BIRD Mini-Dev RAG index from database_description/ CSVs.

Creates a 'bird_minidev' ChromaDB collection with per-table chunks.
Each chunk contains a table's column descriptions, indexed with db_id
metadata so retrieval can filter to the current database.

Usage:
  python scripts/ingest_bird.py
"""
import csv
import io
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import chromadb
from chromadb.utils import embedding_functions
from nl2sql.config import Config


def _default_data_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "bird", "mini_dev_data", "minidev", "MINIDEV",
    )


def build_bird_index(data_dir: str | None = None) -> int:
    """Index BIRD database_description CSVs into 'bird_minidev' collection."""
    root = data_dir or _default_data_dir()
    db_dir = os.path.join(root, "dev_databases")

    client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=Config.EMBED_MODEL_NAME,
    )

    # Replace existing collection
    try:
        client.delete_collection("bird_minidev")
    except Exception:
        pass

    col = client.get_or_create_collection(
        name="bird_minidev",
        embedding_function=ef,
    )

    chunks: list[dict] = []

    for db_id in sorted(os.listdir(db_dir)):
        desc_dir = os.path.join(db_dir, db_id, "database_description")
        if not os.path.isdir(desc_dir):
            continue

        for fname in sorted(os.listdir(desc_dir)):
            if not fname.endswith(".csv"):
                continue

            table_name = os.path.splitext(fname)[0]
            path = os.path.join(desc_dir, fname)

            try:
                # BIRD CSVs use mixed encodings — try UTF-8 first, then latin-1
                content = None
                for enc in ["utf-8", "latin-1", "cp1252"]:
                    try:
                        with open(path, "r", encoding=enc) as test_f:
                            content = test_f.read()
                        break
                    except UnicodeDecodeError:
                        continue
                if content is None:
                    print(f"  Warning: {db_id}/{fname}: unable to decode")
                    continue
                reader = csv.DictReader(io.StringIO(content))
                lines = [f"## {table_name} (database: {db_id})"]
                for row in reader:
                    col_name = (row.get("column_name") or row.get("original_column_name", "")).strip()
                    if not col_name:
                        continue
                    col_desc = (row.get("column_description") or "").strip()
                    val_desc = (row.get("value_description") or "").strip()

                    desc_parts = []
                    if col_desc and col_desc.lower() != col_name.lower():
                        desc_parts.append(col_desc[:150])
                    if val_desc and val_desc != col_desc:
                        v = val_desc.replace("\n", " ").replace("\r", " ").strip()
                        if len(v) > 200:
                            v = v[:200] + "..."
                        desc_parts.append(v)

                    if desc_parts:
                        lines.append(f"  - {col_name}: {'; '.join(desc_parts)}")
                    else:
                        lines.append(f"  - {col_name}")

                if len(lines) > 1:
                    body = "\n".join(lines)
                    chunk_id = f"bird:{db_id}:{table_name}"
                    chunks.append({
                        "id": chunk_id,
                        "document": body,
                        "metadata": {
                            "db_id": db_id,
                            "table_name": table_name,
                            "chunk_type": "domain",
                            "source_path": f"bird://{db_id}/{table_name}",
                        },
                    })
            except Exception as e:
                print(f"  Warning: {db_id}/{fname}: {e}")

        # ── Schema chunks: reflect DDL from SQLite file ──
        db_file = os.path.join(db_dir, db_id, f"{db_id}.sqlite")
        if os.path.isfile(db_file):
            try:
                conn = sqlite3.connect(db_file)
                cur = conn.execute(
                    "SELECT tbl_name, sql FROM sqlite_master "
                    "WHERE type='table' AND sql IS NOT NULL"
                )
                for table_name, ddl in cur.fetchall():
                    chunk_id = f"bird:schema:{db_id}:{table_name}"
                    chunks.append({
                        "id": chunk_id,
                        "document": ddl,
                        "metadata": {
                            "db_id": db_id,
                            "table_name": table_name,
                            "chunk_type": "schema",
                            "source_path": f"bird://{db_id}/{table_name}?type=schema",
                        },
                    })
                conn.close()
            except Exception as e:
                print(f"  Warning: {db_id}/schema: {e}")

    if chunks:
        col.add(
            ids=[c["id"] for c in chunks],
            documents=[c["document"] for c in chunks],
            metadatas=[c["metadata"] for c in chunks],
        )

    print(f"Indexed {len(chunks)} chunks from {len(os.listdir(db_dir))} databases into 'bird_minidev'")
    return len(chunks)


if __name__ == "__main__":
    build_bird_index()
