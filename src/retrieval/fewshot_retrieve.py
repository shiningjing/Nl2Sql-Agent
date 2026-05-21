"""Few-shot retrieval from ChromaDB — separate collection for (Q, SQL) pairs."""
import os
import re
import chromadb
from chromadb.utils import embedding_functions
import src.shared_embedder  # ensure shared model pre-registered in ChromaDB class cache  # noqa: F401
from nl2sql.config import Config

_collection = None
_ef = None


def _get_embedding_function():
    global _ef
    if _ef is None:
        _ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=Config.EMBED_MODEL_NAME
        )
    return _ef


def get_fewshot_collection():
    global _collection
    if _collection is None:
        client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
        _collection = client.get_or_create_collection(
            name="nl2sql_fewshot",
            embedding_function=_get_embedding_function(),
        )
    return _collection


def load_fewshot_corpus(corpus_dir: str) -> list[dict]:
    """Parse fewshot Markdown files into (Q, SQL) chunks.

    Each .md file contains:
      ### Q
      <question>
      ### SQL
      SELECT ...

    Returns list of {"chunk_id": str, "content": str, "metadata": dict}
    """
    fewshot_dir = os.path.join(corpus_dir, "fewshot")
    if not os.path.isdir(fewshot_dir):
        return []

    chunks = []
    for fname in sorted(os.listdir(fewshot_dir)):
        if not fname.endswith(".md"):
            continue
        fpath = os.path.join(fewshot_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            text = f.read()

        # Extract Q and SQL sections
        q_match = re.search(r"###\s*Q\s*\n(.+?)(?:\n###|\Z)", text, re.DOTALL | re.IGNORECASE)
        sql_match = re.search(r"###\s*SQL\s*\n(.+)", text, re.DOTALL | re.IGNORECASE)

        if not q_match or not sql_match:
            continue

        question = q_match.group(1).strip()
        sql = sql_match.group(1).strip()

        chunk_id = f"fewshot_{fname.replace('.md', '')}"
        chunks.append({
            "chunk_id": chunk_id,
            "content": question,
            "metadata": {
                "source_path": fpath,
                "question": question,
                "sql": sql,
            },
        })

    return chunks


def ingest_fewshot_chunks(chunks: list[dict], reset: bool = False):
    """Index few-shot chunks into Chroma (separate collection)."""
    col = get_fewshot_collection()
    if reset:
        client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
        client.delete_collection("nl2sql_fewshot")
        global _collection
        _collection = None
        col = get_fewshot_collection()

    if not chunks:
        return

    col.add(
        ids=[c["chunk_id"] for c in chunks],
        documents=[c["content"] for c in chunks],
        metadatas=[c.get("metadata", {}) for c in chunks],
    )


def retrieve_fewshot(question: str, k: int = 3) -> list[dict]:
    """Search top-k similar (Q, SQL) pairs for a question.

    Returns list of {"question": str, "sql": str, "source": str}
    """
    col = get_fewshot_collection()
    if col.count() == 0:
        return []

    results = col.query(query_texts=[question], n_results=min(k, col.count()))
    items = []
    if results["ids"] and results["ids"][0]:
        for i, doc_id in enumerate(results["ids"][0]):
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            items.append({
                "question": meta.get("question", ""),
                "sql": meta.get("sql", ""),
                "source": meta.get("source_path", ""),
            })
    return items


def retrieve_fewshot_for_db(question: str, db_id: str, k: int = 3) -> list[dict]:
    """Search top-k similar (Q, SQL) pairs filtered by db_id.

    Returns list of {"question": str, "sql": str, "source": str}
    """
    col = get_fewshot_collection()
    if col.count() == 0:
        return []

    results = col.query(
        query_texts=[question],
        n_results=min(k, col.count()),
        where={"db_id": db_id},
    )
    items = []
    if results["ids"] and results["ids"][0]:
        for i, doc_id in enumerate(results["ids"][0]):
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            items.append({
                "question": meta.get("question", ""),
                "sql": meta.get("sql", ""),
                "source": meta.get("source_path", ""),
            })
    return items


def format_fewshot(items: list[dict]) -> str:
    """Format few-shot examples for LLM prompt injection."""
    if not items:
        return ""
    blocks = []
    for i, item in enumerate(items):
        blocks.append(f"Example {i+1}:\nQ: {item['question']}\nSQL: {item['sql']}")
    return "\n\n".join(blocks)


def get_all_fewshot() -> list[dict]:
    """Return all few-shot examples with their metadata and embeddings.

    Returns list of {"id": str, "question": str, "sql": str, "source": str, "embedding": list[float]}
    """
    col = get_fewshot_collection()
    if col.count() == 0:
        return []

    result = col.get(include=["documents", "metadatas", "embeddings"])
    items = []
    embeddings_list = result.get("embeddings")
    embeddings_list = list(embeddings_list) if embeddings_list is not None and len(embeddings_list) > 0 else []
    metadatas_list = result.get("metadatas")
    metadatas_list = list(metadatas_list) if metadatas_list is not None and len(metadatas_list) > 0 else []
    for i, doc_id in enumerate(result["ids"]):
        meta = metadatas_list[i] if i < len(metadatas_list) else {}
        emb = embeddings_list[i] if i < len(embeddings_list) else None
        # Convert numpy array to list if needed
        if emb is not None and hasattr(emb, "tolist"):
            emb = emb.tolist()
        items.append({
            "id": doc_id,
            "question": meta.get("question", ""),
            "sql": meta.get("sql", ""),
            "source": meta.get("source_path", ""),
            "embedding": emb,
        })
    return items


def delete_fewshot_ids(ids: list[str]):
    """Delete specific examples from the fewshot collection by ID."""
    col = get_fewshot_collection()
    col.delete(ids=ids)


def get_fewshot_count() -> int:
    """Return number of examples in the fewshot collection."""
    return get_fewshot_collection().count()


def export_fewshot_to_markdown(items: list[dict], output_dir: str):
    """Write few-shot examples back to .md files."""
    import json
    os.makedirs(output_dir, exist_ok=True)
    for i, item in enumerate(items):
        fname = f"{i+1:02d}.md"
        fpath = os.path.join(output_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(f"### Q\n{item['question']}\n\n### SQL\n{item['sql']}\n")
    return len(items)
