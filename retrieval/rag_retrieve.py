"""Semantic retrieval from ChromaDB."""
import chromadb
import threading as _threading
from chromadb.utils import embedding_functions
import retrieval.embedder  # ensure shared model pre-registered in ChromaDB class cache  # noqa: F401
from storage.config import Config


_collection = None
_ef = None
_ef_lock = _threading.Lock()
_col_lock = _threading.Lock()


def _get_embedding_function():
    global _ef
    if _ef is None:
        with _ef_lock:
            if _ef is None:
                _ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                    model_name=Config.EMBED_MODEL_NAME
                )
    return _ef


def get_collection():
    global _collection
    if _collection is None:
        with _col_lock:
            if _collection is None:
                client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
                _collection = client.get_or_create_collection(
                    name="nl2sql_corpus",
                    embedding_function=_get_embedding_function(),
                )
    return _collection


def ingest_chunks(chunks: list[dict], reset: bool = False):
    """Index chunks into Chroma. If reset, clear and rebuild."""
    col = get_collection()
    if reset:
        client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
        client.delete_collection("nl2sql_corpus")
        global _collection
        _collection = None
        col = get_collection()

    if not chunks:
        return

    col.add(
        ids=[c["chunk_id"] for c in chunks],
        documents=[c["content"] for c in chunks],
        metadatas=[c.get("metadata", {}) for c in chunks],
    )


def retrieve(question: str, k: int = 6) -> list[dict]:
    """Search top-k relevant chunks for a question."""
    col = get_collection()
    results = col.query(query_texts=[question], n_results=k)
    items = []
    if results["ids"] and results["ids"][0]:
        for i, doc_id in enumerate(results["ids"][0]):
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            items.append({
                "content": results["documents"][0][i] if results["documents"] else "",
                "source": meta.get("source_path", ""),
                "metadata": meta,
            })
    return items


def format_retrieved(chunks: list[dict]) -> str:
    """Format retrieved chunks for LLM prompt injection."""
    if not chunks:
        return ""
    blocks = []
    for i, c in enumerate(chunks):
        blocks.append(f"[{i+1}] (source: {c['source']})\n{c['content']}")
    return "\n\n".join(blocks)


_bird_collection = None
_bird_lock = _threading.Lock()


def get_bird_collection():
    """Get the BIRD Mini-Dev ChromaDB collection (must be built via ingest_bird.py)."""
    global _bird_collection
    if _bird_collection is None:
        with _bird_lock:
            if _bird_collection is None:  # double-check
                client = chromadb.PersistentClient(path=Config.CHROMA_PERSIST_DIR)
                _bird_collection = client.get_collection(
                    name="bird_minidev",
                    embedding_function=_get_embedding_function(),
                )
    return _bird_collection


def retrieve_bird(question: str, db_id: str, k: int = 6) -> list[dict]:
    """Search BIRD collection for domain knowledge, filtered to one database."""
    col = get_bird_collection()
    results = col.query(
        query_texts=[question],
        n_results=k,
        where={"db_id": db_id},
    )
    items = []
    if results["ids"] and results["ids"][0]:
        for i, doc_id in enumerate(results["ids"][0]):
            meta = results["metadatas"][0][i] if results["metadatas"] else {}
            items.append({
                "content": results["documents"][0][i] if results["documents"] else "",
                "source": meta.get("source_path", ""),
                "metadata": meta,
            })
    return items


def build_prompt_context(chunks: list[dict]) -> dict:
    """Split retrieval results into schema_chunks and notes_chunks.

    Returns {"schema_text": str, "notes_text": str}
    """
    schema_parts = []
    notes_parts = []

    for c in chunks:
        chunk_type = c.get("metadata", {}).get("chunk_type", "domain") if isinstance(c.get("metadata"), dict) else "domain"
        if chunk_type == "schema":
            schema_parts.append(c["content"])
        else:
            heading = ""
            if isinstance(c.get("metadata"), dict):
                heading = c["metadata"].get("heading", "")
            notes_parts.append(f"## {heading}\n{c['content']}" if heading else c["content"])

    schema_text = ""
    if schema_parts:
        schema_text = "\n\n".join(schema_parts)

    notes_text = ""
    if notes_parts:
        notes_text = "\n\n".join(notes_parts)

    return {"schema_text": schema_text, "notes_text": notes_text}
