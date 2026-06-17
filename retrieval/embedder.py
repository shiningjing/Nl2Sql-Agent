"""Shared SentenceTransformer singleton — eager init.

Loads once at import time and pre-registers in ChromaDB's class-level model cache
so SentenceTransformerEmbeddingFunction (rag_retrieve + fewshot_retrieve) reuses
the same instance. Direct consumers call get_embedder().

Avoids loading BAAI/bge-small-zh-v1.5 multiple times (~100MB each).
"""

from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from sentence_transformers import SentenceTransformer

from storage.config import Config

_embedder = SentenceTransformer(Config.EMBED_MODEL_NAME, local_files_only=True)
SentenceTransformerEmbeddingFunction.models[Config.EMBED_MODEL_NAME] = _embedder


def get_embedder() -> SentenceTransformer:
    return _embedder
