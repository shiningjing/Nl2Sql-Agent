import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.deepseek.com/v1")
    LLM_API_KEY = os.getenv("LLM_API_KEY", "")
    LLM_CHAT_MODEL = os.getenv("LLM_CHAT_MODEL", "deepseek-v4-pro")
    SQL_DIALECT = os.getenv("SQL_DIALECT", "sqlite")
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/demo.db")
    CHROMA_PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "./.chroma")
    CORPUS_DIR = os.getenv("CORPUS_DIR", "./corpus")
    EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "BAAI/bge-small-zh-v1.5")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
