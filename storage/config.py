import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_CFG_DIR = Path(__file__).resolve().parent.parent
_LLM_KEYS_PATH = _CFG_DIR / "llm_keys.json"


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

    # LLM provider presets — each maps to a key in llm_keys.json
    LLM_PRESETS: list[dict] = [
        {"name": "DeepSeek V4 Pro",   "model": "deepseek-v4-pro",   "base_url": "https://api.deepseek.com/v1",     "key_field": "deepseek"},
        {"name": "DeepSeek Chat",     "model": "deepseek-chat",     "base_url": "https://api.deepseek.com/v1",     "key_field": "deepseek"},
        {"name": "DeepSeek Reasoner", "model": "deepseek-reasoner", "base_url": "https://api.deepseek.com/v1",     "key_field": "deepseek"},
        {"name": "OpenAI GPT-4o",     "model": "gpt-4o",            "base_url": "https://api.openai.com/v1",      "key_field": "openai"},
        {"name": "OpenAI GPT-4o-mini","model": "gpt-4o-mini",       "base_url": "https://api.openai.com/v1",      "key_field": "openai"},
        {"name": "Claude Opus 4.7",   "model": "claude-opus-4-7",   "base_url": "https://api.anthropic.com/v1",   "key_field": "anthropic"},
        {"name": "Claude Sonnet 4.6", "model": "claude-sonnet-4-6", "base_url": "https://api.anthropic.com/v1",   "key_field": "anthropic"},
        {"name": "Custom",            "model": "",                   "base_url": "",                               "key_field": ""},
    ]

    @staticmethod
    def load_llm_keys() -> dict:
        """Load API keys from llm_keys.json, return empty dict on failure."""
        try:
            with open(_LLM_KEYS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    @staticmethod
    def save_llm_keys(keys: dict) -> None:
        with open(_LLM_KEYS_PATH, "w", encoding="utf-8") as f:
            json.dump(keys, f, indent=2, ensure_ascii=False)
