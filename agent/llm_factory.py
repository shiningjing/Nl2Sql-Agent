"""LLM factory — contextvars-based per-request model configuration.

Supports OpenAI-compatible APIs (ChatOpenAI) and native Anthropic (ChatAnthropic).
Detection is automatic based on base_url or model name.

Usage:
  # At request entry point:
  set_llm_config(model="deepseek-v4-pro", api_key="sk-xxx", base_url="https://...")

  # In any node:
  chat = get_llm(temperature=0, max_tokens=80)
"""

import contextvars
from langchain_openai import ChatOpenAI
from storage.config import Config as _Config

_llm_config: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "llm_config", default=None
)


def set_llm_config(model: str, api_key: str, base_url: str = ""):
    """Set per-request LLM config. Call once at API / Streamlit entry point."""
    _llm_config.set({
        "model": model,
        "api_key": api_key,
        "base_url": base_url or "",
    })


def clear_llm_config():
    """Reset to env defaults. Call after request if needed."""
    _llm_config.set(None)


def _is_anthropic(model: str, base_url: str) -> bool:
    """Detect whether the config points to Anthropic native API."""
    return ("anthropic" in (base_url or "").lower()
            or "claude" in (model or "").lower())


def get_llm(**overrides):
    """Get a chat model for the current request context.

    Automatically routes to ChatAnthropic or ChatOpenAI based on base_url / model.
    Falls back to env Config when no per-request config is set.

    Common overrides: temperature, max_tokens, streaming
    OpenAI-only: request_timeout. Anthropic-only: timeout.
    """
    cfg = _llm_config.get()
    model = (cfg["model"] if cfg else None) or _Config.LLM_CHAT_MODEL
    api_key = (cfg["api_key"] if cfg else None) or _Config.LLM_API_KEY
    base_url = (cfg["base_url"] if cfg and cfg["base_url"] else None) or _Config.LLM_BASE_URL

    if _is_anthropic(model, base_url):
        from langchain_anthropic import ChatAnthropic

        # Map OpenAI-style param names to Anthropic field names
        if "request_timeout" in overrides:
            overrides.setdefault("default_request_timeout", overrides.pop("request_timeout"))
        if "api_key" in overrides:
            overrides.setdefault("anthropic_api_key", overrides.pop("api_key"))

        return ChatAnthropic(
            model=model,
            anthropic_api_key=api_key,
            anthropic_api_url=base_url,
            **overrides,
        )
    else:
        return ChatOpenAI(
            model=model,
            api_key=api_key,
            base_url=base_url,
            **overrides,
        )
