"""Decomposer node — break complex questions into sub-question DAG."""
import json
import re

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

from nl2sql.config import Config
from src.agent.state import AgentState

from src.prompts import DECOMPOSER_SYSTEM_PROMPT as SYSTEM_PROMPT


def _get_chat():
    return ChatOpenAI(
        model=Config.LLM_CHAT_MODEL,
        api_key=Config.LLM_API_KEY,
        base_url=Config.LLM_BASE_URL,
        temperature=0,
        request_timeout=45,
        max_retries=0,
    )


def _extract_json(response: str) -> dict:
    """Defensive JSON extraction."""
    # Try ```json block first
    m = re.search(r"```(?:json)?\s*(.*?)```", response, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    # Try raw JSON
    m = re.search(r"\{.*\}", response, re.DOTALL)
    if m:
        return json.loads(m.group(0))
    return {}


def _extract_token_usage(response) -> dict:
    tu = response.response_metadata.get("token_usage", {}) if hasattr(response, "response_metadata") else {}
    return {
        "prompt": tu.get("prompt_tokens", 0),
        "completion": tu.get("completion_tokens", 0),
        "total": tu.get("total_tokens", 0),
    }


def decomposer_node(state: AgentState) -> dict:
    """Break complex question into sub-question DAG. LLM call."""
    question = state["question"]

    tlog = state.get("tlog")
    if tlog:
        tlog.node_enter("decomposer", {"question": question[:80]})

    schema_text = state.get("schema_text", "")
    notes_text = state.get("notes_text", "")

    chat = _get_chat()

    parts = [f"## SCHEMA\n{schema_text}"]
    if notes_text:
        parts.append(f"## RETRIEVED NOTES\n{notes_text}")
    parts.append(f"## COMPLEX QUESTION\n{question}")

    user_message = "\n\n".join(parts)

    try:
        import time as _time
        _t0 = _time.time()
        response = chat.invoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_message),
        ])
        _duration = round(_time.time() - _t0, 3)
        raw = response.content
        tu = _extract_token_usage(response)
        parsed = _extract_json(raw)
        steps = parsed.get("steps", [])
    except Exception as e:
        if tlog:
            tlog.llm_error(Config.LLM_CHAT_MODEL, type(e).__name__, str(e)[:300])
            tlog.node_exit("decomposer", {"error": str(e)[:120]})
        raise

    # Single step = LLM decided no decomposition needed.
    # Clear sub_questions so Generator doesn't inject unnecessary CTE instruction.
    if len(steps) == 1:
        steps = []

    # Accumulate token usage
    token_usage = dict(state.get("token_usage", {"prompt": 0, "completion": 0, "total": 0}))
    token_usage = {k: token_usage[k] + tu.get(k, 0) for k in token_usage}

    if tlog:
        tlog.llm_call(Config.LLM_CHAT_MODEL, tu, _duration)
        tlog.node_exit("decomposer", {"step_count": len(steps)})

    return {
        "sub_questions": steps,
        "token_usage": token_usage,
    }
