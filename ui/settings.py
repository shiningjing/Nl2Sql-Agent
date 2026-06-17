"""Settings page — pipeline and LLM configuration."""
import streamlit as st
from storage.config import Config


def _init_session():
    """Lazily initialize settings in session state."""
    defaults = {
        "selected_db_name": None,
        "multi_candidate": True,
        "rag_k": 8,
        "rag_column_prune": False,
        "fewshot_enabled": True,
        "rag_fk_expand": True,
        "llm_model": Config.LLM_CHAT_MODEL,
        "llm_base_url": Config.LLM_BASE_URL,
        "llm_api_key": Config.LLM_API_KEY,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def render_settings():
    _init_session()

    st.title("Settings")

    col1, col2 = st.columns(2)

    # ── Left column: Pipeline ──
    with col1:
        st.subheader("Pipeline")
        st.session_state.multi_candidate = st.toggle(
            "Multi-Candidate", value=st.session_state.multi_candidate,
            help="Generate SQL with 3 temperatures, vote by execution result")
        st.session_state.rag_k = st.slider(
            "RAG chunks", 2, 20, st.session_state.rag_k,
            help="Number of schema + domain chunks to retrieve")
        st.session_state.rag_column_prune = st.toggle(
            "Column Prune", value=st.session_state.rag_column_prune,
            help="Drop low-relevance columns from DDL to reduce prompt size")
        st.session_state.fewshot_enabled = st.toggle(
            "Few-Shot", value=st.session_state.fewshot_enabled,
            help="Retrieve similar Q-SQL pairs from the same database")
        st.session_state.rag_fk_expand = st.toggle(
            "FK Expand", value=st.session_state.rag_fk_expand,
            help="Include 1-hop foreign-key neighbor tables in schema")

    # ── Right column: LLM ──
    with col2:
        st.subheader("LLM Configuration")

        preset_names = [p["name"] for p in Config.LLM_PRESETS]

        current_model = st.session_state.llm_model
        current_base = st.session_state.llm_base_url
        default_idx = len(preset_names) - 1  # "Custom"
        for i, p in enumerate(Config.LLM_PRESETS):
            if p["name"] != "Custom" and p["model"] == current_model and p["base_url"] == current_base:
                default_idx = i
                break

        if "llm_preset" not in st.session_state:
            st.session_state.llm_preset = preset_names[default_idx]

        selected_preset = st.selectbox(
            "Provider", options=preset_names, index=default_idx,
            help="切换提供商自动填充模型名、API 地址和密钥")

        # Only auto-fill when user actually switches preset
        if selected_preset != st.session_state.llm_preset:
            st.session_state.llm_preset = selected_preset
            if selected_preset != "Custom":
                preset = Config.LLM_PRESETS[default_idx]
                st.session_state.llm_model = preset["model"]
                st.session_state.llm_base_url = preset["base_url"]
                if preset.get("key_field"):
                    keys = Config.load_llm_keys()
                    json_key = keys.get(preset["key_field"], "")
                    if json_key:
                        st.session_state.llm_api_key = json_key

        st.session_state.llm_model = st.text_input(
            "Model", value=st.session_state.llm_model,
            help="模型名，传递给 API")
        st.session_state.llm_base_url = st.text_input(
            "API Base URL", value=st.session_state.llm_base_url,
            help="例如 https://api.deepseek.com/v1")
        st.session_state.llm_api_key = st.text_input(
            "API Key", value=st.session_state.llm_api_key,
            type="password", help="优先用 llm_keys.json，为空则用 .env 兜底")

        st.divider()
        st.caption(f"Config 兜底: {Config.LLM_CHAT_MODEL} @ {Config.LLM_BASE_URL}")
