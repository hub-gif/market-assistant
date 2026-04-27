"""
根据 `MA_LLM_TEXT_PROVIDER` 选择文本大模型实现；未设置时与历史行为一致（经 AI_crawler 的 OpenAI 兼容网关）。
"""
from __future__ import annotations

import os

from .crawler_openai_compatible import CrawlerOpenAiCompatibleTextLlm
from .protocol import TextLlmClient

# 模块级单例：避免重复构造；测试可用 `reset_text_llm_client_for_tests` 切换实现。
_client: TextLlmClient | None = None

# 与历史默认行为一致
_DEFAULT_ID = "crawler_openai_compatible"
_ENV_KEY = "MA_LLM_TEXT_PROVIDER"


def _provider_id() -> str:
    raw = (os.environ.get(_ENV_KEY) or _DEFAULT_ID).strip().lower()
    return raw or _DEFAULT_ID


def _build_client(pid: str) -> TextLlmClient:
    if pid in (
        "crawler_openai_compatible",
        "crawler",
        "default",
        "openai_compatible",
    ):
        return CrawlerOpenAiCompatibleTextLlm()
    raise ValueError(
        f"不支持的 {_ENV_KEY}={pid!r}；"
        f"当前仅实现 {_DEFAULT_ID}（经 AI_crawler 的 OpenAI 兼容 `chat/completions`）。"
    )


def get_text_llm() -> TextLlmClient:
    global _client
    if _client is None:
        _client = _build_client(_provider_id())
    return _client


def reset_text_llm_client_for_tests() -> None:
    """供 pytest/集成测试在修改环境变量后清空缓存的客户端。"""
    global _client
    _client = None
