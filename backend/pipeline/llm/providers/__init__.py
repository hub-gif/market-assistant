"""文本大模型调用的协议、适配器与工厂（与具体提示词/业务生成逻辑解耦）。"""
from __future__ import annotations

from .factory import get_text_llm, reset_text_llm_client_for_tests
from .protocol import TextLlmClient

__all__ = [
    "TextLlmClient",
    "get_text_llm",
    "reset_text_llm_client_for_tests",
]
