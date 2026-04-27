"""
解析 OpenAI `message.content`；实现位于 ``pipeline.openai_gateway.chat_content``，仅重导以保持与旧 import 路径兼容。
"""
from __future__ import annotations

from pipeline.openai_gateway.chat_content import normalize_message_content

__all__ = ["normalize_message_content"]
