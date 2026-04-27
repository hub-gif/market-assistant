"""对模型输出去围栏等；与 ``openai_gateway.text_chat.strip_outer_markdown_fence`` 同义。"""
from __future__ import annotations

from pipeline.openai_gateway.text_chat import strip_outer_markdown_fence

__all__ = ["strip_outer_markdown_fence"]
