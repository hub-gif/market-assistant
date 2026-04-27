"""
与 ``pipeline.openai_gateway.estimate.estimate_chat_input_tokens`` 同口径的保守估算。
"""
from __future__ import annotations

from pipeline.openai_gateway.estimate import estimate_chat_input_tokens as estimate_crawler_style_input_tokens

__all__ = ["estimate_crawler_style_input_tokens"]
