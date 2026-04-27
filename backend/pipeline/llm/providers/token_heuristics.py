"""
与 `crawler_copy/.../AI_crawler` 中 `_estimate_chat_input_tokens` 同口径的保守估算，
供预检、策略档位与 OpenAI 兼容适配器共用（无 tiktoken 时避免 max_tokens 400）。
"""
from __future__ import annotations


def estimate_crawler_style_input_tokens(system_prompt: str, user_prompt: str) -> int:
    total_chars = len(system_prompt or "") + len(user_prompt or "")
    return int(total_chars * 0.55) + 512
