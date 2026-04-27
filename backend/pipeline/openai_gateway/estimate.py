"""输入 token 保守估算（与历史 AI_crawler 同口径）。"""
from __future__ import annotations


def estimate_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    total_chars = len(system_prompt or "") + len(user_prompt or "")
    return int(total_chars * 0.55) + 512
