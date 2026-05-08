"""输入 token 保守估算（与历史 AI_crawler 同口径）。"""
from __future__ import annotations

import os


def estimate_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    total_chars = len(system_prompt or "") + len(user_prompt or "")
    return int(total_chars * 0.55) + 512


def budgeted_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    """
    用于 ``max_tokens`` 收紧、策略上下文压缩阈值等与**网关真实 tokenizer**对齐的输入侧估算。

    在 ``estimate_chat_input_tokens`` 之上追加 ``LLM_INPUT_TOKEN_BUDGET_PAD``（默认 2048），
    缓解中文/混排正文下本地启发式**系统性偏低**导致的 ``max_tokens > context − prompt`` 400。
    """
    base = estimate_chat_input_tokens(system_prompt, user_prompt)
    raw = (os.environ.get("LLM_INPUT_TOKEN_BUDGET_PAD") or "2048").strip()
    try:
        pad = max(0, int(raw))
    except ValueError:
        pad = 2048
    return base + pad


def completion_budget_slack_tokens() -> int:
    """
    从「可分配给 completion 的 token」中额外扣减；与 ``budgeted_chat_input_tokens`` 叠加后，
    尽量贴近网关对 ``max_tokens`` 的上限校验。
    """
    raw = (os.environ.get("LLM_COMPLETION_CONTEXT_SLACK") or "1024").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 1024
