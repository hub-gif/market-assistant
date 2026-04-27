"""竞品报告 LLM 调用：经 `providers` 工厂选择后端，并统一对输出做去围栏等归一化。"""
from __future__ import annotations

from .providers.factory import get_text_llm
from .providers.shared.output_normalize import strip_outer_markdown_fence


def call_llm(
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float | None = None,
) -> str:
    raw = get_text_llm().complete_text(
        system_prompt,
        user_prompt,
        temperature=temperature,
    )
    return strip_outer_markdown_fence(raw)


def estimate_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    """与当前所选文本后端的预检一致；默认与 ``AI_crawler`` 的保守估算同口径。"""
    return get_text_llm().estimate_input_tokens(system_prompt, user_prompt)


def llm_context_window_size() -> int:
    """与当前所选后端的上下文上限一致；默认与 ``AI_crawler.chat_completion_text`` 使用的环境变量一致。"""
    return get_text_llm().context_window_tokens()

