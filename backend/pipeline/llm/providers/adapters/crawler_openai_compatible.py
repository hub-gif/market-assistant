"""
经 ``pipeline.openai_gateway.text_chat.chat_completion_text`` 访问 OpenAI 兼容网关（与配料识别等共用环境变量，不再 import 爬虫目录）。
"""
from __future__ import annotations

import os

from pipeline.openai_gateway import chat_completion_text

from ..shared.token_heuristics import estimate_crawler_style_input_tokens


def _llm_context_window_size_from_env() -> int:
    raw = (
        os.environ.get("LLM_CONTEXT_WINDOW")
        or os.environ.get("OPENAI_CONTEXT_WINDOW")
        or "32768"
    ).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return 32768


class CrawlerOpenAiCompatibleTextLlm:
    """
    文本任务默认后端：与历史 ``AI_crawler.chat_completion_text`` 行为一致，实现位于 ``pipeline.openai_gateway``。
    """

    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        kwargs: dict[str, object] = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        return chat_completion_text(**kwargs)

    def estimate_input_tokens(self, system_prompt: str, user_prompt: str) -> int:
        return estimate_crawler_style_input_tokens(system_prompt, user_prompt)

    def context_window_tokens(self) -> int:
        return _llm_context_window_size_from_env()
