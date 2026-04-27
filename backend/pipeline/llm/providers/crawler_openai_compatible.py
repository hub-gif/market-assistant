"""
经 `crawler_copy/jd_pc_search/AI_crawler.chat_completion_text` 访问 OpenAI 兼容网关（与配料识别等共用凭据与配置）。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from django.conf import settings

from .token_heuristics import estimate_crawler_style_input_tokens


def ensure_ai_crawler_path() -> None:
    root = Path(settings.CRAWLER_JD_ROOT).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"爬虫副本目录不存在: {root}")
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)


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
    文本任务默认后端：复用 `AI_crawler` 的 `chat_completion_text` 与上下文预检逻辑。
    更换为其它云厂商时，应新增独立适配器并在 `factory` 中注册，而非修改本类。
    """

    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        ensure_ai_crawler_path()
        import AI_crawler as ac  # noqa: WPS433

        kwargs: dict[str, object] = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        return ac.chat_completion_text(**kwargs)

    def estimate_input_tokens(self, system_prompt: str, user_prompt: str) -> int:
        return estimate_crawler_style_input_tokens(system_prompt, user_prompt)

    def context_window_tokens(self) -> int:
        return _llm_context_window_size_from_env()
