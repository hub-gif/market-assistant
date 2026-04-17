"""竞品报告 LLM 调用：路径注入与网关 ``chat_completion_text`` 封装。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from django.conf import settings


def ensure_ai_crawler_path() -> None:
    root = Path(settings.CRAWLER_JD_ROOT).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"爬虫副本目录不存在: {root}")
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)


def call_llm(system_prompt: str, user_prompt: str) -> str:
    ensure_ai_crawler_path()
    import AI_crawler as ac  # noqa: WPS433

    raw = ac.chat_completion_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    return ac.strip_outer_markdown_fence(raw)


def estimate_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    """与 ``AI_crawler._estimate_chat_input_tokens`` 一致，用于在调用前预判上下文。"""
    total_chars = len(system_prompt or "") + len(user_prompt or "")
    return int(total_chars * 0.55) + 512


def llm_context_window_size() -> int:
    """与 ``AI_crawler.chat_completion_text`` 使用的上下文上限一致。"""
    raw = (
        os.environ.get("LLM_CONTEXT_WINDOW")
        or os.environ.get("OPENAI_CONTEXT_WINDOW")
        or "32768"
    ).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return 32768
