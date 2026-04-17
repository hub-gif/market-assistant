"""
从任务 run 目录加载「报告第九章 · 策略与机会」正文，供策略稿 LLM 与宿主报告对齐（阶段 S1）。

优先顺序：

1. ``strategy_opportunities_llm.json`` 中的 ``markdown``（与 runner 落盘一致）；
2. 否则从 ``competitor_analysis.md`` 截取 ``## 九、策略与机会提示`` 至 ``## 附录`` 之前。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

CHAPTER_NINE_HEADING = "## 九、策略与机会提示"


def extract_chapter_nine_strategy_markdown(full_md: str) -> str:
    """
    提取宿主报告中第九章策略块（含章节标题行），不含「附录」及之后内容。

    若未找到标题，返回空字符串。
    """
    t = full_md or ""
    if not t.strip():
        return ""
    key = CHAPTER_NINE_HEADING
    i = t.find(key)
    if i == -1:
        return ""
    chunk = t[i:]
    j = chunk.find("\n## 附录")
    if j != -1:
        chunk = chunk[:j]
    return chunk.rstrip()


def load_report_strategy_excerpt(
    run_dir: Path | str,
    *,
    max_chars: int = 24_000,
) -> tuple[str, Literal["json_markdown", "competitor_analysis_md", "none"]]:
    """
    返回 ``(节选正文, 来源标签)``。节选可能为空（未生成第九章或未找到标题）。
    """
    root = Path(run_dir)
    cap = max(512, int(max_chars))

    json_path = root / "strategy_opportunities_llm.json"
    if json_path.is_file():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = None
        if isinstance(data, dict):
            md = (data.get("markdown") or "").strip()
            if md:
                s = md if len(md) <= cap else md[: cap - 80].rstrip() + "\n\n…（已截断）\n"
                return s, "json_markdown"

    md_path = root / "competitor_analysis.md"
    if md_path.is_file():
        try:
            full = md_path.read_text(encoding="utf-8")
        except OSError:
            return "", "none"
        block = extract_chapter_nine_strategy_markdown(full)
        if block.strip():
            s = block if len(block) <= cap else block[: cap - 80].rstrip() + "\n\n…（已截断）\n"
            return s, "competitor_analysis_md"

    return "", "none"
