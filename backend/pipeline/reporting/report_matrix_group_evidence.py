"""
从宿主报告 ``competitor_analysis.md`` 中按细类名抽取 **Markdown 四级标题** ``#### {细类名}``
下的正文，用于策略生成时并入「与同细类对齐」的大模型归纳（第五～第八章各块）。

报告生成侧约定：矩阵/价盘/促销/评论/场景等 LLM 小节均以 ``#### `` + 与矩阵一致的细类名为小节标题
（见 ``generate_group_summaries`` 系统提示）。
**8.3 评价正/负向主题**：细类下内层小节使用 ``#####``（由 ``demote_sentiment_inner_h4_to_h5_for_matrix_group`` 处理），
避免与外层 ``#### 细类名`` 同级导致本节正文抽取为空。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Literal


def extract_level4_sections_by_group_title(md: str, group_title: str) -> list[str]:
    """
    返回全文内所有 ``#### {group_title}`` 小节正文（不含标题行），按出现顺序。
    标题须与 ``group_title`` 去首尾空白后**完全一致**。
    """
    title = (group_title or "").strip()
    if not title or not (md or "").strip():
        return []

    lines = md.splitlines()
    n = len(lines)
    blocks: list[str] = []
    i = 0
    while i < n:
        line = lines[i]
        m = re.match(r"^####\s+(.+?)\s*$", line)
        if m and m.group(1).strip() == title:
            i += 1
            chunk: list[str] = []
            while i < n:
                nxt = lines[i]
                if re.match(r"^####\s", nxt):
                    break
                if re.match(r"^###\s", nxt) or re.match(r"^##\s", nxt):
                    break
                if re.match(r"^#\s", nxt) and not nxt.startswith("##"):
                    break
                chunk.append(nxt)
                i += 1
            body = "\n".join(chunk).strip()
            if body:
                blocks.append(body)
            continue
        i += 1
    return blocks


def load_report_matrix_group_evidence_markdown(
    run_dir: Path | str,
    group_title: str,
    *,
    max_chars: int = 28_000,
) -> tuple[str, Literal["competitor_analysis_md", "none"]]:
    """
    读取 ``run_dir/competitor_analysis.md``，抽取该细类在各章大模型小节下的归纳，拼接为一段 Markdown。

    若文件不存在或无任何匹配小节，返回 ``("", "none")``。
    """
    root = Path(run_dir)
    path = root / "competitor_analysis.md"
    cap = max(512, int(max_chars))
    if not path.is_file():
        return "", "none"
    try:
        full = path.read_text(encoding="utf-8")
    except OSError:
        return "", "none"

    parts = extract_level4_sections_by_group_title(full, group_title)
    if not parts:
        return "", "none"

    intro = (
        f"> **说明**：以下为同任务《竞品分析报告》正文中、细类「**{group_title.strip()}**」下 "
        "「#### …」小节的**大模型归纳**摘录（按正文出现顺序拼接），"
        "覆盖矩阵/价盘/促销/评论与场景等块中**已生成**的段落；"
        "若某块未开 LLM 或未产出对应小节，则不会出现在此摘录中。\n\n"
    )
    sep = "\n\n---\n\n"
    body = intro + sep.join(parts)
    if len(body) <= cap:
        return body, "competitor_analysis_md"
    tail = "\n\n…（已截断）\n"
    room = max(400, cap - len(tail))
    return body[: room].rstrip() + tail, "competitor_analysis_md"


__all__ = [
    "extract_level4_sections_by_group_title",
    "load_report_matrix_group_evidence_markdown",
]
