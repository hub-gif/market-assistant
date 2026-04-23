"""
从宿主报告 ``competitor_analysis.md`` 中按细类名抽取 **Markdown 四级标题** ``#### {细类名}``
下的正文，用于策略生成时并入「与同细类对齐」的大模型归纳（第五～第八章各块）。

报告生成侧约定：矩阵/价盘/促销/评论/场景等 LLM 小节均以 ``#### `` + 与矩阵一致的细类名为小节标题
（见 ``generate_group_summaries`` 系统提示）。

**第八章 8.3**（``generate_comment_sentiment_analysis_llm``）在每组 ``#### {细类}`` 下还会再嵌套
``#### 正向体验主题`` 等四级标题（见 ``generate_sections.SENTIMENT_LLM_SYSTEM``），
通用抽取在遇到下一行 ``####`` 时即结束，会把 8.3 正文误判为空；故 8.3 单独解析后再拼回。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

# 与 ``jd_report.build_competitor_markdown`` 中 8.3 小节标题一致。
_SENTIMENT_83_HEADING = "### 8.3 评价正/负向主题（按细类 · 大模型）"

# 与 ``generate_sections.SENTIMENT_LLM_SYSTEM``「建议结构」四级标题一致；嵌套于 8.3 每组 ``#### 细类`` 之下。
_SENTIMENT_LLM_INNER_LEVEL4 = frozenset(
    {
        "正向体验主题",
        "负向评价主题归因",
        "混合评价中的典型张力",
        "使用注意",
    }
)


def _md_splice_out_sentiment_83_section(md: str) -> str:
    """去掉 8.3 整节，避免按细类抽取时把嵌套 ``####`` 误判为同级边界；其它章不变。"""
    i = md.find(_SENTIMENT_83_HEADING)
    if i < 0:
        return md
    tail = md[i + len(_SENTIMENT_83_HEADING) :]
    m = re.search(r"^##\s+", tail, re.MULTILINE)
    if not m:
        return md[:i].rstrip() + "\n\n"
    cut = i + len(_SENTIMENT_83_HEADING) + m.start()
    return md[:i].rstrip() + "\n\n" + md[cut:].lstrip("\n")


def extract_sentiment_83_level4_body(md: str, group_title: str) -> str:
    """
    仅在 ``### 8.3 …`` 节内，抽取 ``#### {group_title}`` 下正文（**不含**该标题行）。

    允许正文内出现 ``#### 正向体验主题`` 等情感归纳子标题；遇下一 peer ``####``（另一细类）或 ``###``/``##`` 则结束。
    """
    title = (group_title or "").strip()
    if not title or not (md or "").strip():
        return ""

    i = md.find(_SENTIMENT_83_HEADING)
    if i < 0:
        return ""
    tail = md[i + len(_SENTIMENT_83_HEADING) :]
    m_end = re.search(r"^##\s+", tail, re.MULTILINE)
    chunk = tail[: m_end.start()] if m_end else tail

    lines = chunk.splitlines()
    n = len(lines)
    j = 0
    while j < n:
        line = lines[j]
        m4 = re.match(r"^####\s+(.+?)\s*$", line)
        if m4 and m4.group(1).strip() == title:
            j += 1
            body_lines: list[str] = []
            while j < n:
                nxt = lines[j]
                mpeer = re.match(r"^####\s+(.+?)\s*$", nxt)
                if mpeer:
                    inner = mpeer.group(1).strip()
                    if inner in _SENTIMENT_LLM_INNER_LEVEL4:
                        body_lines.append(nxt)
                        j += 1
                        continue
                    break
                if re.match(r"^###\s", nxt) or re.match(r"^##\s", nxt):
                    break
                body_lines.append(nxt)
                j += 1
            return "\n".join(body_lines).strip()
        j += 1
    return ""


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

    without_83 = _md_splice_out_sentiment_83_section(full)
    parts = extract_level4_sections_by_group_title(without_83, group_title)
    s83 = extract_sentiment_83_level4_body(full, group_title)
    if s83:
        parts.append(s83)
    if not parts:
        return "", "none"

    intro = (
        f"> **说明**：以下为同任务《竞品分析报告》正文中、细类「**{group_title.strip()}**」下 "
        "「#### …」小节的**大模型归纳**摘录（按正文出现顺序拼接），"
        "覆盖矩阵/价盘/促销/评论与场景等块中**已生成**的段落，"
        "并含 **§8.3 评价正/负向主题（按细类 · 大模型）** 内该细类段落（允许嵌套四级小标题）；"
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
    "extract_sentiment_83_level4_body",
    "load_report_matrix_group_evidence_markdown",
]
