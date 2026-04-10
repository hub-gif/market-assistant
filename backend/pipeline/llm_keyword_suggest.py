"""在报告生成前：基于**全量**评价文本分块调用大模型，联想补充关注词（参与后续统计与报告）。"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from django.conf import settings

MAX_CHUNK_CHARS = 24_000
MAX_CHUNKS = 12

_CHUNK_SYSTEM = """你是电商评价挖掘助手。输入 JSON 含 keyword、excerpt_index、excerpts（一段用户评价正文合集）。
任务：从 excerpts 中抽取值得纳入「关注词/卖点监测」的**中文短语**（2～12 字为主，可为词组）。

硬性规则：
- 仅输出一段 JSON：{"phrases": ["短语1", ...]}，短语共 6～20 条。
- 不要医疗功效、治愈、降血糖承诺；不要完整复制整句评价。
- 不要输出与常见停用词无信息量的单字。
- 不要输出 JSON 以外的文字。"""


def _ensure_ai_crawler_path() -> None:
    root = Path(settings.CRAWLER_JD_ROOT).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"爬虫副本目录不存在: {root}")
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)


def _call_llm(system_prompt: str, user_prompt: str) -> str:
    _ensure_ai_crawler_path()
    import AI_crawler as ac  # noqa: WPS433

    return ac.chat_completion_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )


def _chunk_comment_texts(texts: list[str]) -> list[str]:
    """将全量评价划为若干段，控制单段字符量与最大段数。"""
    parts: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for t in texts:
        s = (t or "").strip()
        if not s:
            continue
        extra = len(s) + 1
        if cur and cur_len + extra > MAX_CHUNK_CHARS:
            parts.append("\n".join(cur))
            cur = []
            cur_len = 0
        cur.append(s)
        cur_len += extra
    if cur:
        parts.append("\n".join(cur))
    if not parts:
        return []
    if len(parts) <= MAX_CHUNKS:
        return parts
    idxs = sorted(
        {min(int(i * len(parts) / MAX_CHUNKS), len(parts) - 1) for i in range(MAX_CHUNKS)}
    )
    return [parts[i] for i in idxs]


def _parse_phrases_object(raw: str) -> list[str]:
    t = raw.strip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    try:
        obj = json.loads(t)
        if isinstance(obj, dict) and isinstance(obj.get("phrases"), list):
            return [str(x).strip() for x in obj["phrases"] if str(x).strip()]
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", t)
    if m:
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict) and isinstance(obj.get("phrases"), list):
                return [str(x).strip() for x in obj["phrases"] if str(x).strip()]
        except json.JSONDecodeError:
            pass
    return []


def suggest_focus_keywords_from_all_comments(
    *,
    keyword: str,
    brief_slice: dict[str, Any],
    all_comment_texts: list[str],
) -> dict[str, Any]:
    """
    读取全量评价（在服务端分块），每块调用模型抽取短语，合并去重后返回 ``suggested_focus_keywords``。
    """
    if not all_comment_texts:
        return {
            "suggested_focus_keywords": [],
            "suggested_scenario_hints": [],
            "rationale": "无评价正文可分析。",
            "chunks_processed": 0,
            "total_comment_texts": 0,
        }

    existing: set[str] = set()
    for x in brief_slice.get("comment_focus_keywords") or []:
        if isinstance(x, dict):
            w = str(x.get("word") or "").strip()
            if w:
                existing.add(w)

    chunks = _chunk_comment_texts(all_comment_texts)
    collected: list[str] = []
    for i, ch in enumerate(chunks):
        payload = {
            "keyword": keyword,
            "excerpt_index": i + 1,
            "excerpts": ch,
        }
        raw = _call_llm(_CHUNK_SYSTEM, json.dumps(payload, ensure_ascii=False))
        collected.extend(_parse_phrases_object(raw))

    seen: set[str] = set()
    merged: list[str] = []
    for p in collected:
        t = p.strip()
        if len(t) < 2 or len(t) > 24:
            continue
        if t in seen or t in existing:
            continue
        seen.add(t)
        merged.append(t)

    out_kw = merged[:22]
    return {
        "suggested_focus_keywords": out_kw,
        "suggested_scenario_hints": [],
        "rationale": (
            f"基于全量 {len(all_comment_texts)} 条评价文本，分 {len(chunks)} 段调用模型抽取短语并去重；"
            f"已排除与当前关注词统计表完全相同的词。"
        ),
        "chunks_processed": len(chunks),
        "total_comment_texts": len(all_comment_texts),
    }


# 兼容旧接口名（若仍有调用）
def suggest_comment_keywords_llm(
    *,
    keyword: str,
    brief_slice: dict[str, Any],
    comment_samples: list[str],
) -> dict[str, Any]:
    return suggest_focus_keywords_from_all_comments(
        keyword=keyword,
        brief_slice=brief_slice,
        all_comment_texts=list(comment_samples),
    )
