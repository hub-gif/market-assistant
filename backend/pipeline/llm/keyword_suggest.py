"""在报告生成前：基于评价正文调用大模型，联想补充**关注词**与**使用场景触发组**（写入 effective_report_config）。"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from django.conf import settings

MAX_CHUNK_CHARS = 24_000
MAX_CHUNKS = 12
# 场景联想单次送入模型的评价摘录上限（字符）；过大易顶上下文
SCENARIO_CORPUS_MAX_CHARS = 18_000

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


_SCENARIO_SYSTEM = """你是电商用户研究助手。输入 JSON 含：
- ``keyword``：监测词；
- ``existing_scenarios``：数组，每项为 ``{"label": "展示名", "triggers": ["子串1", ...]}``。统计时若评价正文**包含任一 trigger 子串**，则计入该 label（与宿主系统规则一致）。
- ``excerpts``：多条用户评价正文摘录（已截断拼接）。

任务：在**不重复** ``existing_scenarios`` 中已有 ``label``（逐字比较，勿改写字）的前提下，从 excerpts 归纳 **4～12 条**新的「用途/场景」监测组，覆盖评论里**明显出现但未被现有组覆盖**的消费情境（如「下午茶」「露营」「宿舍」等，须确有文本依据）。

硬性规则：
- **仅输出**一段 JSON：``{"scenarios": [{"label": "展示名", "triggers": ["子串1", "子串2", ...]}, ...]}``；
- 每条 ``label`` 2～16 字；每组 ``triggers`` 3～10 条，每条 trigger 为 **2～12 字中文**子串，用于**子串命中**计数；
- 不要医疗功效、治愈、降血糖承诺；不要与 existing 的 label 同名或仅差空格；
- 不要输出 JSON 以外的文字。"""


def _sample_corpus_for_scenarios(texts: list[str], *, max_chars: int) -> str:
    """取评价正文前部拼接至 max_chars，供单次场景联想。"""
    parts: list[str] = []
    n = 0
    for t in texts:
        s = (t or "").strip()
        if not s:
            continue
        extra = len(s) + 1
        if n + extra > max_chars:
            remain = max_chars - n - 1
            if remain > 40:
                parts.append(s[:remain])
            break
        parts.append(s)
        n += extra
    return "\n".join(parts)


def _parse_scenarios_object(raw: str) -> list[dict[str, Any]]:
    t = (raw or "").strip()
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```$", "", t)
    try:
        obj = json.loads(t)
    except json.JSONDecodeError:
        obj = None
    if not isinstance(obj, dict):
        m = re.search(r"\{[\s\S]*\}", t)
        if not m:
            return []
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return []
    arr = obj.get("scenarios")
    if not isinstance(arr, list):
        return []
    out: list[dict[str, Any]] = []
    for item in arr:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()[:80]
        tr_raw = item.get("triggers")
        triggers: list[str] = []
        if isinstance(tr_raw, list):
            seen_t: set[str] = set()
            for x in tr_raw[:24]:
                s = str(x).strip()
                if len(s) < 2 or len(s) > 24:
                    continue
                if s in seen_t:
                    continue
                seen_t.add(s)
                triggers.append(s)
        if label and len(triggers) >= 1:
            out.append({"label": label, "triggers": triggers[:12]})
    return out


def suggest_scenario_groups_llm(
    *,
    keyword: str,
    existing_groups: list[dict[str, Any]],
    all_comment_texts: list[str],
) -> dict[str, Any]:
    """
    单次调用模型，基于评价摘录扩展 ``comment_scenario_groups`` 形态的新组（label + triggers）。
    """
    if not all_comment_texts:
        return {
            "suggested_scenario_groups": [],
            "scenario_rationale": "无评价正文可分析。",
        }
    existing_compact: list[dict[str, Any]] = []
    for g in (existing_groups or [])[:36]:
        if not isinstance(g, dict):
            continue
        lab = str(g.get("label") or "").strip()
        tr = g.get("triggers")
        ts: list[str] = []
        if isinstance(tr, list):
            for x in tr[:16]:
                s = str(x).strip()
                if s:
                    ts.append(s[:48])
        if lab and ts:
            existing_compact.append({"label": lab[:80], "triggers": ts})
    excerpts = _sample_corpus_for_scenarios(
        all_comment_texts, max_chars=SCENARIO_CORPUS_MAX_CHARS
    )
    payload = {
        "keyword": keyword,
        "existing_scenarios": existing_compact,
        "excerpts": excerpts,
    }
    raw = _call_llm(_SCENARIO_SYSTEM, json.dumps(payload, ensure_ascii=False))
    scenarios = _parse_scenarios_object(raw)
    return {
        "suggested_scenario_groups": scenarios[:14],
        "scenario_rationale": (
            f"基于约 {len(excerpts)} 字评价摘录单次调用模型；"
            f"在 {len(existing_compact)} 组既有场景之外补充 {len(scenarios[:14])} 组候选。"
        ),
    }


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
