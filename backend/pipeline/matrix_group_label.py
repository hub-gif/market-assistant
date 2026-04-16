"""
与 ``jd_competitor_report._matrix_group_label_from_path`` 同源：
从商详 ``detail_category_path`` 解析 §5 竞品矩阵用的类目展示名（如饼干、米）。
"""
from __future__ import annotations

import re


def category_token_meaningless(seg: str) -> bool:
    """纯数字类目 ID、空串或疑似内部编码的段，不宜直接作为矩阵分组展示名。"""
    t = (seg or "").strip()
    if not t:
        return True
    if t.isdigit():
        return True
    if len(t) >= 14 and re.fullmatch(r"[A-Za-z0-9_\-]+", t):
        return True
    return False


def matrix_display_segment_from_parts(parts: list[str]) -> str | None:
    """
    与历史逻辑一致的主选段；若该段无意义则自右向左找第一段可读文本
    （避免「仅类目码」或中间段为数字 ID 时，把路径误解析成无意义的细类展示名）。
    """
    if not parts:
        return None
    if len(parts) >= 4:
        preferred = parts[-2]
    elif len(parts) >= 3:
        preferred = parts[1]
    elif len(parts) >= 2:
        preferred = parts[1]
    else:
        preferred = parts[0]
    order: list[str] = []
    if preferred:
        order.append(preferred)
    if len(parts) >= 2:
        order.append(parts[-2])
    order.append(parts[-1])
    order.extend(reversed(parts))
    seen: set[str] = set()
    for cand in order:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        if not category_token_meaningless(cand):
            return cand.strip()
    return None


def matrix_group_label_from_detail_path(path: str) -> str:
    """由 ``detail_category_path`` 文本解析细类展示名；空或无可读段则返回空串。"""
    t = (path or "").strip()
    if not t:
        return ""
    parts = [p.strip() for p in t.replace("＞", ">").split(">") if p.strip()]
    if not parts:
        return ""
    key = matrix_display_segment_from_parts(parts)
    return (key[:80] if key else "")
