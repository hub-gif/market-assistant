"""从爬虫导出文案解析销量、评价量等非负整数（与 reporting.charts._cn_volume_int 同源）。"""
from __future__ import annotations

import re


def cn_volume_int(s: str | None) -> int:
    """
    支持「亿」「万」及纯数字；如 ``已售50万+`` → 500000。
    无法解析时返回 0。
    """
    t = (s or "").strip().replace(",", "").replace("，", "")
    if not t:
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)\s*亿", t)
    if m:
        return int(round(float(m.group(1)) * 100_000_000))
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m:
        return int(round(float(m.group(1)) * 10_000))
    m2 = re.search(r"(\d+)", t)
    if m2:
        return int(m2.group(1))
    return 0


def sales_sort_value_from_search_cells(total_sales: str, comment_sales_floor: str) -> int | None:
    """搜索行：优先 ``total_sales``，否则销量楼层；两列皆空则 None。"""
    ts = (total_sales or "").strip()
    fl = (comment_sales_floor or "").strip()
    if not ts and not fl:
        return None
    v = cn_volume_int(ts)
    if v > 0:
        return v
    v2 = cn_volume_int(fl)
    if v2 > 0:
        return v2
    return 0


def comment_count_sort_value_from_cell(comment_count: str) -> int | None:
    if not (comment_count or "").strip():
        return None
    return cn_volume_int(comment_count)


def comment_count_sort_value_from_merged(pipeline_comment_count: str) -> int | None:
    """宽表评价量列（与搜索侧列表文案风格类似）。"""
    return comment_count_sort_value_from_cell(pipeline_comment_count)
