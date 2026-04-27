"""从爬虫导出单元格解析数值价格（供入库索引与数据集筛选）。"""
from __future__ import annotations

import re


def float_price_from_cell(s: str | None) -> float | None:
    t = (s or "").strip().replace(",", "").replace("，", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    if 0 < v < 1_000_000:
        return v
    return None


def effective_list_price_value(coupon: str, price: str, original: str) -> float | None:
    """优先券后价，其次标价，再次原价（与列表侧展示习惯一致）。"""
    for s in (coupon, price, original):
        v = float_price_from_cell(s)
        if v is not None:
            return v
    return None
