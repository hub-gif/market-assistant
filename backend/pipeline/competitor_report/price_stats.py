"""合并表价格列的汇总统计（价盘/LLM 载荷等共用）。"""
from __future__ import annotations

import statistics
from typing import Any


def _price_stats_extended(prices: list[float]) -> dict[str, Any]:
    if not prices:
        return {}
    out: dict[str, Any] = {
        "min": min(prices),
        "max": max(prices),
        "mean": statistics.mean(prices),
        "n": len(prices),
    }
    if len(prices) >= 2:
        out["stdev"] = statistics.stdev(prices)
    if len(prices) >= 2:
        out["median"] = statistics.median(prices)
    if len(prices) >= 4:
        s = sorted(prices)
        n = len(s)
        mid = n // 2
        lower = s[:mid] if n % 2 else s[:mid]
        upper = s[mid + 1 :] if n % 2 else s[mid:]
        out["q1"] = statistics.median(lower) if lower else s[0]
        out["q3"] = statistics.median(upper) if upper else s[-1]
    return out


__all__ = ["_price_stats_extended"]
