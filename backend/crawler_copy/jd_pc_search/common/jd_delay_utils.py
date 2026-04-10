# -*- coding: utf-8 -*-
"""搜索/评论等脚本共用的「请求间隔」解析与 sleep（避免 comment 依赖整份 jd_h5_search_requests）。"""
from __future__ import annotations

import random
import sys
import time


def parse_request_delay_range(s: str | None) -> tuple[float, float]:
    """
    解析 CLI「MIN-MAX」为随机等待区间（秒）。
    例：``30-60`` → uniform(30, 60)。
    """
    t = (s or "").strip()
    if not t:
        raise ValueError("空字符串")
    parts = t.split("-", 1)
    if len(parts) != 2:
        raise ValueError(f"应为 MIN-MAX（秒），如 30-60，收到: {t!r}")
    lo = float(parts[0].strip())
    hi = float(parts[1].strip())
    if lo < 0 or hi < 0:
        raise ValueError("延迟不能为负")
    if lo > hi:
        lo, hi = hi, lo
    return (lo, hi)


def sleep_pc_search_request_gap(delay_range: tuple[float, float] | None) -> None:
    """在已有至少一次请求之后、发起下一次之前调用。"""
    if not delay_range:
        return
    lo, hi = delay_range
    sec = random.uniform(lo, hi)
    print(
        f"[京东] pc_search 间隔 sleep {sec:.1f}s（区间 {lo:g}–{hi:g}）",
        file=sys.stderr,
    )
    time.sleep(sec)
