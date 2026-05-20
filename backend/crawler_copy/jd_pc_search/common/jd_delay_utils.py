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


# pc_search 空包/解析失败时的重试前等待（默认 3–8 秒均匀随机）
DEFAULT_PC_SEARCH_FETCH_RETRY_GAP: tuple[float, float] = (3.0, 8.0)


def parse_fetch_retry_delay_arg(val: object) -> tuple[float, float]:
    """
    解析 `--fetch-retry-delay` / 流水线传入值。

    - None / 空字符串：``(3, 8)``.
    - ``"MIN-MAX"``：与 ``parse_request_delay_range`` 相同；``MIN-MAX`` 均为非正且含 0 时视为关闭等待 ``(0, 0)``。
    - 单值 ``"5"`` 或 ``float``：固定间隔 ``(v, v)``。
    - 二元 tuple/list：视作 ``(lo, hi)``；若两边均 ≤0 则 ``(0, 0)`` 关闭。
    """
    if val is None:
        return DEFAULT_PC_SEARCH_FETCH_RETRY_GAP
    if isinstance(val, (tuple, list)) and len(val) == 2:
        lo, hi = float(val[0]), float(val[1])
        if lo <= 0.0 and hi <= 0.0:
            return (0.0, 0.0)
        if lo > hi:
            lo, hi = hi, lo
        if lo < 0 or hi < 0:
            raise ValueError("重试延迟不能为负")
        return (lo, hi)
    s = str(val).strip()
    if not s:
        return DEFAULT_PC_SEARCH_FETCH_RETRY_GAP
    if "-" in s:
        lo, hi = parse_request_delay_range(s)
        if lo <= 0.0 and hi <= 0.0:
            return (0.0, 0.0)
        return (lo, hi)
    v = float(s)
    if v < 0:
        raise ValueError("重试延迟不能为负")
    return (v, v)


def sleep_pc_search_fetch_retry_gap(
    delay_range: tuple[float, float],
    *,
    log_stream=sys.stderr,
) -> None:
    """同一 body.page/s 空包或需重试时、再次请求前的随机等待。"""
    lo, hi = delay_range
    if lo <= 0.0 and hi <= 0.0:
        return
    if lo > hi:
        lo, hi = hi, lo
    sec = random.uniform(lo, hi)
    print(
        f"[京东] pc_search 空包/失败重试间隔 sleep {sec:.1f}s（区间 {lo:g}–{hi:g}）",
        file=log_stream,
    )
    time.sleep(sec)
