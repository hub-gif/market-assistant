"""竞品简报「集中度」块：对外字段名面向非技术用户，并兼容旧键名。"""
from __future__ import annotations

from typing import Any


def concentration_first_share(block: dict[str, Any] | None) -> Any:
    """最大一家占全部相关行的比例（0～1）。新键 ``first_share``，旧键 ``cr1``。"""
    if not block:
        return None
    v = block.get("first_share")
    if v is not None:
        return v
    return block.get("cr1")


def concentration_top_three_share(block: dict[str, Any] | None) -> Any:
    """前三名合计占全部相关行的比例（0～1）。新键 ``top_three_combined_share``，旧键 ``cr3``。"""
    if not block:
        return None
    v = block.get("top_three_combined_share")
    if v is not None:
        return v
    return block.get("cr3")
