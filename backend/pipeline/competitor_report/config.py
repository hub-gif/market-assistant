"""``report_config`` JSON → 外部市场表行（不再解析预设关注词/场景词组）。"""
from __future__ import annotations

from typing import Any

from .constants import EXTERNAL_MARKET_TABLE_ROWS


def _normalize_external_market_rows(
    raw: Any,
) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(raw, list) or not raw:
        return EXTERNAL_MARKET_TABLE_ROWS
    rows: list[tuple[str, str, str, str]] = []

    def _four_cells(x: Any) -> tuple[str, str, str, str] | None:
        if isinstance(x, (list, tuple)) and len(x) >= 4:
            return tuple(str(c)[:500] for c in x[:4])
        if isinstance(x, dict):
            a = str(x.get("indicator") or x.get("a") or "").strip()[:500]
            b = str(x.get("value_and_scope") or x.get("b") or "").strip()[:500]
            c = str(x.get("source") or x.get("c") or "").strip()[:500]
            d = str(x.get("year") or x.get("d") or "").strip()[:500]
            if any((a, b, c, d)):
                return (a, b, c, d)
        return None

    for item in raw[:24]:
        r = _four_cells(item)
        if r:
            rows.append(r)
    return tuple(rows) if rows else EXTERNAL_MARKET_TABLE_ROWS


def resolve_report_tuning(
    report_config: dict[str, Any] | None,
) -> tuple[tuple[tuple[str, str, str, str], ...]]:
    """仅解析第三方市场摘录表；预设关注词/场景词组已废弃，不再参与报告或 brief。"""
    if not report_config:
        return (EXTERNAL_MARKET_TABLE_ROWS,)
    return (
        _normalize_external_market_rows(
            report_config.get("external_market_table_rows")
        ),
    )


__all__ = [
    "resolve_report_tuning",
    "_normalize_external_market_rows",
]
