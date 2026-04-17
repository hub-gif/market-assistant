"""``report_config`` JSON → 关注词、场景组、外部市场表行。"""
from __future__ import annotations

from typing import Any

from .constants import (
    COMMENT_FOCUS_WORDS,
    COMMENT_SCENARIO_GROUPS,
    EXTERNAL_MARKET_TABLE_ROWS,
)


def _normalize_focus_words(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        return COMMENT_FOCUS_WORDS
    out: list[str] = []
    for x in raw[:120]:
        s = str(x).strip()
        if len(s) > 48:
            s = s[:48]
        if s:
            out.append(s)
    return tuple(out) if out else COMMENT_FOCUS_WORDS


def _normalize_scenario_groups(
    raw: Any,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not isinstance(raw, list) or not raw:
        return COMMENT_SCENARIO_GROUPS
    parsed: list[tuple[str, tuple[str, ...]]] = []
    for item in raw[:40]:
        label = ""
        triggers: list[str] = []
        if isinstance(item, dict):
            label = str(item.get("label") or "").strip()[:80]
            tr = item.get("triggers")
            if isinstance(tr, list):
                for t in tr[:48]:
                    s = str(t).strip()
                    if len(s) > 48:
                        s = s[:48]
                    if s:
                        triggers.append(s)
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            label = str(item[0]).strip()[:80]
            tr = item[1]
            if isinstance(tr, (list, tuple)):
                for t in tr[:48]:
                    s = str(t).strip()
                    if len(s) > 48:
                        s = s[:48]
                    if s:
                        triggers.append(s)
        if label and triggers:
            parsed.append((label, tuple(triggers)))
    return tuple(parsed) if parsed else COMMENT_SCENARIO_GROUPS


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
) -> tuple[
    tuple[str, ...],
    tuple[tuple[str, tuple[str, ...]], ...],
    tuple[tuple[str, str, str, str], ...],
]:
    if not report_config:
        return COMMENT_FOCUS_WORDS, COMMENT_SCENARIO_GROUPS, EXTERNAL_MARKET_TABLE_ROWS
    return (
        _normalize_focus_words(report_config.get("comment_focus_words")),
        _normalize_scenario_groups(report_config.get("comment_scenario_groups")),
        _normalize_external_market_rows(
            report_config.get("external_market_table_rows")
        ),
    )


__all__ = [
    "resolve_report_tuning",
    "_normalize_external_market_rows",
    "_normalize_focus_words",
    "_normalize_scenario_groups",
]
