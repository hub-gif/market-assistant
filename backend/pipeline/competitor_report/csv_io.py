"""CSV 行读取与单元格、价格抽取等通用辅助。"""
from __future__ import annotations

import csv
import re
from pathlib import Path

from .constants import (
    _DETAIL_CATEGORY_PATH_KEY,
    _K_CAT_COL,
    _K_PROP_COL,
    _LIST_PRICE_AND_COUPON_KEYS,
)


def _cell(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = str(row.get(k) or "").strip()
        if v:
            return v
    return ""


def _shortname_from_prop(prop: str) -> str:
    m = re.search(r"简称[:：]\s*([^|]+)", prop or "")
    return m.group(1).strip()[:120] if m else ""


def _detail_category_path_cell(row: dict[str, str]) -> str:
    """细类矩阵与按细类评价统计仅以该列为准；空则视为商详类目不完整。"""
    return _cell(row, _DETAIL_CATEGORY_PATH_KEY, "detail_category_path")


def _search_export_catid_to_shortname_map(rows: list[dict[str, str]]) -> dict[str, str]:
    """列表导出中叶子类目列常为纯数字 ID：用同行规格属性「简称」映射为可读名称。"""
    m: dict[str, str] = {}
    for r in rows:
        cid = _cell(r, _K_CAT_COL).strip()
        if not cid.isdigit():
            continue
        if cid in m:
            continue
        sn = _shortname_from_prop(_cell(r, _K_PROP_COL))
        if sn:
            m[cid] = sn
    return m


def _md_cell(s: str, max_len: int = 120) -> str:
    t = (s or "").replace("\r\n", " ").replace("\n", " ").replace("|", "/")
    t = " ".join(t.split())
    return (t[:max_len] + "…") if max_len > 0 and len(t) > max_len else t


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        return [], []
    raw = path.read_text(encoding="utf-8-sig")
    lines = raw.splitlines()
    if not lines:
        return [], []
    rdr = csv.DictReader(lines)
    fn = rdr.fieldnames or []
    return list(fn), list(rdr)


def _float_price(s: str) -> float | None:
    if not (s or "").strip():
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(s).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _collect_prices(rows: list[dict[str, str]]) -> list[float]:
    out: list[float] = []
    for row in rows:
        for k in _LIST_PRICE_AND_COUPON_KEYS:
            p = _float_price(_cell(row, k))
            if p is not None and 0 < p < 1_000_000:
                out.append(p)
                break
    return out


__all__ = [
    "_cell",
    "_collect_prices",
    "_detail_category_path_cell",
    "_float_price",
    "_md_cell",
    "_read_csv_rows",
    "_search_export_catid_to_shortname_map",
    "_shortname_from_prop",
]
