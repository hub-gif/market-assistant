"""CSV 行读取与单元格、价格抽取等通用辅助。"""
from __future__ import annotations

import csv
import re
from pathlib import Path

from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER

from .constants import (
    _DETAIL_CATEGORY_PATH_KEY,
    _K_CAT_COL,
    _K_PROP_COL,
    _LIST_PRICE_AND_COUPON_KEYS,
)


_TITLE_CELL_KEYS: tuple[str, ...] = (
    MERGED_FIELD_TO_CSV_HEADER["title"],
    "标题(wareName)",
    "title",
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


_SPEC_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p)
    for p in (
        # 容量/重量 × 件数（较长片段优先于单独 ml，见下方排序）
        r"\d+\.?\d*\s*[mM][lL]\s*[×*xXＸ]\s*\d+",
        r"\d+\.?\d*\s*毫升\s*[×*xXＸ]\s*\d+",
        r"\d+\.?\d*\s*[mM][lL]",
        r"\d+\.?\d*\s*毫升",
        r"\d+\.?\d*\s*克",
        r"\d+\.?\d*\s*[kK][gG]",
        r"\d+\.?\d*\s*千克",
        r"\d+\.?\d*\s*斤",
        # 件数：3瓶、三瓶、10支装（阿拉伯或中文数字 + 瓶/支/盒/件/袋）
        r"(?:[一两二三四五六七八九十百千]+|\d+)\s*瓶(?:装)?",
        r"(?:[一两二三四五六七八九十百千]+|\d+)\s*支(?:装)?",
        r"(?:[一两二三四五六七八九十百千]+|\d+)\s*盒(?:装)?",
        r"(?:[一两二三四五六七八九十百千]+|\d+)\s*件(?:装)?",
        r"(?:[一两二三四五六七八九十百千]+|\d+)\s*袋(?:装)?",
        r"\d+\s*件装?",
        r"\d+\s*瓶装?",
        r"\d+\s*支装?",
        r"\d+\s*盒装?",
        r"\d+\s*袋装?",
        r"\d+\s*片装?",
        r"\d+\*\d+",
    )
)


def _norm_spec_token(s: str) -> str:
    return re.sub(r"[\s·\.]+", "", (s or "").lower())


def _spec_hints_subsumed_by_attr(hints: str, attr: str) -> bool:
    """标题拆出的规格片段是否已全部出现在规格属性列中（避免重复堆叠）。"""
    if not hints.strip() or not attr.strip():
        return False
    a = _norm_spec_token(attr)
    for part in hints.split("·"):
        p = part.strip()
        if not p:
            continue
        if _norm_spec_token(p) not in a:
            return False
    return True


def _spec_hints_from_title(title: str) -> str:
    """
    从**标题**抽取常见可计量规格片段（ml、g、**件数**如 3瓶/三瓶、ml×3 等）。
    京东不少 SKU 只在标题写「30ml 三瓶 / 3瓶」，规格属性列为空，价盘应对齐此类信息。
    """
    if not (title or "").strip():
        return ""
    t = title.strip()
    spans: list[tuple[int, int, str]] = []
    for rx in _SPEC_TITLE_PATTERNS:
        for m in rx.finditer(t):
            spans.append((m.start(), m.end(), m.group(0).strip()))
    if not spans:
        return ""
    # 同起点优先取长匹配（如 30ml×3 优先于 30ml）
    spans.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    picked: list[str] = []
    last_end = -1
    seen: set[str] = set()
    for s, e, txt in spans:
        if s < last_end:
            continue
        key = _norm_spec_token(txt)
        if not key or key in seen:
            last_end = max(last_end, e)
            continue
        seen.add(key)
        picked.append(txt)
        last_end = e
        if len(picked) >= 8:
            break
    return "·".join(picked)


def _price_context_spec_merged(row: dict[str, str]) -> str:
    """
    价盘/图表用规格文案：优先「规格属性」列，并用标题中的 ml、g、装量等**补全**（列缺失或列未覆盖的片段）。
    """
    attr = _cell(row, _K_PROP_COL, "attributes")
    title = _cell(row, *_TITLE_CELL_KEYS)
    hints = _spec_hints_from_title(title)
    attr_s = " ".join(attr.split()) if attr.strip() else ""
    if attr_s and hints:
        if _spec_hints_subsumed_by_attr(hints, attr_s):
            merged = attr_s
        else:
            merged = f"{attr_s}；{hints}"
    elif attr_s:
        merged = attr_s
    else:
        merged = hints
    return merged


def _price_context_spec_cell(row: dict[str, str], *, max_len: int = 56) -> str:
    """列表/合并行规格摘录（列 + 标题），价盘与 LLM 摘录用。"""
    merged = _price_context_spec_merged(row)
    return _md_cell(merged, max_len) if merged else ""


__all__ = [
    "_cell",
    "_collect_prices",
    "_detail_category_path_cell",
    "_float_price",
    "_md_cell",
    "_price_context_spec_cell",
    "_price_context_spec_merged",
    "_read_csv_rows",
    "_search_export_catid_to_shortname_map",
    "_shortname_from_prop",
]
