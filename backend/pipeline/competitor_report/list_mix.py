"""列表可见度代理指标、品牌/店铺扇图用的名称列表与计数。"""
from __future__ import annotations

from collections import Counter
from typing import Any

from pipeline.csv.schema import JD_SEARCH_CSV_HEADERS, MERGED_FIELD_TO_CSV_HEADER

from .constants import (
    _LEGACY_LIST_BRAND_TITLE_KEY,
    _LEGACY_SHOP_NAME_KEY,
    _LIST_BRAND_TITLE_HEADER,
    _MERGED_SHOP_CELL_KEYS,
)
from .csv_io import _cell, _collect_prices
from .price_stats import _price_stats_extended


def _structure_names_for_pie_counter(row_names: list[str]) -> list[str]:
    """
    与 ``_counter_mix_top_rows_with_remainder`` / 列表品牌·店铺扇图同一套规则：
    按 strip 后的名称逐行保留一条，便于 ``_brand_cr`` 与饼图 Counter 一致。
    """
    return [(x or "").strip() for x in row_names if (x or "").strip()]


def _brand_cr(cnames: list[str]) -> tuple[float | None, float | None, str, str]:
    """按名称计数返回 (第一大主体份额, 前三合计份额, 头部标签, 头部占比展示字符串)。"""
    if not cnames:
        return None, None, "", ""
    cnt = Counter(cnames)
    total = sum(cnt.values())
    if total <= 0:
        return None, None, "", ""
    mc = cnt.most_common()
    top1_n = mc[0][1] if mc else 0
    top1 = mc[0][0] if mc else ""
    cr1 = top1_n / total
    top3_n = sum(n for _, n in mc[:3])
    cr3 = top3_n / total
    return cr1, cr3, top1, f"{100.0 * top1_n / total:.1f}%"


def _counter_mix_top_rows_with_remainder(
    row_names: list[str], *, top_n: int, remainder_label: str
) -> list[tuple[str, int]]:
    """
    与列表品牌/店铺扇图一致：按 strip 后的名称计数；``most_common(top_n)`` 未覆盖的长尾合并为
    ``remainder_label``，保证各块 count 之和等于可统计行数（与 ``_structure_names_for_pie_counter`` 总条数一致）。
    """
    c = Counter((x or "").strip() for x in row_names if (x or "").strip())
    if not c:
        return []
    total = sum(c.values())
    common = c.most_common(top_n)
    accounted = sum(v for _, v in common)
    rest = total - accounted
    out: list[tuple[str, int]] = list(common)
    if rest > 0:
        out.append((remainder_label, rest))
    return out


def _search_list_proxies(rows: list[dict[str, str]]) -> dict[str, Any]:
    """
    基于 pc_search_export 的「列表可见度」指标，**不是**全渠道零售额或 TAM。
    """
    sku_k = JD_SEARCH_CSV_HEADERS["sku_id"]
    shop_k = JD_SEARCH_CSV_HEADERS["shop_name"]
    page_k = JD_SEARCH_CSV_HEADERS["page"]
    cat_k = JD_SEARCH_CSV_HEADERS["leaf_category"]
    skus: set[str] = set()
    shops: set[str] = set()
    pages: set[str] = set()
    cats: set[str] = set()
    for r in rows:
        s = _cell(r, sku_k)
        if s:
            skus.add(s)
        sh = _cell(r, shop_k)
        if sh:
            shops.add(sh)
        pg = _cell(r, page_k)
        if pg:
            pages.add(pg)
        c = _cell(r, cat_k)
        if c:
            cats.add(c)
    prices = _collect_prices(rows)
    pst = _price_stats_extended(prices)
    return {
        "total_rows": len(rows),
        "unique_skus": len(skus),
        "unique_shops": len(shops),
        "unique_pages": len(pages),
        "page_span": (min((int(p) for p in pages if p.isdigit()), default=None), max((int(p) for p in pages if p.isdigit()), default=None)),
        "unique_leaf_cats": len(cats),
        "list_price_stats": pst,
    }


def _structure_shops(rows: list[dict[str, str]], *, list_export: bool) -> list[str]:
    if list_export:
        return [
            _cell(r, JD_SEARCH_CSV_HEADERS["shop_name"], _LEGACY_SHOP_NAME_KEY)
            for r in rows
            if _cell(r, JD_SEARCH_CSV_HEADERS["shop_name"], _LEGACY_SHOP_NAME_KEY)
        ]
    out: list[str] = []
    for r in rows:
        s = _cell(r, *_MERGED_SHOP_CELL_KEYS)
        if s:
            out.append(s)
    return out


def _structure_brands(rows: list[dict[str, str]], *, list_export: bool) -> list[str]:
    if list_export:
        return [
            _cell(r, _LIST_BRAND_TITLE_HEADER, _LEGACY_LIST_BRAND_TITLE_KEY)
            for r in rows
            if _cell(r, _LIST_BRAND_TITLE_HEADER, _LEGACY_LIST_BRAND_TITLE_KEY)
        ]
    return [
        _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
        for r in rows
        if _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
    ]


__all__ = [
    "_brand_cr",
    "_counter_mix_top_rows_with_remainder",
    "_search_list_proxies",
    "_structure_brands",
    "_structure_names_for_pie_counter",
    "_structure_shops",
]
