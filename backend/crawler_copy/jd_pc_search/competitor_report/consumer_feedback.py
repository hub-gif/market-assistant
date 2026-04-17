"""评价与合并表按细类矩阵对齐：带前缀行、SKU→细类映射、按细类消费者反馈分组。"""
from __future__ import annotations

from pipeline.csv_schema import MERGED_FIELD_TO_CSV_HEADER

from .comment_sentiment import _iter_comment_text_units
from .constants import (
    _COMMENT_CSV_BODY,
    _COMMENT_CSV_SKU,
    _MERGED_SHOP_CELL_KEYS,
)
from .csv_io import _cell, _md_cell
from .matrix_group import _competitor_matrix_group_key, _merged_rows_grouped_for_matrix


def _comment_lines_with_product_context(
    comment_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
    *,
    sku_header: str,
    title_h: str,
) -> list[str]:
    """与 ``comment_rows`` 顺序对齐：带细类/SKU/品名/店铺前缀，供 §8.2 大模型抽样。"""
    sku_meta: dict[str, tuple[str, str, str]] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if not gk:
            continue
        sku_meta[sku] = (
            gk,
            _cell(row, title_h),
            _cell(row, *_MERGED_SHOP_CELL_KEYS),
        )
    out: list[str] = []
    for cr in comment_rows:
        txt = _cell(cr, _COMMENT_CSV_BODY, "tagCommentContent")
        if not txt:
            continue
        sku = _cell(cr, _COMMENT_CSV_SKU, "sku").strip()
        meta = sku_meta.get(sku)
        if meta:
            gname, tit, shop = meta
            prefix = (
                f"【细类：{gname}｜SKU：{sku}｜品名：{_md_cell(tit, 80)}｜"
                f"店铺：{_md_cell(shop, 40)}】"
            )
            out.append(prefix + txt)
        else:
            out.append(txt)
    return out


def _sku_to_matrix_group_map(
    merged_rows: list[dict[str, str]], sku_header: str
) -> dict[str, str]:
    m: dict[str, str] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if gk:
            m[sku] = gk
    return m


def _comment_text_units_for_matrix_group(
    gname: str,
    merged_rows: list[dict[str, str]],
    comment_rows_in_group: list[dict[str, str]],
    sku_header: str,
) -> list[str]:
    """某细类下的评价正文列表；无 flat 时用该细类合并行的 comment_preview。"""
    texts: list[str] = []
    for row in comment_rows_in_group:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if t:
            texts.append(t)
    if texts:
        return texts
    for row in merged_rows:
        if _competitor_matrix_group_key(row) != gname:
            continue
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            texts.append(p)
    return texts


def _consumer_feedback_by_matrix_group(
    *,
    merged_rows: list[dict[str, str]],
    comment_rows: list[dict[str, str]],
    sku_header: str,
) -> list[tuple[str, list[dict[str, str]], list[str]]]:
    """
    与 §5 矩阵同序的细类列表；每项为 (细类名, 该类的 comments_flat 行, 用于场景统计的文本单元)。
    评价 SKU 不在深入样本时归入「未归类（评价 SKU 无对应深入样本）」。
    """
    if not merged_rows:
        if not comment_rows:
            return []
        texts = _iter_comment_text_units(comment_rows, [])
        return [
            (
                "未归类（无深入合并表）",
                list(comment_rows),
                texts,
            )
        ]

    sku_map = _sku_to_matrix_group_map(merged_rows, sku_header)
    merged_by_sku: dict[str, dict[str, str]] = {}
    for row in merged_rows:
        s = _cell(row, sku_header).strip()
        if s:
            merged_by_sku[s] = row
    by_g: dict[str, list[dict[str, str]]] = {}
    for row in comment_rows:
        sku = _cell(row, _COMMENT_CSV_SKU, "sku").strip()
        g = sku_map.get(sku)
        if g:
            by_g.setdefault(g, []).append(row)
            continue
        if sku and sku in merged_by_sku:
            # 深入样本存在但缺 detail_category_path（或路径无法解析为可读细类）：不参与按细类分析
            continue
        by_g.setdefault("未归类（评价 SKU 无对应深入样本）", []).append(row)

    out: list[tuple[str, list[dict[str, str]], list[str]]] = []
    used: set[str] = set()
    for gname, _ in _merged_rows_grouped_for_matrix(merged_rows):
        cr = by_g.get(gname, [])
        tu = _comment_text_units_for_matrix_group(
            gname, merged_rows, cr, sku_header
        )
        out.append((gname, cr, tu))
        used.add(gname)
    for gname, cr in sorted(by_g.items(), key=lambda x: (-len(x[1]), x[0])):
        if gname in used:
            continue
        tu = _comment_text_units_for_matrix_group(
            gname, merged_rows, cr, sku_header
        )
        out.append((gname, cr, tu))
    return out


__all__ = [
    "_comment_lines_with_product_context",
    "_comment_text_units_for_matrix_group",
    "_consumer_feedback_by_matrix_group",
    "_sku_to_matrix_group_map",
]
