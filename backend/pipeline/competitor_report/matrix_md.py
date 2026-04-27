"""竞品矩阵 Markdown 行：配料格与整行管道表单元。"""
from __future__ import annotations

from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER, merged_csv_effective_total_sales

from .constants import (
    _COMMENT_FUZZ_KEYS,
    _DETAIL_PRICE_FINAL_CSV_KEYS,
    _LEGACY_RANK_TAGLINE_KEY,
    _LEGACY_SELLING_POINT_KEY,
    _LIST_SHOW_PRICE_CELL_KEYS,
    _MERGED_SHOP_CELL_KEYS,
    _RANK_TAGLINE_KEY,
    _SELLING_POINT_KEY,
)
from .csv_io import _cell, _detail_category_path_cell, _md_cell
from .ingredients import (
    _ingredients_from_product_attributes,
    _ingredients_single_line,
    _is_ingredient_url_blob,
)


def _matrix_ingredients_cell(row: dict[str, str], *, max_len: int = 420) -> str:
    """
    优先 ``detail_body_ingredients``（配料 OCR/文本）；旧合并表可能为 ``detail_body_image_urls``。
    若为 URL 串则尝试 ``detail_product_attributes`` 中的「配料/配料表：」片段。
    """
    raw = _cell(
        row,
        MERGED_FIELD_TO_CSV_HEADER["detail_body_ingredients"],
        "detail_body_ingredients",
        "detail_body_image_urls",
    )
    if raw and not _is_ingredient_url_blob(raw):
        return _md_cell(_ingredients_single_line(raw), max_len)
    from_attr = _ingredients_from_product_attributes(
        _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["detail_product_attributes"],
            "detail_product_attributes",
        )
    )
    if from_attr:
        return _md_cell(from_attr, max_len)
    if raw and _is_ingredient_url_blob(raw):
        return _md_cell(
            "（详情长图链接，无配料正文；可在采集侧开启配料识别后重新跑批次）",
            max_len,
        )
    return "—"


def _competitor_matrix_md_line(
    row: dict[str, str], *, sku_header: str, title_h: str
) -> str:
    sku = _md_cell(_cell(row, sku_header), 14)
    title = _md_cell(_cell(row, title_h), 56)
    brand = _md_cell(
        _cell(row, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand"), 16
    )
    pj = _md_cell(_cell(row, *_LIST_SHOW_PRICE_CELL_KEYS), 10)
    df = _md_cell(_cell(row, *_DETAIL_PRICE_FINAL_CSV_KEYS), 10)
    shop = _md_cell(_cell(row, *_MERGED_SHOP_CELL_KEYS), 22)
    sell = _md_cell(_cell(row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY), 36)
    rank = _md_cell(
        _cell(row, _RANK_TAGLINE_KEY, _LEGACY_RANK_TAGLINE_KEY), 28
    )
    cat = _md_cell(_detail_category_path_cell(row), 24)
    ing = _matrix_ingredients_cell(row)
    ts_eff = merged_csv_effective_total_sales(row)
    cc = _md_cell(ts_eff or _cell(row, *_COMMENT_FUZZ_KEYS), 14)
    prev = _md_cell(
        _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        ),
        72,
    )
    return (
        f"| {sku} | {title} | {brand} | {pj} | {df} | {shop} | {sell} | {rank} | "
        f"{cat} | {ing} | {cc} | {prev} |"
    )


__all__ = ["_competitor_matrix_md_line", "_matrix_ingredients_cell"]
