"""竞品矩阵细类键：与 ``pipeline.jd.matrix_group_label`` 及 §5 矩阵/扇图同源。"""
from __future__ import annotations

from collections import Counter

from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER
from pipeline.jd.matrix_group_label import (
    matrix_group_label_from_detail_path as _matrix_group_label_from_path,
)

from .csv_io import _cell, _detail_category_path_cell


def _matrix_group_label_from_detail_path(row: dict[str, str]) -> str:
    return _matrix_group_label_from_path(_detail_category_path_cell(row))


_MERGED_SKU_CELL_KEYS: tuple[str, ...] = (
    MERGED_FIELD_TO_CSV_HEADER["sku_id"],
    "sku_id",
    "SKU",
    "SKU(skuId)",
    "sku",
)

# 商详类目路径缺失（如抓取失败）时与有效细类一起在矩阵·细类归纳中占位，避免样本被整条丢弃
UNCATEGORIZED_MATRIX_GROUP_LABEL = "未分类"


def _merged_row_has_sku_for_matrix(row: dict[str, str]) -> bool:
    return bool(_cell(row, *_MERGED_SKU_CELL_KEYS).strip())


def _competitor_matrix_group_key(row: dict[str, str]) -> str:
    """
    竞品矩阵分组：§5 / §8 / 统计图共用。

    优先由 ``detail_category_path`` 解析细类展示名；列为空或段均为无意义编码时，
    若该行仍具备合并表 SKU，则归为 ``未分类``，以便细类载荷与大模型归纳仍覆盖该样本。
    无 SKU 时返回空串（不参与矩阵）。
    """
    ml = _matrix_group_label_from_detail_path(row)
    if ml:
        return ml
    if _merged_row_has_sku_for_matrix(row):
        return UNCATEGORIZED_MATRIX_GROUP_LABEL
    return ""


def _merged_rows_grouped_for_matrix(
    merged_rows: list[dict[str, str]],
) -> list[tuple[str, list[dict[str, str]]]]:
    buckets: dict[str, list[dict[str, str]]] = {}
    for row in merged_rows:
        k = _competitor_matrix_group_key(row)
        if not k:
            continue
        buckets.setdefault(k, []).append(row)

    def sort_key(item: tuple[str, list[dict[str, str]]]) -> tuple[int, int, str]:
        name, rows = item
        miss = name.startswith("未归类")
        return (1 if miss else 0, -len(rows), name)

    return sorted(buckets.items(), key=sort_key)


def _category_mix(
    rows: list[dict[str, str]], *, top_k: int = 12
) -> list[tuple[str, int]]:
    """
    按「可读细类标签」统计 SKU 分布（与 §5 ``_competitor_matrix_group_key`` 同源）；
    含可解析类目及无类目但有 SKU（记为 ``未分类``）的合并行。

    返回 ``most_common(top_k)``，并将未列入 Top K 的款数合并为「（其余细类）」，
    使各块 SKU 数之和等于有效矩阵 SKU 总数（与扇形图、简报 ``category_mix_top`` 一致）。
    """
    labels: list[str] = []
    for r in rows:
        k = _competitor_matrix_group_key(r)
        if k:
            labels.append(k)
    if not labels:
        return []
    c = Counter(labels)
    common = c.most_common(top_k)
    accounted = sum(v for _, v in common)
    total = sum(c.values())
    rest = total - accounted
    out: list[tuple[str, int]] = list(common)
    if rest > 0:
        out.append(("（其余细类）", rest))
    return out


__all__ = [
    "UNCATEGORIZED_MATRIX_GROUP_LABEL",
    "_category_mix",
    "_competitor_matrix_group_key",
    "_matrix_group_label_from_detail_path",
    "_merged_row_has_sku_for_matrix",
    "_merged_rows_grouped_for_matrix",
]
