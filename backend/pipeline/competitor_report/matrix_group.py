"""竞品矩阵细类键：与 ``pipeline.jd.matrix_group_label`` 及 §5 矩阵/扇图同源。"""
from __future__ import annotations

from collections import Counter

from pipeline.jd.matrix_group_label import (
    matrix_group_label_from_detail_path as _matrix_group_label_from_path,
)

from .csv_io import _detail_category_path_cell


def _matrix_group_label_from_detail_path(row: dict[str, str]) -> str:
    return _matrix_group_label_from_path(_detail_category_path_cell(row))


def _competitor_matrix_group_key(row: dict[str, str]) -> str:
    """
    竞品矩阵分组：§5 / §8 / 统计图共用。
    **仅**依据 ``detail_category_path``；列为空或路径段均为无意义编码时不参与矩阵（返回空串）。
    """
    return _matrix_group_label_from_detail_path(row)


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
    仅含 ``detail_category_path`` 可解析为展示名的行。

    返回 ``most_common(top_k)``，并将未列入 Top K 的款数合并为「（其余细类）」，
    使各块 SKU 数之和等于有效矩阵 SKU 总数（与扇形图、简报 ``category_mix_top`` 一致）。
    """
    labels: list[str] = []
    for r in rows:
        k = _matrix_group_label_from_detail_path(r)
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
    "_category_mix",
    "_competitor_matrix_group_key",
    "_matrix_group_label_from_detail_path",
    "_merged_rows_grouped_for_matrix",
]
