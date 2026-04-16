"""任务数据集 ORM 行 ↔ API / 导出用的扁平字典。"""
from __future__ import annotations

from typing import Any

from .csv_schema import (
    COMMENT_CSV_COLUMNS,
    COMMENT_CSV_TO_FIELD,
    DETAIL_CSV_COLUMNS,
    DETAIL_CSV_TO_FIELD,
    JD_SEARCH_INTERNAL_KEYS,
    MERGED_INTERNAL_KEYS,
)
from .models import JdJobCommentRow, JdJobDetailRow, JdJobMergedRow, JdJobSearchRow

DETAIL_FIELDS_ORDER: tuple[str, ...] = tuple(DETAIL_CSV_TO_FIELD[c] for c in DETAIL_CSV_COLUMNS)
COMMENT_FIELDS_ORDER: tuple[str, ...] = tuple(COMMENT_CSV_TO_FIELD[c] for c in COMMENT_CSV_COLUMNS)
MERGED_FIELDS_ORDER: tuple[str, ...] = MERGED_INTERNAL_KEYS


def search_row_to_dict(r: JdJobSearchRow) -> dict[str, Any]:
    out: dict[str, Any] = {"id": r.id, "row_index": r.row_index}
    for k in JD_SEARCH_INTERNAL_KEYS:
        out[k] = getattr(r, k) or ""
    out["matrix_group_label"] = r.matrix_group_label or ""
    return out


def detail_row_to_dict(r: JdJobDetailRow) -> dict[str, Any]:
    out: dict[str, Any] = {"id": r.id, "row_index": r.row_index}
    for k in DETAIL_FIELDS_ORDER:
        out[k] = getattr(r, k) or ""
    out["matrix_group_label"] = r.matrix_group_label or ""
    return out


def comment_row_to_dict(r: JdJobCommentRow) -> dict[str, Any]:
    out: dict[str, Any] = {"id": r.id, "row_index": r.row_index}
    for k in COMMENT_FIELDS_ORDER:
        out[k] = getattr(r, k) or ""
    return out


def merged_row_to_dict(r: JdJobMergedRow) -> dict[str, Any]:
    out: dict[str, Any] = {"id": r.id, "row_index": r.row_index}
    for k in MERGED_FIELDS_ORDER:
        out[k] = getattr(r, k) or ""
    out["matrix_group_label"] = r.matrix_group_label or ""
    return out
