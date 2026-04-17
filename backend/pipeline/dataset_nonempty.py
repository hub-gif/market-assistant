"""按任务扫描库内行，得到「全表至少一格非空」的列，供摘要 / 浏览 / 导出一致裁剪。"""
from __future__ import annotations

from .csv.schema import (
    COMMENT_CSV_COLUMNS,
    COMMENT_CSV_TO_FIELD,
    DETAIL_CSV_COLUMNS,
    DETAIL_CSV_TO_FIELD,
    JD_SEARCH_CSV_HEADERS,
    JD_SEARCH_INTERNAL_KEYS,
    MERGED_FIELD_TO_CSV_HEADER,
    MERGED_INTERNAL_KEYS,
)
from .models import JdJobCommentRow, JdJobDetailRow, JdJobMergedRow, JdJobSearchRow, PipelineJob
from .row_serialize import COMMENT_FIELDS_ORDER, DETAIL_FIELDS_ORDER

MATRIX_GROUP_COLUMN = {"key": "matrix_group_label", "label": "类目"}


def _is_nonempty(val) -> bool:
    if val is None:
        return False
    return str(val).strip() != ""


def nonempty_search_keys_for_job(job: PipelineJob) -> list[str]:
    qs = JdJobSearchRow.objects.filter(job=job)
    present = {k: False for k in JD_SEARCH_INTERNAL_KEYS}
    for row in qs.iterator(chunk_size=400):
        for k in JD_SEARCH_INTERNAL_KEYS:
            if not present[k] and _is_nonempty(getattr(row, k, None)):
                present[k] = True
    return [k for k in JD_SEARCH_INTERNAL_KEYS if present[k]]


def nonempty_detail_fields_for_job(job: PipelineJob) -> list[str]:
    qs = JdJobDetailRow.objects.filter(job=job)
    present = {k: False for k in DETAIL_FIELDS_ORDER}
    for row in qs.iterator(chunk_size=400):
        for k in DETAIL_FIELDS_ORDER:
            if not present[k] and _is_nonempty(getattr(row, k, None)):
                present[k] = True
    return [k for k in DETAIL_FIELDS_ORDER if present[k]]


def nonempty_comment_fields_for_job(job: PipelineJob) -> list[str]:
    qs = JdJobCommentRow.objects.filter(job=job)
    present = {k: False for k in COMMENT_FIELDS_ORDER}
    for row in qs.iterator(chunk_size=400):
        for k in COMMENT_FIELDS_ORDER:
            if not present[k] and _is_nonempty(getattr(row, k, None)):
                present[k] = True
    return [k for k in COMMENT_FIELDS_ORDER if present[k]]


def nonempty_merged_fields_for_job(job: PipelineJob) -> list[str]:
    qs = JdJobMergedRow.objects.filter(job=job)
    present = {k: False for k in MERGED_INTERNAL_KEYS}
    for row in qs.iterator(chunk_size=400):
        for k in MERGED_INTERNAL_KEYS:
            if not present[k] and _is_nonempty(getattr(row, k, None)):
                present[k] = True
    return [k for k in MERGED_INTERNAL_KEYS if present[k]]


def search_columns_for_api(job: PipelineJob) -> list[dict[str, str]]:
    cols = [
        {"key": k, "label": JD_SEARCH_CSV_HEADERS[k]}
        for k in nonempty_search_keys_for_job(job)
    ]
    if JdJobSearchRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        cols.append(dict(MATRIX_GROUP_COLUMN))
    return cols


def _detail_field_to_csv_col(field: str) -> str:
    for col, fn in DETAIL_CSV_TO_FIELD.items():
        if fn == field:
            return col
    return field


def detail_columns_for_api(job: PipelineJob) -> list[dict[str, str]]:
    cols = [
        {"key": f, "label": _detail_field_to_csv_col(f)}
        for f in nonempty_detail_fields_for_job(job)
    ]
    if JdJobDetailRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        cols.append(dict(MATRIX_GROUP_COLUMN))
    return cols


def _comment_field_to_csv_col(field: str) -> str:
    for col, fn in COMMENT_CSV_TO_FIELD.items():
        if fn == field:
            return col
    return field


def comment_columns_for_api(job: PipelineJob) -> list[dict[str, str]]:
    return [
        {"key": f, "label": _comment_field_to_csv_col(f)}
        for f in nonempty_comment_fields_for_job(job)
    ]


def merged_columns_for_api(job: PipelineJob) -> list[dict[str, str]]:
    cols = [
        {"key": k, "label": MERGED_FIELD_TO_CSV_HEADER[k]}
        for k in nonempty_merged_fields_for_job(job)
    ]
    if JdJobMergedRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        cols.append(dict(MATRIX_GROUP_COLUMN))
    return cols


def search_export_headers(job: PipelineJob) -> list[str]:
    keys = nonempty_search_keys_for_job(job)
    h = ["id", "row_index"] + [JD_SEARCH_CSV_HEADERS[k] for k in keys]
    if JdJobSearchRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        h.append(MATRIX_GROUP_COLUMN["label"])
    return h


def detail_export_headers(job: PipelineJob) -> list[str]:
    fields = set(nonempty_detail_fields_for_job(job))
    cols = [c for c in DETAIL_CSV_COLUMNS if DETAIL_CSV_TO_FIELD[c] in fields]
    h = ["id", "row_index"] + cols
    if JdJobDetailRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        h.append(MATRIX_GROUP_COLUMN["label"])
    return h


def comment_export_headers(job: PipelineJob) -> list[str]:
    fields = set(nonempty_comment_fields_for_job(job))
    cols = [c for c in COMMENT_CSV_COLUMNS if COMMENT_CSV_TO_FIELD[c] in fields]
    return ["id", "row_index"] + cols


def merged_export_headers(job: PipelineJob) -> list[str]:
    keys = nonempty_merged_fields_for_job(job)
    h = ["id", "row_index"] + [MERGED_FIELD_TO_CSV_HEADER[k] for k in keys]
    if JdJobMergedRow.objects.filter(job=job).exclude(matrix_group_label="").exists():
        h.append(MATRIX_GROUP_COLUMN["label"])
    return h
