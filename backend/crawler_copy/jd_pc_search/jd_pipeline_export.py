# -*- coding: utf-8 -*-
"""
流水线**落盘层**：合并表 / PC 搜索导出 / 详情扁平 CSV 的列名、行规范化与 UTF-8 BOM 写入。

与 ``jd_keyword_pipeline`` 中的 **采集编排**（Playwright、请求、合并内存行）分离，便于单独阅读与单测。
"""
from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

from pipeline.csv.schema import (  # noqa: E402
    MERGED_CSV_COLUMNS,
    remap_merged_row_english_detail_keys_to_csv_headers,
)
from jd_detail_ware_business_requests import (  # noqa: E402
    DETAIL_WARE_LEAN_CSV_FIELDNAMES,
    WARE_BUSINESS_MERGE_FIELDNAMES,
    WARE_PARSED_CSV_FIELDNAMES,
)
from jd_h5_search_requests import CSV_FIELDS, JD_EXPORT_COLUMN_HEADERS  # noqa: E402

SKU_CSV_HEADER = JD_EXPORT_COLUMN_HEADERS["sku_id"]

_MERGED_EXTRA_FIELDS = (
    ["pipeline_keyword"]
    + list(WARE_BUSINESS_MERGE_FIELDNAMES)
    + ["comment_count", "comment_preview"]
)


def finalize_merged_row_for_disk(merged: dict[str, str]) -> None:
    """英文内部键 → 中文 CSV 列名；评论摘要列名。"""
    remap_merged_row_english_detail_keys_to_csv_headers(merged)
    if "comment_count" in merged:
        merged["评论条数"] = str(merged.pop("comment_count") or "")
    if "comment_preview" in merged:
        merged["评价摘要"] = str(merged.pop("comment_preview") or "")


def merged_csv_fieldnames(merged_csv_mode: str) -> list[str]:
    if (merged_csv_mode or "lean").strip().lower() == "full":
        return list(CSV_FIELDS) + [
            f for f in _MERGED_EXTRA_FIELDS if f not in CSV_FIELDS
        ]
    return list(MERGED_CSV_COLUMNS)


def normalize_merged_rows_for_export(rows: list[dict[str, str]]) -> None:
    """
    整合表落盘前：搜索侧「榜单类文案」与「榜单排名」去掉 ``榜单/曝光：`` 前缀，
    与 ``strip_buyer_ranking_line_prefix`` / 入库规则一致。
    """
    from pipeline.csv.schema import strip_buyer_ranking_line_prefix  # noqa: WPS433

    hot_key = "榜单类文案"
    rank_key = "榜单排名"
    for merged in rows:
        if merged.get(hot_key):
            merged[hot_key] = strip_buyer_ranking_line_prefix(merged[hot_key])
        merged[rank_key] = strip_buyer_ranking_line_prefix(merged.get(rank_key) or "")


def detail_ware_csv_fieldnames(detail_ware_csv_mode: str) -> list[str]:
    if (detail_ware_csv_mode or "lean").strip().lower() == "full":
        return list(WARE_PARSED_CSV_FIELDNAMES)
    return list(DETAIL_WARE_LEAN_CSV_FIELDNAMES)


def dedupe_comment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按 commentId 去重（跨首屏 + 多页列表）。"""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        cid = str(r.get("commentId") or "").strip()
        if cid:
            if cid in seen:
                continue
            seen.add(cid)
        out.append(r)
    return out


def comment_fields_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    previews: list[str] = []
    for r in rows[:8]:
        t = str(r.get("tagCommentContent") or "").strip()
        if t:
            previews.append(t[:400])
    joined = " | ".join(previews)[:4000]
    return {
        "comment_count": str(len(rows)),
        "comment_preview": joined,
    }


def write_pc_search_export_csv(
    path: Path, rows: list[dict[str, str]]
) -> None:
    """写入 ``pc_search_export.csv``（UTF-8 BOM + 全列）。"""
    sbuf = StringIO()
    sw = csv.DictWriter(
        sbuf, fieldnames=list(CSV_FIELDS), extrasaction="ignore"
    )
    sw.writeheader()
    sw.writerows(rows)
    path.write_text("\ufeff" + sbuf.getvalue(), encoding="utf-8")


def write_merged_csv(
    path: Path,
    merged_rows: list[dict[str, str]],
    *,
    merged_csv_mode: str,
) -> tuple[list[str], int]:
    """
    写入合并表；返回 (fieldnames, 列数) 供 ``run_meta`` 使用。
    """
    fieldnames = merged_csv_fieldnames(merged_csv_mode)
    normalize_merged_rows_for_export(merged_rows)
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    w.writerows(merged_rows)
    path.write_text("\ufeff" + buf.getvalue(), encoding="utf-8")
    return fieldnames, len(fieldnames)


def write_detail_ware_csv(
    path: Path,
    detail_csv_rows: list[dict[str, str]],
    *,
    detail_ware_csv_mode: str,
) -> tuple[list[str], int]:
    """写入 ``detail_ware_export.csv``；返回 (fieldnames, 列数)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    detail_fn = detail_ware_csv_fieldnames(detail_ware_csv_mode)
    with path.open("w", encoding="utf-8-sig", newline="") as dcf:
        dw = csv.DictWriter(
            dcf,
            fieldnames=detail_fn,
            extrasaction="ignore",
        )
        dw.writeheader()
        dw.writerows(detail_csv_rows)
    return detail_fn, len(detail_fn)


def write_run_meta_json(path: Path, meta: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


__all__ = [
    "SKU_CSV_HEADER",
    "comment_fields_from_rows",
    "dedupe_comment_rows",
    "detail_ware_csv_fieldnames",
    "finalize_merged_row_for_disk",
    "merged_csv_fieldnames",
    "normalize_merged_rows_for_export",
    "write_detail_ware_csv",
    "write_merged_csv",
    "write_pc_search_export_csv",
    "write_run_meta_json",
]
