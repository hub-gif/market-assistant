"""
任务成功后入库：
- 搜索导出 / 商详导出 / 评价扁平：按任务分表存储，便于分页与导出；
- 合并表：更新全局 ``JdProduct`` + 任务维度 ``JdProductSnapshot``。
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from django.db import transaction
from django.utils import timezone

from .csv_schema import (
    COMMENT_CSV_COLUMNS,
    COMMENT_CSV_TO_FIELD,
    DETAIL_CSV_COLUMNS,
    DETAIL_CSV_TO_FIELD,
    JD_SEARCH_CSV_HEADERS,
    JD_SEARCH_INTERNAL_KEYS,
    MERGED_CSV_COLUMNS,
    MERGED_CSV_TO_FIELD,
    MERGED_FIELD_TO_CSV_HEADER,
    SEARCH_CSV_HEADER_TO_FIELD,
    merged_csv_effective_total_sales,
)
from .models import (
    JdJobCommentRow,
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
    JdProduct,
    JdProductSnapshot,
    PipelineJob,
)

logger = logging.getLogger(__name__)

FILE_MERGED_CSV = "keyword_pipeline_merged.csv"
FILE_PC_SEARCH_CSV = "pc_search_export.csv"
FILE_DETAIL_WARE_CSV = "detail_ware_export.csv"
FILE_COMMENTS_FLAT_CSV = "comments_flat.csv"

SKU_FIELD_MERGED = "SKU(skuId)"
WARE_FIELD = "主商品ID(wareId)"
TITLE_FIELD = "标题(wareName)"

BULK_CHUNK = 400


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    raw = path.read_text(encoding="utf-8-sig")
    lines = raw.splitlines()
    if not lines:
        return []
    return list(csv.DictReader(lines))


def _payload_as_json(row: dict[str, str]) -> dict[str, str]:
    return {str(k): str(v) if v is not None else "" for k, v in row.items()}


def _search_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    vals = {k: "" for k in JD_SEARCH_INTERNAL_KEYS}
    for csv_header, cell in row.items():
        h = (csv_header or "").strip()
        fn = SEARCH_CSV_HEADER_TO_FIELD.get(h)
        if fn:
            vals[fn] = str(cell or "").strip()
    return vals


def _detail_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    return {
        DETAIL_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in DETAIL_CSV_COLUMNS
    }


def _comment_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    return {
        COMMENT_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in COMMENT_CSV_COLUMNS
    }


def _normalize_merged_csv_total_sales(row: dict[str, str]) -> None:
    """列表未写 totalSales 列时，用销量楼层推断，保证入库与快照与报告口径一致。"""
    h = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
    row[h] = merged_csv_effective_total_sales(row)


def _merged_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    return {
        MERGED_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in MERGED_CSV_COLUMNS
    }


def _bulk_create_in_chunks(model, objects: list[Any]) -> None:
    for i in range(0, len(objects), BULK_CHUNK):
        model.objects.bulk_create(objects[i : i + BULK_CHUNK])


def _run_dir(job: PipelineJob) -> Path:
    return Path(job.run_dir or "").expanduser().resolve()


def ingest_job_dataset_rows(job: PipelineJob) -> dict[str, Any]:
    """
    删除该任务旧数据后，将 ``pc_search_export`` / ``detail_ware_export`` / ``comments_flat`` 全量写入数据库。
    """
    if not (job.run_dir or "").strip():
        raise FileNotFoundError("任务无 run_dir")

    run_dir = _run_dir(job)
    stats: dict[str, Any] = {
        "search_rows": 0,
        "detail_rows": 0,
        "comment_rows": 0,
        "merged_table_rows": 0,
    }

    JdJobSearchRow.objects.filter(job=job).delete()
    JdJobDetailRow.objects.filter(job=job).delete()
    JdJobCommentRow.objects.filter(job=job).delete()
    JdJobMergedRow.objects.filter(job=job).delete()

    search_path = run_dir / FILE_PC_SEARCH_CSV
    search_rows = _read_csv_rows(search_path)
    if not search_rows and search_path.is_file() is False:
        pass
    s_objs: list[JdJobSearchRow] = []
    for i, row in enumerate(search_rows):
        kw = _search_row_kwargs(row)
        s_objs.append(JdJobSearchRow(job=job, row_index=i, **kw))
    _bulk_create_in_chunks(JdJobSearchRow, s_objs)
    stats["search_rows"] = len(s_objs)

    detail_path = run_dir / FILE_DETAIL_WARE_CSV
    detail_rows = _read_csv_rows(detail_path)
    d_objs: list[JdJobDetailRow] = []
    for i, row in enumerate(detail_rows):
        kw = _detail_row_kwargs(row)
        d_objs.append(JdJobDetailRow(job=job, row_index=i, **kw))
    _bulk_create_in_chunks(JdJobDetailRow, d_objs)
    stats["detail_rows"] = len(d_objs)

    comment_path = run_dir / FILE_COMMENTS_FLAT_CSV
    comment_rows = _read_csv_rows(comment_path)
    c_objs: list[JdJobCommentRow] = []
    for i, row in enumerate(comment_rows):
        kw = _comment_row_kwargs(row)
        c_objs.append(JdJobCommentRow(job=job, row_index=i, **kw))
    _bulk_create_in_chunks(JdJobCommentRow, c_objs)
    stats["comment_rows"] = len(c_objs)

    merged_path = run_dir / FILE_MERGED_CSV
    merged_rows = _read_csv_rows(merged_path) if merged_path.is_file() else []
    m_objs: list[JdJobMergedRow] = []
    for i, row in enumerate(merged_rows):
        _normalize_merged_csv_total_sales(row)
        kw = _merged_row_kwargs(row)
        m_objs.append(JdJobMergedRow(job=job, row_index=i, **kw))
    _bulk_create_in_chunks(JdJobMergedRow, m_objs)
    stats["merged_table_rows"] = len(m_objs)

    return stats


def ingest_job_merged_csv(job: PipelineJob) -> dict[str, Any]:
    """
    读取合并表，upsert ``JdProduct``，并按 (商品, 任务) 写入 ``JdProductSnapshot``。
    """
    run_dir = _run_dir(job)
    path = run_dir / FILE_MERGED_CSV
    if not path.is_file():
        raise FileNotFoundError(f"合并表不存在: {path}")

    rows = _read_csv_rows(path)
    captured_at = job.updated_at or timezone.now()
    stats = {
        "merged_file": str(path),
        "rows_in_csv": len(rows),
        "rows_ingested": 0,
        "products_created": 0,
        "snapshots_upserted": 0,
    }

    platform = (job.platform or "jd").strip() or "jd"

    for row in rows:
        sku = (row.get(SKU_FIELD_MERGED) or "").strip()
        if not sku:
            continue
        _normalize_merged_csv_total_sales(row)
        payload = _payload_as_json(row)
        title = (row.get(TITLE_FIELD) or "")[:2000]
        ware = (row.get(WARE_FIELD) or "").strip()[:64]
        brand = (row.get("detail_brand") or "").strip()[:512]
        price = (
            (row.get("detail_price_final") or "").strip()
            or (row.get(JD_SEARCH_CSV_HEADERS["coupon_price"]) or "").strip()
            or (row.get(JD_SEARCH_CSV_HEADERS["price"]) or "").strip()
        )[:128]
        cat = (
            (row.get("detail_category_path") or "").strip()
            or (row.get(JD_SEARCH_CSV_HEADERS["leaf_category"]) or "").strip()
        )[:2000]

        product, created = JdProduct.objects.get_or_create(
            platform=platform,
            sku_id=sku,
            defaults={
                "ware_id": ware,
                "title": title,
                "detail_brand": brand,
                "detail_price_final": price,
                "detail_category_path": cat,
                "current_payload": payload,
                "last_job": job,
                "last_captured_at": captured_at,
            },
        )
        if created:
            stats["products_created"] += 1
        else:
            product.ware_id = ware or product.ware_id
            product.title = title or product.title
            product.detail_brand = brand
            product.detail_price_final = price
            product.detail_category_path = cat
            product.current_payload = payload
            product.last_job = job
            product.last_captured_at = captured_at
            product.save(
                update_fields=[
                    "ware_id",
                    "title",
                    "detail_brand",
                    "detail_price_final",
                    "detail_category_path",
                    "current_payload",
                    "last_job",
                    "last_captured_at",
                    "updated_at",
                ]
            )

        JdProductSnapshot.objects.update_or_create(
            product=product,
            job=job,
            defaults={
                "run_dir": job.run_dir or "",
                "captured_at": captured_at,
                "payload": payload,
            },
        )
        stats["snapshots_upserted"] += 1
        stats["rows_ingested"] += 1

    return stats


def ingest_job_full(job: PipelineJob) -> dict[str, Any]:
    """
    先提交搜索/详情/评论（与 CSV 行一一对应），再单独提交合并表主档与快照。
    合并表缺失时仍保留前三类数据，便于仅用列表/评价做回顾。
    """
    out: dict[str, Any] = {}
    with transaction.atomic():
        out["dataset"] = ingest_job_dataset_rows(job)
    try:
        with transaction.atomic():
            out["merged"] = ingest_job_merged_csv(job)
    except FileNotFoundError as e:
        logger.warning("ingest merged skipped job=%s: %s", job.id, e)
        out["merged"] = {"error": str(e), "rows_ingested": 0, "snapshots_upserted": 0}
    return out


def try_ingest_job_full(job: PipelineJob) -> None:
    try:
        stats = ingest_job_full(job)
        logger.info("ingest_job_full job=%s %s", job.id, stats)
    except Exception:
        logger.exception("ingest_job_full failed job=%s", job.id)
