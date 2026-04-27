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

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .csv.schema import (
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
    search_csv_effective_total_sales,
    strip_buyer_ranking_line_prefix,
)
from pipeline.jd.matrix_group_label import matrix_group_label_from_detail_path
from .models import (
    JdJobCommentRow,
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
    JdProduct,
    JdProductSnapshot,
    PipelineJob,
)
from .price_parse import effective_list_price_value, float_price_from_cell
from .volume_parse import (
    comment_count_sort_value_from_cell,
    comment_count_sort_value_from_merged,
    sales_sort_value_from_search_cells,
)

logger = logging.getLogger(__name__)

FILE_MERGED_CSV = "keyword_pipeline_merged.csv"
FILE_PC_SEARCH_CSV = "pc_search_export.csv"
FILE_DETAIL_WARE_CSV = "detail_ware_export.csv"
FILE_COMMENTS_FLAT_CSV = "comments_flat.csv"

SKU_FIELD_MERGED = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
WARE_FIELD = MERGED_FIELD_TO_CSV_HEADER["ware_id"]
TITLE_FIELD = MERGED_FIELD_TO_CSV_HEADER["title"]

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


def _normalize_search_csv_total_sales(row: dict[str, str]) -> None:
    h = JD_SEARCH_CSV_HEADERS["total_sales"]
    row[h] = search_csv_effective_total_sales(row)


def _search_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    vals = {k: "" for k in JD_SEARCH_INTERNAL_KEYS}
    for csv_header, cell in row.items():
        h = (csv_header or "").strip()
        fn = SEARCH_CSV_HEADER_TO_FIELD.get(h)
        if fn:
            vals[fn] = str(cell or "").strip()
    return vals


def _detail_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    kw = {
        DETAIL_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in DETAIL_CSV_COLUMNS
    }
    if kw.get("buyer_ranking_line"):
        kw["buyer_ranking_line"] = strip_buyer_ranking_line_prefix(kw["buyer_ranking_line"])
    return kw


def _comment_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    return {
        COMMENT_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in COMMENT_CSV_COLUMNS
    }


def _normalize_merged_csv_total_sales(row: dict[str, str]) -> None:
    """列表未写 totalSales 列时，用销量楼层推断，保证入库与快照与报告计数一致。"""
    h = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
    row[h] = merged_csv_effective_total_sales(row)


def _merged_row_kwargs(row: dict[str, str]) -> dict[str, str]:
    kw = {
        MERGED_CSV_TO_FIELD[col]: str(row.get(col) or "").strip() for col in MERGED_CSV_COLUMNS
    }
    if kw.get("buyer_ranking_line"):
        kw["buyer_ranking_line"] = strip_buyer_ranking_line_prefix(kw["buyer_ranking_line"])
    return kw


def _bulk_create_in_chunks(model, objects: list[Any]) -> None:
    for i in range(0, len(objects), BULK_CHUNK):
        model.objects.bulk_create(objects[i : i + BULK_CHUNK])


def _sync_search_rows_matrix_labels(
    job: PipelineJob, merged_kw_list: list[tuple[int, dict[str, str]]]
) -> None:
    """按 SKU 将合并表解析出的类目回填到搜索行（与 §5 矩阵**同一细类划分**）。"""
    sku_to_mg: dict[str, str] = {}
    for _, kw in merged_kw_list:
        sk = (kw.get("sku_id") or "").strip()
        if not sk:
            continue
        mg = matrix_group_label_from_detail_path(kw.get("detail_category_path") or "")
        if mg:
            sku_to_mg[sk] = mg
    if not sku_to_mg:
        return
    chunk: list[JdJobSearchRow] = []
    for r in JdJobSearchRow.objects.filter(job=job).iterator(chunk_size=400):
        sk = (r.sku_id or "").strip()
        if sk and sk in sku_to_mg:
            r.matrix_group_label = sku_to_mg[sk]
            chunk.append(r)
        if len(chunk) >= 400:
            JdJobSearchRow.objects.bulk_update(chunk, ["matrix_group_label"])
            chunk.clear()
    if chunk:
        JdJobSearchRow.objects.bulk_update(chunk, ["matrix_group_label"])


def _run_dir(job: PipelineJob) -> Path:
    return Path(job.run_dir or "").expanduser().resolve()


def resolve_and_validate_run_dir(path_str: str) -> Path:
    """
    将用户输入解析为 ``LOW_GI_PROJECT_ROOT/data/JD`` 下的绝对路径，且须为已存在目录。

    相对路径相对 ``data/JD``（与创建任务时 ``pipeline_run_dir`` 语义一致）。
    """
    if not (path_str or "").strip():
        raise ValueError("run_dir 为空")
    root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not root:
        raise ValueError("LOW_GI_PROJECT_ROOT 未配置")
    project_data = Path(root).resolve() / "data" / "JD"
    p = Path(path_str.strip()).expanduser()
    if not p.is_absolute():
        p = project_data / p
    p = p.resolve()
    jd = project_data.resolve()
    try:
        p.relative_to(jd)
    except ValueError as e:
        raise ValueError(f"路径须位于京东数据目录下：{jd}") from e
    if not p.is_dir():
        raise ValueError(f"目录不存在：{p}")
    return p


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
    search_kw_list: list[tuple[int, dict[str, str]]] = []
    for i, row in enumerate(search_rows):
        _normalize_search_csv_total_sales(row)
        kw = _search_row_kwargs(row)
        search_kw_list.append((i, kw))
    s_objs: list[JdJobSearchRow] = []
    for i, kw in search_kw_list:
        pv = effective_list_price_value(
            kw.get("coupon_price"), kw.get("price"), kw.get("original_price")
        )
        sv = sales_sort_value_from_search_cells(
            kw.get("total_sales"), kw.get("comment_sales_floor")
        )
        cv = comment_count_sort_value_from_cell(kw.get("comment_count"))
        s_objs.append(
            JdJobSearchRow(
                job=job,
                row_index=i,
                matrix_group_label="",
                price_value=pv,
                sales_sort_value=sv,
                comment_count_sort_value=cv,
                **kw,
            )
        )
    _bulk_create_in_chunks(JdJobSearchRow, s_objs)
    stats["search_rows"] = len(s_objs)

    detail_path = run_dir / FILE_DETAIL_WARE_CSV
    detail_rows = _read_csv_rows(detail_path)
    d_objs: list[JdJobDetailRow] = []
    for i, row in enumerate(detail_rows):
        kw = _detail_row_kwargs(row)
        dpv = float_price_from_cell(kw.get("detail_price_final"))
        mg = matrix_group_label_from_detail_path(kw.get("detail_category_path") or "")
        d_objs.append(
            JdJobDetailRow(
                job=job,
                row_index=i,
                matrix_group_label=mg,
                detail_price_value=dpv,
                **kw,
            )
        )
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
    merged_kw_list: list[tuple[int, dict[str, str]]] = []
    for i, row in enumerate(merged_rows):
        _normalize_merged_csv_total_sales(row)
        kw = _merged_row_kwargs(row)
        merged_kw_list.append((i, kw))
    m_objs: list[JdJobMergedRow] = []
    for i, kw in merged_kw_list:
        mg = matrix_group_label_from_detail_path(kw.get("detail_category_path") or "")
        pv = effective_list_price_value(
            kw.get("coupon_price"), kw.get("price"), kw.get("original_price")
        )
        msv = sales_sort_value_from_search_cells(
            kw.get("total_sales"), kw.get("comment_sales_floor")
        )
        mcv = comment_count_sort_value_from_merged(
            kw.get("pipeline_comment_count")
        )
        m_objs.append(
            JdJobMergedRow(
                job=job,
                row_index=i,
                matrix_group_label=mg,
                price_value=pv,
                sales_sort_value=msv,
                comment_count_sort_value=mcv,
                **kw,
            )
        )
    _bulk_create_in_chunks(JdJobMergedRow, m_objs)
    stats["merged_table_rows"] = len(m_objs)
    _sync_search_rows_matrix_labels(job, merged_kw_list)

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
        br_h = MERGED_FIELD_TO_CSV_HEADER["buyer_ranking_line"]
        br = (row.get(br_h) or "").strip()
        if br:
            row[br_h] = strip_buyer_ranking_line_prefix(br)
        payload = _payload_as_json(row)
        title = (row.get(TITLE_FIELD) or "")[:2000]
        ware = (row.get(WARE_FIELD) or "").strip()[:64]
        brand = (row.get(MERGED_FIELD_TO_CSV_HEADER["detail_brand"]) or "").strip()[:512]
        price = (
            (row.get(MERGED_FIELD_TO_CSV_HEADER["detail_price_final"]) or "").strip()
            or (row.get(JD_SEARCH_CSV_HEADERS["coupon_price"]) or "").strip()
            or (row.get(JD_SEARCH_CSV_HEADERS["price"]) or "").strip()
        )[:128]
        cat = (
            (row.get(MERGED_FIELD_TO_CSV_HEADER["detail_category_path"]) or "").strip()
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
