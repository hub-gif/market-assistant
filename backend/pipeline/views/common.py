"""Pipeline API 视图共享常量与辅助函数。"""
from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.http import Http404

from ..models import (
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
    JobStatus,
    PipelineJob,
)

# 在线预览最大字节（超出则截断并提示下载）
PREVIEW_MAX_BYTES = 2 * 1024 * 1024

# 允许下载的相对文件名（均在 run_dir 下）
DOWNLOAD_NAMES = frozenset(
    {
        "merged",
        "pc_search",
        "comments",
        "detail_ware",
        "report",
    }
)


def jd_data_root() -> Path:
    root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    return (Path(root) / "data" / "JD").resolve()


def safe_file_for_job(run_dir_str: str, name: str) -> Path:
    if name not in DOWNLOAD_NAMES:
        raise Http404("unknown file")
    base = Path(run_dir_str).resolve()
    jd_root = jd_data_root().resolve()
    try:
        base.relative_to(jd_root)
    except ValueError:
        raise Http404("invalid run_dir")

    mapping = {
        "merged": "keyword_pipeline_merged.csv",
        "pc_search": "pc_search_export.csv",
        "comments": "comments_flat.csv",
        "detail_ware": "detail_ware_export.csv",
        "report": "competitor_analysis.md",
    }
    f = base / mapping[name]
    if not f.is_file():
        raise Http404("file not found")
    return f


def job_run_dir_usable(job: PipelineJob) -> bool:
    """成功、已终止或已暂停（断点产物）且已写入 run_dir 时，可预览/下载批次文件。"""
    return bool((job.run_dir or "").strip()) and job.status in (
        JobStatus.SUCCESS,
        JobStatus.CANCELLED,
        JobStatus.PAUSED,
    )


def dataset_job(pk: int) -> PipelineJob:
    job = PipelineJob.objects.filter(pk=pk).first()
    if not job:
        raise Http404()
    return job


def read_page_params(request) -> tuple[int, int]:
    page_size = min(max(int(request.query_params.get("page_size", 50)), 1), 200)
    page = max(int(request.query_params.get("page", 1)), 1)
    return page, page_size


def report_group_options_for_job(job: PipelineJob) -> list[str]:
    """类目选项：与第五章矩阵一致，来自合并表商品详情页类目路径解析。"""
    qs = (
        JdJobMergedRow.objects.filter(job=job)
        .exclude(matrix_group_label="")
        .values_list("matrix_group_label", flat=True)
        .distinct()
    )
    return sorted({str(x) for x in qs if x})


def detail_category_path_options(job: PipelineJob) -> list[str]:
    return list(
        JdJobDetailRow.objects.filter(job=job)
        .exclude(detail_category_path="")
        .values_list("detail_category_path", flat=True)
        .distinct()
        .order_by("detail_category_path")[:400]
    )


def shop_options_for_job(job: PipelineJob) -> list[str]:
    """任务内各表出现的店铺名去重排序（搜索 shop_name、商详/宽表店铺列）。"""
    names: set[str] = set()
    for v in (
        JdJobSearchRow.objects.filter(job=job)
        .exclude(shop_name="")
        .values_list("shop_name", flat=True)
        .distinct()
    ):
        t = str(v).strip()
        if t:
            names.add(t)
    for v in (
        JdJobDetailRow.objects.filter(job=job)
        .exclude(detail_shop_name="")
        .values_list("detail_shop_name", flat=True)
        .distinct()
    ):
        t = str(v).strip()
        if t:
            names.add(t)
    for v in (
        JdJobMergedRow.objects.filter(job=job)
        .exclude(shop_name="")
        .values_list("shop_name", flat=True)
        .distinct()
    ):
        t = str(v).strip()
        if t:
            names.add(t)
    for v in (
        JdJobMergedRow.objects.filter(job=job)
        .exclude(detail_shop_name="")
        .values_list("detail_shop_name", flat=True)
        .distinct()
    ):
        t = str(v).strip()
        if t:
            names.add(t)
    return sorted(names)
