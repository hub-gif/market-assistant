"""任务入库后的数据集浏览、筛选与导出。"""
from __future__ import annotations

from django.http import HttpResponse
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..dataset_api import (
    DETAIL_SORT_FIELDS,
    MERGED_SORT_FIELDS,
    SEARCH_SORT_FIELDS,
    apply_detail_filters,
    apply_detail_order,
    apply_merged_filters,
    apply_merged_order,
    apply_search_filters,
    apply_search_order,
    detail_category_q_from_request,
    filter_echo,
    parse_sort_meta,
    price_bounds_from_request,
    report_group_from_request,
    shop_from_request,
)
from ..dataset_nonempty import (
    comment_columns_for_api,
    detail_columns_for_api,
    merged_columns_for_api,
    search_columns_for_api,
)
from ..export_job import build_csv_bytes, build_json_bytes, build_xlsx_bytes
from ..models import (
    JdJobCommentRow,
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
)
from ..row_serialize import (
    comment_row_to_dict,
    detail_row_to_dict,
    merged_row_to_dict,
    search_row_to_dict,
)
from .common import (
    dataset_job,
    detail_category_path_options,
    read_page_params,
    report_group_options_for_job,
    shop_options_for_job,
)


class JobDatasetSummaryView(APIView):
    """任务在库中的搜索/详情/评价行数（入库后可用）。"""

    def get(self, request, pk: int):
        job = dataset_job(pk)
        return Response(
            {
                "job_id": job.id,
                "keyword": job.keyword,
                "status": job.status,
                "search_rows": JdJobSearchRow.objects.filter(job=job).count(),
                "detail_rows": JdJobDetailRow.objects.filter(job=job).count(),
                "comment_rows": JdJobCommentRow.objects.filter(job=job).count(),
                "merged_rows": JdJobMergedRow.objects.filter(job=job).count(),
                "search_columns": search_columns_for_api(job),
                "detail_columns": detail_columns_for_api(job),
                "comment_columns": comment_columns_for_api(job),
                "merged_columns": merged_columns_for_api(job),
                "category_options": report_group_options_for_job(job),
                "shop_options": shop_options_for_job(job),
                "detail_category_path_options": detail_category_path_options(job),
                "dataset_sort_help": {
                    "search": sorted(SEARCH_SORT_FIELDS),
                    "detail": sorted(DETAIL_SORT_FIELDS),
                    "merged": sorted(MERGED_SORT_FIELDS),
                    "comments": ["row_index"],
                },
            }
        )


class JobDatasetSearchView(APIView):
    def get(self, request, pk: int):
        job = dataset_job(pk)
        page, page_size = read_page_params(request)
        sort, desc = parse_sort_meta(request)
        sort_eff = sort if sort in SEARCH_SORT_FIELDS else "row_index"
        rg = report_group_from_request(request)
        sp = shop_from_request(request)
        pmin, pmax = price_bounds_from_request(request)
        dcq = detail_category_q_from_request(request)
        qs = JdJobSearchRow.objects.filter(job=job)
        qs = apply_search_filters(qs, request)
        qs = apply_search_order(qs, sort_eff, desc)
        total = qs.count()
        start = (page - 1) * page_size
        rows = qs[start : start + page_size]
        return Response(
            {
                "total": total,
                "page": page,
                "page_size": page_size,
                "filters": filter_echo(
                    report_group=rg,
                    shop=sp,
                    price_min=pmin,
                    price_max=pmax,
                    detail_category_q=dcq,
                    sort=sort_eff,
                    desc=desc,
                ),
                "results": [search_row_to_dict(r) for r in rows],
            }
        )


class JobDatasetDetailView(APIView):
    def get(self, request, pk: int):
        job = dataset_job(pk)
        page, page_size = read_page_params(request)
        sort, desc = parse_sort_meta(request)
        sort_eff = sort if sort in DETAIL_SORT_FIELDS else "row_index"
        rg = report_group_from_request(request)
        sp = shop_from_request(request)
        pmin, pmax = price_bounds_from_request(request)
        dcq = detail_category_q_from_request(request)
        qs = JdJobDetailRow.objects.filter(job=job)
        qs = apply_detail_filters(qs, request)
        qs = apply_detail_order(qs, sort_eff, desc)
        total = qs.count()
        start = (page - 1) * page_size
        rows = qs[start : start + page_size]
        return Response(
            {
                "total": total,
                "page": page,
                "page_size": page_size,
                "filters": filter_echo(
                    report_group=rg,
                    shop=sp,
                    price_min=pmin,
                    price_max=pmax,
                    detail_category_q=dcq,
                    sort=sort_eff,
                    desc=desc,
                ),
                "results": [detail_row_to_dict(r) for r in rows],
            }
        )


class JobDatasetCommentsView(APIView):
    def get(self, request, pk: int):
        job = dataset_job(pk)
        page, page_size = read_page_params(request)
        sku_id = (request.query_params.get("sku_id") or "").strip()
        qs = JdJobCommentRow.objects.filter(job=job)
        if sku_id:
            qs = qs.filter(sku_id=sku_id)
        total = qs.count()
        start = (page - 1) * page_size
        rows = qs.order_by("row_index")[start : start + page_size]
        return Response(
            {
                "total": total,
                "page": page,
                "page_size": page_size,
                "sku_filter": sku_id or None,
                "results": [comment_row_to_dict(r) for r in rows],
            }
        )


class JobDatasetMergedView(APIView):
    def get(self, request, pk: int):
        job = dataset_job(pk)
        page, page_size = read_page_params(request)
        sort, desc = parse_sort_meta(request)
        sort_eff = sort if sort in MERGED_SORT_FIELDS else "row_index"
        rg = report_group_from_request(request)
        sp = shop_from_request(request)
        pmin, pmax = price_bounds_from_request(request)
        dcq = detail_category_q_from_request(request)
        qs = JdJobMergedRow.objects.filter(job=job)
        qs = apply_merged_filters(qs, request)
        qs = apply_merged_order(qs, sort_eff, desc)
        total = qs.count()
        start = (page - 1) * page_size
        rows = qs[start : start + page_size]
        return Response(
            {
                "total": total,
                "page": page,
                "page_size": page_size,
                "filters": filter_echo(
                    report_group=rg,
                    shop=sp,
                    price_min=pmin,
                    price_max=pmax,
                    detail_category_q=dcq,
                    sort=sort_eff,
                    desc=desc,
                ),
                "results": [merged_row_to_dict(r) for r in rows],
            }
        )


class JobDatasetExportView(APIView):
    """下载：kind=search|detail|comments|merged|all，export_fmt=json|csv|xlsx。

    ``merged``：库内合并宽表行（与 lean 合并 CSV 列一致，入库后导出）。
    注意：勿使用查询参数名 ``format``，DRF 会将其用于内容协商，非 json 时易在进视图前 404。
    """

    def get(self, request, pk: int):
        job = dataset_job(pk)
        kind = (request.query_params.get("kind") or "search").strip().lower()
        fmt = (request.query_params.get("export_fmt") or "json").strip().lower()
        if kind not in ("search", "detail", "comments", "all", "merged"):
            return Response(
                {"detail": "kind 须为 search / detail / comments / all / merged"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if fmt not in ("json", "csv", "xlsx"):
            return Response(
                {"detail": "export_fmt 须为 json / csv / xlsx"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            if fmt == "json":
                data, filename = build_json_bytes(job=job, kind=kind)
                resp = HttpResponse(data, content_type="application/json; charset=utf-8")
            elif fmt == "csv":
                data, filename = build_csv_bytes(job=job, kind=kind)
                resp = HttpResponse(data, content_type="text/csv; charset=utf-8")
            else:
                data, filename = build_xlsx_bytes(job=job, kind=kind)
                resp = HttpResponse(
                    data,
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        resp["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp
