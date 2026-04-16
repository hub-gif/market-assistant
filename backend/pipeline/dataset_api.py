"""库内数据浏览 API：排序、价格与报告细类筛选（查询参数解析与 QuerySet 变换）。"""
from __future__ import annotations

from typing import Any

from django.db.models import F, Q, QuerySet
from django.db.models.expressions import OrderBy
from rest_framework.request import Request

SEARCH_SORT_FIELDS = frozenset(
    {"row_index", "price", "sku_id", "title", "leaf_category", "matrix_group_label"}
)
DETAIL_SORT_FIELDS = frozenset(
    {
        "row_index",
        "price",
        "sku_id",
        "detail_category_path",
        "detail_brand",
        "matrix_group_label",
    }
)
MERGED_SORT_FIELDS = frozenset(
    {
        "row_index",
        "price",
        "sku_id",
        "title",
        "leaf_category",
        "detail_category_path",
        "matrix_group_label",
    }
)


def _parse_opt_float(val: str | None) -> float | None:
    if val is None or not str(val).strip():
        return None
    try:
        return float(str(val).strip())
    except ValueError:
        return None


def parse_sort_meta(request: Request) -> tuple[str, bool]:
    sort = (request.query_params.get("sort") or "row_index").strip()
    order = (request.query_params.get("order") or "asc").strip().lower()
    desc = order == "desc"
    return sort, desc


def price_bounds_from_request(request: Request) -> tuple[float | None, float | None]:
    return (
        _parse_opt_float(request.query_params.get("price_min")),
        _parse_opt_float(request.query_params.get("price_max")),
    )


def report_group_from_request(request: Request) -> str:
    """与 §5 矩阵一致的细类名（如「饼干」「米」）；对应查询参数 ``report_group``。"""
    return (request.query_params.get("report_group") or "").strip()


def detail_category_q_from_request(request: Request) -> str:
    return (request.query_params.get("detail_category_q") or "").strip()


def filter_echo(
    *,
    report_group: str,
    price_min: float | None,
    price_max: float | None,
    detail_category_q: str,
    sort: str,
    desc: bool,
) -> dict[str, Any]:
    return {
        "report_group": report_group or None,
        "price_min": price_min,
        "price_max": price_max,
        "detail_category_q": detail_category_q or None,
        "sort": sort,
        "order": "desc" if desc else "asc",
    }


def apply_search_filters(qs: QuerySet, request: Request) -> QuerySet:
    rg = report_group_from_request(request)
    if rg:
        qs = qs.filter(Q(matrix_group_label=rg) | Q(leaf_category=rg))
    pmin, pmax = price_bounds_from_request(request)
    if pmin is not None:
        qs = qs.filter(price_value__gte=pmin)
    if pmax is not None:
        qs = qs.filter(price_value__lte=pmax)
    return qs


def apply_search_order(qs: QuerySet, sort: str, desc: bool) -> QuerySet:
    sort = sort if sort in SEARCH_SORT_FIELDS else "row_index"
    if sort == "price":
        return qs.order_by(
            OrderBy(F("price_value"), descending=desc, nulls_last=True),
            "row_index",
        )
    if sort == "row_index":
        return qs.order_by(OrderBy(F("row_index"), descending=desc))
    field = {
        "sku_id": "sku_id",
        "title": "title",
        "leaf_category": "leaf_category",
        "matrix_group_label": "matrix_group_label",
    }[sort]
    return qs.order_by(
        OrderBy(F(field), descending=desc, nulls_last=True),
        "row_index",
    )


def apply_detail_filters(qs: QuerySet, request: Request) -> QuerySet:
    rg = report_group_from_request(request)
    if rg:
        qs = qs.filter(matrix_group_label=rg)
    q = detail_category_q_from_request(request)
    if q:
        qs = qs.filter(detail_category_path__icontains=q)
    pmin, pmax = price_bounds_from_request(request)
    if pmin is not None:
        qs = qs.filter(detail_price_value__gte=pmin)
    if pmax is not None:
        qs = qs.filter(detail_price_value__lte=pmax)
    return qs


def apply_detail_order(qs: QuerySet, sort: str, desc: bool) -> QuerySet:
    sort = sort if sort in DETAIL_SORT_FIELDS else "row_index"
    if sort == "price":
        return qs.order_by(
            OrderBy(F("detail_price_value"), descending=desc, nulls_last=True),
            "row_index",
        )
    if sort == "row_index":
        return qs.order_by(OrderBy(F("row_index"), descending=desc))
    field = {
        "sku_id": "sku_id",
        "detail_category_path": "detail_category_path",
        "detail_brand": "detail_brand",
        "matrix_group_label": "matrix_group_label",
    }[sort]
    return qs.order_by(
        OrderBy(F(field), descending=desc, nulls_last=True),
        "row_index",
    )


def apply_merged_filters(qs: QuerySet, request: Request) -> QuerySet:
    rg = report_group_from_request(request)
    if rg:
        qs = qs.filter(matrix_group_label=rg)
    q = detail_category_q_from_request(request)
    if q:
        qs = qs.filter(detail_category_path__icontains=q)
    pmin, pmax = price_bounds_from_request(request)
    if pmin is not None:
        qs = qs.filter(price_value__gte=pmin)
    if pmax is not None:
        qs = qs.filter(price_value__lte=pmax)
    return qs


def apply_merged_order(qs: QuerySet, sort: str, desc: bool) -> QuerySet:
    sort = sort if sort in MERGED_SORT_FIELDS else "row_index"
    if sort == "price":
        return qs.order_by(
            OrderBy(F("price_value"), descending=desc, nulls_last=True),
            "row_index",
        )
    if sort == "row_index":
        return qs.order_by(OrderBy(F("row_index"), descending=desc))
    field = {
        "sku_id": "sku_id",
        "title": "title",
        "leaf_category": "leaf_category",
        "detail_category_path": "detail_category_path",
        "matrix_group_label": "matrix_group_label",
    }[sort]
    return qs.order_by(
        OrderBy(F(field), descending=desc, nulls_last=True),
        "row_index",
    )
