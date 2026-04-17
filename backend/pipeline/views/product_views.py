"""跨任务的京东商品与快照查询。"""
from __future__ import annotations

from django.db.models import Count, Q
from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from ..models import JdProduct, JdProductSnapshot
from ..serializers import (
    JdProductDetailSerializer,
    JdProductListSerializer,
    JdProductSnapshotBriefSerializer,
    JdProductSnapshotDetailSerializer,
)


class JdProductListView(APIView):
    """已入库 SKU 分页列表；支持按标题/SKU/品牌模糊搜、按合并表中的 pipeline_keyword 精确筛。"""

    def get(self, request):
        limit = min(max(int(request.query_params.get("limit", 50)), 1), 200)
        offset = max(int(request.query_params.get("offset", 0)), 0)
        q = (request.query_params.get("q") or "").strip()
        kw = (request.query_params.get("keyword") or "").strip()
        qs = JdProduct.objects.annotate(snapshot_count=Count("snapshots"))
        if q:
            qs = qs.filter(
                Q(sku_id__icontains=q)
                | Q(title__icontains=q)
                | Q(detail_brand__icontains=q)
            )
        if kw:
            from ..csv_schema import MERGED_FIELD_TO_CSV_HEADER

            h_kw = MERGED_FIELD_TO_CSV_HEADER["pipeline_keyword"]
            qs = qs.filter(
                Q(current_payload__pipeline_keyword=kw)
                | Q(**{f"current_payload__{h_kw}": kw})
            )
        total = qs.count()
        page = qs.order_by("-updated_at")[offset : offset + limit]
        return Response(
            {
                "total": total,
                "limit": limit,
                "offset": offset,
                "results": JdProductListSerializer(page, many=True).data,
            }
        )


class JdProductDetailView(APIView):
    def get(self, request, sku_id: str):
        platform = (request.query_params.get("platform") or "jd").strip() or "jd"
        obj = (
            JdProduct.objects.annotate(snapshot_count=Count("snapshots"))
            .filter(platform=platform, sku_id=sku_id)
            .first()
        )
        if not obj:
            raise Http404()
        return Response(JdProductDetailSerializer(obj).data)


class JdProductSnapshotListView(APIView):
    """某 SKU 的历史快照列表（不含整包 payload，便于时间线）。"""

    def get(self, request, sku_id: str):
        platform = (request.query_params.get("platform") or "jd").strip() or "jd"
        product = JdProduct.objects.filter(platform=platform, sku_id=sku_id).first()
        if not product:
            raise Http404()
        snaps = (
            product.snapshots.select_related("job")
            .order_by("-captured_at")
            .all()
        )
        return Response(
            {
                "platform": platform,
                "sku_id": sku_id,
                "count": snaps.count(),
                "results": JdProductSnapshotBriefSerializer(snaps, many=True).data,
            }
        )


class JdProductSnapshotDetailView(APIView):
    """单条快照完整 payload，用于历史回放与字段级对比。"""

    def get(self, request, pk: int):
        snap = (
            JdProductSnapshot.objects.select_related("product", "job")
            .filter(pk=pk)
            .first()
        )
        if not snap:
            raise Http404()
        return Response(JdProductSnapshotDetailSerializer(snap).data)
