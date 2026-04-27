"""将任务目录下的 CSV 重新入库。"""
from __future__ import annotations

from django.http import Http404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..ingest import ingest_job_full
from ..models import JobStatus, PipelineJob


@method_decorator(csrf_exempt, name="dispatch")
class JobImportMergedView(APIView):
    """将指定任务目录下搜索/详情/评价 CSV 与合并表重新写入数据库（幂等：先清空该任务三类行再全量插入）。"""

    def post(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if job.status == JobStatus.RUNNING:
            return Response(
                {"detail": "执行中不可入库，请待任务结束或终止后再试"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not (job.run_dir or "").strip():
            return Response(
                {
                    "detail": "任务未绑定 run_dir。可 PATCH /api/pipeline/jobs/<id>/ "
                    "传入 run_dir，或使用 python manage.py ingest_pipeline_dataset。"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            stats = ingest_job_full(job)
        except FileNotFoundError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(stats)
