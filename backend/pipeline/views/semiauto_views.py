"""半自动 JD 监听任务 API：启动、确认登录、结束任务。"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

from django.conf import settings
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..models import JobStatus, PipelineJob
from ..semiauto_tasks import start_semiauto_job
from ..serializers import PipelineJobSerializer

logger = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name="dispatch")
class SemiAutoJobCreateView(APIView):
    """POST /api/jobs/semiauto/ — 创建半自动任务并在后台线程中启动浏览器监听。"""

    def post(self, request):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        keyword = (request.data.get("keyword") or "").strip()
        if not keyword:
            return Response({"detail": "keyword 不能为空"}, status=status.HTTP_400_BAD_REQUEST)

        job = PipelineJob.objects.create(
            platform="jd",
            keyword=keyword,
            status=JobStatus.RUNNING,
            source_type="semiauto",
            semiauto_phase="browser_open",
        )

        t = threading.Thread(target=start_semiauto_job, args=(job.id,), daemon=True)
        t.start()

        return Response(PipelineJobSerializer(job).data, status=status.HTTP_201_CREATED)


@method_decorator(csrf_exempt, name="dispatch")
class SemiAutoConfirmLoginView(APIView):
    """POST /api/jobs/<id>/semiauto/confirm-login/ — 写入登录确认信号，触发监听开始。"""

    def post(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk, source_type="semiauto").first()
        if not job:
            return Response({"detail": "任务不存在或不是半自动任务"}, status=status.HTTP_404_NOT_FOUND)
        if job.status not in (JobStatus.RUNNING,):
            return Response({"detail": f"任务状态不可操作：{job.status}"}, status=status.HTTP_409_CONFLICT)

        run_dir = (job.run_dir or "").strip()
        if not run_dir:
            return Response({"detail": "run_dir 尚未就绪，请稍后重试"}, status=status.HTTP_409_CONFLICT)

        login_file = Path(run_dir) / ".login_confirmed"
        try:
            login_file.touch()
        except Exception as e:
            logger.exception("confirm_login 写入信号文件失败 job=%s", pk)
            return Response({"detail": f"写入信号文件失败：{e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"detail": "已发送登录确认"})


@method_decorator(csrf_exempt, name="dispatch")
class SemiAutoRestartListenView(APIView):
    """POST /api/jobs/<id>/semiauto/restart-listen/ — 触发监听重新挂载（不关浏览器）。"""

    def post(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk, source_type="semiauto").first()
        if not job:
            return Response({"detail": "任务不存在或不是半自动任务"}, status=status.HTTP_404_NOT_FOUND)
        if job.status not in (JobStatus.RUNNING,):
            return Response({"detail": f"任务状态不可操作：{job.status}"}, status=status.HTTP_409_CONFLICT)

        run_dir = (job.run_dir or "").strip()
        if not run_dir:
            return Response({"detail": "run_dir 尚未就绪，请稍后重试"}, status=status.HTTP_409_CONFLICT)
        phase = (job.semiauto_phase or "").strip()
        if phase != "listening":
            return Response(
                {"detail": f"仅在「监听中」阶段可重启监听（当前 phase={phase or '未知'}）"},
                status=status.HTTP_409_CONFLICT,
            )

        restart_file = Path(run_dir) / ".restart_listen_requested"
        try:
            restart_file.touch()
        except Exception as e:
            logger.exception("semiauto_restart_listen 写入信号文件失败 job=%s", pk)
            return Response({"detail": f"写入信号文件失败：{e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"detail": "已请求重启监听，数秒内控制台应出现「重新挂载」日志"})


@method_decorator(csrf_exempt, name="dispatch")
class SemiAutoStopView(APIView):
    """POST /api/jobs/<id>/semiauto/stop/ — 写入停止信号，触发 JSON 落盘与后处理链。"""

    def post(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk, source_type="semiauto").first()
        if not job:
            return Response({"detail": "任务不存在或不是半自动任务"}, status=status.HTTP_404_NOT_FOUND)
        if job.status not in (JobStatus.RUNNING,):
            return Response({"detail": f"任务状态不可操作：{job.status}"}, status=status.HTTP_409_CONFLICT)

        run_dir = (job.run_dir or "").strip()
        if not run_dir:
            return Response({"detail": "run_dir 尚未就绪，请稍后重试"}, status=status.HTTP_409_CONFLICT)

        stop_file = Path(run_dir) / ".stop_requested"
        try:
            stop_file.touch()
        except Exception as e:
            logger.exception("semiauto_stop 写入信号文件失败 job=%s", pk)
            return Response({"detail": f"写入信号文件失败：{e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({"detail": "停止信号已发送，等待数据处理完成"})
