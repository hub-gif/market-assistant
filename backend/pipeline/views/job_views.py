"""任务生命周期：列表/详情、取消/续跑、下载与预览、报告默认配置、重新生成报告。"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

import requests
from django.conf import settings
from django.http import FileResponse, Http404, HttpResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..ingest import resolve_and_validate_run_dir
from ..jd.runner import (
    build_competitor_brief_for_job,
    get_default_report_config,
    merge_llm_supplement_with_rules_report,
    regenerate_competitor_report,
    write_competitor_analysis_markdown,
)
from ..llm.generate import generate_competitor_report_markdown_llm
from ..models import JobStatus, PipelineJob
from ..serializers import (
    CreatePipelineJobSerializer,
    JobReportConfigPatchSerializer,
    JobResumeRequestSerializer,
    PipelineJobSerializer,
    RegenerateReportRequestSerializer,
)
from ..tasks import execute_job
from .common import PREVIEW_MAX_BYTES, job_run_dir_usable, safe_file_for_job

logger = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name="dispatch")
class JobListCreateView(APIView):
    def get(self, request):
        qs = (
            PipelineJob.objects.select_related("checkpoint_row")
            .all()
            .order_by("-created_at")[:200]
        )
        return Response(PipelineJobSerializer(qs, many=True).data)

    def post(self, request):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        ser = CreatePipelineJobSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        raw_rc = data.get("report_config")
        if not isinstance(raw_rc, dict) or raw_rc == {}:
            report_config_initial = get_default_report_config()
        else:
            report_config_initial = raw_rc
        job = PipelineJob.objects.create(
            platform=data["platform"],
            keyword=data["keyword"],
            max_skus=data.get("max_skus"),
            page_start=data.get("page_start"),
            page_to=data.get("page_to"),
            pipeline_run_dir=data.get("pipeline_run_dir") or "",
            cookie_file_path=data.get("cookie_file_path") or "",
            cookie_text=data.get("cookie_text") or "",
            pvid=data.get("pvid") or "",
            request_delay=data.get("request_delay") or "",
            list_pages=data.get("list_pages") or "",
            scenario_filter_enabled=data.get("scenario_filter_enabled"),
            report_config=report_config_initial,
            status=JobStatus.PENDING,
        )
        t = threading.Thread(target=execute_job, args=(job.id,), daemon=True)
        t.start()
        return Response(
            PipelineJobSerializer(job).data,
            status=status.HTTP_201_CREATED,
        )


@method_decorator(csrf_exempt, name="dispatch")
class JobDetailView(APIView):
    def get(self, request, pk: int):
        job = (
            PipelineJob.objects.filter(pk=pk)
            .select_related("checkpoint_row")
            .first()
        )
        if not job:
            raise Http404()
        return Response(PipelineJobSerializer(job).data)

    def patch(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        body = request.data if isinstance(request.data, dict) else {}
        update_fields: list[str] = []
        if "report_config" in body:
            ser = JobReportConfigPatchSerializer(data={"report_config": body["report_config"]})
            ser.is_valid(raise_exception=True)
            job.report_config = ser.validated_data["report_config"]
            update_fields.append("report_config")
        if "run_dir" in body:
            try:
                job.run_dir = str(
                    resolve_and_validate_run_dir(str(body.get("run_dir") or ""))
                )
            except ValueError as e:
                return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
            update_fields.append("run_dir")
        if not update_fields:
            return Response(
                {"detail": "请提供 report_config 或 run_dir（用于绑定已有批次目录）"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        job.save(update_fields=update_fields + ["updated_at"])
        return Response(PipelineJobSerializer(job).data)


@method_decorator(csrf_exempt, name="dispatch")
class JobCancelView(APIView):
    """
    终止：将 ``cancellation_requested`` 置位后，执行线程会尽快 ``terminate`` 采集子进程
    （效果接近在终端对脚本按 Ctrl+C），并保留已写入运行目录的文件。
    """

    def post(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if job.status not in (
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.PAUSED,
        ):
            return Response(
                {"detail": "仅待执行、执行中或已暂停的任务可终止"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        job.cancellation_requested = True
        job.save(update_fields=["cancellation_requested", "updated_at"])
        return Response(PipelineJobSerializer(job).data)


@method_decorator(csrf_exempt, name="dispatch")
class JobResumeView(APIView):
    """
    从 Cookie 暂停断点继续：可选请求体 ``{ "cookie_text": "..." }`` 更新 Cookie；
    置位 ``resume_from_checkpoint`` 并拉起与新建任务相同的采集子进程（环境变量 ``PIPELINE_RESUME=1``）。
    """

    def post(self, request, pk: int):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if job.status != JobStatus.PAUSED:
            return Response(
                {"detail": "仅「已暂停（待换 Cookie 续跑）」的任务可继续执行"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        ser = JobResumeRequestSerializer(data=request.data or {})
        ser.is_valid(raise_exception=True)
        raw_cookie = ser.validated_data.get("cookie_text") or ""
        from ..cookie_paste import normalize_browser_cookie_paste

        norm = normalize_browser_cookie_paste(raw_cookie)
        update_fields = ["resume_from_checkpoint", "error_message", "updated_at"]
        job.resume_from_checkpoint = True
        job.error_message = ""
        if norm:
            job.cookie_text = norm
            update_fields.insert(0, "cookie_text")
        job.save(update_fields=update_fields)
        t = threading.Thread(target=execute_job, args=(job.id,), daemon=True)
        t.start()
        job = (
            PipelineJob.objects.filter(pk=pk)
            .select_related("checkpoint_row")
            .first()
        )
        return Response(PipelineJobSerializer(job).data, status=status.HTTP_200_OK)


class ReportConfigDefaultsView(APIView):
    """返回 ``jd_competitor_report`` 中与脚本常量一致的默认报告调参 JSON。"""

    def get(self, request):
        try:
            return Response(get_default_report_config())
        except FileNotFoundError as e:
            return Response(
                {"detail": str(e)},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )


class JobDownloadView(APIView):
    def get(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job or not job_run_dir_usable(job):
            raise Http404()
        name = (request.query_params.get("name") or "").strip().lower()
        path = safe_file_for_job(job.run_dir, name)
        return FileResponse(
            path.open("rb"),
            as_attachment=True,
            filename=path.name,
        )


class JobPreviewView(APIView):
    """浏览器内联查看产出（CSV / Markdown 文本），大文件截断。"""

    def get(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job or not job_run_dir_usable(job):
            raise Http404()
        name = (request.query_params.get("name") or "").strip().lower()
        fpath = safe_file_for_job(job.run_dir, name)
        raw = fpath.read_bytes()
        truncated = len(raw) > PREVIEW_MAX_BYTES
        if truncated:
            raw = raw[:PREVIEW_MAX_BYTES]
        text = raw.decode("utf-8-sig", errors="replace")
        if truncated:
            text += "\n\n... [内容已截断，完整文件请使用下载]\n"

        if name == "report":
            ctype = "text/markdown; charset=utf-8"
        else:
            ctype = "text/csv; charset=utf-8"
        resp = HttpResponse(text, content_type=ctype)
        resp["X-Preview-Truncated"] = "1" if truncated else "0"
        resp["X-Preview-Filename"] = fpath.name
        return resp


@method_decorator(csrf_exempt, name="dispatch")
class JobRegenerateReportView(APIView):
    """基于任务已有 ``run_dir`` 内 CSV 重新生成 ``competitor_analysis.md``（不重新爬取）。"""

    def post(self, request, pk: int):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if job.status != JobStatus.SUCCESS or not (job.run_dir or "").strip():
            return Response(
                {"detail": "仅可对已成功且已写入 run_dir 的任务重新生成报告"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        ser = RegenerateReportRequestSerializer(data=request.data or {})
        ser.is_valid(raise_exception=True)
        generator = ser.validated_data.get("generator") or "rules"
        rc = job.report_config if isinstance(job.report_config, dict) else None
        try:
            regenerate_competitor_report(job.run_dir, job.keyword, report_config=rc)
        except FileNotFoundError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        if generator == "llm":
            try:
                rules_md = (
                    Path(job.run_dir) / "competitor_analysis.md"
                ).read_text(encoding="utf-8")
                brief = build_competitor_brief_for_job(
                    job.run_dir, job.keyword, report_config=rc
                )
                md = generate_competitor_report_markdown_llm(brief, job.keyword)
                md = merge_llm_supplement_with_rules_report(md, rules_md)
                write_competitor_analysis_markdown(job.run_dir, md)
            except FileNotFoundError as e:
                return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
            except ValueError as e:
                msg = str(e)
                logger.warning(
                    "regenerate-report LLM ValueError job_id=%s: %s", pk, msg
                )
                if "run_dir 不在京东数据目录下" in msg:
                    return Response(
                        {"detail": msg},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                if "请设置环境变量" in msg:
                    return Response(
                        {"detail": msg + "（运行 Django 的终端需能读取到该环境变量）"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                if "提示词过长" in msg or "上下文上限" in msg:
                    return Response(
                        {"detail": msg},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                return Response(
                    {"detail": msg},
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            except requests.RequestException as e:
                return Response(
                    {"detail": f"大模型网关错误：{e}"},
                    status=status.HTTP_502_BAD_GATEWAY,
                )
        return Response(PipelineJobSerializer(job).data)
