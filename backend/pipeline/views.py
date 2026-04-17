from __future__ import annotations

import logging
import mimetypes
import threading
from pathlib import Path
from typing import Any

import requests
from django.conf import settings
from django.db.models import Count, Q
from django.http import FileResponse, Http404, HttpResponse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .dataset_api import (
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
from .dataset_nonempty import (
    comment_columns_for_api,
    detail_columns_for_api,
    merged_columns_for_api,
    search_columns_for_api,
)
from .export_job import build_csv_bytes, build_json_bytes, build_xlsx_bytes
from .row_serialize import (
    comment_row_to_dict,
    detail_row_to_dict,
    merged_row_to_dict,
    search_row_to_dict,
)
from .ingest import ingest_job_full, resolve_and_validate_run_dir
from .reporting.brief_pack import build_brief_pack_zip_bytes
from .reporting.strategy_draft import build_strategy_draft_markdown
from .jd.runner import (
    build_competitor_brief_for_job,
    get_default_report_config,
    merge_llm_supplement_with_rules_report,
    regenerate_competitor_report,
    write_competitor_analysis_markdown,
)
from .llm.generate import (
    generate_competitor_report_markdown_llm,
    generate_strategy_draft_markdown_llm,
)
from .reporting.md_document_export import markdown_to_docx_bytes, markdown_to_pdf_bytes
from .models import (
    JdJobCommentRow,
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
    JdProduct,
    JdProductSnapshot,
    JobStatus,
    PipelineJob,
)
from .serializers import (
    CreatePipelineJobSerializer,
    JdProductDetailSerializer,
    JdProductListSerializer,
    JdProductSnapshotBriefSerializer,
    JdProductSnapshotDetailSerializer,
    JobReportConfigPatchSerializer,
    JobResumeRequestSerializer,
    PipelineJobSerializer,
    RegenerateReportRequestSerializer,
    StrategyDraftRequestSerializer,
)
from .tasks import execute_job

logger = logging.getLogger(__name__)

# 在线预览最大字节（超出则截断并提示下载）
_PREVIEW_MAX_BYTES = 2 * 1024 * 1024

# 允许下载的相对文件名（均在 run_dir 下）
_DOWNLOAD_NAMES = frozenset(
    {
        "merged",
        "pc_search",
        "comments",
        "detail_ware",
        "report",
    }
)


def _jd_data_root() -> Path:
    root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    return (Path(root) / "data" / "JD").resolve()


def _safe_file_for_job(run_dir_str: str, name: str) -> Path:
    if name not in _DOWNLOAD_NAMES:
        raise Http404("unknown file")
    base = Path(run_dir_str).resolve()
    jd_root = _jd_data_root().resolve()
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


def _job_run_dir_usable(job: PipelineJob) -> bool:
    """成功、已终止或已暂停（断点产物）且已写入 run_dir 时，可预览/下载批次文件。"""
    return bool((job.run_dir or "").strip()) and job.status in (
        JobStatus.SUCCESS,
        JobStatus.CANCELLED,
        JobStatus.PAUSED,
    )


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
        from .cookie_paste import normalize_browser_cookie_paste

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
        if not job or not _job_run_dir_usable(job):
            raise Http404()
        name = (request.query_params.get("name") or "").strip().lower()
        path = _safe_file_for_job(job.run_dir, name)
        return FileResponse(
            path.open("rb"),
            as_attachment=True,
            filename=path.name,
        )


class JobPreviewView(APIView):
    """浏览器内联查看产出（CSV / Markdown 文本），大文件截断。"""

    def get(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job or not _job_run_dir_usable(job):
            raise Http404()
        name = (request.query_params.get("name") or "").strip().lower()
        fpath = _safe_file_for_job(job.run_dir, name)
        raw = fpath.read_bytes()
        truncated = len(raw) > _PREVIEW_MAX_BYTES
        if truncated:
            raw = raw[:_PREVIEW_MAX_BYTES]
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
        # 规则版报告先落盘；run_dir 校验、缺 CSV 等与是否走 LLM 无关，统一返回 400（勿误用 503）
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
                # AI_crawler：缺密钥/网关、提示词过长；jd_runner：run_dir 越界等
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


class JobCompetitorBriefView(APIView):
    """单次任务的结构化竞品摘要（JSON，与 ``competitor_analysis.md`` **同一套计数规则**，规则驱动无 LLM）。"""

    def get(self, request, pk: int):
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
                {"detail": "仅可对已成功且含 run_dir 的任务获取竞品摘要"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            data = build_competitor_brief_for_job(
                job.run_dir,
                job.keyword,
                report_config=job.report_config
                if isinstance(job.report_config, dict)
                else None,
            )
        except FileNotFoundError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(data)


class JobCompetitorBriefPackView(APIView):
    """ZIP：完整 Markdown 报告 + 结构化 JSON + 要点摘录 Markdown + 说明文本。"""

    def get(self, request, pk: int):
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
                {"detail": "仅可对已成功且含 run_dir 的任务导出简报包"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            brief = build_competitor_brief_for_job(
                job.run_dir,
                job.keyword,
                report_config=job.report_config
                if isinstance(job.report_config, dict)
                else None,
            )
            zip_bytes = build_brief_pack_zip_bytes(Path(job.run_dir), brief)
        except FileNotFoundError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        filename_ascii = f"job_{pk}_competitor_brief_pack.zip"
        resp = HttpResponse(zip_bytes, content_type="application/zip")
        resp["Content-Disposition"] = f'attachment; filename="{filename_ascii}"'
        return resp


@method_decorator(csrf_exempt, name="dispatch")
class JobStrategyDraftView(APIView):
    """
    市场策略制定 Markdown：策略框架 + 附录；默认规则生成，可选 ``generator=llm``（``AI_crawler.chat_completion_text``）。
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
        if job.status != JobStatus.SUCCESS or not (job.run_dir or "").strip():
            return Response(
                {"detail": "仅可对已成功且含 run_dir 的任务生成策略制定稿"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        ser = StrategyDraftRequestSerializer(data=request.data or {})
        ser.is_valid(raise_exception=True)
        vd = ser.validated_data
        notes = (vd.get("business_notes") or "").strip()
        strategy_decisions = {
            "product_role": vd.get("product_role") or "",
            "time_horizon": vd.get("time_horizon") or "",
            "success_criteria": vd.get("success_criteria") or "",
            "non_goals": vd.get("non_goals") or "",
            "battlefield_one_line": vd.get("battlefield_one_line") or "",
            "positioning_choice": vd.get("positioning_choice") or "",
            "competitive_stance": vd.get("competitive_stance") or "",
            "pillar_product": vd.get("pillar_product") or "",
            "pillar_price": vd.get("pillar_price") or "",
            "pillar_channel": vd.get("pillar_channel") or "",
            "pillar_comm": vd.get("pillar_comm") or "",
            "ack_risk_keywords": bool(vd.get("ack_risk_keywords")),
            "ack_risk_price": bool(vd.get("ack_risk_price")),
            "ack_risk_concentration": bool(vd.get("ack_risk_concentration")),
        }
        try:
            brief = build_competitor_brief_for_job(
                job.run_dir,
                job.keyword,
                report_config=job.report_config
                if isinstance(job.report_config, dict)
                else None,
            )
        except FileNotFoundError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        gen_at = timezone.now().isoformat()
        generator = (vd.get("generator") or "rules").strip()
        try:
            if generator == "llm":
                md = generate_strategy_draft_markdown_llm(
                    job_id=job.id,
                    keyword=job.keyword,
                    brief=brief,
                    business_notes=notes,
                    generated_at_iso=gen_at,
                    strategy_decisions=strategy_decisions,
                )
                src = "llm_text_ai_crawler_v1"
            else:
                md = build_strategy_draft_markdown(
                    job_id=job.id,
                    keyword=job.keyword,
                    brief=brief,
                    business_notes=notes,
                    generated_at_iso=gen_at,
                    strategy_decisions=strategy_decisions,
                )
                src = "structured_summary_rules_v1"
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except requests.RequestException as e:
            return Response(
                {"detail": f"大模型网关错误：{e}"},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        return Response(
            {
                "schema_version": 1,
                "job_id": job.id,
                "keyword": job.keyword,
                "generated_at": gen_at,
                "source": src,
                "markdown": md,
            }
        )


@method_decorator(csrf_exempt, name="dispatch")
class JobExportDocumentView(APIView):
    """
    将 Markdown 导出为 Word（.docx）或简易 PDF。
    - GET：``kind=report``，读取 ``run_dir/competitor_analysis.md``；若文件缺失但已有合并表，
      则先按任务配置调用 ``regenerate_competitor_report`` 再导出（与「报告生成」规则版一致）。
    - POST：``kind=strategy``，请求体 JSON 字段 ``markdown`` 为策略稿正文（与前端 sessionStorage 一致）。
    PDF 依赖本机中文字体或环境变量 ``MA_PDF_FONT`` 指向 .ttf。
    """

    def get(self, request, pk: int):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if not _job_run_dir_usable(job):
            return Response(
                {"detail": "仅可对已成功或已终止且含 run_dir 的任务导出"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        fmt = (request.query_params.get("fmt") or "docx").strip().lower()
        kind = (request.query_params.get("kind") or "report").strip().lower()
        if kind != "report":
            return Response(
                {"detail": "GET 仅支持 kind=report；策略稿请用 POST 提交 markdown"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if fmt not in ("docx", "pdf"):
            return Response(
                {"detail": "fmt 须为 docx 或 pdf"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        path = Path(job.run_dir) / "competitor_analysis.md"
        if not path.is_file():
            rc = job.report_config if isinstance(job.report_config, dict) else None
            try:
                regenerate_competitor_report(job.run_dir, job.keyword, report_config=rc)
            except FileNotFoundError as e:
                return Response(
                    {"detail": str(e)},
                    status=status.HTTP_404_NOT_FOUND,
                )
            except ValueError as e:
                return Response(
                    {"detail": str(e)},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if not path.is_file():
                return Response(
                    {"detail": "报告文件不存在且未能从合并表生成，请先在「报告生成」重新生成"},
                    status=status.HTTP_404_NOT_FOUND,
                )
        md = path.read_text(encoding="utf-8")
        asset_root = Path(job.run_dir).resolve()
        try:
            if fmt == "docx":
                data = markdown_to_docx_bytes(md, asset_root=asset_root)
                ct = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                fn = f"job_{pk}_competitor_report.docx"
            else:
                data = markdown_to_pdf_bytes(md, asset_root=asset_root)
                ct = "application/pdf"
                fn = f"job_{pk}_competitor_report.pdf"
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        resp = HttpResponse(data, content_type=ct)
        resp["Content-Disposition"] = f'attachment; filename="{fn}"'
        return resp

    def post(self, request, pk: int):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            return Response(
                {"detail": "请先在 market_assistant/.env 中配置 LOW_GI_PROJECT_ROOT"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job:
            raise Http404()
        if not _job_run_dir_usable(job):
            return Response(
                {"detail": "仅可对已成功或已终止且含 run_dir 的任务导出"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        body = request.data if isinstance(request.data, dict) else {}
        kind = (body.get("kind") or "strategy").strip().lower()
        fmt = (body.get("fmt") or "docx").strip().lower()
        md = (body.get("markdown") or "").strip()
        if kind != "strategy":
            return Response(
                {"detail": "POST 仅支持 kind=strategy"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not md:
            return Response(
                {"detail": "markdown 不能为空"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if fmt not in ("docx", "pdf"):
            return Response(
                {"detail": "fmt 须为 docx 或 pdf"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            if fmt == "docx":
                data = markdown_to_docx_bytes(md)
                ct = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                fn = f"job_{pk}_strategy_draft.docx"
            else:
                data = markdown_to_pdf_bytes(md)
                ct = "application/pdf"
                fn = f"job_{pk}_strategy_draft.pdf"
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        resp = HttpResponse(data, content_type=ct)
        resp["Content-Disposition"] = f'attachment; filename="{fn}"'
        return resp


class JobReportAssetView(APIView):
    """安全读取 ``run_dir/report_assets/*`` 下的 PNG 等（供 Markdown 预览插图）。"""

    def get(self, request, pk: int):
        job = PipelineJob.objects.filter(pk=pk).first()
        if not job or not _job_run_dir_usable(job):
            raise Http404()
        rel = (request.query_params.get("path") or "").strip().replace("\\", "/")
        if not rel or ".." in Path(rel).parts:
            return Response(
                {"detail": "path 非法"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        base = Path(job.run_dir).resolve()
        assets_root = (base / "report_assets").resolve()
        target = (base / rel).resolve()
        try:
            target.relative_to(assets_root)
        except ValueError:
            raise Http404()
        if not target.is_file():
            raise Http404()
        ctype, _ = mimetypes.guess_type(str(target))
        return FileResponse(
            target.open("rb"),
            content_type=ctype or "application/octet-stream",
        )


def _dataset_job(pk: int) -> PipelineJob:
    job = PipelineJob.objects.filter(pk=pk).first()
    if not job:
        raise Http404()
    return job


def _read_page_params(request) -> tuple[int, int]:
    page_size = min(max(int(request.query_params.get("page_size", 50)), 1), 200)
    page = max(int(request.query_params.get("page", 1)), 1)
    return page, page_size


def _report_group_options_for_job(job: PipelineJob) -> list[str]:
    """类目选项：与第五章矩阵一致，来自合并表商品详情页类目路径解析。"""
    qs = (
        JdJobMergedRow.objects.filter(job=job)
        .exclude(matrix_group_label="")
        .values_list("matrix_group_label", flat=True)
        .distinct()
    )
    return sorted({str(x) for x in qs if x})


def _detail_category_path_options(job: PipelineJob) -> list[str]:
    return list(
        JdJobDetailRow.objects.filter(job=job)
        .exclude(detail_category_path="")
        .values_list("detail_category_path", flat=True)
        .distinct()
        .order_by("detail_category_path")[:400]
    )


def _shop_options_for_job(job: PipelineJob) -> list[str]:
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


class JobDatasetSummaryView(APIView):
    """任务在库中的搜索/详情/评价行数（入库后可用）。"""

    def get(self, request, pk: int):
        job = _dataset_job(pk)
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
                "category_options": _report_group_options_for_job(job),
                "shop_options": _shop_options_for_job(job),
                "detail_category_path_options": _detail_category_path_options(job),
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
        job = _dataset_job(pk)
        page, page_size = _read_page_params(request)
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
        job = _dataset_job(pk)
        page, page_size = _read_page_params(request)
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
        job = _dataset_job(pk)
        page, page_size = _read_page_params(request)
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
        job = _dataset_job(pk)
        page, page_size = _read_page_params(request)
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
        job = _dataset_job(pk)
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
            from .csv_schema import MERGED_FIELD_TO_CSV_HEADER

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
