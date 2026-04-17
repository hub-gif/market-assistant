"""任务维度的竞品摘要、简报包、策略稿、Markdown 导出与报告资源文件。"""
from __future__ import annotations

import mimetypes
from pathlib import Path

import requests
from django.conf import settings
from django.http import FileResponse, Http404, HttpResponse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from ..jd.runner import (
    build_competitor_brief_for_job,
    regenerate_competitor_report,
)
from ..llm.generate import generate_strategy_draft_markdown_llm
from ..models import JobStatus, PipelineJob
from ..reporting.brief_pack import build_brief_pack_zip_bytes
from ..reporting.md_document_export import markdown_to_docx_bytes, markdown_to_pdf_bytes
from ..reporting.report_strategy_excerpt import load_report_strategy_excerpt
from ..reporting.strategy_draft import build_strategy_draft_markdown
from ..serializers import PipelineJobSerializer, StrategyDraftRequestSerializer
from .common import job_run_dir_usable


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
        excerpt_src = "none"
        report_excerpt = ""
        try:
            report_excerpt, excerpt_src = load_report_strategy_excerpt(job.run_dir)
        except OSError:
            report_excerpt, excerpt_src = "", "none"
        rc_job = job.report_config if isinstance(job.report_config, dict) else None
        try:
            if generator == "llm":
                md = generate_strategy_draft_markdown_llm(
                    job_id=job.id,
                    keyword=job.keyword,
                    brief=brief,
                    business_notes=notes,
                    generated_at_iso=gen_at,
                    strategy_decisions=strategy_decisions,
                    report_strategy_excerpt=report_excerpt,
                    report_config=rc_job,
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
                    report_config=rc_job,
                )
                src = "structured_summary_rules_v1"
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except requests.RequestException as e:
            return Response(
                {"detail": f"大模型网关错误：{e}"},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        body: dict[str, object] = {
            "schema_version": 1,
            "job_id": job.id,
            "keyword": job.keyword,
            "generated_at": gen_at,
            "source": src,
            "markdown": md,
            "report_strategy_excerpt_source": excerpt_src,
            "report_strategy_excerpt_chars": len(report_excerpt or ""),
        }
        return Response(body)


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
        if not job_run_dir_usable(job):
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
        if not job_run_dir_usable(job):
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
        if not job or not job_run_dir_usable(job):
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
