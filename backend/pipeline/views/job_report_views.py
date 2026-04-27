"""任务维度的竞品摘要、简报包、策略稿、Markdown 导出与报告资源文件。"""
from __future__ import annotations

import mimetypes
from typing import Any
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
from ..llm.generate_marketing_detail import generate_marketing_detail_pack
from ..models import JobStatus, PipelineJob
from ..reporting.brief_pack import build_brief_pack_zip_bytes
from ..reporting.brief_strategy_scope import (
    filter_brief_for_strategy_matrix_group,
    list_matrix_groups_for_api,
    resolve_strategy_matrix_group_index,
)
from ..reporting.marketing_pack_persist import persist_marketing_detail_pack_v1
from ..reporting.md_document_export import markdown_to_docx_bytes, markdown_to_pdf_bytes
from ..reporting.report_matrix_group_evidence import (
    load_report_matrix_group_evidence_markdown,
)
from ..reporting.report_strategy_excerpt import load_report_strategy_excerpt
from ..reporting.strategy_draft import build_strategy_draft_markdown
from ..strategy_decision_keys import build_strategy_decisions_dict
from ..serializers import (
    MarketingDetailPackRequestSerializer,
    PipelineJobSerializer,
    StrategyDraftRequestSerializer,
)
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
        if isinstance(data, dict):
            data = dict(data)
            data["matrix_groups"] = list_matrix_groups_for_api(data)
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
        strategy_decisions = build_strategy_decisions_dict(vd)
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

        matrix_groups = list_matrix_groups_for_api(brief)
        sg_idx = vd.get("strategy_matrix_group_index")
        sg_lbl = (vd.get("strategy_matrix_group") or "").strip()
        scope_idx, scope_err = resolve_strategy_matrix_group_index(
            brief,
            matrix_group_index=sg_idx,
            matrix_group_label=sg_lbl or None,
        )
        if scope_err:
            return Response({"detail": scope_err}, status=status.HTTP_400_BAD_REQUEST)
        strategy_scope_applied: dict[str, Any] | None = None
        if scope_idx is not None:
            brief = filter_brief_for_strategy_matrix_group(
                brief, matrix_group_index=scope_idx
            )
            raw_sa = brief.get("strategy_scope_applied")
            strategy_scope_applied = (
                raw_sa if isinstance(raw_sa, dict) else None
            )

        report_matrix_evidence_md = ""
        report_matrix_evidence_src = "none"
        if scope_idx is not None and 0 <= scope_idx < len(matrix_groups):
            gnm = (matrix_groups[scope_idx].get("group") or "").strip()
            if gnm:
                report_matrix_evidence_md, report_matrix_evidence_src = (
                    load_report_matrix_group_evidence_markdown(
                        job.run_dir,
                        gnm,
                    )
                )

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
                    report_matrix_group_evidence_md=report_matrix_evidence_md
                    or None,
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
            "matrix_groups": matrix_groups,
            "strategy_scope_applied": strategy_scope_applied,
            "report_matrix_group_evidence_source": report_matrix_evidence_src,
            "report_matrix_group_evidence_chars": len(report_matrix_evidence_md or ""),
        }
        return Response(body)


@method_decorator(csrf_exempt, name="dispatch")
class JobMarketingDetailPackView(APIView):
    """
    根据浏览器会话中的策略稿 Markdown，经「核心信息卡」再派生**营销内容**（多触点文案 JSON）。
    两步均走 ``call_llm``；事实约束见提示词。
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
                {"detail": "仅可对已成功且含 run_dir 的任务生成营销内容"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        ser = MarketingDetailPackRequestSerializer(data=request.data or {})
        ser.is_valid(raise_exception=True)
        vd = ser.validated_data
        md = (vd.get("strategy_markdown") or "").strip()
        notes = (vd.get("business_notes") or "").strip()
        raw_sd = vd.get("strategy_decisions")
        strategy_decisions = raw_sd if isinstance(raw_sd, dict) else {}
        gen_at = timezone.now().isoformat()
        try:
            inner = generate_marketing_detail_pack(
                keyword=job.keyword,
                strategy_markdown=md,
                strategy_decisions=strategy_decisions,
                business_notes=notes,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_502_BAD_GATEWAY)
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
            "source": "llm_marketing_detail_pack_v1",
            **inner,
        }
        try:
            persist_marketing_detail_pack_v1(job.run_dir, body)
        except OSError:
            pass
        return Response(body)


@method_decorator(csrf_exempt, name="dispatch")
class JobExportDocumentView(APIView):
    """
    将 Markdown 导出为 Word（.docx）或简易 PDF。
    - GET：``kind=report``，读取 ``run_dir/competitor_analysis.md``；若文件缺失但已有合并表，
      则先按任务配置调用 ``regenerate_competitor_report`` 再导出（与「报告生成」规则版一致）。
    - POST：``kind=strategy``，请求体 ``markdown`` 为策略稿正文；``kind=marketing_detail`` 为营销内容等派生稿（同一套转版逻辑，下载文件名不同）。
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
        if kind not in ("strategy", "marketing_detail"):
            return Response(
                {"detail": "POST 的 kind 须为 strategy 或 marketing_detail"},
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
            if kind == "marketing_detail":
                base = f"job_{pk}_marketing_detail_pack"
            else:
                base = f"job_{pk}_strategy_draft"
            if fmt == "docx":
                data = markdown_to_docx_bytes(md)
                ct = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                fn = f"{base}.docx"
            else:
                data = markdown_to_pdf_bytes(md)
                ct = "application/pdf"
                fn = f"{base}.pdf"
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
