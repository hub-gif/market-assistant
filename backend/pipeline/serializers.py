import json
from pathlib import Path

from django.conf import settings
from rest_framework import serializers

from .cookie_paste import normalize_browser_cookie_paste
from .models import (
    JdProduct,
    JdProductSnapshot,
    JobStatus,
    PipelineJob,
    PipelineJobCheckpoint,
)

# 与 views._safe_file_for_job 中 mapping 一致，供前端展示「数据源是否就绪」
_REPORT_CONFIG_ALLOWED_KEYS = frozenset(
    {
        "llm_comment_sentiment",
        "llm_matrix_group_summaries",
        "llm_price_group_summaries",
        "llm_comment_group_summaries",
        "comment_focus_words",
        "comment_scenario_groups",
        "external_market_table_rows",
    }
)


def validate_report_config_body(value: dict) -> dict:
    if not isinstance(value, dict):
        raise serializers.ValidationError("须为 JSON 对象")
    value = dict(value)
    value.pop("llm_section_bridges", None)
    extra = set(value.keys()) - _REPORT_CONFIG_ALLOWED_KEYS
    if extra:
        raise serializers.ValidationError(
            f"未知字段：{', '.join(sorted(extra))}"
        )
    if "llm_comment_sentiment" in value and value["llm_comment_sentiment"] is not None:
        if not isinstance(value["llm_comment_sentiment"], bool):
            raise serializers.ValidationError("llm_comment_sentiment 须为 true 或 false")
    for k in (
        "llm_matrix_group_summaries",
        "llm_price_group_summaries",
        "llm_comment_group_summaries",
    ):
        if k in value and value[k] is not None and not isinstance(value[k], bool):
            raise serializers.ValidationError(f"{k} 须为 true 或 false")
    raw = json.dumps(value, ensure_ascii=False)
    if len(raw) > 120_000:
        raise serializers.ValidationError("报告配置体积过大")
    return value


_ARTIFACT_FILES: tuple[tuple[str, str], ...] = (
    ("merged", "keyword_pipeline_merged.csv"),
    ("pc_search", "pc_search_export.csv"),
    ("comments", "comments_flat.csv"),
    ("detail_ware", "detail_ware_export.csv"),
    ("report", "competitor_analysis.md"),
)


class PipelineJobSerializer(serializers.ModelSerializer):
    """列表/详情不返回 cookie 正文。"""

    inline_cookie_used = serializers.SerializerMethodField()
    analysis_artifacts = serializers.SerializerMethodField()
    checkpoint = serializers.SerializerMethodField()

    class Meta:
        model = PipelineJob
        fields = [
            "id",
            "platform",
            "keyword",
            "max_skus",
            "page_start",
            "page_to",
            "pipeline_run_dir",
            "cookie_file_path",
            "inline_cookie_used",
            "pvid",
            "request_delay",
            "list_pages",
            "scenario_filter_enabled",
            "report_config",
            "status",
            "cancellation_requested",
            "resume_from_checkpoint",
            "checkpoint",
            "run_dir",
            "error_message",
            "analysis_artifacts",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "inline_cookie_used",
            "analysis_artifacts",
            "checkpoint",
            "status",
            "cancellation_requested",
            "resume_from_checkpoint",
            "run_dir",
            "error_message",
            "created_at",
            "updated_at",
            "report_config",
        ]

    def get_inline_cookie_used(self, obj: PipelineJob) -> bool:
        return bool((obj.cookie_text or "").strip())

    def get_checkpoint(self, obj: PipelineJob) -> dict | None:
        try:
            c = obj.checkpoint_row
        except PipelineJobCheckpoint.DoesNotExist:
            return None
        return {
            "phase": c.phase,
            "payload": c.payload,
            "hint_zh": c.hint_zh,
            "updated_at": c.updated_at,
        }

    def get_analysis_artifacts(self, obj: PipelineJob) -> dict[str, bool] | None:
        if obj.status not in (
            JobStatus.SUCCESS,
            JobStatus.CANCELLED,
            JobStatus.PAUSED,
        ) or not (obj.run_dir or "").strip():
            return None
        try:
            base = Path(obj.run_dir).expanduser().resolve()
            return { key: (base / name).is_file() for key, name in _ARTIFACT_FILES }
        except (OSError, ValueError, RuntimeError):
            return None


class JdProductListSerializer(serializers.ModelSerializer):
    """列表：不含整包 payload，减少流量。"""

    snapshot_count = serializers.IntegerField(read_only=True, required=False)

    class Meta:
        model = JdProduct
        fields = [
            "id",
            "platform",
            "sku_id",
            "ware_id",
            "title",
            "detail_brand",
            "detail_price_final",
            "last_captured_at",
            "last_job",
            "snapshot_count",
        ]


class JdProductDetailSerializer(serializers.ModelSerializer):
    snapshot_count = serializers.IntegerField(read_only=True, required=False)

    class Meta:
        model = JdProduct
        fields = [
            "id",
            "platform",
            "sku_id",
            "ware_id",
            "title",
            "detail_brand",
            "detail_price_final",
            "detail_category_path",
            "current_payload",
            "last_job",
            "last_captured_at",
            "snapshot_count",
            "created_at",
            "updated_at",
        ]


class JdProductSnapshotBriefSerializer(serializers.ModelSerializer):
    job_keyword = serializers.CharField(source="job.keyword", read_only=True)

    class Meta:
        model = JdProductSnapshot
        fields = ["id", "job", "job_keyword", "run_dir", "captured_at"]


class JdProductSnapshotDetailSerializer(serializers.ModelSerializer):
    job_keyword = serializers.CharField(source="job.keyword", read_only=True)
    sku_id = serializers.CharField(source="product.sku_id", read_only=True)
    platform = serializers.CharField(source="product.platform", read_only=True)

    class Meta:
        model = JdProductSnapshot
        fields = [
            "id",
            "platform",
            "sku_id",
            "job",
            "job_keyword",
            "run_dir",
            "captured_at",
            "payload",
        ]


def _jd_data_root() -> Path:
    root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not root:
        raise serializers.ValidationError("服务器未配置 LOW_GI_PROJECT_ROOT")
    return (Path(root) / "data" / "JD").resolve()


class JobResumeRequestSerializer(serializers.Serializer):
    """从断点续跑时可选更新 Cookie。"""

    cookie_text = serializers.CharField(
        required=False,
        allow_blank=True,
        default="",
        max_length=500_000,
    )


class CreatePipelineJobSerializer(serializers.Serializer):
    keyword = serializers.CharField(max_length=256, trim_whitespace=True)
    platform = serializers.ChoiceField(choices=["jd"], default="jd")
    max_skus = serializers.IntegerField(required=False, min_value=1, allow_null=True)
    page_start = serializers.IntegerField(required=False, min_value=1, allow_null=True)
    page_to = serializers.IntegerField(required=False, min_value=1, allow_null=True)
    pipeline_run_dir = serializers.CharField(
        required=False, allow_blank=True, max_length=1024, default=""
    )
    cookie_file_path = serializers.CharField(
        required=False, allow_blank=True, max_length=2048, default=""
    )
    cookie_text = serializers.CharField(
        required=False,
        allow_blank=True,
        default="",
        max_length=500_000,
    )
    pvid = serializers.CharField(required=False, allow_blank=True, max_length=128, default="")
    request_delay = serializers.CharField(
        required=False, allow_blank=True, max_length=64, default=""
    )
    list_pages = serializers.CharField(
        required=False, allow_blank=True, max_length=64, default=""
    )
    scenario_filter_enabled = serializers.BooleanField(required=False, allow_null=True)
    report_config = serializers.JSONField(required=False, default=dict)

    def validate_report_config(self, value):
        if not value:
            return {}
        return validate_report_config_body(value)

    def validate_cookie_text(self, value: str) -> str:
        return normalize_browser_cookie_paste(value or "")

    def validate_pipeline_run_dir(self, value: str) -> str:
        v = (value or "").strip()
        if not v:
            return ""
        p = Path(v).expanduser()
        jd_root = _jd_data_root()
        if p.is_absolute():
            try:
                p.resolve().relative_to(jd_root)
            except ValueError:
                raise serializers.ValidationError(
                    f"绝对路径须位于京东数据目录下：{jd_root}"
                )
        else:
            bad = ("..",)
            if any(part in bad for part in p.parts):
                raise serializers.ValidationError("路径不能包含 ..")
        return v

    def validate_cookie_file_path(self, value: str) -> str:
        v = (value or "").strip()
        if not v:
            return ""
        p = Path(v).expanduser().resolve()
        if not p.is_file():
            raise serializers.ValidationError(f"Cookie 文件不存在：{p}")
        low = Path(settings.LOW_GI_PROJECT_ROOT).resolve()
        try:
            p.relative_to(low)
        except ValueError:
            raise serializers.ValidationError(
                "Cookie 文件路径须位于 LOW_GI_PROJECT_ROOT 目录之下"
            )
        return str(p)


class JobReportConfigPatchSerializer(serializers.Serializer):
    report_config = serializers.JSONField()

    def validate_report_config(self, value):
        if not isinstance(value, dict):
            raise serializers.ValidationError("须为 JSON 对象")
        return validate_report_config_body(value)


class RegenerateReportRequestSerializer(serializers.Serializer):
    """重新生成竞品报告：规则引擎或大模型（与 ``AI_crawler.chat_completion_text`` 同一网关）。"""

    generator = serializers.ChoiceField(
        choices=["rules", "llm"],
        default="rules",
        required=False,
    )


class StrategyDraftRequestSerializer(serializers.Serializer):
    """市场策略制定：业务备注 + 可选「决策填空/勾选」，与 competitor-brief 合并为策略向 Markdown。"""

    business_notes = serializers.CharField(
        required=False,
        allow_blank=True,
        default="",
        max_length=20_000,
        trim_whitespace=False,
    )
    product_role = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=500, trim_whitespace=False
    )
    time_horizon = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=200, trim_whitespace=False
    )
    success_criteria = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=2000, trim_whitespace=False
    )
    non_goals = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=1000, trim_whitespace=False
    )
    battlefield_one_line = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=1000, trim_whitespace=False
    )
    positioning_choice = serializers.ChoiceField(
        choices=["", "top", "mid", "entry", "different"],
        default="",
        required=False,
    )
    competitive_stance = serializers.ChoiceField(
        choices=["", "flank", "head_on", "both", "undecided"],
        default="",
        required=False,
    )
    pillar_product = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=800, trim_whitespace=False
    )
    pillar_price = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=800, trim_whitespace=False
    )
    pillar_channel = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=800, trim_whitespace=False
    )
    pillar_comm = serializers.CharField(
        required=False, allow_blank=True, default="", max_length=800, trim_whitespace=False
    )
    ack_risk_keywords = serializers.BooleanField(required=False, default=False)
    ack_risk_price = serializers.BooleanField(required=False, default=False)
    ack_risk_concentration = serializers.BooleanField(required=False, default=False)
    generator = serializers.ChoiceField(
        choices=["rules", "llm"],
        default="rules",
        required=False,
    )
