from django.contrib import admin

from .models import (
    JdJobCommentRow,
    JdJobDetailRow,
    JdJobMergedRow,
    JdJobSearchRow,
    JdProduct,
    JdProductSnapshot,
    PipelineJob,
)


@admin.register(PipelineJob)
class PipelineJobAdmin(admin.ModelAdmin):
    list_display = ("id", "platform", "keyword", "status", "created_at")
    list_filter = ("status", "platform")
    search_fields = ("keyword", "run_dir")


@admin.register(JdProduct)
class JdProductAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "platform",
        "sku_id",
        "title",
        "detail_brand",
        "last_captured_at",
        "last_job",
    )
    list_filter = ("platform",)
    search_fields = ("sku_id", "title", "detail_brand", "ware_id")
    raw_id_fields = ("last_job",)


@admin.register(JdProductSnapshot)
class JdProductSnapshotAdmin(admin.ModelAdmin):
    list_display = ("id", "product", "job", "captured_at", "run_dir")
    list_filter = ("captured_at",)
    search_fields = ("run_dir", "product__sku_id")
    raw_id_fields = ("product", "job")


@admin.register(JdJobSearchRow)
class JdJobSearchRowAdmin(admin.ModelAdmin):
    list_display = ("id", "job", "row_index", "sku_id")
    list_filter = ("job",)
    search_fields = ("sku_id",)
    raw_id_fields = ("job",)


@admin.register(JdJobDetailRow)
class JdJobDetailRowAdmin(admin.ModelAdmin):
    list_display = ("id", "job", "row_index", "sku_id")
    list_filter = ("job",)
    search_fields = ("sku_id",)
    raw_id_fields = ("job",)


@admin.register(JdJobCommentRow)
class JdJobCommentRowAdmin(admin.ModelAdmin):
    list_display = ("id", "job", "row_index", "sku_id")
    list_filter = ("job",)
    search_fields = ("sku_id",)
    raw_id_fields = ("job",)


@admin.register(JdJobMergedRow)
class JdJobMergedRowAdmin(admin.ModelAdmin):
    list_display = ("id", "job", "row_index", "sku_id")
    list_filter = ("job",)
    search_fields = ("sku_id", "pipeline_keyword")
    raw_id_fields = ("job",)
