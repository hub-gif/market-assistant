from django.db import models


class JobStatus(models.TextChoices):
    PENDING = "pending", "待执行"
    RUNNING = "running", "执行中"
    SUCCESS = "success", "成功"
    FAILED = "failed", "失败"
    CANCELLED = "cancelled", "已终止"
    PAUSED = "paused", "已暂停（待换 Cookie 续跑）"


class PipelineJob(models.Model):
    platform = models.CharField(max_length=32, default="jd", db_index=True)
    keyword = models.CharField(max_length=256)
    max_skus = models.PositiveIntegerField(null=True, blank=True)
    page_start = models.PositiveIntegerField(null=True, blank=True)
    page_to = models.PositiveIntegerField(null=True, blank=True)
    # 相对 data/JD 的子路径，或绝对路径（须在 Low GI/data/JD 下）；空则时间戳_关键词
    pipeline_run_dir = models.TextField(blank=True, default="")
    # Cookie：二选一优先 cookie_text（任务内写入临时文件再跑流水线）
    cookie_file_path = models.TextField(blank=True, default="")
    cookie_text = models.TextField(blank=True, default="")
    pvid = models.CharField(max_length=128, blank=True, default="")
    request_delay = models.CharField(
        max_length=64,
        blank=True,
        default="",
        help_text='如 "30-60"；空则沿用副本默认',
    )
    list_pages = models.CharField(
        max_length=64,
        blank=True,
        default="",
        help_text='评论分页，如 "1-2"；空则沿用副本默认',
    )
    scenario_filter_enabled = models.BooleanField(null=True, blank=True)
    # 竞品报告 / competitor-brief：关注词、场景词组、外部市场表等（JSON，空对象=用爬虫脚本默认）
    report_config = models.JSONField(default=dict, blank=True)
    status = models.CharField(
        max_length=16,
        choices=JobStatus.choices,
        default=JobStatus.PENDING,
        db_index=True,
    )
    cancellation_requested = models.BooleanField(default=False, db_index=True)
    resume_from_checkpoint = models.BooleanField(default=False, db_index=True)
    run_dir = models.TextField(blank=True, default="")
    error_message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"[{self.platform}] {self.keyword} ({self.status})"


class PipelineJobCheckpoint(models.Model):
    """Cookie 暂停续跑等场景的断点元数据（与任务一对一）。"""

    job = models.OneToOneField(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="checkpoint_row",
    )
    phase = models.CharField(max_length=32, db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    hint_zh = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]

    def __str__(self) -> str:
        return f"checkpoint job={self.job_id} phase={self.phase}"


class JdProduct(models.Model):
    """京东 SKU 主档：同一 ``platform`` + ``sku_id`` 唯一，多次抓取时覆盖为最新一行合并表数据。"""

    platform = models.CharField(max_length=16, default="jd", db_index=True)
    sku_id = models.CharField(max_length=64, db_index=True)
    ware_id = models.CharField(max_length=64, blank=True, default="")
    title = models.TextField(blank=True, default="")
    detail_brand = models.CharField(max_length=512, blank=True, default="")
    detail_price_final = models.CharField(max_length=128, blank=True, default="")
    detail_category_path = models.TextField(blank=True, default="")
    current_payload = models.JSONField(default=dict, blank=True)
    last_job = models.ForeignKey(
        PipelineJob,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="touched_products",
    )
    last_captured_at = models.DateTimeField(null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["platform", "sku_id"],
                name="uniq_pipeline_jdproduct_platform_sku",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.platform}:{self.sku_id}"


class JdProductSnapshot(models.Model):
    """某次流水线任务下该 SKU 的整行快照，用于历史对比与回放。"""

    product = models.ForeignKey(
        JdProduct,
        on_delete=models.CASCADE,
        related_name="snapshots",
    )
    job = models.ForeignKey(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="product_snapshots",
    )
    run_dir = models.TextField(blank=True, default="")
    captured_at = models.DateTimeField(db_index=True)
    payload = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-captured_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["product", "job"],
                name="uniq_pipeline_jdproductsnapshot_product_job",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.product_id} @ job {self.job_id}"


class JdLeafCategoryNorm(models.Model):
    """叶类目归一：与导出 ``leaf_category`` 原文一致（去空格），用于任务内类目筛选索引。"""

    label = models.CharField(max_length=512, unique=True, db_index=True)

    class Meta:
        ordering = ["label"]

    def __str__(self) -> str:
        return self.label[:80]


class JdJobSearchRow(models.Model):
    """单次任务下 PC 搜索导出表一行，字段与 ``pc_search_export.csv`` 列一一对应（内部英文属性名）。"""

    job = models.ForeignKey(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="job_search_rows",
    )
    row_index = models.PositiveIntegerField()
    item_id = models.TextField(blank=True, default="")
    sku_id = models.TextField(blank=True, default="", db_index=True)
    title = models.TextField(blank=True, default="")
    price = models.TextField(blank=True, default="")
    coupon_price = models.TextField(blank=True, default="")
    original_price = models.TextField(blank=True, default="")
    selling_point = models.TextField(blank=True, default="")
    comment_sales_floor = models.TextField(blank=True, default="")
    total_sales = models.TextField(blank=True, default="")
    hot_list_rank = models.TextField(blank=True, default="")
    comment_count = models.TextField(blank=True, default="")
    shop_name = models.TextField(blank=True, default="")
    shop_url = models.TextField(blank=True, default="")
    shop_info_url = models.TextField(blank=True, default="")
    location = models.TextField(blank=True, default="")
    detail_url = models.TextField(blank=True, default="")
    image = models.TextField(blank=True, default="")
    seckill_info = models.TextField(blank=True, default="")
    attributes = models.TextField(blank=True, default="")
    leaf_category = models.TextField(blank=True, default="")
    leaf_category_norm = models.ForeignKey(
        JdLeafCategoryNorm,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="search_rows",
    )
    matrix_group_label = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="报告细类",
        help_text="与 §5 矩阵同源：由合并表商详路径解析；可按 SKU 从合并表回填",
    )
    price_value = models.FloatField(null=True, blank=True, db_index=True)
    platform = models.TextField(blank=True, default="")
    keyword = models.TextField(blank=True, default="")
    page = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["row_index"]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "row_index"],
                name="uniq_pipeline_jdjobsearchrow_job_idx",
            ),
        ]
        indexes = [
            models.Index(fields=["job", "sku_id"]),
            models.Index(fields=["job", "leaf_category_norm"]),
            models.Index(fields=["job", "matrix_group_label"]),
            models.Index(fields=["job", "price_value"]),
        ]

    def __str__(self) -> str:
        return f"job{self.job_id} search#{self.row_index}"


class JdJobDetailRow(models.Model):
    """单次任务下 ``detail_ware_export.csv`` 一行（lean：skuId + 与合并表一致的商详子集）。"""

    job = models.ForeignKey(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="job_detail_rows",
    )
    row_index = models.PositiveIntegerField()
    sku_id = models.TextField(blank=True, default="", db_index=True)
    detail_brand = models.TextField(blank=True, default="")
    detail_price_final = models.TextField(blank=True, default="")
    detail_shop_name = models.TextField(blank=True, default="")
    detail_category_path = models.TextField(blank=True, default="")
    detail_product_attributes = models.TextField(blank=True, default="")
    detail_body_ingredients = models.TextField(blank=True, default="")
    buyer_ranking_line = models.TextField(blank=True, default="")
    buyer_promo_text = models.TextField(blank=True, default="")
    detail_price_value = models.FloatField(null=True, blank=True, db_index=True)
    matrix_group_label = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="报告细类",
        help_text="与 §5 矩阵同源：由 detail_category_path 解析",
    )

    class Meta:
        ordering = ["row_index"]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "row_index"],
                name="uniq_pipeline_jdjobdetailrow_job_idx",
            ),
        ]
        indexes = [
            models.Index(fields=["job", "sku_id"]),
            models.Index(fields=["job", "detail_price_value"]),
            models.Index(fields=["job", "matrix_group_label"]),
        ]

    def __str__(self) -> str:
        return f"job{self.job_id} detail#{self.row_index}"


class JdJobCommentRow(models.Model):
    """单次任务下 ``comments_flat.csv`` 一行。"""

    job = models.ForeignKey(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="job_comment_rows",
    )
    row_index = models.PositiveIntegerField()
    sku_id = models.TextField(blank=True, default="", db_index=True)
    comment_id = models.TextField(blank=True, default="")
    user_nick_name = models.TextField(blank=True, default="")
    tag_comment_content = models.TextField(blank=True, default="")
    comment_date = models.TextField(blank=True, default="")
    buy_count_text = models.TextField(blank=True, default="")
    large_pic_urls = models.TextField(blank=True, default="")
    comment_score = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["row_index"]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "row_index"],
                name="uniq_pipeline_jdjobcommentrow_job_idx",
            ),
        ]
        indexes = [
            models.Index(fields=["job", "sku_id"]),
        ]

    def __str__(self) -> str:
        return f"job{self.job_id} cmt#{self.row_index}"


class JdJobMergedRow(models.Model):
    """单次任务下合并宽表一行（lean：搜索列 + 商详子集 + 评论摘要），与 ``keyword_pipeline_merged.csv`` 列一一对应。"""

    job = models.ForeignKey(
        PipelineJob,
        on_delete=models.CASCADE,
        related_name="job_merged_rows",
    )
    row_index = models.PositiveIntegerField()
    pipeline_keyword = models.TextField(blank=True, default="")
    sku_id = models.TextField(blank=True, default="", db_index=True)
    ware_id = models.TextField(blank=True, default="")
    title = models.TextField(blank=True, default="")
    price = models.TextField(blank=True, default="")
    coupon_price = models.TextField(blank=True, default="")
    original_price = models.TextField(blank=True, default="")
    selling_point = models.TextField(blank=True, default="")
    hot_list_rank = models.TextField(blank=True, default="")
    comment_fuzzy = models.TextField(blank=True, default="")
    comment_sales_floor = models.TextField(blank=True, default="")
    total_sales = models.TextField(blank=True, default="")
    shop_name = models.TextField(blank=True, default="")
    detail_url = models.TextField(blank=True, default="")
    image = models.TextField(blank=True, default="")
    attributes = models.TextField(blank=True, default="")
    leaf_category = models.TextField(blank=True, default="")
    leaf_category_norm = models.ForeignKey(
        JdLeafCategoryNorm,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="merged_rows",
    )
    matrix_group_label = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="报告细类",
        help_text="与 §5 矩阵同源：由 detail_category_path 解析",
    )
    price_value = models.FloatField(null=True, blank=True, db_index=True)
    keyword = models.TextField(blank=True, default="")
    page = models.TextField(blank=True, default="")
    detail_brand = models.TextField(blank=True, default="")
    detail_price_final = models.TextField(blank=True, default="")
    detail_shop_name = models.TextField(blank=True, default="")
    detail_category_path = models.TextField(blank=True, default="")
    detail_product_attributes = models.TextField(blank=True, default="")
    detail_body_ingredients = models.TextField(blank=True, default="")
    buyer_ranking_line = models.TextField(blank=True, default="")
    buyer_promo_text = models.TextField(blank=True, default="")
    pipeline_comment_count = models.TextField(blank=True, default="")
    comment_preview = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["row_index"]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "row_index"],
                name="uniq_pipeline_jdjobmergedrow_job_idx",
            ),
        ]
        indexes = [
            models.Index(fields=["job", "sku_id"]),
            models.Index(fields=["job", "leaf_category_norm"]),
            models.Index(fields=["job", "matrix_group_label"]),
            models.Index(fields=["job", "price_value"]),
        ]

    def __str__(self) -> str:
        return f"job{self.job_id} merged#{self.row_index}"
