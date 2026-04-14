# -*- coding: utf-8 -*-
"""
合并表 / 搜索导出表入库后若 ``total_sales`` 为空，可按与 ingest 相同规则从 ``comment_sales_floor`` 补全。

  python manage.py refresh_jd_merged_total_sales
  python manage.py refresh_jd_merged_total_sales --job-id 42
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from pipeline.csv_schema import (
    JD_SEARCH_CSV_HEADERS,
    MERGED_FIELD_TO_CSV_HEADER,
    merged_csv_effective_total_sales,
    search_csv_effective_total_sales,
)
from pipeline.models import JdJobMergedRow, JdJobSearchRow


class Command(BaseCommand):
    help = "从销量楼层推断并回填 JdJobMergedRow / JdJobSearchRow 的 total_sales（与 ingest 一致）。"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--job-id",
            type=int,
            default=None,
            help="仅处理该 PipelineJob；默认处理全部任务下的行",
        )

    def handle(self, *args, **options) -> None:
        job_id = options.get("job_id")
        n_merged = self._refresh_merged(job_id)
        n_search = self._refresh_search(job_id)
        self.stdout.write(
            self.style.SUCCESS(
                f"完成：JdJobMergedRow 更新约 {n_merged} 行，"
                f"JdJobSearchRow 更新约 {n_search} 行"
            )
        )

    def _refresh_merged(self, job_id: int | None) -> int:
        qs = JdJobMergedRow.objects.all().order_by("id")
        if job_id is not None:
            qs = qs.filter(job_id=job_id)

        h_ts = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
        h_fl = MERGED_FIELD_TO_CSV_HEADER["comment_sales_floor"]
        updates: list[JdJobMergedRow] = []
        n_changed = 0
        for r in qs.iterator(chunk_size=800):
            row = {h_ts: r.total_sales or "", h_fl: r.comment_sales_floor or ""}
            eff = merged_csv_effective_total_sales(row)
            if eff and eff != (r.total_sales or "").strip():
                r.total_sales = eff
                updates.append(r)
                n_changed += 1
            if len(updates) >= 500:
                JdJobMergedRow.objects.bulk_update(updates, ["total_sales"])
                updates.clear()
        if updates:
            JdJobMergedRow.objects.bulk_update(updates, ["total_sales"])
        return n_changed

    def _refresh_search(self, job_id: int | None) -> int:
        qs = JdJobSearchRow.objects.all().order_by("id")
        if job_id is not None:
            qs = qs.filter(job_id=job_id)

        h_ts = JD_SEARCH_CSV_HEADERS["total_sales"]
        h_fl = JD_SEARCH_CSV_HEADERS["comment_sales_floor"]
        updates: list[JdJobSearchRow] = []
        n_changed = 0
        for r in qs.iterator(chunk_size=800):
            row = {h_ts: r.total_sales or "", h_fl: r.comment_sales_floor or ""}
            eff = search_csv_effective_total_sales(row)
            if eff and eff != (r.total_sales or "").strip():
                r.total_sales = eff
                updates.append(r)
                n_changed += 1
            if len(updates) >= 500:
                JdJobSearchRow.objects.bulk_update(updates, ["total_sales"])
                updates.clear()
        if updates:
            JdJobSearchRow.objects.bulk_update(updates, ["total_sales"])
        return n_changed
