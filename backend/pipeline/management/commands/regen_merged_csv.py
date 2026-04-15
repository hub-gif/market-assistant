# -*- coding: utf-8 -*-
"""补全并规范化已有 run 目录下的 ``keyword_pipeline_merged.csv``（lean 列序，与 detail_ware 对齐）。"""
from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pipeline.ingest import resolve_and_validate_run_dir
from pipeline.jd.merged_regen import write_keyword_pipeline_merged_lean_csv


class Command(BaseCommand):
    help = "将 keyword_pipeline_merged.csv 重写为 lean 宽表列序，并刷新榜单/购买者摘要列。"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--run-dir",
            type=str,
            required=True,
            help="相对 data/JD 的子路径，或位于 data/JD 下的绝对路径",
        )

    def handle(self, *args, **options):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            raise CommandError("请在 .env 中配置 LOW_GI_PROJECT_ROOT")
        raw = str(options["run_dir"] or "").strip()
        try:
            run_path = resolve_and_validate_run_dir(raw)
        except ValueError as e:
            raise CommandError(str(e)) from e

        n, p = write_keyword_pipeline_merged_lean_csv(run_path)
        self.stdout.write(self.style.SUCCESS(f"已写 {n} 行 -> {p}"))
