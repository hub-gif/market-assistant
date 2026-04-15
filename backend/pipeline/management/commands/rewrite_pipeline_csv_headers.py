# -*- coding: utf-8 -*-
"""
将已有 run 目录下 CSV 表头重写为 ``csv_schema`` 纯中文表头（仅重命名与列序，不修改业务逻辑）。

在 ``backend`` 目录::

  python manage.py rewrite_pipeline_csv_headers --run-dir pipeline_runs/20260413_104252_低GI
  python manage.py rewrite_pipeline_csv_headers --run-dir pipeline_runs/xxx --dry-run

仅处理部分文件::

  python manage.py rewrite_pipeline_csv_headers --run-dir pipeline_runs/xxx --file keyword_pipeline_merged.csv
"""
from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pipeline.csv_header_rewrite import rewrite_run_dir_csv_headers
from pipeline.ingest import (
    FILE_COMMENTS_FLAT_CSV,
    FILE_DETAIL_WARE_CSV,
    FILE_MERGED_CSV,
    FILE_PC_SEARCH_CSV,
    resolve_and_validate_run_dir,
)


class Command(BaseCommand):
    help = "将 pipeline run 目录内 CSV 表头规范为 csv_schema 中的中文表头。"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--run-dir",
            type=str,
            required=True,
            help="相对 data/JD 的子路径，或位于 data/JD 下的绝对路径",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="只打印将执行的操作，不写回文件",
        )
        parser.add_argument(
            "--file",
            action="append",
            dest="files",
            metavar="NAME",
            help=(
                "只处理指定文件名，可多次传入。"
                f"可选: {FILE_MERGED_CSV}, {FILE_PC_SEARCH_CSV}, "
                f"{FILE_COMMENTS_FLAT_CSV}, {FILE_DETAIL_WARE_CSV}"
            ),
        )

    def handle(self, *args, **options):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            raise CommandError("请在 .env 中配置 LOW_GI_PROJECT_ROOT")

        raw = str(options["run_dir"] or "").strip()
        try:
            run_path = resolve_and_validate_run_dir(raw)
        except ValueError as e:
            raise CommandError(str(e)) from e

        dry = bool(options["dry_run"])
        only = options.get("files") or None

        msgs = rewrite_run_dir_csv_headers(run_path, dry_run=dry, only=only)
        for msg in msgs:
            self.stdout.write(msg)
        if dry:
            self.stdout.write(self.style.WARNING("dry-run：未写入磁盘"))
        else:
            self.stdout.write(self.style.SUCCESS(f"完成: {run_path}"))
