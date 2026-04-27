# -*- coding: utf-8 -*-
"""
将磁盘上已存在的流水线批次目录（``data/JD/pipeline_runs/...``）导入数据库：

- ``pc_search_export.csv`` / ``detail_ware_export.csv`` / ``comments_flat.csv``
- ``keyword_pipeline_merged.csv``（合并宽表 + JdProduct / JdProductSnapshot）

用法（在 ``backend`` 目录下）::

  python manage.py ingest_pipeline_dataset --run-dir pipeline_runs/20260413_104252_低GI

或绝对路径（仍须在 ``data/JD`` 下）::

  python manage.py ingest_pipeline_dataset --run-dir "D:/.../data/JD/pipeline_runs/xxx"

绑定已有 ``PipelineJob``::

  python manage.py ingest_pipeline_dataset --job-id 12 --run-dir pipeline_runs/xxx

新建任务并入库（关键词优先读 ``run_meta.json``）::

  python manage.py ingest_pipeline_dataset --create --run-dir pipeline_runs/xxx --keyword 低GI
"""
from __future__ import annotations

import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pipeline.ingest import FILE_MERGED_CSV, ingest_job_full, resolve_and_validate_run_dir
from pipeline.models import JobStatus, PipelineJob


class Command(BaseCommand):
    help = "将已有 pipeline run 目录下的 CSV 导入数据库（搜索/详情/评价/合并表与商品快照）。"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--run-dir",
            type=str,
            required=True,
            help="相对 data/JD 的子路径，或位于 data/JD 下的绝对路径",
        )
        parser.add_argument(
            "--job-id",
            type=int,
            default=None,
            help="绑定到已有 PipelineJob；未给则须配合 --create",
        )
        parser.add_argument(
            "--create",
            action="store_true",
            help="新建 PipelineJob（success）并写入 run_dir 后入库",
        )
        parser.add_argument(
            "--keyword",
            type=str,
            default="",
            help="与 --create 合用；默认尝试从 run_meta.json 读取 keyword",
        )

    def handle(self, *args, **options):
        if not (settings.LOW_GI_PROJECT_ROOT or "").strip():
            raise CommandError("请在 .env 中配置 LOW_GI_PROJECT_ROOT")

        raw = str(options["run_dir"] or "").strip()
        try:
            run_path = resolve_and_validate_run_dir(raw)
        except ValueError as e:
            raise CommandError(str(e)) from e

        merged = run_path / FILE_MERGED_CSV
        if not merged.is_file():
            self.stdout.write(
                self.style.WARNING(
                    f"缺少 {FILE_MERGED_CSV}，仍将尝试导入搜索/详情/评价（合并表与快照会跳过或报错）。"
                )
            )

        job_id = options.get("job_id")
        create = bool(options.get("create"))
        kw_in = (options.get("keyword") or "").strip()

        if job_id and create:
            raise CommandError("请只使用 --job-id 或 --create 之一")

        if create:
            meta_kw = ""
            meta_path = run_path / "run_meta.json"
            if meta_path.is_file():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    if isinstance(meta, dict):
                        meta_kw = str(meta.get("keyword") or "").strip()
                except (json.JSONDecodeError, OSError):
                    pass
            keyword = kw_in or meta_kw or "imported"
            job = PipelineJob.objects.create(
                platform="jd",
                keyword=keyword[:256],
                status=JobStatus.SUCCESS,
                run_dir=str(run_path),
            )
            self.stdout.write(self.style.NOTICE(f"已创建任务 id={job.id} keyword={job.keyword!r}"))
        elif job_id:
            job = PipelineJob.objects.filter(pk=job_id).first()
            if not job:
                raise CommandError(f"找不到 PipelineJob id={job_id}")
            job.run_dir = str(run_path)
            job.save(update_fields=["run_dir", "updated_at"])
            self.stdout.write(self.style.NOTICE(f"已更新任务 id={job.id} 的 run_dir"))
        else:
            raise CommandError("请指定 --job-id 绑定已有任务，或使用 --create 新建任务")

        try:
            stats = ingest_job_full(job)
        except FileNotFoundError as e:
            raise CommandError(str(e)) from e

        self.stdout.write(self.style.SUCCESS(json.dumps(stats, ensure_ascii=False, indent=2)))
        self.stdout.write(
            self.style.NOTICE(
                f"完成。前端可打开任务 {job.id}，数据集接口："
                f"/api/pipeline/jobs/{job.id}/dataset/summary/ 等。"
            )
        )
