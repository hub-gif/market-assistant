# -*- coding: utf-8 -*-
"""由 ``execute_job`` 在子进程中调用，勿直接用于交互调试。"""
from __future__ import annotations

import os
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pipeline.cookie_paste import normalize_browser_cookie_paste
from pipeline.jd.runner import run_jd_keyword_and_report
from pipeline.models import PipelineJob


class Command(BaseCommand):
    help = "内部命令：在独立进程中执行京东流水线（便于父进程 terminate 模拟 Ctrl+C）。"

    def add_arguments(self, parser) -> None:
        parser.add_argument("job_id", type=int)

    def handle(self, *args, **options) -> None:
        job_id = int(options["job_id"])
        run_dir = (os.environ.get("PIPELINE_JOB_RUN_DIR") or "").strip()
        if not run_dir:
            raise CommandError("缺少环境变量 PIPELINE_JOB_RUN_DIR")

        job = PipelineJob.objects.filter(pk=job_id).first()
        if not job:
            raise CommandError(f"任务不存在: {job_id}")

        cookie_path = (os.environ.get("PIPELINE_JOB_COOKIE_PATH") or "").strip() or None
        if cookie_path and not Path(cookie_path).is_file():
            cookie_path = None
        # 与父进程 env 双保险：粘贴 Cookie 仍以 DB 为准再落盘（避免 Windows 等环境下 env 未传到子进程）
        _from_db = normalize_browser_cookie_paste(job.cookie_text or "")
        if not cookie_path and _from_db:
            runtime_dir = Path(settings.BASE_DIR) / "runtime_cookies"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            worker_cookie = (runtime_dir / f"job_{job_id}_cookie_worker.txt").resolve()
            worker_cookie.write_text(_from_db, encoding="utf-8")
            cookie_path = str(worker_cookie)

        rc = job.report_config if isinstance(job.report_config, dict) else {}

        run_jd_keyword_and_report(
            job.keyword,
            max_skus=job.max_skus,
            page_start=job.page_start,
            page_to=job.page_to,
            pipeline_run_dir=run_dir,
            cookie_file_path=cookie_path,
            pvid=(job.pvid or "").strip() or None,
            request_delay=(job.request_delay or "").strip() or None,
            list_pages=(job.list_pages or "").strip() or None,
            scenario_filter_enabled=job.scenario_filter_enabled,
            report_config=rc or None,
            cancel_check=None,
        )
