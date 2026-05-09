# -*- coding: utf-8 -*-
"""
收敛半自动任务在任务列表点「终止」后仍卡在「执行中 / 终止处理中」的数据库状态。

典型原因：对应的后台线程已退出，无法再清 ``cancellation_requested``。

用法（在 ``backend`` 目录下，已激活 venv）::

  python manage.py semiauto_settle_stuck --job-id 33
  python manage.py semiauto_settle_stuck --dry-run
  python manage.py semiauto_settle_stuck
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from pipeline.semiauto_tasks import settle_semiauto_stuck_cancel_flags


class Command(BaseCommand):
    help = "半自动：将 running + cancellation_requested 的任务置为 cancelled（仅数据库）"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--job-id",
            type=int,
            default=None,
            help="只处理指定任务主键；不传则处理所有匹配行",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="仅打印将匹配的任务 id，不写库",
        )

    def handle(self, *args, **options):
        jid = options.get("job_id")
        dry = bool(options.get("dry_run"))
        n, ids = settle_semiauto_stuck_cancel_flags(job_id=jid, dry_run=dry)
        if dry:
            self.stdout.write(self.style.WARNING(f"dry-run：将匹配 {n} 条 → {ids}"))
        else:
            self.stdout.write(self.style.SUCCESS(f"已收敛 {n} 条 → {ids}"))
