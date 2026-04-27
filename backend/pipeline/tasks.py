from __future__ import annotations

import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from .cookie_paste import normalize_browser_cookie_paste
from .ingest import try_ingest_job_full
from .jd.runner import (
    resolve_pipeline_run_directory_for_job,
    try_write_competitor_report_if_merged_exists,
)
from .models import JobStatus, PipelineJob


def execute_job(job_id: int) -> None:
    job = PipelineJob.objects.filter(pk=job_id).first()
    if not job:
        return

    job.status = JobStatus.RUNNING
    job.error_message = ""
    job.save(update_fields=["status", "error_message", "updated_at"])

    job.refresh_from_db()
    if job.cancellation_requested:
        job.status = JobStatus.CANCELLED
        job.cancellation_requested = False
        job.error_message = "已终止（任务开始后立即收到终止请求）。"
        job.updated_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "cancellation_requested",
                "error_message",
                "updated_at",
            ],
        )
        PipelineJob.objects.filter(pk=job_id).update(cookie_text="")
        return

    cookie_temp: Path | None = None
    try:
        if job.platform != "jd":
            raise ValueError(f"暂不支持平台: {job.platform}")

        cookie_path_for_pipeline: str | None = None
        _cookie_body = normalize_browser_cookie_paste(job.cookie_text or "")
        if _cookie_body:
            runtime_dir = Path(settings.BASE_DIR) / "runtime_cookies"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            cookie_temp = (runtime_dir / f"job_{job_id}_cookie.txt").resolve()
            cookie_temp.write_text(_cookie_body, encoding="utf-8")
            cookie_path_for_pipeline = str(cookie_temp)
        elif (job.cookie_file_path or "").strip():
            cookie_path_for_pipeline = job.cookie_file_path.strip()

        rc_cfg = job.report_config if isinstance(job.report_config, dict) else {}
        run_dir_path = resolve_pipeline_run_directory_for_job(job)
        run_dir_path.mkdir(parents=True, exist_ok=True)

        manage_py = Path(settings.BASE_DIR) / "manage.py"
        env = os.environ.copy()
        env["PIPELINE_JOB_RUN_DIR"] = str(run_dir_path.resolve())
        if cookie_path_for_pipeline:
            env["PIPELINE_JOB_COOKIE_PATH"] = str(cookie_path_for_pipeline)

        proc = subprocess.Popen(
            [sys.executable, str(manage_py), "run_pipeline_job", str(job_id)],
            cwd=str(Path(settings.BASE_DIR).resolve()),
            env=env,
            stdin=subprocess.DEVNULL,
        )
        user_terminated = False
        while True:
            if proc.poll() is not None:
                break
            time.sleep(0.25)
            if PipelineJob.objects.filter(
                pk=job_id, cancellation_requested=True
            ).exists():
                user_terminated = True
                proc.terminate()
                break

        if proc.poll() is None:
            try:
                proc.wait(timeout=25)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    pass
        else:
            proc.wait()

        returncode = proc.returncode
        if returncode is None:
            returncode = -1

        if user_terminated:
            job.status = JobStatus.CANCELLED
            job.run_dir = str(run_dir_path.resolve())
            job.cancellation_requested = False
            job.error_message = (
                "已终止：已结束采集子进程（与在终端对脚本按 Ctrl+C 类似，可能留下部分文件）。"
            )
            try_write_competitor_report_if_merged_exists(
                run_dir_path,
                (job.keyword or "").strip(),
                report_config=rc_cfg or None,
            )
        elif returncode == 0:
            job.status = JobStatus.SUCCESS
            job.run_dir = str(run_dir_path.resolve())
            job.error_message = ""
            job.cancellation_requested = False
        else:
            job.status = JobStatus.FAILED
            job.run_dir = str(run_dir_path.resolve())
            job.cancellation_requested = False
            job.error_message = f"流水线子进程异常退出（exit {returncode}）。"
    except Exception as e:
        job.status = JobStatus.FAILED
        job.cancellation_requested = False
        job.error_message = f"{e}\n\n{traceback.format_exc()}"
    job.updated_at = timezone.now()
    job.save(
        update_fields=[
            "status",
            "run_dir",
            "error_message",
            "cancellation_requested",
            "updated_at",
        ],
    )
    if job.status == JobStatus.SUCCESS:
        try_ingest_job_full(PipelineJob.objects.get(pk=job_id))
    if cookie_temp is not None and cookie_temp.is_file():
        try:
            cookie_temp.unlink()
        except OSError:
            pass
    PipelineJob.objects.filter(pk=job_id).update(cookie_text="")
