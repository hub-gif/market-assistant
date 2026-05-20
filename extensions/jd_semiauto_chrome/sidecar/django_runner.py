# -*- coding: utf-8 -*-
"""通过 manage.py 调用现有 Django 入库（不修改 pipeline 代码）。"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from paths import manage_py, project_root, python_executable, run_dir_relative_to_jd


def _run_manage(args: list[str], *, timeout: float | None = 600.0) -> tuple[int, str, str]:
    cmd = [python_executable(), str(manage_py()), *args]
    env = dict(**__import__("os").environ)
    if not env.get("LOW_GI_PROJECT_ROOT"):
        env["LOW_GI_PROJECT_ROOT"] = str(project_root())
    proc = subprocess.run(
        cmd,
        cwd=str(manage_py().parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
    )
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def _shell_json(code: str) -> Any:
    wrapped = (
        "import json\n"
        "from django.utils import timezone\n"
        f"_out = None\n"
        f"try:\n"
        f"{_indent(code)}\n"
        f"except Exception as e:\n"
        f"    _out = {{'error': str(e)}}\n"
        f"import json as _json\n"
        f"print(_json.dumps(_out, ensure_ascii=False))\n"
    )

    rc, out, err = _run_manage(["shell", "-c", wrapped], timeout=120.0)
    if rc != 0:
        raise RuntimeError(err.strip() or out.strip() or f"manage.py shell 退出码 {rc}")
    line = ""
    for line in reversed((out or "").splitlines()):
        line = line.strip()
        if line.startswith("{"):
            break
    if not line:
        raise RuntimeError("shell 无 JSON 输出")
    data = json.loads(line)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data


def _indent(code: str) -> str:
    return "\n".join("    " + ln for ln in code.strip().splitlines())


def create_semiauto_job(*, keyword: str, run_dir: Path) -> int:
    """创建 RUNNING 半自动任务（不启动 Playwright）。"""
    run_dir_s = str(run_dir.resolve())
    kw = (keyword or "manual").strip() or "manual"
    code = f"""
from pipeline.models import PipelineJob, JobStatus
j = PipelineJob.objects.create(
    platform="jd",
    keyword={kw!r},
    status=JobStatus.RUNNING,
    source_type="semiauto",
    semiauto_phase="waiting_login",
    run_dir={run_dir_s!r},
)
_out = {{"job_id": j.id}}
"""
    data = _shell_json(code)
    return int(data["job_id"])


def update_semiauto_job(
    job_id: int,
    *,
    semiauto_phase: str | None = None,
    error_message: str | None = None,
) -> None:
    if semiauto_phase is None and error_message is None:
        return
    lines = ["updates = {}"]
    if semiauto_phase is not None:
        lines.append(f'updates["semiauto_phase"] = {semiauto_phase!r}')
    if error_message is not None:
        lines.append(f'updates["error_message"] = {error_message!r}')
    lines.append("updates['updated_at'] = timezone.now()")
    body = "\n".join(lines)
    code = f"""
from pipeline.models import PipelineJob
{body}
n = PipelineJob.objects.filter(pk={int(job_id)}).update(**updates)
_out = {{"updated": n}}
"""
    _shell_json(code)


def ingest_job_dataset(job_id: int, run_dir: Path) -> dict[str, Any]:
    rel = run_dir_relative_to_jd(run_dir)
    rc, out, err = _run_manage(
        ["ingest_pipeline_dataset", "--job-id", str(job_id), "--run-dir", rel],
        timeout=900.0,
    )
    if rc != 0:
        msg = (err or out).strip() or f"ingest 退出码 {rc}"
        if len(msg) > 800:
            msg = msg[:800] + "…"
        raise RuntimeError(msg)
    try:
        return json.loads(out.strip().splitlines()[-1]) if out.strip() else {}
    except json.JSONDecodeError:
        return {"stdout": out[-2000:]}


def mark_job_success(job_id: int) -> None:
    code = f"""
from pipeline.models import PipelineJob, JobStatus
PipelineJob.objects.filter(pk={int(job_id)}).update(
    status=JobStatus.SUCCESS,
    semiauto_phase="done",
    error_message="",
    updated_at=timezone.now(),
)
_out = {{"ok": True}}
"""
    _shell_json(code)


def mark_job_failed(job_id: int, message: str) -> None:
    msg = (message or "插件盘后失败")[:2000]
    code = f"""
from pipeline.models import PipelineJob, JobStatus
PipelineJob.objects.filter(pk={int(job_id)}).update(
    status=JobStatus.FAILED,
    semiauto_phase="done",
    error_message={msg!r},
    updated_at=timezone.now(),
)
_out = {{"ok": True}}
"""
    _shell_json(code)
