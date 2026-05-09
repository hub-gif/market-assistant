"""
半自动 JD 监听任务：后台线程管理子进程生命周期，链式执行 JSON 落盘 → CSV 解析 → 数据入库。

用法（由 semiauto_views.py 启动）::

    import threading
    t = threading.Thread(target=start_semiauto_job, args=(job.id,), daemon=True)
    t.start()
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from .ingest import try_ingest_job_full
from .models import JobStatus, PipelineJob

import logging

logger = logging.getLogger(__name__)


# 轮询状态文件的间隔（秒）
_POLL_INTERVAL = 0.8
# 等待子进程初始化的最长时间（秒），之后强制前进
_PHASE_TIMEOUT = 120.0


def _low_gi_project_root() -> Path:
    """读取 Django settings 中的 LOW_GI_PROJECT_ROOT，返回 Path。"""
    root = getattr(settings, "LOW_GI_PROJECT_ROOT", None) or ""
    return Path(root).resolve()


def _semiauto_base_dir() -> Path:
    return _low_gi_project_root() / "data" / "JD" / "sb_cdp_api_semiauto"


def _make_run_dir(keyword: str) -> Path:
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_kw = "".join(c if (c.isalnum() or c in "-_") else "_" for c in (keyword or "manual")[:32])
    name = f"{ts}_{safe_kw}"
    d = _semiauto_base_dir() / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def _update_job(job_id: int, **kwargs) -> PipelineJob | None:
    """原子更新任务字段，防止 Django ORM 缓存问题。"""
    try:
        PipelineJob.objects.filter(pk=job_id).update(updated_at=timezone.now(), **kwargs)
        return PipelineJob.objects.get(pk=job_id)
    except PipelineJob.DoesNotExist:
        return None


def _poll_semiauto_status_or_cancel(
    *,
    run_dir: Path,
    marker: str,
    timeout: float,
    job_id: int,
    proc: subprocess.Popen,
    phase_if_ok: str,
) -> str:
    """
    轮询 marker 文件、用户终止、子进程死活。

    返回 ``ok`` | ``timeout`` | ``cancelled`` | ``proc_dead``。
    ok 时已写入 ``semiauto_phase=phase_if_ok``。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (run_dir / marker).is_file():
            _update_job(job_id, semiauto_phase=phase_if_ok)
            return "ok"
        if PipelineJob.objects.filter(pk=job_id, cancellation_requested=True).exists():
            return "cancelled"
        if proc.poll() is not None:
            return "proc_dead"
        time.sleep(_POLL_INTERVAL)
    return "timeout"


def _reap_listen_proc(proc: subprocess.Popen) -> int | None:
    """阻塞直至子进程结束；必要时 escalate 到 kill（对齐 ``tasks.execute_job``）。"""
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
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            pass
    return proc.returncode


def _semiauto_abort_on_user_cancel(proc: subprocess.Popen, stop_file: Path, job_id: int) -> None:
    """任务列表「终止」：写 stop + terminate + 收口 + 标记已终止。"""
    logger.info("semiauto_tasks: job=%s 收到 cancellation_requested，结束监听子进程", job_id)
    try:
        stop_file.parent.mkdir(parents=True, exist_ok=True)
        stop_file.touch()
    except OSError:
        logger.exception("semiauto_tasks: touch stop_file 失败 job=%s path=%s", job_id, stop_file)
    try:
        proc.terminate()
    except OSError:
        logger.exception("semiauto_tasks: proc.terminate 失败 job=%s", job_id)

    rc = _reap_listen_proc(proc)
    logger.info("semiauto_tasks: job=%s 监听子进程已结束 rc=%s（用户终止）", job_id, rc)

    _update_job(
        job_id,
        status=JobStatus.CANCELLED,
        semiauto_phase="done",
        cancellation_requested=False,
        error_message=(
            "已终止：已结束半自动监听子进程；run_dir 内若有 JSON 可保留，可自行补解析入库。"
        ),
    )


def start_semiauto_job(job_id: int) -> None:
    """
    后台线程主函数。执行完整半自动生命周期：

    1. 创建 run_dir，更新 job.run_dir
    2. 启动 ``run_listen_demo.py`` 子进程
    3. 等待 ``.status_waiting_login``（过程可因任务列表「终止」而中止）
    4. 等待 ``.status_listening``（同上）
    5. 等待子进程退出（半自动页写 ``.stop_requested`` 或任务列表置 ``cancellation_requested`` 后
       会 ``terminate`` 子进程；正常退出后进入后处理）
    6. 若用户终止：``status=cancelled``，不跑 CSV/入库
    7. 否则：JSON→CSV、``try_ingest_job_full``、``status=success``

    卡在「执行中 + 终止处理中」且后台线程已失联时，可用
    ``manage.py semiauto_settle_stuck`` 收敛数据库状态。
    """
    job = PipelineJob.objects.filter(pk=job_id).first()
    if not job:
        logger.error("semiauto_tasks: job %s 不存在", job_id)
        return

    keyword = (job.keyword or "manual").strip()

    try:
        run_dir = _make_run_dir(keyword)
    except Exception:
        logger.exception("semiauto_tasks: 创建 run_dir 失败 job=%s", job_id)
        _update_job(job_id, status=JobStatus.FAILED, error_message="创建 run_dir 失败")
        return

    login_file = run_dir / ".login_confirmed"
    stop_file = run_dir / ".stop_requested"
    restart_file = run_dir / ".restart_listen_requested"

    # 将 run_dir 写回任务（ingest 需要）
    _update_job(job_id, run_dir=str(run_dir), semiauto_phase="browser_open")

    # ── 启动子进程 ─────────────────────────────────────────────────────────────
    crawler_copy = Path(settings.BASE_DIR) / "crawler_copy"
    script = crawler_copy / "sb_browser" / "platforms" / "jd_semiauto" / "run_listen_demo.py"

    cmd = [
        sys.executable,
        str(script),
        "--run-dir", str(run_dir),
        "--keyword", keyword,
        "--login-file", str(login_file),
        "--stop-file", str(stop_file),
        "--restart-file", str(restart_file),
    ]
    logger.info("semiauto_tasks: 启动子进程 job=%s cmd=%s", job_id, cmd)

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(crawler_copy),
            stdin=subprocess.DEVNULL,
        )
    except Exception:
        logger.exception("semiauto_tasks: 启动子进程失败 job=%s", job_id)
        _update_job(job_id, status=JobStatus.FAILED, error_message="启动浏览器进程失败")
        return

    # ── 等待进入等待登录状态（兼容任务列表「终止」：轮询 cancellation_requested）────────
    r0 = _poll_semiauto_status_or_cancel(
        run_dir=run_dir,
        marker=".status_waiting_login",
        timeout=_PHASE_TIMEOUT,
        job_id=job_id,
        proc=proc,
        phase_if_ok="waiting_login",
    )
    if r0 == "cancelled":
        _semiauto_abort_on_user_cancel(proc, stop_file, job_id)
        return
    if r0 == "proc_dead":
        logger.warning(
            "semiauto_tasks: job=%s 子进程在 .status_waiting_login 前退出，进入失败",
            job_id,
        )
        _reap_listen_proc(proc)
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            cancellation_requested=False,
            error_message="半自动监听子进程在早期异常退出（未进入等待登录状态）。",
        )
        return
    if r0 == "timeout":
        logger.warning("semiauto_tasks: job=%s .status_waiting_login 超时，强制前进", job_id)
        _update_job(job_id, semiauto_phase="waiting_login")
    else:
        logger.info("semiauto_tasks: job=%s phase=waiting_login", job_id)

    # ── 等待进入监听状态（最长 30 分钟，可被「终止」打断）────────────────────────────
    r1 = _poll_semiauto_status_or_cancel(
        run_dir=run_dir,
        marker=".status_listening",
        timeout=1800.0,
        job_id=job_id,
        proc=proc,
        phase_if_ok="listening",
    )
    if r1 == "cancelled":
        _semiauto_abort_on_user_cancel(proc, stop_file, job_id)
        return
    if r1 == "proc_dead":
        logger.warning(
            "semiauto_tasks: job=%s 子进程在监听开始前退出，进入失败",
            job_id,
        )
        _reap_listen_proc(proc)
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            cancellation_requested=False,
            error_message="半自动监听子进程在未进入监听阶段时退出。",
        )
        return
    if r1 == "timeout":
        logger.warning("semiauto_tasks: job=%s .status_listening 超时，强制前进", job_id)
        _update_job(job_id, semiauto_phase="listening")
    else:
        logger.info("semiauto_tasks: job=%s phase=listening", job_id)

    # ── 等待子进程退出（半自动页「结束任务」写 stop；任务列表「终止」见上行轮询）───────
    user_cancelled = False
    while proc.poll() is None:
        time.sleep(_POLL_INTERVAL)
        if PipelineJob.objects.filter(pk=job_id, cancellation_requested=True).exists():
            user_cancelled = True
            try:
                stop_file.touch()
            except OSError:
                logger.exception(
                    "semiauto_tasks: stop_file.touch 失败 job=%s path=%s",
                    job_id,
                    stop_file,
                )
            try:
                proc.terminate()
            except OSError:
                logger.exception("semiauto_tasks: terminate 失败 job=%s", job_id)
            break

    _reap_listen_proc(proc)

    if user_cancelled or PipelineJob.objects.filter(
        pk=job_id,
        cancellation_requested=True,
    ).exists():
        _update_job(
            job_id,
            status=JobStatus.CANCELLED,
            semiauto_phase="done",
            cancellation_requested=False,
            error_message=(
                "已终止：已结束半自动监听子进程；run_dir 内若有 JSON 可保留，可自行补解析入库。"
            ),
        )
        logger.info("semiauto_tasks: job=%s 已进入已终止状态（任务列表终止）", job_id)
        return

    rc = proc.returncode
    logger.info("semiauto_tasks: job=%s 子进程退出 rc=%s", job_id, rc)
    _update_job(job_id, semiauto_phase="stopping")

    # ── 后处理：解析 JSON → CSV ────────────────────────────────────────────────
    try:
        _run_parse_csv(run_dir, crawler_copy)
    except Exception:
        logger.exception("semiauto_tasks: CSV 解析失败 job=%s", job_id)
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            error_message="JSON→CSV 解析失败，数据已落盘，可手动重试入库",
        )
        return

    # ── 后处理：数据入库 ───────────────────────────────────────────────────────
    job = PipelineJob.objects.filter(pk=job_id).first()
    if job:
        try_ingest_job_full(job)

    # ── 完成 ──────────────────────────────────────────────────────────────────
    _update_job(job_id, status=JobStatus.SUCCESS, semiauto_phase="done")
    logger.info("semiauto_tasks: job=%s 完成", job_id)


def settle_semiauto_stuck_cancel_flags(
    *,
    job_id: int | None = None,
    dry_run: bool = False,
) -> tuple[int, list[int]]:
    """
    将仍为 **执行中** 且 **已请求终止** 的半自动任务批量改为 **已终止**（只改数据库）。

    适用于后台守护线程已退出、无法自动清 ``cancellation_requested`` 的残留行。
    使用前请确认已无对应 ``run_listen_demo`` 进程，避免与真实在跑任务打架。
    """
    qs = PipelineJob.objects.filter(
        source_type="semiauto",
        status=JobStatus.RUNNING,
        cancellation_requested=True,
    )
    if job_id is not None:
        qs = qs.filter(pk=job_id)
    ids = list(qs.values_list("pk", flat=True))
    if dry_run or not ids:
        return (len(ids), ids)
    qs.update(
        status=JobStatus.CANCELLED,
        cancellation_requested=False,
        semiauto_phase="done",
        error_message=(
            "已终止：由 manage.py semiauto_settle_stuck 同步数据库"
            "（此前「终止」未由后台线程自动收尾）。"
        ),
        updated_at=timezone.now(),
    )
    for pk in ids:
        logger.info("semiauto_tasks: settle_semiauto_stuck_cancel_flags 已收敛 job_id=%s", pk)
    return (len(ids), ids)


def finish_semiauto_after_browser(
    job_id: int,
    *,
    crawler_copy: Path | None = None,
) -> bool:
    """
    在半自动子进程已结束但后台线程未跑完后处理时（例如进程被强杀、线程卡住），
    补跑：JSON 目录→``run_parse_semiauto_to_csv``→``try_ingest_job_full``→状态成功。

    要求：任务已写入有效 ``run_dir``，且该目录下已有半自动落盘的 JSON。

    :return: 是否将任务更新为成功；若解析失败则为 False 且任务标记 failed。
    """
    job = PipelineJob.objects.filter(pk=job_id).first()
    if not job:
        logger.error("finish_semiauto_after_browser: job %s 不存在", job_id)
        return False

    run_dir_raw = (job.run_dir or "").strip()
    if not run_dir_raw:
        logger.error("finish_semiauto_after_browser: job %s 无 run_dir", job_id)
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            error_message="无 run_dir，无法执行 JSON→CSV 与入库",
        )
        return False

    run_dir = Path(run_dir_raw)
    if not run_dir.is_dir():
        logger.error(
            "finish_semiauto_after_browser: job %s run_dir 非目录 %s",
            job_id,
            run_dir,
        )
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            error_message=f"run_dir 不存在或不是目录: {run_dir}",
        )
        return False

    cc = crawler_copy or (Path(settings.BASE_DIR) / "crawler_copy")
    _update_job(job_id, semiauto_phase="stopping")

    try:
        _run_parse_csv(run_dir, cc)
    except Exception:
        logger.exception(
            "finish_semiauto_after_browser: CSV 解析失败 job=%s",
            job_id,
        )
        _update_job(
            job_id,
            status=JobStatus.FAILED,
            error_message="JSON→CSV 解析失败，数据已落盘，可修正后重试入库",
        )
        return False

    job = PipelineJob.objects.filter(pk=job_id).first()
    if job:
        try_ingest_job_full(job)

    _update_job(
        job_id,
        status=JobStatus.SUCCESS,
        semiauto_phase="done",
        error_message="",
    )
    logger.info("finish_semiauto_after_browser: job=%s 后处理完成，已置为成功", job_id)
    return True


def _run_parse_csv(run_dir: Path, crawler_copy: Path) -> None:
    """在 crawler_copy 环境中调用 run_parse_semiauto_to_csv.run()。"""
    if str(crawler_copy) not in sys.path:
        sys.path.insert(0, str(crawler_copy))

    import importlib.util
    src = crawler_copy / "sb_browser" / "platforms" / "jd_semiauto" / "run_parse_semiauto_to_csv.py"
    spec = importlib.util.spec_from_file_location("run_parse_semiauto_to_csv", src)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载模块: {src}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    n_list, n_detail, n_comment, n_merged = mod.run(run_dir)
    logger.info(
        "semiauto_tasks: CSV 解析完成 list=%s detail=%s comment=%s merged=%s",
        n_list, n_detail, n_comment, n_merged,
    )
