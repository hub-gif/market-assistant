# -*- coding: utf-8 -*-
"""
插件半自动会话：对齐产品阶段（waiting_login → listening → stopping → done），
盘后复用 run_parse_semiauto_to_csv + ingest_pipeline_dataset。
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    from . import django_runner, postprocess_runner, run_dir as run_dir_mod
    from .writer import RunDirWriter, validate_run_dir_under_jd
except ImportError:
    import django_runner
    import postprocess_runner
    import run_dir as run_dir_mod
    from writer import RunDirWriter, validate_run_dir_under_jd

logger = logging.getLogger(__name__)

PHASE_WAITING_LOGIN = "waiting_login"
PHASE_LISTENING = "listening"
PHASE_STOPPING = "stopping"
PHASE_DONE = "done"
PHASE_FAILED = "failed"
PHASE_IDLE = "idle"


@dataclass
class SessionSnapshot:
    phase: str = PHASE_IDLE
    keyword: str = ""
    run_dir: str | None = None
    job_id: int | None = None
    error_message: str = ""
    create_django_job: bool = True
    counts: dict[str, int] = field(
        default_factory=lambda: {"list": 0, "detail": 0, "comment": 0, "graphic": 0, "written": 0}
    )


class SemiautoSessionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._writer: RunDirWriter | None = None
        self._run_dir: Path | None = None
        self._keyword: str = ""
        self._job_id: int | None = None
        self._phase: str = PHASE_IDLE
        self._error: str = ""
        self._create_job: bool = True
        self._finish_thread: threading.Thread | None = None

    @staticmethod
    def _counts_from_disk(rd: Path) -> dict[str, int]:
        counts = {"list": 0, "detail": 0, "comment": 0, "graphic": 0, "written": 0}
        for kind in ("list", "detail", "comment", "graphic"):
            d = rd / kind
            if d.is_dir():
                n = sum(1 for _ in d.glob("*.json"))
                counts[kind] = n
                counts["written"] += n
        return counts

    def snapshot(self) -> SessionSnapshot:
        with self._lock:
            counts = {"list": 0, "detail": 0, "comment": 0, "graphic": 0, "written": 0}
            if self._writer:
                counts = self._writer.file_counts()
                counts["written"] = sum(counts.get(k, 0) for k in ("list", "detail", "comment", "graphic"))
            elif self._run_dir and self._run_dir.is_dir():
                counts = self._counts_from_disk(self._run_dir)
            return SessionSnapshot(
                phase=self._phase,
                keyword=self._keyword,
                run_dir=str(self._run_dir) if self._run_dir else None,
                job_id=self._job_id,
                error_message=self._error,
                create_django_job=self._create_job,
                counts=counts,
            )

    def listening(self) -> bool:
        with self._lock:
            return self._phase == PHASE_LISTENING and self._writer is not None

    def start(
        self,
        *,
        keyword: str,
        run_dir: str | None = None,
        create_django_job: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            if self._phase not in (PHASE_IDLE, PHASE_DONE, PHASE_FAILED):
                raise ValueError(f"当前阶段不可启动：{self._phase}")

        kw = (keyword or "manual").strip() or "manual"
        rd = Path(run_dir).expanduser().resolve() if run_dir else run_dir_mod.make_run_dir(kw)
        rd = validate_run_dir_under_jd(rd)
        run_dir_mod.write_run_meta(rd, keyword=kw, capture_mode="chrome_extension")
        run_dir_mod.touch_marker(rd, ".status_waiting_login")

        job_id: int | None = None
        if create_django_job:
            try:
                job_id = django_runner.create_semiauto_job(keyword=kw, run_dir=rd)
            except Exception as e:
                logger.exception("创建 PipelineJob 失败")
                raise RuntimeError(f"创建任务记录失败：{e}") from e

        with self._lock:
            self._run_dir = rd
            self._keyword = kw
            self._job_id = job_id
            self._phase = PHASE_WAITING_LOGIN
            self._error = ""
            self._create_job = create_django_job
            self._writer = None

        return {
            "phase": PHASE_WAITING_LOGIN,
            "run_dir": str(rd),
            "job_id": job_id,
            "keyword": kw,
        }

    def confirm_login(self) -> dict[str, Any]:
        with self._lock:
            if self._phase != PHASE_WAITING_LOGIN or not self._run_dir:
                raise ValueError("请先启动任务并处于「等待登录」阶段")
            rd = self._run_dir
            kw = self._keyword
            job_id = self._job_id

        run_dir_mod.touch_marker(rd, ".login_confirmed")
        run_dir_mod.touch_marker(rd, ".status_listening")

        if job_id:
            try:
                django_runner.update_semiauto_job(job_id, semiauto_phase=PHASE_LISTENING)
            except Exception as e:
                logger.warning("更新 job phase 失败: %s", e)

        with self._lock:
            self._writer = RunDirWriter(rd, keyword=kw)
            self._phase = PHASE_LISTENING

        return {"phase": PHASE_LISTENING, "run_dir": str(rd), "job_id": job_id}

    @staticmethod
    def _keyword_from_run_meta(rd: Path) -> str:
        p = rd / "run_meta.json"
        if p.is_file():
            try:
                meta = json.loads(p.read_text(encoding="utf-8"))
                kw = str(meta.get("keyword") or "").strip()
                if kw:
                    return kw
            except Exception:
                pass
        return "manual"

    def maybe_attach_listening(self, run_dir: str | None) -> None:
        """sidecar 进程重启后，凭 run_dir 标记恢复 listening（扩展 storage 仍显示监听中）。"""
        with self._lock:
            if self._phase == PHASE_LISTENING and self._writer is not None:
                return

        if not run_dir:
            raise ValueError("未在监听阶段，拒绝写入")

        rd = validate_run_dir_under_jd(Path(run_dir).expanduser().resolve())
        if run_dir_mod.marker_exists(rd, ".stop_requested"):
            raise ValueError("任务已结束，无法继续写入")
        if not run_dir_mod.marker_exists(rd, ".status_listening"):
            raise ValueError("未在监听阶段，拒绝写入（请先点「确认登录」）")

        kw = self._keyword_from_run_meta(rd)
        with self._lock:
            if self._phase == PHASE_LISTENING and self._writer is not None:
                return
            self._run_dir = rd
            self._keyword = kw
            self._writer = RunDirWriter(rd, keyword=kw)
            self._phase = PHASE_LISTENING
            self._error = ""
        logger.info("已从磁盘恢复监听 run_dir=%s keyword=%s", rd, kw)

    def write_batch(
        self, items: list[dict[str, Any]], *, run_dir: str | None = None
    ) -> dict[str, Any]:
        self.maybe_attach_listening(run_dir)
        with self._lock:
            if self._phase != PHASE_LISTENING or self._writer is None:
                raise ValueError("未在监听阶段，拒绝写入")
            writer = self._writer
        return writer.write_batch(items)

    def stop(self, *, run_postprocess: bool = True) -> dict[str, Any]:
        with self._lock:
            if self._phase not in (PHASE_WAITING_LOGIN, PHASE_LISTENING):
                raise ValueError(f"当前阶段不可结束：{self._phase}")
            rd = self._run_dir
            job_id = self._job_id
            kw = self._keyword
            if not rd:
                raise ValueError("无 run_dir")
            self._phase = PHASE_STOPPING
            self._writer = None

        if rd:
            run_dir_mod.touch_marker(rd, ".stop_requested")

        if job_id:
            try:
                django_runner.update_semiauto_job(job_id, semiauto_phase=PHASE_STOPPING)
            except Exception:
                pass

        if run_postprocess:
            t = threading.Thread(
                target=self._finish_worker,
                args=(rd, job_id, kw),
                daemon=True,
                name="jd_plugin_finish",
            )
            with self._lock:
                self._finish_thread = t
            t.start()
            return {
                "phase": PHASE_STOPPING,
                "run_dir": str(rd),
                "job_id": job_id,
                "message": "已结束采集，正在盘后解析与入库…",
            }

        with self._lock:
            self._phase = PHASE_DONE
        return {"phase": PHASE_DONE, "run_dir": str(rd), "job_id": job_id}

    @staticmethod
    def _count_capture_json(rd: Path) -> int:
        n = 0
        for sub in ("list", "detail", "comment", "graphic"):
            d = rd / sub
            if d.is_dir():
                n += sum(1 for _ in d.glob("*.json"))
        return n

    def _finish_worker(self, rd: Path, job_id: int | None, _kw: str) -> None:
        err = ""
        try:
            n_json = self._count_capture_json(rd)
            if n_json == 0:
                raise RuntimeError(
                    "未采集到任何 JSON。请确认：① 扩展已重载；② 已点「确认登录」；"
                    "③ 在 www.jd.com 搜索/打开商详（非仅 Playwright 窗口）。"
                )
            postprocess_runner.run_parse_semiauto_to_csv(rd)
            if job_id:
                django_runner.ingest_job_dataset(job_id, rd)
                django_runner.mark_job_success(job_id)
        except Exception as e:
            logger.exception("插件盘后失败 run_dir=%s", rd)
            err = str(e)
            if job_id:
                try:
                    django_runner.mark_job_failed(job_id, err)
                except Exception:
                    pass

        with self._lock:
            if err:
                self._phase = PHASE_FAILED
                self._error = err[:2000]
            else:
                self._phase = PHASE_DONE
                self._error = ""
            # 保留 _run_dir，/job/status 仍可统计磁盘 JSON；下次 start 会覆盖
            self._job_id = None
            self._writer = None

    # ── 兼容旧 /session/* API ─────────────────────────────────────────────

    def legacy_session_start(self, run_dir_s: str, keyword: str) -> dict[str, Any]:
        """旧 API：指定 run_dir 并立即进入 listening（无 Django 任务）。"""
        self.start(keyword=keyword, run_dir=run_dir_s, create_django_job=False)
        out = self.confirm_login()
        return {"ok": True, "run_dir": out["run_dir"], "keyword": keyword, "phase": out["phase"]}

    def legacy_session_stop(self) -> dict[str, Any]:
        with self._lock:
            phase = self._phase
            rd = self._run_dir
        if phase == PHASE_LISTENING:
            return self.stop(run_postprocess=False)
        with self._lock:
            self._writer = None
            self._run_dir = None
            self._phase = PHASE_IDLE
        return {"ok": True, "run_dir": str(rd) if rd else None}


# 单例
SESSION = SemiautoSessionManager()
