# -*- coding: utf-8 -*-
"""本地 sidecar：完整半自动流程 + 落盘（状态在 sidecar 进程内，扩展侧用 storage 镜像）。"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from .semiauto_session import PHASE_LISTENING, SESSION, SessionSnapshot
    from .writer import RunDirWriter, validate_run_dir_under_jd
except ImportError:
    from semiauto_session import PHASE_LISTENING, SESSION, SessionSnapshot
    from writer import RunDirWriter, validate_run_dir_under_jd

_DEFAULT_PORT = 8765
_CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    for k, v in _CORS.items():
        handler.send_header(k, v)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _status_body(snap: SessionSnapshot) -> dict[str, Any]:
    listening = snap.phase == PHASE_LISTENING
    return {
        "ok": True,
        "phase": snap.phase,
        "active": listening,
        "listening": listening,
        "run_dir": snap.run_dir,
        "keyword": snap.keyword,
        "job_id": snap.job_id,
        "error_message": snap.error_message,
        "counts": snap.counts,
    }


class SidecarHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("JD_SEMIAUTO_SIDECAR_VERBOSE", "").strip() in ("1", "true", "yes"):
            super().log_message(fmt, *args)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        for k, v in _CORS.items():
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/health", "/job/status"):
            _json_response(self, 200, _status_body(SESSION.snapshot()))
            return
        _json_response(self, 404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        payload = self._read_json()
        if payload is None:
            return

        if path == "/job/start":
            self._job_start(payload)
        elif path == "/job/confirm-login":
            self._job_confirm_login()
        elif path == "/job/stop":
            self._job_stop(payload)
        elif path == "/capture/batch":
            self._capture_batch(payload)
        elif path == "/session/start":
            self._legacy_start(payload)
        elif path == "/session/stop":
            self._legacy_stop()
        else:
            _json_response(self, 404, {"ok": False, "error": "not_found"})

    def _read_json(self) -> dict[str, Any] | None:
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw_body.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            _json_response(self, 400, {"ok": False, "error": "invalid_json"})
            return None

    def _job_start(self, payload: dict[str, Any]) -> None:
        keyword = str(payload.get("keyword") or "").strip()
        if not keyword:
            _json_response(self, 400, {"ok": False, "error": "keyword_required"})
            return
        create_job = payload.get("create_django_job", True)
        if isinstance(create_job, str):
            create_job = create_job.lower() not in ("0", "false", "no")
        run_dir = str(payload.get("run_dir") or "").strip() or None
        try:
            out = SESSION.start(
                keyword=keyword,
                run_dir=run_dir,
                create_django_job=bool(create_job),
            )
            _json_response(self, 200, {"ok": True, **_status_body(SESSION.snapshot()), **out})
        except Exception as e:
            _json_response(self, 400, {"ok": False, "error": str(e)})

    def _job_confirm_login(self) -> None:
        try:
            SESSION.confirm_login()
            _json_response(self, 200, {"ok": True, **_status_body(SESSION.snapshot())})
        except Exception as e:
            _json_response(self, 409, {"ok": False, "error": str(e)})

    def _job_stop(self, payload: dict[str, Any]) -> None:
        run_pp = payload.get("run_postprocess", True)
        if isinstance(run_pp, str):
            run_pp = run_pp.lower() not in ("0", "false", "no")
        try:
            out = SESSION.stop(run_postprocess=bool(run_pp))
            body = {"ok": True, **_status_body(SESSION.snapshot()), **out}
            _json_response(self, 200, body)
        except Exception as e:
            _json_response(self, 409, {"ok": False, "error": str(e)})

    def _capture_batch(self, payload: dict[str, Any]) -> None:
        items = payload.get("items")
        if not isinstance(items, list):
            _json_response(self, 400, {"ok": False, "error": "items_must_be_list"})
            return
        run_dir = str(payload.get("run_dir") or "").strip() or None
        try:
            result = SESSION.write_batch(items, run_dir=run_dir)
            snap = SESSION.snapshot()
            _json_response(
                self,
                200,
                {"ok": True, **result, **_status_body(snap)},
            )
        except Exception as e:
            _json_response(self, 409, {"ok": False, "error": str(e)})

    def _legacy_start(self, payload: dict[str, Any]) -> None:
        run_dir_s = str(payload.get("run_dir") or "").strip()
        kw = str(payload.get("keyword") or "manual").strip()
        if not run_dir_s:
            _json_response(self, 400, {"ok": False, "error": "run_dir_required"})
            return
        try:
            SESSION.legacy_session_start(run_dir_s, kw)
            _json_response(self, 200, {"ok": True, **_status_body(SESSION.snapshot())})
        except Exception as e:
            _json_response(self, 400, {"ok": False, "error": str(e)})

    def _legacy_stop(self) -> None:
        try:
            SESSION.legacy_session_stop()
            _json_response(self, 200, {"ok": True, **_status_body(SESSION.snapshot())})
        except Exception as e:
            _json_response(self, 409, {"ok": False, "error": str(e)})


def run_server(host: str = "127.0.0.1", port: int | None = None) -> None:
    p = port if port is not None else int(os.environ.get("JD_SEMIAUTO_SIDECAR_PORT", str(_DEFAULT_PORT)))
    httpd = ThreadingHTTPServer((host, p), SidecarHandler)
    print(f"[jd_semiauto sidecar] http://{host}:{p}  (Ctrl+C 停止)", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[jd_semiauto sidecar] 已停止", flush=True)


if __name__ == "__main__":
    run_server()
