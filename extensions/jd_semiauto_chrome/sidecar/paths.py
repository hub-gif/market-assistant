# -*- coding: utf-8 -*-
"""项目路径（对齐 pipeline.semiauto_tasks / settings.LOW_GI_PROJECT_ROOT）。"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def python_executable() -> str:
    """优先使用项目 .venv，与 runserver 环境一致。"""
    root = project_root()
    for rel in (
        ".venv/Scripts/python.exe",
        ".venv/bin/python",
        "backend/.venv/Scripts/python.exe",
    ):
        p = root / rel.replace("/", os.sep)
        if p.is_file():
            return str(p)
    return sys.executable


def project_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        return Path(raw).expanduser().resolve()
    # extensions/jd_semiauto_chrome/sidecar -> market_assistant
    return Path(__file__).resolve().parents[3]


def jd_data_root() -> Path:
    return project_root() / "data" / "JD"


def semiauto_base_dir() -> Path:
    return jd_data_root() / "sb_cdp_api_semiauto"


def crawler_copy_dir() -> Path:
    return project_root() / "backend" / "crawler_copy"


def manage_py() -> Path:
    return project_root() / "backend" / "manage.py"


def run_dir_relative_to_jd(run_dir: Path) -> str:
    """供 manage.py ingest 使用的相对 data/JD 路径。"""
    rd = run_dir.expanduser().resolve()
    jd = jd_data_root().resolve()
    try:
        return str(rd.relative_to(jd)).replace("\\", "/")
    except ValueError:
        return str(rd)
