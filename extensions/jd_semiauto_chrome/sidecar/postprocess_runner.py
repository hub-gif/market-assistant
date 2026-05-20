# -*- coding: utf-8 -*-
"""调用现有 postprocess.run_parse_semiauto_to_csv（子进程，cwd=crawler_copy）。"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from paths import crawler_copy_dir, python_executable


def run_parse_semiauto_to_csv(run_dir: Path) -> None:
    cc = crawler_copy_dir()
    if not cc.is_dir():
        raise FileNotFoundError(f"crawler_copy 不存在: {cc}")
    cmd = [
        python_executable(),
        "-m",
        "sb_browser.platforms.jd_semiauto.postprocess.run_parse_semiauto_to_csv",
        "--dir",
        str(run_dir.resolve()),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(cc),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=1800.0,
    )
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-1500:]
        raise RuntimeError(f"JSON→CSV 失败 (rc={proc.returncode}): {tail}")
