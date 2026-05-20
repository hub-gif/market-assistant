# -*- coding: utf-8 -*-
"""创建半自动 run_dir（逻辑对齐 pipeline.semiauto_tasks._make_run_dir）。"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from paths import semiauto_base_dir


def make_run_dir(keyword: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_kw = "".join(
        c if (c.isalnum() or c in "-_") else "_" for c in (keyword or "manual")[:32]
    )
    name = f"{ts}_{safe_kw}"
    d = semiauto_base_dir() / name
    d.mkdir(parents=True, exist_ok=True)
    return d.resolve()


def write_run_meta(run_dir: Path, *, keyword: str, capture_mode: str = "chrome_extension") -> None:
    meta = {
        "keyword": (keyword or "manual").strip() or "manual",
        "capture_mode": capture_mode,
        "platform": "jd",
    }
    (run_dir / "run_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def touch_marker(run_dir: Path, name: str) -> None:
    p = run_dir / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()


def marker_exists(run_dir: Path, name: str) -> bool:
    return (run_dir / name).is_file()
