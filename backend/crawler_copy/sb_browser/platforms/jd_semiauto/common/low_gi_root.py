# -*- coding: utf-8 -*-
"""项目根：`LOW_GI_PROJECT_ROOT`，否则推断 market_assistant 根目录。"""
from __future__ import annotations

import os
from pathlib import Path


def _market_assistant_root() -> Path:
    # common/ → jd_semiauto → platforms → sb_browser → crawler_copy → backend → market_assistant
    return Path(__file__).resolve().parents[6]


def low_gi_project_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        p = Path(raw).expanduser().resolve()
    else:
        p = _market_assistant_root().resolve()
    if not p.is_dir():
        raise RuntimeError(f"LOW_GI_PROJECT_ROOT 不是有效目录: {p}")
    return p


def jd_semiauto_data_dir() -> Path:
    return (low_gi_project_root() / "data" / "JD" / "sb_cdp_api_semiauto").resolve()
