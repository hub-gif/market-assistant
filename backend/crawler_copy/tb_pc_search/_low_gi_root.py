# -*- coding: utf-8 -*-
"""
数据工作区根目录：与京东侧相同，由 ``LOW_GI_PROJECT_ROOT`` 或本仓库根定位；
淘宝/天猫落盘使用 ``<项目根>/data/TB/``，与 ``data/JD/`` 并列，避免混放。
"""
from __future__ import annotations

import os
from pathlib import Path


def _market_assistant_root() -> Path:
    """本文件位于 backend/crawler_copy/tb_pc_search/_low_gi_root.py → 上溯 3 级为 MA 根。"""
    return Path(__file__).resolve().parents[3]


def low_gi_project_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        p = Path(raw).expanduser().resolve()
    else:
        p = _market_assistant_root().resolve()
    if not p.is_dir():
        raise RuntimeError(f"LOW_GI_PROJECT_ROOT 不是有效目录: {p}")
    return p


def tb_data_dir() -> Path:
    """``data/TB`` 根目录；批次目录建议为 ``data/TB/pipeline_runs/<时间戳>_<主题>/`` 等，与京东侧命名习惯对齐。"""
    p = (low_gi_project_root() / "data" / "TB").resolve()
    return p
