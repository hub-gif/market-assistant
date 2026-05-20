# -*- coding: utf-8 -*-
"""
小红书平台专用：项目根解析与 ``data/XHS/`` 路径，与京东 ``jd_pc_search/_low_gi_root``、淘宝 ``tb_pc_search/_low_gi_root`` 分列，避免混入 ``sb_browser`` 根目录。

``LOW_GI_PROJECT_ROOT`` 未设置时为 **market_assistant** 仓库根。
"""
from __future__ import annotations

import os
from pathlib import Path


def _market_assistant_root() -> Path:
    """本文件深度 ``.../sb_browser/platforms/xiaohongshu/`` → 上溯 **5** 级为 MA 根。"""
    return Path(__file__).resolve().parents[5]


def low_gi_project_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        p = Path(raw).expanduser().resolve()
    else:
        p = _market_assistant_root().resolve()
    if not p.is_dir():
        raise RuntimeError(f"LOW_GI_PROJECT_ROOT 不是有效目录: {p}")
    return p


def xhs_data_dir() -> Path:
    """``<项目根>/data/XHS``。"""
    return (low_gi_project_root() / "data" / "XHS").resolve()
