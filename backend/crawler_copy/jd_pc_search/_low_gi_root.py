# -*- coding: utf-8 -*-
"""
数据工作区根目录：流水线与爬虫副本在其下读写 ``data/JD/`` 等。

- 若已设置 ``LOW_GI_PROJECT_ROOT``（如 Django settings 或 ``market_assistant/.env``），使用该路径。
- 未设置时默认为 **本仓库根**（``market_assistant``），便于独立克隆后无需再指向上级目录。
"""
from __future__ import annotations

import os
from pathlib import Path


def _market_assistant_root() -> Path:
    """本文件位于 backend/crawler_copy/jd_pc_search/_low_gi_root.py → 上溯 3 级为 MA 根。"""
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
