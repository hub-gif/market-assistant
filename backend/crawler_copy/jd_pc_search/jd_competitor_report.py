# -*- coding: utf-8 -*-
"""
兼容入口（历史路径）：竞品报告实现已迁至 ``pipeline.jd_competitor_report``。

- 推荐：在 ``backend`` 目录执行 ``python -m pipeline.jd_competitor_report``
- 本文件：将 ``backend`` 加入 ``sys.path`` 后转发至 ``pipeline`` 模块，便于仍在爬虫目录下执行 ``python jd_competitor_report.py`` 的旧习惯。
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend = Path(__file__).resolve().parents[2]
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from pipeline.jd_competitor_report import *  # noqa: F403
