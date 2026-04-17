# -*- coding: utf-8 -*-
"""
在爬虫目录下直接执行时的入口：将 ``backend`` 加入 ``sys.path`` 后调用
``pipeline.competitor_report.jd_report``（与 ``python -m pipeline.competitor_report.jd_report`` 一致）。

推荐在 ``backend`` 目录执行::

  python -m pipeline.competitor_report.jd_report
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend = Path(__file__).resolve().parents[2]
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from pipeline.competitor_report import jd_report as _jd  # noqa: E402

if __name__ == "__main__":
    _jd.main()
