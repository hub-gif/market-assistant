# -*- coding: utf-8 -*-
"""
兼容入口：竞品报告实现见 ``pipeline.competitor_report.jd_report``。

命令行：``python -m pipeline.jd_competitor_report``；爬虫目录下的 ``jd_competitor_report.py`` 仍转发至此。
"""
from __future__ import annotations

import pipeline.competitor_report.jd_report as _jd

# ``from jd_report import *`` 不会绑定以下划线开头的名称；外部常以
# ``import pipeline.jd_competitor_report as jcr`` 再访问 ``jcr._read_csv_rows`` 等。
# 这里把实现模块中除模块身份相关 dunder 以外的符号全部同步到本模块，避免再漏导出。
_SKIP = frozenset(
    {
        "__name__",
        "__doc__",
        "__file__",
        "__package__",
        "__loader__",
        "__spec__",
        "__path__",
        "__cached__",
        "__builtins__",
        "__annotations__",
    }
)
for _k, _v in vars(_jd).items():
    if _k in _SKIP or (_k.startswith("__") and _k.endswith("__")):
        continue
    globals()[_k] = _v

if __name__ == "__main__":
    _jd.main()
