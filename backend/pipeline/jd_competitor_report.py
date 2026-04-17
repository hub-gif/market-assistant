# -*- coding: utf-8 -*-
"""
兼容入口：竞品报告实现见 ``pipeline.competitor_report.jd_report``。

命令行：``python -m pipeline.jd_competitor_report``；爬虫目录下的 ``jd_competitor_report.py`` 仍转发至此。
"""
from __future__ import annotations

from pipeline.competitor_report.jd_report import *  # noqa: F403, F401
from pipeline.competitor_report.jd_report import (  # noqa: F401
    main,
    # ``import *`` 不导出以下划线开头的名称；测试与调试会经本兼容入口访问这些辅助函数。
    _comment_lines_with_product_context,
    _comment_sentiment_lexicon,
    _consumer_feedback_by_matrix_group,
    _counter_mix_top_rows_with_remainder,
    _merged_rows_grouped_for_matrix,
    _sku_to_matrix_group_map,
    _structure_names_for_pie_counter,
)

if __name__ == "__main__":
    main()
