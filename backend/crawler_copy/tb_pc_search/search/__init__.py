# -*- coding: utf-8 -*-
"""
搜索/商品**列表页**相关：关键词或类目下的列表、分页、字段解析与落盘。

首版可在此放列表页 Playwright/Requests 等入口脚本，与 ``detail/`` 等子包平级再拆。

共享的 mtop 实现见上级目录包 ``../mtop/``；入口需将 ``tb_pc_search`` 加入 ``sys.path`` 后 ``import mtop``。
推荐列表 Cookie 导出与 mtop 均见 ``taobao_mtop_recommend_requests.py`` 顶部常量（``TB_EXPORT_COOKIE_FILE``、``RUN_DEFAULTS``），Playwright Chromium。
"""
