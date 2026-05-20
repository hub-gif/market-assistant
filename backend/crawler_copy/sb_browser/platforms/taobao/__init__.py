# -*- coding: utf-8 -*-
"""淘宝：**CDP Mode** + **只读**监听 mtop JSON（复制真实 ``getResponseBody``，非本地重放）。浏览器由 ``sb_browser.browsers.get_sb`` 创建。

落盘：``_low_gi_root``（``data/TB``）、``notes_sink``（``sb_cdp_mtop_raw/<运行时间>/``）。

与 ``tb_pc_search`` Playwright 的 ``pw_user_data`` 勿混用目录。
"""
