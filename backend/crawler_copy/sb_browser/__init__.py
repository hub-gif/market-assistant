# -*- coding: utf-8 -*-
"""
**SeleniumBase UC** 包：``browsers`` 建会话，``platforms`` 跑站点。

* 仅开浏览器 / 导出 Cookie：改 ``sb_browser/browsers/session.py`` 顶部 ``SB_*`` 后运行该文件。
* 各平台：``platforms/<站点>/run_*.py``，内部 ``get_sb()``。
* 淘宝：默认 ``data/TB/sb_cdp_mtop_raw/<YYYYMMDDHHMMSS>/``（见 ``platforms/taobao/notes_sink``）。
* CDP JSON 监听（``activate_cdp_mode`` 后）：``cdp_json_listen``；
  ChromeDriver ``performance`` 轮询（``uc_cdp_events``）：``performance_response_listen``。

与 ``tb_pc_search`` Playwright 的 ``pw_user_data`` **勿混用目录**。
"""
