# -*- coding: utf-8 -*-
"""京东半自动默认参数：**独立**于淘宝 ``constants_semiauto``。"""
from __future__ import annotations

# CDP activate 首开（勿点地址栏以免叠新标签）
SEMI_JD_DEFAULT_LANDING_URL = "https://www.jd.com/"

# 含 /api?（列表）、/?（商详 GET）、client.action（评论等 POST）
SEMI_JD_LISTEN_URL_CONTAINS: tuple[str, ...] = (
    "api.m.jd.com/api",
    "api.m.jd.com/?",
    "api.m.jd.com/client.action",
)

# CDP 落地页就绪后：是否阻塞终端等用户登录后按回车再挂监听
SEMI_JD_PAUSE_FOR_LOGIN_CONFIRM: bool = True

# activate_cdp_mode 后等待首屏加载（秒）
SEMI_JD_POST_ACTIVATE_SLEEP_SEC: float = 3.0

# finalize_json_reads_blocking 参数
SEMI_JD_FINALIZE_OVERALL_TIMEOUT_SEC: float = 10.0   # 整体上限
SEMI_JD_FINALIZE_PER_SEND_TIMEOUT_SEC: float = 3.0   # 单次 getResponseBody 上限

# 存活标签 pending 条目合计 **严格大于** 该值时清空（设为 17 即 pending≥18 时清空，
# 若为 16 即 pending≥17）。每轮 finalize 开始与结束前各检查一次，避免逐 tab finalize 中途再次堆满。
# ≤0 关闭。
SEMI_JD_PENDING_BURST_CLEAR_AT: int = 17

# 监听轮询间隔（秒）；新标签扫描间隔（秒）；状态打印间隔（秒）；最大捕获条数
SEMI_JD_LISTEN_POLL_SEC: float = 0.85
SEMI_JD_NEW_TAB_SCAN_SEC: float = 0.85   # 多标签：每隔多少秒检查一次新开标签
SEMI_JD_LISTEN_STATUS_EVERY_SEC: float = 5.0
SEMI_JD_MAX_CAPTURES: int = 1200

# 关页后 TargetDestroyed 若未触发，_tab_sessions 会泄漏；每到 HTTP 扫表时用 /json/list 对账剔除。
SEMI_JD_PRUNE_STALE_TABS_WITH_JSON_LIST: bool = True

# finalize_all 内会话数 ≥ 该值时，每处理 N 个打一条进度（避免十几分钟无终端输出）
SEMI_JD_FINALIZE_PROGRESS_EVERY: int = 8

# 落盘
SEMI_JD_SAVE_JSON: bool = True
SEMI_JD_SAVE_RUN_DIR_BY_TIME: bool = True
