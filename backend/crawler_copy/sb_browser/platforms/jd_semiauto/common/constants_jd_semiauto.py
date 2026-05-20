# -*- coding: utf-8 -*-
"""京东半自动默认参数：**独立**于淘宝 ``taobao_semiauto.common.constants_taobao_semiauto``。"""
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

# 存活标签 pending 合计 **严格大于** 该值则先删非保留 URL，仍超限再全清（见 SEMI_JD_PENDING_BURST_PROTECT_URL_SUBSTR）。
# ≤0 关闭。
SEMI_JD_PENDING_BURST_CLEAR_AT: int = 16

# pending 暴增时先删「非核心」条目；URL（小写匹配）中含以下任一子串的 **延后删除**，
# 减少商详 wareBusiness 等主包在未拉正文前被误清（仍可在第二轮全清兜底）。
SEMI_JD_PENDING_BURST_PROTECT_URL_SUBSTR: tuple[str, ...] = (
    "pc_detailpage_warebusiness",
    "detailpage_getwarebusiness",
    "getitemdetail",
)

# 监听轮询间隔（秒）；新标签 HTTP 兜底扫表间隔（秒）；状态打印间隔（秒）；最大捕获条数
SEMI_JD_LISTEN_POLL_SEC: float = 0.85
SEMI_JD_NEW_TAB_SCAN_SEC: float = 0.85   # 多标签：/json/list 补挂监听；可与 LISTEN_POLL 同频
SEMI_JD_NEW_TAB_HTTP_SCAN_MIN_SEC: float = 0.35  # flows 下限，勿固定 1.0 以免拖慢新标签挂载
SEMI_JD_LISTEN_STATUS_EVERY_SEC: float = 5.0
SEMI_JD_MAX_CAPTURES: int = 1200

# 关页后 TargetDestroyed 若未触发，_tab_sessions 会泄漏；每到 HTTP 扫表时用 /json/list 对账剔除。
SEMI_JD_PRUNE_STALE_TABS_WITH_JSON_LIST: bool = True

# finalize_all 内会话数 ≥ 该值时，每处理 N 个打一条进度（避免十几分钟无终端输出）
SEMI_JD_FINALIZE_PROGRESS_EVERY: int = 8

# 落盘
SEMI_JD_SAVE_JSON: bool = True
SEMI_JD_SAVE_RUN_DIR_BY_TIME: bool = True
