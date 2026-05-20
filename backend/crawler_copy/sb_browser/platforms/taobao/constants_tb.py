# -*- coding: utf-8 -*-
"""淘宝 Web：CDP ``sb.cdp.*`` 用的落地页、搜索框与要监听的接口 URL 片段占位。"""

# 先落首页再输入搜索，Cookie/Referer 更接近真实访问
TB_DEFAULT_URL = "https://www.taobao.com/"

# ---------------------------------------------------------------------------
# mtop 搜索推荐（与 tb_pc_search / 浏览器 Network 实测对齐）
# ---------------------------------------------------------------------------
# 实测：Request Method GET，Status 200，``type=jsonp`` & ``dataType=jsonp``，
# Host ``h5api.m.taobao.com``，path 含 ``.../recommend/2.0/?...``（data 内为 SRP 参数，含 q、page 等）。
# CDP 只按「URL 子串」过滤，这里用 **含协议与 /2.0/** 的前缀，减少误匹配其它域下同名 api 串。
TB_MTOP_LISTEN_URL_CONTAINS = (
    "https://h5api.m.taobao.com/h5/mtop.relationrecommend.wirelessrecommend.recommend/2.0/"
)
# 同上 path（GET/jsonp）还会出现「导航/控件」类调用，与其它 SRP data 混在一起；按其 query ``data`` 里稳定出现的片段排除
TB_MTOP_URL_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    "showTypeControl",
    "aiNavigation",
)

# 落盘时只保留与「主 SRP 样式大包」一类的 JSON：已成功解析且 ``parsed['data']`` 含下列键（小包如预加载无 ``iconStyle``）
TB_MTOP_SAVE_REQUIRE_DATA_KEYS: tuple[str, ...] = ("iconStyle",)
TB_MTOP_SAVE_ONLY_MAIN_BUNDLE: bool = True
# True：``data/TB/sb_cdp_mtop_raw/<YYYYMMDDHHMMSS>/``；False：文件直接铺在 ``sb_cdp_mtop_raw/`` 根下
TB_MTOP_SAVE_RUN_DIR_BY_TIME: bool = True

# JSON 落盘成功后，在同目录导出 ``mtop_items_<关键词>_t<stamp>.csv``（``parsed.data.itemsArray``）
TB_MTOP_EXPORT_CSV_AFTER_JSON: bool = True

# 淘宝首页 / 搜索入口：主站搜索框 #q；若你换到 s.taobao.com 搜索结果页可改本处
TB_SELECTOR_SEARCH_INPUT = "#q"  # TODO 视实际页面可调
TB_SELECTOR_SEARCH_BUTTON = ""  # TODO；留空则回车提交

# ---------------------------------------------------------------------------
# 人机节奏：``flows`` 里 ``sb.sleep`` / 部分 CDP ``timeout`` 用 ``random.uniform(min, max)``（秒）
# ---------------------------------------------------------------------------
# 全局节奏倍率：乘到多数「等待/翻页/落地/搜索前后」的 (min,max) 上；``1.1``≈整体慢 10%，略降频。
# 不作用于 ``TB_WAIT_*_TIMEOUT``（等待元素仍按原值，避免把超时撑得过长）。
# 不作用于贝塞尔 ``TB_HUMANLIKE_MOUSE_STEP_PAUSE`` / ``PRESS``（避免轨迹忽快忽慢失真）。
TB_PACING_SCALE: float = 1.0
# 翻页点击后，在 ``TB_SLEEP_PAGER_AFTER_NEXT_CLICK`` 之外再随机多歇一段（仅多页时生效；单页为 0 则无感）
TB_PAGER_INTER_PAGE_EXTRA_SLEEP: tuple[float, float] = (0.18, 0.52)

TB_SLEEP_AFTER_LANDING_ACTIVATE: tuple[float, float] = (2.1, 3.5)
TB_SLEEP_BEFORE_FIRST_SEARCH_ACTION: tuple[float, float] = (0.7, 1.55)
TB_SLEEP_AFTER_FOCUS_SEARCH_INPUT_PREP: tuple[float, float] = (0.22, 0.54)
TB_SLEEP_AFTER_CLICK_SEARCH_INPUT: tuple[float, float] = (0.17, 0.44)
TB_SLEEP_AFTER_TYPING_KEYWORD: tuple[float, float] = (0.38, 1.08)
TB_SLEEP_AFTER_SUBMIT_KEYS: tuple[float, float] = (1.85, 2.95)
TB_SLEEP_AFTER_SUBMIT_BUTTON: tuple[float, float] = (1.42, 2.38)
TB_WAIT_SEARCH_INPUT_VISIBLE_TIMEOUT: tuple[float, float] = (17.5, 24.8)
TB_WAIT_SEARCH_BUTTON_VISIBLE_TIMEOUT: tuple[float, float] = (3.2, 5.9)
# ``press_keys`` / ``clear_input`` 里 ``select`` 用的超时（秒）；``None`` 用 SeleniumBase 默认 ``SMALL_TIMEOUT``
TB_CDP_PRESS_KEYS_TIMEOUT_SEC: float | None = None

# ---------------------------------------------------------------------------
# 搜索结果翻页（进入 SRP 后点「下一页」；CDP 监听整段保持，`finalize` 每页各拉一次）
# ---------------------------------------------------------------------------
# 总页数（含第 1 页）；1 = 仍只采首屏结果不打翻页。
TB_PAGER_MAX_PAGES: int = 2
TB_SLEEP_PAGER_AFTER_NEXT_CLICK: tuple[float, float] = (2.3, 4.05)
# 非空则优先该 selector；否则依次尝试 ``TB_PAGER_NEXT_SELECTORS``（PC 搜索结果底部分页）。
TB_SELECTOR_PAGER_NEXT = "button.next-btn.next-medium.next-btn-normal.next-pagination-item.next-next>span.next-btn-helper"
TB_PAGER_NEXT_SELECTORS: tuple[str, ...] = (
    "#mainsrp-pager li.next:not(.next-disabled) a",
    '#mainsrp-pager ul li[class*="next"]:not([class*="next-disabled"]) a',
    "li.next:not(.next-disabled) a",
)
TB_WAIT_PAGER_NEXT_VISIBLE_TIMEOUT: tuple[float, float] = (8.5, 12.9)

# ---------------------------------------------------------------------------
# 类人操作（贝塞尔鼠标轨迹 + 列表页随机滚动）；模块 ``sb_browser.cdp_human_motion``
# ---------------------------------------------------------------------------
TB_HUMANLIKE_ENABLED: bool = True
TB_HUMANLIKE_AFTER_LANDING_SCROLL: bool = True
TB_HUMANLIKE_AFTER_SEARCH_BROWSE_SCROLL: bool = True
TB_HUMANLIKE_LISTING_SCROLL_BURSTS: tuple[int, int] = (2, 5)
TB_HUMANLIKE_SCROLL_PAUSE: tuple[float, float] = (0.09, 0.36)
TB_HUMANLIKE_SCROLL_DELTA_PX: tuple[int, int] = (96, 412)
TB_HUMANLIKE_MOUSE_SEGMENTS: tuple[int, int] = (16, 30)
TB_HUMANLIKE_MOUSE_STEP_PAUSE: tuple[float, float] = (0.007, 0.034)
TB_HUMANLIKE_MOUSE_PRESS_PAUSE: tuple[float, float] = (0.038, 0.12)
# ``scrollIntoView`` 使用 ``smooth`` 的概率（否则 ``instant``）；平滑后会多歇一会等滚动结束
TB_HUMANLIKE_SCROLLINTO_SMOOTH_CHANCE: float = 0.32
TB_HUMANLIKE_SCROLL_SMOOTH_CHANCE: float = 0.36
# 单次目标滚动像素拆成多段小段滑动的概率（像犹豫/精读）
TB_HUMANLIKE_SCROLL_MULTI_STEP_CHANCE: float = 0.44
TB_HUMANLIKE_SCROLL_MICRO_STEPS_MAX: int = 5
# 点搜索框 / 翻页前额外迟疑（秒）
TB_HUMANLIKE_HESITATION_BEFORE_CLICK: tuple[float, float] = (0.04, 0.31)
# 长词分段 ``press_keys``，段间停顿；短词不切分
TB_HUMANLIKE_TYPING_CHUNK_CHARS: tuple[int, int] = (2, 5)
TB_HUMANLIKE_TYPING_BETWEEN_CHUNK_SLEEP: tuple[float, float] = (0.035, 0.24)
# 贝塞尔开始前、``scrollIntoView`` 之后的小停顿
TB_HUMANLIKE_MOUSE_PRE_PATH_SLEEP: tuple[float, float] = (0.02, 0.19)
