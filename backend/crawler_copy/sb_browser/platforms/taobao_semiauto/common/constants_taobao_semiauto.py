# -*- coding: utf-8 -*-
"""淘宝半自动默认参数（Playwright 线）；监听口径可按采集目标在下列常量收紧/扩展。"""
from __future__ import annotations

SEMI_DEFAULT_LANDING_URL = "https://www.taobao.com/"

# URL 级粗筛：`api_capture_kind` 与监听共用。
# 含天猫商详场景的 MTOP：评论等接口常为 ``h5api.m.tmall.com``（与 ``h5api.m.taobao.com`` 并列）。
SEMI_PLAYWRIGHT_JSON_HOST_HINTS: tuple[str, ...] = (
    "h5api.m.taobao.com",
    "h5api.m.tmall.com",
    "guide-acs.m.taobao.com",
)

# 命中 host 仍忽略的 URL 片段（减少噪声）
SEMI_PLAYWRIGHT_URL_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    "showTypeControl",
    "aiNavigation",
)

# 落盘 HTML/大文本时 UTF-8 字节上限（避免单文件过大）
SEMI_MAX_BODY_TEXT_STORE_BYTES: int = 900_000

# --- 列表（目标列表）：在满足 api + URL path 且 data 含 itemsArray 的前提下 ---
# 若 URL/解析体含下列片段则视为「非目标列表场景」→ 不归为 list（常为 mtop）。
# 例：与主搜同接口的 downSideRecommend、hover 浮层、猜你喜欢等（见 data/TB/sample/list.txt 的 qSource）
SEMI_TB_LIST_CAPTURE_NOISE_MARKERS: tuple[str, ...] = (
    "downsiderecommend",
    "search_downsiderecommend",
    "hoveritem",
)

# --- 列表 api/url：还须 ``parsed.data.itemsArray`` 存在（见 api_capture_kind）；且 **未** 命中 NOISE ---
SEMI_TB_LIST_CAPTURE_APIS_EXACT: tuple[str, ...] = (
    "mtop.relationrecommend.wirelessrecommend.recommend",
)
SEMI_TB_LIST_CAPTURE_URL_SUBSTRINGS: tuple[str, ...] = (
    "mtop.relationrecommend.wirelessrecommend.recommend",
)

# --- 评论：精确 api 或 URL/api 子串；``parsed['data']`` 子树须命中下列 **任一** 字段（对照 data/TB/sample/commant.txt） ---
SEMI_TB_COMMENT_PAYLOAD_MARKER_KEYS: tuple[str, ...] = (
    "rateList",
)

SEMI_TB_COMMENT_CAPTURE_APIS_EXACT: tuple[str, ...] = (
    "mtop.taobao.rate.detaillist.get",
)
SEMI_TB_COMMENT_API_SUBSTRINGS: tuple[str, ...] = (
    "rate.detaillist",
    "detaillist.get",
    "mtop.taobao.rate.",
)

SEMI_TB_COMMENT_CAPTURE_URL_SUBSTRINGS: tuple[str, ...] = (
    "rate.detaillist",
    "detaillist.get",
    "mtop.taobao.rate",
)

# --- 商详主文档 ---
SEMI_TB_DETAIL_HOST_SUFFIXES_REQUIRE_PATH: tuple[str, ...] = (
    "item.taobao.com",
    "detail.tmall.com",
)
SEMI_TB_DETAIL_HOST_SUFFIXES_LOOSE: tuple[str, ...] = (
    "npcitem.taobao.hk",
    "npcitem.taobao.com",
)
SEMI_TB_DETAIL_PATH_SNIPPETS: tuple[str, ...] = (
    "/item.htm",
    "/item_o.htm",
    "/detail.htm",
)

# 商详页 HTML 预读窗口（字节级截断在 handler 内按字符切片近似）
SEMI_TB_DETAIL_HTML_PEEK_CHARS: int = 524_288
