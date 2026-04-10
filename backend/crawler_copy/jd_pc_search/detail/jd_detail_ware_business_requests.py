# -*- coding: utf-8 -*-
"""
京东 PC 详情 ``pc_detailpage_wareBusiness``（appid=pc-item-soa）。

**唯一链路**：打开 ``item.jd.com/{sku}.html``，可选点击底部锚点栏「商品详情」
（``#SPXQ-tab-column``，SPXQ=商品详情），再拦截页面发起的 ``api.m.jd.com`` 请求
（不经 Node 拼 ``h5st``，与真实浏览器一致）。开关见 ``CLICK_PRODUCT_DETAIL_TAB``。

**定位 tab 相关 JSON**：切到「商品详情」后，左侧 ``left-tabs-content`` 里 ``#SPXQ-title`` 下方常见 **整块 HTML 注入**（参数表 ``_scoped_*``、SSD 图文 ``ssd-module-wrap``、``#detail-main`` / ``#related-layout-head`` 等），
随后 Network 里会多出大量 **图片/CSS**（如 ``img30.360buyimg.com``、``.avif`` / ``background-image``），这些才是图文详情本体，**不是**一条可替代的「详情 JSON」。
页内脚本可能对 ``api.m.jd.com/client.action`` 发 ``mGetsByColor``（搭配 ``h5st``）等，用于模块内 **凑单/关联 SKU 价签**，与主图长页是两类数据。
结构化字段（品牌、编号、类目等）仍以本脚本拦截的 ``pc_detailpage_wareBusiness`` 为主；若要对齐 DevTools，设 ``LOG_API_M_JD_TRACE=True`` 看各阶段 ``functionId`` 与 URL。

未命中接口、HTTP 非 200、正文空、无法解析 JSON 或解析后业务字段全空时，按 ``FETCH_MAX_ATTEMPTS`` /
``FETCH_RETRY_DELAY_SEC`` 自动重试（``fetch_ware_business`` 的 ``max_attempts`` / ``retry_delay_sec``）。

落盘 / 单 SKU 标准输出时，可对 JSON **缩进 + 递归键排序**（``NORMALIZE_WARE_JSON`` / ``SORT_JSON_KEYS``），便于阅读与 diff。

**输出路径（均在文件顶部用变量配置）**：
- 设 ``OUTPUT_SKU_AND_BODY_IMAGES_ONLY=True``（默认）时，**仅搜集** ``skuId``、``detail_body_ingredients``、``detail_body_ingredients_source_url``（视觉命中时的图源）：不写 ware 原始 JSON；``OUT_PARSED`` / ``OUT_PARSED_DIR`` / ``OUT_PARSED_CSV`` 含上述列；批量时可不配 ``OUT_DIR``（配 ``OUT_PARSED_CSV`` 或 ``OUT_PARSED_DIR`` 即可）。
- 若 ``OUTPUT_SKU_AND_BODY_IMAGES_ONLY=False``：**原始接口 JSON** 单 SKU ``OUT``、批量 ``OUT_DIR`` / ``ware_{sku}.json``；解析扁平含全部 ``detail_*``；汇总表 ``OUT_PARSED_CSV``。

**解析 API**：``flatten_ware_business`` / ``parse_ware_business_response_text`` / ``ware_parsed_row``，
列 ``detail_body_ingredients`` 为配料表文本（由 ``#detail-main`` 长图经 ``AI_crawler`` 自后向前多模态识别）；列 ``detail_body_ingredients_source_url`` 为**实际用于识别**的那张长图 URL（命中即停）。未配置 API 或识别失败时配料列为原因说明、图源列为空。内部 ``meta["detail_body_image_urls"]`` 仍为全部长图 URL 串，仅供解析用。

Cookie：``../common/jd_cookie.txt``（或配置项 ``COOKIE_FILE`` / ``COOKIE_OVERRIDE``），经 ``add_cookies`` 注入。

依赖: pip install playwright && playwright install chromium（``USE_CHROME=True`` 时用本机 Chrome）

用法: 改下方「运行配置」后执行 ``python jd_detail_ware_business_requests.py``（无命令行参数）。
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# 运行配置（按需改这里）
# ---------------------------------------------------------------------------
# 路径：副本通过 LOW_GI_PROJECT_ROOT 指向「Low GI」根目录（与搜索 / 流水线一致）
_JD_PC_SEARCH = Path(__file__).resolve().parents[1]
if str(_JD_PC_SEARCH) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH))
from _low_gi_root import low_gi_project_root  # noqa: E402

_PROJECT_ROOT = low_gi_project_root()
_PROJECT_DATA = _PROJECT_ROOT / "data" / "JD"

# SKU：单个商品 ID（item.jd.com/{SKU}.html）；与 SKU_FILE 二选一，不可都填或都空
SKU = "100307995056"
# SKU_FILE：每行一个 SKU，# 开头为注释；启用时须清空 SKU，并设置 OUT_DIR
SKU_FILE = ""
# OUT：单 SKU 时 **原始** 接口 JSON；OUTPUT_SKU_AND_BODY_IMAGES_ONLY=True 时不写入。空则 stdout 见下方
OUT = ""
# OUT_DIR：批量写 ware_{sku}.json；极简模式下可不配（配合 OUT_PARSED_CSV / OUT_PARSED_DIR）
OUT_DIR = ""
# OUT_PARSED：单 SKU 时 **解析扁平** 结果；写入 JSON（含 skuId、http_status 与 WARE_BUSINESS_MERGE_FIELDNAMES）；
# 空字符串表示不写解析文件（仅写 OUT 或仅打印）
OUT_PARSED = str(_PROJECT_DATA / "ware_out_parsed.json")
# OUT_PARSED_DIR：批量时目录，每个 SKU 写 ``ware_{sku}_parsed.json``；空表示批量也不落盘解析结果
# 常用：str(_PROJECT_DATA / "ware_parsed")
OUT_PARSED_DIR = ""
# OUT_PARSED_CSV：解析结果汇总表（UTF-8 BOM）；每次运行结束时写入，行数=本次请求的 SKU 数
# （单 SKU、批量均适用）；空表示不写。常用：str(_PROJECT_DATA / "ware_detail_summary.csv")
OUT_PARSED_CSV = str(_PROJECT_DATA / "ware_detail_summary.csv")
# COOKIE_FILE：Cookie 文本路径；空则使用 jd_pc_search/common/jd_cookie.txt
COOKIE_FILE = ""
# COOKIE_OVERRIDE：非空时覆盖文件中的整段 Cookie 请求头
COOKIE_OVERRIDE = ""
# TIMEOUT_SEC：打开商品页与等待拦截的总超时（秒）
TIMEOUT_SEC = 45.0
# GOTO_WAIT_UNTIL：``page.goto`` 的 wait_until。京东详情页常因长连接/埋点迟迟不触发 ``load``，默认用 ``domcontentloaded``；若需严格等全部资源可改 ``load``。
GOTO_WAIT_UNTIL = "domcontentloaded"
# FETCH_MAX_ATTEMPTS：未捕获接口、非 200、正文空或解析后无有效字段时最多重试次数
FETCH_MAX_ATTEMPTS = 3
# FETCH_RETRY_DELAY_SEC：相邻两次尝试之间的休眠（秒）
FETCH_RETRY_DELAY_SEC = 2.0
# CLICK_PRODUCT_DETAIL_TAB：True 时在进入商品页后点击「商品详情」tab（#SPXQ-tab-column），便于滚到详情区并触发与 tab 相关的请求
CLICK_PRODUCT_DETAIL_TAB = True
# LOG_API_M_JD_TRACE：True 时在 stderr 列出本次打开商品页过程中每条 api.m.jd.com 响应（阶段+functionId+URL），用于对照 DevTools 找 tab 对应接口
LOG_API_M_JD_TRACE = False
# COLLECT_DETAIL_MAIN_IMAGE_URLS：True 时在点击商品详情 tab 并等待后，从 #detail-main 抽取图文 URL（style 中 background-image、img src、zbViewWeChatMiniImages），
# 补全为 https 后写入 meta（见 DETAIL_BODY_IMAGE_URL_SEPARATOR）；再经多模态写入列 detail_body_ingredients（配料文本）与 detail_body_ingredients_source_url（命中图源）
COLLECT_DETAIL_MAIN_IMAGE_URLS = True
# EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES：True 时 detail_body_ingredients 为配料表文本、detail_body_ingredients_source_url 为命中图源；False 时配料列恒为空、图源列恒为空。需 .env 中 OPENAI_* / LLM_*（见上级目录 AI_crawler.py）
EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES = True
# DETAIL_BODY_IMAGE_URL_SEPARATOR：写入 CSV/JSON 单单元格时的分隔符
DETAIL_BODY_IMAGE_URL_SEPARATOR = "; "
# DETAIL_BODY_IMAGE_URLS_MAX_CHARS：单单元格最大字符数（兼顾 Excel 单元格上限）
DETAIL_BODY_IMAGE_URLS_MAX_CHARS = 31000
# OUTPUT_SKU_AND_BODY_IMAGES_ONLY：True 时落盘/stdout 为 skuId + detail_body_ingredients + detail_body_ingredients_source_url（不写 ware 原始 JSON；CSV 三列）；仍为抓 wareBusiness 打开页面，若已抽到图文 URL 则不再因接口扁平为空而重试
OUTPUT_SKU_AND_BODY_IMAGES_ONLY = True
# HEADED：True 显示浏览器窗口（调页面/登录态）
HEADED = False
# USE_CHROME：True 使用本机已安装的 Google Chrome（channel=chrome），否则用内置 Chromium
USE_CHROME = True
# DEBUG_PAUSE：True 时请求结束后终端按回车再关浏览器
DEBUG_PAUSE = False
# PRETTY_STDOUT：单 SKU 且无 OUT、且 NORMALIZE_WARE_JSON=False 时，是否在终端缩进打印（不排序键）
PRETTY_STDOUT = True
# VERBOSE_HTTP：True 在 stderr 打印本次拦截的 URL/状态/响应体摘要（Cookie 会截断）
VERBOSE_HTTP = False
# HTTP_LOG：非空路径则写入完整 request/response JSON（多 SKU 时为数组）；对照 Network 用
HTTP_LOG = ""  # 例：str(_PROJECT_DATA / "ware_http_log.json")
# VERBOSE_HTTP_BODY_LIMIT：与 VERBOSE_HTTP 合用时，stderr 中响应体最多字符数
VERBOSE_HTTP_BODY_LIMIT = 8000
# NORMALIZE_WARE_JSON：True 时对成功解析的 JSON 规整后写出（缩进 + 可选键排序），失败则仍写原文
NORMALIZE_WARE_JSON = True
# SORT_JSON_KEYS：True 时递归按字典键名排序，便于 diff 与浏览；False 保留接口原始字段顺序
SORT_JSON_KEYS = True
# JSON_INDENT：规整时的缩进空格数；0 表示紧凑单行（仍可能已排序）
JSON_INDENT = 2
# ---------------------------------------------------------------------------

_JD_DIR = Path(__file__).resolve().parent
_JD_PC_SEARCH_DIR = _JD_DIR.parent
if str(_JD_PC_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH_DIR))
_DEFAULT_COOKIE_PATH = (_JD_DIR.parent / "common" / "jd_cookie.txt").resolve()

_JD_DETAIL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
_JD_DETAIL_CONTEXT_EXTRA_HEADERS: dict[str, str] = {
    "sec-ch-ua": (
        '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
    ),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# PC 详情页底部锚点：商品详情（与页面 id 一致，改版时需对照 DOM）
_JD_TAB_PRODUCT_DETAIL_SELECTOR = "#SPXQ-tab-column"


def _jd_function_id_from_api_url(url: str) -> str:
    """从 ``api.m.jd.com?...`` 的 query 取 ``functionId``；无则空串。"""
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get("functionId") or q.get("functionid")
        if not v:
            return ""
        return str(v[0]).strip()
    except Exception:
        return ""


def _print_api_m_jd_trace(rows: list[dict[str, Any]], *, sku_id: str) -> None:
    if not rows:
        print(
            f"[京东] api.m.jd.com 轨迹 sku={sku_id}：未观察到该域名任何响应（Cookie/拦截或接口已迁域）。",
            file=sys.stderr,
        )
        return
    print(
        f"[京东] api.m.jd.com 轨迹 sku={sku_id} 共 {len(rows)} 条（按时间顺序；"
        "after_tab 内无新行则点击 tab 未触发该域名 XHR）",
        file=sys.stderr,
    )
    for i, r in enumerate(rows, 1):
        fid = r.get("functionId") or "(无 query functionId)"
        print(
            f"  {i}. [{r.get('phase')}] HTTP {r.get('status')}  {fid}",
            file=sys.stderr,
        )
        u = (r.get("url") or "")[:900]
        print(f"      {u}", file=sys.stderr)
    print(
        "[京东] 提示：若仍对不上 DevTools，请看 POST 请求的 form/body 里的 functionId；"
        "图文详情也可能在首屏 HTML、iframe 或非 api.m.jd.com 的静态资源。",
        file=sys.stderr,
    )


def _click_jd_product_detail_tab(page: Any, *, timeout_ms: int) -> None:
    """点击「商品详情」锚点 tab；失败仅打日志，不中断（部分模板无此节点）。"""
    cap = max(2_000, min(12_000, int(timeout_ms)))
    try:
        loc = page.locator(_JD_TAB_PRODUCT_DETAIL_SELECTOR)
        loc.wait_for(state="visible", timeout=cap)
        loc.click(timeout=cap)
    except Exception as e:
        print(
            f"[京东] 未点击商品详情 tab（{_JD_TAB_PRODUCT_DETAIL_SELECTOR}）: {e}",
            file=sys.stderr,
        )


# #detail-main 内 style 块中的 background-image:url(...)
_CSS_BG_URL_RE = re.compile(r"url\s*\(\s*([^)]+)\s*\)", re.I)
# zbViewWeChatMiniImages 的 value="a.jpg,b.jpg,..."
_ZB_MINI_VALUE_RE = re.compile(
    r"zbViewWeChatMiniImages[^>]*\bvalue\s*=\s*[\"']([^\"']+)[\"']",
    re.I | re.DOTALL,
)


def _normalize_jd_detail_asset_url(raw: str) -> str:
    """将 //、/sku/jfs、/cms/jfs 等补全为可访问的 https URL。"""
    s = (raw or "").strip()
    if len(s) >= 2 and s[0] in "\"'" and s[-1] == s[0]:
        s = s[1:-1].strip()
    if not s or s.lower().startswith("data:"):
        return ""
    if s.startswith("//"):
        return ("https:" + s)[:900]
    if s.startswith("http://"):
        return ("https://" + s[7:])[:900]
    if s.startswith("https://"):
        return s[:900]
    if s.startswith("/sku/jfs/"):
        return ("https://img30.360buyimg.com" + s)[:900]
    if s.startswith("/cms/jfs/"):
        return ("https://img12.360buyimg.com" + s)[:900]
    if s.startswith("/jfs/"):
        return ("https://img30.360buyimg.com/sku/jfs" + s[4:])[:900]
    if s.startswith("jfs/"):
        return ("https://img30.360buyimg.com/sku/" + s)[:900]
    return s[:900]


def _dedupe_urls_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        u = (u or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _urls_from_detail_main_inner_html(html: str) -> list[str]:
    raw: list[str] = []
    if not (html or "").strip():
        return raw
    for m in _CSS_BG_URL_RE.finditer(html):
        inner = (m.group(1) or "").strip().strip("\"'")
        if inner:
            raw.append(inner)
    zm = _ZB_MINI_VALUE_RE.search(html)
    if zm:
        for part in (zm.group(1) or "").split(","):
            p = part.strip()
            if p:
                raw.append(p)
    for m in re.finditer(
        r"""<img\b[^>]*\bsrc\s*=\s*(['"])(?P<u>.*?)\1""",
        html,
        re.I | re.DOTALL,
    ):
        u = (m.group("u") or "").strip()
        if u:
            raw.append(u)
    for m in re.finditer(r"""<img\b[^>]*\bsrc\s*=\s*([^\s>'"]+)""", html, re.I):
        u = (m.group(1) or "").strip()
        if u:
            raw.append(u)
    return raw


def scrape_detail_main_body_urls_joined(
    page: Any,
    *,
    wait_ms: int = 8_000,
    separator: str | None = None,
    max_chars: int | None = None,
) -> str:
    """
    从当前页 ``#detail-main`` 收集图文资源 URL（SSD 背景图、img、zbViewWeChatMiniImages），
    补全为 https，去重保序后用 ``separator`` 拼成一段字符串（供 CSV 单格）。
    页面须已展示商品详情区（通常需先点 ``#SPXQ-tab-column``）。
    """
    if not COLLECT_DETAIL_MAIN_IMAGE_URLS:
        return ""
    sep = (
        separator
        if separator is not None
        else (DETAIL_BODY_IMAGE_URL_SEPARATOR or "; ")
    )
    cap = max(2000, min(15_000, int(wait_ms)))
    loc = page.locator("#detail-main")
    try:
        loc.wait_for(state="attached", timeout=cap)
    except Exception:
        return ""
    try:
        html = loc.inner_html(timeout=min(5_000, cap))
    except Exception:
        html = ""
    dom_srcs: list[str] = []
    try:
        dom_srcs = page.evaluate(
            """() => {
              const r = document.querySelector('#detail-main');
              if (!r) return [];
              return Array.from(r.querySelectorAll('img'))
                .map(i => (i.currentSrc || i.src || '').trim())
                .filter(Boolean);
            }"""
        )
    except Exception:
        dom_srcs = []
    if not isinstance(dom_srcs, list):
        dom_srcs = []

    candidates = _urls_from_detail_main_inner_html(html) + [str(x) for x in dom_srcs]
    normalized: list[str] = []
    for c in candidates:
        n = _normalize_jd_detail_asset_url(c)
        if not n:
            continue
        low = n.lower()
        if "sku-market-gw.jd.com" in low and low.endswith(".css"):
            continue
        if "list.jd.com" in low or "item.jd.com" in low or "mall.jd.com" in low:
            if not any(
                low.endswith(ext)
                for ext in (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif", ".dpg")
            ):
                continue
        normalized.append(n)

    uniq = _dedupe_urls_preserve_order(normalized)
    joined = sep.join(uniq)
    mxc = (
        int(max_chars)
        if max_chars is not None
        else int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS)
    )
    if mxc > 0 and len(joined) > mxc:
        joined = joined[: mxc - 3] + "..."
    return joined


def _normalize_ware_json_tree(obj: Any, *, sort_keys: bool) -> Any:
    """递归规整：字典可选按键名排序，列表保持元素顺序。"""
    if isinstance(obj, dict):
        pairs = [
            (k, _normalize_ware_json_tree(v, sort_keys=sort_keys))
            for k, v in obj.items()
        ]
        if sort_keys:
            pairs.sort(key=lambda kv: kv[0])
        return dict(pairs)
    if isinstance(obj, list):
        return [_normalize_ware_json_tree(x, sort_keys=sort_keys) for x in obj]
    return obj


def _format_ware_response_text(
    text: str,
    *,
    normalize: bool,
    sort_keys: bool,
    indent: int,
) -> tuple[str, bool]:
    """
    尝试将接口 body 规整为可读 JSON。
    返回 (输出文本, 是否已成功按 JSON 处理)。
    """
    if not normalize or not (text or "").strip():
        return text, False
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return text, False
    obj = _normalize_ware_json_tree(obj, sort_keys=sort_keys)
    if indent > 0:
        out = json.dumps(obj, ensure_ascii=False, indent=indent) + "\n"
    else:
        out = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    return out, True


# 与 pc_detailpage_wareBusiness 响应对应的扁平字段（字符串，缺失为空），顺序即 CSV 建议列序
WARE_BUSINESS_MERGE_FIELDNAMES: tuple[str, ...] = (
    "detail_sku_title",
    "detail_price_final",
    "detail_price_original",
    "detail_purchase_price",
    "detail_shop_name",
    "detail_shop_id",
    "detail_shop_url",
    "detail_vender_id",
    "detail_stock_text",
    "detail_delivery_promise",
    "detail_sku_name",
    "detail_product_id",
    "detail_main_sku_id",
    "detail_page_sku_id",
    "detail_brand",
    "detail_category_path",
    "detail_main_image",
    "detail_product_attributes",
    "detail_belt_banner",
    "detail_csfh_text",
    # 来自 DOM #detail-main（非 wareBusiness JSON）；列语义为「详情长图衍生信息」，常为 URL 串，流水线可替换为配料表文本
    "detail_body_ingredients",
    # 视觉识别命中时：实际用于解析配料的那张详情长图 URL（自后向前首次通过校验）
    "detail_body_ingredients_source_url",
)


def _empty_ware_flat() -> dict[str, str]:
    return {k: "" for k in WARE_BUSINESS_MERGE_FIELDNAMES}


def _strip_htmlish(s: str, *, max_len: int) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = " ".join(t.split()).strip()
    return t[:max_len] if max_len > 0 else t


def _s(obj: Any) -> str:
    if obj is None:
        return ""
    return str(obj).strip()


def _join_url(u: str) -> str:
    u = u.strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    return u


def _jd_product_image_url(path: str) -> str:
    """与搜索侧一致：jfs/ 相对路径补全为可访问 URL。"""
    p = (path or "").strip()
    if not p:
        return ""
    if p.startswith("//"):
        return "https:" + p
    if p.startswith("http://"):
        return "https://" + p[7:]
    if p.startswith("https://"):
        return p[:800]
    if p.startswith("jfs/"):
        return "https://img13.360buyimg.com/n2/s480x480_" + p[:700]
    return p[:800]


def flatten_ware_business(obj: Any) -> dict[str, str]:
    """
    将 ``pc_detailpage_wareBusiness`` 的 JSON 根对象压成扁平字符串字典。
    非 dict 或字段缺失时对应值为空串。
    """
    out = _empty_ware_flat()
    if not isinstance(obj, dict):
        return out

    sh = obj.get("skuHeadVO")
    sh = sh if isinstance(sh, dict) else {}
    out["detail_sku_title"] = _s(sh.get("skuTitle"))[:2000]

    price = obj.get("price")
    price = price if isinstance(price, dict) else {}
    fp = price.get("finalPrice")
    fp = fp if isinstance(fp, dict) else {}
    out["detail_price_final"] = _s(fp.get("price")) or _s(price.get("p"))[:64]
    out["detail_price_original"] = _s(price.get("op")) or _s(price.get("p"))[:64]

    bp = obj.get("bestPromotion")
    bp = bp if isinstance(bp, dict) else {}
    out["detail_purchase_price"] = _s(bp.get("purchasePrice"))[:64]

    ishop = obj.get("itemShopInfo")
    ishop = ishop if isinstance(ishop, dict) else {}
    out["detail_shop_name"] = _s(ishop.get("shopName"))[:500]
    out["detail_shop_id"] = _s(ishop.get("shopId"))[:32]
    out["detail_shop_url"] = _join_url(_s(ishop.get("shopUrl")))[:500]

    pc = obj.get("pageConfigVO")
    pc = pc if isinstance(pc, dict) else {}
    out["detail_vender_id"] = _s(pc.get("venderId"))[:32]
    sk = pc.get("skuid")
    out["detail_page_sku_id"] = _s(sk)[:32]
    cats = pc.get("catName")
    if isinstance(cats, list):
        parts = [_s(x) for x in cats if _s(x)]
        out["detail_category_path"] = " > ".join(parts)[:500]
    src = _s(pc.get("src"))
    if src:
        out["detail_main_image"] = _jd_product_image_url(src)

    mi = obj.get("mainImageVO")
    if isinstance(mi, dict) and not out["detail_main_image"]:
        mia = mi.get("mainImageArea")
        if isinstance(mia, dict):
            iu = _s(mia.get("imageUrl"))
            if iu:
                out["detail_main_image"] = _jd_product_image_url(iu)

    si = obj.get("stockInfo")
    si = si if isinstance(si, dict) else {}
    out["detail_stock_text"] = _strip_htmlish(_s(si.get("stockDesc")), max_len=500)
    if not out["detail_stock_text"]:
        out["detail_stock_text"] = _strip_htmlish(_s(si.get("promiseResult")), max_len=500)
    out["detail_delivery_promise"] = _strip_htmlish(
        _s(si.get("promiseResult") or si.get("promiseInfoText")), max_len=800
    )

    wim = obj.get("wareInfoReadMap")
    wim = wim if isinstance(wim, dict) else {}
    out["detail_sku_name"] = _s(wim.get("sku_name"))[:2000]
    out["detail_product_id"] = _s(wim.get("product_id"))[:32]
    out["detail_main_sku_id"] = _s(wim.get("main_sku_id"))[:32]
    out["detail_brand"] = _s(wim.get("cn_brand"))[:200]
    if not out["detail_brand"]:
        pav = obj.get("productAttributeVO")
        if isinstance(pav, dict):
            for it in pav.get("attributes") or []:
                if not isinstance(it, dict):
                    continue
                if _s(it.get("labelName")) == "品牌":
                    out["detail_brand"] = _s(it.get("labelValue"))[:200]
                    break

    pav = obj.get("productAttributeVO")
    attrs: list[str] = []
    if isinstance(pav, dict):
        for it in pav.get("attributes") or []:
            if not isinstance(it, dict):
                continue
            ln, lv = _s(it.get("labelName")), _s(it.get("labelValue"))
            if ln and lv:
                attrs.append(f"{ln}:{lv}")
    out["detail_product_attributes"] = "; ".join(attrs)[:4000]

    out["detail_belt_banner"] = _join_url(_s(obj.get("beltBanner")))[:800]
    out["detail_csfh_text"] = _s(obj.get("csfhText"))[:200]

    return out


# 与 OUT_PARSED_CSV / 流水线 detail CSV 列一致
WARE_PARSED_CSV_FIELDNAMES: tuple[str, ...] = (
    "skuId",
    "http_status",
    *WARE_BUSINESS_MERGE_FIELDNAMES,
)

# 仅 sku + 配料（本脚本 OUTPUT_SKU_AND_BODY_IMAGES_ONLY 时 main 写 CSV/解析 JSON 用）
SKU_BODY_IMAGES_ONLY_FIELDNAMES: tuple[str, ...] = (
    "skuId",
    "detail_body_ingredients",
)

# 与合并表 lean 商详块一致 + skuId；keyword_pipeline DETAIL_WARE_CSV_MODE=lean 写 detail_ware_export.csv
DETAIL_WARE_LEAN_CSV_FIELDNAMES: tuple[str, ...] = (
    "skuId",
    "detail_brand",
    "detail_price_final",
    "detail_shop_name",
    "detail_category_path",
    "detail_product_attributes",
    "detail_body_ingredients",
)


def detail_ware_lean_csv_row(
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
) -> dict[str, str]:
    """lean 详情汇总表一行（无 http_status）；字段来自 ``ware_parsed_row`` 子集。"""
    full = ware_parsed_row(
        sku,
        http_status,
        response_text,
        detail_body_ingredients=detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
    )
    return {k: str(full.get(k) or "") for k in DETAIL_WARE_LEAN_CSV_FIELDNAMES}


def minimal_sku_body_images_row(
    sku: str,
    detail_body_ingredients: str,
    *,
    detail_body_ingredients_source_url: str = "",
) -> dict[str, str]:
    """``skuId``、配料文本（图源仅内部流程使用，不写入极简 CSV）。"""
    _ = detail_body_ingredients_source_url  # 保留参数供调用方兼容
    u = (detail_body_ingredients or "").strip()
    mxc = max(0, int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS))
    if mxc and len(u) > mxc:
        u = u[:mxc]
    return {
        "skuId": str(sku).strip(),
        "detail_body_ingredients": u,
    }


def _write_minimal_body_images_json(
    path: Path,
    sku: str,
    detail_body_ingredients: str,
    *,
    detail_body_ingredients_source_url: str = "",
) -> None:
    row = minimal_sku_body_images_row(
        sku,
        detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[京东] 已写图文 URL JSON：{path}", file=sys.stderr)


def parse_ware_business_response_text(text: str) -> tuple[dict[str, str], bool]:
    """
    解析响应体字符串。
    返回 ``(扁平字典, 是否成功解析为 JSON 对象)``；失败时扁平字典各键均为 ``""``。
    """
    raw = (text or "").strip()
    if not raw:
        return _empty_ware_flat(), False
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_ware_flat(), False
    if not isinstance(obj, dict):
        return _empty_ware_flat(), False
    return flatten_ware_business(obj), True


def ware_parsed_row(
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
) -> dict[str, str]:
    """单行扁平结果：``skuId``、``http_status`` 与 ``WARE_BUSINESS_MERGE_FIELDNAMES``（含 DOM 长图 URL 或配料文本）。"""
    flat, _ = parse_ware_business_response_text(
        response_text if http_status == 200 else ""
    )
    row: dict[str, str] = {
        "skuId": str(sku).strip(),
        "http_status": str(http_status),
        **flat,
    }
    u = (detail_body_ingredients or "").strip()
    if u:
        mxc = max(0, int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS))
        row["detail_body_ingredients"] = u[:mxc] if mxc else u
    src = (detail_body_ingredients_source_url or "").strip()
    if src:
        mxc = max(0, int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS))
        row["detail_body_ingredients_source_url"] = src[:mxc] if mxc else src
    return row


def ware_fetch_should_retry(http_status: int, response_text: str) -> bool:
    """
    True 表示应重试：未命中接口、HTTP 非 200、正文空、非 JSON、或解析后业务字段全空。
    """
    if int(http_status) != 200:
        return True
    raw = (response_text or "").strip()
    if not raw:
        return True
    flat, ok = parse_ware_business_response_text(raw)
    if not ok:
        return True
    return not any((v or "").strip() for v in flat.values())


def format_ware_response_for_save(
    text: str,
    *,
    normalize: bool = True,
    sort_keys: bool = True,
    indent: int = 2,
) -> str:
    """流水线等落盘用：尽量输出缩进 + 可选键排序的 JSON 文本（失败则保留原文）。"""
    body, _ok = _format_ware_response_text(
        text or "",
        normalize=normalize,
        sort_keys=sort_keys,
        indent=max(0, int(indent)),
    )
    return body


def _write_ware_parsed_json(
    path: Path,
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
) -> None:
    row = ware_parsed_row(
        sku,
        http_status,
        response_text,
        detail_body_ingredients=detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[京东] 已写解析 JSON：{path}", file=sys.stderr)


def _read_jd_cookie_file_raw(cookie_file: str | None) -> str:
    """多行合并为 ``; `` 分隔（与 Node ``readCookieFile`` 一致）。"""
    path = Path(cookie_file) if (cookie_file or "").strip() else _DEFAULT_COOKIE_PATH
    path = path.resolve()
    if not path.is_file():
        return ""
    chunks: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        t = line.strip()
        if t and not t.startswith("#"):
            chunks.append(t)
    return "; ".join(chunks).strip()


def _cookie_header_to_playwright(cookie_header: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, value = part.partition("=")
        name, value = name.strip(), value.strip()
        if not name:
            continue
        rows.append(
            {
                "name": name,
                "value": value,
                "domain": ".jd.com",
                "path": "/",
                "secure": True,
            }
        )
    return rows


def _headers_for_verbose(h: dict[str, str], *, cookie_preview: int = 96) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in h.items():
        if k.lower() == "cookie" and len(v) > cookie_preview:
            out[k] = (
                v[:cookie_preview]
                + f"...（共 {len(v)} 字符，完整值请用 --http-log）"
            )
        else:
            out[k] = v
    return out


def _print_http_verbose(meta: dict[str, Any], *, body_max: int) -> None:
    req = meta["request"]
    res = meta["response"]
    body = res.get("body") or ""
    if len(body) > body_max:
        body_show = (
            body[:body_max]
            + f"\n...（stderr 已截断，响应共 {len(body)} 字符；完整见 --http-log）"
        )
    else:
        body_show = body
    req_block: dict[str, Any] = {"method": req.get("method", "GET")}
    if req.get("url"):
        req_block["url"] = req["url"]
    for k in ("via", "item_page", "referrer_document", "note"):
        if k in req:
            req_block[k] = req[k]
    hdrs = req.get("headers")
    if hdrs:
        req_block["headers"] = _headers_for_verbose(hdrs)
    block = {
        "skuId": meta.get("skuId"),
        "request": req_block,
        "response": {
            "url": res.get("url"),
            "status": res.get("status"),
            "status_text": res.get("status_text"),
            "headers": res.get("headers"),
            "body": body_show,
        },
    }
    sys.stderr.write(
        "[京东] HTTP 详情（stderr）:\n"
        + json.dumps(block, ensure_ascii=False, indent=2)
        + "\n"
    )


def _fetch_ware_business_once(
    context: Any,
    page: Any,
    sku_id: str,
    *,
    cookie_file: str | None = None,
    timeout_ms: int = 30_000,
    cookie_override: str = "",
) -> tuple[int, str, dict[str, Any]]:
    """单次打开商品页并拦截 ``pc_detailpage_wareBusiness`` 响应（无重试）。"""
    raw_cookie = (cookie_override or "").strip() or _read_jd_cookie_file_raw(cookie_file)
    if not raw_cookie:
        print(
            "[京东] 需要 Cookie：--cookie 或 jd_cookie.txt（--cookie-file）",
            file=sys.stderr,
        )
        sys.exit(2)
    context.clear_cookies()
    try:
        context.add_cookies(_cookie_header_to_playwright(raw_cookie))
    except Exception as e:
        print(f"[京东] add_cookies 警告: {e}", file=sys.stderr)
    item_url = f"https://item.jd.com/{str(sku_id).strip()}.html"
    matches: list[tuple[int, str, str, dict[str, str]]] = []
    trace_rows: list[dict[str, Any]] = []
    trace_phase = "opening"

    def _on_response(response: Any) -> None:
        try:
            u = response.url
            if LOG_API_M_JD_TRACE and "api.m.jd.com" in u:
                ct = ""
                try:
                    ct = (response.headers.get("content-type") or "").strip()
                except Exception:
                    pass
                trace_rows.append(
                    {
                        "phase": trace_phase,
                        "status": response.status,
                        "functionId": _jd_function_id_from_api_url(u),
                        "content_type": ct[:160],
                        "url": u[:2000],
                    }
                )
            if (
                "api.m.jd.com" in u
                and "pc_detailpage_wareBusiness" in u
                and "functionId=" in u
            ):
                st = response.status
                body = response.text()
                matches.append((st, body, u, dict(response.headers)))
        except Exception:
            pass

    detail_joined = ""
    page.on("response", _on_response)
    try:
        _wu = (GOTO_WAIT_UNTIL or "domcontentloaded").strip()
        page.goto(item_url, wait_until=_wu, timeout=timeout_ms)
        trace_phase = "loaded"
        if CLICK_PRODUCT_DETAIL_TAB:
            _click_jd_product_detail_tab(page, timeout_ms=timeout_ms)
            trace_phase = "after_tab"
        extra = min(12_000, max(3_000, timeout_ms // 2))
        page.wait_for_timeout(extra)
        if COLLECT_DETAIL_MAIN_IMAGE_URLS:
            try:
                detail_joined = scrape_detail_main_body_urls_joined(
                    page,
                    wait_ms=min(timeout_ms, 12_000),
                )
            except Exception as e:
                print(
                    f"[京东] 抽取 #detail-main 图文 URL 失败: {e}",
                    file=sys.stderr,
                )
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass
    if LOG_API_M_JD_TRACE:
        _print_api_m_jd_trace(trace_rows, sku_id=str(sku_id).strip())
    if not matches:
        meta: dict[str, Any] = {
            "skuId": str(sku_id).strip(),
            "request": {
                "method": "GET",
                "via": "capture_page_navigation",
                "item_page": item_url,
                "note": "未捕获到 pc_detailpage_wareBusiness（页面可能改版或 Cookie 未登录）",
            },
            "response": {"status": 0, "status_text": "", "headers": {}, "body": ""},
        }
        if LOG_API_M_JD_TRACE:
            meta["api_m_jd_trace"] = trace_rows
        meta["detail_body_image_urls"] = detail_joined
        return 0, "", meta
    ok_rows = [m for m in matches if m[0] == 200]
    st, body, u, rh = ok_rows[-1] if ok_rows else matches[-1]
    meta = {
        "skuId": str(sku_id).strip(),
        "request": {
            "method": "GET",
            "url": u,
            "via": "capture_page_navigation",
            "item_page": item_url,
            "captures_count": len(matches),
        },
        "response": {
            "url": u,
            "status": st,
            "status_text": "",
            "headers": rh,
            "body": body,
        },
    }
    if LOG_API_M_JD_TRACE:
        meta["api_m_jd_trace"] = trace_rows
    meta["detail_body_image_urls"] = detail_joined
    return st, body, meta


def fetch_ware_business(
    context: Any,
    page: Any,
    sku_id: str,
    *,
    cookie_file: str | None = None,
    timeout_ms: int = 30_000,
    cookie_override: str = "",
    max_attempts: int = 1,
    retry_delay_sec: float = 2.0,
    cancel_check: Callable[[], bool] | None = None,
) -> tuple[int, str, dict[str, Any]]:
    """
    打开商品页并拦截 ``pc_detailpage_wareBusiness``。
    ``max_attempts``>1 时，在结果为空或失败时按 ``retry_delay_sec`` 间隔重试。
    """
    sid = str(sku_id).strip()
    n = max(1, int(max_attempts))
    last: tuple[int, str, dict[str, Any]] = (0, "", {})
    for i in range(n):
        if cancel_check is not None and cancel_check():
            return last
        if i > 0:
            delay = max(0.0, float(retry_delay_sec))
            if delay > 0:
                time.sleep(delay)
        if cancel_check is not None and cancel_check():
            return last
        code, text, meta = _fetch_ware_business_once(
            context,
            page,
            sid,
            cookie_file=cookie_file,
            timeout_ms=timeout_ms,
            cookie_override=cookie_override,
        )
        last = (code, text, meta)
        if OUTPUT_SKU_AND_BODY_IMAGES_ONLY and (
            meta.get("detail_body_image_urls") or ""
        ).strip():
            if i > 0:
                print(
                    f"[京东] sku={sid} 第 {i + 1} 次尝试已成功（已抽到 #detail-main 图文 URL）",
                    file=sys.stderr,
                )
            break
        if not ware_fetch_should_retry(code, text):
            if i > 0:
                print(
                    f"[京东] sku={sid} 第 {i + 1} 次尝试已成功",
                    file=sys.stderr,
                )
            break
        print(
            f"[京东] sku={sid} 详情结果为空或无效 (HTTP {code})，"
            f"重试 {i + 1}/{n}…",
            file=sys.stderr,
        )
    return last


def _detail_body_ingredients_column_value(
    urls_joined: str,
    *,
    vision_mod: Any,
    vision_ok: bool,
) -> tuple[str, str]:
    """
    返回 ``(detail_body_ingredients, detail_body_ingredients_source_url)``。
    配料列为文本；失败时为 ``【未识别到配料】…``。图源列仅在视觉成功命中配料时非空。
    """
    if not EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES:
        return "", ""
    if not vision_ok or vision_mod is None:
        return (
            "【未识别到配料】未配置或无效的多模态 API（见 market_assistant/.env 中 OPENAI_* / LLM_*）。",
            "",
        )
    raw = (urls_joined or "").strip()
    try:
        fn = getattr(
            vision_mod,
            "extract_ingredients_from_body_image_urls_reversed_with_source",
            None,
        )
        if callable(fn):
            text, src_url = fn(raw)
            return str(text or "").strip(), (str(src_url).strip() if src_url else "")
        text = str(
            vision_mod.extract_ingredients_from_body_image_urls_reversed(raw)
        ).strip()
        return text, ""
    except Exception as e:
        print(f"[京东] 配料视觉提取异常: {e}", file=sys.stderr)
        return f"【未识别到配料】识别异常：{e}"[:800], ""


def main() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    sku = (SKU or "").strip()
    sku_file = (SKU_FILE or "").strip()
    out_path = (OUT or "").strip()
    out_dir = (OUT_DIR or "").strip()
    if bool(sku) == bool(sku_file):
        print("请在文件顶部配置 SKU 或 SKU_FILE（二选一）", file=sys.stderr)
        sys.exit(2)
    if sku_file and out_path:
        print("使用 SKU_FILE 时不要配置 OUT，请用 OUT_DIR", file=sys.stderr)
        sys.exit(2)

    out_parsed = (OUT_PARSED or "").strip()
    out_parsed_dir = (OUT_PARSED_DIR or "").strip()
    out_parsed_csv = (OUT_PARSED_CSV or "").strip()
    output_minimal = bool(OUTPUT_SKU_AND_BODY_IMAGES_ONLY)
    if sku_file and not out_dir:
        if not (
            output_minimal
            and (out_parsed_csv or out_parsed_dir)
        ):
            print("使用 SKU_FILE 时请配置 OUT_DIR", file=sys.stderr)
            sys.exit(2)
    if sku_file and out_parsed and not out_parsed_dir:
        print(
            "[京东] 批量模式请配置 OUT_PARSED_DIR（每 SKU 写 ware_{sku}_parsed.json）；"
            "已忽略 OUT_PARSED",
            file=sys.stderr,
        )
        out_parsed = ""

    cookie_file_cli = (COOKIE_FILE or "").strip()
    if cookie_file_cli:
        cookie_file_cli = str(Path(cookie_file_cli).resolve())
    cf = cookie_file_cli or None
    cookie_override = (COOKIE_OVERRIDE or "").strip()
    timeout_ms = max(1000, int(float(TIMEOUT_SEC) * 1000))

    skus = [sku] if sku else []
    if sku_file:
        raw = Path(sku_file).read_text(encoding="utf-8")
        for line in raw.splitlines():
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            skus.append(t)
    if not skus:
        print("无有效 SKU", file=sys.stderr)
        sys.exit(2)

    http_log_path = (HTTP_LOG or "").strip()
    http_records: list[dict[str, Any]] = []
    pretty = bool(PRETTY_STDOUT)
    verbose_http = bool(VERBOSE_HTTP)
    verbose_body_lim = int(VERBOSE_HTTP_BODY_LIMIT)
    normalize_json = bool(NORMALIZE_WARE_JSON)
    sort_keys = bool(SORT_JSON_KEYS)
    json_indent = max(0, int(JSON_INDENT))
    parsed_csv_rows: list[dict[str, str]] = []

    vision_mod: Any = None
    vision_ok = False
    if EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES:
        try:
            import AI_crawler as vision_mod  # noqa: WPS433

            vision_mod._resolve_credentials(None, None, None)
            vision_ok = True
        except Exception as e:
            print(
                f"[京东] 已开启配料视觉提取但未就绪（{e}），"
                f"列 detail_body_ingredients / detail_body_ingredients_source_url 将按未就绪处理",
                file=sys.stderr,
            )

    def emit(
        code: int,
        text: str,
        s: str,
        detail_body_ingredients: str = "",
        *,
        detail_body_ingredients_source_url: str = "",
    ) -> None:
        if code == 403:
            print(
                f"[京东] SKU {s}：接口 403，请更新 Cookie 或设 USE_CHROME=True。",
                file=sys.stderr,
            )
            frag = (text or "").strip().replace("\n", " ")[:400]
            if frag:
                print(f"[京东] 响应片段：{frag}", file=sys.stderr)
        if code != 200:
            print(f"[京东] SKU {s} HTTP {code}", file=sys.stderr)

        if output_minimal:
            row_m = minimal_sku_body_images_row(
                s,
                detail_body_ingredients,
                detail_body_ingredients_source_url=detail_body_ingredients_source_url,
            )
            if out_dir:
                if out_parsed_dir:
                    pd = Path(out_parsed_dir).resolve() / f"ware_{s}_parsed.json"
                    _write_minimal_body_images_json(
                        pd,
                        s,
                        detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if out_path:
                if out_parsed:
                    _write_minimal_body_images_json(
                        Path(out_parsed).resolve(),
                        s,
                        detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if len(skus) != 1:
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if out_parsed:
                _write_minimal_body_images_json(
                    Path(out_parsed).resolve(),
                    s,
                    detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            sys.stdout.write(
                json.dumps(row_m, ensure_ascii=False, indent=2) + "\n"
            )
            if out_parsed_csv:
                parsed_csv_rows.append(dict(row_m))
            return

        if out_dir:
            out_p = Path(out_dir).resolve() / f"ware_{s}.json"
            out_p.parent.mkdir(parents=True, exist_ok=True)
            body, _ok = _format_ware_response_text(
                text,
                normalize=normalize_json,
                sort_keys=sort_keys,
                indent=json_indent,
            )
            out_p.write_text(body, encoding="utf-8")
            print(f"[京东] 已写 {out_p}", file=sys.stderr)
            if out_parsed_dir:
                pd = Path(out_parsed_dir).resolve() / f"ware_{s}_parsed.json"
                _write_ware_parsed_json(
                    pd,
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                )
            return
        if out_path:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            if normalize_json:
                body, _ok = _format_ware_response_text(
                    text,
                    normalize=True,
                    sort_keys=sort_keys,
                    indent=json_indent,
                )
                Path(out_path).write_text(body, encoding="utf-8")
            elif pretty:
                try:
                    obj = json.loads(text)
                    Path(out_path).write_text(
                        json.dumps(obj, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                except json.JSONDecodeError:
                    Path(out_path).write_text(text, encoding="utf-8")
            else:
                Path(out_path).write_text(text, encoding="utf-8")
            print(f"[京东] 已写 {out_path}", file=sys.stderr)
            if out_parsed:
                _write_ware_parsed_json(
                    Path(out_parsed).resolve(),
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                )
            return
        if len(skus) != 1:
            sys.stdout.write(text + ("\n" if text and not text.endswith("\n") else ""))
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                )
            return
        if out_parsed:
            _write_ware_parsed_json(
                Path(out_parsed).resolve(),
                s,
                code,
                text,
                detail_body_ingredients=detail_body_ingredients,
                detail_body_ingredients_source_url=detail_body_ingredients_source_url,
            )
        if normalize_json:
            body, ok = _format_ware_response_text(
                text,
                normalize=True,
                sort_keys=sort_keys,
                indent=json_indent,
            )
            sys.stdout.write(
                body
                if ok
                else text + ("\n" if text and not text.endswith("\n") else "")
            )
        elif pretty:
            try:
                obj = json.loads(text)
                sys.stdout.write(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
            except json.JSONDecodeError:
                sys.stdout.write(text + ("\n" if not text.endswith("\n") else ""))
        else:
            sys.stdout.write(text + ("\n" if text and not text.endswith("\n") else ""))
        if out_parsed_csv:
            parsed_csv_rows.append(
                ware_parsed_row(
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            )

    max_att = max(1, int(FETCH_MAX_ATTEMPTS))
    retry_sec = max(0.0, float(FETCH_RETRY_DELAY_SEC))

    def run_one(page: Any, s: str) -> None:
        code, text, meta = fetch_ware_business(
            page.context,
            page,
            s,
            cookie_file=cf,
            timeout_ms=timeout_ms,
            cookie_override=cookie_override,
            max_attempts=max_att,
            retry_delay_sec=retry_sec,
        )
        if verbose_http:
            _print_http_verbose(meta, body_max=max(500, verbose_body_lim))
        if http_log_path:
            http_records.append(meta)
        print(f"[京东] sku={s} HTTP {code}", file=sys.stderr)
        if code == 0 and meta.get("request", {}).get("note"):
            print(f"[京东] {meta['request']['note']}", file=sys.stderr)
        body_urls_meta = str(meta.get("detail_body_image_urls") or "").strip()
        body_col, body_src = _detail_body_ingredients_column_value(
            body_urls_meta,
            vision_mod=vision_mod,
            vision_ok=vision_ok,
        )
        if EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES and body_col:
            if str(body_col).startswith("【未识别"):
                print(f"[京东] sku={s} {body_col}", file=sys.stderr)
            elif body_src:
                print(
                    f"[京东] sku={s} 已自详情长图解析配料表（首次命中即停）图源: {body_src}",
                    file=sys.stderr,
                )
            else:
                print(f"[京东] sku={s} 已自详情长图解析配料表（首次命中即停）", file=sys.stderr)
        emit(
            code,
            text,
            s,
            body_col,
            detail_body_ingredients_source_url=body_src,
        )

    headed = bool(HEADED or DEBUG_PAUSE)
    use_chrome = bool(USE_CHROME or DEBUG_PAUSE)
    launch_kw: dict[str, Any] = {"headless": not headed}
    if use_chrome:
        launch_kw["channel"] = "chrome"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(**launch_kw)
        context = browser.new_context(
            user_agent=_JD_DETAIL_UA,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            extra_http_headers=dict(_JD_DETAIL_CONTEXT_EXTRA_HEADERS),
        )
        page = context.new_page()
        if output_minimal:
            print(
                "[京东] 极简输出：skuId + detail_body_ingredients"
                "（仍打开页抓接口以加载 DOM）",
                file=sys.stderr,
            )
        else:
            print(
                "[京东] 加载商品页并拦截 pc_detailpage_wareBusiness",
                file=sys.stderr,
            )
        try:
            for s in skus:
                run_one(page, s)
        finally:
            if DEBUG_PAUSE:
                print(
                    "[京东] 调试：浏览器仍将保持打开；在此终端按回车后再关闭…",
                    file=sys.stderr,
                )
                try:
                    input()
                except EOFError:
                    pass
            try:
                page.close()
            except Exception:
                pass
            browser.close()

    if parsed_csv_rows and out_parsed_csv:
        csv_fields = list(
            SKU_BODY_IMAGES_ONLY_FIELDNAMES
            if output_minimal
            else WARE_PARSED_CSV_FIELDNAMES
        )
        cpp = Path(out_parsed_csv).resolve()
        cpp.parent.mkdir(parents=True, exist_ok=True)
        with cpp.open("w", encoding="utf-8-sig", newline="") as cf:
            w = csv.DictWriter(cf, fieldnames=csv_fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(parsed_csv_rows)
        print(
            f"[京东] 已写解析 CSV：{cpp}（{len(parsed_csv_rows)} 行）",
            file=sys.stderr,
        )

    if http_log_path:
        log_p = Path(http_log_path).resolve()
        log_p.parent.mkdir(parents=True, exist_ok=True)
        payload: Any = http_records[0] if len(http_records) == 1 else http_records
        log_p.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"[京东] 已写 HTTP 往返记录：{log_p}", file=sys.stderr)


if __name__ == "__main__":
    main()


