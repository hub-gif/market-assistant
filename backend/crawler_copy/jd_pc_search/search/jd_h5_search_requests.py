# -*- coding: utf-8 -*-
"""
京东搜索 — requests 版（批量/HTML/粘贴 URL）。

与 ``jd_search_playwright.py`` 对齐的请求行
----------------------------------------
- **默认**（仅 ``--q``、``--format items``、且无 ``--url`` / ``--url-file`` / ``--api-params-file``）：
  与同目录 ``jd_search_playwright.py`` 一样，由 Node ``jd_export_search_request.js`` 生成
  **url + headers**（含 ``get_h5st``），再用 **requests** 发 GET。
  区别仅 TLS：本脚本为 urllib3，Playwright 为 Chromium；若遇 jfe 403 请改用 ``jd_search_playwright.py``。
- Cookie：Node ``jd_export_search_request.js`` 默认读 **../common/jd_cookie.txt**；若 Python 传入 ``cookie_file`` 或 CLI 指定 ``--cookie-file``，则 Node 使用对应文件。仅 ``--cookie`` 粘贴字符串时仍由 **requests** 路径使用，与 Node 签包无关。
- 需要 **search.action HTML** 抽商品时加 ``--h5-html``。

与淘宝的差异（重要）
-------------------
- 淘宝列表数据走 **mtop JSON/JSONP**，必须 **t + sign**。
- H5 **search.action** 易风控；pc 搜索 API 在 **api.m.jd.com**（h5st 由 Node 生成）。
  默认路径下按抓包规则推进 **body.page** 与 **body.s**（同逻辑页内两包：**page** 为 **2L-1、2L**（L 为 CLI 逻辑页）；
  **s** 按上一包响应 **max(1, 自然位-1)** 累加，自然位 = ``wareList`` 非显式广告条数）。
  **每逻辑页固定 2 次** pc_search；CSV 列 ``page`` 仍为**逻辑页**；单页入库 SKU 仍可在约 **60** 条处封顶（第二包照常请求以同步游标）。
  ``--page N`` 会先按每逻辑页 2 包跳过前 N-1 页再采当前页。
- ``--csv`` 列名在淘宝 ``CANONICAL_FIELDS`` 基础上去掉 ``features`` / ``promotion_tags``，另含末尾 ``platform`` / ``keyword`` / ``page``；
  卖点、评价量（``commentFuzzy``）、``commentSalesFloor`` 等为独立列，字段随接口结构略有差异。

功能
----
- --url / --url-file / --api-params-file：三选一（与仅 --q 的默认 pc API 路径互斥）
- --q + items（默认）：pc API（同 Playwright 的 URL/Header）
- --q + --h5-html：so.m.jd.com search.action HTML

用法
----
  cd crawler/jd_pc_search/search
  python jd_h5_search_requests.py --q 低GI --page 1 --csv --out ../../data/jd_h5_p1.csv

  python jd_h5_search_requests.py --url "https://api.m.jd.com/..."
"""

from __future__ import annotations

import argparse
import csv
import html as html_module
import json
import os
import random
import re
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import requests

_JD_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_JD_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_JD_PKG_ROOT))
from common.jd_delay_utils import parse_request_delay_range, sleep_pc_search_request_gap

_JD_PC_SEARCH_DIR = Path(__file__).resolve().parent

# PC 搜索：与浏览器一致，**每逻辑页固定 2 包** pc_search（body.page 连续 +1：第 L 页为 2L-1 与 2L）。
# **Δs = max(1, 自然位条数-1)**（wareList 非广告计自然位）。跳过前序屏时同样每逻辑页 2 包推进游标。
# CLI 的 --page 是**逻辑页**，不是请求 body.page（避免混淆）。
JD_PC_SEARCH_ITEMS_PER_PAGE = 60
JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE = 2
# 重试仍失败时推进 s 的兜底（与常见两包 Δs≈21～22 一致）
JD_PC_SEARCH_FALLBACK_S_STEP = 22


def pc_search_response_is_empty_ware_list(body: str) -> bool:
    """合法 JSON 且 ``data.wareList`` 为明确空数组时视为「已无更多商品」，不宜再强推游标。"""
    p = _loads_json_or_jsonp((body or "").strip())
    if not isinstance(p, dict):
        return False
    data = p.get("data")
    return isinstance(data, dict) and data.get("wareList") == []


def pc_search_should_retry_fetch(
    body: str, *, has_rows: bool, s_step: int
) -> bool:
    """
    是否应对同一 body.page / s 重发请求。
    空体、非 JSON 短包等视为瞬时故障；明确的 ``data.wareList == []`` 不重试。
    """
    if has_rows or s_step > 0:
        return False
    s = (body or "").strip()
    if not s:
        return True
    p = _loads_json_or_jsonp(s)
    if p is None:
        return len(s) < 12000
    if not isinstance(p, dict):
        return False
    data = p.get("data")
    if isinstance(data, dict) and data.get("wareList") == []:
        return False
    return True


def jd_pc_api_body_page_first_pack(logical_page_1based: int) -> int:
    """逻辑页 L（从 1 起）第一包请求里的 body.page = 2L - 1。"""
    L = max(1, int(logical_page_1based))
    return 2 * L - 1


def _jd_row_count_for_page(rows: list[dict[str, str]], page: int) -> int:
    ps = str(page)
    return sum(1 for r in rows if (r.get("page") or "").strip() == ps)


def export_pc_search_request_json(
    keyword: str,
    api_body_page: int,
    *,
    s: int = 1,
    pvid: str | None = None,
    cookie_file: str | None = None,
) -> dict[str, Any]:
    """Node 输出 {url, headers}。api_body_page / s 为请求 body 里的 page、s（与抓包一致，非 CSV 页码）。"""
    cmd = [
        "node",
        str(_JD_PC_SEARCH_DIR / "jd_export_search_request.js"),
        "--q",
        keyword,
        "--page",
        str(api_body_page),
        "--s",
        str(max(1, int(s))),
    ]
    pv = (pvid or "").strip()
    if pv:
        cmd.extend(["--pvid", pv])
    cf = (cookie_file or "").strip()
    if cf:
        cmd.extend(["--cookie-file", cf])
    r = subprocess.run(
        cmd,
        cwd=str(_JD_PC_SEARCH_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if r.returncode != 0:
        print(r.stderr or r.stdout, file=sys.stderr)
        sys.exit(r.returncode or 1)
    return json.loads(r.stdout)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# --ua-preset：H5 search.action 遇「去 APP」风控时可换移动端 UA 试一次（不保证有效）。
UA_PRESETS: dict[str, str] = {
    "chrome_win": USER_AGENT,
    "iphone_m": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "android_m": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
}

# 在此粘贴浏览器请求头里的完整 Cookie（一整行）。也可改用 ../common/jd_cookie.txt。
MY_COOKIE = r"""

""".strip()


def _is_h5_ware_search_url(url: str) -> bool:
    u = urlparse(url.strip())
    return "so.m.jd.com" in (u.netloc or "") and "search.action" in (u.path or "")


def _is_jd_api_url(url: str) -> bool:
    u = urlparse(url.strip())
    n, p = u.netloc or "", u.path or ""
    return "api.m.jd.com" in n or ("jd.com" in n and "client.action" in p)


def build_request_headers(
    cookie: str,
    *,
    request_url: str,
    ua_preset: str,
    referer_override: str | None = None,
) -> dict[str, str]:
    ua = UA_PRESETS.get(ua_preset, UA_PRESETS["chrome_win"])
    is_mobile = ua_preset in ("iphone_m", "android_m")
    is_api = _is_jd_api_url(request_url)
    url_l = request_url.lower()
    # 你抓包这类属于 search.jd.com 触发的 pc_search_searchWare
    is_search_pc_api = ("appid=search-pc-java" in url_l) or ("client=pc" in url_l) or (
        "functionid=pc_search_searchware" in url_l
    )

    h: dict[str, str] = {
        "User-Agent": ua,
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if is_api:
        # XHR / client.action 常见头（与桌面 HTML 导航略有区别）
        h["Accept"] = "application/json, text/plain, */*"
        if is_search_pc_api:
            ref = (referer_override or "").strip()
            if not ref:
                ref = "https://search.jd.com/Search"
            h["Referer"] = ref
            h["Origin"] = "https://search.jd.com"
            h["x-referer-page"] = "https://search.jd.com/Search"
            h["x-rp-client"] = "h5_1.0.0"
            h["Priority"] = "u=1, i"
        else:
            h["Referer"] = "https://so.m.jd.com/"
            h["Origin"] = "https://so.m.jd.com"
        h["sec-fetch-dest"] = "empty"
        h["sec-fetch-mode"] = "cors"
        h["sec-fetch-site"] = "same-site"
    else:
        h["Accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        )
        h["Upgrade-Insecure-Requests"] = "1"
        h["Referer"] = "https://so.m.jd.com/"
        h["sec-fetch-dest"] = "document"
        h["sec-fetch-mode"] = "navigate"
        h["sec-fetch-site"] = "same-site"
        h["sec-fetch-user"] = "?1"

    if is_mobile:
        h["sec-ch-ua-mobile"] = "?1"
        h["sec-ch-ua-platform"] = '"Android"' if ua_preset == "android_m" else '"iOS"'
    else:
        h["sec-ch-ua"] = '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
        h["sec-ch-ua-mobile"] = "?0"
        h["sec-ch-ua-platform"] = '"Windows"'

    if cookie.strip():
        h["Cookie"] = cookie.strip()
    return h


def _read_cookie_file(path: str) -> str:
    """读取 Cookie 文件：忽略空行与 # 注释行；多行非注释内容用 '; ' 拼接。"""
    raw = Path(path).read_text(encoding="utf-8")
    chunks: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        chunks.append(s)
    return "; ".join(chunks).strip()


def load_cookie(args: argparse.Namespace) -> str:
    if getattr(args, "cookie", None) and str(args.cookie).strip():
        return str(args.cookie).strip()
    if getattr(args, "cookie_file", None):
        path = args.cookie_file
        if not os.path.isfile(path):
            print(f"Cookie 文件不存在: {path}", file=sys.stderr)
            sys.exit(1)
        return _read_cookie_file(path)
    if MY_COOKIE.strip():
        return MY_COOKIE.strip()
    default_txt = Path(__file__).resolve().parent.parent / "common" / "jd_cookie.txt"
    if default_txt.is_file():
        return _read_cookie_file(str(default_txt))
    return os.environ.get("JD_COOKIE", "").strip()


def _noncomment_lines(text: str) -> list[str]:
    out: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def parse_jd_api_params_text(text: str) -> list[tuple[str, str]]:
    """
    用于把 DevTools「Query String Parameters」粘成文件后读取。

    支持两种格式（同一文件只用一种）：
    - 每行 key=value（value 可含 =；支持重复键如两个 t）
    - 键一行、值一行（偶数行；与部分工具导出格式一致）
    """
    lines = _noncomment_lines(text)
    if not lines:
        return []
    if any("=" in ln for ln in lines):
        pairs: list[tuple[str, str]] = []
        for ln in lines:
            if "=" not in ln:
                raise ValueError(f"混用格式：本行应有 '=': {ln[:80]}")
            k, _, v = ln.partition("=")
            k, v = k.strip(), v.strip()
            if not k:
                raise ValueError(f"空的参数名: {ln[:80]}")
            pairs.append((k, v))
        return pairs
    if len(lines) % 2:
        raise ValueError("交替键值格式需要偶数行（键一行、值一行）")
    return [(lines[i].strip(), lines[i + 1].strip()) for i in range(0, len(lines), 2)]


def build_jd_api_url_from_param_pairs(pairs: list[tuple[str, str]]) -> str:
    return "https://api.m.jd.com/api?" + urlencode(pairs, doseq=True)


def read_url_list_file(path: str) -> list[str]:
    return _noncomment_lines(Path(path).read_text(encoding="utf-8"))


# 与淘宝 CANONICAL_FIELDS 基本一致，但不导出 features / promotion_tags（用户侧去重简化）；
# 末尾追加 platform / keyword / page。
JD_ITEM_CSV_FIELDS = (
    "item_id",
    "sku_id",
    "title",
    # "title_plain",
    "price",
    "coupon_price",
    "original_price",
    "selling_point",
    "comment_sales_floor",
    "hot_list_rank",
    "comment_count",
    "shop_name",
    "shop_url",
    "shop_info_url",
    "location",
    "detail_url",
    "image",
    "seckill_info",
    "attributes",
    "leaf_category",
    # "same_count",
    # "relation_score",
    # "is_p4p",
    "platform",
    "keyword",
    "page",
)

# 导出列名：中文说明(JSON 中主要原始字段名)，便于对照接口
JD_EXPORT_COLUMN_HEADERS: dict[str, str] = {
    "item_id": "主商品ID(wareId)",
    "sku_id": "SKU(skuId)",
    "title": "标题(wareName)",
    # "title_plain": "标题纯文本(wareName)",
    "price": "标价(jdPrice,jdPriceText,realPrice)",
    "coupon_price": "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)",
    "original_price": "原价(oriPrice,originalPrice,marketPrice)",
    "selling_point": "卖点(sellingPoint)",
    "comment_sales_floor": "销量楼层(commentSalesFloor)",
    "hot_list_rank": "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)",
    "comment_count": "评价量(commentFuzzy)",
    "shop_name": "店铺名(shopName)",
    "shop_url": "店铺链接(shopUrl,shopId)",
    "shop_info_url": "店铺信息链接(shopInfoUrl,brandUrl)",
    "location": "地域(deliveryAddress,area,procity)",
    "detail_url": "商品链接(toUrl,clickUrl,item.m.jd.com)",
    "image": "主图(imageurl,imageUrl)",
    # "video_cover": "视频封面(videoImage,videoPic)",
    # "video_dimension": "视频比例(videoRatio)",
    "seckill_info": "秒杀(seckillInfo,secKill)",
    "attributes": "规格属性(propertyList,color,catid,shortName)",
    "leaf_category": "类目(leafCategory,cid3Name,catid)",
    # "same_count": "同款数(sameStyleCount,sameCount)",
    # "relation_score": "相关度(relationScore,score)",
    # "is_p4p": "广告位(isAdv,isAd,extensionId)",
    "platform": "平台(platform)",
    "keyword": "搜索词(keyword)",
    "page": "页码(page)",
}

CSV_FIELDS = tuple(JD_EXPORT_COLUMN_HEADERS[k] for k in JD_ITEM_CSV_FIELDS)


def jd_row_to_export(row: dict[str, str]) -> dict[str, str]:
    """内部键 → 导出列名；无内容则不写入该键（JSON 稀疏对象；CSV 缺列按空单元格写出）。"""
    out: dict[str, str] = {}
    for k in JD_ITEM_CSV_FIELDS:
        v = str(row.get(k, "") or "").strip()
        if not v:
            continue
        out[JD_EXPORT_COLUMN_HEADERS[k]] = v
    return out


def _jd_empty_export_row() -> dict[str, str]:
    return {k: "" for k in JD_ITEM_CSV_FIELDS}


def _human_text(s: str, max_len: int = 2000) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = html_module.unescape(t)
    t = " ".join(t.split()).strip()
    return t[:max_len]


def _safe_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://so.m.jd.com" + u
    if u.startswith("http://"):
        return "https://" + u[7:]
    return u


# 搜索接口里常见仅返回 jfs/...，需拼到 360buyimg 下（与 PC 列表 n2/s480x480 规格一致；img10–img14 等 CDN 可互换）
_JD_PRODUCT_IMG_HOST = "https://img13.360buyimg.com"


def _jd_product_image_url(u: str) -> str:
    """
    主图补全为可访问 URL。
    例：jfs/t1/402762/.../xxx.jpg → https://img13.360buyimg.com/n2/s480x480_jfs/t1/402762/.../xxx.jpg
    """
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://"):
        return "https://" + u[7:]
    if u.startswith("https://"):
        return u
    low = u.lower()
    if "360buyimg.com" in low and not re.match(r"^https?://", u, re.I):
        return "https://" + u.lstrip("/")
    p = u.lstrip("/")
    if p.startswith("jfs/"):
        return f"{_JD_PRODUCT_IMG_HOST}/n2/s480x480_{p}"
    if re.match(r"^n[0-9]+/", p):
        return f"{_JD_PRODUCT_IMG_HOST}/{p}"
    return u


def build_h5_search_url(keyword: str, page: int) -> str:
    """
    京东 H5 搜索 URL（可从你的示例 URL 还原出默认参数）。
    分页参数在不同实验中可能是 page / pageNo / p。这里优先使用 page。
    若你抓包发现分页字段不同，建议直接用 --url 指定每页 URL，或在本函数内改参数名。
    """
    base = "https://so.m.jd.com/ware/search.action"
    q = {
        "keyword": keyword,
        "searchFrom": "home",
        "sf": "14",
        "as": "0",
        "sourceType": "H5_home_page_search",
        "page": str(page),
    }
    return base + "?" + urlencode(q, safe=":%/,+")


def _detect_blocked(html_text: str) -> str | None:
    if not (html_text and html_text.strip()):
        return None
    t = html_text.lower()
    if "当前人数过多" in html_text or "前往京东app" in t or "前往京东APP" in html_text:
        return (
            "服务端限流/风控提示（纯文本）。search.action 易风控；"
            "含 h5st 的 api 请用 search/jd_search_playwright.py，或粘贴完整 URL（--url）；"
            "或 jd_low_gi_playwright.py 整页抓取。"
        )
    if "plogin.m.jd.com" in t or "passport.jd.com" in t:
        return "疑似被要求登录（跳转到登录域）。"
    # pc_search 正常 JSON 里字段名常含 risk/verify 等子串，宽松匹配会误判。
    payload = _loads_json_or_jsonp(html_text)
    if payload is not None:
        raw_probe: list[dict[str, Any]] = []
        _walk_collect_jd_wares(payload, raw_probe)
        if raw_probe:
            return None
        if re.search(
            r"risk\.jd\.com|riskhandler|/risk/|sev\.jd|verify\.jd\.com|"
            r"sliderverify|slide_verify|securityverify",
            html_text,
            re.I,
        ):
            return "疑似进入风控/验证页（risk/verify）。"
    elif "risk" in t and ("handler" in t or "verify" in t):
        return "疑似进入风控/验证页（risk/verify）。"
    if "验证码" in html_text or "安全验证" in html_text:
        return "疑似需要验证码/安全验证。"
    return None


def strip_jsonp(body: str) -> Any:
    """去掉 JSONP 包裹，解析为 Python 对象（与 taobao strip_jsonp 思路一致）。"""
    text = body.strip().rstrip(";")
    m = re.match(r"^[a-zA-Z_$][a-zA-Z0-9_$]*\((.*)\)\s*$", text, re.DOTALL)
    if not m:
        raise ValueError("响应不是预期的 JSONP 格式")
    return json.loads(m.group(1))


def _loads_json_or_jsonp(text: str) -> Any | None:
    s = text.strip()
    if not s:
        return None
    if s.startswith("{") or s.startswith("["):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return None
    try:
        return strip_jsonp(s)
    except (json.JSONDecodeError, ValueError):
        return None


JD_SKU_KEYS = ("wareId", "skuId", "itemId", "wid", "productId")
JD_TITLE_KEYS = ("wareName", "wname", "name", "title", "skuName")
# pc_search_searchWare 常见：jdPrice / jdPriceText / realPrice
JD_PRICE_KEYS = (
    "jdPrice",
    "jdPriceText",
    "realPrice",
    "price",
    "priceText",
    "p",
    "salePrice",
    "purchasePrice",
)
JD_IMG_KEYS = ("imageurl", "imageUrl", "imgUrl", "picUrl", "image")
JD_ORIGINAL_PRICE_KEYS = (
    "oriPrice",
    "originalPrice",
    "marketPrice",
    "linePrice",
    "mPrice",
    "maoPrice",
    "strikePrice",
)
# finalPrice 为对象，见 _jd_parse_final_price；勿在此放 dict 键名
JD_COUPON_KEYS = ("couponPrice", "subsidyPrice", "promoPrice")
JD_DETAIL_URL_KEYS = (
    "toUrl",
    "clickUrl",
    "wareUrl",
    "href",
    "itemUrl",
    "detailUrl",
    "skuUrl",
    "pUrl",
)
JD_SHOP_URL_KEYS = ("shopUrl", "storeUrl", "shopURL", "jumpUrl")
JD_LOC_KEYS = ("deliveryAddress", "area", "procity", "stockAddress", "sendAddr")
JD_TAG_LIST_KEYS = (
    "wareBeltList",
    "beltList",
    "labelList",
    "tagList",
    "wareTags",
    "tags",
    "icons",
    "serviceIcons",
    "iconList",
    "iconList1",
    "iconList2",
    "iconList3",
    "iconList4",
    "textTagList",
    "beltAttrs",
    "serviceTags",
    "benefitList",
    "sellPoints",
    "commonTagList",
)

# 单块对象里含 title:[str,…]（如 newRegionFloor）
JD_TAG_NEST_DICT_KEYS = (
    "newRegionFloor",
    "midTagList",
    "paragraphInfo",
    "floatLayerInfo",
    "drawer",
)

# JSON 里常见 title:["高膳食纤维","低GI食品"]、title:["礼盒装…热卖榜第1名"] 等字符串数组，需整树收集
JD_LIST_STRING_TEXT_KEYS = frozenset(
    {
        "title",
        "subtitle",
        "subtitles",
        "sellpoint",
        "sellpoints",
        "usp",
        "usps",
        "textlist",
        "textlists",
        "wordlist",
        "keywords",
        "highlight",
        "highlights",
        "sellingpoint",
        "sellingpoints",
        "featurelist",
        "pointlist",
        "points",
        "shorttitle",
        "shorttitles",
        "recommendreason",
        "reasonlist",
        "icontexts",
        "icontext",
        "tagtexts",
        "descs",
        "labels",
    }
)


def _sval_jd(d: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None or isinstance(v, (dict, list)):
            continue
        t = str(v).strip()
        if t:
            return t
    return ""


def _jd_flatten_ware(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for nest_key in ("wareInfo", "product", "skuInfo", "item", "main", "content", "base"):
        inner = out.get(nest_key)
        if isinstance(inner, dict):
            for k, v in inner.items():
                if k not in out or out[k] in (None, "", [], {}):
                    out[k] = v
    ex = out.get("exContent") or out.get("excontent")
    if isinstance(ex, dict):
        for k, v in ex.items():
            if k not in out or out[k] in (None, "", [], {}):
                out[k] = v
    return out


def _jd_norm_key(s: str) -> str:
    """去重用：压缩空白，便于合并「相同文案、空白不同」的重复。"""
    return " ".join(str(s).split()).strip()


def _jd_unique_ordered(strings: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in strings:
        t = _jd_norm_key(x)
        if len(t) < 2:
            continue
        k = t.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def _jd_benefit_list_title_lines(d: dict[str, Any]) -> list[str]:
    """benefitList[].title：PC 搜索里 sellingPoint 常为 null，卖点多在此。"""
    bl = d.get("benefitList")
    if not isinstance(bl, list):
        return []
    lines: list[str] = []
    for it in bl[:30]:
        if not isinstance(it, dict):
            continue
        t = it.get("title")
        if isinstance(t, list):
            for x in t[:25]:
                if isinstance(x, str) and x.strip():
                    lines.append(_jd_norm_key(x))
        elif isinstance(t, str) and t.strip():
            lines.append(_jd_norm_key(t))
    return lines


def _jd_sell_points_lines(d: dict[str, Any]) -> list[str]:
    """sellPoints：字符串列表或对象列表。"""
    sp = d.get("sellPoints")
    if not isinstance(sp, list):
        return []
    out: list[str] = []
    for x in sp[:25]:
        if isinstance(x, str) and x.strip():
            out.append(_jd_norm_key(x))
        elif isinstance(x, dict):
            t = _sval_jd(x, ("text", "title", "name", "desc"))
            if t:
                out.append(_jd_norm_key(t))
    return out


def _jd_selling_point_norm_set(d: dict[str, Any]) -> set[str]:
    """sellingPoint、benefitList.title、sellPoints 规范化键，用于树遍历去重。"""
    out: set[str] = set()
    sp = d.get("sellingPoint")
    if isinstance(sp, list):
        for x in sp:
            if isinstance(x, str) and x.strip():
                out.add(_jd_norm_key(x).casefold())
    for t in _jd_benefit_list_title_lines(d):
        out.add(t.casefold())
    for t in _jd_sell_points_lines(d):
        out.add(t.casefold())
    return out


def _jd_iter_tag_strings(d: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    sp_dup = _jd_selling_point_norm_set(d)

    def add(s: str, *, skip_if_selling_dup: bool = False) -> None:
        t = _jd_norm_key(s)
        if len(t) < 2:
            return
        if skip_if_selling_dup and sp_dup and t.casefold() in sp_dup:
            return
        k = t.casefold()
        if k in seen:
            return
        seen.add(k)
        out.append(t)

    sub_keys = (
        "text",
        "name",
        "title",
        "desc",
        "msg",
        "beltMsg",
        "labelName",
        "typeName",
        "showName",
        "content",
        "beltTitle",
    )
    for key in JD_TAG_LIST_KEYS:
        v = d.get(key)
        is_benefit = key == "benefitList"
        if isinstance(v, list):
            for it in v[:30]:
                if isinstance(it, str):
                    add(it)
                elif isinstance(it, dict):
                    for sk in sub_keys:
                        t = it.get(sk)
                        sk_skip = is_benefit and sk == "title"
                        if isinstance(t, list):
                            for x in t[:25]:
                                if isinstance(x, str) and x.strip():
                                    add(x.strip(), skip_if_selling_dup=sk_skip)
                        elif isinstance(t, str) and t.strip():
                            add(t.strip(), skip_if_selling_dup=sk_skip)
                            break
        elif isinstance(v, dict):
            for sk in sub_keys:
                t = v.get(sk)
                if isinstance(t, list):
                    for x in t[:25]:
                        if isinstance(x, str) and x.strip():
                            add(x.strip())
                elif isinstance(t, str) and t.strip():
                    add(t.strip())
                    break

    for key in JD_TAG_NEST_DICT_KEYS:
        v = d.get(key)
        if not isinstance(v, dict):
            continue
        tl = v.get("title")
        if isinstance(tl, list):
            for x in tl[:25]:
                if isinstance(x, str) and x.strip():
                    add(x.strip(), skip_if_selling_dup=True)
        elif isinstance(tl, str) and tl.strip():
            add(tl.strip(), skip_if_selling_dup=True)

    mtl = d.get("midTagList")
    if isinstance(mtl, list):
        for it in mtl[:25]:
            if not isinstance(it, dict):
                continue
            for sk in ("text", "name", "title", "desc", "msg"):
                t = it.get(sk)
                if isinstance(t, str) and t.strip():
                    add(t.strip())
                    break
    return out


def _jd_collect_list_string_fragments(
    root: Any,
    *,
    selling_norm_skip: set[str] | frozenset[str] | None = None,
    max_depth: int = 14,
    max_list_len: int = 40,
    max_branch_lists: int = 80,
) -> tuple[list[str], list[str]]:
    """
    从整棵 JSON 子树收集「键在 JD_LIST_STRING_TEXT_KEYS 且值为字符串数组」的文案。

    返回 (逐条短句, 每组用 · 拼接的整组)，供 hot_list_rank 等使用。
    """
    skip_sp = selling_norm_skip or set()
    individuals: list[str] = []
    groups: list[str] = []
    seen_ind: set[str] = set()
    seen_grp: set[str] = set()
    list_walk_count = 0

    def add_ind(s: str) -> None:
        t = _jd_norm_key(s)
        if len(t) < 2:
            return
        k = t.casefold()
        if skip_sp and k in skip_sp:
            return
        if k in seen_ind:
            return
        seen_ind.add(k)
        individuals.append(t)

    def walk(obj: Any, depth: int) -> None:
        nonlocal list_walk_count
        if depth > max_depth or obj is None:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                if (
                    isinstance(v, list)
                    and lk in JD_LIST_STRING_TEXT_KEYS
                    and list_walk_count < max_branch_lists
                ):
                    list_walk_count += 1
                    elems: list[str] = []
                    for x in v[:max_list_len]:
                        if isinstance(x, str) and x.strip():
                            t = _jd_norm_key(x)
                            if skip_sp and t.casefold() in skip_sp:
                                continue
                            elems.append(t)
                            add_ind(t)
                        elif isinstance(x, dict):
                            t = _sval_jd(
                                x,
                                ("text", "name", "title", "desc", "value", "content"),
                            )
                            if t:
                                t = _jd_norm_key(t)
                                if skip_sp and t.casefold() in skip_sp:
                                    continue
                                elems.append(t)
                                add_ind(t)
                    if elems:
                        gline = " · ".join(elems)
                        gk = gline.casefold()
                        if gk not in seen_grp:
                            seen_grp.add(gk)
                            groups.append(gline)
                walk(v, depth + 1)
        elif isinstance(obj, list):
            for x in obj[:90]:
                walk(x, depth + 1)

    walk(root, 0)
    return individuals, groups


# commentSalesFloor 等 attr+text 格式化时跳过明显非文案类 attr
JD_ATTR_TEXT_SKIP_ATTRS = frozenset(
    {
        "wareid",
        "skuid",
        "itemid",
        "imageurl",
        "imgurl",
        "href",
        "url",
        "cid",
        "shopid",
        "venderid",
    }
)


def _jd_is_rank_text(s: str) -> bool:
    t = s.strip()
    if not t:
        return False
    if "榜" in t:
        return True
    if "TOP" in t.upper() and len(t) <= 48:
        return True
    if re.search(r"第\s*\d+\s*名", t):
        return True
    if "热销" in t and len(t) <= 36:
        return True
    return False


def _jd_format_price_show_dict(ps: dict[str, Any]) -> tuple[str, str]:
    parts: list[str] = []
    coupon = ""
    for k, v in ps.items():
        if isinstance(v, str) and v.strip():
            parts.append(f"{k}={v.strip()[:100]}")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            parts.append(f"{k}={v}")
    line = " | ".join(parts)[:500]
    for ck in ("couponPrice", "purchasePrice", "finalPrice", "subsidyPrice"):
        v = ps.get(ck)
        if isinstance(v, str) and v.strip():
            coupon = v.strip()[:80]
            break
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            coupon = str(v)
            break
    return line, coupon


def _jd_parse_final_price(d: dict[str, Any]) -> tuple[str, str]:
    """
    pc 搜索 ware.finalPrice：到手价 + estimatedPrice。
    返回 (estimatedPrice 字符串, 展示用「到手价:25.5」)。
    """
    for fk in ("finalPrice", "mockFinalPrice", "intervalNewPrice"):
        fp = d.get(fk)
        if not isinstance(fp, dict):
            continue
        est = fp.get("estimatedPrice")
        if est is None:
            est = fp.get("price")
        tit = fp.get("title")
        tit_s = str(tit).strip() if tit is not None else ""
        if not tit_s:
            tit_s = "到手价"
        es = str(est).strip() if est is not None else ""
        if not es:
            continue
        return es, f"{tit_s}:{es}"
    return "", ""


def _jd_selling_point_text(d: dict[str, Any]) -> str:
    """优先根字段 sellingPoint；为空时用 benefitList / sellPoints（接口常返回 sellingPoint=null）。"""
    lines: list[str] = []
    sp = d.get("sellingPoint")
    if isinstance(sp, list):
        lines = [
            _jd_norm_key(x) for x in sp[:25] if isinstance(x, str) and x.strip()
        ]
    if not lines:
        lines = _jd_benefit_list_title_lines(d) + _jd_sell_points_lines(d)
    return _human_text(" | ".join(_jd_unique_ordered(lines)), 1200)


def _jd_format_comment_sales_floor(d: dict[str, Any]) -> str:
    """仅根字段 commentSalesFloor：[{attr,text},…]。"""
    csf = d.get("commentSalesFloor")
    if not isinstance(csf, list):
        return ""
    parts: list[str] = []
    for el in csf[:30]:
        if not isinstance(el, dict):
            continue
        text = el.get("text") or el.get("msg") or el.get("desc")
        if not isinstance(text, str) or not text.strip():
            continue
        t = _jd_norm_key(text)
        attr = el.get("attr") or el.get("type") or el.get("key") or ""
        if isinstance(attr, str) and attr.strip():
            lk = attr.strip().lower()
            if lk in JD_ATTR_TEXT_SKIP_ATTRS:
                parts.append(t)
            else:
                parts.append(f"{attr.strip()}:{t}")
        else:
            parts.append(t)
    return _human_text(" | ".join(parts), 800)


def _jd_seckill_text(d: dict[str, Any]) -> str:
    for k in ("seckillInfo", "secKill", "secKillInfo", "miaosha", "flashSale"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:400]
        if isinstance(v, dict):
            t = _sval_jd(v, ("text", "title", "name", "status"))
            if t:
                return t[:400]
    return ""


def _jd_attributes_line(d: dict[str, Any]) -> str:
    chunks: list[str] = []
    for pl_key in ("propertyList", "properties", "wareProps", "props"):
        pl = d.get(pl_key)
        if not isinstance(pl, list):
            continue
        for el in pl[:25]:
            if isinstance(el, dict):
                k = str(el.get("name") or el.get("key") or "").strip()
                v = str(el.get("value") or el.get("text") or "").strip()
                if k and v:
                    chunks.append(f"{k}:{v}")
            elif isinstance(el, str) and el.strip():
                chunks.append(el.strip()[:120])
        if chunks:
            break
    return " | ".join(chunks)[:4000]


def _jd_attributes_line_full(d: dict[str, Any]) -> str:
    """属性行 + pc 搜索常见 color / catid / shortName。"""
    base = _jd_attributes_line(d)
    extra: list[str] = []
    c = d.get("color")
    if isinstance(c, str) and c.strip():
        extra.append(f"颜色规格:{c.strip()}")
    cid = d.get("catid") or d.get("cid3") or d.get("cid3Name")
    if cid is not None and str(cid).strip():
        extra.append(f"类目:{str(cid).strip()}")
    sn = d.get("shortName")
    if isinstance(sn, str) and sn.strip():
        extra.append(f"简称:{sn.strip()}")
    parts = [p for p in extra if p]
    if base:
        parts.append(base)
    return " | ".join(parts)[:4000]


def _jd_is_p4p_flag(d: dict[str, Any]) -> str:
    for k in ("isAdv", "isAd", "isP4p", "is_p4p", "adFlag"):
        v = d.get(k)
        if v is None:
            continue
        return str(v).strip()[:20]
    if d.get("extension_id") or d.get("extensionId") or d.get("adLog"):
        return "true"
    return ""


def _jd_is_explicit_pc_search_ad_ware(d0: dict[str, Any]) -> bool:
    """
    仅显式广告标记。extensionId/adLog 在自然商品上也常见，不能用来扣减 body.s。
    """
    for k in ("isAdv", "isAd", "isP4p", "is_p4p", "adFlag"):
        v = d0.get(k)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y"):
            return True
    return False


def _pc_search_s_delta_from_natural_slot_count(n_natural: int, slot_total: int) -> int:
    """
    pc_search 两包满屏：下一包 ``body.s = 上一包 s + Δ``，抓包为 **Δ = 自然位条数 - 1**。
    例：首包 ``s=1``、``wareList`` 非广告 22 条 → 次包 ``s=22``（即 ``1+21``）。
    """
    cnt = n_natural if n_natural > 0 else max(0, slot_total)
    return max(1, cnt - 1)


def _pc_search_body_s_step_from_raw_ware_list(raw_list: list[dict[str, Any]]) -> int:
    """
    无 ``wareList`` 时的兜底：树遍历去重 SKU + 去显式广告得自然位数，再套 ``Δ=自然位-1``。
    """
    seen_sku: set[str] = set()
    n = 0
    for obj in raw_list:
        d0 = _jd_flatten_ware(obj)
        if _jd_is_explicit_pc_search_ad_ware(d0):
            continue
        sku = _sval_jd(d0, JD_SKU_KEYS)
        if sku.isdigit() and len(sku) >= 5:
            if sku in seen_sku:
                continue
            seen_sku.add(sku)
        n += 1
    return _pc_search_s_delta_from_natural_slot_count(n, len(raw_list))


def _pc_search_s_step_from_payload_data_ware_list(payload: Any) -> int | None:
    """
    与抓包 ``response1.js`` + 第二包请求（``page=2,s=22``）一致：
    ``data.wareList`` 内非广告条数为 ``N`` 时，**``s`` 增量为 ``max(1, N-1)``**。
    """
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    wl = data.get("wareList")
    if not isinstance(wl, list) or len(wl) == 0:
        return None
    if not all(isinstance(x, dict) for x in wl):
        return None
    n = 0
    for w in wl:
        d0 = _jd_flatten_ware(w)
        if _jd_is_explicit_pc_search_ad_ware(d0):
            continue
        n += 1
    return _pc_search_s_delta_from_natural_slot_count(n, len(wl))


def pc_search_ware_list_slot_count_from_body(text: str) -> int | None:
    """Return len(data.wareList) from pc_search JSON/JSONP body, or None."""
    payload = _loads_json_or_jsonp(text)
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    wl = data.get("wareList")
    return len(wl) if isinstance(wl, list) else None


def _jd_coerce_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip():
        t = v.strip()
        if t.lstrip("-").isdigit():
            try:
                return int(t)
            except ValueError:
                return None
    return None


def _extract_pc_search_next_s_from_payload(
    payload: Any, *, request_page: int, request_s: int
) -> int | None:
    want_p = request_page + 1
    cands: list[int] = []

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            p = _jd_coerce_int(o.get("page"))
            if p is None:
                p = _jd_coerce_int(o.get("pageNo")) or _jd_coerce_int(
                    o.get("pageIndex")
                )
            s_v = _jd_coerce_int(o.get("s"))
            if p == want_p and s_v is not None and s_v > request_s:
                cands.append(s_v)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(payload)
    return min(cands) if cands else None


def _looks_like_jd_ware(d: dict[str, Any]) -> bool:
    d0 = _jd_flatten_ware(d)
    sku = _sval_jd(d0, JD_SKU_KEYS)
    if not (sku.isdigit() and len(sku) >= 5):
        return False
    title = _sval_jd(d0, JD_TITLE_KEYS)
    return len(title) >= 2


def _normalize_jd_api_row(d: dict[str, Any], *, keyword: str, page: int) -> dict[str, str]:
    """
    将 pc_search_searchWare 单条 ware（及同类结构）规整为与淘宝 CSV 对齐的宽表。

    主路径字段：wareId/skuId、wareName、jdPrice/jdPriceText、oriPrice、finalPrice（到手价）、
    sellingPoint、commentFuzzy（评价量）、commentSalesFloor、benefitList/newRegionFloor、
    iconList1–4、promotionSet、wareBuried、shopId/shopName、color/catid、isAdv/extensionId 等；
    仍保留树遍历兜底以兼容字段变更。
    """
    d = _jd_flatten_ware(d)
    sku = _sval_jd(d, JD_SKU_KEYS)
    ware = _sval_jd(d, ("wareId", "wid"))
    item_id = ware if ware else sku

    title = _human_text(_sval_jd(d, JD_TITLE_KEYS), 2000)
    price = _human_text(_sval_jd(d, JD_PRICE_KEYS), 120)
    orig = _human_text(_sval_jd(d, JD_ORIGINAL_PRICE_KEYS), 120)
    coupon_p = _human_text(_sval_jd(d, JD_COUPON_KEYS), 80)
    fp_coupon, _ = _jd_parse_final_price(d)
    if fp_coupon:
        if not coupon_p:
            coupon_p = fp_coupon
        elif fp_coupon not in coupon_p:
            coupon_p = _human_text(f"{coupon_p}/{fp_coupon}", 120)

    ps = d.get("priceShow")
    _, ps_coupon = (
        _jd_format_price_show_dict(ps) if isinstance(ps, dict) else ("", "")
    )
    if not coupon_p and ps_coupon:
        coupon_p = ps_coupon

    selling_point = _jd_selling_point_text(d)
    comment_count = _human_text(
        _sval_jd(
            d,
            ("commentFuzzy", "comment_fuzzy", "cmtFuzzy", "evaluationFuzzy"),
        ),
        120,
    )
    comment_sales_floor = _jd_format_comment_sales_floor(d)

    tag_strs = _jd_iter_tag_strings(d)
    arr_ind, _arr_groups = _jd_collect_list_string_fragments(
        d, selling_norm_skip=_jd_selling_point_norm_set(d)
    )
    tag_seen_norm = {t.casefold() for t in tag_strs}
    extra_from_arrays = [
        x for x in arr_ind if _jd_norm_key(x).casefold() not in tag_seen_norm
    ]

    merged_for_signals = _jd_unique_ordered(tag_strs + extra_from_arrays)
    rank_parts = _jd_unique_ordered(
        [t for t in merged_for_signals if _jd_is_rank_text(t)]
    )

    hot_list_rank = _human_text(" | ".join(rank_parts), 600)

    shop = _human_text(
        _sval_jd(d, ("shopName", "venderName", "storeName", "shop_name")), 200
    )
    shop_url = _safe_url(_sval_jd(d, JD_SHOP_URL_KEYS))[:2000]
    if not shop_url:
        sid = d.get("shopId")
        if sid is not None and str(sid).strip():
            shop_url = f"https://mall.jd.com/index-{str(sid).strip()}.html"[:2000]
    shop_info_url = _safe_url(_sval_jd(d, ("shopInfoUrl", "brandUrl")))[:2000]

    loc = _human_text(_sval_jd(d, JD_LOC_KEYS), 200)

    detail_raw = _sval_jd(d, JD_DETAIL_URL_KEYS)
    if detail_raw:
        detail_url = _safe_url(detail_raw)[:2500]
    else:
        detail_url = (
            f"https://item.m.jd.com/product/{sku}.html" if sku else ""
        )

    img_raw = _sval_jd(
        d,
        JD_IMG_KEYS + ("squareImage", "squarePic", "imgDfsUrl"),
    )
    image = _jd_product_image_url(img_raw)[:1200] if img_raw else ""

    seckill = _jd_seckill_text(d)
    attributes = _jd_attributes_line_full(d)
    leaf = str(
        d.get("leafCategory") or d.get("cid3Name") or d.get("catid") or ""
    ).strip()[:80]
    samec = str(d.get("sameStyleCount") or d.get("sameCount") or "").strip()[:40]
    rel = str(d.get("relationScore") or d.get("score") or "").strip()[:40]
    is_p4p = _jd_is_p4p_flag(d)

    out = _jd_empty_export_row()
    out["item_id"] = item_id[:80]
    out["sku_id"] = sku[:80]
    out["title"] = title
    out["title_plain"] = title
    out["price"] = price
    out["coupon_price"] = coupon_p
    out["original_price"] = orig
    out["selling_point"] = selling_point
    out["comment_sales_floor"] = comment_sales_floor
    out["hot_list_rank"] = hot_list_rank
    out["comment_count"] = comment_count
    out["shop_name"] = shop
    out["shop_url"] = shop_url
    out["shop_info_url"] = shop_info_url
    out["location"] = loc
    out["detail_url"] = detail_url
    out["image"] = image
    out["seckill_info"] = _human_text(seckill, 400)
    out["attributes"] = attributes
    out["leaf_category"] = leaf
    out["same_count"] = samec
    out["relation_score"] = rel
    out["is_p4p"] = is_p4p
    out["platform"] = "京东"
    out["keyword"] = keyword
    out["page"] = str(page)
    return out


def _walk_collect_jd_wares(obj: Any, acc: list[dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        if _looks_like_jd_ware(obj):
            acc.append(obj)
        for v in obj.values():
            _walk_collect_jd_wares(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _walk_collect_jd_wares(x, acc)


def _parse_jd_json_payload_rows_and_ware_slots(
    payload: Any, *, keyword: str, page: int
) -> tuple[list[dict[str, str]], int, int]:
    """
    一次遍历：导出商品行、树遍历 ware 数（兜底）、以及 **body.s 用的自然位步长**。
    """
    raw_list: list[dict[str, Any]] = []
    _walk_collect_jd_wares(payload, raw_list)
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for d in raw_list:
        row = _normalize_jd_api_row(d, keyword=keyword, page=page)
        sku = row.get("sku_id", "")
        if not sku or sku in seen:
            continue
        seen.add(sku)
        rows.append(row)
    s_slots = len(raw_list)
    s_scroll = _pc_search_body_s_step_from_raw_ware_list(raw_list)
    return rows, s_slots, s_scroll


def parse_items_from_jd_json_payload(payload: Any, *, keyword: str, page: int) -> list[dict[str, str]]:
    rows, _, _ = _parse_jd_json_payload_rows_and_ware_slots(
        payload, keyword=keyword, page=page
    )
    return rows


def parse_items_and_pc_search_s_step_from_response_body(
    text: str,
    *,
    keyword: str,
    page: int,
    request_api_page: int | None = None,
    request_body_s: int | None = None,
) -> tuple[list[dict[str, str]], int]:
    """
    解析商品列表，并给出本次响应对应的 **body.s 游标增量**。

    JSON：优先顺序：① 响应内嵌下一跳 ``s``；② ``data.wareList`` 得自然位 ``N`` → **Δs=max(1,N-1)**；
    ③ 树遍历兜底，同上 Δ 规则；无 ``wareList`` 时对 ``len(rows)`` 亦用 Δ。
    HTML：增量仍为解析行数（非 pc_search 两包逻辑）。
    """
    payload = _loads_json_or_jsonp(text)
    if payload is not None:
        rows, s_slots, s_scroll = _parse_jd_json_payload_rows_and_ware_slots(
            payload, keyword=keyword, page=page
        )
        step_api: int | None = None
        if request_api_page is not None and request_body_s is not None:
            next_s = _extract_pc_search_next_s_from_payload(
                payload,
                request_page=request_api_page,
                request_s=request_body_s,
            )
            if next_s is not None:
                d = next_s - request_body_s
                if d > 0:
                    step_api = d
        step_wl = _pc_search_s_step_from_payload_data_ware_list(payload)
        if rows:
            if step_api is not None:
                step = step_api
            elif step_wl is not None:
                step = step_wl
            else:
                step = s_scroll if s_scroll > 0 else _pc_search_s_delta_from_natural_slot_count(
                    len(rows), len(rows)
                )
            return rows, max(1, step)
        if s_slots > 0:
            if step_api is not None:
                step = step_api
            elif step_wl is not None:
                step = step_wl
            else:
                step = (
                    s_scroll
                    if s_scroll > 0
                    else _pc_search_s_delta_from_natural_slot_count(0, s_slots)
                )
            return [], max(1, step)
        # 结构与 ware 识别不匹配时，与 parse_items_from_response_body 一样再试 HTML
    rows = parse_items_from_html(text, keyword=keyword, page=page)
    return rows, (len(rows) if rows else 0)


def parse_items_from_response_body(text: str, *, keyword: str, page: int) -> list[dict[str, str]]:
    """先尝试 JSON/JSONP（client.action），失败再按 HTML 解析。"""
    payload = _loads_json_or_jsonp(text)
    if payload is not None:
        rows = parse_items_from_jd_json_payload(payload, keyword=keyword, page=page)
        if rows:
            return rows
    return parse_items_from_html(text, keyword=keyword, page=page)


def _jd_minimal_html_row(
    *,
    keyword: str,
    page: int,
    sku_id: str,
    title: str,
    price: str,
    detail_url: str,
    shop_name: str,
    comment_count: str,
    image: str,
) -> dict[str, str]:
    out = _jd_empty_export_row()
    out["item_id"] = sku_id
    out["sku_id"] = sku_id
    out["title"] = title
    out["title_plain"] = title
    out["price"] = price
    out["detail_url"] = detail_url
    out["shop_name"] = shop_name
    out["comment_count"] = comment_count
    out["image"] = image
    out["platform"] = "京东"
    out["keyword"] = keyword
    out["page"] = str(page)
    return out


def _collect_items_from_json_like(
    html_text: str, keyword: str, page: int
) -> list[dict[str, str]]:
    """
    策略 A：从内嵌 JSON/脚本片段中用正则抓取 wareId/wareName/price 等。
    该策略对结构变更相对鲁棒，但字段名可能变化，因此做多套模板。
    """
    items: list[dict[str, str]] = []

    # 常见字段组合：wareId + wareName + jdPrice / price / priceText
    patterns: list[re.Pattern[str]] = [
        re.compile(
            r'"wareId"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"wareName"\s*:\s*"(?P<title>[^"]{2,300})"'
            r'[\s\S]{0,1200}?'
            r'"(?:jdPrice|price|priceText|mainPrice)"\s*:\s*"?(?P<price>[\d.]{1,12})"?',
            re.I,
        ),
        re.compile(
            r'"skuId"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"title"\s*:\s*"(?P<title>[^"]{2,300})"'
            r'[\s\S]{0,1200}?'
            r'"(?:price|priceText|jdPrice)"\s*:\s*"?(?P<price>[\d.]{1,12})"?',
            re.I,
        ),
        # 兜底：只要 sku + title，价格缺失也接受
        re.compile(
            r'"(?:wareId|skuId)"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"(?:wareName|title|name)"\s*:\s*"(?P<title>[^"]{2,300})"',
            re.I,
        ),
    ]

    seen: set[str] = set()
    for pat in patterns:
        for m in pat.finditer(html_text):
            sku = (m.groupdict().get("sku") or "").strip()
            title = _human_text(m.groupdict().get("title") or "", 300)
            price = (m.groupdict().get("price") or "").strip()
            if not sku or not title:
                continue
            if sku in seen:
                continue
            seen.add(sku)
            detail = f"https://item.m.jd.com/product/{sku}.html"
            items.append(
                _jd_minimal_html_row(
                    keyword=keyword,
                    page=page,
                    sku_id=sku,
                    title=title,
                    price=price,
                    detail_url=detail,
                    shop_name="",
                    comment_count="",
                    image="",
                )
            )
        if items:
            # 命中一套 pattern 后就不再叠加下一套，避免重复/误配
            break
    return items


def _collect_items_from_dom(html_text: str, keyword: str, page: int) -> list[dict[str, str]]:
    """
    策略 B：从 DOM/属性中抓取 data-sku + title/price/href。
    不依赖 BeautifulSoup（零依赖），但对结构变化更敏感。
    """
    items: list[dict[str, str]] = []
    seen: set[str] = set()

    # 以 data-sku 为锚点，截取一个窗口做二次抽取
    for m in re.finditer(r'data-sku\s*=\s*"(?P<sku>\d{5,20})"', html_text, re.I):
        sku = (m.group("sku") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)

        win = html_text[m.start() : m.start() + 4500]

        # 链接
        href = ""
        mh = re.search(r'href\s*=\s*"([^"]+)"', win, re.I)
        if mh:
            href = _safe_url(mh.group(1))
        if not href:
            href = f"https://item.m.jd.com/product/{sku}.html"

        # 标题
        title = ""
        for tp in (
            r'title\s*=\s*"([^"]{2,300})"',
            r'alt\s*=\s*"([^"]{2,300})"',
            r'data-name\s*=\s*"([^"]{2,300})"',
        ):
            mt = re.search(tp, win, re.I)
            if mt:
                title = _human_text(mt.group(1), 300)
                break

        # 价格（HTML 上可能是 ¥xx.xx / &yen;xx.xx）
        price = ""
        mp = re.search(r"(?:¥|&yen;)\s*([\d.]{1,12})", win)
        if mp:
            price = mp.group(1).strip()

        # 店铺（H5 列表可能没有）
        shop = ""
        ms = re.search(r'data-shopname\s*=\s*"([^"]{2,80})"', win, re.I)
        if ms:
            shop = _human_text(ms.group(1), 80)

        # 图片
        image = ""
        mi = re.search(r'(?:data-lazy-img|data-img|src)\s*=\s*"([^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', win, re.I)
        if mi:
            image = _jd_product_image_url(mi.group(1))[:1200]

        if not title:
            # 标题拿不到时宁可跳过，避免充斥“空标题”
            continue

        items.append(
            _jd_minimal_html_row(
                keyword=keyword,
                page=page,
                sku_id=sku,
                title=title,
                price=price,
                detail_url=href[:2000],
                shop_name=shop,
                comment_count="",
                image=image,
            )
        )

    return items


def parse_items_from_html(html_text: str, *, keyword: str, page: int) -> list[dict[str, str]]:
    # 优先尝试 JSON-like（更稳），再退回 DOM
    parsed = _collect_items_from_json_like(html_text, keyword, page)
    if not parsed:
        parsed = _collect_items_from_dom(html_text, keyword, page)

    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for row in parsed:
        sku = (row.get("sku_id") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        rows.append(row)
    return rows


def _normalize_url_keep_query(url: str, **override: str) -> str:
    """
    用于 --url 模式下“覆写” keyword/page 等参数（若用户希望用同一 URL 只改 page）。
    这里较保守：只在 query 中覆盖字段，不碰 path/host。
    """
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    for k, v in override.items():
        q[k] = [v]
    new_query = urlencode({k: v[0] for k, v in q.items()}, doseq=False, safe=":%/,+")
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def iter_request_urls(args: argparse.Namespace) -> list[tuple[int, str]]:
    """
    返回 [(page_idx, url), ...]。
    client.action 等与 sign 绑定的 URL 禁止改 query（否则会破坏 sign），故只请求一次。
    """
    out: list[tuple[int, str]] = []
    if getattr(args, "url_file", None):
        path = args.url_file
        if not os.path.isfile(path):
            print(f"--url-file 不存在: {path}", file=sys.stderr)
            sys.exit(2)
        urls = read_url_list_file(path)
        if not urls:
            print("--url-file 中没有有效 URL", file=sys.stderr)
            sys.exit(2)
        for i, u in enumerate(urls):
            out.append((i + 1, u.strip()))
        return out
    if getattr(args, "api_params_file", None):
        path = args.api_params_file
        if not os.path.isfile(path):
            print(f"--api-params-file 不存在: {path}", file=sys.stderr)
            sys.exit(2)
        try:
            pairs = parse_jd_api_params_text(Path(path).read_text(encoding="utf-8"))
        except ValueError as e:
            print(f"解析 --api-params-file 失败: {e}", file=sys.stderr)
            sys.exit(2)
        if not pairs:
            print("--api-params-file 解析结果为空", file=sys.stderr)
            sys.exit(2)
        out.append((args.page, build_jd_api_url_from_param_pairs(pairs)))
        return out
    if args.url:
        u0 = args.url.strip()
        if _is_h5_ware_search_url(u0):
            pe = args.page_to if args.page_to is not None else args.page
            for p in range(args.page, pe + 1):
                out.append((p, _normalize_url_keep_query(u0, keyword=args.q, page=str(p))))
        else:
            out.append((args.page, u0))
    else:
        pe = args.page_to if args.page_to is not None else args.page
        for p in range(args.page, pe + 1):
            out.append((p, build_h5_search_url(args.q, p)))
    return out


def fetch_html(session: requests.Session, url: str, headers: dict[str, str], timeout: float) -> str:
    r = session.get(url, headers=headers, timeout=timeout)
    if r.status_code == 403 and _is_jd_api_url(url):
        print(
            "[京东] api.m.jd.com 返回 HTTP 403（请核对 Cookie、h5st 与 query 是否一致）。",
            file=sys.stderr,
        )
        print(
            f"  响应头 server={r.headers.get('server')!r} content-length={r.headers.get('content-length')!r}",
            file=sys.stderr,
        )
    r.raise_for_status()
    return r.text


def main() -> None:
    p = argparse.ArgumentParser(description="京东搜索 GET（requests）：H5 HTML 或 api JSON")
    p.add_argument("--url", help="浏览器里复制的完整请求 URL")
    p.add_argument(
        "--url-file",
        metavar="PATH",
        help="每行一条完整 URL（多页各复制一条；# 行为注释）",
    )
    p.add_argument(
        "--api-params-file",
        metavar="PATH",
        help="粘贴 Query 参数为文本：每行 key=value，或键一行/值一行；拼成 https://api.m.jd.com/api?...",
    )
    p.add_argument(
        "--search-referer",
        default="",
        metavar="URL",
        help="可选：api.m.jd.com 请求的 Referer 覆盖",
    )
    p.add_argument("--cookie", default="", help="Cookie 请求头（优先于 MY_COOKIE / jd_cookie.txt）")
    p.add_argument(
        "--cookie-file",
        help="从指定文件读取 Cookie；默认会尝试 ../common/jd_cookie.txt",
    )
    p.add_argument("--timeout", type=float, default=30.0, help="HTTP 超时秒数")
    p.add_argument("--raw", action="store_true", help="仅打印原始响应体（便于排查）")
    p.add_argument(
        "--ua-preset",
        choices=tuple(UA_PRESETS.keys()),
        default="chrome_win",
        help="User-Agent 预设；search.action 被「去 APP」拦截时可试 iphone_m / android_m",
    )
    p.add_argument(
        "--q",
        default="低GI",
        help="搜索词：pc API（默认）或 --h5-html 时 search.action；或覆盖 --url 里 search.action 的 keyword",
    )
    p.add_argument(
        "--h5-html",
        action="store_true",
        help="仅 --q 且无 --url 等时：从 so.m.jd.com search.action 拉 HTML 解析商品；默认改为与 jd_search_playwright 相同的 pc API（Node 出 url+headers）",
    )
    p.add_argument(
        "--page",
        type=int,
        default=1,
        help="起始逻辑页（从 1 起；不是请求 body.page。第 L 页对应两次 API：body.page=2L-1 与 2L）",
    )
    p.add_argument(
        "--page-to",
        type=int,
        default=None,
        metavar="N",
        help="结束逻辑页（含）；与 --page 搭配；每逻辑页固定 2 包 API。",
    )
    p.add_argument(
        "--page-delay",
        type=float,
        default=1.2,
        help="多页时逻辑页之间的额外间隔秒数（与 --request-delay 可叠加）",
    )
    p.add_argument(
        "--request-delay",
        default=None,
        metavar="MIN-MAX",
        help="每次 pc_search 完成后到下一次之间的随机等待（秒），如 30-60；不设则仅逻辑页间用 --page-delay",
    )
    p.add_argument(
        "--fetch-retries",
        type=int,
        default=3,
        metavar="N",
        help="同一 body.page/s 遇空包或零解析时，除首次外最多再试 N 次（默认 3，即最多共 4 次请求）",
    )
    p.add_argument(
        "--fetch-retry-delay",
        type=float,
        default=2.0,
        help="上述重试之间的间隔秒数（不走 --request-delay 长间隔）",
    )
    p.add_argument(
        "--pvid",
        default="",
        metavar="ID",
        help="pc_search body 与 Referer 的 pvid（与搜索页 URL 一致）；空则 Node 用默认 pvid",
    )
    p.add_argument(
        "--format",
        choices=("raw", "items"),
        default="items",
        help="raw=原始 HTML；items=规整商品列表（JSON 或 CSV）",
    )
    p.add_argument("--csv", action="store_true", help="仅与 --format items 合用：输出 CSV（--out 写文件）")
    p.add_argument("--out", help="raw 时写 HTML；items 时写商品 JSON 或 CSV（见 --csv）")
    args = p.parse_args()

    mode_count = sum(
        1
        for x in (
            bool(args.url and str(args.url).strip()),
            bool(getattr(args, "url_file", None)),
            bool(getattr(args, "api_params_file", None)),
        )
        if x
    )
    if mode_count > 1:
        print("--url、--url-file、--api-params-file 只能三选一", file=sys.stderr)
        sys.exit(2)

    if args.page_to is not None and args.page_to < args.page:
        print("--page-to 必须大于等于 --page", file=sys.stderr)
        sys.exit(2)
    if args.raw and args.page_to is not None:
        print("--raw 与 --page-to 请分开使用", file=sys.stderr)
        sys.exit(2)
    if args.csv and args.format != "items":
        print("--csv 需与 --format items 同时使用", file=sys.stderr)
        sys.exit(2)
    if getattr(args, "url_file", None) and args.page_to is not None:
        print("--url-file 已可写多行 URL，请勿再使用 --page-to", file=sys.stderr)
        sys.exit(2)
    if (
        getattr(args, "api_params_file", None)
        and args.page_to is not None
        and args.page_to > args.page
    ):
        print(
            "--api-params-file 仅生成单条 URL；翻页请更新 body/h5st 后换新文件，或改用 --url-file",
            file=sys.stderr,
        )
        sys.exit(2)
    if (
        args.url
        and not _is_h5_ware_search_url(args.url)
        and args.page_to is not None
        and args.page_to > args.page
    ):
        print(
            "非 search.action 的 --url（例如带 h5st 的 api）与参数绑定，不能使用 --page-to；"
            "请每页在浏览器 Network 中分别复制完整 URL，或使用 --url-file。",
            file=sys.stderr,
        )
        sys.exit(2)

    req_delay_range: tuple[float, float] | None = None
    if getattr(args, "request_delay", None):
        try:
            req_delay_range = parse_request_delay_range(args.request_delay)
        except ValueError as e:
            print(f"[京东] --request-delay 无效: {e}", file=sys.stderr)
            sys.exit(2)
    if getattr(args, "fetch_retries", 0) < 0:
        print("[京东] --fetch-retries 不能为负", file=sys.stderr)
        sys.exit(2)

    cookie = load_cookie(args)
    session = requests.Session()
    session.trust_env = False

    referer_ov = (args.search_referer or "").strip() or None

    use_pc_api_like_playwright = (
        mode_count == 0
        and not args.h5_html
        and args.format == "items"
    )
    _node_cookie_file: str | None = None
    _cfa = getattr(args, "cookie_file", None)
    if _cfa and str(_cfa).strip():
        _p = Path(str(_cfa).strip()).expanduser().resolve()
        if _p.is_file():
            _node_cookie_file = str(_p)
    if use_pc_api_like_playwright and str(args.cookie or "").strip() and not _node_cookie_file:
        print(
            "[京东] 提示：默认 pc API 路径下 h5st 由 Node 生成，Cookie 来自 common/jd_cookie.txt 或 --cookie-file；"
            "仅 --cookie 字符串不会传给 Node。",
            file=sys.stderr,
        )

    def dump_out(text: str) -> None:
        if args.out:
            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.out).write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text)

    kw_for_row = args.q
    apf = getattr(args, "api_params_file", None)
    if apf:
        try:
            for k, v in parse_jd_api_params_text(Path(apf).read_text(encoding="utf-8")):
                if k == "keyword":
                    kw_for_row = v
                    break
        except ValueError:
            pass

    # --- raw：仅输出第一页 URL 的响应（避免与 --page-to 语义冲突）---
    if args.format == "raw" or args.raw:
        plan_raw = list(iter_request_urls(args))
        if len(plan_raw) > 1:
            print("提示：--raw 仅输出 plan 中第一条 URL 的响应体。", file=sys.stderr)
        _, url = plan_raw[0]
        headers = build_request_headers(
            cookie,
            request_url=url,
            ua_preset=args.ua_preset,
            referer_override=referer_ov,
        )
        body = fetch_html(session, url, headers, args.timeout)
        dump_out(body)
        return

    # --- items：多 URL 合并去重（默认与 jd_search_playwright 相同 url+headers）---
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()

    if use_pc_api_like_playwright:
        page_start = max(1, args.page)
        pe = args.page_to if args.page_to is not None else page_start
        pe = max(page_start, pe)
        n_pc_pages = pe - page_start + 1

        api_page = 1
        api_s = 1
        run_aborted = False
        node_pvid = (args.pvid or "").strip() or None

        def _fetch_pc_body(ap: int, as_: int) -> tuple[str, str]:
            data = export_pc_search_request_json(
                args.q,
                ap,
                s=as_,
                pvid=node_pvid,
                cookie_file=_node_cookie_file,
            )
            u = data["url"]
            hdrs = {str(k): str(v) for k, v in data["headers"].items()}
            return u, fetch_html(session, u, hdrs, args.timeout)

        _pc_fetch_n = 0

        def _fetch_pc_body_spaced(ap: int, as_: int) -> tuple[str, str]:
            nonlocal _pc_fetch_n
            if _pc_fetch_n > 0:
                sleep_pc_search_request_gap(req_delay_range)
            u, b = _fetch_pc_body(ap, as_)
            _pc_fetch_n += 1
            return u, b

        max_fetch_tries = max(1, int(args.fetch_retries) + 1)
        retry_pause = max(0.0, float(args.fetch_retry_delay))
        last_s_step = JD_PC_SEARCH_FALLBACK_S_STEP

        for _skip_screen in range(max(0, page_start - 1)):
            for _skip_chunk in range(JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE):
                url, body = "", ""
                _skip_rows: list[dict[str, str]] = []
                s_step = 0
                blocked: str | None = None
                for rt in range(max_fetch_tries):
                    if rt == 0:
                        url, body = _fetch_pc_body_spaced(api_page, api_s)
                    else:
                        if retry_pause > 0:
                            time.sleep(retry_pause)
                        print(
                            f"[京东] 跳过前序屏 重试 {rt}/{max_fetch_tries - 1} "
                            f"body.page={api_page} body.s={api_s}",
                            file=sys.stderr,
                        )
                        url, body = _fetch_pc_body(api_page, api_s)
                    blocked = _detect_blocked(body)
                    if blocked:
                        break
                    _skip_rows, s_step = (
                        parse_items_and_pc_search_s_step_from_response_body(
                            body,
                            keyword=kw_for_row,
                            page=page_start,
                            request_api_page=api_page,
                            request_body_s=api_s,
                        )
                    )
                    if _skip_rows or s_step > 0:
                        break
                    if rt + 1 < max_fetch_tries and pc_search_should_retry_fetch(
                        body, has_rows=bool(_skip_rows), s_step=s_step
                    ):
                        continue
                    break
                if blocked:
                    print(
                        f"[京东] 跳过前序屏时 body.page={api_page} body.s={api_s}：{blocked}",
                        file=sys.stderr,
                    )
                    print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                    run_aborted = True
                    break
                if s_step <= 0 and not _skip_rows:
                    if pc_search_response_is_empty_ware_list(body):
                        print(
                            "[京东] 跳过前序屏：接口返回空 wareList，停止",
                            file=sys.stderr,
                        )
                        run_aborted = True
                        break
                    print(
                        f"[京东] 跳过前序屏 本包仍无有效数据，按 Δs={last_s_step} 强推进游标并继续",
                        file=sys.stderr,
                    )
                    api_page += 1
                    api_s += last_s_step
                    continue
                last_s_step = max(last_s_step, s_step)
                api_page += 1
                api_s += s_step
            if run_aborted:
                break

        if not run_aborted:
            expect_after_skip = jd_pc_api_body_page_first_pack(page_start)
            if api_page != expect_after_skip:
                print(
                    f"[京东] 警告：跳过前序页后首包 body.page 应为 {expect_after_skip}，当前 {api_page}",
                    file=sys.stderr,
                )

        if not run_aborted:
            for user_p in range(page_start, pe + 1):
                page_aborted = False
                expect_first = jd_pc_api_body_page_first_pack(user_p)
                if api_page != expect_first:
                    print(
                        f"[京东] 警告：逻辑第{user_p}页 首包 body.page 应为 {expect_first}，当前 {api_page}",
                        file=sys.stderr,
                    )
                for _attempt in range(JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE):
                    url, body = "", ""
                    rows: list[dict[str, str]] = []
                    s_step = 0
                    blocked: str | None = None
                    for rt in range(max_fetch_tries):
                        if rt == 0:
                            url, body = _fetch_pc_body_spaced(api_page, api_s)
                        else:
                            if retry_pause > 0:
                                time.sleep(retry_pause)
                            print(
                                f"[京东] 逻辑第{user_p}页 第{_attempt + 1}包 "
                                f"重试 {rt}/{max_fetch_tries - 1} "
                                f"body.page={api_page} body.s={api_s}",
                                file=sys.stderr,
                            )
                            url, body = _fetch_pc_body(api_page, api_s)
                        blocked = _detect_blocked(body)
                        if blocked:
                            break
                        rows, s_step = (
                            parse_items_and_pc_search_s_step_from_response_body(
                                body,
                                keyword=kw_for_row,
                                page=user_p,
                                request_api_page=api_page,
                                request_body_s=api_s,
                            )
                        )
                        if rows or s_step > 0:
                            break
                        if rt + 1 < max_fetch_tries and pc_search_should_retry_fetch(
                            body, has_rows=bool(rows), s_step=s_step
                        ):
                            continue
                        break
                    if blocked:
                        print(
                            f"[京东] CSV第{user_p}页 "
                            f"body.page={api_page} body.s={api_s}：{blocked}",
                            file=sys.stderr,
                        )
                        print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                        print(
                            "  建议：jd_pc_search/search/jd_search_playwright.py；"
                            "或 DevTools 复制 api URL 作 --url；"
                            "或 jd_low_gi_playwright.py --headed。",
                            file=sys.stderr,
                        )
                        page_aborted = True
                        break

                    if not rows and s_step <= 0:
                        if pc_search_response_is_empty_ware_list(body):
                            print(
                                f"[京东] CSV第{user_p}页 "
                                f"body.page={api_page} body.s={api_s}："
                                f"接口空 wareList，无更多商品，停止采集",
                                file=sys.stderr,
                            )
                            page_aborted = True
                            break
                        print(
                            f"[京东] CSV第{user_p}页 "
                            f"body.page={api_page} body.s={api_s}："
                            f"多次重试后仍无商品；按 Δs={last_s_step} 强推进并继续下一包",
                            file=sys.stderr,
                        )
                        print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                        if args.out and args.csv:
                            dbg = Path(args.out).with_suffix(
                                ".debug.json"
                                if body.lstrip().startswith("{")
                                else ".debug.html"
                            )
                            dbg.write_text(body, encoding="utf-8")
                            print(f"  已保存调试样本: {dbg}", file=sys.stderr)
                        api_page += 1
                        api_s += last_s_step
                        continue

                    if not rows and s_step > 0:
                        last_s_step = max(last_s_step, s_step)
                        api_page += 1
                        api_s += s_step
                        continue

                    n_added = 0
                    for r in rows:
                        sku = (r.get("sku_id") or "").strip()
                        if not sku or sku in seen:
                            continue
                        if (
                            _jd_row_count_for_page(all_rows, user_p)
                            >= JD_PC_SEARCH_ITEMS_PER_PAGE
                        ):
                            break
                        seen.add(sku)
                        all_rows.append(r)
                        n_added += 1

                    last_s_step = max(last_s_step, s_step)
                    api_page += 1
                    api_s += s_step
                if page_aborted:
                    run_aborted = True
                    break
                if user_p < pe:
                    time.sleep(max(0.0, args.page_delay))

        if n_pc_pages > 1 or any(
            _jd_row_count_for_page(all_rows, p) > 30
            for p in range(page_start, pe + 1)
        ):
            print(
                f"（PC 搜索：每逻辑页 2 包 body.page 连续 +1；CSV 列 page=逻辑页、"
                f"单页约≤{JD_PC_SEARCH_ITEMS_PER_PAGE} 条；合并后共 {len(all_rows)} 条）",
                file=sys.stderr,
            )
    else:
        plan = []
        for page_idx, url in iter_request_urls(args):
            h = build_request_headers(
                cookie,
                request_url=url,
                ua_preset=args.ua_preset,
                referer_override=referer_ov,
            )
            plan.append((page_idx, url, h))

        for idx, (page_idx, url, headers) in enumerate(plan):
            body = fetch_html(session, url, headers, args.timeout)

            blocked = _detect_blocked(body)
            if blocked:
                print(f"[京东] 第{page_idx}页：{blocked}", file=sys.stderr)
                print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                print(
                    "  建议：jd_pc_search/search/jd_search_playwright.py；或 DevTools 复制 api URL 作 --url；"
                    "或 jd_low_gi_playwright.py --headed。",
                    file=sys.stderr,
                )
                break

            rows = parse_items_from_response_body(
                body, keyword=kw_for_row, page=page_idx
            )
            if not rows:
                print(
                    f"[京东] 第{page_idx}页：未解析到商品（或 JSON 结构变化）。",
                    file=sys.stderr,
                )
                print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                if args.out and args.csv:
                    dbg = Path(args.out).with_suffix(
                        ".debug.json"
                        if body.lstrip().startswith("{")
                        else ".debug.html"
                    )
                    dbg.write_text(body, encoding="utf-8")
                    print(f"  已保存调试文件: {dbg}", file=sys.stderr)
                break

            for r in rows:
                sku = (r.get("sku_id") or "").strip()
                if not sku or sku in seen:
                    continue
                seen.add(sku)
                all_rows.append(r)

            if idx < len(plan) - 1:
                time.sleep(max(0.0, args.page_delay))

        if args.page_to is not None and len(plan) > 1:
            print(f"（已按计划请求多页，合并后 {len(all_rows)} 条）", file=sys.stderr)

    export_rows = [jd_row_to_export(r) for r in all_rows]
    if args.csv:
        buf = StringIO()
        w = csv.DictWriter(buf, fieldnames=list(CSV_FIELDS), extrasaction="ignore")
        w.writeheader()
        w.writerows(export_rows)
        body = buf.getvalue()
        if args.out:
            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.out).write_text("\ufeff" + body, encoding="utf-8")
        else:
            sys.stdout.write(body)
    else:
        txt = json.dumps(export_rows, ensure_ascii=False, indent=2)
        dump_out(txt)


if __name__ == "__main__":
    main()

