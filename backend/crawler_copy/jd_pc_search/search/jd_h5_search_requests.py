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

**模块划分**：响应 JSON/HTML 解析、商品行扁平化、游标与重试判定在 ``jd_h5_search_parse.py``；
本文件保留 Node 签 URL、``requests``/Header、CLI ``main``，并从解析模块 re-export ``CSV_FIELDS`` 等以兼容旧导入。
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
_BACKEND_ROOT = Path(__file__).resolve().parents[3]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))
from common.jd_delay_utils import parse_request_delay_range, sleep_pc_search_request_gap
from pipeline.csv.schema import JD_SEARCH_CSV_HEADERS as JD_EXPORT_COLUMN_HEADERS  # noqa: E402

_JD_PC_SEARCH_DIR = Path(__file__).resolve().parent
if str(_JD_PC_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH_DIR))

from jd_h5_search_parse import (  # noqa: E402
    CSV_FIELDS,
    JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE,
    JD_PC_SEARCH_FALLBACK_S_STEP,
    JD_PC_SEARCH_ITEMS_PER_PAGE,
    JD_SKU_KEYS,
    _detect_blocked,
    _jd_flatten_ware,
    _jd_row_count_for_page,
    _sval_jd,
    jd_pc_api_body_page_first_pack,
    jd_row_to_export,
    parse_items_and_pc_search_s_step_from_response_body,
    parse_items_from_response_body,
    pc_search_response_is_empty_ware_list,
    pc_search_should_retry_fetch,
    pc_search_ware_list_slot_count_from_body,
)


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

