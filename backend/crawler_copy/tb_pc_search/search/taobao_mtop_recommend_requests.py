# -*- coding: utf-8 -*-
"""
淘宝 mtop.relationrecommend.wirelessrecommend.recommend — **仅用 Playwright Chromium** 发起 JSONP GET。

子模块：`h5_sign` 签名、`recommend_params` 内层 params 与 Referer、`transport.fetch_mtop_jsonp`（APIRequestContext）、
`recommend_client` 编排与落盘、`item_extract` 解析。

默认先发 SRP 再拉 mtor（见 ``TB_WARMUP_GOTO_SRP_BEFORE_MTOP``、``TB_WARMUP_POST_LOAD_MS``）。

Cookie 来源（优先级）：``TB_COOKIE`` → ``TB_COOKIE_FILE`` → ``MY_COOKIE`` → 同目录 ``taobao_cookie.txt`` → 环境变量 ``TAOBAO_COOKIE``。

其余行为一律改文件顶部 ``TB_*`` / ``RUN_DEFAULTS``（无命令行参数），例如 ``TB_SAVE_RAW_DIR``、``TB_FORMAT``、``TB_HEADLESS``、``TB_URL``、``TB_DRY_RUN``、``TB_USER_DATA_DIR`` 等。

**仅导出 Cookie**（不写 mtop）：``TB_EXPORT_COOKIE_FILE`` 设为非空路径后无参运行；与 ``TB_EXPORT_WAIT_MS`` / ``TB_EXPORT_START_URL`` / ``TB_HEADLESS`` 共用。

示例::

  python backend/crawler_copy/tb_pc_search/search/taobao_mtop_recommend_requests.py
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
from argparse import Namespace
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlencode

from playwright.sync_api import BrowserContext, Playwright, sync_playwright

# 与 tb_pc_search 包同层（本文件在 search/ 下）
_TB_PC_SEARCH = Path(__file__).resolve().parent.parent
_CRAWLER_COPY = _TB_PC_SEARCH.parent  # crawler_copy
for _tb_path in (_TB_PC_SEARCH, _CRAWLER_COPY):
    if str(_tb_path) not in sys.path:
        sys.path.insert(0, str(_tb_path))
from _low_gi_root import low_gi_project_root  # noqa: E402
from playwright_session import (  # noqa: E402 — 与本包同级
    TbChromiumSession,
    cookie_rows_to_header,
    launch_ephemeral_chromium_like_search,
    launch_persistent_chromium,
    resolve_tb_user_data_dir,
)

from mtop.h5_sign import (  # noqa: E402
    build_query_params,
    encode_data_query_value,
    mtop_auto_t_sign,
)
from mtop.item_extract import CANONICAL_FIELDS, parse_items_from_mtop_payload, row_dedup_key  # noqa: E402
from mtop.jsonp import mtop_stderr_hint, strip_jsonp  # noqa: E402
from mtop.recommend_client import (  # noqa: E402
    mtop_fetch_json_payload,
    mtop_recommend_dry_run_bundle,
    query_params_from_url,
    resolve_path_under_root,
    save_mtop_exchange,
    sleep_before_request,
)
from mtop.recommend_params import (  # noqa: E402
    MTOP_PATH,
    build_default_mtop_headers,
    build_inner_params_from_args,
    resolve_mtop_referer_url,
)
from mtop.transport import fetch_mtop_jsonp  # noqa: E402

PROJECT_ROOT = low_gi_project_root()

# 本文件内嵌的 Cookie 一整段；优先级：TB_COOKIE → TB_COOKIE_FILE → 本变量 → 同目录 taobao_cookie.txt → 环境变量 TAOBAO_COOKIE
MY_COOKIE = r"""

""".strip()

# ---------------------------------------------------------------------------
# 运行配置：无命令行参数，只改下面 TB_*。（RUN_DEFAULTS 由这些变量组装，键名需与下游 args 一致）
# ---------------------------------------------------------------------------

# True=只打印拼装结果（URL/Headers/request_plain JSON），不启动 Playwright、不发请求
TB_DRY_RUN = False
# 非空时：整条 mtop GET 的 URL（可从浏览器 Network 复制），优先于自动签名拼装；多页模式不可与 TB_PAGE_TO 同时用
TB_URL = None

# 直接写整段 Cookie 请求头（最高）
TB_COOKIE = ""
# 从该文件读 Cookie；路径相对项目根；TB_COOKIE 非空时优先于本项
TB_COOKIE_FILE = None
# 单次 mtop（及 SRP 导航）HTTP 超时秒数
TB_TIMEOUT = 30.0

# True：只输出原始 JSONP 文本并提前结束；与「多页 TB_PAGE_TO」互斥
TB_RAW = False
# JSONP 回调名（与 URL 查询参数 callback= 一致）
TB_CALLBACK = "mtopjsonp6"
# 手写 mtop 查询参数 t、sign；留空则用 Cookie 自动算。多页连续拉取时不要填
TB_T = None
TB_SIGN = None
# 参与签名的 appKey，与抓包、淘宝开放平台配置一致
TB_APP_KEY = "12574478"
# 内层 data 里的 appId（整型常写成字符串即可）
TB_APP_ID = "34385"

# 搜索词，写入内层 params.q，并用于拼 SRP Referer
TB_Q = "低GI"
# 起始页（从 1）；与 TB_PAGE_TO 组成多页区间
TB_PAGE = 1
# 非空：结束页（含），与 TB_PAGE 一起连续拉多页再合并去重；仅当 TB_FORMAT 为 items；不可与 TB_URL 同用
TB_PAGE_TO = None
# 多页时，页与页之间的固定睡眠秒数（在「单页内 request 随机间隔」之外）
TB_PAGE_DELAY = 0.9
# 每一次发 mtop GET 前随机睡眠 [min,max] 秒；若 max<=0 则完全不等待（见 sleep_before_request）
TB_REQUEST_DELAY_MIN = 30.0
TB_REQUEST_DELAY_MAX = 60.0

# 非空：每次请求后把原文等落盘到此目录（相对项目根）；用于抓包对照 / 排错
TB_SAVE_RAW_DIR = ""
# True：落盘 JSON 里把 Cookie 打成占位，避免泄露真 Cookie
TB_REDACT_COOKIE_IN_SAVE = False
# 内层 params 里每页条数 n / pageSize
TB_PAGE_SIZE = 48
# full=整段官方 JSON；items=只输出解析后的商品列表；both=先 full 再 items 两段
TB_FORMAT = "full"
# True 且 TB_FORMAT=items 时输出 CSV；否则为 JSON 数组
TB_CSV = False
# 结果写到该文件（相对项目根）；None/空 则打印到 stdout
TB_OUT = None

# 完整 Referer URL；非空则不再按 SRP 规则拼 Referer
TB_REFERER = ""
# True：Referer 只打短域 https://s.taobao.com/（与抓包对齐用）
TB_SHORT_REFERER = False
# SRP Referer 上 clientPreloadId= 的值；与淘宝预加载会话一致时更稳，可空让脚本按会话生成
TB_PRELOAD_ID = ""
# Referer 查询串里的 spm（可为空）
TB_SPM = ""
# 覆盖内层 params.pageSource；空则用 recommend_params 默认值
TB_PAGE_SOURCE = ""
# 覆盖内层 viewResolution，如 548x1345；空则用库内默认（与抓包 device 模板一致）
TB_VIEW_RESOLUTION = ""
# True：无头 Chromium；False：有头（调登录、过风控时常用）
TB_HEADLESS = True

# 非空：Playwright 持久化目录（相对项目根或绝对）。空则仍为「临时浏览器 + 文件/tb Cookie」。
# **勿与其它工具或另一套脚本共写同一 Chromium Profile 路径**（防 Cookie / 会话错乱）；未指定时使用 ``tb_pc_search/pw_user_data``。
# 与 ``TB_DRY_RUN=True`` 同时使用时不会启动浏览器，干跑仍依赖 ``TB_COOKIE`` / ``taobao_cookie.txt``。
TB_USER_DATA_DIR = ""

# ---------- 仅导出 Cookie（不写 mtop）：下面路径非空时，只开 SRP、把 Cookie 写到文件后退出 ----------
TB_EXPORT_COOKIE_FILE = ""
# 打开页面后额外等待毫秒再取 Cookie（给登录/脚本落 cookie 时间）
TB_EXPORT_WAIT_MS = 4000
# 导出时首跳 URL；空则按 TB_Q 拼默认 SRP
TB_EXPORT_START_URL = ""

# 发 mtop 前是否在同一 BrowserContext 里先访问与 Referer 一致的 SRP（减少冷调用风控）
TB_WARMUP_GOTO_SRP_BEFORE_MTOP = True
# SRP 加载到 domcontentloaded 之后再停多少毫秒（0=不额外等）
TB_WARMUP_POST_LOAD_MS = 2000

# 逐项对应下方 TB_*，供 Namespace 传给 h5_sign / recommend_*；键名请勿随意改名
RUN_DEFAULTS: dict[str, Any] = {
    "dry_run": TB_DRY_RUN,
    "url": TB_URL,
    "cookie": TB_COOKIE,
    "cookie_file": TB_COOKIE_FILE,
    "timeout": TB_TIMEOUT,
    "raw": TB_RAW,
    "callback": TB_CALLBACK,
    "t": TB_T,
    "sign": TB_SIGN,
    "app_key": TB_APP_KEY,
    "app_id": TB_APP_ID,
    "q": TB_Q,
    "page": TB_PAGE,
    "page_to": TB_PAGE_TO,
    "page_delay": TB_PAGE_DELAY,
    "save_raw_dir": TB_SAVE_RAW_DIR,
    "redact_cookie_in_save": TB_REDACT_COOKIE_IN_SAVE,
    "page_size": TB_PAGE_SIZE,
    "format": TB_FORMAT,
    "csv": TB_CSV,
    "out": TB_OUT,
    "referer": TB_REFERER,
    "short_referer": TB_SHORT_REFERER,
    "preload_id": TB_PRELOAD_ID,
    "spm": TB_SPM,
    "page_source": TB_PAGE_SOURCE,
    "view_resolution": TB_VIEW_RESOLUTION,
    "headless": TB_HEADLESS,
    "user_data_dir": TB_USER_DATA_DIR,
}


def namespace_from_run_defaults() -> Namespace:
    """合并 ``RUN_DEFAULTS`` 与 ``TB_REQUEST_DELAY_*``，得到下游用的 ``args``（无命令行）。"""
    d = dict(RUN_DEFAULTS)
    d["request_delay_min"] = float(TB_REQUEST_DELAY_MIN)
    d["request_delay_max"] = float(TB_REQUEST_DELAY_MAX)
    return Namespace(**d)


def write_output_file(path_str: str, body: str, *, bom_utf8: bool = False) -> None:
    p = resolve_path_under_root(path_str, PROJECT_ROOT)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(("\ufeff" + body) if bom_utf8 else body, encoding="utf-8")


def load_cookie(args: Namespace) -> str:
    if getattr(args, "cookie", None) and str(args.cookie).strip():
        return str(args.cookie).strip()
    if getattr(args, "cookie_file", None):
        path = args.cookie_file
        if not os.path.isfile(path):
            print(f"Cookie 文件不存在: {path}", file=sys.stderr)
            sys.exit(1)
        return open(path, encoding="utf-8").read().strip()
    if MY_COOKIE.strip():
        return MY_COOKIE.strip()
    default_txt = Path(__file__).resolve().parent / "taobao_cookie.txt"
    if default_txt.is_file():
        return default_txt.read_text(encoding="utf-8").strip()
    return os.environ.get("TAOBAO_COOKIE", "").strip()


def _configure_stdio_utf8() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _default_cookie_export_url(q: str) -> str:
    qs = (q or "").strip() or "低GI"
    return "https://s.taobao.com/search?q=" + quote_plus(qs, safe="")


def _cookies_to_header(rows: list[dict]) -> str:
    parts: list[str] = []
    for c in rows:
        name = c.get("name")
        if not name:
            continue
        val = c.get("value")
        parts.append(f"{name}={val if val is not None else ''}")
    return "; ".join(parts)


def _open_playwright_tb_session(pw_inst: Playwright, *, headless: bool) -> TbChromiumSession:
    """临时浏览器，或 ``TB_USER_DATA_DIR`` 非空时的 Playwright 持久化目录。"""
    ud = (TB_USER_DATA_DIR or "").strip()
    if ud:
        p = resolve_tb_user_data_dir(ud)
        p.mkdir(parents=True, exist_ok=True)
        return launch_persistent_chromium(pw_inst, user_data_dir=p, headless=headless)
    return launch_ephemeral_chromium_like_search(pw_inst, headless=headless)


def _cookie_for_sign_after_session(
    context: BrowserContext,
    cookie_loaded: str,
) -> str:
    """持久化模式下以当前上下文的 Cookie 为准（含登录后颁发的 token）。"""
    if (TB_USER_DATA_DIR or "").strip():
        ck = cookie_rows_to_header(context.cookies()).strip()
        return ck if ck else cookie_loaded.strip()
    return cookie_loaded.strip()


def _playwright_tb_cookie_rows(cookie_header: str) -> list[dict[str, Any]]:
    """``Cookie`` 请求头拆解为 Playwright ``add_cookies`` 条目（``.taobao.com``）。"""
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
                "domain": ".taobao.com",
                "path": "/",
                "secure": True,
            }
        )
    return rows


def _inject_tb_cookies_into_context(context: BrowserContext, cookie: str) -> None:
    """将 Cookie 头写入上下文，供导航与后续 APIRequest 共用。"""
    if not (cookie or "").strip():
        return
    try:
        context.add_cookies(_playwright_tb_cookie_rows(cookie))
    except Exception as e:
        print(f"[taobao] add_cookies: {e}", file=sys.stderr)


def _warmup_srp_goto(
    context: BrowserContext,
    args: argparse.Namespace,
    *,
    srp_page: int,
    post_load_ms: int,
) -> None:
    """先发 mtop 前打开与 Referer 一致的 SRP。"""
    ref_url = resolve_mtop_referer_url(args, srp_page=srp_page)
    short = ref_url[:100] + ("…" if len(ref_url) > 100 else "")
    print(f"[taobao] SRP 前置: {short}", file=sys.stderr)
    page = context.new_page()
    try:
        page.goto(ref_url, wait_until="domcontentloaded", timeout=120_000)
        w = max(0, int(post_load_ms))
        if w:
            page.wait_for_timeout(w)
    finally:
        page.close()


def run_export_cookies_only(
    *,
    out_path: str,
    q: str,
    start_url: str,
    headless: bool,
    wait_ms: int,
) -> None:
    start = (start_url or "").strip() or _default_cookie_export_url(q)
    with sync_playwright() as pw:
        sess = _open_playwright_tb_session(pw, headless=headless)
        try:
            ctx = sess.context
            page = ctx.new_page()
            page.goto(start, wait_until="domcontentloaded", timeout=120_000)
            w = max(0, int(wait_ms))
            if w:
                page.wait_for_timeout(w)
            header = _cookies_to_header(ctx.cookies())
        finally:
            sess.close()
    if not header.strip():
        print(
            "未取到 Cookie；请去掉无头或以有头登录后再试。", file=sys.stderr,
        )
        sys.exit(2)
    outp = resolve_path_under_root((out_path or "").strip(), PROJECT_ROOT)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(header, encoding="utf-8")
    print(f"已写入 Cookie: {outp.resolve()}", file=sys.stderr)


def main() -> None:
    _configure_stdio_utf8()
    _export = (TB_EXPORT_COOKIE_FILE or "").strip()
    if _export:
        run_export_cookies_only(
            out_path=_export,
            q=str(TB_Q),
            start_url=str(TB_EXPORT_START_URL or "").strip(),
            headless=bool(TB_HEADLESS),
            wait_ms=int(TB_EXPORT_WAIT_MS),
        )
        return

    args = namespace_from_run_defaults()

    if args.page_to is not None and args.page_to < args.page:
        print("TB_PAGE_TO 必须大于等于 TB_PAGE", file=sys.stderr)
        sys.exit(2)
    if args.page_to is not None and args.url:
        print("多页抓取（TB_PAGE_TO）不能与 TB_URL 同时使用", file=sys.stderr)
        sys.exit(2)
    if args.page_to is not None and (
        (getattr(args, "t", None) and str(args.t).strip())
        or (getattr(args, "sign", None) and str(args.sign).strip())
    ):
        print("多页抓取时请省略 TB_T / TB_SIGN（依赖 Cookie 自动签名）", file=sys.stderr)
        sys.exit(2)
    if args.raw and args.page_to is not None:
        print("TB_RAW 与多页抓取请分开配置", file=sys.stderr)
        sys.exit(2)

    cookie = load_cookie(args)
    app_key = str(args.app_key).strip()

    if args.dry_run:
        bundle = mtop_recommend_dry_run_bundle(
            cookie,
            args,
            page_idx=int(args.page),
            app_key=app_key,
            redact_request_cookie=bool(getattr(args, "redact_cookie_in_save", False)),
        )
        err = bundle.get("error")
        if err:
            print(err, file=sys.stderr)
            sys.exit(2)
        print(json.dumps(bundle, ensure_ascii=False, indent=2))
        return

    with sync_playwright() as pw_inst:
        sess = _open_playwright_tb_session(pw_inst, headless=bool(args.headless))
        try:
            context = sess.context
            api = sess.request
            cookie = _cookie_for_sign_after_session(context, cookie)
            if (TB_USER_DATA_DIR or "").strip() and not (cookie or "").strip():
                print(
                    "Playwright 持久化目录中尚无 Cookie；可在本脚本打开的浏览器内先登录养好 Profile，"
                    "或关闭 TB_USER_DATA_DIR、仅用 taobao_cookie.txt / TB_COOKIE。",
                    file=sys.stderr,
                )
                sys.exit(2)
            if not (TB_USER_DATA_DIR or "").strip():
                _inject_tb_cookies_into_context(context, cookie)
            if args.url:
                u = args.url.strip()
                if TB_WARMUP_GOTO_SRP_BEFORE_MTOP:
                    _warmup_srp_goto(
                        context,
                        args,
                        srp_page=int(args.page),
                        post_load_ms=TB_WARMUP_POST_LOAD_MS,
                    )
                headers = build_default_mtop_headers(cookie, args, srp_page=args.page)
                sleep_before_request(args.request_delay_min, args.request_delay_max)
                fetch_u = fetch_mtop_jsonp(
                    api,
                    u,
                    headers=headers,
                    callback=str(args.callback),
                    timeout=args.timeout,
                )
                text = fetch_u.raw_text
                qp = query_params_from_url(u)
                parsed_url = fetch_u.parsed
                parse_err_u = fetch_u.parse_error
                parse_exc_u: BaseException | None = None
                if parsed_url is None:
                    try:
                        strip_jsonp(text, args.callback)
                    except (json.JSONDecodeError, ValueError) as e:
                        parse_exc_u = e
                        if parse_err_u is None:
                            parse_err_u = repr(e)
                save_mtop_exchange(
                    PROJECT_ROOT,
                    (getattr(args, "save_raw_dir", None) or "").strip() or None,
                    page_idx=None,
                    request_url=u,
                    query_params=qp,
                    raw_text=text,
                    parsed=parsed_url,
                    parse_error=parse_err_u,
                    cookie=cookie,
                    headers=headers,
                    app_key_for_sign=app_key,
                    redact_request_cookie=bool(getattr(args, "redact_cookie_in_save", False)),
                )
                if args.raw:
                    print(text)
                    return
                if parse_exc_u is not None:
                    print("原始响应（JSONP 解析失败）：", file=sys.stderr)
                    print(text[:2000], file=sys.stderr)
                    raise parse_exc_u
                payload = parsed_url
                rows = parse_items_from_mtop_payload(payload)
            elif args.raw:
                if TB_WARMUP_GOTO_SRP_BEFORE_MTOP:
                    _warmup_srp_goto(
                        context,
                        args,
                        srp_page=int(args.page),
                        post_load_ms=TB_WARMUP_POST_LOAD_MS,
                    )
                inner = build_inner_params_from_args(args, page=args.page)
                data_val = encode_data_query_value(str(args.app_id), inner)
                manual_t = getattr(args, "t", None) and str(args.t).strip()
                manual_sign = getattr(args, "sign", None) and str(args.sign).strip()
                t_str, sign_str, err = mtop_auto_t_sign(
                    cookie,
                    app_key,
                    data_val,
                    manual_t=manual_t or None,
                    manual_sign=manual_sign or None,
                )
                if err:
                    print(err, file=sys.stderr)
                    if "Cookie" in err or "_m_h5_tk" in err:
                        print("可补全 taobao_cookie.txt 或设置 TB_URL（浏览器复制的完整请求 URL）。", file=sys.stderr)
                    sys.exit(2)
                assert t_str and sign_str
                qs = build_query_params(
                    t=t_str,
                    sign=sign_str,
                    data_json=data_val,
                    callback=str(args.callback),
                    app_key=app_key,
                )
                url = MTOP_PATH + "?" + urlencode(qs)
                hdr = build_default_mtop_headers(cookie, args, srp_page=args.page)
                sleep_before_request(args.request_delay_min, args.request_delay_max)
                res_r = fetch_mtop_jsonp(
                    api,
                    url,
                    headers=hdr,
                    callback=str(args.callback),
                    timeout=args.timeout,
                )
                raw_raw = res_r.raw_text
                parsed_r = res_r.parsed
                parse_err_r = res_r.parse_error
                if parsed_r is None:
                    try:
                        strip_jsonp(raw_raw, args.callback)
                    except (json.JSONDecodeError, ValueError) as e:
                        if parse_err_r is None:
                            parse_err_r = repr(e)
                save_mtop_exchange(
                    PROJECT_ROOT,
                    (getattr(args, "save_raw_dir", None) or "").strip() or None,
                    page_idx=args.page,
                    request_url=url,
                    query_params=qs,
                    raw_text=raw_raw,
                    parsed=parsed_r,
                    parse_error=parse_err_r,
                    cookie=cookie,
                    headers=hdr,
                    app_key_for_sign=app_key,
                    redact_request_cookie=bool(getattr(args, "redact_cookie_in_save", False)),
                )
                print(raw_raw)
                return
            else:
                page_end = args.page_to if args.page_to is not None else args.page
                merge_seen: set[str] = set()
                all_rows: list[dict[str, str]] = []
                last_payload: dict[str, Any] | None = None
                for page_idx in range(args.page, page_end + 1):
                    try:
                        if TB_WARMUP_GOTO_SRP_BEFORE_MTOP:
                            _warmup_srp_goto(
                                context,
                                args,
                                srp_page=page_idx,
                                post_load_ms=TB_WARMUP_POST_LOAD_MS,
                            )
                        last_payload = mtop_fetch_json_payload(
                            api, cookie, args, page_idx, app_key, args.timeout, PROJECT_ROOT
                        )
                    except ValueError as e:
                        print(str(e), file=sys.stderr)
                        sys.exit(2)
                    for row in parse_items_from_mtop_payload(last_payload):
                        dk = row_dedup_key(row)
                        if not dk or dk in merge_seen:
                            continue
                        merge_seen.add(dk)
                        all_rows.append(row)
                    if page_idx < page_end:
                        time.sleep(max(0.0, args.page_delay))
                payload = last_payload or {}
                rows = all_rows
                if args.page_to is not None and last_payload is not None:
                    print(
                        f"（已拉取第 {args.page}～{page_end} 页，合并后 {len(rows)} 条）",
                        file=sys.stderr,
                    )
        finally:
            sess.close()

    if args.csv and args.format != "items":
        print("TB_CSV 需与 TB_FORMAT=items 同时启用", file=sys.stderr)
        sys.exit(2)
    if args.page_to is not None and args.format != "items":
        print("多页合并仅支持 TB_FORMAT=items", file=sys.stderr)
        sys.exit(2)

    full_txt = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.format == "full":
        if args.out:
            write_output_file(args.out, full_txt)
        else:
            print(full_txt)
        if rows:
            print(
                f"（解析到 {len(rows)} 条商品，可将 TB_FORMAT 设为 items；CSV 时再设 TB_CSV=True）",
                file=sys.stderr,
            )
        else:
            print(
                "（未解析到商品节点，可在 mtop.item_extract 扩展字段映射）",
                file=sys.stderr,
            )
    elif args.format == "items":
        if args.csv:
            buf = StringIO()
            if rows:
                w = csv.DictWriter(buf, fieldnames=list(CANONICAL_FIELDS), extrasaction="ignore")
                w.writeheader()
                w.writerows(rows)
            csv_body = buf.getvalue()
            if args.out:
                write_output_file(args.out, csv_body, bom_utf8=True)
            else:
                sys.stdout.write(csv_body)
        else:
            items_txt = json.dumps(rows, ensure_ascii=False, indent=2)
            if args.out:
                write_output_file(args.out, items_txt)
            else:
                print(items_txt)
    else:
        print(full_txt)
        print(json.dumps(rows, ensure_ascii=False, indent=2))

    hint = mtop_stderr_hint(payload)
    if hint:
        print(hint, file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
