# -*- coding: utf-8 -*-
"""
京东 PC 详情 **采集层**：打开商品页、拦截 ``pc_detailpage_wareBusiness``、从 ``#detail-main`` 抽图文 URL。

解析 JSON 扁平字段见 ``jd_detail_ware_parse``。
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

_JD_DIR = Path(__file__).resolve().parent
if str(_JD_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_DIR))
_JD_PC_SEARCH_DIR = _JD_DIR.parent
if str(_JD_PC_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH_DIR))

from jd_detail_ware_parse import ware_fetch_should_retry

_DEFAULT_COOKIE_PATH = (_JD_DIR.parent / "common" / "jd_cookie.txt").resolve()


@dataclass(frozen=True)
class WareFetchRuntime:
    """单次打开详情页时的行为开关（与 ``jd_detail_ware_business_requests`` 顶部配置对应）。"""

    log_api_m_jd_trace: bool = False
    goto_wait_until: str = "domcontentloaded"
    click_product_detail_tab: bool = True
    collect_detail_main_image_urls: bool = True
    detail_body_image_url_separator: str = "; "
    detail_body_image_urls_max_chars: int = 31000


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


# PC 详情页底部锚点：商品详情（与页面 id 一致，改版时需对照 DOM）
_JD_TAB_PRODUCT_DETAIL_SELECTOR = "#SPXQ-tab-column"


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
    runtime: WareFetchRuntime | None = None,
) -> str:
    """
    从当前页 ``#detail-main`` 收集图文资源 URL（SSD 背景图、img、zbViewWeChatMiniImages），
    补全为 https，去重保序后用 ``separator`` 拼成一段字符串（供 CSV 单格）。
    页面须已展示商品详情区（通常需先点 ``#SPXQ-tab-column``）。
    """
    rt = runtime or WareFetchRuntime()
    if not rt.collect_detail_main_image_urls:
        return ""
    sep = (
        separator
        if separator is not None
        else (rt.detail_body_image_url_separator or "; ")
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
        else int(rt.detail_body_image_urls_max_chars)
    )
    if mxc > 0 and len(joined) > mxc:
        joined = joined[: mxc - 3] + "..."
    return joined


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
    runtime: WareFetchRuntime | None = None,
) -> tuple[int, str, dict[str, Any]]:
    """单次打开商品页并拦截 ``pc_detailpage_wareBusiness`` 响应（无重试）。"""
    rt = runtime or WareFetchRuntime()
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
            if rt.log_api_m_jd_trace and "api.m.jd.com" in u:
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
        _wu = (rt.goto_wait_until or "domcontentloaded").strip()
        page.goto(item_url, wait_until=_wu, timeout=timeout_ms)
        trace_phase = "loaded"
        if rt.click_product_detail_tab:
            _click_jd_product_detail_tab(page, timeout_ms=timeout_ms)
            trace_phase = "after_tab"
        extra = min(12_000, max(3_000, timeout_ms // 2))
        page.wait_for_timeout(extra)
        if rt.collect_detail_main_image_urls:
            try:
                detail_joined = scrape_detail_main_body_urls_joined(
                    page,
                    wait_ms=min(timeout_ms, 12_000),
                    runtime=rt,
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
    if rt.log_api_m_jd_trace:
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
        if rt.log_api_m_jd_trace:
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
    if rt.log_api_m_jd_trace:
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
    output_sku_and_body_images_only: bool = False,
    runtime: WareFetchRuntime | None = None,
) -> tuple[int, str, dict[str, Any]]:
    """
    打开商品页并拦截 ``pc_detailpage_wareBusiness``。
    ``max_attempts``>1 时，在结果为空或失败时按 ``retry_delay_sec`` 间隔重试。
    """
    rt = runtime or WareFetchRuntime()
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
            runtime=rt,
        )
        last = (code, text, meta)
        if output_sku_and_body_images_only and (
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


__all__ = [
    "WareFetchRuntime",
    "_print_http_verbose",
    "_read_jd_cookie_file_raw",
    "fetch_ware_business",
    "scrape_detail_main_body_urls_joined",
]
