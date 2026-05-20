# -*- coding: utf-8 -*-
"""
MTOP JSONP：**Playwright Chromium** 的 ``APIRequestContext`` 发 GET，
与 TLS/HTTP 栈一致；配合 ``mtop_jsonp_script_headers`` 构造头。

Accept-Encoding 仅 ``gzip, deflate``，避免声明 br/zstd 而本地解压失败。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from playwright.sync_api import APIRequestContext

from .constants import USER_AGENT
from .jsonp import strip_jsonp

__all__ = [
    "MtopJsonpFetchResult",
    "fetch_mtop_jsonp",
    "mtop_jsonp_script_headers",
]


def mtop_jsonp_script_headers(
    cookie: str,
    *,
    referer: str,
    user_agent: str | None = None,
    accept_encoding: str = "gzip, deflate",
) -> dict[str, str]:
    """
    与搜索页拉取 mtop JSONP 脚本时常见的浏览器请求头一致。

    :param referer: 须为完整 URL；复现抓包时常用 ``build_pc_search_referer`` 或浏览器复制的值。
    """
    ua = (user_agent or "").strip() or USER_AGENT
    h: dict[str, str] = {
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": accept_encoding,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "same-site",
    }
    if cookie.strip():
        h["Cookie"] = cookie.strip()
    return h


@dataclass(frozen=True)
class MtopJsonpFetchResult:
    """一次 GET 的原文与 JSONP 解析结果。"""

    raw_text: str
    parsed: dict[str, Any] | None
    parse_error: str | None


def fetch_mtop_jsonp(
    api: APIRequestContext,
    url: str,
    *,
    headers: dict[str, str],
    callback: str,
    timeout: float,
) -> MtopJsonpFetchResult:
    """
    使用 Playwright ``APIRequestContext.get``；成功则 ``strip_jsonp``。
    HTTP 非 2xx 时抛 ``RuntimeError``。
    """
    timeout_ms = max(1000, int(float(timeout) * 1000))
    resp = api.get(url, headers=headers, timeout=timeout_ms)
    if resp.status != 200:
        snippet = ""
        try:
            snippet = (resp.text() or "")[:240]
        except Exception:
            pass
        raise RuntimeError(
            f"mtop GET HTTP {resp.status} url={url[:96]}…  body[:240]={snippet!r}"
        )
    raw = resp.text()
    parsed: dict[str, Any] | None = None
    parse_err: str | None = None
    try:
        parsed = strip_jsonp(raw, callback)
    except (ValueError, json.JSONDecodeError) as e:
        parse_err = repr(e)
    return MtopJsonpFetchResult(raw_text=raw, parsed=parsed, parse_error=parse_err)
