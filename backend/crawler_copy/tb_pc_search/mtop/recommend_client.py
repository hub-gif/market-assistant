# -*- coding: utf-8 -*-
"""
推荐列表 mtop：随机等待、落盘、拼装 URL 与 **Playwright** GET（多页主路径）。
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from playwright.sync_api import APIRequestContext

from .h5_sign import (
    build_query_params,
    encode_data_query_value,
    extract_m_h5_token,
    mtop_auto_t_sign,
)
from .jsonp import strip_jsonp
from .recommend_params import (
    MTOP_RECOMMEND_BASE_URL,
    build_default_mtop_headers,
    build_inner_params_from_args,
)
from .transport import fetch_mtop_jsonp

__all__ = [
    "mtop_fetch_json_payload",
    "mtop_recommend_dry_run_bundle",
    "query_params_from_url",
    "resolve_path_under_root",
    "save_mtop_exchange",
    "sleep_before_request",
]


def headers_for_archive(
    headers: dict[str, str],
    *,
    redact_cookie: bool,
) -> dict[str, str]:
    """落盘 / 对照用请求头；脱敏时仅将 Cookie 替换为占位，便于与浏览器逐项对比。"""
    out = dict(headers)
    if redact_cookie and "Cookie" in out:
        out["Cookie"] = "[omitted]"
    return out


def build_request_plain(
    *,
    cookie: str,
    query_params: dict[str, str],
    app_key_for_sign: str,
) -> dict[str, Any]:
    """
    签名与 data：token、t、sign 拼串，以及外层/内层 data 解析。
    （HTTP 头见落盘字段 ``request.headers``，不重复放在此对象中。）
    """
    data_json = query_params.get("data") or ""
    t = query_params.get("t") or ""
    sign = query_params.get("sign") or ""
    tok = extract_m_h5_token(cookie.strip()) if cookie.strip() else None
    sign_input = f"{tok}&{t}&{app_key_for_sign}&{data_json}" if tok else ""
    data_outer: dict[str, Any] | str | list | None = None
    inner_parsed: Any = None
    inner_err: str | None = None
    outer_err: str | None = None
    try:
        data_outer = json.loads(data_json) if data_json else None
    except json.JSONDecodeError as e:
        outer_err = repr(e)
        data_outer = None
    if isinstance(data_outer, dict) and isinstance(data_outer.get("params"), str):
        try:
            inner_parsed = json.loads(data_outer["params"])
        except json.JSONDecodeError as e:
            inner_err = repr(e)
    blob: dict[str, Any] = {
        "_m_h5_sign_token_only": tok or "",
        "sign_concat_before_md5": sign_input,
        "sign_md5_hex": sign,
        "t": t,
        "app_key": app_key_for_sign,
        "data_query_string_exact": data_json,
        "data_outer_json_obj": data_outer,
        "params_inner_json_obj": inner_parsed,
    }
    if outer_err is not None:
        blob["data_outer_parse_error"] = outer_err
    if inner_err is not None:
        blob["params_inner_parse_error"] = inner_err
    return blob


def mtop_recommend_dry_run_bundle(
    cookie: str,
    args: argparse.Namespace,
    *,
    page_idx: int,
    app_key: str,
    redact_request_cookie: bool,
) -> dict[str, Any]:
    """不发起 HTTP：输出将与 Playwright GET 相同的 URL、参、headers、request_plain。"""
    pasted = (getattr(args, "url", None) or "").strip()
    if pasted:
        headers = build_default_mtop_headers(cookie, args, srp_page=page_idx)
        qp = query_params_from_url(pasted)
        plain = build_request_plain(
            cookie=cookie,
            query_params=qp,
            app_key_for_sign=app_key.strip(),
        )
        return {
            "mode": "pasted_browser_url",
            "request_url_full": pasted,
            "query_params": qp,
            "headers": headers_for_archive(headers, redact_cookie=redact_request_cookie),
            "request_plain": plain,
        }

    headers = build_default_mtop_headers(cookie, args, srp_page=page_idx)
    inner = build_inner_params_from_args(args, page=page_idx)
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
        return {"mode": "assemble", "error": err}
    qs = build_query_params(
        t=t_str,
        sign=sign_str,
        data_json=data_val,
        callback=str(args.callback),
        app_key=app_key,
    )
    url = MTOP_RECOMMEND_BASE_URL + "?" + urlencode(qs)
    plain = build_request_plain(
        cookie=cookie,
        query_params=qs,
        app_key_for_sign=app_key.strip(),
    )
    return {
        "mode": "assembled_locally",
        "request_url": url,
        "query_params": qs,
        "headers": headers_for_archive(headers, redact_cookie=redact_request_cookie),
        "request_plain": plain,
    }


def resolve_path_under_root(path_str: str, project_root: Path) -> Path:
    """相对路径相对项目根；绝对路径则 expanduser + resolve。"""
    p = Path(path_str.strip()).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (project_root / p).resolve()


def sleep_before_request(
    request_delay_min: float,
    request_delay_max: float,
    *,
    log_stream=sys.stderr,
) -> None:
    """每次请求前随机等待；max<=0 时不等待。"""
    hi = float(request_delay_max)
    if hi <= 0:
        return
    lo = max(0.0, float(request_delay_min))
    if lo > hi:
        lo, hi = hi, lo
    delay = random.uniform(lo, hi)
    print(f"请求前随机等待 {delay:.1f} 秒…", file=log_stream)
    time.sleep(delay)


def save_mtop_exchange(
    project_root: Path,
    save_raw_dir: str | None,
    *,
    page_idx: int | None,
    request_url: str,
    query_params: dict[str, str],
    raw_text: str,
    parsed: dict[str, Any] | None,
    parse_error: str | None,
    cookie: str | None = None,
    headers: dict[str, str] | None = None,
    app_key_for_sign: str | None = None,
    redact_request_cookie: bool = False,
    log_stream=sys.stderr,
) -> None:
    if not (save_raw_dir or "").strip():
        return
    out = resolve_path_under_root((save_raw_dir or "").strip(), project_root)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ms = int(time.time() * 1000) % 1_000_000
    ptag = f"p{page_idx}" if page_idx is not None else "url"
    fname = out / f"mtop_{ts}_{ms}_{ptag}.json"
    req: dict[str, Any] = {
        "url": request_url,
        "query_params": query_params,
        "cookie_on_wire": bool((cookie or "").strip()),
    }
    if headers is not None:
        req["headers"] = headers_for_archive(
            headers,
            redact_cookie=redact_request_cookie,
        )
    doc: dict[str, Any] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "page": page_idx,
        "request": req,
        "response_raw_text": raw_text,
        "response_parsed": parsed,
        "response_parse_error": parse_error,
    }
    if (
        cookie is not None
        and (app_key_for_sign or "").strip()
    ):
        doc["request_plain"] = build_request_plain(
            cookie=cookie,
            query_params=query_params,
            app_key_for_sign=str(app_key_for_sign).strip(),
        )
    fname.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已保存请求/响应原文: {fname}", file=log_stream)


def query_params_from_url(full_url: str) -> dict[str, str]:
    q = urlparse(full_url).query
    flat: dict[str, str] = {}
    for k, vals in parse_qs(q, keep_blank_values=True).items():
        flat[k] = vals[0] if len(vals) == 1 else ",".join(vals)
    return flat


def mtop_fetch_json_payload(
    api: APIRequestContext,
    cookie: str,
    args: argparse.Namespace,
    page_idx: int,
    app_key: str,
    timeout: float,
    project_root: Path,
) -> dict[str, Any]:
    """拼装 URL，经 Playwright ``api.get`` 拉取 JSONP 并去壳。"""
    headers = build_default_mtop_headers(cookie, args, srp_page=page_idx)
    inner = build_inner_params_from_args(args, page=page_idx)
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
        raise ValueError(err)
    assert t_str is not None and sign_str is not None
    qs = build_query_params(
        t=t_str,
        sign=sign_str,
        data_json=data_val,
        callback=str(args.callback),
        app_key=app_key,
    )
    url = MTOP_RECOMMEND_BASE_URL + "?" + urlencode(qs)
    sleep_before_request(
        float(getattr(args, "request_delay_min", 30.0)),
        float(getattr(args, "request_delay_max", 60.0)),
    )
    res = fetch_mtop_jsonp(
        api,
        url,
        headers=headers,
        callback=str(args.callback),
        timeout=timeout,
    )
    raw = res.raw_text
    parsed = res.parsed
    parse_err = res.parse_error
    save_mtop_exchange(
        project_root,
        (getattr(args, "save_raw_dir", None) or "").strip() or None,
        page_idx=page_idx,
        request_url=url,
        query_params=qs,
        raw_text=raw,
        parsed=parsed,
        parse_error=parse_err,
        cookie=cookie,
        headers=headers,
        app_key_for_sign=app_key,
        redact_request_cookie=bool(getattr(args, "redact_cookie_in_save", False)),
    )
    if parsed is None:
        strip_jsonp(raw, str(args.callback))
    return parsed  # type: ignore[return-value]
