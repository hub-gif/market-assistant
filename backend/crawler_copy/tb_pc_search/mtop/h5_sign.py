# -*- coding: utf-8 -*-
"""
淘宝 H5 mtop 签名与 data 串拼装。

- ``_m_h5_tk`` 位于 Cookie，格式 ``token_epoch``，参与签名的为 **token**（下划线前一段）。
- ``sign = md5hex(f"{token}&{t}&{app_key}&{data_json}")``，其中 ``t`` 为毫秒时间戳字符串。

其他 mTop 接口可复用 ``encode_data_query_value``、``build_query_params`` 与本模块的 token/sign 计算。
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any

__all__ = [
    "build_query_params",
    "encode_data_query_value",
    "extract_m_h5_token",
    "mtop_auto_t_sign",
    "mtop_md5_sign",
]


def extract_m_h5_token(cookie: str) -> str | None:
    """从 Cookie 解析 _m_h5_tk，取第一段为签名用 token（格式 token_epoch）。"""
    m = re.search(r"(?:^|;\s*)_m_h5_tk=([^;]+)", cookie.strip(), re.I)
    if not m:
        return None
    raw = m.group(1).strip()
    if "_" not in raw:
        return None
    return raw.split("_", 1)[0]


def mtop_md5_sign(token: str, t: str, app_key: str, data: str) -> str:
    """与 H5 mtop 一致的 sign：md5hex(token & t & appKey & data)。"""
    s = f"{token}&{t}&{app_key}&{data}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def encode_data_query_value(app_id: str, inner: dict[str, Any]) -> str:
    """包装为接口要求的 data 内层 JSON 字符串（作为查询参数 ``data=`` 的值，整体再 JSON 一次由调用方组织）。"""
    outer = {
        "appId": app_id,
        "params": json.dumps(inner, ensure_ascii=False, separators=(",", ":")),
    }
    return json.dumps(outer, ensure_ascii=False, separators=(",", ":"))


def build_query_params(
    *,
    t: str,
    sign: str,
    data_json: str,
    callback: str = "mtopjsonp6",
    jsv: str = "2.7.4",
    app_key: str = "12574478",
    api: str = "mtop.relationrecommend.wirelessrecommend.recommend",
    v: str = "2.0",
    timeout_ms: str = "10000",
) -> dict[str, str]:
    """标准 mTop GET 查询参数字典（具体 api/v 可按接口替换）。"""
    return {
        "jsv": jsv,
        "appKey": app_key,
        "t": t,
        "sign": sign,
        "api": api,
        "v": v,
        "timeout": timeout_ms,
        "type": "jsonp",
        "dataType": "jsonp",
        "callback": callback,
        "data": data_json,
        "bx-ua": "fast-load",
    }


def mtop_auto_t_sign(
    cookie: str,
    app_key: str,
    data_json: str,
    *,
    manual_t: str | None = None,
    manual_sign: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    """
    根据 Cookie 与 data_json 得到 ``(t, sign, err)``；成功时 ``err`` 为 None。

    若同时传入 ``manual_t`` 与 ``manual_sign``，则原样使用（须与 data、会话一致）。
    """
    if manual_t and manual_sign:
        return (manual_t.strip(), manual_sign.strip(), None)
    if manual_t or manual_sign:
        return (None, None, "请同时提供 manual_t 与 manual_sign，或二者都省略以使用自动签名。")
    tok = extract_m_h5_token(cookie)
    if not tok:
        return (None, None, "Cookie 中无有效 _m_h5_tk，请补全登录 Cookie。")
    t_str = str(int(time.time() * 1000))
    return (t_str, mtop_md5_sign(tok, t_str, app_key, data_json), None)
