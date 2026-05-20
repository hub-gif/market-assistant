# -*- coding: utf-8 -*-
"""mtop JSONP 响应解析与常见错误提示（风控、非法请求等）。"""
from __future__ import annotations

import json
import re
from typing import Any

__all__ = ["mtop_stderr_hint", "strip_jsonp"]


def strip_jsonp(body: str, callback: str | None = None) -> dict[str, Any]:
    text = body.strip().rstrip(";")
    if callback:
        prefix = f"{callback}("
        if text.startswith(prefix) and text.endswith(")"):
            inner = text[len(prefix) : text.rfind(")")]
            return json.loads(inner)
    m = re.match(r"^[a-zA-Z0-9_]+\((.*)\)\s*$", text, re.DOTALL)
    if not m:
        raise ValueError("响应不是预期的 JSONP 格式")
    return json.loads(m.group(1))


def mtop_stderr_hint(payload: dict[str, Any]) -> str | None:
    """若判定为风控/业务失败，返回写入 stderr 的说明；成功则 None。"""
    ret = payload.get("ret")
    parts: list[str] = []
    if isinstance(ret, list):
        for x in ret:
            if isinstance(x, str):
                parts.append(x)
    blob = " ".join(parts)

    data = payload.get("data")
    punish = False
    if isinstance(data, dict):
        u = data.get("url")
        if isinstance(u, str) and ("punish" in u or "bixi.alicdn.com" in u):
            punish = True

    if "RGV587" in blob or punish or "被挤爆" in blob or "哎哟喂" in blob:
        return (
            "提示：以上为淘宝风控拦截（RGV587），不是脚本解析错误。"
            "即使已用 Playwright 的 Chromium 网络栈，仍可能被环境/频率/人机策略拦截；"
            "请保持 Cookie 与 _m_h5_tk 同会话，t/sign 与 data 一致（勿用占位符）。"
            "可将 TB_HEADLESS 设为 False 使用有头模式、降低 TB_REQUEST_DELAY_*；或把浏览器当场完整请求 URL 写入 TB_URL。"
        )

    if any("FAIL_SYS_ILLEGAL_ACCESS" in x or "非法请求" in x for x in parts):
        return (
            "提示：FAIL_SYS_ILLEGAL_ACCESS 多为 t/sign 与 data 或 _m_h5_tk 不一致。"
            "勿填写占位符 TB_T；请留白 TB_T/TB_SIGN 让脚本根据 Cookie 自动签名，"
            "或将与当前会话一致的完整请求 URL 写入 TB_URL。"
        )

    if parts and isinstance(parts[0], str) and not parts[0].startswith("SUCCESS::"):
        return f"提示：接口未返回 SUCCESS，ret={blob[:300]}"

    return None
