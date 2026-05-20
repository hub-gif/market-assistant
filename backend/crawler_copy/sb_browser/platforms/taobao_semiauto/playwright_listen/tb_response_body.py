# -*- coding: utf-8 -*-
"""淘宝监听：从 Response 正文解析 JSON / JSONP / 识别 HTML（与 ``data/TB/sample`` 对齐）。"""
from __future__ import annotations

import json
import re
from typing import Any, Literal

from sb_browser.platforms.taobao_semiauto.common import constants_taobao_semiauto as _cfg

ParseShape = Literal["empty", "json", "jsonp", "html", "unparsed"]


def truncate_utf8_bytes(s: str, max_bytes: int) -> tuple[str, bool]:
    """按 UTF-8 字节截断，返回 ``(文本, 是否发生过截断)``。"""
    if max_bytes <= 0:
        return "", True
    raw = s.encode("utf-8")
    if len(raw) <= max_bytes:
        return s, False
    cut = bytearray(raw[:max_bytes])
    while cut and (cut[-1] & 0b1100_0000) == 0b1000_0000:
        cut.pop()
    return bytes(cut).decode("utf-8", errors="ignore"), True


def looks_like_html_document(text: str) -> bool:
    s = text.lstrip("\ufeff \t\r\n").lower()
    return s.startswith("<!doctype html") or s.startswith("<html")

_JSONP_LEAD_RE = re.compile(r"^[A-Za-z_$][\w$.]*\s*\(")


def _strip_jsonp_inner(text: str) -> str | None:
    """取 ``callback(...)`` 内层载荷；外层仅一对括号时使用 ``rfind`` 即可（与 MTOP JSONP 样例一致）。"""
    s = text.lstrip("\ufeff \t\r\n")
    if not _JSONP_LEAD_RE.match(s):
        return None
    left = s.find("(")
    right = s.rfind(")")
    if left < 0 or right <= left:
        return None
    return s[left + 1 : right].strip()


def parse_tb_response_body(body_text: str) -> tuple[Any | None, ParseShape]:
    """返回 ``(parsed, shape)``。HTML 或未识别正文时 ``parsed`` 为 ``None``。"""
    raw = body_text or ""
    if not raw.strip():
        return None, "empty"

    stripped = raw.lstrip("\ufeff \t\r\n")
    if looks_like_html_document(stripped):
        return None, "html"

    try:
        return json.loads(stripped), "json"
    except json.JSONDecodeError:
        pass

    inner = _strip_jsonp_inner(stripped)
    if inner:
        try:
            return json.loads(inner), "jsonp"
        except json.JSONDecodeError:
            return None, "unparsed"

    return None, "unparsed"


def store_body_payload(*, shape: ParseShape, full_text: str) -> tuple[str | None, bool]:
    """根据 shape 决定落盘正文：HTML 或未解析时写入截断 ``body_text``。"""
    max_b = getattr(_cfg, "SEMI_MAX_BODY_TEXT_STORE_BYTES", 900_000)
    if shape in ("html", "unparsed"):
        return truncate_utf8_bytes(full_text, max_b)
    return None, False
