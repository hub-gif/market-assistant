# -*- coding: utf-8 -*-
"""中文版 DevTools「复制为文本」：抽取请求 URL、referer；导出常含 Cookie，勿入库。"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


_REQUEST_URL_HEADING = "请求网址"


def extract_request_url_from_devtools_cn_export(text: str) -> str:
    ls = text.splitlines()
    for i, raw in enumerate(ls):
        if raw.strip() == _REQUEST_URL_HEADING:
            for j in range(i + 1, min(i + 12, len(ls))):
                u = ls[j].strip()
                if u.startswith(("http://", "https://")):
                    return u
            break
    return ""


def _normalize_header_token(line: str) -> str | None:
    t = line.strip()
    if not t or ":" in t or t.startswith(":"):
        return None
    low = t.lower()
    # 单行「键:值」形如 cookie: xxx
    if low in frozenset(
        {"referer", "x-referer-page", "origin", ":authority"}
    ):
        return low.replace(":authority", "authority")
    return None


def extract_next_line_headers(text: str) -> dict[str, str]:
    """
    DevTools 文本里常为「键名单独一行 / 取值下一行」。
    """
    ls = text.splitlines()
    out: dict[str, str] = {}
    i = 0
    while i < len(ls):
        key = _normalize_header_token(ls[i])
        if key and i + 1 < len(ls):
            val = ls[i + 1].strip()
            if val and not _normalize_header_token(val):
                out[key] = val
                i += 2
                continue
        # 单行 key: value（少数）
        m = re.match(r"^([\w\-]+)\s*:\s*(.+)$", ls[i].strip())
        if m:
            k, v = m.group(1).strip().lower(), m.group(2).strip()
            if k not in ("http",):  # skip garbage
                out[k] = v
        i += 1
    # 兜底：整块里找 item.jd 商品页 URL
    m2 = re.search(
        r"https?://item\.jd\.com/(\d+)\.html",
        text,
        flags=re.IGNORECASE,
    )
    if m2:
        out.setdefault("_html_sku_hint", m2.group(1))
        out.setdefault("x-referer-page", m2.group(0))
    return out


def sku_from_item_jd_url(url: str) -> str:
    m = re.search(r"item\.jd\.com/(\d+)\.html", url or "", flags=re.IGNORECASE)
    return str(m.group(1)).strip() if m else ""


def sku_from_warebusiness_get_url(request_url: str) -> str:
    """ wareBusiness：`body` query 中为 JSON ``{"skuId":"..."}"``。"""
    u = (request_url or "").strip()
    if not u:
        return ""
    q = parse_qs(urlparse(u).query)
    bod = (q.get("body") or [None])[0]
    if not bod:
        return ""
    raw = unquote(str(bod))
    try:
        j = json.loads(raw)
        if isinstance(j, dict):
            sid = str(j.get("skuId") or "").strip()
            return sid if sid.isdigit() else ""
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


def infer_capture_kind(request_url: str) -> str:
    u = (request_url or "").strip().lower()
    fid = function_id_hint_from_url(request_url).lower()

    if "pc_search" in fid or fid.endswith("searchware"):
        return "list"
    if "pc_detailpage_warebusiness" in fid:
        return "detail"
    if "pc_item_getwaregraphic" in fid:
        return "graphic"
    if any(
        x in fid
        for x in (
            "getlegowaredetailcomment",
            "getcommentlistpage",
            "comment",
        )
    ):
        return "comment"
    if "/client.action" in u:
        return "comment"
    if "functionid=" in u and "detailpage" in u:
        return "detail"
    return "unknown"


def function_id_hint_from_url(request_url: str) -> str:
    q = parse_qs(urlparse(request_url).query)
    for k in ("functionId", "functionid"):
        vs = q.get(k)
        if vs and vs[0]:
            return str(vs[0]).strip()
    return ""


def resolve_sku_for_devtools_capture(
    *,
    kind: str,
    request_url: str,
    header_map: dict[str, str],
) -> str:
    if kind == "list":
        return ""
    if kind == "detail" or kind == "graphic":
        s = sku_from_warebusiness_get_url(request_url)
        if s:
            return s
    ref = (
        (header_map.get("x-referer-page") or "").strip()
        or (header_map.get("referer") or "").strip()
    )
    s2 = sku_from_item_jd_url(ref)
    if s2:
        return s2
    return (header_map.get("_html_sku_hint") or "").strip()


def scan_devtools_txt_paths(paths: list[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in paths:
        if not p.is_file():
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        url = extract_request_url_from_devtools_cn_export(text)
        hdrs = extract_next_line_headers(text)
        kind = infer_capture_kind(url)
        sku = resolve_sku_for_devtools_capture(
            kind=kind, request_url=url, header_map=hdrs
        )
        fid = function_id_hint_from_url(url)
        out.append(
            {
                "file": str(p.resolve()),
                "filename": p.name,
                "capture_kind": kind,
                "resolved_sku": sku or "",
                "function_id": fid or "",
                "request_url": url,
                "referer_hints": {
                    k: hdrs[k]
                    for k in ("referer", "x-referer-page", "_html_sku_hint")
                    if k in hdrs
                },
                "note": "仅请求头摘录；响应 JSON 请在 Network→Response 另存或由 CDP 半自动抓取。",
            }
        )
    return out


DEFAULT_RELATIVE_PIPELINE_JSON_DIR = Path("data") / "JD" / "pipeline_runs" / "json"
