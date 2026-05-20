# -*- coding: utf-8 -*-
"""盘后 dedupe_key：商详 SKU、列表 wareList 序列 + page/s，评论 commentId 摘要等；监听不写。"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from urllib.parse import parse_qs, unquote, urlparse

from sb_browser.cdp_json_listen import CapturedJsonResponse


# 不参与 URL body 顶层指纹的噪声（列表键仍主要靠 page/s + wareList SKU 序列）
_DROP_BODY_TOP_KEYS = frozenset(
    {
        "uuid",
        "loginType",
        "lng",
        "lat",
        "poiPos",
        "gLng1",
        "gLat1",
        "fingerPrint",
        "eid",
    }
)


def _sha256_utf8(text: str) -> str:
    raw = text.strip().encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def jd_url_body_dict(url: str) -> dict[str, Any]:
    """解析 ``api.m.jd.com`` 类 URL 的 ``body=`` JSON 对象为 dict。"""
    try:
        q = parse_qs(urlparse((url or "").strip()).query, keep_blank_values=False)
        raw = (q.get("body") or [None])[0]
        if not raw:
            return {}
        s = unquote(str(raw))
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _scrub_body_top(d: dict[str, Any]) -> dict[str, Any]:
    return {str(k): v for k, v in d.items() if str(k) not in _DROP_BODY_TOP_KEYS}


def _detail_sku_id(resolved_sku: str, url: str, parsed: Any) -> str:
    """商详可用的数位 SKU（优先 classify 结果，再 URL body，再 parsed 浅层）。"""
    sku = (resolved_sku or "").strip()
    if sku.isdigit() and len(sku) >= 5:
        return sku
    bd = jd_url_body_dict(url)
    for k in ("skuId", "wareId"):
        v = bd.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit() and len(s) >= 5:
            return s
    root = parsed if isinstance(parsed, dict) else {}
    for k in ("skuId", "wareId"):
        v = root.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit() and len(s) >= 5:
            return s
    data = root.get("data") if isinstance(root.get("data"), dict) else {}
    if isinstance(data, dict):
        for k in ("skuId", "wareId"):
            v = data.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s.isdigit() and len(s) >= 5:
                return s
    return ""


def _extract_list_natural_skus(parsed: dict[str, Any]) -> tuple[str, ...]:
    """与搜索结果解析一致的 wareList 顺位 SKU（跳过显式广告位）。"""
    try:
        from jd_pc_search.search.jd_h5_search_parse import (  # noqa: WPS433
            JD_SKU_KEYS,
            _jd_flatten_ware,
            _jd_is_explicit_pc_search_ad_ware,
            _sval_jd,
        )
    except ImportError:
        return ()
    data = parsed.get("data")
    if not isinstance(data, dict):
        return ()
    wl = data.get("wareList")
    if not isinstance(wl, list):
        return ()
    out: list[str] = []
    for w in wl:
        if not isinstance(w, dict):
            continue
        d0 = _jd_flatten_ware(w)
        if _jd_is_explicit_pc_search_ad_ware(d0):
            continue
        sku = _sval_jd(d0, JD_SKU_KEYS).strip()
        if sku.isdigit() and len(sku) >= 5:
            out.append(sku)
    return tuple(out)


def _collect_comment_ids(obj: Any, acc: list[str], *, budget: int) -> int:
    """深度收集 ``commentId``（京东多种评论 JSON 版型）。"""
    if budget <= 0:
        return 0
    if isinstance(obj, dict):
        cid = obj.get("commentId")
        if cid is not None:
            s = str(cid).strip()
            if s:
                acc.append(s)
        for v in obj.values():
            budget = _collect_comment_ids(v, acc, budget=budget - 1)
            if budget <= 0:
                return 0
    elif isinstance(obj, list):
        for x in obj[:800]:
            budget = _collect_comment_ids(x, acc, budget=budget - 1)
            if budget <= 0:
                return 0
    return budget


def _saved_capture_body_surrogate(blob: dict[str, Any]) -> str:
    """磁盘 JSON 无原始 response 正文时，用于与在线去重对齐的近似串（唯影响需 ``rsp_sha`` 的回退键）。"""
    echo = blob.get("_cdp_response_body_echo")
    if isinstance(echo, str) and echo.strip():
        return echo.strip()
    parsed = blob.get("parsed")
    if parsed is None:
        return ""
    try:
        return json.dumps(parsed, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return ""


def semantic_dedupe_key_parts(
    *,
    url: str,
    body_text: str,
    parsed: Any,
    kind: str,
    resolved_sku: str,
    function_id: str,
) -> str:
    """与盘后 ``postprocess``、以及工具侧在线 ``CapturedJsonResponse`` 共用的去重键。"""
    rsp = (body_text or "").strip()
    rsp_sha = _sha256_utf8(rsp) if rsp else "empty_rsp"
    fid = (function_id or "").strip().lower()
    sku_guess = (resolved_sku or "").strip()
    k = (kind or "").strip().lower()

    if k == "detail":
        dsku = _detail_sku_id(sku_guess, url, parsed)
        if dsku:
            return f"d:sku:{dsku}"
        return f"d:rsp:{rsp_sha}"

    parsed_dict = parsed if isinstance(parsed, dict) else {}

    if k == "list":
        skus = _extract_list_natural_skus(parsed_dict)
        bd = jd_url_body_dict(url)
        page_v = bd.get("page", "")
        s_v = bd.get("s", "")
        p_s = f"{page_v}:{s_v}"
        if skus:
            sku_sig = ",".join(skus)
            return f"l:ps:{p_s}:{_sha256_utf8(sku_sig)}"

        scr = _scrub_body_top(bd) if bd else {}
        bd_sig = ""
        if scr:
            bd_sig = json.dumps(scr, sort_keys=True, default=str)
        fb = "|".join((p_s, fid, bd_sig, rsp_sha))
        return f"l:fb:{_sha256_utf8(fb)}"

    if k == "comment":
        ids: list[str] = []
        _collect_comment_ids(parsed_dict, ids, budget=4000)
        uniq_sorted = ",".join(sorted(set(ids))) if ids else ""
        sku_part = sku_guess if sku_guess.isdigit() and len(sku_guess) >= 5 else ""
        if uniq_sorted:
            return f"c:sku:{sku_part}:ids:{_sha256_utf8(uniq_sorted)}"

        bd = jd_url_body_dict(url)
        scr = _scrub_body_top(bd) if bd else {}
        if scr:
            return f"c:sku:{sku_part}:req:{_sha256_utf8(json.dumps(scr, sort_keys=True, default=str))}"

        suf = sku_part or "x"
        return f"c:rsp:{suf}:{rsp_sha}"

    if k == "graphic":
        dsku = _detail_sku_id(sku_guess, url, parsed)
        sfx = fid[:48] if fid else "x"
        if dsku:
            return f"g:sku:{dsku}:{sfx}"
        return f"g:rsp:{rsp_sha}"

    if k == "unknown":
        return f"u:rsp:{rsp_sha}"

    return f"x:rsp:{rsp_sha}"


def semantic_dedupe_key_for_saved_capture_blob(blob: dict[str, Any]) -> str | None:
    """已落盘的 ``*.json`` blob（须含 ``capture_kind`` / ``parsed`` 等）；无法识别类别则 ``None``。"""
    kind = str(blob.get("capture_kind") or "").strip().lower()
    if kind not in ("list", "detail", "comment", "graphic", "unknown"):
        return None
    return semantic_dedupe_key_parts(
        url=str(blob.get("url") or ""),
        body_text=_saved_capture_body_surrogate(blob),
        parsed=blob.get("parsed"),
        kind=kind,
        resolved_sku=str(blob.get("resolved_sku") or ""),
        function_id=str(blob.get("function_id") or ""),
    )


def semantic_dedupe_key(
    *,
    row: CapturedJsonResponse,
    kind: str,
    resolved_sku: str,
    function_id: str,
) -> str:
    rsp = ((getattr(row, "body_text", None) or "") or "").strip()
    return semantic_dedupe_key_parts(
        url=row.url or "",
        body_text=rsp,
        parsed=row.parsed,
        kind=kind,
        resolved_sku=resolved_sku,
        function_id=function_id,
    )
