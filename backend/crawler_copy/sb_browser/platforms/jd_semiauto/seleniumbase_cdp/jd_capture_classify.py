# -*- coding: utf-8 -*-
"""CDP 捕获：list/detail/comment/graphic 粗分 + ``resolved_sku``（functionId 优先，否则结构启发）。"""
from __future__ import annotations

import json as _json
from typing import Any
from urllib.parse import parse_qs, urlparse

from sb_browser.cdp_json_listen import CapturedJsonResponse

SKUISH_KEYS = frozenset({"skuId", "wareId", "productId", "sku_id", "SKU"})


def function_id_from_jd_api_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    q = parse_qs(urlparse(u).query, keep_blank_values=False)
    for k in ("functionId", "functionid"):
        vs = q.get(k)
        if vs and vs[0]:
            return str(vs[0]).strip()
    return ""


def _fid_norm(url: str) -> str:
    return function_id_from_jd_api_url(url).strip().lower()


def _looks_like_pc_search(parsed: dict[str, Any]) -> bool:
    data = parsed.get("data")
    if not isinstance(data, dict):
        return False
    for key in ("wareList", "wareListPro"):
        wl = data.get(key)
        if isinstance(wl, list) and len(wl) > 0:
            return True
    return False


def _sku_from_url_body(url: str) -> str:
    """从 URL 查询参数 body 的 JSON 里提取 skuId（如 pc_detailpage_wareBusiness 格式）。"""
    try:
        q = parse_qs(urlparse(url).query, keep_blank_values=False)
        body_str = (q.get("body") or [""])[0]
        if not body_str:
            return ""
        body = _json.loads(body_str)
        sku = body.get("skuId") or body.get("wareId") or ""
        s = str(sku).strip()
        return s if s.isdigit() and len(s) >= 5 else ""
    except Exception:
        return ""


def _looks_like_ware_graphic(parsed: dict[str, Any]) -> bool:
    """商详配料/长图文 ``pc_item_getWareGraphic``：``data.graphicContent`` 为含懒加载图的 HTML。"""
    data = parsed.get("data")
    if not isinstance(data, dict):
        return False
    gc = data.get("graphicContent")
    return isinstance(gc, str) and "data-lazyload=" in gc


def _looks_like_ware_business(parsed: dict[str, Any]) -> bool:
    # productAttributeVO 出现在根层级（pc_detailpage_wareBusiness 的新版响应结构）
    if "productAttributeVO" in parsed:
        return True
    data = parsed.get("data")
    if not isinstance(data, dict):
        return False
    if "wareInfo" in data or "componentWareInfo" in data:
        return True
    # 扁平后常见：根上带 shareUrl 等
    if any(k in data for k in ("ybPackUrl", "propertyGroupList", "productName")):
        return True
    return False


def _looks_like_comment(parsed: dict[str, Any]) -> bool:
    if not isinstance(parsed, dict):
        return False
    if isinstance(parsed.get("commentInfoList"), list):
        return True
    if isinstance(parsed.get("lastCommentInfoList"), list):
        return True
    # getCommentListPage：floors 在根层级
    floors = parsed.get("floors")
    if isinstance(floors, list) and floors:
        return True
    data = parsed.get("data")
    if isinstance(data, dict):
        if isinstance(data.get("commentInfoList"), list):
            return True
        fl = data.get("floors")
        if isinstance(fl, list) and fl:
            return True
    # client.action 评论接口：floors 在 result 下，mId 含 "comment"
    result_obj = parsed.get("result")
    if isinstance(result_obj, dict):
        fl = result_obj.get("floors")
        if isinstance(fl, list) and any(
            "comment" in str(f.get("mId", "")).lower() for f in fl
        ):
            return True
    return False


def _first_skuish_value(obj: Any, *, budget: int) -> tuple[str, int]:
    """
    深度优先找第一个 «skuId»/«wareId» 等键的**非空字符串值**（数字也会 str 化）。
    返回 (sku, remaining_budget)。
    """
    if budget <= 0:
        return "", 0
    if isinstance(obj, dict):
        for key in SKUISH_KEYS:
            if key in obj:
                v = obj.get(key)
                s = str(v).strip() if v is not None else ""
                if s and s.isdigit() and len(s) >= 5:
                    return s, budget - 1
        for v in obj.values():
            found, budget = _first_skuish_value(v, budget=budget - 1)
            if found:
                return found, budget
        return "", budget - 1
    if isinstance(obj, list):
        for x in obj[:80]:
            found, budget = _first_skuish_value(x, budget=budget - 1)
            if found:
                return found, budget
        return "", budget - 1
    return "", budget - 1


def classify_jd_capture(row: CapturedJsonResponse) -> tuple[str, str, str]:
    """
    返回 ``(kind, resolved_sku, function_id)``。

    kind: ``list`` | ``detail`` | ``comment`` | ``graphic`` | ``unknown``
    resolved_sku: 列表类固定空串；详情/评论尽力从 JSON 解出，失败为 ``""``。
    """
    url = (row.url or "").strip()
    fid = function_id_from_jd_api_url(url)
    fid_l = fid.strip().lower()

    parsed_obj = row.parsed
    if not isinstance(parsed_obj, dict):
        return "unknown", "", fid

    if row.parse_error:
        return "unknown", "", fid

    # ----- URL functionId（list 仅认 body 内非空 wareList，见 _looks_like_pc_search）-----

    if "pc_detailpage_warebusiness" in fid_l:
        sku, _ = _first_skuish_value(parsed_obj, budget=600)
        if not sku:
            sku = _sku_from_url_body(url)
        return "detail", sku, fid

    if (
        "getlegowaredetailcomment" in fid_l
        or "getcommentlistpage" in fid_l
        or "commentlist" in fid_l
    ):
        sku, _ = _first_skuish_value(parsed_obj, budget=600)
        return "comment", sku, fid

    if "pc_item_getwaregraphic" in fid_l:
        sku = _sku_from_url_body(url)
        if not sku:
            sku, _ = _first_skuish_value(parsed_obj, budget=600)
        return "graphic", sku, fid

    # ----- Body heuristics -----
    if _looks_like_pc_search(parsed_obj):
        return "list", "", fid or ""

    if _looks_like_comment(parsed_obj):
        sku, _ = _first_skuish_value(parsed_obj, budget=600)
        return "comment", sku, fid or ""

    if _looks_like_ware_graphic(parsed_obj):
        sku = _sku_from_url_body(url)
        if not sku:
            sku, _ = _first_skuish_value(parsed_obj, budget=600)
        return "graphic", sku, fid or ""

    if _looks_like_ware_business(parsed_obj):
        sku, _ = _first_skuish_value(parsed_obj, budget=600)
        if not sku:
            sku = _sku_from_url_body(url)
        return "detail", sku, fid or ""

    return "unknown", "", fid or ""
