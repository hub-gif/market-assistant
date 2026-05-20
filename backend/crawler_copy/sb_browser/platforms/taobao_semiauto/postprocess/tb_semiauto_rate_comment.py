# -*- coding: utf-8 -*-
"""淘宝 ``mtop.taobao.rate.detaillist`` 的 ``rateList`` → 京东评价行字典（键与 ``jd_item_comment_parse`` 输出对齐）。"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


def _maybe_json_obj(s: str) -> Any:
    t = (s or "").strip()
    if not t:
        return None
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return None


def _coerce_parsed_data_root(parsed: Any) -> dict[str, Any] | None:
    if not isinstance(parsed, dict):
        return None
    data = parsed.get("data")
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        inner = _maybe_json_obj(data)
        return inner if isinstance(inner, dict) else None
    return None


def _rate_list_and_auction(parsed: dict[str, Any]) -> tuple[list[Any], str]:
    root = _coerce_parsed_data_root(parsed)
    if not root:
        return [], ""
    rl = root.get("rateList")
    if not isinstance(rl, list):
        rl = []
    auc = ""
    auction = root.get("auctionNumId")
    if auction is not None:
        auc = str(auction).strip()
    return rl, auc


def auction_num_id_from_h5_capture_blob(blob: dict[str, Any]) -> str:
    """从捕获包 URL（``data`` 查询参数 JSON）中取 ``auctionNumId``，无则退回根级 ``auctionNumId``。"""
    u = str(blob.get("url") or "")
    parsed = blob.get("parsed")
    parsed_dict = parsed if isinstance(parsed, dict) else {}
    _, from_data = _rate_list_and_auction(parsed_dict)
    if from_data:
        return from_data
    try:
        qs = parse_qs(urlparse(u).query)
        raw = (qs.get("data") or [""])[0]
        inner = _maybe_json_obj(unquote(raw))
        if isinstance(inner, dict):
            aid = inner.get("auctionNumId")
            if aid is not None:
                return str(aid).strip()
    except Exception:
        pass
    return ""


def _normalize_pic(url: str) -> str:
    s = url.strip()
    if s.startswith("//"):
        return "https:" + s
    return s


def _pics_from_tb_rate(rate: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for p in rate.get("feedPicList") or []:
        if not isinstance(p, dict):
            continue
        for k in ("thumbnail", "url", "largePic"):
            val = p.get(k)
            if val:
                t = _normalize_pic(str(val).strip())
                if t and t not in urls:
                    urls.append(t)
                break
    for raw in rate.get("feedPicPathList") or []:
        if isinstance(raw, str) and raw.strip():
            t = _normalize_pic(raw.strip())
            if t not in urls:
                urls.append(t)
    return urls


def _clean(text: Any) -> str:
    if text is None:
        return ""
    return " ".join(str(text).strip().split())


def tb_rate_row_to_jd_comment_row(sku: str, rate: dict[str, Any]) -> dict[str, Any]:
    """单条淘宝评价 → ``extract_comment_rows_from_parsed`` 同款键，供 ``comments_flat.csv`` / 合并表沿用。"""
    sku_f = str(sku or "").strip()
    cid = str(rate.get("id") or rate.get("feedId") or "").strip()
    nick = _clean(rate.get("userNick") or rate.get("reduceUserNick") or "")
    content = _clean(rate.get("feedback") or rate.get("feedbackTitle") or "")
    date_v = rate.get("feedbackDate") or rate.get("createTime") or rate.get("createTimeInterval") or ""
    date_s = _clean(date_v)
    repeat = rate.get("repeatBusiness") == "true" or str(rate.get("repeatBusiness") or "").lower() == "true"
    buy_txt = "重复购买" if repeat else ""
    score = _clean(rate.get("userStar") or rate.get("rateType") or "")
    pics = _pics_from_tb_rate(rate)

    auction = str(rate.get("auctionNumId") or "").strip()
    row_sku = sku_f or auction

    return {
        "sku": row_sku,
        "commentId": cid,
        "userNickName": nick,
        "tagCommentContent": content,
        "commentDate": date_s,
        "buyCountText": buy_txt,
        "largePicURLs": pics,
        "commentScore": score,
    }


def extract_tb_comment_rows_from_blob(blob: dict[str, Any]) -> list[dict[str, Any]]:
    """读取半自动捕获包 ``comment/*.json``，输出京东形评价行列表。"""
    parsed = blob.get("parsed")
    if not isinstance(parsed, dict):
        return []
    sku = auction_num_id_from_h5_capture_blob(blob)
    rl, root_auc = _rate_list_and_auction(parsed)
    if root_auc and not sku:
        sku = root_auc
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rate in rl:
        if not isinstance(rate, dict):
            continue
        row = tb_rate_row_to_jd_comment_row(sku, rate)
        cid = str(row.get("commentId") or "").strip()
        dedup = f"{row.get('sku')}:{cid}" if cid else f"{sku}:{id(rate)}"
        if dedup in seen:
            continue
        seen.add(dedup)
        out.append(row)
    return out
