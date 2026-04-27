# -*- coding: utf-8 -*-
"""
京东商品评价 JSON 的**纯解析**：从 Lego / 列表页响应中抽取扁平评价行。

请求签名与 Playwright 见 ``jd_h5_item_comment_requests``。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

def parse_list_pages_spec(spec: str) -> list[str]:
    """
    --list-pages：``1-5`` → 1..5；``1,3,5`` → 单页序列；单数字 ``2`` → [\"2\"]。
    """
    s = (spec or "").strip()
    if not s:
        return ["1"]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    if "-" in s:
        parts = s.split("-", 1)
        lo, hi = int(parts[0].strip()), int(parts[1].strip())
        if lo > hi:
            lo, hi = hi, lo
        return [str(i) for i in range(lo, hi + 1)]
    return [s]


def category_and_first_guid_from_lego(parsed: Any) -> tuple[str, str]:
    """从 getLegoWareDetailComment 的 commentInfoList[0] 取 category（maidianInfo 前缀）与 guid。"""
    if not isinstance(parsed, dict):
        return "", ""
    lst = parsed.get("commentInfoList")
    if not isinstance(lst, list) or not lst:
        return "", ""
    first = lst[0]
    if not isinstance(first, dict):
        return "", ""
    guid = str(first.get("guid") or "").strip()
    maidian = str(first.get("maidianInfo") or "").strip()
    category = maidian.split("_", 1)[0].strip() if maidian else ""
    return category, guid


def loads_jd_plain_json(text: str) -> Any:
    s = (text or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def jd_business_ok(parsed: Any) -> bool:
    if not isinstance(parsed, dict):
        return False
    if parsed.get("success") is False:
        return False
    c = parsed.get("code")
    if c is None:
        return True
    return c == 0 or str(c) == "0"


def _clean_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return " ".join(s.split()) if s else ""


def _large_pic_urls_from_picture_list(pil: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(pil, list):
        return out
    for p in pil:
        if not isinstance(p, dict):
            continue
        u = p.get("largePicURL") or p.get("largePicUrl")
        if u:
            t = str(u).strip()
            if t and t not in out:
                out.append(t)
    return out


def _is_jd_single_comment_dict(d: dict) -> bool:
    """区分「一条评价」与标签/楼层等对象（getCommentListPage 里多为 commentInfo 扁平结构）。"""
    cid = d.get("commentId")
    if cid is None or str(cid).strip() == "":
        return False
    if d.get("userNickName") is None and not (
        d.get("tagCommentContent") or d.get("commentData")
    ):
        return False
    return True


def _walk_collect_comment_dicts(obj: Any, acc: list[dict[str, Any]]) -> None:
    """深度遍历 JSON，收集所有像单条评价的 dict（含 Lego 的 commentInfoList 项与列表页的 commentInfo）。"""
    if isinstance(obj, dict):
        if _is_jd_single_comment_dict(obj):
            acc.append(obj)
        for v in obj.values():
            _walk_collect_comment_dicts(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _walk_collect_comment_dicts(x, acc)


def _row_from_comment_dict(sku: str, item: dict[str, Any]) -> dict[str, Any]:
    text = _clean_text(
        item.get("tagCommentContent") or item.get("commentData")
    )
    buy = _clean_text(
        item.get("buyCountText") or item.get("repurchaseInfo")
    )
    date = _clean_text(
        item.get("commentDate") or item.get("newCommentDate")
    )
    return {
        "sku": str(sku).strip(),
        "commentId": str(item.get("commentId") or "").strip(),
        "userNickName": _clean_text(item.get("userNickName")),
        "tagCommentContent": text,
        "commentDate": date,
        "buyCountText": buy,
        "largePicURLs": _large_pic_urls_from_picture_list(
            item.get("pictureInfoList")
        ),
        "commentScore": str(item.get("commentScore") or "").strip(),
    }


def extract_comment_rows_from_parsed(sku: str, parsed: Any) -> list[dict[str, Any]]:
    """
    从整段 parsed 深度遍历抽取评价：
    - getLegoWareDetailComment：commentInfoList / lastCommentInfoList
    - getCommentListPage：result.floors → data 里 { commentInfo: {...} } 已拍平为内层字段，同上
    """
    if not isinstance(parsed, dict):
        return []
    acc: list[dict[str, Any]] = []
    _walk_collect_comment_dicts(parsed, acc)
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for item in acc:
        cid = str(item.get("commentId") or "").strip()
        dedup_key = f"{sku}:{cid}" if cid else f"{sku}:{id(item)}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        rows.append(_row_from_comment_dict(sku, item))
    return rows


def extract_comment_rows_from_jsonl_file(path: Path) -> list[dict[str, Any]]:
    """离线：从采集 JSONL 每行 { sku, parsed } 抽取扁平评价行。"""
    all_rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        try:
            rec = json.loads(s)
        except json.JSONDecodeError:
            continue
        sku = str(rec.get("sku") or "").strip()
        parsed = rec.get("parsed")
        all_rows.extend(extract_comment_rows_from_parsed(sku, parsed))
    return all_rows
