# -*- coding: utf-8 -*-
"""聚合接口四分类（functionId 优先）；含商详配料长图 ``pc_item_getWareGraphic`` → ``graphic``。"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse

Kind = Literal["list", "detail", "comment", "graphic"]

_FUNCTION_ID_TO_KIND: dict[str, Kind] = {
    "pc_detailpage_wareBusiness": "detail",
    "getLegoWareDetailComment": "comment",
    "getCommentListPage": "comment",
    "pc_item_getWareGraphic": "graphic",
}


def function_id_from_url(url: str) -> str | None:
    if not url:
        return None
    qs = parse_qs(urlparse(url).query)
    for key in ("functionId", "functionid"):
        vals = qs.get(key)
        if vals and vals[0]:
            return vals[0].strip()
    return None


def kind_from_parsed(parsed: Any) -> Kind | None:
    if not isinstance(parsed, dict):
        return None
    if "productAttributeVO" in parsed:
        return "detail"
    if "commentFloorShowNum" in parsed and "commentIconInfo" in parsed:
        return "comment"
    if isinstance(parsed.get("commentInfoList"), list):
        return "comment"
    if isinstance(parsed.get("floors"), list) and parsed["floors"]:
        return "comment"
    result = parsed.get("result")
    if isinstance(result, dict):
        fl = result.get("floors")
        if isinstance(fl, list) and fl:
            return "comment"
    data = parsed.get("data")
    if isinstance(data, dict):
        for key in ("wareList", "wareListPro"):
            wl = data.get(key)
            if isinstance(wl, list) and len(wl) > 0:
                return "list"
    if isinstance(data, dict):
        gc = data.get("graphicContent")
        if isinstance(gc, str) and gc.strip():
            if "data-lazyload=" in gc or "background-image:url" in gc.replace(" ", ""):
                return "graphic"
    return None


def classify_jd_aggregate(*, url: str = "", parsed: Any = None) -> Kind | None:
    from_body = kind_from_parsed(parsed)
    if from_body is not None:
        return from_body
    fid = function_id_from_url(url)
    if not fid:
        return None
    if fid in _FUNCTION_ID_TO_KIND:
        return _FUNCTION_ID_TO_KIND[fid]
    fid_l = fid.lower()
    for reg, k in _FUNCTION_ID_TO_KIND.items():
        if reg.lower() == fid_l:
            return k
    return None


def classify_capture_envelope(obj: dict[str, Any]) -> Kind | None:
    url = (obj.get("url") or "").strip()
    p = obj.get("parsed")
    return classify_jd_aggregate(url=url, parsed=p)


def _sample_paths_low_gi() -> list[Path]:
    from sb_browser.platforms.jd_semiauto.common.low_gi_root import low_gi_project_root

    root = low_gi_project_root()
    rel = (
        "data/JD/sb_cdp_api_semiauto/20260508_155635_清肌水光精华液/list/jd_list_001_kw_清肌水光精华液_t20260508_155636.json",
        "data/JD/sb_cdp_api_semiauto/20260508_155635_清肌水光精华液/detail/jd_detail_001_sku_10136677977625_kw_清肌水光精华液_t20260508_155636.json",
        "data/JD/sb_cdp_api_semiauto/20260508_155635_清肌水光精华液/comment/jd_comment_001_sku_unknown_kw_清肌水光精华液_t20260508_155636.json",
    )
    return [root / p for p in rel]


if __name__ == "__main__":
    _cc = Path(__file__).resolve().parents[4]
    if str(_cc) not in sys.path:
        sys.path.insert(0, str(_cc))
    paths = [Path(p) for p in sys.argv[1:]] or _sample_paths_low_gi()
    for fp in paths:
        if not fp.is_file():
            print(f"skip (missing): {fp}", file=sys.stderr)
            continue
        obj = json.loads(fp.read_text(encoding="utf-8"))
        k = classify_capture_envelope(obj)
        print(f"{k}\t{fp.name}\tfunction_id={obj.get('function_id')!r}")
