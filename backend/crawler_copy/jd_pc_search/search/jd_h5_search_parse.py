# -*- coding: utf-8 -*-
"""
京东 PC 搜索响应**解析**：JSON/HTML → 商品行、游标步进、重试判定。

HTTP、Node 签 URL 与 CLI 见 ``jd_h5_search_requests``。
"""
from __future__ import annotations

import html as html_module
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

_BACKEND_ROOT = Path(__file__).resolve().parents[3]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))
from pipeline.csv_schema import JD_SEARCH_CSV_HEADERS as JD_EXPORT_COLUMN_HEADERS  # noqa: E402

_JD_PC_SEARCH_DIR = Path(__file__).resolve().parent

# PC 搜索：与浏览器一致，**每逻辑页固定 2 包** pc_search（body.page 连续 +1：第 L 页为 2L-1 与 2L）。
# **Δs = max(1, 自然位条数-1)**（wareList 非广告计自然位）。跳过前序屏时同样每逻辑页 2 包推进游标。
# CLI 的 --page 是**逻辑页**，不是请求 body.page（避免混淆）。
JD_PC_SEARCH_ITEMS_PER_PAGE = 60
JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE = 2
# 重试仍失败时推进 s 的兜底（与常见两包 Δs≈21～22 一致）
JD_PC_SEARCH_FALLBACK_S_STEP = 22

def pc_search_response_is_empty_ware_list(body: str) -> bool:
    """合法 JSON 且 ``data.wareList`` 为明确空数组时视为「已无更多商品」，不宜再强推游标。"""
    p = _loads_json_or_jsonp((body or "").strip())
    if not isinstance(p, dict):
        return False
    data = p.get("data")
    return isinstance(data, dict) and data.get("wareList") == []


def pc_search_should_retry_fetch(
    body: str, *, has_rows: bool, s_step: int
) -> bool:
    """
    是否应对同一 body.page / s 重发请求。
    空体、非 JSON 短包等视为瞬时故障；明确的 ``data.wareList == []`` 不重试。
    """
    if has_rows or s_step > 0:
        return False
    s = (body or "").strip()
    if not s:
        return True
    p = _loads_json_or_jsonp(s)
    if p is None:
        return len(s) < 12000
    if not isinstance(p, dict):
        return False
    data = p.get("data")
    if isinstance(data, dict) and data.get("wareList") == []:
        return False
    return True

def jd_pc_api_body_page_first_pack(logical_page_1based: int) -> int:
    """逻辑页 L（从 1 起）第一包请求里的 body.page = 2L - 1。"""
    L = max(1, int(logical_page_1based))
    return 2 * L - 1


def _jd_row_count_for_page(rows: list[dict[str, str]], page: int) -> int:
    ps = str(page)
    return sum(1 for r in rows if (r.get("page") or "").strip() == ps)
# 与淘宝 CANONICAL_FIELDS 基本一致，但不导出 features / promotion_tags（用户侧去重简化）；
# 末尾追加 platform / keyword / page。
JD_ITEM_CSV_FIELDS = (
    "item_id",
    "sku_id",
    "title",
    # "title_plain",
    "price",
    "coupon_price",
    "original_price",
    "selling_point",
    "comment_sales_floor",
    "total_sales",
    "hot_list_rank",
    "comment_count",
    "shop_name",
    "shop_url",
    "shop_info_url",
    "location",
    "detail_url",
    "image",
    "seckill_info",
    "attributes",
    "leaf_category",
    # "same_count",
    # "relation_score",
    # "is_p4p",
    "platform",
    "keyword",
    "page",
)

CSV_FIELDS = tuple(JD_EXPORT_COLUMN_HEADERS[k] for k in JD_ITEM_CSV_FIELDS)


def jd_row_to_export(row: dict[str, str]) -> dict[str, str]:
    """内部键 → 导出列名；无内容则不写入该键（JSON 稀疏对象；CSV 缺列按空单元格写出）。"""
    out: dict[str, str] = {}
    for k in JD_ITEM_CSV_FIELDS:
        v = str(row.get(k, "") or "").strip()
        if not v:
            continue
        out[JD_EXPORT_COLUMN_HEADERS[k]] = v
    return out


def _jd_empty_export_row() -> dict[str, str]:
    return {k: "" for k in JD_ITEM_CSV_FIELDS}
def _human_text(s: str, max_len: int = 2000) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = html_module.unescape(t)
    t = " ".join(t.split()).strip()
    return t[:max_len]


def _safe_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://so.m.jd.com" + u
    if u.startswith("http://"):
        return "https://" + u[7:]
    return u


# 搜索接口里常见仅返回 jfs/...，需拼到 360buyimg 下（与 PC 列表 n2/s480x480 规格一致；img10–img14 等 CDN 可互换）
_JD_PRODUCT_IMG_HOST = "https://img13.360buyimg.com"


def _jd_product_image_url(u: str) -> str:
    """
    主图补全为可访问 URL。
    例：jfs/t1/402762/.../xxx.jpg → https://img13.360buyimg.com/n2/s480x480_jfs/t1/402762/.../xxx.jpg
    """
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("http://"):
        return "https://" + u[7:]
    if u.startswith("https://"):
        return u
    low = u.lower()
    if "360buyimg.com" in low and not re.match(r"^https?://", u, re.I):
        return "https://" + u.lstrip("/")
    p = u.lstrip("/")
    if p.startswith("jfs/"):
        return f"{_JD_PRODUCT_IMG_HOST}/n2/s480x480_{p}"
    if re.match(r"^n[0-9]+/", p):
        return f"{_JD_PRODUCT_IMG_HOST}/{p}"
    return u
def _detect_blocked(html_text: str) -> str | None:
    if not (html_text and html_text.strip()):
        return None
    t = html_text.lower()
    if "当前人数过多" in html_text or "前往京东app" in t or "前往京东APP" in html_text:
        return (
            "服务端限流/风控提示（纯文本）。search.action 易风控；"
            "含 h5st 的 api 请用 search/jd_search_playwright.py，或粘贴完整 URL（--url）；"
            "或 jd_low_gi_playwright.py 整页抓取。"
        )
    if "plogin.m.jd.com" in t or "passport.jd.com" in t:
        return "疑似被要求登录（跳转到登录域）。"
    # pc_search 正常 JSON 里字段名常含 risk/verify 等子串，宽松匹配会误判。
    payload = _loads_json_or_jsonp(html_text)
    if payload is not None:
        raw_probe: list[dict[str, Any]] = []
        _walk_collect_jd_wares(payload, raw_probe)
        if raw_probe:
            return None
        if re.search(
            r"risk\.jd\.com|riskhandler|/risk/|sev\.jd|verify\.jd\.com|"
            r"sliderverify|slide_verify|securityverify",
            html_text,
            re.I,
        ):
            return "疑似进入风控/验证页（risk/verify）。"
    elif "risk" in t and ("handler" in t or "verify" in t):
        return "疑似进入风控/验证页（risk/verify）。"
    if "验证码" in html_text or "安全验证" in html_text:
        return "疑似需要验证码/安全验证。"
    return None
def strip_jsonp(body: str) -> Any:
    """去掉 JSONP 包裹，解析为 Python 对象（与 taobao strip_jsonp 思路一致）。"""
    text = body.strip().rstrip(";")
    m = re.match(r"^[a-zA-Z_$][a-zA-Z0-9_$]*\((.*)\)\s*$", text, re.DOTALL)
    if not m:
        raise ValueError("响应不是预期的 JSONP 格式")
    return json.loads(m.group(1))


def _loads_json_or_jsonp(text: str) -> Any | None:
    s = text.strip()
    if not s:
        return None
    if s.startswith("{") or s.startswith("["):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return None
    try:
        return strip_jsonp(s)
    except (json.JSONDecodeError, ValueError):
        return None
JD_SKU_KEYS = ("wareId", "skuId", "itemId", "wid", "productId")
JD_TITLE_KEYS = ("wareName", "wname", "name", "title", "skuName")
# pc_search_searchWare 常见：jdPrice / jdPriceText / realPrice
JD_PRICE_KEYS = (
    "jdPrice",
    "jdPriceText",
    "realPrice",
    "price",
    "priceText",
    "p",
    "salePrice",
    "purchasePrice",
)
JD_IMG_KEYS = ("imageurl", "imageUrl", "imgUrl", "picUrl", "image")
JD_ORIGINAL_PRICE_KEYS = (
    "oriPrice",
    "originalPrice",
    "marketPrice",
    "linePrice",
    "mPrice",
    "maoPrice",
    "strikePrice",
)
# finalPrice 为对象，见 _jd_parse_final_price；勿在此放 dict 键名
JD_COUPON_KEYS = ("couponPrice", "subsidyPrice", "promoPrice")
JD_DETAIL_URL_KEYS = (
    "toUrl",
    "clickUrl",
    "wareUrl",
    "href",
    "itemUrl",
    "detailUrl",
    "skuUrl",
    "pUrl",
)
JD_SHOP_URL_KEYS = ("shopUrl", "storeUrl", "shopURL", "jumpUrl")
JD_LOC_KEYS = ("deliveryAddress", "area", "procity", "stockAddress", "sendAddr")
JD_TAG_LIST_KEYS = (
    "wareBeltList",
    "beltList",
    "labelList",
    "tagList",
    "wareTags",
    "tags",
    "icons",
    "serviceIcons",
    "iconList",
    "iconList1",
    "iconList2",
    "iconList3",
    "iconList4",
    "textTagList",
    "beltAttrs",
    "serviceTags",
    "benefitList",
    "sellPoints",
    "commonTagList",
)

# 单块对象里含 title:[str,…]（如 newRegionFloor）
JD_TAG_NEST_DICT_KEYS = (
    "newRegionFloor",
    "midTagList",
    "paragraphInfo",
    "floatLayerInfo",
    "drawer",
)

# JSON 里常见 title:["高膳食纤维","低GI食品"]、title:["礼盒装…热卖榜第1名"] 等字符串数组，需整树收集
JD_LIST_STRING_TEXT_KEYS = frozenset(
    {
        "title",
        "subtitle",
        "subtitles",
        "sellpoint",
        "sellpoints",
        "usp",
        "usps",
        "textlist",
        "textlists",
        "wordlist",
        "keywords",
        "highlight",
        "highlights",
        "sellingpoint",
        "sellingpoints",
        "featurelist",
        "pointlist",
        "points",
        "shorttitle",
        "shorttitles",
        "recommendreason",
        "reasonlist",
        "icontexts",
        "icontext",
        "tagtexts",
        "descs",
        "labels",
    }
)
def _sval_jd(d: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None or isinstance(v, (dict, list)):
            continue
        t = str(v).strip()
        if t:
            return t
    return ""


def _jd_flatten_ware(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for nest_key in ("wareInfo", "product", "skuInfo", "item", "main", "content", "base"):
        inner = out.get(nest_key)
        if isinstance(inner, dict):
            for k, v in inner.items():
                if k not in out or out[k] in (None, "", [], {}):
                    out[k] = v
    ex = out.get("exContent") or out.get("excontent")
    if isinstance(ex, dict):
        for k, v in ex.items():
            if k not in out or out[k] in (None, "", [], {}):
                out[k] = v
    return out


def _jd_norm_key(s: str) -> str:
    """去重用：压缩空白，便于合并「相同文案、空白不同」的重复。"""
    return " ".join(str(s).split()).strip()


def _jd_unique_ordered(strings: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in strings:
        t = _jd_norm_key(x)
        if len(t) < 2:
            continue
        k = t.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def _jd_benefit_list_title_lines(d: dict[str, Any]) -> list[str]:
    """benefitList[].title：PC 搜索里 sellingPoint 常为 null，卖点多在此。"""
    bl = d.get("benefitList")
    if not isinstance(bl, list):
        return []
    lines: list[str] = []
    for it in bl[:30]:
        if not isinstance(it, dict):
            continue
        t = it.get("title")
        if isinstance(t, list):
            for x in t[:25]:
                if isinstance(x, str) and x.strip():
                    lines.append(_jd_norm_key(x))
        elif isinstance(t, str) and t.strip():
            lines.append(_jd_norm_key(t))
    return lines


def _jd_sell_points_lines(d: dict[str, Any]) -> list[str]:
    """sellPoints：字符串列表或对象列表。"""
    sp = d.get("sellPoints")
    if not isinstance(sp, list):
        return []
    out: list[str] = []
    for x in sp[:25]:
        if isinstance(x, str) and x.strip():
            out.append(_jd_norm_key(x))
        elif isinstance(x, dict):
            t = _sval_jd(x, ("text", "title", "name", "desc"))
            if t:
                out.append(_jd_norm_key(t))
    return out


def _jd_selling_point_norm_set(d: dict[str, Any]) -> set[str]:
    """sellingPoint、benefitList.title、sellPoints 规范化键，用于树遍历去重。"""
    out: set[str] = set()
    sp = d.get("sellingPoint")
    if isinstance(sp, list):
        for x in sp:
            if isinstance(x, str) and x.strip():
                out.add(_jd_norm_key(x).casefold())
    for t in _jd_benefit_list_title_lines(d):
        out.add(t.casefold())
    for t in _jd_sell_points_lines(d):
        out.add(t.casefold())
    return out


def _jd_iter_tag_strings(d: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    sp_dup = _jd_selling_point_norm_set(d)

    def add(s: str, *, skip_if_selling_dup: bool = False) -> None:
        t = _jd_norm_key(s)
        if len(t) < 2:
            return
        if skip_if_selling_dup and sp_dup and t.casefold() in sp_dup:
            return
        k = t.casefold()
        if k in seen:
            return
        seen.add(k)
        out.append(t)

    sub_keys = (
        "text",
        "name",
        "title",
        "desc",
        "msg",
        "beltMsg",
        "labelName",
        "typeName",
        "showName",
        "content",
        "beltTitle",
    )
    for key in JD_TAG_LIST_KEYS:
        v = d.get(key)
        is_benefit = key == "benefitList"
        if isinstance(v, list):
            for it in v[:30]:
                if isinstance(it, str):
                    add(it)
                elif isinstance(it, dict):
                    for sk in sub_keys:
                        t = it.get(sk)
                        sk_skip = is_benefit and sk == "title"
                        if isinstance(t, list):
                            for x in t[:25]:
                                if isinstance(x, str) and x.strip():
                                    add(x.strip(), skip_if_selling_dup=sk_skip)
                        elif isinstance(t, str) and t.strip():
                            add(t.strip(), skip_if_selling_dup=sk_skip)
                            break
        elif isinstance(v, dict):
            for sk in sub_keys:
                t = v.get(sk)
                if isinstance(t, list):
                    for x in t[:25]:
                        if isinstance(x, str) and x.strip():
                            add(x.strip())
                elif isinstance(t, str) and t.strip():
                    add(t.strip())
                    break

    for key in JD_TAG_NEST_DICT_KEYS:
        v = d.get(key)
        if not isinstance(v, dict):
            continue
        tl = v.get("title")
        if isinstance(tl, list):
            for x in tl[:25]:
                if isinstance(x, str) and x.strip():
                    add(x.strip(), skip_if_selling_dup=True)
        elif isinstance(tl, str) and tl.strip():
            add(tl.strip(), skip_if_selling_dup=True)

    mtl = d.get("midTagList")
    if isinstance(mtl, list):
        for it in mtl[:25]:
            if not isinstance(it, dict):
                continue
            for sk in ("text", "name", "title", "desc", "msg"):
                t = it.get(sk)
                if isinstance(t, str) and t.strip():
                    add(t.strip())
                    break
    return out


def _jd_collect_list_string_fragments(
    root: Any,
    *,
    selling_norm_skip: set[str] | frozenset[str] | None = None,
    max_depth: int = 14,
    max_list_len: int = 40,
    max_branch_lists: int = 80,
) -> tuple[list[str], list[str]]:
    """
    从整棵 JSON 子树收集「键在 JD_LIST_STRING_TEXT_KEYS 且值为字符串数组」的文案。

    返回 (逐条短句, 每组用 · 拼接的整组)，供 hot_list_rank 等使用。
    """
    skip_sp = selling_norm_skip or set()
    individuals: list[str] = []
    groups: list[str] = []
    seen_ind: set[str] = set()
    seen_grp: set[str] = set()
    list_walk_count = 0

    def add_ind(s: str) -> None:
        t = _jd_norm_key(s)
        if len(t) < 2:
            return
        k = t.casefold()
        if skip_sp and k in skip_sp:
            return
        if k in seen_ind:
            return
        seen_ind.add(k)
        individuals.append(t)

    def walk(obj: Any, depth: int) -> None:
        nonlocal list_walk_count
        if depth > max_depth or obj is None:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                if (
                    isinstance(v, list)
                    and lk in JD_LIST_STRING_TEXT_KEYS
                    and list_walk_count < max_branch_lists
                ):
                    list_walk_count += 1
                    elems: list[str] = []
                    for x in v[:max_list_len]:
                        if isinstance(x, str) and x.strip():
                            t = _jd_norm_key(x)
                            if skip_sp and t.casefold() in skip_sp:
                                continue
                            elems.append(t)
                            add_ind(t)
                        elif isinstance(x, dict):
                            t = _sval_jd(
                                x,
                                ("text", "name", "title", "desc", "value", "content"),
                            )
                            if t:
                                t = _jd_norm_key(t)
                                if skip_sp and t.casefold() in skip_sp:
                                    continue
                                elems.append(t)
                                add_ind(t)
                    if elems:
                        gline = " · ".join(elems)
                        gk = gline.casefold()
                        if gk not in seen_grp:
                            seen_grp.add(gk)
                            groups.append(gline)
                walk(v, depth + 1)
        elif isinstance(obj, list):
            for x in obj[:90]:
                walk(x, depth + 1)

    walk(root, 0)
    return individuals, groups


# commentSalesFloor 等 attr+text 格式化时跳过明显非文案类 attr
JD_ATTR_TEXT_SKIP_ATTRS = frozenset(
    {
        "wareid",
        "skuid",
        "itemid",
        "imageurl",
        "imgurl",
        "href",
        "url",
        "cid",
        "shopid",
        "venderid",
    }
)


def _jd_is_rank_text(s: str) -> bool:
    t = s.strip()
    if not t:
        return False
    if "榜" in t:
        return True
    if "TOP" in t.upper() and len(t) <= 48:
        return True
    if re.search(r"第\s*\d+\s*名", t):
        return True
    if "热销" in t and len(t) <= 36:
        return True
    return False


def _jd_format_price_show_dict(ps: dict[str, Any]) -> tuple[str, str]:
    parts: list[str] = []
    coupon = ""
    for k, v in ps.items():
        if isinstance(v, str) and v.strip():
            parts.append(f"{k}={v.strip()[:100]}")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            parts.append(f"{k}={v}")
    line = " | ".join(parts)[:500]
    for ck in ("couponPrice", "purchasePrice", "finalPrice", "subsidyPrice"):
        v = ps.get(ck)
        if isinstance(v, str) and v.strip():
            coupon = v.strip()[:80]
            break
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            coupon = str(v)
            break
    return line, coupon


def _jd_parse_final_price(d: dict[str, Any]) -> tuple[str, str]:
    """
    pc 搜索 ware.finalPrice：到手价 + estimatedPrice。
    返回 (estimatedPrice 字符串, 展示用「到手价:25.5」)。
    """
    for fk in ("finalPrice", "mockFinalPrice", "intervalNewPrice"):
        fp = d.get(fk)
        if not isinstance(fp, dict):
            continue
        est = fp.get("estimatedPrice")
        if est is None:
            est = fp.get("price")
        tit = fp.get("title")
        tit_s = str(tit).strip() if tit is not None else ""
        if not tit_s:
            tit_s = "到手价"
        es = str(est).strip() if est is not None else ""
        if not es:
            continue
        return es, f"{tit_s}:{es}"
    return "", ""


def _jd_selling_point_text(d: dict[str, Any]) -> str:
    """优先根字段 sellingPoint；为空时用 benefitList / sellPoints（接口常返回 sellingPoint=null）。"""
    lines: list[str] = []
    sp = d.get("sellingPoint")
    if isinstance(sp, list):
        lines = [
            _jd_norm_key(x) for x in sp[:25] if isinstance(x, str) and x.strip()
        ]
    if not lines:
        lines = _jd_benefit_list_title_lines(d) + _jd_sell_points_lines(d)
    return _human_text(" | ".join(_jd_unique_ordered(lines)), 1200)


def _jd_format_comment_sales_floor(d: dict[str, Any]) -> str:
    """仅根字段 commentSalesFloor：[{attr,text},…]。"""
    csf = d.get("commentSalesFloor")
    if not isinstance(csf, list):
        return ""
    parts: list[str] = []
    for el in csf[:30]:
        if not isinstance(el, dict):
            continue
        text = el.get("text") or el.get("msg") or el.get("desc")
        if not isinstance(text, str) or not text.strip():
            continue
        t = _jd_norm_key(text)
        attr = el.get("attr") or el.get("type") or el.get("key") or ""
        if isinstance(attr, str) and attr.strip():
            lk = attr.strip().lower()
            if lk in JD_ATTR_TEXT_SKIP_ATTRS:
                parts.append(t)
            else:
                parts.append(f"{attr.strip()}:{t}")
        else:
            parts.append(t)
    return _human_text(" | ".join(parts), 800)


def _jd_seckill_text(d: dict[str, Any]) -> str:
    for k in ("seckillInfo", "secKill", "secKillInfo", "miaosha", "flashSale"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:400]
        if isinstance(v, dict):
            t = _sval_jd(v, ("text", "title", "name", "status"))
            if t:
                return t[:400]
    return ""


def _jd_attributes_line(d: dict[str, Any]) -> str:
    chunks: list[str] = []
    for pl_key in ("propertyList", "properties", "wareProps", "props"):
        pl = d.get(pl_key)
        if not isinstance(pl, list):
            continue
        for el in pl[:25]:
            if isinstance(el, dict):
                k = str(el.get("name") or el.get("key") or "").strip()
                v = str(el.get("value") or el.get("text") or "").strip()
                if k and v:
                    chunks.append(f"{k}:{v}")
            elif isinstance(el, str) and el.strip():
                chunks.append(el.strip()[:120])
        if chunks:
            break
    return " | ".join(chunks)[:4000]


def _jd_attributes_line_full(d: dict[str, Any]) -> str:
    """属性行 + pc 搜索常见 color / catid / shortName。"""
    base = _jd_attributes_line(d)
    extra: list[str] = []
    c = d.get("color")
    if isinstance(c, str) and c.strip():
        extra.append(f"颜色规格:{c.strip()}")
    cid = d.get("catid") or d.get("cid3") or d.get("cid3Name")
    if cid is not None and str(cid).strip():
        extra.append(f"类目:{str(cid).strip()}")
    sn = d.get("shortName")
    if isinstance(sn, str) and sn.strip():
        extra.append(f"简称:{sn.strip()}")
    parts = [p for p in extra if p]
    if base:
        parts.append(base)
    return " | ".join(parts)[:4000]


def _jd_is_p4p_flag(d: dict[str, Any]) -> str:
    for k in ("isAdv", "isAd", "isP4p", "is_p4p", "adFlag"):
        v = d.get(k)
        if v is None:
            continue
        return str(v).strip()[:20]
    if d.get("extension_id") or d.get("extensionId") or d.get("adLog"):
        return "true"
    return ""


def _jd_is_explicit_pc_search_ad_ware(d0: dict[str, Any]) -> bool:
    """
    仅显式广告标记。extensionId/adLog 在自然商品上也常见，不能用来扣减 body.s。
    """
    for k in ("isAdv", "isAd", "isP4p", "is_p4p", "adFlag"):
        v = d0.get(k)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y"):
            return True
    return False


def _pc_search_s_delta_from_natural_slot_count(n_natural: int, slot_total: int) -> int:
    """
    pc_search 两包满屏：下一包 ``body.s = 上一包 s + Δ``，抓包为 **Δ = 自然位条数 - 1**。
    例：首包 ``s=1``、``wareList`` 非广告 22 条 → 次包 ``s=22``（即 ``1+21``）。
    """
    cnt = n_natural if n_natural > 0 else max(0, slot_total)
    return max(1, cnt - 1)


def _pc_search_body_s_step_from_raw_ware_list(raw_list: list[dict[str, Any]]) -> int:
    """
    无 ``wareList`` 时的兜底：树遍历去重 SKU + 去显式广告得自然位数，再套 ``Δ=自然位-1``。
    """
    seen_sku: set[str] = set()
    n = 0
    for obj in raw_list:
        d0 = _jd_flatten_ware(obj)
        if _jd_is_explicit_pc_search_ad_ware(d0):
            continue
        sku = _sval_jd(d0, JD_SKU_KEYS)
        if sku.isdigit() and len(sku) >= 5:
            if sku in seen_sku:
                continue
            seen_sku.add(sku)
        n += 1
    return _pc_search_s_delta_from_natural_slot_count(n, len(raw_list))


def _pc_search_s_step_from_payload_data_ware_list(payload: Any) -> int | None:
    """
    与抓包 ``response1.js`` + 第二包请求（``page=2,s=22``）一致：
    ``data.wareList`` 内非广告条数为 ``N`` 时，**``s`` 增量为 ``max(1, N-1)``**。
    """
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    wl = data.get("wareList")
    if not isinstance(wl, list) or len(wl) == 0:
        return None
    if not all(isinstance(x, dict) for x in wl):
        return None
    n = 0
    for w in wl:
        d0 = _jd_flatten_ware(w)
        if _jd_is_explicit_pc_search_ad_ware(d0):
            continue
        n += 1
    return _pc_search_s_delta_from_natural_slot_count(n, len(wl))


def pc_search_ware_list_slot_count_from_body(text: str) -> int | None:
    """Return len(data.wareList) from pc_search JSON/JSONP body, or None."""
    payload = _loads_json_or_jsonp(text)
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    wl = data.get("wareList")
    return len(wl) if isinstance(wl, list) else None


def _jd_coerce_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip():
        t = v.strip()
        if t.lstrip("-").isdigit():
            try:
                return int(t)
            except ValueError:
                return None
    return None


def _extract_pc_search_next_s_from_payload(
    payload: Any, *, request_page: int, request_s: int
) -> int | None:
    want_p = request_page + 1
    cands: list[int] = []

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            p = _jd_coerce_int(o.get("page"))
            if p is None:
                p = _jd_coerce_int(o.get("pageNo")) or _jd_coerce_int(
                    o.get("pageIndex")
                )
            s_v = _jd_coerce_int(o.get("s"))
            if p == want_p and s_v is not None and s_v > request_s:
                cands.append(s_v)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(payload)
    return min(cands) if cands else None


def _looks_like_jd_ware(d: dict[str, Any]) -> bool:
    d0 = _jd_flatten_ware(d)
    sku = _sval_jd(d0, JD_SKU_KEYS)
    if not (sku.isdigit() and len(sku) >= 5):
        return False
    title = _sval_jd(d0, JD_TITLE_KEYS)
    return len(title) >= 2


def _normalize_jd_api_row(d: dict[str, Any], *, keyword: str, page: int) -> dict[str, str]:
    """
    将 pc_search_searchWare 单条 ware（及同类结构）规整为与淘宝 CSV 对齐的宽表。

    主路径字段：wareId/skuId、wareName、jdPrice/jdPriceText、oriPrice、finalPrice（到手价）、
    sellingPoint、commentFuzzy（评价量）、commentSalesFloor、benefitList/newRegionFloor、
    iconList1–4、promotionSet、wareBuried、shopId/shopName、color/catid、isAdv/extensionId 等；
    仍保留树遍历兜底以兼容字段变更。
    """
    d = _jd_flatten_ware(d)
    sku = _sval_jd(d, JD_SKU_KEYS)
    ware = _sval_jd(d, ("wareId", "wid"))
    item_id = ware if ware else sku

    title = _human_text(_sval_jd(d, JD_TITLE_KEYS), 2000)
    price = _human_text(_sval_jd(d, JD_PRICE_KEYS), 120)
    orig = _human_text(_sval_jd(d, JD_ORIGINAL_PRICE_KEYS), 120)
    coupon_p = _human_text(_sval_jd(d, JD_COUPON_KEYS), 80)
    fp_coupon, _ = _jd_parse_final_price(d)
    if fp_coupon:
        if not coupon_p:
            coupon_p = fp_coupon
        elif fp_coupon not in coupon_p:
            coupon_p = _human_text(f"{coupon_p}/{fp_coupon}", 120)

    ps = d.get("priceShow")
    _, ps_coupon = (
        _jd_format_price_show_dict(ps) if isinstance(ps, dict) else ("", "")
    )
    if not coupon_p and ps_coupon:
        coupon_p = ps_coupon

    selling_point = _jd_selling_point_text(d)
    comment_count = _human_text(
        _sval_jd(
            d,
            ("commentFuzzy", "comment_fuzzy", "cmtFuzzy", "evaluationFuzzy"),
        ),
        120,
    )
    comment_sales_floor = _jd_format_comment_sales_floor(d)
    total_sales = _human_text(
        _sval_jd(
            d,
            ("totalSales", "total_sales", "TotalSales"),
        ),
        400,
    )

    tag_strs = _jd_iter_tag_strings(d)
    arr_ind, _arr_groups = _jd_collect_list_string_fragments(
        d, selling_norm_skip=_jd_selling_point_norm_set(d)
    )
    tag_seen_norm = {t.casefold() for t in tag_strs}
    extra_from_arrays = [
        x for x in arr_ind if _jd_norm_key(x).casefold() not in tag_seen_norm
    ]

    merged_for_signals = _jd_unique_ordered(tag_strs + extra_from_arrays)
    rank_parts = _jd_unique_ordered(
        [t for t in merged_for_signals if _jd_is_rank_text(t)]
    )

    hot_list_rank = _human_text(" | ".join(rank_parts), 600)

    shop = _human_text(
        _sval_jd(d, ("shopName", "venderName", "storeName", "shop_name")), 200
    )
    shop_url = _safe_url(_sval_jd(d, JD_SHOP_URL_KEYS))[:2000]
    if not shop_url:
        sid = d.get("shopId")
        if sid is not None and str(sid).strip():
            shop_url = f"https://mall.jd.com/index-{str(sid).strip()}.html"[:2000]
    shop_info_url = _safe_url(_sval_jd(d, ("shopInfoUrl", "brandUrl")))[:2000]

    loc = _human_text(_sval_jd(d, JD_LOC_KEYS), 200)

    detail_raw = _sval_jd(d, JD_DETAIL_URL_KEYS)
    if detail_raw:
        detail_url = _safe_url(detail_raw)[:2500]
    else:
        detail_url = (
            f"https://item.m.jd.com/product/{sku}.html" if sku else ""
        )

    img_raw = _sval_jd(
        d,
        JD_IMG_KEYS + ("squareImage", "squarePic", "imgDfsUrl"),
    )
    image = _jd_product_image_url(img_raw)[:1200] if img_raw else ""

    seckill = _jd_seckill_text(d)
    attributes = _jd_attributes_line_full(d)
    leaf = str(
        d.get("leafCategory") or d.get("cid3Name") or d.get("catid") or ""
    ).strip()[:80]
    samec = str(d.get("sameStyleCount") or d.get("sameCount") or "").strip()[:40]
    rel = str(d.get("relationScore") or d.get("score") or "").strip()[:40]
    is_p4p = _jd_is_p4p_flag(d)

    out = _jd_empty_export_row()
    out["item_id"] = item_id[:80]
    out["sku_id"] = sku[:80]
    out["title"] = title
    out["title_plain"] = title
    out["price"] = price
    out["coupon_price"] = coupon_p
    out["original_price"] = orig
    out["selling_point"] = selling_point
    out["comment_sales_floor"] = comment_sales_floor
    out["total_sales"] = total_sales
    out["hot_list_rank"] = hot_list_rank
    out["comment_count"] = comment_count
    out["shop_name"] = shop
    out["shop_url"] = shop_url
    out["shop_info_url"] = shop_info_url
    out["location"] = loc
    out["detail_url"] = detail_url
    out["image"] = image
    out["seckill_info"] = _human_text(seckill, 400)
    out["attributes"] = attributes
    out["leaf_category"] = leaf
    out["same_count"] = samec
    out["relation_score"] = rel
    out["is_p4p"] = is_p4p
    out["platform"] = "京东"
    out["keyword"] = keyword
    out["page"] = str(page)
    return out


def _walk_collect_jd_wares(obj: Any, acc: list[dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        if _looks_like_jd_ware(obj):
            acc.append(obj)
        for v in obj.values():
            _walk_collect_jd_wares(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _walk_collect_jd_wares(x, acc)


def _parse_jd_json_payload_rows_and_ware_slots(
    payload: Any, *, keyword: str, page: int
) -> tuple[list[dict[str, str]], int, int]:
    """
    一次遍历：导出商品行、树遍历 ware 数（兜底）、以及 **body.s 用的自然位步长**。
    """
    raw_list: list[dict[str, Any]] = []
    _walk_collect_jd_wares(payload, raw_list)
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for d in raw_list:
        row = _normalize_jd_api_row(d, keyword=keyword, page=page)
        sku = row.get("sku_id", "")
        if not sku or sku in seen:
            continue
        seen.add(sku)
        rows.append(row)
    s_slots = len(raw_list)
    s_scroll = _pc_search_body_s_step_from_raw_ware_list(raw_list)
    return rows, s_slots, s_scroll


def parse_items_from_jd_json_payload(payload: Any, *, keyword: str, page: int) -> list[dict[str, str]]:
    rows, _, _ = _parse_jd_json_payload_rows_and_ware_slots(
        payload, keyword=keyword, page=page
    )
    return rows


def parse_items_and_pc_search_s_step_from_response_body(
    text: str,
    *,
    keyword: str,
    page: int,
    request_api_page: int | None = None,
    request_body_s: int | None = None,
) -> tuple[list[dict[str, str]], int]:
    """
    解析商品列表，并给出本次响应对应的 **body.s 游标增量**。

    JSON：优先顺序：① 响应内嵌下一跳 ``s``；② ``data.wareList`` 得自然位 ``N`` → **Δs=max(1,N-1)**；
    ③ 树遍历兜底，同上 Δ 规则；无 ``wareList`` 时对 ``len(rows)`` 亦用 Δ。
    HTML：增量仍为解析行数（非 pc_search 两包逻辑）。
    """
    payload = _loads_json_or_jsonp(text)
    if payload is not None:
        rows, s_slots, s_scroll = _parse_jd_json_payload_rows_and_ware_slots(
            payload, keyword=keyword, page=page
        )
        step_api: int | None = None
        if request_api_page is not None and request_body_s is not None:
            next_s = _extract_pc_search_next_s_from_payload(
                payload,
                request_page=request_api_page,
                request_s=request_body_s,
            )
            if next_s is not None:
                d = next_s - request_body_s
                if d > 0:
                    step_api = d
        step_wl = _pc_search_s_step_from_payload_data_ware_list(payload)
        if rows:
            if step_api is not None:
                step = step_api
            elif step_wl is not None:
                step = step_wl
            else:
                step = s_scroll if s_scroll > 0 else _pc_search_s_delta_from_natural_slot_count(
                    len(rows), len(rows)
                )
            return rows, max(1, step)
        if s_slots > 0:
            if step_api is not None:
                step = step_api
            elif step_wl is not None:
                step = step_wl
            else:
                step = (
                    s_scroll
                    if s_scroll > 0
                    else _pc_search_s_delta_from_natural_slot_count(0, s_slots)
                )
            return [], max(1, step)
        # 结构与 ware 识别不匹配时，与 parse_items_from_response_body 一样再试 HTML
    rows = parse_items_from_html(text, keyword=keyword, page=page)
    return rows, (len(rows) if rows else 0)


def parse_items_from_response_body(text: str, *, keyword: str, page: int) -> list[dict[str, str]]:
    """先尝试 JSON/JSONP（client.action），失败再按 HTML 解析。"""
    payload = _loads_json_or_jsonp(text)
    if payload is not None:
        rows = parse_items_from_jd_json_payload(payload, keyword=keyword, page=page)
        if rows:
            return rows
    return parse_items_from_html(text, keyword=keyword, page=page)


def _jd_minimal_html_row(
    *,
    keyword: str,
    page: int,
    sku_id: str,
    title: str,
    price: str,
    detail_url: str,
    shop_name: str,
    comment_count: str,
    image: str,
) -> dict[str, str]:
    out = _jd_empty_export_row()
    out["item_id"] = sku_id
    out["sku_id"] = sku_id
    out["title"] = title
    out["title_plain"] = title
    out["price"] = price
    out["detail_url"] = detail_url
    out["shop_name"] = shop_name
    out["comment_count"] = comment_count
    out["image"] = image
    out["platform"] = "京东"
    out["keyword"] = keyword
    out["page"] = str(page)
    return out


def _collect_items_from_json_like(
    html_text: str, keyword: str, page: int
) -> list[dict[str, str]]:
    """
    策略 A：从内嵌 JSON/脚本片段中用正则抓取 wareId/wareName/price 等。
    该策略对结构变更相对鲁棒，但字段名可能变化，因此做多套模板。
    """
    items: list[dict[str, str]] = []

    # 常见字段组合：wareId + wareName + jdPrice / price / priceText
    patterns: list[re.Pattern[str]] = [
        re.compile(
            r'"wareId"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"wareName"\s*:\s*"(?P<title>[^"]{2,300})"'
            r'[\s\S]{0,1200}?'
            r'"(?:jdPrice|price|priceText|mainPrice)"\s*:\s*"?(?P<price>[\d.]{1,12})"?',
            re.I,
        ),
        re.compile(
            r'"skuId"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"title"\s*:\s*"(?P<title>[^"]{2,300})"'
            r'[\s\S]{0,1200}?'
            r'"(?:price|priceText|jdPrice)"\s*:\s*"?(?P<price>[\d.]{1,12})"?',
            re.I,
        ),
        # 兜底：只要 sku + title，价格缺失也接受
        re.compile(
            r'"(?:wareId|skuId)"\s*:\s*"?(?P<sku>\d{5,20})"?'
            r'[\s\S]{0,1200}?'
            r'"(?:wareName|title|name)"\s*:\s*"(?P<title>[^"]{2,300})"',
            re.I,
        ),
    ]

    seen: set[str] = set()
    for pat in patterns:
        for m in pat.finditer(html_text):
            sku = (m.groupdict().get("sku") or "").strip()
            title = _human_text(m.groupdict().get("title") or "", 300)
            price = (m.groupdict().get("price") or "").strip()
            if not sku or not title:
                continue
            if sku in seen:
                continue
            seen.add(sku)
            detail = f"https://item.m.jd.com/product/{sku}.html"
            items.append(
                _jd_minimal_html_row(
                    keyword=keyword,
                    page=page,
                    sku_id=sku,
                    title=title,
                    price=price,
                    detail_url=detail,
                    shop_name="",
                    comment_count="",
                    image="",
                )
            )
        if items:
            # 命中一套 pattern 后就不再叠加下一套，避免重复/误配
            break
    return items


def _collect_items_from_dom(html_text: str, keyword: str, page: int) -> list[dict[str, str]]:
    """
    策略 B：从 DOM/属性中抓取 data-sku + title/price/href。
    不依赖 BeautifulSoup（零依赖），但对结构变化更敏感。
    """
    items: list[dict[str, str]] = []
    seen: set[str] = set()

    # 以 data-sku 为锚点，截取一个窗口做二次抽取
    for m in re.finditer(r'data-sku\s*=\s*"(?P<sku>\d{5,20})"', html_text, re.I):
        sku = (m.group("sku") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)

        win = html_text[m.start() : m.start() + 4500]

        # 链接
        href = ""
        mh = re.search(r'href\s*=\s*"([^"]+)"', win, re.I)
        if mh:
            href = _safe_url(mh.group(1))
        if not href:
            href = f"https://item.m.jd.com/product/{sku}.html"

        # 标题
        title = ""
        for tp in (
            r'title\s*=\s*"([^"]{2,300})"',
            r'alt\s*=\s*"([^"]{2,300})"',
            r'data-name\s*=\s*"([^"]{2,300})"',
        ):
            mt = re.search(tp, win, re.I)
            if mt:
                title = _human_text(mt.group(1), 300)
                break

        # 价格（HTML 上可能是 ¥xx.xx / &yen;xx.xx）
        price = ""
        mp = re.search(r"(?:¥|&yen;)\s*([\d.]{1,12})", win)
        if mp:
            price = mp.group(1).strip()

        # 店铺（H5 列表可能没有）
        shop = ""
        ms = re.search(r'data-shopname\s*=\s*"([^"]{2,80})"', win, re.I)
        if ms:
            shop = _human_text(ms.group(1), 80)

        # 图片
        image = ""
        mi = re.search(r'(?:data-lazy-img|data-img|src)\s*=\s*"([^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', win, re.I)
        if mi:
            image = _jd_product_image_url(mi.group(1))[:1200]

        if not title:
            # 标题拿不到时宁可跳过，避免充斥“空标题”
            continue

        items.append(
            _jd_minimal_html_row(
                keyword=keyword,
                page=page,
                sku_id=sku,
                title=title,
                price=price,
                detail_url=href[:2000],
                shop_name=shop,
                comment_count="",
                image=image,
            )
        )

    return items


def parse_items_from_html(html_text: str, *, keyword: str, page: int) -> list[dict[str, str]]:
    # 优先尝试 JSON-like（更稳），再退回 DOM
    parsed = _collect_items_from_json_like(html_text, keyword, page)
    if not parsed:
        parsed = _collect_items_from_dom(html_text, keyword, page)

    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for row in parsed:
        sku = (row.get("sku_id") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        rows.append(row)
    return rows

