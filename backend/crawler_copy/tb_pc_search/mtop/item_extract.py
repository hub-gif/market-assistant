# -*- coding: utf-8 -*-
"""
mtop 推荐接口响应中商品节点的启发式解析与规整（CSV 列、去重）。

与具体 HTTP 客户端解耦，供 CLI 或其它任务复用。
"""
from __future__ import annotations

import html as html_module
import json
import re
from typing import Any
from urllib.parse import unquote

ITEM_TITLE_KEYS = (
    "title",
    "raw_title",
    "auctionTitle",
    "tTitle",
    "mainTitle",
    "subject",
    "name",
    "short_title",
    "abstract",
    "subTitle",
    "subtitle",
    "recommendReason",
)
ITEM_PRICE_KEYS = (
    "view_price",
    "price",
    "priceWap",
    "reservePrice",
    "promPrice",
    "afterPrice",
    "formattedPrice",
    "price_num",
)
ITEM_ID_KEYS = (
    "item_id",
    "itemId",
    "auctionId",
    "item_id_str",
    "nid",
    "openId",
)
ITEM_URL_KEYS = (
    "auctionURL",
    "item_url",
    "detailUrl",
    "itemUrl",
    "href",
    "url",
    "link",
    "wapAuctionURL",
)
ITEM_SHOP_KEYS = ("nick", "shopTitle", "shop_name", "sellerNick", "seller", "shopName")
ITEM_PIC_KEYS = ("pic_path", "picUrl", "pic", "img", "image", "pictUrl", "imageUrl")
ITEM_SALES_KEYS = (
    "view_sales",
    "sales",
    "realSales",
    "realSalesRaw",
    "sold",
    "sold_count",
    "saleNum",
    "saleCount",
    "volume",
    "payNum",
    "pay_num",
    "uvsum",
    "sellFuzzy",
    "sell_fuzzy",
    "fuzzySoldCount",
    "labelIntense",
)
ITEM_COMMENT_KEYS = (
    "comment_count",
    "commentCount",
    "reviewCount",
    "rateCount",
    "cfav",
    "favcount",
)
ITEM_SHOP_URL_KEYS = ("shopURL", "shop_url", "shopUrl", "sellerUrl", "shopLink", "storeURL")
ITEM_VIDEO_KEYS = (
    "videoUrl",
    "video_url",
    "videoURL",
    "playUrl",
    "picV",
    "videoIcon",
    "vmIconUrl",
)
ITEM_LOC_KEYS = ("procity", "area", "location", "loc")


CANONICAL_FIELDS = (
    "item_id",
    "sku_id",
    "uniqpid",
    # "extra_params",
    "title",
    # "title_plain",
    "price",
    "coupon_price",
    "price_show",
    "original_price",
    "sales",
    "hot_list_rank",
    # "hot_list_info",
    "shop_tag",
    # "label_order",
    # "icons_line",
    "comment_count",
    "shop_name",
    "shop_info_title",
    "shop_url",
    "shop_info_url",
    "shop_logo",
    "seller_uid",
    "location",
    "detail_url",
    "image",
    "video_url",
    "video_cover",
    "video_dimension",
    "seckill_info",
    # "second_kill_icon_url",
    # "ump_price_log",
    "attributes",
    "features",
    "promotion_tags",
    "leaf_category",
    "same_count",
    "relation_score",
    "is_p4p",
)


def _sval(d: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            continue
        t = str(v).strip()
        if t:
            return t
    return ""


def _human_text(s: str, max_len: int = 4000) -> str:
    """去掉 HTML 标签与实体，压缩空白，供 CSV 给人阅读。"""
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = html_module.unescape(t)
    t = " ".join(t.split()).strip()
    return t[:max_len] if max_len > 0 else t


def _title_plain(title: str) -> str:
    return _human_text(title, 2000)


def _scalar_strings_from_obj(
    obj: Any,
    *,
    max_depth: int,
    out: list[str],
    _depth: int = 0,
) -> None:
    if _depth > max_depth or len(out) > 80:
        return
    if isinstance(obj, str):
        s = " ".join(obj.split()).strip()
        if 2 <= len(s) <= 160 and not s.startswith("http"):
            out.append(s)
    elif isinstance(obj, dict):
        for v in obj.values():
            _scalar_strings_from_obj(v, max_depth=max_depth, out=out, _depth=_depth + 1)
    elif isinstance(obj, list):
        for x in obj[:40]:
            _scalar_strings_from_obj(x, max_depth=max_depth, out=out, _depth=_depth + 1)


def _is_tracking_noise_text(s: str) -> bool:
    """埋点/算法残留片段，不展示在销量汇总里。"""
    if not s:
        return True
    sl = s.lower()
    if "_coefp" in s or "lf_aclog" in sl or "tpp_buckets" in sl or "aplus_abtest" in sl:
        return True
    if re.search(r"\d+-\d{8,}-\d+-\d", s):
        return True
    if re.fullmatch(r"[\d.|]+", s.strip()) and len(s) <= 12:
        return True
    return False


def _extract_hot_list_rank_only(d: dict[str, Any]) -> str:
    hi = d.get("hotListInfo")
    if not isinstance(hi, dict):
        return ""
    for hk in ("rank_short_text", "text", "short_text", "title", "rankText"):
        hv = hi.get(hk)
        if isinstance(hv, str) and hv.strip():
            return _human_text(hv, 400)
    return ""


def _extract_hot_list_info_full(d: dict[str, Any]) -> str:
    """hotListInfo 全部键值（值去 HTML），便于 CSV 保留完整榜单信息。"""
    hi = d.get("hotListInfo")
    if not isinstance(hi, dict):
        return ""
    pairs: list[str] = []
    for hk in sorted(hi.keys(), key=str):
        hv = hi.get(hk)
        if hv is None:
            continue
        if isinstance(hv, (dict, list)):
            try:
                vs = json.dumps(hv, ensure_ascii=False, separators=(",", ":"))[:500]
            except (TypeError, ValueError):
                vs = str(hv)[:500]
        else:
            vs = _human_text(str(hv), 500)
        if vs:
            pairs.append(f"{hk}={vs}")
    return " | ".join(pairs)[:2000]


def _extract_sales_text(d: dict[str, Any]) -> str:
    direct = _sval(d, ITEM_SALES_KEYS)
    parts: list[str] = []
    if direct:
        parts.append(_human_text(direct, 400))

    for key in (
        "hotListInfo",
        "sellText",
        "salesDesc",
        "sales_text",
        "umpSellPoint",
        "itemPoints",
    ):
        v = d.get(key)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            parts.append(_human_text(v, 200))
        elif isinstance(v, dict):
            if key == "hotListInfo":
                for hk, hv in v.items():
                    if isinstance(hv, str) and hv.strip() and hk != "rank_short_text":
                        parts.append(_human_text(hv, 200))
                continue
            found: list[str] = []
            _scalar_strings_from_obj(v, max_depth=4, out=found)
            for s in found:
                if any(
                    kw in s
                    for kw in (
                        "已售",
                        "人付款",
                        "人购买",
                        "件",
                        "月销",
                        "年销",
                        "销量",
                        "笔",
                        "收下",
                        "热度",
                        "热销",
                        "榜单",
                    )
                ):
                    parts.append(s)
                elif re.search(r"\d", s) and len(s) <= 40:
                    parts.append(s)
        elif isinstance(v, list):
            for el in v[:25]:
                if isinstance(el, dict):
                    t = _sval(el, ("text", "title", "name", "desc", "content", "message"))
                    if t:
                        parts.append(_human_text(t, 200))
                elif isinstance(el, str) and el.strip():
                    parts.append(el.strip()[:200])

    seen: set[str] = set()
    uniq: list[str] = []
    for p in parts:
        p = p.strip()
        if not p or p in seen or len(p) > 300 or _is_tracking_noise_text(p):
            continue
        seen.add(p)
        uniq.append(p)
    return " | ".join(uniq)[:1200]


def _extract_comment_text(d: dict[str, Any]) -> str:
    c = _sval(d, ITEM_COMMENT_KEYS)
    if c:
        return c[:120]
    for key in ("hotListInfo",):
        v = d.get(key)
        if isinstance(v, dict):
            for subk, subv in v.items():
                if "comment" in subk.lower() or "review" in subk.lower():
                    if isinstance(subv, str) and subv.strip():
                        return subv.strip()[:120]
    return ""


PROMO_KEYS = (
    "couponActivityId",
    "couponTag",
    "priceTag",
    "zkFinalPrice",
    "reservePrice",
)


def _format_price_show_dict(ps: dict[str, Any]) -> tuple[str, str]:
    """返回 (展示文案如「券后价¥0.62」, 券后数字串)。"""
    unit = str(ps.get("unit") or "¥").strip()
    price = str(ps.get("price") or "").strip()
    desc = str(ps.get("priceDesc") or "").strip()
    pre = str(ps.get("preText") or "").strip()
    line = f"{pre}{desc}{unit}{price}".strip()
    return line, price


def _extract_second_kill(d: dict[str, Any]) -> tuple[str, str]:
    sk = d.get("secondKillInfo")
    if not isinstance(sk, dict):
        return "", ""
    t1 = _human_text(str(sk.get("text1") or ""), 200)
    t3 = _human_text(str(sk.get("text3") or ""), 200)
    icon = str(sk.get("iconUrl") or "").strip()
    parts = [x for x in (t1, t3) if x]
    summary = " ".join(parts)[:300]
    return summary, icon[:2000]


def _extract_extra_sku_id(d: dict[str, Any]) -> str:
    ep = d.get("extraParams")
    if not isinstance(ep, list):
        return ""
    for el in ep:
        if not isinstance(el, dict):
            continue
        if str(el.get("key") or "") == "skuId":
            return str(el.get("value") or "").strip()
    return ""


def _extract_extra_params_kv(d: dict[str, Any]) -> str:
    """extraParams 全部键值，value 做 URL 解码便于阅读。"""
    ep = d.get("extraParams")
    if not isinstance(ep, list):
        return ""
    pairs: list[str] = []
    for el in ep:
        if not isinstance(el, dict):
            continue
        k = str(el.get("key") or "").strip()
        if not k:
            continue
        raw_v = el.get("value")
        vs = "" if raw_v is None else unquote(str(raw_v).strip())
        vs = " ".join(vs.split())[:300]
        pairs.append(f"{k}={vs}")
    return " | ".join(pairs)[:2500]


def _extract_structured_usp(d: dict[str, Any]) -> str:
    su = d.get("structuredUSPInfo")
    if not isinstance(su, list):
        return ""
    parts: list[str] = []
    for el in su:
        if not isinstance(el, dict):
            continue
        pn = _human_text(str(el.get("propertyName") or ""), 200)
        pv = _human_text(str(el.get("propertyValueName") or ""), 400)
        if pn and pv:
            parts.append(f"{pn}:{pv}")
    return " | ".join(parts)[:4000]


def _extract_ump_price_log(d: dict[str, Any]) -> str:
    u = d.get("umpPriceLog")
    if not isinstance(u, dict):
        return ""
    priority = (
        "traceId",
        "s_id",
        "price_from",
        "item_price",
        "b_s",
        "price_stage",
        "x_object_id",
        "ump_invoke",
        "umpLog",
    )
    pairs: list[str] = []
    seen: set[str] = set()
    for k in priority:
        if k not in u:
            continue
        raw = u[k]
        vs = _human_text(str(raw), 1200) if isinstance(raw, str) else str(raw)
        pairs.append(f"{k}={vs}")
        seen.add(k)
    for k in sorted(u.keys(), key=str):
        if k in seen:
            continue
        raw = u[k]
        if isinstance(raw, (dict, list)):
            try:
                vs = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))[:400]
            except (TypeError, ValueError):
                vs = str(raw)[:400]
        else:
            vs = _human_text(str(raw), 400)
        pairs.append(f"{k}={vs}")
    return " | ".join(pairs)[:3500]


def _extract_icons_line(d: dict[str, Any]) -> str:
    icons = d.get("icons")
    parts: list[str] = []
    if isinstance(icons, list):
        for it in icons[:50]:
            if not isinstance(it, dict):
                continue
            alias = str(it.get("alias") or "").strip()
            text = _human_text(str(it.get("text") or ""), 160)
            dom = str(it.get("domClass") or "").strip()
            if text and alias:
                parts.append(f"{alias}({text})")
            elif text:
                parts.append(text)
            elif alias:
                parts.append(alias)
            elif dom:
                parts.append(dom)
    if parts:
        return " | ".join(parts)[:2500]
    il = d.get("iconList")
    if isinstance(il, str) and il.strip():
        return _human_text(il, 800)
    return ""


def _label_order_str(d: dict[str, Any]) -> str:
    lo = d.get("labelOrder")
    if isinstance(lo, list):
        return ",".join(str(x) for x in lo if x is not None)[:600]
    return ""


def _extract_video_block(d: dict[str, Any]) -> tuple[str, str, str]:
    v = d.get("video")
    if not isinstance(v, dict):
        return "", "", ""
    vurl = _normalize_detail_url(str(v.get("videoUrl") or "").strip())
    if vurl.startswith("http://"):
        vurl = "https://" + vurl[7:]
    cover = _normalize_detail_url(str(v.get("coverUrl") or "").strip())
    if cover.startswith("http://"):
        cover = "https://" + cover[7:]
    dim = str(v.get("videoDimension") or "").strip()
    return vurl[:2000], cover[:2000], dim[:40]


def _extract_shop_info_block(d: dict[str, Any]) -> tuple[str, str, str]:
    si = d.get("shopInfo")
    if not isinstance(si, dict):
        return "", "", ""
    name = str(si.get("title") or "").strip()
    surl = _normalize_detail_url(str(si.get("url") or "").strip())
    logo = _normalize_detail_url(str(si.get("shopLogo") or "").strip())
    if logo.startswith("http://"):
        logo = "https://" + logo[7:]
    return name[:200], surl[:2000], logo[:2000]


def _extract_promotion_line(d: dict[str, Any]) -> str:
    """给人看的促销摘要：不含 utLogMap / umpPriceLog / 内部 labelOrder 等。"""
    chunks: list[str] = []
    ps = d.get("priceShow")
    if isinstance(ps, dict):
        line, _ = _format_price_show_dict(ps)
        if line:
            chunks.append(line)
    sk_txt, _ = _extract_second_kill(d)
    if sk_txt:
        chunks.append(sk_txt)
    for k in PROMO_KEYS:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            chunks.append(f"{k}={v.strip()[:80]}")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            chunks.append(f"{k}={v}")
    v2 = d.get("promotion") or d.get("promotions")
    if isinstance(v2, list):
        for el in v2[:15]:
            if isinstance(el, dict):
                t = _sval(el, ("text", "title", "name", "desc"))
                if t:
                    chunks.append(t[:120])
            elif isinstance(el, str):
                chunks.append(el[:120])
    return " | ".join(chunks)[:1200]


def _flatten_item_merge(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for nest_key in ("item", "auction", "itemData", "main"):
        inner = out.get(nest_key)
        if isinstance(inner, dict):
            del out[nest_key]
            for k, v in inner.items():
                if k not in out or out[k] in (None, "", [], {}):
                    out[k] = v
    ex = out.get("exContent")
    if isinstance(ex, dict):
        for k, v in ex.items():
            if k not in out or out[k] in (None, "", [], {}):
                out[k] = v
    return out


def _looks_like_item(d: dict[str, Any]) -> bool:
    if len(d) < 2:
        return False
    d = _flatten_item_merge(d)
    iid = _sval(d, ITEM_ID_KEYS)
    if iid.isdigit() and len(iid) >= 8:
        return True
    url = _sval(d, ITEM_URL_KEYS)
    if url and ("item.htm" in url or "detail.tmall" in url or "item.taobao.com" in url):
        return True
    title = _sval(d, ITEM_TITLE_KEYS)
    price = _sval(d, ITEM_PRICE_KEYS)
    if len(title) >= 6 and (price or url) and _sval(d, ITEM_PIC_KEYS):
        return True
    return False


def _walk_collect_items(obj: Any, acc: list[dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        if _looks_like_item(obj):
            acc.append(_flatten_item_merge(obj))
        for v in obj.values():
            _walk_collect_items(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _walk_collect_items(x, acc)


def _normalize_detail_url(u: str) -> str:
    u = u.strip()
    if not u:
        return ""
    u = unquote(u)
    if u.startswith("//"):
        u = "https:" + u
    elif u.startswith("http://"):
        u = "https://" + u[7:]
    return u


def _extract_features(d: dict[str, Any]) -> str:
    parts: list[str] = []

    def add_text(x: Any) -> None:
        if x is None:
            return
        if isinstance(x, str):
            t = _human_text(x, 200)
            if t:
                parts.append(t)
        elif isinstance(x, dict):
            for k in ("text", "title", "name", "desc", "brief", "alias"):
                v = x.get(k)
                if isinstance(v, str) and v.strip():
                    parts.append(_human_text(v, 160))
                    break

    for key in ("icons", "iconList", "serviceIcons", "titles", "tags", "tagList"):
        v = d.get(key)
        if isinstance(v, list):
            for it in v[:20]:
                add_text(it)
        elif isinstance(v, dict):
            add_text(v)

    for key in ("usp", "uspInfo", "itemTags", "icons_text"):
        v = d.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(_human_text(v, 200))

    seen: set[str] = set()
    uniq: list[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return " | ".join(uniq)[:800]


def _item_id_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"[?&](?:id|itemId)=(\d+)", url, re.I)
    if m:
        return m.group(1)
    m = re.search(r"/item/(\d+)\.htm", url, re.I)
    if m:
        return m.group(1)
    return ""


def _is_valid_product_row(row: dict[str, str]) -> bool:
    """过滤店铺卡片、纯活动链等伪商品行。"""
    iid = (row.get("item_id") or "").strip()
    if iid.isdigit() and len(iid) >= 8:
        return True
    url = (row.get("detail_url") or "").strip()
    if not url:
        return False
    low = url.lower()
    if "shop/view_shop" in low or "store.taobao.com/shop" in low:
        return False
    if "pages.tmall.com" in low and "item.htm" not in low:
        return False
    ext = _item_id_from_url(url)
    return bool(ext and len(ext) >= 8)


def normalize_product_record(raw: dict[str, Any]) -> dict[str, str]:
    d = _flatten_item_merge(raw)
    title = _sval(d, ITEM_TITLE_KEYS)
    price = _sval(d, ITEM_PRICE_KEYS)
    orig = _sval(
        d,
        ("originPrice", "originalPrice", "zkFinalPrice", "priceTag", "reservePrice"),
    )
    iid = _sval(d, ITEM_ID_KEYS)
    sku_id = _extract_extra_sku_id(d)
    extra_kv = _extract_extra_params_kv(d)
    uniqpid = str(d.get("uniqpid") or "").strip()[:80]
    rel_sc = d.get("relationScore")
    relation_score = f"{rel_sc}".strip() if rel_sc is not None else ""
    is_p4p = str(d.get("isP4p") or "").strip()[:20]
    url = _normalize_detail_url(_sval(d, ITEM_URL_KEYS))
    if not iid and url:
        iid = _item_id_from_url(url)
    pic = _sval(d, ITEM_PIC_KEYS)
    if pic and pic.startswith("//"):
        pic = "https:" + pic
    shop = _sval(d, ITEM_SHOP_KEYS)
    shop_url = _normalize_detail_url(_sval(d, ITEM_SHOP_URL_KEYS))
    si_name, si_url, si_logo = _extract_shop_info_block(d)
    shop_info_title = si_name[:200] if si_name else ""
    shop_info_url = si_url[:2000] if si_url else ""
    if not shop and si_name:
        shop = si_name
    if si_url:
        shop_url = shop_url or si_url
    shop_logo = si_logo

    ps = d.get("priceShow")
    price_show_line, coupon_p = _format_price_show_dict(ps) if isinstance(ps, dict) else ("", "")

    sales = _extract_sales_text(d) or _sval(d, ITEM_SALES_KEYS)
    sales = _human_text(sales, 1200) if sales else ""
    hot_rank = _extract_hot_list_rank_only(d)
    hot_list_full = _extract_hot_list_info_full(d)
    shop_tag = _human_text(str(d.get("shopTag") or ""), 300)
    label_order = _label_order_str(d)
    icons_line = _extract_icons_line(d)
    seller_uid = str(d.get("userId") or "").strip()[:300]
    comment = _extract_comment_text(d)

    vurl, vcover, vdim = _extract_video_block(d)
    if not vurl:
        video_flat = _sval(d, ITEM_VIDEO_KEYS)
        if video_flat:
            vurl = video_flat if video_flat.startswith("http") else _normalize_detail_url(video_flat)
    if vurl.startswith("//"):
        vurl = "https:" + vurl[2:]
    if vurl.startswith("http://"):
        vurl = "https://" + vurl[7:]

    seckill_txt, seckill_icon = _extract_second_kill(d)
    seckill_col = (seckill_txt or "")[:400]
    ump_log = _extract_ump_price_log(d)

    loc = _sval(d, ITEM_LOC_KEYS)
    attributes = _extract_structured_usp(d)
    feats = _extract_features(d)
    promo = _extract_promotion_line(d)
    leaf = str(d.get("leafCategory") or "").strip()[:80]
    samec = str(d.get("sameCount") or "").strip()[:40]

    title_ht = _human_text(title, 2000)

    out = {k: "" for k in CANONICAL_FIELDS}
    out["item_id"] = iid
    out["sku_id"] = sku_id[:80]
    out["uniqpid"] = uniqpid
    # out["extra_params"] = extra_kv
    out["title"] = title_ht
    # out["title_plain"] = title_ht
    out["price"] = _human_text(price, 120) if price else ""
    out["coupon_price"] = _human_text(coupon_p, 80) if coupon_p else ""
    out["price_show"] = _human_text(price_show_line, 200) if price_show_line else ""
    out["original_price"] = _human_text(orig, 120) if orig else ""
    out["detail_url"] = url[:2500] if url else ""
    out["image"] = pic[:1200] if pic else ""
    out["shop_name"] = _human_text(shop, 200) if shop else ""
    # out["shop_info_title"] = _human_text(shop_info_title, 200) if shop_info_title else ""
    out["shop_url"] = shop_url[:2000] if shop_url else ""
    out["shop_info_url"] = shop_info_url
    out["shop_logo"] = shop_logo[:2000] if shop_logo else ""
    out["sales"] = sales
    out["hot_list_rank"] = hot_rank
    # out["hot_list_info"] = hot_list_full
    out["shop_tag"] = shop_tag
    # out["label_order"] = label_order
    # out["icons_line"] = icons_line
    out["comment_count"] = _human_text(comment, 200) if comment else ""
    out["seller_uid"] = seller_uid
    out["location"] = _human_text(loc, 200) if loc else ""
    out["video_url"] = vurl[:2000] if vurl else ""
    out["video_cover"] = vcover[:2000] if vcover else ""
    out["video_dimension"] = vdim
    out["seckill_info"] = seckill_col
    # out["second_kill_icon_url"] = seckill_icon if seckill_icon else ""
    # out["ump_price_log"] = ump_log
    out["attributes"] = attributes
    out["features"] = _human_text(feats, 1200) if feats else ""
    out["promotion_tags"] = _human_text(promo, 2000) if promo else ""
    out["leaf_category"] = leaf
    out["same_count"] = samec
    out["relation_score"] = relation_score[:40]
    out["is_p4p"] = is_p4p
    return out


def row_dedup_key(row: dict[str, str]) -> str:
    d = row["item_id"] or row["detail_url"] or row["title"][:80]
    if row.get("sku_id"):
        d = f"{d}#sku{row['sku_id']}"
    return d


def parse_items_from_mtop_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    """从 mtop JSON 整棵树中启发式抽取商品（去重 item_id / 链接）。"""
    raw_list: list[dict[str, Any]] = []
    _walk_collect_items(payload, raw_list)

    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for raw in raw_list:
        row = normalize_product_record(raw)
        if not _is_valid_product_row(row):
            continue
        dedup = row_dedup_key(row)
        if not dedup or dedup in seen:
            continue
        seen.add(dedup)
        rows.append(row)
    return rows
