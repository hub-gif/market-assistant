# -*- coding: utf-8 -*-
"""淘宝 PC 商详 SSR HTML：从 ``window.__ICE_APP_CONTEXT__`` 注入段解析 lean 商详（与京东 ware 扁平字典键对齐）。"""
from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, urlparse


def _sku_hint_from_detail_url(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        sid = (q.get("skuId") or [""])[0]
        return str(sid).strip()
    except Exception:
        return ""


def extract_ice_app_loader_b_object(html: str) -> dict[str, Any] | None:
    """
    天猫/淘宝 2025 SSR 商详常在内联脚本中用 ``var b = { ... };`` 与 ``window.__ICE_APP_CONTEXT__`` 合并。
    用 ``json.JSONDecoder.raw_decode`` 从首个 ``{`` 起取整段对象。
    """
    if not (html or "").strip():
        return None
    markers = ("var b = ", "var b=")
    start = -1
    for m in markers:
        i = html.find(m)
        if i >= 0:
            brace = html.find("{", i + len(m))
            if brace >= 0:
                start = brace
                break
    if start < 0:
        alt = re.search(r"window\.__ICE_APP_CONTEXT__\s*=\s*(\{)", html)
        if alt:
            start = alt.start(1)
    if start < 0:
        return None
    try:
        obj, _end = json.JSONDecoder().raw_decode(html, start)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def _dig_res(ice: dict[str, Any]) -> dict[str, Any] | None:
    ld = ice.get("loaderData")
    if not isinstance(ld, dict):
        return None
    home = ld.get("home")
    if not isinstance(home, dict):
        return None
    data = home.get("data")
    if not isinstance(data, dict):
        return None
    res = data.get("res")
    return res if isinstance(res, dict) else None


def _first_price_from_sku2info(sku2info: dict[str, Any]) -> str:
    for _k, v in (sku2info or {}).items():
        if not isinstance(v, dict):
            continue
        pr = v.get("price")
        if isinstance(pr, dict):
            t = str(pr.get("priceText") or "").strip()
            if t:
                return t
    return ""


def tb_detail_lean_flat_from_html(html: str, *, url: str = "") -> tuple[dict[str, str], str]:
    """
    返回 ``(ware_merge_dict, resolved_sku_hint)``。
    字典键与京东 lean 合并用英文内部键一致，便于 ``finalize_merged_row_for_disk``。
    """
    sku_hint = _sku_hint_from_detail_url(url)
    out: dict[str, str] = {
        "detail_brand": "",
        "detail_price_final": "",
        "detail_shop_name": "",
        "detail_category_path": "",
        "detail_product_attributes": "",
        "detail_body_ingredients": "",
        "buyer_ranking_line": "",
        "buyer_promo_text": "",
    }
    ice = extract_ice_app_loader_b_object(html)
    if not ice:
        return out, sku_hint
    res = _dig_res(ice)
    if not res:
        return out, sku_hint

    seller = res.get("seller") if isinstance(res.get("seller"), dict) else {}
    item = res.get("item") if isinstance(res.get("item"), dict) else {}
    sku_core = res.get("skuCore") if isinstance(res.get("skuCore"), dict) else {}
    comps = res.get("componentsVO") if isinstance(res.get("componentsVO"), dict) else {}

    out["detail_shop_name"] = str(seller.get("shopName") or seller.get("sellerNick") or "").strip()

    vague = str(item.get("vagueSellCount") or "").strip()
    if vague:
        out["buyer_ranking_line"] = f"销量 {vague}"

    sku2info = sku_core.get("sku2info")
    price_text = ""
    if isinstance(sku2info, dict):
        if sku_hint and str(sku_hint) in sku2info:
            pv = sku2info[str(sku_hint)]
            if isinstance(pv, dict):
                pr = pv.get("price")
                if isinstance(pr, dict):
                    price_text = str(pr.get("priceText") or "").strip()
        if not price_text:
            price_text = _first_price_from_sku2info(sku2info)
    pv2 = comps.get("priceVO") if isinstance(comps.get("priceVO"), dict) else {}
    pr2 = pv2.get("price") if isinstance(pv2.get("price"), dict) else {}
    if not price_text:
        price_text = str(pr2.get("priceText") or "").strip()
    sym = str(pr2.get("priceUnit") or "￥").strip()
    if price_text:
        out["detail_price_final"] = f"{sym}{price_text}" if sym and sym not in price_text else price_text

    ext = comps.get("extensionInfoVO") if isinstance(comps.get("extensionInfoVO"), dict) else {}
    infos = ext.get("infos") if isinstance(ext.get("infos"), list) else []
    param_lines: list[str] = []
    ing_parts: list[str] = []
    brand_guess = ""
    guarantee_texts: list[str] = []

    for blk in infos:
        if not isinstance(blk, dict):
            continue
        btype = str(blk.get("type") or "")
        items = blk.get("items") if isinstance(blk.get("items"), list) else []
        if btype == "BASE_PROPS":
            for it in items:
                if not isinstance(it, dict):
                    continue
                ttl = str(it.get("title") or "").strip()
                texts = it.get("text") if isinstance(it.get("text"), list) else []
                tx = " ".join(str(x).strip() for x in texts if str(x).strip())
                if ttl and tx:
                    param_lines.append(f"{ttl}:{tx}")
                if ttl == "品牌":
                    brand_guess = tx
                if ttl and ("配料" in ttl or "成分" in ttl):
                    ing_parts.append(f"{ttl}:{tx}")
        if btype in ("GUARANTEE", "GUARANTEE_NEW"):
            for it in items:
                if not isinstance(it, dict):
                    continue
                tit = str(it.get("title") or "").strip()
                txlist = it.get("text") if isinstance(it.get("text"), list) else []
                body = " ".join(str(x).strip() for x in txlist if str(x).strip())
                if tit and body:
                    guarantee_texts.append(f"{tit}:{body[:240]}")

    out["detail_brand"] = brand_guess
    tvo = comps.get("titleVO") if isinstance(comps.get("titleVO"), dict) else {}
    sts = tvo.get("subTitles") if isinstance(tvo.get("subTitles"), list) else []
    if sts and isinstance(sts[0], dict):
        subt = str(sts[0].get("title") or "").strip()
        if subt:
            out["buyer_promo_text"] = subt[:2000]

    endorse = res.get("itemEndorseVO") if isinstance(res.get("itemEndorseVO"), dict) else {}
    elis = endorse.get("endorseList") if isinstance(endorse.get("endorseList"), list) else []
    extra_lines: list[str] = []
    for e in elis:
        if not isinstance(e, dict):
            continue
        tls = e.get("textList") if isinstance(e.get("textList"), list) else []
        extra_lines.extend(str(x).strip() for x in tls if str(x).strip())
    if extra_lines:
        prom = out.get("buyer_promo_text") or ""
        tail = "; ".join(extra_lines[:12])
        out["buyer_promo_text"] = (prom + "; " + tail).strip("; ")[:2000]

    if guarantee_texts:
        promo = out.get("buyer_promo_text") or ""
        out["buyer_promo_text"] = (promo + " |保障:" + "; ".join(guarantee_texts[:6]))[-2000:]

    leaf = str(item.get("leafCategory") or res.get("rootCatId") or "").strip()
    if not leaf:
        plus = res.get("plusViewVO") if isinstance(res.get("plusViewVO"), dict) else {}
        ipv = plus.get("industryParamVO") if isinstance(plus.get("industryParamVO"), dict) else {}
        ep = ipv.get("enhanceParamList") if isinstance(ipv.get("enhanceParamList"), list) else []
        cats: list[str] = []
        for p in ep:
            if isinstance(p, dict):
                vn = str(p.get("propertyName") or "").strip()
                if vn in ("类目", "分类"):
                    cats.append(str(p.get("valueName") or "").strip())
        if cats:
            leaf = ";".join(cats)
    out["detail_category_path"] = leaf
    out["detail_product_attributes"] = "\n".join(param_lines[:80])
    out["detail_body_ingredients"] = "\n".join(ing_parts[:40]) if ing_parts else ""

    resolved = sku_hint
    if not resolved:
        sb = sku_core.get("skuBase") if isinstance(sku_core.get("skuBase"), dict) else {}
        sklist = sb.get("skus") if isinstance(sb.get("skus"), list) else []
        if sklist and isinstance(sklist[0], dict):
            resolved = str(sklist[0].get("skuId") or "").strip()
    if not resolved:
        resolved = str(item.get("itemId") or "").strip()

    return out, (resolved or sku_hint)


def tb_item_id_from_detail_url(url: str) -> str:
    """商详链接 ``id`` 参数（主商品数字 ID）。"""
    try:
        q = parse_qs(urlparse(url).query)
        return str((q.get("id") or [""])[0]).strip()
    except Exception:
        return ""
