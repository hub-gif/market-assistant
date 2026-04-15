# -*- coding: utf-8 -*-
"""
商详 JSON → **购买者可理解的优惠与权益摘要**（用于促销策略分析，而非字段堆砌）。

设计原则：
- **先回答「我买这件能怎样」**：到手价相对标价、是否有券/补贴提示、硬约束（如不可用东券）。
- **再列可感知的物流/售后权益**：价保、退换、送达等，用短标签 + 一句说明。
- **原始杂乱节点**（如 abData、埋点）不进入摘要。
- **优惠拆解**（若存在 ``preferenceVO.preferencePopUp.expression``）：购买立减、红包抵扣金额、券/促销/国补占位等，与腰带价、到手价**对照阅读**。

输入为 ``pc_detailpage_wareBusiness`` 类接口的 **JSON 根对象**（与 ``flatten_ware_business`` 同源）；
若你保存的是完整响应，根级字段与之一致即可。
"""
from __future__ import annotations

import json
import re
from typing import Any

from pipeline.csv_schema import strip_buyer_ranking_line_prefix


def _s(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _strip_html(text: str, *, max_len: int = 800) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    t = " ".join(t.split()).strip()
    return t[:max_len] if max_len > 0 else t


def _parse_float_maybe(s: str) -> float | None:
    t = (s or "").strip().replace(",", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _price_from_gather_vo(obj: dict[str, Any]) -> dict[str, Any]:
    """warePriceGatherVO.priceItemList → 到手价 / 京东价等。"""
    out: dict[str, Any] = {
        "hand_price": "",
        "hand_label": "",
        "jd_price": "",
        "jd_price_hit_line": False,
        "raw_items": [],
    }
    wpg = obj.get("warePriceGatherVO")
    if not isinstance(wpg, dict):
        return out
    pil = wpg.get("priceItemList")
    if not isinstance(pil, list):
        return out
    for it in pil:
        if not isinstance(it, dict):
            continue
        ptype = _s(it.get("priceType"))
        price = _s(it.get("price"))
        hit_line = bool(it.get("hitLine"))
        labels: list[str] = []
        for lb in it.get("priceLabelList") or []:
            if isinstance(lb, dict) and _s(lb.get("labelTxt")):
                labels.append(_s(lb.get("labelTxt")))
        out["raw_items"].append(
            {"priceType": ptype, "price": price, "hitLine": hit_line, "labels": labels}
        )
        if ptype == "finalPrice" and price:
            out["hand_price"] = price
            out["hand_label"] = labels[0] if labels else "到手价"
        if ptype == "jdPrice" and price:
            out["jd_price"] = price
            out["jd_price_hit_line"] = hit_line
    return out


def _price_from_classic_price_block(obj: dict[str, Any]) -> dict[str, Any]:
    """兼容仅有 price.finalPrice / price.p 的旧结构。"""
    price = obj.get("price")
    if not isinstance(price, dict):
        return {}
    fp = price.get("finalPrice")
    fp = fp if isinstance(fp, dict) else {}
    hand = _s(fp.get("price")) or _s(price.get("p"))
    jd = _s(price.get("op")) or _s(price.get("p"))
    return {
        "hand_price": hand,
        "hand_label": "到手价",
        "jd_price": jd if jd != hand else "",
        "jd_price_hit_line": bool(_s(price.get("op"))),
    }


def _preference_bundle(obj: dict[str, Any]) -> dict[str, Any]:
    """
    详情页「优惠弹层」同源：立减、红包、券、促销、国补占位；以及包邮/返豆等短标签。
    对应前端 preferenceVO（与 warePriceGatherVO 互补）。
    """
    out: dict[str, Any] = {
        "expression": {},
        "subtrahends": [],
        "shared_labels": [],
        "popup_preferences": [],
    }
    pv = obj.get("preferenceVO")
    if not isinstance(pv, dict):
        return out
    for lb in pv.get("againSharedLabel") or []:
        if isinstance(lb, dict):
            name = _s(lb.get("labelName"))
            if name:
                out["shared_labels"].append(name[:120])
    ppop = pv.get("preferencePopUp")
    if not isinstance(ppop, dict):
        return out
    for it in ppop.get("againSharedPreference") or []:
        if not isinstance(it, dict):
            continue
        line = _s(it.get("text"))
        val = _s(it.get("value"))
        st = _s(it.get("shortText"))
        if st and val:
            out["popup_preferences"].append(f"{st}：{val}"[:300])
        elif line and val:
            out["popup_preferences"].append(f"{line}：{val}"[:300])
        elif val:
            out["popup_preferences"].append(val[:300])
    ex = ppop.get("expression")
    if not isinstance(ex, dict):
        return out
    out["expression"] = {
        "base_price": _s(ex.get("basePrice"))[:32],
        "discount_desc": _s(ex.get("discountDesc"))[:32],
        "discount_amount": _s(ex.get("discountAmount"))[:32],
        "red_amount": _s(ex.get("redAmount"))[:32],
        "coupon_amount": _s(ex.get("couponAmount"))[:32],
        "promotion_amount": _s(ex.get("promotionAmount"))[:32],
        "gov_amount": _s(ex.get("govAmount"))[:32],
    }
    for sub in ex.get("subtrahends") or []:
        if not isinstance(sub, dict):
            continue
        out["subtrahends"].append(
            {
                "category": _s(sub.get("topDesc"))[:32],
                "description": _s(sub.get("preferenceDesc"))[:200],
                "amount": _s(sub.get("preferenceAmount"))[:32],
                "preference_type": _s(sub.get("preferenceType"))[:16],
            }
        )
    return out


def _gov_support_surface(obj: dict[str, Any]) -> dict[str, Any]:
    """政府补贴/国补腰带等（页面开关与展示，非到手计算结果）。"""
    g = obj.get("govSupportInfo")
    if not isinstance(g, dict):
        return {}
    return {
        "gov_subsidy_flag": bool(g.get("govSubsidy")),
        "gov_support_flag": bool(g.get("govSupport")),
        "subsidy_type": _s(g.get("subsidyType"))[:64],
        "subsidy_scene": _s(g.get("subsidyScene"))[:32],
        "right_text": _s(g.get("rightText"))[:200],
        "belt_banner_url": _s(g.get("beltBanner"))[:300],
    }


def _best_promotion_summary(obj: dict[str, Any]) -> dict[str, Any]:
    bp = obj.get("bestPromotion")
    if not isinstance(bp, dict):
        return {"purchase_price": "", "can_get_coupon": []}
    cgc = bp.get("canGetCoupon")
    coupons: list[str] = []
    if isinstance(cgc, list):
        for c in cgc[:12]:
            if isinstance(c, dict):
                t = _s(c.get("name") or c.get("desc") or c.get("couponTitle"))
                if t:
                    coupons.append(t[:120])
            elif c:
                coupons.append(_s(str(c))[:120])
    return {
        "purchase_price": _s(bp.get("purchasePrice"))[:32],
        "can_get_coupon": coupons,
    }


def _warm_tips(obj: dict[str, Any]) -> list[str]:
    tips: list[str] = []
    wv = obj.get("warmTipVO")
    if isinstance(wv, dict):
        for t in wv.get("tips") or []:
            if isinstance(t, dict):
                txt = _s(t.get("tipTxt"))
                if txt:
                    tips.append(txt[:300])
    prom = obj.get("promotion")
    if isinstance(prom, dict):
        pr = _s(prom.get("prompt"))
        if pr and pr not in tips:
            tips.append(pr[:300])
    return tips


def _rankings(obj: dict[str, Any]) -> list[str]:
    out: list[str] = []
    rl = obj.get("rankInfoList")
    if isinstance(rl, list):
        for it in rl[:8]:
            if isinstance(it, dict):
                n = _s(it.get("rankName"))
                if n:
                    out.append(n[:200])
    return out


def _service_tag_labels(obj: dict[str, Any]) -> list[str]:
    """主图区服务标：短标签，去重。"""
    seen: set[str] = set()
    labels: list[str] = []
    st = obj.get("serviceTagsVO")
    if isinstance(st, dict):
        for key in ("basicNewIcons", "basicIcons"):
            for it in st.get(key) or []:
                if isinstance(it, dict):
                    tx = _s(it.get("text"))
                    if tx and tx not in seen:
                        seen.add(tx)
                        labels.append(tx[:80])
    return labels[:16]


def _service_tag_details(obj: dict[str, Any], *, limit: int = 6) -> list[dict[str, str]]:
    """每条：标题 + 一句「你能获得什么」说明（来自 tip，已去 HTML）。"""
    out: list[dict[str, str]] = []
    st = obj.get("serviceTagsVO")
    if not isinstance(st, dict):
        return out
    for key in ("basicNewIcons", "basicIcons"):
        for it in st.get(key) or []:
            if not isinstance(it, dict):
                continue
            title = _s(it.get("text"))
            if not title:
                continue
            tip = _strip_html(_s(it.get("tip")), max_len=400)
            out.append({"title": title[:80], "what_you_get": tip})
            if len(out) >= limit:
                return out
    return out


def _delivery_one_liner(obj: dict[str, Any]) -> str:
    si = obj.get("stockInfo")
    if isinstance(si, dict):
        pr = _s(si.get("promiseResult") or si.get("promiseInfoText"))
        if pr:
            return _strip_html(pr, max_len=500)
    sv = obj.get("stockVO")
    if isinstance(sv, dict):
        pr = _s(sv.get("promiseInfoText"))
        if pr:
            return _strip_html(pr, max_len=500)
    return ""


def _logistics_icons(obj: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    sv = obj.get("stockVO")
    if isinstance(sv, dict):
        pil = sv.get("promiseIconList")
        if isinstance(pil, list):
            for it in pil[:10]:
                if isinstance(it, dict) and _s(it.get("text")):
                    texts.append(_s(it.get("text"))[:40])
    return texts


def _bottom_cta_hint(obj: dict[str, Any]) -> str:
    bb = obj.get("bottomBtnVO")
    if not isinstance(bb, dict):
        return ""
    items = bb.get("bottomBtnItems")
    if not isinstance(items, list) or not items:
        return ""
    it0 = items[0]
    if not isinstance(it0, dict):
        return ""
    bs = it0.get("buttonStyle")
    if not isinstance(bs, dict):
        return ""
    tf = bs.get("textFormat")
    if not isinstance(tf, dict):
        return ""
    return _strip_html(_s(tf.get("text")), max_len=200)


def _belt_surface(obj: dict[str, Any]) -> str:
    bi = obj.get("beltBannerInfo")
    if isinstance(bi, dict):
        r = _s(bi.get("bannerRightText") or bi.get("bannerRightTextOfficialDiscount"))
        if r:
            return r[:120]
    return ""


def _enterprise_hint(obj: dict[str, Any]) -> str:
    ch = obj.get("corpHighInfo")
    if isinstance(ch, dict):
        return _s(ch.get("tip"))[:300]
    return ""


def _user_segment_flags(obj: dict[str, Any]) -> dict[str, bool]:
    ui = obj.get("userInfo")
    new_people = bool(ui.get("newPeople")) if isinstance(ui, dict) else False
    return {"new_people": new_people}


def _build_summary_lines(
    *,
    gather: dict[str, Any],
    classic: dict[str, Any],
    bp_sum: dict[str, Any],
    pref: dict[str, Any],
    gov_surf: dict[str, Any],
    warm: list[str],
    delivery: str,
    cta: str,
    flags: dict[str, bool],
) -> list[str]:
    """3～6 条中文短句，面向「我买能怎样」。"""
    lines: list[str] = []

    hand = gather.get("hand_price") or classic.get("hand_price") or bp_sum.get("purchase_price")
    jd_p = gather.get("jd_price") or classic.get("jd_price")
    hl = gather.get("hand_label") or classic.get("hand_label") or "到手价"

    if hand:
        if jd_p and _parse_float_maybe(jd_p) and _parse_float_maybe(hand):
            a, b = _parse_float_maybe(jd_p), _parse_float_maybe(hand)
            if a is not None and b is not None and a > b:
                diff = round(a - b, 2)
                lines.append(
                    f"当前展示「{hl}」约 {hand} 元，相对页面标价 {jd_p} 元约低 {diff} 元（以结算页为准）。"
                )
            else:
                lines.append(
                    f"当前展示「{hl}」约 {hand} 元（页面标价 {jd_p} 元，以结算页为准）。"
                )
        else:
            lines.append(f"当前展示「{hl}」约 {hand} 元（以结算页为准）。")

    ex = pref.get("expression") or {}
    dd = _s(ex.get("discount_desc"))
    da = _s(ex.get("discount_amount"))
    ra = _s(ex.get("red_amount"))
    subs = pref.get("subtrahends") or []
    if dd or da or ra or subs:
        parts: list[str] = []
        if dd and da:
            parts.append(f"{dd}约 {da} 元")
        elif dd:
            parts.append(dd)
        for s in subs:
            if not isinstance(s, dict):
                continue
            cat = _s(s.get("category"))
            desc = _s(s.get("description"))
            if cat and desc:
                parts.append(f"{cat}：{desc}")
            elif desc:
                parts.append(desc)
        if ra and not subs:
            parts.append(f"红包类约 {ra} 元")
        elif ra and subs and not any("红包" in _s(p) for p in parts):
            parts.append(f"红包类约 {ra} 元")
        if parts:
            lines.append(
                "详情页优惠拆解（与腰带/到手价对照）："
                + "；".join(parts[:5])
                + "（以结算页为准）。"
            )

    if pref.get("shared_labels"):
        lines.append(
            "其他权益标签："
            + "、".join(pref["shared_labels"][:4])
            + "。"
        )
    if pref.get("popup_preferences"):
        lines.append(
            "其他权益："
            + "；".join(pref["popup_preferences"][:4])
            + "。"
        )

    if gov_surf.get("gov_subsidy_flag") or gov_surf.get("gov_support_flag"):
        rt = _s(gov_surf.get("right_text"))
        if rt:
            lines.append(f"国补/政府补贴相关展示：{_strip_html(rt, max_len=160)}（以活动规则为准）。")
        else:
            lines.append("页面含政府补贴/国补相关入口（以活动规则与结算为准）。")

    if flags.get("new_people") and cta:
        lines.append(
            f"新人相关文案：{_strip_html(cta)}（若你不是新人，价格与活动可能不同）。"
        )
    elif cta:
        lines.append(f"主按钮文案：{_strip_html(cta)}。")

    if warm:
        lines.append("购买限制与提示：" + "；".join(warm[:4]) + "。")

    # 榜单仅走 visibility.rankings → buyer_ranking_line_from_profile，避免 buyer_promo 重复

    if delivery:
        lines.append("送达：" + delivery + "。")

    if bp_sum.get("can_get_coupon"):
        lines.append(
            "可领券/活动入口（摘要）："
            + "；".join(bp_sum["can_get_coupon"][:4])
            + "。"
        )

    return lines[:8]


def extract_buyer_offer_profile(obj: Any) -> dict[str, Any]:
    """
    从商详 JSON 根对象生成结构化摘要。

    返回 dict 可直接 ``json.dumps(..., ensure_ascii=False)``；无有效输入时仍返回带 ``schema_version`` 的空壳。
    """
    empty: dict[str, Any] = {
        "schema_version": 1,
        "sku_id_hint": "",
        "price_snapshot": {},
        "discount_mechanism": {},
        "gov_support_surface": {},
        "best_promotion": {},
        "purchase_constraints": {"warm_tips": [], "ware_flags": {}},
        "marketing_surface": {},
        "visibility": {"rankings": []},
        "after_sales_short_labels": [],
        "after_sales_details": [],
        "delivery": {"one_liner": "", "logistics_icons": []},
        "buyer_summary_lines": [],
        "notes": "摘要由规则生成，结算价与活动以京东下单页为准。",
    }
    if not isinstance(obj, dict):
        return empty

    pc = obj.get("pageConfigVO")
    sku_hint = ""
    if isinstance(pc, dict):
        sku_hint = _s(pc.get("skuid"))

    gather = _price_from_gather_vo(obj)
    classic = _price_from_classic_price_block(obj)
    if not gather.get("hand_price") and classic.get("hand_price"):
        gather["hand_price"] = classic["hand_price"]
        gather["hand_label"] = classic.get("hand_label") or gather.get("hand_label")
    if not gather.get("jd_price") and classic.get("jd_price"):
        gather["jd_price"] = classic["jd_price"]

    bp_sum = _best_promotion_summary(obj)
    pref_bundle = _preference_bundle(obj)
    gov_surf = _gov_support_surface(obj)
    warm = _warm_tips(obj)
    rankings = _rankings(obj)
    short_labels = _service_tag_labels(obj)
    details = _service_tag_details(obj)
    delivery = _delivery_one_liner(obj)
    log_icons = _logistics_icons(obj)
    cta = _bottom_cta_hint(obj)
    belt = _belt_surface(obj)
    ent = _enterprise_hint(obj)
    flags = _user_segment_flags(obj)

    wim = obj.get("wareInfoReadMap")
    ware_flags: dict[str, str] = {}
    if isinstance(wim, dict):
        for k in ("isCanUseDQ", "msbybt", "productBybt"):
            if k in wim:
                ware_flags[k] = _s(wim.get(k))

    bybt = obj.get("bybtInfo")
    if isinstance(bybt, dict) and bybt.get("productBybt"):
        ware_flags["productBybt"] = "1"

    lines = _build_summary_lines(
        gather=gather,
        classic=classic,
        bp_sum=bp_sum,
        pref=pref_bundle,
        gov_surf=gov_surf,
        warm=warm,
        delivery=delivery,
        cta=cta,
        flags=flags,
    )
    if ent:
        et = ent[:200].strip()
        if et and et[-1] not in "。！？…":
            et += "。"
        lines.append("企业采购提示：" + et)

    out = {
        "schema_version": 1,
        "sku_id_hint": sku_hint,
        "price_snapshot": {
            "hand_price": gather.get("hand_price") or bp_sum.get("purchase_price"),
            "hand_label": gather.get("hand_label") or "到手价",
            "jd_list_price": gather.get("jd_price"),
            "jd_list_hit_line": gather.get("jd_price_hit_line"),
            "price_items": gather.get("raw_items"),
        },
        "discount_mechanism": pref_bundle,
        "gov_support_surface": gov_surf,
        "best_promotion": bp_sum,
        "purchase_constraints": {
            "warm_tips": warm,
            "ware_flags": ware_flags,
        },
        "marketing_surface": {
            "belt_right_text": belt,
            "bottom_button_hint": _strip_html(cta, max_len=200),
        },
        "visibility": {"rankings": rankings},
        "after_sales_short_labels": short_labels,
        "after_sales_details": details,
        "delivery": {"one_liner": delivery, "logistics_icons": log_icons},
        "enterprise_channel": ent,
        "user_segment": flags,
        "buyer_summary_lines": lines,
        "notes": "摘要由规则生成；价格、券、补贴以结算页为准，此处不罗列埋点/实验字段。",
    }
    return out


# --- 与 ``pipeline.jd.buyer_offer_export_csv`` / 流水线 detail_ware 列对齐的扁平字段 ---

_PROMO_EXCLUDE_PREFIXES_FOR_FLAT = ("榜单/曝光", "送达：", "企业采购提示：")
_DEFAULT_BUYER_PROMO_SEP = " | "


def buyer_ranking_line_from_profile(prof: dict[str, Any]) -> str:
    """榜单名单列（如 ``粗粮饼干热卖榜·第5名。``），无 ``榜单/曝光：`` 前缀；无榜单时为空串。"""
    vis = prof.get("visibility")
    if not isinstance(vis, dict):
        return ""
    rk = vis.get("rankings")
    if not isinstance(rk, list) or not rk:
        return ""
    first = strip_buyer_ranking_line_prefix(str(rk[0]).strip())
    if not first:
        return ""
    body = first if first.endswith("。") else first + "。"
    return body


def buyer_promo_text_from_profile(
    prof: dict[str, Any],
    *,
    sep: str = _DEFAULT_BUYER_PROMO_SEP,
) -> str:
    """
    从 ``buyer_summary_lines`` 取句，去掉榜单/送达/企业采购句，用 ``sep`` 拼接。
    """
    raw = prof.get("buyer_summary_lines")
    if not isinstance(raw, list):
        return ""
    parts: list[str] = []
    for line in raw:
        s = str(line).strip()
        if not s:
            continue
        if any(s.startswith(p) for p in _PROMO_EXCLUDE_PREFIXES_FOR_FLAT):
            continue
        parts.append(s)
    return sep.join(parts)


def extract_buyer_offer_profile_from_json_text(text: str) -> dict[str, Any]:
    """解析响应体字符串，失败时返回空壳摘要。"""
    raw = (text or "").strip()
    if not raw:
        return extract_buyer_offer_profile({})
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return extract_buyer_offer_profile({})
    return extract_buyer_offer_profile(obj)
