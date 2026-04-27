# -*- coding: utf-8 -*-
"""
``pc_detailpage_wareBusiness`` 响应的**纯解析**：JSON → 扁平字段、落盘前 JSON 规整、是否应重试。

与 Playwright 拦截、DOM 抽图（``jd_detail_ware_fetch``）分离，便于单测与阅读。
"""
from __future__ import annotations

import json
import re
from typing import Any

# 与 Excel 单元格上限兼顾的默认截断（可被调用方覆盖）
DEFAULT_DETAIL_BODY_MAX_CHARS = 31000


def _normalize_ware_json_tree(obj: Any, *, sort_keys: bool) -> Any:
    """递归规整：字典可选按键名排序，列表保持元素顺序。"""
    if isinstance(obj, dict):
        pairs = [
            (k, _normalize_ware_json_tree(v, sort_keys=sort_keys))
            for k, v in obj.items()
        ]
        if sort_keys:
            pairs.sort(key=lambda kv: kv[0])
        return dict(pairs)
    if isinstance(obj, list):
        return [_normalize_ware_json_tree(x, sort_keys=sort_keys) for x in obj]
    return obj


def format_ware_response_text(
    text: str,
    *,
    normalize: bool,
    sort_keys: bool,
    indent: int,
) -> tuple[str, bool]:
    """
    尝试将接口 body 规整为可读 JSON。
    返回 (输出文本, 是否已成功按 JSON 处理)。
    """
    if not normalize or not (text or "").strip():
        return text, False
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return text, False
    obj = _normalize_ware_json_tree(obj, sort_keys=sort_keys)
    if indent > 0:
        out = json.dumps(obj, ensure_ascii=False, indent=indent) + "\n"
    else:
        out = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    return out, True


def format_ware_response_for_save(
    text: str,
    *,
    normalize: bool = True,
    sort_keys: bool = True,
    indent: int = 2,
) -> str:
    """流水线等落盘用：尽量输出缩进 + 可选键排序的 JSON 文本（失败则保留原文）。"""
    body, _ok = format_ware_response_text(
        text or "",
        normalize=normalize,
        sort_keys=sort_keys,
        indent=max(0, int(indent)),
    )
    return body


# 与 pc_detailpage_wareBusiness 响应对应的扁平字段（字符串，缺失为空），顺序即 CSV 建议列序
WARE_BUSINESS_MERGE_FIELDNAMES: tuple[str, ...] = (
    "detail_sku_title",
    "detail_price_final",
    "detail_price_original",
    "detail_purchase_price",
    "detail_shop_name",
    "detail_shop_id",
    "detail_shop_url",
    "detail_vender_id",
    "detail_stock_text",
    "detail_delivery_promise",
    "detail_sku_name",
    "detail_product_id",
    "detail_main_sku_id",
    "detail_page_sku_id",
    "detail_brand",
    "detail_category_path",
    "detail_main_image",
    "detail_product_attributes",
    "detail_belt_banner",
    "detail_csfh_text",
    # 来自 DOM #detail-main（非 wareBusiness JSON）；列语义为「详情长图衍生信息」，常为 URL 串，流水线可替换为配料表文本
    "detail_body_ingredients",
    # 视觉识别命中时：实际用于解析配料的那张详情长图 URL（自后向前首次通过校验）
    "detail_body_ingredients_source_url",
)


def _empty_ware_flat() -> dict[str, str]:
    return {k: "" for k in WARE_BUSINESS_MERGE_FIELDNAMES}


def _strip_htmlish(s: str, *, max_len: int) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = " ".join(t.split()).strip()
    return t[:max_len] if max_len > 0 else t


def _s(obj: Any) -> str:
    if obj is None:
        return ""
    return str(obj).strip()


def _join_url(u: str) -> str:
    u = u.strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    return u


def _jd_product_image_url(path: str) -> str:
    """与搜索侧一致：jfs/ 相对路径补全为可访问 URL。"""
    p = (path or "").strip()
    if not p:
        return ""
    if p.startswith("//"):
        return "https:" + p
    if p.startswith("http://"):
        return "https://" + p[7:]
    if p.startswith("https://"):
        return p[:800]
    if p.startswith("jfs/"):
        return "https://img13.360buyimg.com/n2/s480x480_" + p[:700]
    return p[:800]


def flatten_ware_business(obj: Any) -> dict[str, str]:
    """
    将 ``pc_detailpage_wareBusiness`` 的 JSON 根对象压成扁平字符串字典。
    非 dict 或字段缺失时对应值为空串。
    """
    out = _empty_ware_flat()
    if not isinstance(obj, dict):
        return out

    sh = obj.get("skuHeadVO")
    sh = sh if isinstance(sh, dict) else {}
    out["detail_sku_title"] = _s(sh.get("skuTitle"))[:2000]

    price = obj.get("price")
    price = price if isinstance(price, dict) else {}
    fp = price.get("finalPrice")
    fp = fp if isinstance(fp, dict) else {}
    out["detail_price_final"] = _s(fp.get("price")) or _s(price.get("p"))[:64]
    out["detail_price_original"] = _s(price.get("op")) or _s(price.get("p"))[:64]

    bp = obj.get("bestPromotion")
    bp = bp if isinstance(bp, dict) else {}
    out["detail_purchase_price"] = _s(bp.get("purchasePrice"))[:64]

    ishop = obj.get("itemShopInfo")
    ishop = ishop if isinstance(ishop, dict) else {}
    out["detail_shop_name"] = _s(ishop.get("shopName"))[:500]
    out["detail_shop_id"] = _s(ishop.get("shopId"))[:32]
    out["detail_shop_url"] = _join_url(_s(ishop.get("shopUrl")))[:500]

    pc = obj.get("pageConfigVO")
    pc = pc if isinstance(pc, dict) else {}
    out["detail_vender_id"] = _s(pc.get("venderId"))[:32]
    sk = pc.get("skuid")
    out["detail_page_sku_id"] = _s(sk)[:32]
    cats = pc.get("catName")
    if isinstance(cats, list):
        parts = [_s(x) for x in cats if _s(x)]
        out["detail_category_path"] = " > ".join(parts)[:500]
    src = _s(pc.get("src"))
    if src:
        out["detail_main_image"] = _jd_product_image_url(src)

    mi = obj.get("mainImageVO")
    if isinstance(mi, dict) and not out["detail_main_image"]:
        mia = mi.get("mainImageArea")
        if isinstance(mia, dict):
            iu = _s(mia.get("imageUrl"))
            if iu:
                out["detail_main_image"] = _jd_product_image_url(iu)

    si = obj.get("stockInfo")
    si = si if isinstance(si, dict) else {}
    out["detail_stock_text"] = _strip_htmlish(_s(si.get("stockDesc")), max_len=500)
    if not out["detail_stock_text"]:
        out["detail_stock_text"] = _strip_htmlish(_s(si.get("promiseResult")), max_len=500)
    out["detail_delivery_promise"] = _strip_htmlish(
        _s(si.get("promiseResult") or si.get("promiseInfoText")), max_len=800
    )

    wim = obj.get("wareInfoReadMap")
    wim = wim if isinstance(wim, dict) else {}
    out["detail_sku_name"] = _s(wim.get("sku_name"))[:2000]
    out["detail_product_id"] = _s(wim.get("product_id"))[:32]
    out["detail_main_sku_id"] = _s(wim.get("main_sku_id"))[:32]
    out["detail_brand"] = _s(wim.get("cn_brand"))[:200]
    if not out["detail_brand"]:
        pav = obj.get("productAttributeVO")
        if isinstance(pav, dict):
            for it in pav.get("attributes") or []:
                if not isinstance(it, dict):
                    continue
                if _s(it.get("labelName")) == "品牌":
                    out["detail_brand"] = _s(it.get("labelValue"))[:200]
                    break

    pav = obj.get("productAttributeVO")
    attrs: list[str] = []
    if isinstance(pav, dict):
        for it in pav.get("attributes") or []:
            if not isinstance(it, dict):
                continue
            ln, lv = _s(it.get("labelName")), _s(it.get("labelValue"))
            if ln and lv:
                attrs.append(f"{ln}:{lv}")
    out["detail_product_attributes"] = "; ".join(attrs)[:4000]

    out["detail_belt_banner"] = _join_url(_s(obj.get("beltBanner")))[:800]
    out["detail_csfh_text"] = _s(obj.get("csfhText"))[:200]

    return out


# 与 OUT_PARSED_CSV / 流水线 detail CSV 列一致
WARE_PARSED_CSV_FIELDNAMES: tuple[str, ...] = (
    "skuId",
    "http_status",
    *WARE_BUSINESS_MERGE_FIELDNAMES,
)

# 仅 sku + 配料（本脚本 OUTPUT_SKU_AND_BODY_IMAGES_ONLY 时 main 写 CSV/解析 JSON 用）
SKU_BODY_IMAGES_ONLY_FIELDNAMES: tuple[str, ...] = (
    "skuId",
    "detail_body_ingredients",
)

# 与合并表 lean 商详块一致 + SKU；keyword_pipeline DETAIL_WARE_CSV_MODE=lean 写 detail_ware_export.csv（纯中文表头）
DETAIL_WARE_LEAN_CSV_FIELDNAMES: tuple[str, ...] = (
    "SKU",
    "品牌",
    "到手价",
    "店铺名称",
    "类目路径",
    "商品参数",
    "配料表",
    "榜单排名",
    "促销摘要",
)

# ``ware_parsed_row`` 可提供的 lean 列（购买者摘要由独立抽取补充）
_DETAIL_WARE_LEAN_FROM_RESPONSE_KEYS: tuple[str, ...] = (
    "skuId",
    "detail_brand",
    "detail_price_final",
    "detail_shop_name",
    "detail_category_path",
    "detail_product_attributes",
    "detail_body_ingredients",
)


def detail_ware_lean_csv_row(
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
    buyer_ranking_line: str = "",
    buyer_promo_text: str = "",
    max_cell_chars: int | None = None,
) -> dict[str, str]:
    """lean 详情汇总表一行（无 http_status）；字段来自 ``ware_parsed_row`` 子集 + 购买者摘要列。"""
    full = ware_parsed_row(
        sku,
        http_status,
        response_text,
        detail_body_ingredients=detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
        max_cell_chars=max_cell_chars,
    )
    out = {k: str(full.get(k) or "") for k in _DETAIL_WARE_LEAN_FROM_RESPONSE_KEYS}
    out["buyer_ranking_line"] = (buyer_ranking_line or "").strip()
    out["buyer_promo_text"] = (buyer_promo_text or "").strip()
    _cn = DETAIL_WARE_LEAN_CSV_FIELDNAMES
    _en = (
        "skuId",
        "detail_brand",
        "detail_price_final",
        "detail_shop_name",
        "detail_category_path",
        "detail_product_attributes",
        "detail_body_ingredients",
        "buyer_ranking_line",
        "buyer_promo_text",
    )
    return {_cn[i]: str(out.get(_en[i]) or "") for i in range(len(_cn))}


def minimal_sku_body_images_row(
    sku: str,
    detail_body_ingredients: str,
    *,
    detail_body_ingredients_source_url: str = "",
    max_cell_chars: int | None = None,
) -> dict[str, str]:
    """``skuId``、配料文本（图源仅内部流程使用，不写入极简 CSV）。"""
    _ = detail_body_ingredients_source_url  # 保留参数供调用方兼容
    u = (detail_body_ingredients or "").strip()
    mxc = max(0, int(max_cell_chars if max_cell_chars is not None else DEFAULT_DETAIL_BODY_MAX_CHARS))
    if mxc and len(u) > mxc:
        u = u[:mxc]
    return {
        "skuId": str(sku).strip(),
        "detail_body_ingredients": u,
    }


def parse_ware_business_response_text(text: str) -> tuple[dict[str, str], bool]:
    """
    解析响应体字符串。
    返回 ``(扁平字典, 是否成功解析为 JSON 对象)``；失败时扁平字典各键均为 ``""``。
    """
    raw = (text or "").strip()
    if not raw:
        return _empty_ware_flat(), False
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_ware_flat(), False
    if not isinstance(obj, dict):
        return _empty_ware_flat(), False
    return flatten_ware_business(obj), True


def ware_parsed_row(
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
    max_cell_chars: int | None = None,
) -> dict[str, str]:
    """单行扁平结果：``skuId``、``http_status`` 与 ``WARE_BUSINESS_MERGE_FIELDNAMES``（含 DOM 长图 URL 或配料文本）。"""
    flat, _ = parse_ware_business_response_text(
        response_text if http_status == 200 else ""
    )
    row: dict[str, str] = {
        "skuId": str(sku).strip(),
        "http_status": str(http_status),
        **flat,
    }
    mxc = max(0, int(max_cell_chars if max_cell_chars is not None else DEFAULT_DETAIL_BODY_MAX_CHARS))
    u = (detail_body_ingredients or "").strip()
    if u:
        row["detail_body_ingredients"] = u[:mxc] if mxc else u
    src = (detail_body_ingredients_source_url or "").strip()
    if src:
        row["detail_body_ingredients_source_url"] = src[:mxc] if mxc else src
    return row


def ware_fetch_should_retry(http_status: int, response_text: str) -> bool:
    """
    True 表示应重试：未命中接口、HTTP 非 200、正文空、非 JSON、或解析后业务字段全空。
    """
    if int(http_status) != 200:
        return True
    raw = (response_text or "").strip()
    if not raw:
        return True
    flat, ok = parse_ware_business_response_text(raw)
    if not ok:
        return True
    return not any((v or "").strip() for v in flat.values())
