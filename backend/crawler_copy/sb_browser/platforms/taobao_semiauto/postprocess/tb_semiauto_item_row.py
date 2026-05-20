# -*- coding: utf-8 -*-
"""淘宝列表 MTOP ``parse_items_from_mtop_payload`` 结果 → 京东 ``JD_ITEM_CSV_FIELDS`` 内部行。"""
from __future__ import annotations

from typing import Any


def tb_canonical_item_to_jd_row(
    canon: dict[str, str],
    *,
    keyword: str,
    page: str,
    platform: str = "淘宝",
) -> dict[str, str]:
    """将 ``tb_pc_search.mtop.item_extract`` 规整行映射为京东搜索导出内部键。"""
    sales = (canon.get("sales") or "").strip()
    feat = (canon.get("features") or "").strip()
    shop_tag = (canon.get("shop_tag") or "").strip()
    selling = shop_tag[:500] if shop_tag else feat[:500]

    return {
        "item_id": (canon.get("item_id") or "").strip(),
        "sku_id": (canon.get("sku_id") or "").strip(),
        "title": (canon.get("title") or "").strip(),
        "price": (canon.get("price") or "").strip(),
        "coupon_price": (canon.get("coupon_price") or "").strip(),
        "original_price": (canon.get("original_price") or "").strip(),
        "selling_point": selling,
        "comment_sales_floor": "",
        "total_sales": sales,
        "hot_list_rank": (canon.get("hot_list_rank") or "").strip(),
        "comment_count": (canon.get("comment_count") or "").strip(),
        "shop_name": (canon.get("shop_name") or "").strip(),
        "shop_url": (canon.get("shop_url") or "").strip(),
        "shop_info_url": (canon.get("shop_info_url") or "").strip(),
        "location": (canon.get("location") or "").strip(),
        "detail_url": (canon.get("detail_url") or "").strip(),
        "image": (canon.get("image") or "").strip(),
        "seckill_info": (canon.get("seckill_info") or "").strip(),
        "attributes": (canon.get("attributes") or "").strip(),
        "leaf_category": (canon.get("leaf_category") or "").strip(),
        "platform": platform,
        "keyword": keyword,
        "page": str(page).strip() or "1",
    }


def tb_list_keyword_for_parse(blob: dict[str, Any], parsed: dict[str, Any]) -> str:
    """与京东类似：任务 keyword；淘宝列表 MTOP 常在 URL ``data`` 的 ``params``/``q`` 里，此处仅先用任务词。"""
    return str(blob.get("keyword") or "manual").strip() or "manual"
