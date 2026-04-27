# -*- coding: utf-8 -*-
"""
将历史 JD 流水线 CSV 表头规范为 ``pipeline.csv.schema`` 中的纯中文表头（仅重命名与列序，不改单元格内容逻辑）。

用于已落盘的 ``pipeline_runs/...`` 目录；新跑批次由爬虫直接写出新表头。
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .schema import (
    COMMENT_CSV_COLUMNS,
    COMMENT_ROW_DICT_KEYS,
    DETAIL_CSV_COLUMNS,
    JD_SEARCH_CSV_HEADERS,
    JD_SEARCH_INTERNAL_KEYS,
    MERGED_CSV_COLUMNS,
    MERGED_LEAN_DETAIL_INTERNAL_KEYS,
    LEAN_DETAIL_CSV_HEADERS,
)


def _search_fieldnames_canonical() -> list[str]:
    return [JD_SEARCH_CSV_HEADERS[k] for k in JD_SEARCH_INTERNAL_KEYS]


def _merged_fieldnames_canonical() -> list[str]:
    return list(MERGED_CSV_COLUMNS)


def _detail_fieldnames_canonical() -> list[str]:
    return list(DETAIL_CSV_COLUMNS)


def _comment_fieldnames_canonical() -> list[str]:
    return list(COMMENT_CSV_COLUMNS)


def build_legacy_header_map() -> dict[str, str]:
    """
    旧表头字符串 → 新表头（纯中文或与 schema 一致）。

    覆盖：带英文括号的搜索/合并表、英文 ``pipeline_keyword`` / ``detail_*``、
    评价 CSV 的接口字段名、商详 ``skuId`` 等。
    """
    m: dict[str, str] = {}

    # --- 合并表 / pc_search 曾用的「括号英文」列名 ---
    legacy_search_pairs: tuple[tuple[str, str], ...] = (
        ("主商品ID(wareId)", JD_SEARCH_CSV_HEADERS["item_id"]),
        ("SKU(skuId)", JD_SEARCH_CSV_HEADERS["sku_id"]),
        ("标题(wareName)", JD_SEARCH_CSV_HEADERS["title"]),
        (
            "标价(jdPrice,jdPriceText,realPrice)",
            JD_SEARCH_CSV_HEADERS["price"],
        ),
        (
            "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)",
            JD_SEARCH_CSV_HEADERS["coupon_price"],
        ),
        (
            "原价(oriPrice,originalPrice,marketPrice)",
            JD_SEARCH_CSV_HEADERS["original_price"],
        ),
        ("卖点(sellingPoint)", JD_SEARCH_CSV_HEADERS["selling_point"]),
        ("销量楼层(commentSalesFloor)", JD_SEARCH_CSV_HEADERS["comment_sales_floor"]),
        ("销量展示(totalSales)", JD_SEARCH_CSV_HEADERS["total_sales"]),
        (
            "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)",
            JD_SEARCH_CSV_HEADERS["hot_list_rank"],
        ),
        ("评价量(commentFuzzy)", JD_SEARCH_CSV_HEADERS["comment_count"]),
        ("店铺名(shopName)", JD_SEARCH_CSV_HEADERS["shop_name"]),
        ("店铺链接(shopUrl,shopId)", JD_SEARCH_CSV_HEADERS["shop_url"]),
        (
            "店铺信息链接(shopInfoUrl,brandUrl)",
            JD_SEARCH_CSV_HEADERS["shop_info_url"],
        ),
        ("地域(deliveryAddress,area,procity)", JD_SEARCH_CSV_HEADERS["location"]),
        (
            "商品链接(toUrl,clickUrl,item.m.jd.com)",
            JD_SEARCH_CSV_HEADERS["detail_url"],
        ),
        ("主图(imageurl,imageUrl)", JD_SEARCH_CSV_HEADERS["image"]),
        ("秒杀(seckillInfo,secKill)", JD_SEARCH_CSV_HEADERS["seckill_info"]),
        (
            "规格属性(propertyList,color,catid,shortName)",
            JD_SEARCH_CSV_HEADERS["attributes"],
        ),
        ("类目(leafCategory,cid3Name,catid)", JD_SEARCH_CSV_HEADERS["leaf_category"]),
        ("平台(platform)", JD_SEARCH_CSV_HEADERS["platform"]),
        ("搜索词(keyword)", JD_SEARCH_CSV_HEADERS["keyword"]),
        ("页码(page)", JD_SEARCH_CSV_HEADERS["page"]),
    )
    for old, new in legacy_search_pairs:
        m[old] = new

    # 合并表首列
    m["pipeline_keyword"] = "流水线关键词"

    # 商详块：历史合并表曾直接写英文内部键
    for ik, zh in zip(MERGED_LEAN_DETAIL_INTERNAL_KEYS, LEAN_DETAIL_CSV_HEADERS):
        m[ik] = zh
    m["comment_count"] = "评论条数"
    m["comment_preview"] = "评价摘要"

    # --- comments_flat：接口字段 / 小写 sku ---
    for api_k, zh in zip(COMMENT_ROW_DICT_KEYS, COMMENT_CSV_COLUMNS):
        m[api_k] = zh
    m["sku"] = "SKU"

    # --- detail_ware_export ---
    m["skuId"] = "SKU"

    return m


LEGACY_HEADER_MAP: dict[str, str] = build_legacy_header_map()


def _normalize_field_key(k: str | None) -> str:
    return (k or "").strip().lstrip("\ufeff")


def _row_with_canonical_keys(
    row: dict[str, str],
    legacy_map: dict[str, str],
) -> dict[str, str]:
    """将一行从任意旧表头映射为 canonical 列名；同列多旧键时取非空优先。"""
    out: dict[str, str] = {}
    for raw_k, v in row.items():
        k = _normalize_field_key(raw_k)
        if not k:
            continue
        nk = legacy_map.get(k, k)
        sv = "" if v is None else str(v)
        if nk not in out or (sv.strip() and not str(out.get(nk, "")).strip()):
            out[nk] = sv
    return out


def rewrite_csv_inplace(
    path: Path,
    fieldnames: list[str],
    legacy_map: dict[str, str],
    *,
    dry_run: bool = False,
) -> tuple[bool, str]:
    """
    读 UTF-8 BOM CSV，按 ``fieldnames`` 写出（缺列填空）。

    返回 (是否执行写入, 说明)。
    """
    path = path.expanduser().resolve()
    if not path.is_file():
        return False, f"跳过（不存在）: {path}"

    with path.open(encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        rows_in = list(rdr)

    if not rows_in:
        return False, f"空文件: {path}"

    rows_out: list[dict[str, str]] = []
    extras: set[str] = set()
    for row in rows_in:
        canon = _row_with_canonical_keys(row, legacy_map)
        for k in canon:
            if k not in fieldnames:
                extras.add(k)
        rows_out.append(canon)

    fieldnames_out = list(fieldnames) + sorted(extras)

    if dry_run:
        return True, f"[dry-run] 将写 {len(rows_out)} 行 -> {path.name}"

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames_out,
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(
            [{fn: str(r.get(fn, "") or "") for fn in fieldnames_out} for r in rows_out]
        )

    return True, f"已写 {len(rows_out)} 行 -> {path}"


def rewrite_run_dir_csv_headers(
    run_dir: Path,
    *,
    dry_run: bool = False,
    only: Iterable[str] | None = None,
) -> list[str]:
    """
    处理单个 run 目录下四种 CSV（存在则处理）。

    ``only`` 为文件名子集，例如 ``("keyword_pipeline_merged.csv",)``。
    """
    from pipeline.ingest import (
        FILE_COMMENTS_FLAT_CSV,
        FILE_DETAIL_WARE_CSV,
        FILE_MERGED_CSV,
        FILE_PC_SEARCH_CSV,
    )

    run_dir = run_dir.expanduser().resolve()
    lm = LEGACY_HEADER_MAP
    only_set = {x.strip() for x in only} if only else None

    tasks: list[tuple[str, list[str]]] = [
        (FILE_MERGED_CSV, _merged_fieldnames_canonical()),
        (FILE_PC_SEARCH_CSV, _search_fieldnames_canonical()),
        (FILE_COMMENTS_FLAT_CSV, _comment_fieldnames_canonical()),
        (FILE_DETAIL_WARE_CSV, _detail_fieldnames_canonical()),
    ]

    messages: list[str] = []
    for fname, fieldnames in tasks:
        if only_set is not None and fname not in only_set:
            continue
        ok, msg = rewrite_csv_inplace(
            run_dir / fname,
            fieldnames,
            lm,
            dry_run=dry_run,
        )
        if ok or "空文件" in msg or "不存在" in msg:
            messages.append(msg)
    return messages
