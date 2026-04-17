"""
与 ``jd_pc_search`` 导出 CSV 列对齐的字段名映射（入库 / API / 导出共用）。
内部键与爬虫侧 ``JD_ITEM_CSV_FIELDS`` / ``WARE_PARSED_CSV_FIELDNAMES`` 一致，便于对照源码。
"""
from __future__ import annotations

import re


def strip_buyer_ranking_line_prefix(value: str) -> str:
    """去掉榜单列上的 ``榜单/曝光：`` 历史前缀，展示与入库一致。"""
    s = (value or "").strip()
    prefix = "榜单/曝光："
    if s.startswith(prefix):
        return s[len(prefix) :].strip()
    if s.startswith("榜单/曝光"):
        return s[len("榜单/曝光") :].lstrip("：").strip()
    return s


# --- 搜索导出 pc_search_export.csv（纯中文表头，与 jd_h5_search_requests.JD_EXPORT_COLUMN_HEADERS 一致）---
JD_SEARCH_INTERNAL_KEYS: tuple[str, ...] = (
    "item_id",
    "sku_id",
    "title",
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
    "platform",
    "keyword",
    "page",
)

JD_SEARCH_CSV_HEADERS: dict[str, str] = {
    "item_id": "主商品ID",
    "sku_id": "SKU",
    "title": "标题",
    "price": "标价",
    "coupon_price": "券后到手价",
    "original_price": "原价",
    "selling_point": "卖点",
    "comment_sales_floor": "销量楼层",
    "total_sales": "销量展示",
    "hot_list_rank": "榜单类文案",
    "comment_count": "评价量",
    "shop_name": "店铺名",
    "shop_url": "店铺链接",
    "shop_info_url": "店铺信息链接",
    "location": "地域",
    "detail_url": "商品链接",
    "image": "主图",
    "seckill_info": "秒杀",
    "attributes": "规格属性",
    "leaf_category": "类目",
    "platform": "平台",
    "keyword": "搜索词",
    "page": "页码",
}

# CSV 表头 -> 模型属性名
SEARCH_CSV_HEADER_TO_FIELD: dict[str, str] = {
    h: k for k, h in JD_SEARCH_CSV_HEADERS.items()
}

# lean 商详：ORM 内部键（英文 snake_case）；CSV 表头为中文（见 DETAIL_CSV_COLUMNS）
MERGED_LEAN_DETAIL_INTERNAL_KEYS: tuple[str, ...] = (
    "detail_brand",
    "detail_price_final",
    "detail_shop_name",
    "detail_category_path",
    "detail_product_attributes",
    "detail_body_ingredients",
    "buyer_ranking_line",
    "buyer_promo_text",
)

LEAN_DETAIL_EXPORT_FIELDNAMES: tuple[str, ...] = MERGED_LEAN_DETAIL_INTERNAL_KEYS

LEAN_DETAIL_CSV_HEADERS: tuple[str, ...] = (
    "品牌",
    "到手价",
    "店铺名称",
    "类目路径",
    "商品参数",
    "配料表",
    "榜单排名",
    "促销摘要",
)

DETAIL_CSV_HEADER_TO_FIELD: dict[str, str] = dict(
    zip(LEAN_DETAIL_CSV_HEADERS, MERGED_LEAN_DETAIL_INTERNAL_KEYS)
)

# --- 商详 detail_ware_export.csv（lean：SKU + 上列；full 模式爬虫仍可能多列，入库只认 DETAIL_CSV_COLUMNS）---
JD_DETAIL_MERGE_KEYS: tuple[str, ...] = MERGED_LEAN_DETAIL_INTERNAL_KEYS

DETAIL_CSV_COLUMNS: tuple[str, ...] = ("SKU", *LEAN_DETAIL_CSV_HEADERS)

DETAIL_CSV_TO_FIELD: dict[str, str] = {
    "SKU": "sku_id",
    **DETAIL_CSV_HEADER_TO_FIELD,
}

# --- 评价 comments_flat.csv（表头中文；爬虫行字典仍用英文 API 键，写出时映射）---
COMMENT_CSV_COLUMNS: tuple[str, ...] = (
    "SKU",
    "评价ID",
    "用户昵称",
    "评价内容",
    "评价时间",
    "购买次数",
    "晒图链接",
    "评分",
)

COMMENT_CSV_TO_FIELD: dict[str, str] = {
    "SKU": "sku_id",
    "评价ID": "comment_id",
    "用户昵称": "user_nick_name",
    "评价内容": "tag_comment_content",
    "评价时间": "comment_date",
    "购买次数": "buy_count_text",
    "晒图链接": "large_pic_urls",
    "评分": "comment_score",
}

# 爬虫评价行 dict 键（与京东接口字段一致）→ CSV 中文表头
COMMENT_ROW_DICT_KEYS: tuple[str, ...] = (
    "sku",
    "commentId",
    "userNickName",
    "tagCommentContent",
    "commentDate",
    "buyCountText",
    "largePicURLs",
    "commentScore",
)

# --- 合并宽表 keyword_pipeline_merged.csv（lean = 搜索块 + 商详块 + 评论块；改列请改对应块，勿在尾部堆列）---

MERGED_SEARCH_INTERNAL_KEYS: tuple[str, ...] = (
    "pipeline_keyword",
    "sku_id",
    "ware_id",
    "title",
    "price",
    "coupon_price",
    "original_price",
    "selling_point",
    "hot_list_rank",
    "comment_fuzzy",
    "comment_sales_floor",
    "total_sales",
    "shop_name",
    "detail_url",
    "image",
    "attributes",
    "leaf_category",
    "keyword",
    "page",
)


def _merged_search_csv_header_for_internal(k: str) -> str:
    """整合表搜索块：内部键 → 中文 CSV 表头（与 PC 搜索导出列名对齐，合并表专有列单独处理）。"""
    if k == "ware_id":
        return JD_SEARCH_CSV_HEADERS["item_id"]
    if k == "comment_fuzzy":
        return "评价量"
    return JD_SEARCH_CSV_HEADERS[k]


MERGED_SEARCH_CSV_COLUMNS: tuple[str, ...] = (
    "流水线关键词",
    *(
        _merged_search_csv_header_for_internal(k)
        for k in MERGED_SEARCH_INTERNAL_KEYS[1:]
    ),
)

# 商详块：CSV 为中文表头；内部键见 MERGED_LEAN_DETAIL_INTERNAL_KEYS
MERGED_LEAN_DETAIL_KEYS: tuple[str, ...] = LEAN_DETAIL_CSV_HEADERS

MERGED_COMMENT_CSV_COLUMNS: tuple[str, ...] = (
    "评论条数",
    "评价摘要",
)

MERGED_COMMENT_INTERNAL_KEYS: tuple[str, ...] = (
    "pipeline_comment_count",
    "comment_preview",
)

MERGED_CSV_COLUMNS: tuple[str, ...] = (
    *MERGED_SEARCH_CSV_COLUMNS,
    *MERGED_LEAN_DETAIL_KEYS,
    *MERGED_COMMENT_CSV_COLUMNS,
)

MERGED_INTERNAL_KEYS: tuple[str, ...] = (
    *MERGED_SEARCH_INTERNAL_KEYS,
    *MERGED_LEAN_DETAIL_INTERNAL_KEYS,
    *MERGED_COMMENT_INTERNAL_KEYS,
)

assert len(MERGED_CSV_COLUMNS) == len(MERGED_INTERNAL_KEYS)

MERGED_CSV_TO_FIELD: dict[str, str] = dict(zip(MERGED_CSV_COLUMNS, MERGED_INTERNAL_KEYS))

MERGED_FIELD_TO_CSV_HEADER: dict[str, str] = {
    internal: csv_h for csv_h, internal in MERGED_CSV_TO_FIELD.items()
}


def remap_merged_row_english_detail_keys_to_csv_headers(merged: dict[str, str]) -> None:
    """整合表写入前：将 ``ware_flat`` / 抽取逻辑产生的英文商详与购买者内部键改为中文 CSV 列名（原地修改）。"""
    for ik, zh in zip(MERGED_LEAN_DETAIL_INTERNAL_KEYS, LEAN_DETAIL_CSV_HEADERS):
        if ik in merged:
            merged[zh] = str(merged.get(ik) or "")
            del merged[ik]


def infer_total_sales_from_sales_floor(cell: str) -> str:
    """
    从「销量楼层(commentSalesFloor)」列文案截取可作 ``销量展示(totalSales)`` 的片段（与列表接口未单独落 totalSales 列时的兜底一致）。
    """
    t = (cell or "").strip()
    if not t:
        return ""
    m = re.search(r"已售\s*[\d,，.+]*\s*[万亿]?\s*\+?", t)
    if m:
        return m.group(0).strip()
    m2 = re.search(r"已售\s*[\d,，.+\s万千亿]+", t)
    return m2.group(0).strip() if m2 else ""


def merged_csv_effective_total_sales(row: dict[str, str]) -> str:
    """合并表一行：优先已有 ``销量展示(totalSales)`` 列，否则从销量楼层推断。"""
    h_ts = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
    h_fl = MERGED_FIELD_TO_CSV_HEADER["comment_sales_floor"]
    direct = str(row.get(h_ts) or "").strip()
    if direct:
        return direct
    return infer_total_sales_from_sales_floor(str(row.get(h_fl) or ""))


def search_csv_effective_total_sales(row: dict[str, str]) -> str:
    """PC 搜索导出表一行：与 ``merged_csv_effective_total_sales`` 解析规则一致（中文表头）。"""
    h_ts = JD_SEARCH_CSV_HEADERS["total_sales"]
    h_fl = JD_SEARCH_CSV_HEADERS["comment_sales_floor"]
    direct = str(row.get(h_ts) or "").strip()
    if direct:
        return direct
    return infer_total_sales_from_sales_floor(str(row.get(h_fl) or ""))
