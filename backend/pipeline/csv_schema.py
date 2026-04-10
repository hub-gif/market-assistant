"""
与 ``jd_pc_search`` 导出 CSV 列对齐的字段名映射（入库 / API / 导出共用）。
内部键与爬虫侧 ``JD_ITEM_CSV_FIELDS`` / ``WARE_PARSED_CSV_FIELDNAMES`` 一致，便于对照源码。
"""
from __future__ import annotations

# --- 搜索导出 pc_search_export.csv（列名为中文，与 jd_h5_search_requests.JD_EXPORT_COLUMN_HEADERS 一致）---
JD_SEARCH_INTERNAL_KEYS: tuple[str, ...] = (
    "item_id",
    "sku_id",
    "title",
    "price",
    "coupon_price",
    "original_price",
    "selling_point",
    "comment_sales_floor",
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
    "item_id": "主商品ID(wareId)",
    "sku_id": "SKU(skuId)",
    "title": "标题(wareName)",
    "price": "标价(jdPrice,jdPriceText,realPrice)",
    "coupon_price": "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)",
    "original_price": "原价(oriPrice,originalPrice,marketPrice)",
    "selling_point": "卖点(sellingPoint)",
    "comment_sales_floor": "销量楼层(commentSalesFloor)",
    "hot_list_rank": "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)",
    "comment_count": "评价量(commentFuzzy)",
    "shop_name": "店铺名(shopName)",
    "shop_url": "店铺链接(shopUrl,shopId)",
    "shop_info_url": "店铺信息链接(shopInfoUrl,brandUrl)",
    "location": "地域(deliveryAddress,area,procity)",
    "detail_url": "商品链接(toUrl,clickUrl,item.m.jd.com)",
    "image": "主图(imageurl,imageUrl)",
    "seckill_info": "秒杀(seckillInfo,secKill)",
    "attributes": "规格属性(propertyList,color,catid,shortName)",
    "leaf_category": "类目(leafCategory,cid3Name,catid)",
    "platform": "平台(platform)",
    "keyword": "搜索词(keyword)",
    "page": "页码(page)",
}

# CSV 表头 -> 模型属性名
SEARCH_CSV_HEADER_TO_FIELD: dict[str, str] = {
    h: k for k, h in JD_SEARCH_CSV_HEADERS.items()
}

# lean 商详子集：合并宽表商详块、detail_ware_export（lean）、JdJobDetailRow 共用（CSV 列名与 ORM 一致）
LEAN_DETAIL_EXPORT_FIELDNAMES: tuple[str, ...] = (
    "detail_brand",
    "detail_price_final",
    "detail_shop_name",
    "detail_category_path",
    "detail_product_attributes",
    "detail_body_ingredients",
)

# --- 商详 detail_ware_export.csv（lean：skuId + 上列；full 模式爬虫仍可能多列，入库只认 DETAIL_CSV_COLUMNS）---
JD_DETAIL_MERGE_KEYS: tuple[str, ...] = LEAN_DETAIL_EXPORT_FIELDNAMES

DETAIL_CSV_COLUMNS: tuple[str, ...] = ("skuId", *JD_DETAIL_MERGE_KEYS)

DETAIL_CSV_TO_FIELD: dict[str, str] = {
    "skuId": "sku_id",
    **{k: k for k in JD_DETAIL_MERGE_KEYS},
}

# --- 评价 comments_flat.csv ---
COMMENT_CSV_COLUMNS: tuple[str, ...] = (
    "sku",
    "commentId",
    "userNickName",
    "tagCommentContent",
    "commentDate",
    "buyCountText",
    "largePicURLs",
    "commentScore",
)

COMMENT_CSV_TO_FIELD: dict[str, str] = {
    "sku": "sku_id",
    "commentId": "comment_id",
    "userNickName": "user_nick_name",
    "tagCommentContent": "tag_comment_content",
    "commentDate": "comment_date",
    "buyCountText": "buy_count_text",
    "largePicURLs": "large_pic_urls",
    "commentScore": "comment_score",
}

# --- 合并宽表 keyword_pipeline_merged.csv（lean = 搜索块 + 商详块 + 评论块；改列请改对应块，勿在尾部堆列）---

MERGED_SEARCH_CSV_COLUMNS: tuple[str, ...] = (
    "pipeline_keyword",
    "SKU(skuId)",
    "主商品ID(wareId)",
    "标题(wareName)",
    "标价(jdPrice,jdPriceText,realPrice)",
    "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)",
    "原价(oriPrice,originalPrice,marketPrice)",
    "卖点(sellingPoint)",
    "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)",
    "评价量(commentFuzzy)",
    "销量楼层(commentSalesFloor)",
    "店铺名(shopName)",
    "商品链接(toUrl,clickUrl,item.m.jd.com)",
    "主图(imageurl,imageUrl)",
    "规格属性(propertyList,color,catid,shortName)",
    "类目(leafCategory,cid3Name,catid)",
    "搜索词(keyword)",
    "页码(page)",
)

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
    "shop_name",
    "detail_url",
    "image",
    "attributes",
    "leaf_category",
    "keyword",
    "page",
)

# 商详块：列名与 ORM 属性同名；与 LEAN_DETAIL_EXPORT_FIELDNAMES / 流水线 lean 一致
MERGED_LEAN_DETAIL_KEYS: tuple[str, ...] = LEAN_DETAIL_EXPORT_FIELDNAMES

MERGED_COMMENT_CSV_COLUMNS: tuple[str, ...] = (
    "comment_count",
    "comment_preview",
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
    *MERGED_LEAN_DETAIL_KEYS,
    *MERGED_COMMENT_INTERNAL_KEYS,
)

assert len(MERGED_CSV_COLUMNS) == len(MERGED_INTERNAL_KEYS)

MERGED_CSV_TO_FIELD: dict[str, str] = dict(zip(MERGED_CSV_COLUMNS, MERGED_INTERNAL_KEYS))

MERGED_FIELD_TO_CSV_HEADER: dict[str, str] = {
    internal: csv_h for csv_h, internal in MERGED_CSV_TO_FIELD.items()
}
