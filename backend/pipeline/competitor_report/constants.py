"""CSV 表头键、运行默认调参与关注词/场景配置（与 ``pipeline.competitor_report.jd_report`` 顶层一致）。"""
from __future__ import annotations

from pipeline.csv_schema import (
    COMMENT_CSV_COLUMNS,
    JD_SEARCH_CSV_HEADERS,
    MERGED_FIELD_TO_CSV_HEADER,
)

_JD_LIST_PRICE_KEY = JD_SEARCH_CSV_HEADERS["price"]
_COUPON_SHOW_PRICE_KEY = JD_SEARCH_CSV_HEADERS["coupon_price"]
_ORIGINAL_LIST_PRICE_KEY = JD_SEARCH_CSV_HEADERS["original_price"]
_SELLING_POINT_KEY = JD_SEARCH_CSV_HEADERS["selling_point"]
_RANK_TAGLINE_KEY = JD_SEARCH_CSV_HEADERS["hot_list_rank"]

# 历史批次 CSV 表头（括号英文）；新批次为纯中文，读取时新键优先
_LEGACY_JD_LIST_PRICE_KEY = "标价(jdPrice,jdPriceText,realPrice)"
_LEGACY_COUPON_SHOW_PRICE_KEY = (
    "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)"
)
_LEGACY_SHOP_NAME_KEY = "店铺名(shopName)"
_LEGACY_RANK_TAGLINE_KEY = "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)"
_LEGACY_COMMENT_FUZZ_KEY = "评价量(commentFuzzy)"
_LEGACY_SELLING_POINT_KEY = "卖点(sellingPoint)"
_LIST_BRAND_TITLE_HEADER = "店铺信息标题"
_LEGACY_LIST_BRAND_TITLE_KEY = "店铺信息标题(shopInfoTitle,brandName)"

_DETAIL_PRICE_FINAL_CSV_KEYS: tuple[str, ...] = (
    MERGED_FIELD_TO_CSV_HEADER["detail_price_final"],
    "detail_price_final",
)
_LIST_PRICE_AND_COUPON_KEYS: tuple[str, ...] = (
    *_DETAIL_PRICE_FINAL_CSV_KEYS,
    _JD_LIST_PRICE_KEY,
    _LEGACY_JD_LIST_PRICE_KEY,
    _COUPON_SHOW_PRICE_KEY,
    _LEGACY_COUPON_SHOW_PRICE_KEY,
)

# 报告摘录「标价」：列表标价优先，缺省时用商详到手价列兜底
_LIST_SHOW_PRICE_CELL_KEYS: tuple[str, ...] = (
    _JD_LIST_PRICE_KEY,
    _LEGACY_JD_LIST_PRICE_KEY,
    MERGED_FIELD_TO_CSV_HEADER["detail_price_final"],
    "detail_price_final",
)

_MERGED_SHOP_CELL_KEYS: tuple[str, ...] = (
    MERGED_FIELD_TO_CSV_HEADER["detail_shop_name"],
    "detail_shop_name",
    JD_SEARCH_CSV_HEADERS["shop_name"],
    _LEGACY_SHOP_NAME_KEY,
)

_COMMENT_FUZZ_KEYS: tuple[str, ...] = (
    MERGED_FIELD_TO_CSV_HEADER["comment_fuzzy"],
    _LEGACY_COMMENT_FUZZ_KEY,
)

_COMMENT_CSV_SKU = COMMENT_CSV_COLUMNS[0]
_COMMENT_CSV_BODY = COMMENT_CSV_COLUMNS[3]
_COMMENT_CSV_SCORE = COMMENT_CSV_COLUMNS[7]  # 「评分」→ commentScore

# 评价星级与 §8.2 分桶：先按评分筛正负，再在对应子集内统计口语短语（无评分时回退关键词）
_COMMENT_SCORE_NEG_MAX = 2  # 1～2 星 → 偏负向
_COMMENT_SCORE_POS_MIN = 4  # 4～5 星 → 偏正向（3 星为中评，归入中性）

_DETAIL_CATEGORY_PATH_KEY = MERGED_FIELD_TO_CSV_HEADER["detail_category_path"]
_K_CAT_COL = JD_SEARCH_CSV_HEADERS["leaf_category"]
_K_PROP_COL = JD_SEARCH_CSV_HEADERS["attributes"]

EXTERNAL_MARKET_TABLE_ROWS: tuple[tuple[str, str, str, str], ...] = ()

COMMENT_FOCUS_WORDS: tuple[str, ...] = (
    "口感",
    "甜",
    "糖",
    "血糖",
    "控糖",
    "低糖",
    "无糖",
    "饱腹",
    "升糖",
    "GI",
    "gi",
    "孕妇",
    "老人",
    "糖尿病",
    "价格",
    "贵",
    "便宜",
    "回购",
    "包装",
    "物流",
    "分量",
    "量少",
    "克重",
)

COMMENT_SCENARIO_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("早餐/代餐", ("早餐", "代餐", "早饭", "当早餐", "当早饭", "早上吃", "晨起")),
    ("零食/加餐/解馋", ("零食", "加餐", "嘴馋", "小零食", "解馋", "垫肚子", "饿了", "肚子饿", "两餐之间", "间食")),
    ("控糖/血糖相关", ("控糖", "血糖高", "升糖", "糖友", "糖尿病", "孕期控糖", "妊娠糖", "血糖")),
    ("孕期/育儿", ("孕期", "孕妇", "怀孕", "产妇", "坐月子", "哺乳", "给宝宝", "给娃", "孩子吃", "小孩吃", "宝宝吃")),
    ("健身/减脂", ("减肥", "减脂", "瘦身", "健身", "卡路里", "热量低", "低脂")),
    ("长辈/家庭", ("老人", "爸妈", "父母", "长辈", "爷爷奶奶", "给家里")),
    ("办公/外出", ("办公室", "上班吃", "出门", "外出", "随身带", "包里", "便携")),
    ("送礼/囤货", ("送礼", "送人", "囤货", "年货")),
    ("夜宵/熬夜", ("夜宵", "熬夜", "晚上饿")),
)

__all__ = [
    "_COMMENT_CSV_BODY",
    "_COMMENT_CSV_SCORE",
    "_COMMENT_CSV_SKU",
    "COMMENT_FOCUS_WORDS",
    "COMMENT_SCENARIO_GROUPS",
    "EXTERNAL_MARKET_TABLE_ROWS",
    "_COMMENT_FUZZ_KEYS",
    "_COMMENT_SCORE_NEG_MAX",
    "_COMMENT_SCORE_POS_MIN",
    "_COUPON_SHOW_PRICE_KEY",
    "_DETAIL_CATEGORY_PATH_KEY",
    "_DETAIL_PRICE_FINAL_CSV_KEYS",
    "_JD_LIST_PRICE_KEY",
    "_K_CAT_COL",
    "_K_PROP_COL",
    "_LEGACY_COUPON_SHOW_PRICE_KEY",
    "_LEGACY_JD_LIST_PRICE_KEY",
    "_LEGACY_LIST_BRAND_TITLE_KEY",
    "_LEGACY_RANK_TAGLINE_KEY",
    "_LEGACY_SELLING_POINT_KEY",
    "_LEGACY_SHOP_NAME_KEY",
    "_LIST_BRAND_TITLE_HEADER",
    "_LIST_PRICE_AND_COUPON_KEYS",
    "_LIST_SHOW_PRICE_CELL_KEYS",
    "_MERGED_SHOP_CELL_KEYS",
    "_ORIGINAL_LIST_PRICE_KEY",
    "_RANK_TAGLINE_KEY",
    "_SELLING_POINT_KEY",
]
