# -*- coding: utf-8 -*-
"""
关键词 → 调用 ``jd_keyword_pipeline`` 全链路采集 → 生成 **标准化竞品分析报告**（Markdown）。

报告结构对齐常见竞品分析框架：研究范围与方法、执行摘要、**整体市场观察（列表可见度 proxy）**、
市场与竞争结构、**按细分类目分组的竞品对比矩阵**、价格分析（含规则化价差/活动信号与可选 **细类价盘·促销** 大模型归纳）、**按细分类目的消费者反馈与用户画像**、**策略与机会提示**（以大模型归纳为主，可选）与附录；并明确数据边界。
若运行配置中提供了外部市场规模摘录（``EXTERNAL_MARKET_TABLE_ROWS``），则追加对应表格小节；否则不输出占位行。

依赖：全量抓取时与 ``jd_keyword_pipeline.py`` 相同（Node、h5st、Playwright、``common/jd_cookie.txt``）。
**仅复用已有目录生成报告时**不需要跑浏览器，只需该目录下已有 CSV / ``run_meta.json``。

用法：

- **重新抓取并出报告**：``EXISTING_RUN_DIR = None``，配置 ``KEYWORD``（及可选 ``OVERRIDE_*``），执行 ``python jd_competitor_report.py``。
- **只分析已有批次**：将 ``EXISTING_RUN_DIR`` 设为 ``pipeline_runs/<时间戳>_<关键词>/`` 的绝对或相对路径（相对当前工作目录），
  再执行同一命令；**不重新抓取**。关键词优先用本文件 ``KEYWORD``，否则读 ``run_meta.json`` 的 ``keyword``，再否则从目录名
  ``YYYYMMDD_HHMMSS_<词>`` 推断。

流水线其余参数（评论分页、延迟等）仍在 ``jd_keyword_pipeline.py`` 顶部配置。

输出：在对应运行目录下覆盖写入 ``competitor_analysis.md``。
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import re
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

import jd_keyword_pipeline as kpl  # noqa: E402
from pipeline.csv_schema import (  # noqa: E402
    COMMENT_CSV_COLUMNS,
    JD_SEARCH_CSV_HEADERS,
    MERGED_FIELD_TO_CSV_HEADER,
    merged_csv_effective_total_sales,
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

# ---------------------------------------------------------------------------
# 运行配置（按需改这里）
# ---------------------------------------------------------------------------
# KEYWORD：京东 PC 搜索词；全量抓取时必填。「仅已有目录」模式下可留空，改从 run_meta / 目录名推断。
KEYWORD = "低GI"
# 已有流水线目录（含 keyword_pipeline_merged.csv 等）时设为路径则**不重新抓取**，只生成 competitor_analysis.md。
EXISTING_RUN_DIR = None
# EXISTING_RUN_DIR = r"data\JD\pipeline_runs\20260408_144606_低GI"  # 相对数据根或绝对路径
# 以下非 None 时仅本次运行临时覆盖 jd_keyword_pipeline 中同名变量（不改 pipeline 文件）
OVERRIDE_MAX_SKUS: int | None = None
OVERRIDE_PAGE_START: int | None = None
OVERRIDE_PAGE_TO: int | None = None

# 评价/预览文本中可统计的「低 GI / 控糖」语境词（命中次数供侧写，非严谨 NLP）
# 可选：第三方市场规模 / 行业增速等（每行四列：指标 | 数值与口径 | 来源 | 年份）。留空则不生成该小节。
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
)

# 用途/场景：每组 (展示名, 触发子串…)。每条评价若命中组内任一子串则该组 +1；同一条可属多组。
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


def _normalize_focus_words(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        return COMMENT_FOCUS_WORDS
    out: list[str] = []
    for x in raw[:120]:
        s = str(x).strip()
        if len(s) > 48:
            s = s[:48]
        if s:
            out.append(s)
    return tuple(out) if out else COMMENT_FOCUS_WORDS


def _normalize_scenario_groups(
    raw: Any,
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if not isinstance(raw, list) or not raw:
        return COMMENT_SCENARIO_GROUPS
    parsed: list[tuple[str, tuple[str, ...]]] = []
    for item in raw[:40]:
        label = ""
        triggers: list[str] = []
        if isinstance(item, dict):
            label = str(item.get("label") or "").strip()[:80]
            tr = item.get("triggers")
            if isinstance(tr, list):
                for t in tr[:48]:
                    s = str(t).strip()
                    if len(s) > 48:
                        s = s[:48]
                    if s:
                        triggers.append(s)
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            label = str(item[0]).strip()[:80]
            tr = item[1]
            if isinstance(tr, (list, tuple)):
                for t in tr[:48]:
                    s = str(t).strip()
                    if len(s) > 48:
                        s = s[:48]
                    if s:
                        triggers.append(s)
        if label and triggers:
            parsed.append((label, tuple(triggers)))
    return tuple(parsed) if parsed else COMMENT_SCENARIO_GROUPS


def _normalize_external_market_rows(
    raw: Any,
) -> tuple[tuple[str, str, str, str], ...]:
    if not isinstance(raw, list) or not raw:
        return EXTERNAL_MARKET_TABLE_ROWS
    rows: list[tuple[str, str, str, str]] = []

    def _four_cells(x: Any) -> tuple[str, str, str, str] | None:
        if isinstance(x, (list, tuple)) and len(x) >= 4:
            return tuple(str(c)[:500] for c in x[:4])
        if isinstance(x, dict):
            a = str(x.get("indicator") or x.get("a") or "").strip()[:500]
            b = str(x.get("value_and_scope") or x.get("b") or "").strip()[:500]
            c = str(x.get("source") or x.get("c") or "").strip()[:500]
            d = str(x.get("year") or x.get("d") or "").strip()[:500]
            if any((a, b, c, d)):
                return (a, b, c, d)
        return None

    for item in raw[:24]:
        r = _four_cells(item)
        if r:
            rows.append(r)
    return tuple(rows) if rows else EXTERNAL_MARKET_TABLE_ROWS


def resolve_report_tuning(
    report_config: dict[str, Any] | None,
) -> tuple[
    tuple[str, ...],
    tuple[tuple[str, tuple[str, ...]], ...],
    tuple[tuple[str, str, str, str], ...],
]:
    if not report_config:
        return COMMENT_FOCUS_WORDS, COMMENT_SCENARIO_GROUPS, EXTERNAL_MARKET_TABLE_ROWS
    return (
        _normalize_focus_words(report_config.get("comment_focus_words")),
        _normalize_scenario_groups(report_config.get("comment_scenario_groups")),
        _normalize_external_market_rows(
            report_config.get("external_market_table_rows")
        ),
    )


def _cell(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = str(row.get(k) or "").strip()
        if v:
            return v
    return ""


_DETAIL_CATEGORY_PATH_KEY = MERGED_FIELD_TO_CSV_HEADER["detail_category_path"]
_K_CAT_COL = JD_SEARCH_CSV_HEADERS["leaf_category"]
_K_PROP_COL = JD_SEARCH_CSV_HEADERS["attributes"]


def _shortname_from_prop(prop: str) -> str:
    m = re.search(r"简称[:：]\s*([^|]+)", prop or "")
    return m.group(1).strip()[:120] if m else ""


def _detail_category_path_cell(row: dict[str, str]) -> str:
    """细类矩阵与按细类评价统计仅以该列为准；空则视为商详类目不完整。"""
    return _cell(row, _DETAIL_CATEGORY_PATH_KEY, "detail_category_path")


def _search_export_catid_to_shortname_map(rows: list[dict[str, str]]) -> dict[str, str]:
    """列表导出中叶子类目列常为纯数字 ID：用同行规格属性「简称」映射为可读名称。"""
    m: dict[str, str] = {}
    for r in rows:
        cid = _cell(r, _K_CAT_COL).strip()
        if not cid.isdigit():
            continue
        if cid in m:
            continue
        sn = _shortname_from_prop(_cell(r, _K_PROP_COL))
        if sn:
            m[cid] = sn
    return m


def _md_cell(s: str, max_len: int = 120) -> str:
    t = (s or "").replace("\r\n", " ").replace("\n", " ").replace("|", "/")
    t = " ".join(t.split())
    return (t[:max_len] + "…") if max_len > 0 and len(t) > max_len else t


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        return [], []
    raw = path.read_text(encoding="utf-8-sig")
    lines = raw.splitlines()
    if not lines:
        return [], []
    rdr = csv.DictReader(lines)
    fn = rdr.fieldnames or []
    return list(fn), list(rdr)


def _float_price(s: str) -> float | None:
    if not (s or "").strip():
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(s).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _collect_prices(rows: list[dict[str, str]]) -> list[float]:
    out: list[float] = []
    for row in rows:
        for k in _LIST_PRICE_AND_COUPON_KEYS:
            p = _float_price(_cell(row, k))
            if p is not None and 0 < p < 1_000_000:
                out.append(p)
                break
    return out


# 列表/合并表中「卖点、腰带」常见活动话术子串（行级命中，非严谨 NLP）
_PROMO_SUBSTRINGS_IN_COPY: tuple[str, ...] = (
    "满减",
    "秒杀",
    "限时",
    "优惠券",
    "领券",
    "券后",
    "百亿补贴",
    "包邮",
    "赠品",
    "买赠",
    "第二件",
    "第2件",
    "直降",
    "特价",
    "促销",
    "套装",
    "任选",
    "到手价",
    "补贴",
    "聚划算",
    "预售",
    "定金",
    "返现",
    "折扣",
    "加购",
    "下单立减",
)


def _analyze_price_promotions(rows: list[dict[str, str]]) -> dict[str, Any]:
    """
    从列表或合并行中归纳「标价 vs 券后/到手」及卖点/腰带中的活动话术信号，
    供 §6.1 与结构化摘要使用（**页面展示口径**，非结算实付）。
    """
    n = len(rows)
    with_jd = with_cp = with_both = 0
    coupon_below = 0
    pct_offs: list[float] = []
    ori_above_list = 0
    for row in rows:
        jd = _float_price(_cell(row, _JD_LIST_PRICE_KEY, _LEGACY_JD_LIST_PRICE_KEY))
        cp = _float_price(
            _cell(row, _COUPON_SHOW_PRICE_KEY, _LEGACY_COUPON_SHOW_PRICE_KEY)
        )
        ori = _float_price(_cell(row, _ORIGINAL_LIST_PRICE_KEY))
        if jd is not None and jd > 0:
            with_jd += 1
        if cp is not None and cp > 0:
            with_cp += 1
        if jd is not None and cp is not None and jd > 0 and cp > 0:
            with_both += 1
            if cp + 1e-6 < jd:
                coupon_below += 1
                pct_offs.append((jd - cp) / jd * 100.0)
        if (
            ori is not None
            and jd is not None
            and ori > 0
            and jd > 0
            and ori > jd + 1e-6
        ):
            ori_above_list += 1

    selling_nonempty = sum(
        1
        for r in rows
        if _cell(r, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY).strip()
    )
    rank_nonempty = sum(
        1
        for r in rows
        if _cell(r, _RANK_TAGLINE_KEY, _LEGACY_RANK_TAGLINE_KEY).strip()
    )

    kw_row_hits: dict[str, int] = {}
    for kw in _PROMO_SUBSTRINGS_IN_COPY:
        c = 0
        for row in rows:
            blob = (
                _cell(row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY)
                + " "
                + _cell(row, _RANK_TAGLINE_KEY, _LEGACY_RANK_TAGLINE_KEY)
            )
            if kw in blob:
                c += 1
        if c:
            kw_row_hits[kw] = c
    top_promos = sorted(kw_row_hits.items(), key=lambda x: -x[1])[:14]

    median_pct = statistics.median(pct_offs) if pct_offs else None
    mean_pct = statistics.mean(pct_offs) if pct_offs else None
    share_below = (
        (coupon_below / with_both) if with_both else None
    )

    return {
        "row_count": n,
        "rows_with_list_price": with_jd,
        "rows_with_coupon_price": with_cp,
        "rows_with_both_list_and_coupon": with_both,
        "rows_coupon_below_list_price": coupon_below,
        "share_coupon_below_list_when_both": share_below,
        "median_discount_pct_when_coupon_below": median_pct,
        "mean_discount_pct_when_coupon_below": mean_pct,
        "rows_original_price_above_list_price": ori_above_list,
        "rows_selling_point_nonempty": selling_nonempty,
        "rows_rank_tagline_nonempty": rank_nonempty,
        "promo_keyword_row_hits_top": [
            {"keyword": k, "rows": v} for k, v in top_promos
        ],
    }


def _markdown_price_promotion_section(p: dict[str, Any]) -> list[str]:
    """§6.1 优惠活动与价差信号（Markdown 行列表）。"""
    lines: list[str] = [
        "### 6.1 优惠活动与价差信号（页面展示摘录）",
        "",
        "- **口径**：与上节价量统计**同一批行**；比较的是列表/合并表中的**展示标价**与**展示券后/到手价**（字段见表头），"
        "反映页面呈现的活动与券信息，**不等于**用户结算实付或历史最低价。",
        "",
    ]
    wb = int(p.get("rows_with_both_list_and_coupon") or 0)
    if wb <= 0:
        lines.append(
            "- **标价与券后价可对齐比较**的有效行不足，本节仅摘录卖点/腰带中的活动话术（若有）。"
        )
        lines.append("")
    else:
        cb = int(p.get("rows_coupon_below_list_price") or 0)
        sh = p.get("share_coupon_below_list_when_both")
        med = p.get("median_discount_pct_when_coupon_below")
        mean = p.get("mean_discount_pct_when_coupon_below")
        lines.append(
            f"- **同时解析到标价与券后/到手价** 的行：**{wb}**；其中展示「到手/券后」**严格低于**「标价」的行：**{cb}**"
            + (
                f"（占可对齐行的 **{100.0 * float(sh):.1f}%**）"
                if isinstance(sh, (int, float))
                else ""
            )
            + "。"
        )
        if med is not None:
            frag_mean = (
                f"，平均价差约 **{float(mean):.1f}%**" if mean is not None else ""
            )
            lines.append(
                f"- **价差力度（仅「券后低于标价」子集）**：展示价差的中位数约 **{float(med):.1f}%**（相对标价）{frag_mean}；"
                "通常对应满减、券、限时价等在列表上的叠加呈现。"
            )
        elif cb > 0:
            lines.append(
                "- **价差**：存在「券后低于标价」样本，但条数较少，未给出稳健分位数；建议结合 §5 单品对照。"
            )
        lines.append("")
        oa = int(p.get("rows_original_price_above_list_price") or 0)
        if oa > 0:
            lines.append(
                f"- **划线原价高于当前标价** 的行约 **{oa}** 条（常见「划线价 + 当前价」促销陈列，具体以页面为准）。"
            )
            lines.append("")
    sn = int(p.get("rows_selling_point_nonempty") or 0)
    rn = int(p.get("rows_rank_tagline_nonempty") or 0)
    nr = int(p.get("row_count") or 0)
    if nr > 0:
        lines.append(
            f"- **卖点字段非空**：**{sn}** / **{nr}** 行；**榜单/腰带类文案非空**：**{rn}** / **{nr}** 行（用于观察申报活动与心智标签）。"
        )
        lines.append("")
    top = p.get("promo_keyword_row_hits_top") or []
    if isinstance(top, list) and top:
        lines.append("- **活动话术在列表行中的出现面**（卖点+腰带合并扫描预设子串；**同一行可含多词**，为行级命中次数）：")
        parts = []
        for it in top[:10]:
            if isinstance(it, dict):
                k = it.get("keyword") or ""
                v = it.get("rows")
                if k and v is not None:
                    parts.append(f"「{k}」**{int(v)}** 行")
        if parts:
            lines.append("  - " + "；".join(parts) + "。")
        lines.append(
            "  - **解读**：高频词反映列表侧**主推活动类型**（如满减、百亿补贴、赠品）；与 §6 分布表结合看「低价来自常态价还是大促价带」。"
        )
    else:
        lines.append(
            "- **活动话术**：未在卖点/腰带字段中命中预设促销子串（可能字段为空或话术与词表不一致）。"
        )
    lines.append("")
    return lines


def _comment_keyword_hits(
    rows: list[dict[str, str]],
    focus_words: tuple[str, ...],
) -> Counter[str]:
    c: Counter[str] = Counter()
    texts: list[str] = []
    for row in rows:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if t:
            texts.append(t)
    blob = "\n".join(texts)
    for w in focus_words:
        if len(w) == 1:
            continue
        n = blob.count(w)
        if n:
            c[w] += n
    return c


def _merge_comment_previews(merged_rows: list[dict[str, str]]) -> str:
    parts: list[str] = []
    for row in merged_rows:
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            parts.append(p)
    return "\n".join(parts)


def _iter_comment_text_units(
    comment_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
) -> list[str]:
    """逐条评价正文；无 flat 评论时用合并表 comment_preview 按行兜底。"""
    out: list[str] = []
    for row in comment_rows:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if t:
            out.append(t)
    if out:
        return out
    for row in merged_rows:
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            out.append(p)
    return out


_POS_LEX = (
    "好",
    "赞",
    "满意",
    "回购",
    "推荐",
    "不错",
    "喜欢",
    "香",
    "实惠",
    "值得",
    "棒",
    "鲜嫩",
    "好吃",
    "划算",
    "正品",
    "好评",
)
_NEG_LEX = (
    "差",
    "烂",
    "难吃",
    "失望",
    "假",
    "骗",
    "退货",
    "不建议",
    "糟糕",
    "难用",
    "臭",
    "差评",
    "不好",
    "难喝",
    "发霉",
)
# 条形图/摘要用：多字短语优先，避免只显示「硬、差」等单字
_POS_LEXEME_DETAIL = (
    "已经回购很多次",
    "还会再买",
    "值得回购",
    "推荐购买",
    "性价比很高",
    "性价比不错",
    "物美价廉",
    "物流很快",
    "包装很用心",
    "包装完好",
    "口感很好",
    "味道不错",
    "很好吃",
    "香而不腻",
    "饱腹感不错",
    "控糖很友好",
    "低糖很适合",
    "代餐很方便",
    "品质很稳定",
    "值得信赖",
    "软硬适中",
)
_NEG_LEXEME_DETAIL = (
    "口感偏硬",
    "口感很硬",
    "咬不动",
    "发硬",
    "硬邦邦",
    "口感发粘",
    "太甜了",
    "甜得发腻",
    "甜度过高",
    "不太好吃",
    "很难吃",
    "味道很奇怪",
    "有股怪味",
    "一股异味",
    "包装破损",
    "漏气受潮",
    "日期不新鲜",
    "临期产品",
    "质量很差",
    "不值这个价",
    "与描述不符",
    "疑似假货",
    "发货特别慢",
    "物流太慢了",
    "售后很差",
    "退款很麻烦",
    "不建议购买",
    "不会再买",
)


def _lex_tuple_classify(*parts: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for tup in parts:
        for w in tup:
            w = (w or "").strip()
            if w and w not in seen:
                seen.add(w)
                out.append(w)
    out.sort(key=len, reverse=True)
    return tuple(out)


_POS_CLASS = _lex_tuple_classify(_POS_LEX, _POS_LEXEME_DETAIL)
_NEG_CLASS = _lex_tuple_classify(_NEG_LEX, _NEG_LEXEME_DETAIL)
_POS_LEX_HITS = tuple(sorted(_POS_LEXEME_DETAIL, key=len, reverse=True))
_NEG_LEX_HITS = tuple(sorted(_NEG_LEXEME_DETAIL, key=len, reverse=True))


def _lexeme_hits_in_texts(
    texts: list[str], lexemes: tuple[str, ...]
) -> list[dict[str, Any]]:
    """每条文本内同一短语只计 1 次；``lexemes`` 宜按长度降序以优先匹配更长表述。"""
    c: Counter[str] = Counter()
    for raw in texts:
        s = (raw or "").strip()
        if not s:
            continue
        seen_line: set[str] = set()
        for k in lexemes:
            if not k or k not in s:
                continue
            if k in seen_line:
                continue
            seen_line.add(k)
            c[k] += 1
    return [{"word": w, "texts_matched": n} for w, n in c.most_common(18)]


def _comment_sentiment_lexicon(texts: list[str]) -> dict[str, Any]:
    """
    基于预设词表对每条文本做正/负向粗判（非深度学习）；同条同时含正负词时计为「混合」。
    短语级词频仅在对应语境下统计（条形图用 ``_POS_LEX_HITS`` / ``_NEG_LEX_HITS``）。
    """
    pos_only = neg_only = mixed = neutral = 0
    corpus_pos_mixed: list[str] = []
    corpus_neg_mixed: list[str] = []
    for t in texts:
        s = (t or "").strip()
        if not s:
            neutral += 1
            continue
        hp = any(k in s for k in _POS_CLASS)
        hn = any(k in s for k in _NEG_CLASS)
        if hp and hn:
            mixed += 1
            corpus_pos_mixed.append(s)
            corpus_neg_mixed.append(s)
        elif hp:
            pos_only += 1
            corpus_pos_mixed.append(s)
        elif hn:
            neg_only += 1
            corpus_neg_mixed.append(s)
        else:
            neutral += 1
    total = len(texts)
    pos_lex = _lexeme_hits_in_texts(corpus_pos_mixed, _POS_LEX_HITS)
    neg_lex = _lexeme_hits_in_texts(corpus_neg_mixed, _NEG_LEX_HITS)
    return {
        "method": "keyword_lexicon",
        "text_units": total,
        "positive_only": pos_only,
        "negative_only": neg_only,
        "mixed_positive_and_negative": mixed,
        "neutral_or_empty": neutral,
        "positive_lexicon_sample": list(_POS_LEX[:10]) + list(_POS_LEXEME_DETAIL[:5]),
        "negative_lexicon_sample": list(_NEG_LEX[:10]) + list(_NEG_LEXEME_DETAIL[:5]),
        "positive_tone_lexeme_hits": pos_lex,
        "negative_tone_lexeme_hits": neg_lex,
        "lexeme_scope_note": (
            "「正向短语」仅在命中正向词表的评价条内统计（含混合条）；"
            "「负向短语」仅在命中负向词表的评价条内统计（含混合条）；每条每短语最多计 1 次；"
            "统计的是预设口语片段，非分词模型。"
        ),
    }


def _comment_lines_with_product_context(
    comment_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
    *,
    sku_header: str,
    title_h: str,
) -> list[str]:
    """与 ``comment_rows`` 顺序对齐：带细类/SKU/品名/店铺前缀，供 §8.2 大模型抽样。"""
    sku_meta: dict[str, tuple[str, str, str]] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if not gk:
            continue
        sku_meta[sku] = (
            gk,
            _cell(row, title_h),
            _cell(row, *_MERGED_SHOP_CELL_KEYS),
        )
    out: list[str] = []
    for cr in comment_rows:
        txt = _cell(cr, _COMMENT_CSV_BODY, "tagCommentContent")
        if not txt:
            continue
        sku = _cell(cr, _COMMENT_CSV_SKU, "sku").strip()
        meta = sku_meta.get(sku)
        if meta:
            gname, tit, shop = meta
            prefix = (
                f"【细类：{gname}｜SKU：{sku}｜品名：{_md_cell(tit, 80)}｜"
                f"店铺：{_md_cell(shop, 40)}】"
            )
            out.append(prefix + txt)
        else:
            out.append(txt)
    return out


def _matrix_excerpt_line_for_llm(row: dict[str, str], title_h: str) -> str:
    title = _md_cell(_cell(row, title_h), 100)
    sp = _md_cell(_cell(row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY), 120)
    ing_raw = _ingredients_from_product_attributes(
        _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["detail_product_attributes"],
            "detail_product_attributes",
        )
    )
    ing = _md_cell(_ingredients_single_line(ing_raw), 100) if ing_raw else ""
    chunks: list[str] = []
    if title:
        chunks.append(title)
    if sp:
        chunks.append(f"卖点:{sp}")
    if ing:
        chunks.append(f"配料:{ing}")
    return "｜".join(chunks) if chunks else "（无标题摘录）"


def _listing_price_snippet_for_llm(row: dict[str, str], title_h: str) -> str:
    title = _md_cell(_cell(row, title_h), 72)
    lp = _cell(row, *_LIST_SHOW_PRICE_CELL_KEYS)
    cp = _cell(row, _COUPON_SHOW_PRICE_KEY, _LEGACY_COUPON_SHOW_PRICE_KEY)
    dp = _cell(row, *_DETAIL_PRICE_FINAL_CSV_KEYS)
    return f"{title}｜标价:{lp}｜券后:{cp}｜详情价:{dp}"


def build_matrix_groups_llm_payload(
    merged_rows: list[dict[str, str]],
    *,
    title_h: str,
    sku_header: str = "",
) -> list[dict[str, Any]]:
    """供 ``generate_matrix_group_summaries_llm``：与 §5 细类划分一致。"""
    _ = sku_header
    if not merged_rows:
        return []
    out: list[dict[str, Any]] = []
    for gname, grows in _merged_rows_grouped_for_matrix(merged_rows):
        prices = _collect_prices(grows)
        pst = _price_stats_extended(prices) if prices else {"n": 0}
        lines = [_matrix_excerpt_line_for_llm(r, title_h) for r in grows[:24]]
        out.append(
            {
                "group": gname,
                "sku_count": len(grows),
                "price_stats": pst,
                "lines": lines,
            }
        )
    return out


def build_price_groups_llm_payload(
    merged_rows: list[dict[str, str]],
    *,
    title_h: str,
    sku_header: str = "",
) -> list[dict[str, Any]]:
    """供 ``generate_price_group_summaries_llm``。"""
    _ = sku_header
    if not merged_rows:
        return []
    out: list[dict[str, Any]] = []
    for gname, grows in _merged_rows_grouped_for_matrix(merged_rows):
        prices = _collect_prices(grows)
        pst = _price_stats_extended(prices) if prices else {"n": 0}
        snippets = [_listing_price_snippet_for_llm(r, title_h) for r in grows[:16]]
        out.append(
            {
                "group": gname,
                "sku_count": len(grows),
                "price_stats": pst,
                "listing_snippets": snippets,
            }
        )
    return out


def _promo_snippet_for_llm(row: dict[str, str], title_h: str) -> str:
    """单条 SKU：合并表中的「促销摘要 / 榜单排名 / 列表卖点与腰带」摘录，供促销 LLM 用。"""
    title = _md_cell(_cell(row, title_h), 56)
    promo = _cell(
        row,
        MERGED_FIELD_TO_CSV_HEADER["buyer_promo_text"],
        "buyer_promo_text",
    )
    br = _cell(
        row,
        MERGED_FIELD_TO_CSV_HEADER["buyer_ranking_line"],
        "buyer_ranking_line",
    )
    sp = _cell(row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY)
    belt = _cell(row, _RANK_TAGLINE_KEY, _LEGACY_RANK_TAGLINE_KEY)
    parts: list[str] = [title]
    if promo.strip():
        parts.append(
            f"{MERGED_FIELD_TO_CSV_HEADER['buyer_promo_text']}:{_md_cell(promo, 360)}"
        )
    if br.strip():
        parts.append(
            f"{MERGED_FIELD_TO_CSV_HEADER['buyer_ranking_line']}:{_md_cell(br, 120)}"
        )
    if sp.strip():
        parts.append(f"{JD_SEARCH_CSV_HEADERS['selling_point']}:{_md_cell(sp, 100)}")
    if belt.strip():
        parts.append(
            f"{JD_SEARCH_CSV_HEADERS['hot_list_rank']}:{_md_cell(belt, 80)}"
        )
    return "｜".join(parts) if len(parts) > 1 else (parts[0] if parts else "")


def build_promo_groups_llm_payload(
    merged_rows: list[dict[str, str]],
    *,
    title_h: str,
    sku_header: str = "",
) -> list[dict[str, Any]]:
    """供 ``generate_promo_group_summaries_llm``：与 §5/§6 细类划分一致。"""
    _ = sku_header
    if not merged_rows:
        return []
    out: list[dict[str, Any]] = []
    for gname, grows in _merged_rows_grouped_for_matrix(merged_rows):
        snippets = [_promo_snippet_for_llm(r, title_h) for r in grows[:16]]
        nonempty = sum(
            1
            for r in grows
            if _cell(
                r,
                MERGED_FIELD_TO_CSV_HEADER["buyer_promo_text"],
                "buyer_promo_text",
            ).strip()
        )
        out.append(
            {
                "group": gname,
                "sku_count": len(grows),
                "rows_with_buyer_promo_text": nonempty,
                "promo_snippets": snippets,
            }
        )
    return out


def build_comment_groups_llm_payload(
    *,
    feedback_groups: list[tuple[str, list[dict[str, str]], list[str]]],
    focus_words: tuple[str, ...],
    merged_rows: list[dict[str, str]],
    sku_header: str,
    title_h: str,
) -> list[dict[str, Any]]:
    """供 ``generate_comment_group_summaries_llm``。"""
    if not feedback_groups:
        return []
    sku_meta: dict[str, tuple[str, str, str]] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if not gk:
            continue
        sku_meta[sku] = (
            gk,
            _cell(row, title_h),
            _cell(row, *_MERGED_SHOP_CELL_KEYS),
        )
    out: list[dict[str, Any]] = []
    for gname, cr, tu in feedback_groups:
        if not tu and not cr:
            continue
        gh = _group_keyword_hits(cr, tu, focus_words=focus_words)
        focus_hit_lines = [
            f"「{w}」{n} 次" for w, n in gh.most_common(14) if n > 0
        ]
        snippets: list[str] = []
        for row in cr[:48]:
            txt = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
            if not txt:
                continue
            sku = _cell(row, _COMMENT_CSV_SKU, "sku").strip()
            meta = sku_meta.get(sku)
            if meta:
                sg, tit, shop = meta
                prefix = (
                    f"【细类：{sg}｜SKU：{sku}｜品名：{_md_cell(tit, 60)}｜"
                    f"店铺：{_md_cell(shop, 28)}】"
                )
                snippets.append(prefix + txt[:300])
            else:
                snippets.append(
                    f"【细类：{gname}｜SKU：{sku or '—'}】" + txt[:320]
                )
            if len(snippets) >= 16:
                break
        eff = tu[:28]
        if len(tu) > 28:
            eff = list(eff) + [f"…共 {len(tu)} 条有效文本，此处截断"]
        out.append(
            {
                "group": gname,
                "comment_flat_rows": f"评价行 {len(cr)}；有效文本单元 {len(tu)}",
                "effective_text_lines": eff,
                "focus_hit_lines": focus_hit_lines,
                "sample_text_snippets": snippets,
            }
        )
    return out


def build_scenario_groups_llm_payload(
    *,
    feedback_groups: list[tuple[str, list[dict[str, str]], list[str]]],
    scenario_groups: tuple[tuple[str, tuple[str, ...]], ...],
    merged_rows: list[dict[str, str]],
    sku_header: str,
    title_h: str,
) -> dict[str, Any]:
    """供 ``generate_scenario_group_summaries_llm``；计数与 §8.3 图右栏（场景）一致。"""
    if not feedback_groups:
        return {}
    sku_meta: dict[str, tuple[str, str, str]] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if not gk:
            continue
        sku_meta[sku] = (
            gk,
            _cell(row, title_h),
            _cell(row, *_MERGED_SHOP_CELL_KEYS),
        )
    lexicon = [
        {"label": lbl, "trigger_examples": list(trigs[:12])}
        for lbl, trigs in scenario_groups
    ]
    groups_out: list[dict[str, Any]] = []
    for gname, cr, tu in feedback_groups:
        if not tu and not cr:
            continue
        scen_g, scen_ng = _comment_scenario_counts(tu, scenario_groups)
        dist: list[dict[str, Any]] = []
        for lbl, n in scen_g.most_common():
            if n <= 0:
                continue
            dist.append(
                {
                    "scenario": lbl,
                    "mention_rows": int(n),
                    "share_of_effective_texts": round(
                        float(n) / float(scen_ng), 4
                    )
                    if scen_ng > 0
                    else 0.0,
                }
            )
        snippets: list[str] = []
        for row in cr:
            txt = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
            if not txt:
                continue
            if not _text_hits_scenario_triggers(txt, scenario_groups):
                continue
            sku = _cell(row, _COMMENT_CSV_SKU, "sku").strip()
            meta = sku_meta.get(sku)
            if meta:
                sg, tit, shop = meta
                prefix = (
                    f"【细类：{sg}\uff5cSKU：{sku}\uff5c品名：{_md_cell(tit, 60)}\uff5c"
                    f"店铺：{_md_cell(shop, 28)}】"
                )
                snippets.append(prefix + txt[:300])
            else:
                snippets.append(
                    f"【细类：{gname}\uff5cSKU：{sku or '—'}】" + txt[:320]
                )
            if len(snippets) >= 16:
                break
        if len(snippets) < 5:
            for row in cr:
                txt = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
                if not txt:
                    continue
                sku = _cell(row, _COMMENT_CSV_SKU, "sku").strip()
                meta = sku_meta.get(sku)
                if meta:
                    sg, tit, shop = meta
                    prefix = (
                        f"【细类：{sg}\uff5cSKU：{sku}\uff5c品名：{_md_cell(tit, 60)}\uff5c"
                        f"店铺：{_md_cell(shop, 28)}】"
                    )
                    snippets.append(prefix + txt[:260])
                else:
                    snippets.append(
                        f"【细类：{gname}\uff5cSKU：{sku or '—'}】" + txt[:280]
                    )
                if len(snippets) >= 10:
                    break
        groups_out.append(
            {
                "group": gname,
                "effective_text_count": int(scen_ng),
                "scenario_distribution": dist[:18],
                "sample_text_snippets": snippets,
            }
        )
    if not groups_out:
        return {}
    return {"scenario_lexicon": lexicon, "groups": groups_out}


def build_comment_sentiment_llm_payload(
    texts: list[str],
    *,
    attributed_texts: list[str] | None = None,
    max_samples_positive: int = 16,
    max_samples_negative: int = 30,
    max_samples_mixed: int = 10,
    max_chars_per_review: int = 300,
    semantic_pool_max: int = 40,
    shuffle_seed: str = "",
) -> dict[str, Any]:
    """
    供大模型做正/负向语义归纳：附规则统计、关键词分桶抽样，以及 **sample_reviews_semantic_pool**
   （全量去重后的评价句确定性洗牌抽样，供模型结合语境自行判断褒贬）。

    ``sentiment_bucket_method`` 标明分桶依据为子串词表；条形图与 lexicon 仍与此口径一致，
    但正文归纳应以模型对 ``sample_reviews_semantic_pool`` 的整句理解为准。
    """
    pos_only_texts: list[str] = []
    neg_only_texts: list[str] = []
    mixed_texts: list[str] = []
    use_attr = (
        attributed_texts is not None
        and len(attributed_texts) == len(texts)
    )
    all_unique_disp: list[str] = []
    seen_unique: set[str] = set()
    for i, t in enumerate(texts):
        s = (t or "").strip()
        if not s:
            continue
        disp = (
            (attributed_texts[i] or s).strip()
            if use_attr
            else s
        )
        if disp and disp not in seen_unique:
            seen_unique.add(disp)
            all_unique_disp.append(disp)
        hp = any(k in s for k in _POS_CLASS)
        hn = any(k in s for k in _NEG_CLASS)
        if hp and hn:
            mixed_texts.append(disp)
        elif hp:
            pos_only_texts.append(disp)
        elif hn:
            neg_only_texts.append(disp)

    def _semantic_pool(seq: list[str], cap: int) -> list[str]:
        """去重列表的洗牌子样本；shuffle_seed 非空时按种子固定顺序以便同任务可复现。"""
        if not seq or cap <= 0:
            return []
        work = list(seq)
        if (shuffle_seed or "").strip():
            h = hashlib.sha256(shuffle_seed.encode("utf-8")).digest()
            rnd = random.Random(int.from_bytes(h[:8], "big"))
            rnd.shuffle(work)
        out: list[str] = []
        for raw in work:
            if len(raw) > max_chars_per_review:
                out.append(raw[:max_chars_per_review] + "…")
            else:
                out.append(raw)
            if len(out) >= cap:
                break
        return out

    semantic_pool = _semantic_pool(all_unique_disp, semantic_pool_max)

    def _sample(seq: list[str], cap: int) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in seq:
            if raw in seen:
                continue
            seen.add(raw)
            if len(raw) > max_chars_per_review:
                out.append(raw[:max_chars_per_review] + "…")
            else:
                out.append(raw)
            if len(out) >= cap:
                break
        return out

    lex = _comment_sentiment_lexicon(texts)
    pos_h = lex.get("positive_tone_lexeme_hits") or []
    neg_h = lex.get("negative_tone_lexeme_hits") or []
    pos_h_top = [x for x in pos_h[:12] if isinstance(x, dict)]
    neg_h_top = [x for x in neg_h[:12] if isinstance(x, dict)]
    return {
        "comment_sentiment_lexicon": lex,
        "positive_lexeme_hits_top": pos_h_top,
        "negative_lexeme_hits_top": neg_h_top,
        "sentiment_bucket_method": "keyword_substring_heuristic",
        "sample_reviews_semantic_pool": semantic_pool,
        "sample_reviews_positive_biased": _sample(pos_only_texts, max_samples_positive),
        "sample_reviews_negative_biased": _sample(neg_only_texts, max_samples_negative),
        "sample_reviews_mixed_tone": _sample(mixed_texts, max_samples_mixed),
    }


def _mermaid_pie_focus_keywords(hits: Counter[str], *, top_k: int = 8) -> str:
    """关注词全局 Top 的 Mermaid pie（便于渲染或导出工具识别）。"""
    top = hits.most_common(top_k)
    if not top:
        return ""
    rest = list(hits.most_common())
    if len(rest) > top_k:
        others_n = sum(n for _, n in rest[top_k:])
    else:
        others_n = 0
    lines = ["```mermaid", 'pie title 关注词命中次数（全局 Top，子串计数）']
    for w, n in top:
        if n <= 0:
            continue
        label = (w or "?").replace('"', "'").replace("\n", " ")[:18]
        lines.append(f'    "{label}（{n}）" : {n}')
    if others_n > 0:
        lines.append(f'    "其余词合计" : {others_n}')
    lines.append("```")
    return "\n".join(lines)


def _comment_scenario_counts(
    texts: list[str],
    scenario_groups: tuple[tuple[str, tuple[str, ...]], ...],
) -> tuple[Counter[str], int]:
    """每组统计「至少命中一个触发词」的条数。返回 (各组条数, 有效文本条数)。"""
    c: Counter[str] = Counter()
    n = len(texts)
    for blob in texts:
        for label, triggers in scenario_groups:
            if any(t in blob for t in triggers):
                c[label] += 1
    return c, n


def _text_hits_scenario_triggers(
    text: str,
    scenario_groups: tuple[tuple[str, tuple[str, ...]], ...],
) -> bool:
    blob = text or ""
    for _lbl, triggers in scenario_groups:
        if any(t in blob for t in triggers):
            return True
    return False


def _scenario_summary_bullets(counter: Counter[str], n_texts: int, top_k: int = 5) -> list[str]:
    if n_texts <= 0 or not counter:
        return []
    ordered = counter.most_common()
    lines: list[str] = []
    head = ordered[:top_k]
    parts = []
    for label, cnt in head:
        pct = 100.0 * cnt / n_texts
        parts.append(f"「{label}」约 **{cnt}** 条（占有效文本 **{pct:.0f}%**）")
    lines.append(
        "用户自述的用途/场景（基于预设词组，**非语义分类**）：" + "；".join(parts) + "。"
    )
    tail = [lbl for lbl, n in ordered[top_k:] if n > 0]
    if tail:
        lines.append(f"另有提及较少的场景标签：{'、'.join(tail)}。")
    return lines


def _sku_to_matrix_group_map(
    merged_rows: list[dict[str, str]], sku_header: str
) -> dict[str, str]:
    m: dict[str, str] = {}
    for row in merged_rows:
        sku = _cell(row, sku_header).strip()
        if not sku:
            continue
        gk = _competitor_matrix_group_key(row)
        if gk:
            m[sku] = gk
    return m


def _comment_text_units_for_matrix_group(
    gname: str,
    merged_rows: list[dict[str, str]],
    comment_rows_in_group: list[dict[str, str]],
    sku_header: str,
) -> list[str]:
    """某细类下的评价正文列表；无 flat 时用该细类合并行的 comment_preview。"""
    texts: list[str] = []
    for row in comment_rows_in_group:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if t:
            texts.append(t)
    if texts:
        return texts
    for row in merged_rows:
        if _competitor_matrix_group_key(row) != gname:
            continue
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            texts.append(p)
    return texts


def _group_keyword_hits(
    comment_rows_in_group: list[dict[str, str]],
    texts_fallback: list[str],
    *,
    focus_words: tuple[str, ...],
) -> Counter[str]:
    h = _comment_keyword_hits(comment_rows_in_group, focus_words)
    if h:
        return h
    if not texts_fallback:
        return Counter()
    blob = "\n".join(texts_fallback)
    c: Counter[str] = Counter()
    for w in focus_words:
        if len(w) < 2:
            continue
        n = blob.count(w)
        if n:
            c[w] += n
    return c


def _consumer_feedback_by_matrix_group(
    *,
    merged_rows: list[dict[str, str]],
    comment_rows: list[dict[str, str]],
    sku_header: str,
) -> list[tuple[str, list[dict[str, str]], list[str]]]:
    """
    与 §5 矩阵同序的细类列表；每项为 (细类名, 该类的 comments_flat 行, 用于场景统计的文本单元)。
    评价 SKU 不在深入样本时归入「未归类（评价 SKU 无对应深入样本）」。
    """
    if not merged_rows:
        if not comment_rows:
            return []
        texts = _iter_comment_text_units(comment_rows, [])
        return [
            (
                "未归类（无深入合并表）",
                list(comment_rows),
                texts,
            )
        ]

    sku_map = _sku_to_matrix_group_map(merged_rows, sku_header)
    merged_by_sku: dict[str, dict[str, str]] = {}
    for row in merged_rows:
        s = _cell(row, sku_header).strip()
        if s:
            merged_by_sku[s] = row
    by_g: dict[str, list[dict[str, str]]] = {}
    for row in comment_rows:
        sku = _cell(row, _COMMENT_CSV_SKU, "sku").strip()
        g = sku_map.get(sku)
        if g:
            by_g.setdefault(g, []).append(row)
            continue
        if sku and sku in merged_by_sku:
            # 深入样本存在但缺 detail_category_path（或路径无法解析为可读细类）：不参与按细类分析
            continue
        by_g.setdefault("未归类（评价 SKU 无对应深入样本）", []).append(row)

    out: list[tuple[str, list[dict[str, str]], list[str]]] = []
    used: set[str] = set()
    for gname, _ in _merged_rows_grouped_for_matrix(merged_rows):
        cr = by_g.get(gname, [])
        tu = _comment_text_units_for_matrix_group(
            gname, merged_rows, cr, sku_header
        )
        out.append((gname, cr, tu))
        used.add(gname)
    for gname, cr in sorted(by_g.items(), key=lambda x: (-len(x[1]), x[0])):
        if gname in used:
            continue
        tu = _comment_text_units_for_matrix_group(
            gname, merged_rows, cr, sku_header
        )
        out.append((gname, cr, tu))
    return out


def _run_batch_label(run_dir: Path) -> str:
    name = run_dir.name
    m = re.match(r"^(\d{8})_(\d{6})_", name)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return name


def _resolve_existing_run_dir(raw: str | Path | None) -> Path | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    p = Path(s).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    return p


def _infer_keyword(run_dir: Path, meta: dict[str, Any] | None) -> str:
    if meta:
        k = str(meta.get("keyword") or "").strip()
        if k:
            return k
    m = re.match(r"^\d{8}_\d{6}_(.+)$", run_dir.name)
    if m:
        return m.group(1).strip()
    return ""


def _pc_search_result_count_from_raw(
    run_dir: Path,
) -> tuple[int | None, str, list[int], int, int]:
    """
    从 ``pc_search_raw/*.json`` 读取 ``data.resultCount``（京东 PC 搜索接口返回的检索命中规模）。
    多文件时取众数；返回 (众数值, data.listKeyWord 首见值, 出现过的不同取值升序, 解析到的样本文件数)。
    """
    raw_dir = run_dir / "pc_search_raw"
    if not raw_dir.is_dir():
        return None, "", [], 0, 0
    counts: list[int] = []
    list_kw = ""
    n_files = 0
    for p in sorted(raw_dir.glob("*.json")):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeError):
            continue
        n_files += 1
        if not isinstance(obj, dict):
            continue
        data = obj.get("data")
        if not isinstance(data, dict):
            continue
        rc = data.get("resultCount")
        val: int | None = None
        if isinstance(rc, int) and not isinstance(rc, bool) and rc >= 0:
            val = rc
        elif isinstance(rc, str) and rc.strip().isdigit():
            val = int(rc.strip())
        if val is not None:
            counts.append(val)
        lk = data.get("listKeyWord")
        if isinstance(lk, str) and lk.strip() and not list_kw:
            list_kw = lk.strip()
    if not counts:
        return None, list_kw, [], n_files, 0
    consensus_rc, _freq = Counter(counts).most_common(1)[0]
    uniques = sorted(set(counts))
    return consensus_rc, list_kw, uniques, n_files, len(counts)


def _structure_names_for_pie_counter(row_names: list[str]) -> list[str]:
    """
    与 ``_counter_mix_top_rows_with_remainder`` / 列表品牌·店铺扇图同一套规则：
    按 strip 后的名称逐行保留一条，便于 ``_brand_cr`` 与饼图 Counter 一致。
    """
    return [(x or "").strip() for x in row_names if (x or "").strip()]


def _brand_cr(cnames: list[str]) -> tuple[float | None, float | None, str, str]:
    """按名称计数返回 (第一大主体份额, 前三合计份额, 头部标签, 头部占比展示字符串)。"""
    if not cnames:
        return None, None, "", ""
    cnt = Counter(cnames)
    total = sum(cnt.values())
    if total <= 0:
        return None, None, "", ""
    mc = cnt.most_common()
    top1_n = mc[0][1] if mc else 0
    top1 = mc[0][0] if mc else ""
    cr1 = top1_n / total
    top3_n = sum(n for _, n in mc[:3])
    cr3 = top3_n / total
    return cr1, cr3, top1, f"{100.0 * top1_n / total:.1f}%"


def _counter_mix_top_rows_with_remainder(
    row_names: list[str], *, top_n: int, remainder_label: str
) -> list[tuple[str, int]]:
    """
    与列表品牌/店铺扇图一致：按 strip 后的名称计数；``most_common(top_n)`` 未覆盖的长尾合并为
    ``remainder_label``，保证各块 count 之和等于可统计行数（与 ``_structure_names_for_pie_counter`` 总条数一致）。
    """
    c = Counter((x or "").strip() for x in row_names if (x or "").strip())
    if not c:
        return []
    total = sum(c.values())
    common = c.most_common(top_n)
    accounted = sum(v for _, v in common)
    rest = total - accounted
    out: list[tuple[str, int]] = list(common)
    if rest > 0:
        out.append((remainder_label, rest))
    return out


def _price_stats_extended(prices: list[float]) -> dict[str, Any]:
    if not prices:
        return {}
    out: dict[str, Any] = {
        "min": min(prices),
        "max": max(prices),
        "mean": statistics.mean(prices),
        "n": len(prices),
    }
    if len(prices) >= 2:
        out["stdev"] = statistics.stdev(prices)
    if len(prices) >= 2:
        out["median"] = statistics.median(prices)
    if len(prices) >= 4:
        s = sorted(prices)
        n = len(s)
        mid = n // 2
        lower = s[:mid] if n % 2 else s[:mid]
        upper = s[mid + 1 :] if n % 2 else s[mid:]
        out["q1"] = statistics.median(lower) if lower else s[0]
        out["q3"] = statistics.median(upper) if upper else s[-1]
    return out


def _search_list_proxies(rows: list[dict[str, str]]) -> dict[str, Any]:
    """
    基于 pc_search_export 的「列表可见度」指标，**不是**全渠道零售额或 TAM。
    """
    sku_k = JD_SEARCH_CSV_HEADERS["sku_id"]
    shop_k = JD_SEARCH_CSV_HEADERS["shop_name"]
    page_k = JD_SEARCH_CSV_HEADERS["page"]
    cat_k = JD_SEARCH_CSV_HEADERS["leaf_category"]
    skus: set[str] = set()
    shops: set[str] = set()
    pages: set[str] = set()
    cats: set[str] = set()
    for r in rows:
        s = _cell(r, sku_k)
        if s:
            skus.add(s)
        sh = _cell(r, shop_k)
        if sh:
            shops.add(sh)
        pg = _cell(r, page_k)
        if pg:
            pages.add(pg)
        c = _cell(r, cat_k)
        if c:
            cats.add(c)
    prices = _collect_prices(rows)
    pst = _price_stats_extended(prices)
    return {
        "total_rows": len(rows),
        "unique_skus": len(skus),
        "unique_shops": len(shops),
        "unique_pages": len(pages),
        "page_span": (min((int(p) for p in pages if p.isdigit()), default=None), max((int(p) for p in pages if p.isdigit()), default=None)),
        "unique_leaf_cats": len(cats),
        "list_price_stats": pst,
    }


def _category_token_meaningless(seg: str) -> bool:
    """纯数字类目 ID、空串或疑似内部编码的段，不宜直接作为矩阵分组展示名。"""
    t = (seg or "").strip()
    if not t:
        return True
    if t.isdigit():
        return True
    if len(t) >= 14 and re.fullmatch(r"[A-Za-z0-9_\-]+", t):
        return True
    return False


def _matrix_display_segment_from_parts(parts: list[str]) -> str | None:
    """
    与历史逻辑一致的主选段；若该段无意义则自右向左找第一段可读文本
    （避免「仅类目码」或中间段为数字 ID 时整组成品名式乱桶）。
    """
    if not parts:
        return None
    if len(parts) >= 4:
        preferred = parts[-2]
    elif len(parts) >= 3:
        preferred = parts[1]
    elif len(parts) >= 2:
        preferred = parts[1]
    else:
        preferred = parts[0]
    order: list[str] = []
    if preferred:
        order.append(preferred)
    if len(parts) >= 2:
        order.append(parts[-2])
    order.append(parts[-1])
    order.extend(reversed(parts))
    seen: set[str] = set()
    for cand in order:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        if not _category_token_meaningless(cand):
            return cand.strip()
    return None


def _matrix_group_label_from_path(path: str) -> str:
    """由 ``detail_category_path`` 文本解析细类展示名；空或无可读段则返回空串。"""
    t = (path or "").strip()
    if not t:
        return ""
    parts = [p.strip() for p in t.replace("＞", ">").split(">") if p.strip()]
    if not parts:
        return ""
    key = _matrix_display_segment_from_parts(parts)
    return (key[:80] if key else "")


def _matrix_group_label_from_detail_path(row: dict[str, str]) -> str:
    return _matrix_group_label_from_path(_detail_category_path_cell(row))


def _competitor_matrix_group_key(row: dict[str, str]) -> str:
    """
    竞品矩阵分组：§5 / §8 / 统计图共用。
    **仅**依据 ``detail_category_path``；列为空或路径段均为无意义编码时不参与矩阵（返回空串）。
    """
    return _matrix_group_label_from_detail_path(row)


def _merged_rows_grouped_for_matrix(
    merged_rows: list[dict[str, str]],
) -> list[tuple[str, list[dict[str, str]]]]:
    buckets: dict[str, list[dict[str, str]]] = {}
    for row in merged_rows:
        k = _competitor_matrix_group_key(row)
        if not k:
            continue
        buckets.setdefault(k, []).append(row)

    def sort_key(item: tuple[str, list[dict[str, str]]]) -> tuple[int, int, str]:
        name, rows = item
        miss = name.startswith("未归类")
        return (1 if miss else 0, -len(rows), name)

    return sorted(buckets.items(), key=sort_key)


def _category_mix(
    rows: list[dict[str, str]], *, top_k: int = 12
) -> list[tuple[str, int]]:
    """
    按「可读细类标签」统计 SKU 分布（与 §5 ``_competitor_matrix_group_key`` 同源）；
    仅含 ``detail_category_path`` 可解析为展示名的行。

    返回 ``most_common(top_k)``，并将未列入 Top K 的款数合并为「（其余细类）」，
    使各块 SKU 数之和等于有效矩阵 SKU 总数（与扇形图、简报 ``category_mix_top`` 一致）。
    """
    labels: list[str] = []
    for r in rows:
        k = _matrix_group_label_from_detail_path(r)
        if k:
            labels.append(k)
    if not labels:
        return []
    c = Counter(labels)
    common = c.most_common(top_k)
    accounted = sum(v for _, v in common)
    total = sum(c.values())
    rest = total - accounted
    out: list[tuple[str, int]] = list(common)
    if rest > 0:
        out.append(("（其余细类）", rest))
    return out


def _structure_shops(rows: list[dict[str, str]], *, list_export: bool) -> list[str]:
    if list_export:
        return [
            _cell(r, JD_SEARCH_CSV_HEADERS["shop_name"], _LEGACY_SHOP_NAME_KEY)
            for r in rows
            if _cell(r, JD_SEARCH_CSV_HEADERS["shop_name"], _LEGACY_SHOP_NAME_KEY)
        ]
    out: list[str] = []
    for r in rows:
        s = _cell(r, *_MERGED_SHOP_CELL_KEYS)
        if s:
            out.append(s)
    return out


def _structure_brands(rows: list[dict[str, str]], *, list_export: bool) -> list[str]:
    if list_export:
        return [
            _cell(r, _LIST_BRAND_TITLE_HEADER, _LEGACY_LIST_BRAND_TITLE_KEY)
            for r in rows
            if _cell(r, _LIST_BRAND_TITLE_HEADER, _LEGACY_LIST_BRAND_TITLE_KEY)
        ]
    return [
        _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
        for r in rows
        if _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
    ]


def _is_ingredient_url_blob(s: str) -> bool:
    """详情主图 URL 串（分号分隔）或单列以 http 开头。"""
    t = (s or "").strip()
    if not t:
        return False
    if t.startswith(("http://", "https://")):
        return True
    head = t[:400]
    if ("https://" in head or "http://" in head) and (
        ";" in t or len(t) > 180 or t.count("http") >= 2
    ):
        return True
    return False


def _ingredients_from_product_attributes(attrs: str) -> str:
    m = re.search(r"配料(?:表)?[:：]\s*([^;；]+)", attrs or "")
    return m.group(1).strip() if m else ""


def _ingredients_single_line(s: str) -> str:
    """与 ``AI_crawler.normalize_ingredients_text_for_csv`` 一致：多行配料压成一行（行间 ``；``），便于表格/CSV。"""
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    if len(lines) <= 1:
        return lines[0] if lines else ""
    return "；".join(lines)


def _matrix_ingredients_cell(row: dict[str, str], *, max_len: int = 420) -> str:
    """
    优先 ``detail_body_ingredients``（配料 OCR/文本）；旧合并表可能为 ``detail_body_image_urls``。
    若为 URL 串则尝试 ``detail_product_attributes`` 中的「配料/配料表：」片段。
    """
    raw = _cell(
        row,
        MERGED_FIELD_TO_CSV_HEADER["detail_body_ingredients"],
        "detail_body_ingredients",
        "detail_body_image_urls",
    )
    if raw and not _is_ingredient_url_blob(raw):
        return _md_cell(_ingredients_single_line(raw), max_len)
    from_attr = _ingredients_from_product_attributes(
        _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["detail_product_attributes"],
            "detail_product_attributes",
        )
    )
    if from_attr:
        return _md_cell(from_attr, max_len)
    if raw and _is_ingredient_url_blob(raw):
        return _md_cell(
            "（详情长图链接，无配料正文；可在采集侧开启配料识别后重新跑批次）",
            max_len,
        )
    return "—"


def _competitor_matrix_md_line(
    row: dict[str, str], *, sku_header: str, title_h: str
) -> str:
    sku = _md_cell(_cell(row, sku_header), 14)
    title = _md_cell(_cell(row, title_h), 56)
    brand = _md_cell(
        _cell(row, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand"), 16
    )
    pj = _md_cell(_cell(row, *_LIST_SHOW_PRICE_CELL_KEYS), 10)
    df = _md_cell(_cell(row, *_DETAIL_PRICE_FINAL_CSV_KEYS), 10)
    shop = _md_cell(_cell(row, *_MERGED_SHOP_CELL_KEYS), 22)
    sell = _md_cell(_cell(row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY), 36)
    rank = _md_cell(
        _cell(row, _RANK_TAGLINE_KEY, _LEGACY_RANK_TAGLINE_KEY), 28
    )
    cat = _md_cell(_detail_category_path_cell(row), 24)
    ing = _matrix_ingredients_cell(row)
    ts_eff = merged_csv_effective_total_sales(row)
    cc = _md_cell(ts_eff or _cell(row, *_COMMENT_FUZZ_KEYS), 14)
    prev = _md_cell(
        _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        ),
        72,
    )
    return (
        f"| {sku} | {title} | {brand} | {pj} | {df} | {shop} | {sell} | {rank} | "
        f"{cat} | {ing} | {cc} | {prev} |"
    )


def _strategy_hints(
    *,
    cr1: float | None,
    pst: dict[str, Any],
    hits: Counter[str],
    n_comments: int,
    scen_counts: Counter[str],
    scen_n_texts: int,
) -> list[str]:
    """基于规则的「提示性」结论，均标注待验证。"""
    hints: list[str] = []
    if cr1 is not None and cr1 >= 0.45:
        hints.append(
            "样本内品牌集中度较高（第一大品牌份额偏高），头部玩家占据显著曝光；新原料/解决方案宜明确差异化价值主张（**需线下渠道与招商信息交叉验证**）。"
        )
    elif cr1 is not None and cr1 < 0.25:
        hints.append(
            "样本内品牌较分散，品类或关键词下竞争格局未固化，存在定位与叙事空间（**需扩大样本页数与关键词矩阵验证**）。"
        )
    if pst.get("stdev") and pst.get("mean") and pst["mean"] > 0:
        cv = pst["stdev"] / pst["mean"]
        if cv > 0.35:
            hints.append(
                "价格离散度较高，同时存在偏低价与偏高价陈列，可分别对标「性价比带」与「品质/功能带」竞品（**终端到手价受促销影响，非成本结构**）。"
            )
    if hits:
        top = hits.most_common(3)
        top_s = "、".join(w for w, _ in top)
        hints.append(
            f"评价文本中「{top_s}」等主题出现较多，可作为消费者沟通与产品卖点的假设输入（**非严格主题模型，建议人工抽样复核**）。"
        )
    if n_comments < 5:
        hints.append(
            "有效评价样本偏少，消费者洞察部分仅作方向参考，正式结论建议加大 SKU 数或评论分页。"
        )
    if scen_n_texts >= 5 and scen_counts:
        top_lbl, top_n = scen_counts.most_common(1)[0]
        share = top_n / scen_n_texts
        if share >= 0.25:
            hints.append(
                f"用途/场景中「{top_lbl}」在约 {100 * share:.0f}% 的有效评价自述中出现，可作为沟通场景与卖点的优先假设（**词组规则，建议抽样核对原句**）。"
            )
    if not hints:
        hints.append(
            "当前样本下自动规则未触发强信号；请结合业务目标人工解读对比矩阵与原始 CSV。"
        )
    return hints


def _embed_chart(run_dir: Path, filename: str, caption: str = "") -> list[str]:
    """若 ``report_assets/<filename>`` 存在则返回插图 Markdown 片段。"""
    if not (run_dir / "report_assets" / filename).is_file():
        return []
    cap = (caption or "").strip()
    out: list[str] = []
    if cap:
        out.append(f"*{cap}*")
        out.append("")
    out.append(f"![](report_assets/{filename})")
    out.append("")
    return out


def _scenario_group_asset_slug(group: str, index: int) -> str:
    """与 ``pipeline.reporting.charts`` 中场景分组图文件名规则一致（勿改格式）。"""
    raw = (group or "").strip()
    core = re.sub(r"[^\w\u4e00-\u9fff-]", "", raw)[:20]
    if not core:
        core = "group"
    return f"i{index:02d}_{core}"


def _focus_scenario_combo_bar_filename(group: str, index: int) -> str:
    """关注词 + 使用场景并排条形图（与 ``pipeline.reporting.charts.save_combo_focus_scenario_bar`` 同源）。"""
    slug = _scenario_group_asset_slug(group, index)
    return f"chart_focus_and_scenarios_bar__{slug}.png"


def _matrix_prices_sales_chart_filename(group: str, index: int) -> str:
    """与 ``pipeline.reporting.charts.generate_report_charts`` 中 ``chart_matrix_prices_sales__*`` 一致。"""
    slug = _scenario_group_asset_slug(group, index)
    return f"chart_matrix_prices_sales__{slug}.png"


def _lines_4_reading_brand(
    *,
    cr1: float | None,
    cr3: float | None,
    top: str,
    brand_rows_n: int,
    n_structure: int,
) -> list[str]:
    if cr1 is None or not (top or "").strip():
        return []
    lines = [
        "",
        "**数据解读（规则摘要）**：",
        "",
        f"- 在含品牌字段的 **{brand_rows_n}** 条列表行（占本章结构样本 **{n_structure}** 行）中，"
        f"「{_md_cell(top.strip(), 36)}」曝光约占 **{100 * cr1:.1f}%**（按行计，同一 SKU 多行会重复计）。",
    ]
    if cr3 is not None:
        lines.append(
            f"- 前三品牌合计约 **{100 * cr3:.1f}%**；若该比例偏高，说明搜索页品牌集中度高，"
            "新品需搭配清晰的差异定位与资源投放，避免与头部在泛词下正面撞车。"
        )
    lines.append("")
    return lines


def _lines_4_reading_shop(
    *,
    cr1: float | None,
    cr3: float | None,
    top: str,
    shop_rows_n: int,
    n_structure: int,
) -> list[str]:
    if not shop_rows_n:
        return []
    lines = [
        "",
        "**数据解读（规则摘要）**：",
        "",
        f"- 含店铺名的列表行共 **{shop_rows_n}** 条（结构样本 **{n_structure}** 行），反映搜索曝光下的店铺格局。",
    ]
    if cr1 is not None and (top or "").strip():
        lines.append(
            f"- 第一大店铺「{_md_cell(top.strip(), 40)}」约占 **{100 * cr1:.1f}%**；"
            "该指标刻画的是**列表可见度**而非销量，适合用于判断货架被哪些店铺占据。"
        )
    if cr3 is not None:
        lines.append(
            f"- 前三店铺合计约 **{100 * cr3:.1f}%**；若集中度高，可考虑从店铺矩阵、旗舰店/专营店布局等角度拆解竞争。"
        )
    lines.append("")
    return lines


def build_competitor_markdown(
    *,
    run_dir: Path,
    keyword: str,
    merged_rows: list[dict[str, str]],
    search_export_rows: list[dict[str, str]],
    comment_rows: list[dict[str, str]],
    meta: dict[str, Any] | None,
    report_config: dict[str, Any] | None = None,
    llm_sentiment_section_md: str | None = None,
    llm_matrix_section_md: str | None = None,
    llm_price_groups_section_md: str | None = None,
    llm_promo_groups_section_md: str | None = None,
    llm_scenario_groups_section_md: str | None = None,
    llm_comment_groups_section_md: str | None = None,
    llm_strategy_opportunities_section_md: str | None = None,
) -> str:
    focus_words, scenario_groups, external_rows = resolve_report_tuning(report_config)
    sku_header = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    batch = _run_batch_label(run_dir)
    n_sku = len(merged_rows)
    n_cmt = len(comment_rows)
    n_sku_pathed = sum(1 for r in merged_rows if _detail_category_path_cell(r))
    n_sku_matrix = sum(1 for r in merged_rows if _competitor_matrix_group_key(r))

    list_export = len(search_export_rows) > 0
    structure_rows = search_export_rows if list_export else merged_rows
    n_structure = len(structure_rows)
    shops_s = _structure_shops(structure_rows, list_export=list_export)
    brands_s = _structure_brands(structure_rows, list_export=list_export)
    shops_for_cr = _structure_names_for_pie_counter(shops_s)
    brands_for_cr = _structure_names_for_pie_counter(brands_s)
    cr1_shop, cr3_shop, top_shop_s, _ = _brand_cr(shops_for_cr)
    cr1_list_brand, cr3_list_brand, top_list_brand, _ = _brand_cr(brands_for_cr)
    # §4.3 类目分布：深入合并表口径，与 §5 竞品矩阵一致（非搜索列表行）
    cm_structure = _category_mix(merged_rows, top_k=12)
    min_brand_rows = max(5, int(0.02 * n_structure)) if n_structure else 5

    brands_deep = [
        _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
        for r in merged_rows
        if _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
    ]
    cr1_deep, cr3_deep, top_brand_deep, _top_share_deep = _brand_cr(brands_deep)
    cr1_hints = (
        cr1_shop if list_export and cr1_shop is not None else cr1_deep
    )

    pst_merged = _price_stats_extended(_collect_prices(merged_rows))
    pst_list = (
        _price_stats_extended(_collect_prices(search_export_rows))
        if list_export
        else {}
    )
    # 价格分析（§2 要点、§6、策略提示）：优先「列表全量」；无列表或无解析价时再用合并表深入样本
    pst = (
        pst_list
        if list_export and pst_list.get("n", 0) > 0
        else pst_merged
    )
    price_analysis_basis_cn = (
        f"PC 搜索列表导出共 **{len(search_export_rows)}** 行中的展示价（标价/券后等）"
        if list_export and pst_list.get("n", 0) > 0
        else f"已深入抓取的 **{n_sku}** 个 SKU 合并数据中的展示价"
    )
    promo_rows = (
        search_export_rows
        if list_export and pst_list.get("n", 0) > 0
        else merged_rows
    )
    promo_sig = _analyze_price_promotions(promo_rows)

    hits = _comment_keyword_hits(comment_rows, focus_words)
    if not hits:
        blob = _merge_comment_previews(merged_rows)
        for w in focus_words:
            if len(w) < 2:
                continue
            n = blob.count(w)
            if n:
                hits[w] += n

    comment_texts = _iter_comment_text_units(comment_rows, merged_rows)
    sentiment_lex = _comment_sentiment_lexicon(comment_texts)
    scen_counts, scen_n_texts = _comment_scenario_counts(
        comment_texts, scenario_groups
    )

    feedback_groups = _consumer_feedback_by_matrix_group(
        merged_rows=merged_rows,
        comment_rows=comment_rows,
        sku_header=sku_header,
    )
    matrix_groups_for_exec = _merged_rows_grouped_for_matrix(merged_rows)
    multi_feedback_cat = len(matrix_groups_for_exec) >= 2

    (
        api_rc,
        api_list_kw,
        api_rc_uniques,
        api_raw_json_n,
        api_rc_n_values,
    ) = _pc_search_result_count_from_raw(run_dir)

    has_external_market = bool(external_rows)

    lines: list[str] = [
        f"# 竞品分析报告（京东 PC 渠道）",
        "",
        f"> **监测主题**：{keyword}  ",
        f"> **数据批次**：{batch}  ",
        f"> **报告生成**：自动化草稿，**仅供内部研讨**，不构成市场承诺或投资建议。",
        "",
        "---",
        "",
        "## 一、研究范围、数据来源与局限",
        "",
        "### 1.1 研究范围",
        "",
        f"- **搜索关键词**：「{keyword}」",
        f"- **分析对象**：流水线拉取的 **{n_sku}** 个 SKU（搜索排序靠前子样本，非全站普查）。",
    ]
    if n_sku:
        n_sku_nop = n_sku - n_sku_pathed
        n_sku_unparsed = n_sku_pathed - n_sku_matrix
        lines.append(
            f"- **细类分析口径**：**{n_sku_matrix}** 个 SKU 具备可参与 **§5～§8** 的商详 "
            f"``detail_category_path``（且路径可解析为可读细类）；另有 **{n_sku_nop}** 个缺该字段、"
            f"**{n_sku_unparsed}** 个有路径但无可读细类段，**未纳入**细类矩阵与按细类评价统计。"
        )
    if meta:
        lines.append(
            f"- **搜索列表页**：逻辑第 **{meta.get('page_start')}** 页至第 **{meta.get('page_to')}** 页；"
            f"搜索导出共 **{meta.get('pc_search_export_rows', '—')}** 行（含未深入拉详情的商品）。"
        )
    lines.extend(
        [
            "",
            "### 1.2 数据来源",
            "",
            "- **渠道**：京东 PC 端公开商品列表、商详与评价等可访问数据。",
            "- **可追溯**：原始表格与接口响应保存在本批次任务输出目录，供内部复核；对外分享请脱敏。",
            "",
            "### 1.3 方法说明（指标含义）",
            "",
            "- **价格**：自页面「标价 / 券后价 / 详情价」等抽取的**展示价**，含促销与规格差异，**不等于**出厂价或成本。**第六章** 在具备可用的搜索列表导出时，优先以**列表全量**统计；否则使用**已深入 SKU** 的合并数据；**§6.1** 归纳标价与券后价差及卖点/腰带中的**活动话术信号**。",
            "- **品牌/店铺集中度（第四章）**：有列表全量时按列表行计店铺与品牌占比；无列表导出时按深入 SKU 合并表估算。",
            "- **评价主题词**：对评价正文做**预设词表子串计数**，非分词主题模型，适合扫方向，**需抽样人工验证**。",
            "- **用途/场景**：对每条评价独立判断是否命中预设场景词；一条可计入多个场景，统计的是「提及该场景的评价条数」而非用户数。",
            "- **用户画像（第八章）**：正负面粗判含**口语短语**级摘录；关注词与场景**仅按细类**以**同图左右并列**展示（左为关注词命中次数，右为场景占有效文本 **%**）；见 §8.3。",
            "- **细类划分（§5～§8）**：**仅**依据合并表 ``detail_category_path``；该列为空或无法解析出可读细类段的 SKU **不参与**竞品矩阵与按细类评价统计（相关评价条亦**不进入**按细类图表）。",
            "- **检索结果规模**：来自京东 PC 搜索返回的「结果条数」类指标，表示平台侧申报的匹配数量级，**不等于**动销、库存或独立 SKU 数。",
            "",
            "### 1.4 主要局限",
            "",
            "- 仅覆盖 **京东 PC**，不含天猫、抖音、线下、B2B 原料端。",
            "- 样本量由本次抓取上限与搜索页数决定，**结论外推需谨慎**。",
            "- 详情配料与宣称以页面展示为准，**与真实配方可能不一致**（合规与实测另议）。",
            (
                "- **行业零售额、TAM、CAGR 等**：无法从本批次数据推导；本报告已纳入任务中配置的第三方摘录，见 **§3.5**。"
                if has_external_market
                else "- **行业零售额、TAM、CAGR 等**：无法从本批次数据推导；本报告未纳入外部摘录（可在任务报告调参中维护市场信息表）。"
            ),
            "",
            "---",
            "",
            "## 二、执行摘要（要点）",
            "",
        ]
    )

    exec_bullets: list[str] = []
    exec_bullets.append(
        f"在关键词「{keyword}」下，本次深入分析 **{n_sku}** 个 SKU，关联评价文本 **{n_cmt}** 条。"
    )
    if list_export and cr1_shop is not None and top_shop_s:
        src = f"列表全量 **{n_structure}** 行"
        if cr3_shop is not None:
            exec_bullets.append(
                f"竞争结构（{src}，§4）：**店铺** 第一大店铺份额 ≈ **{100 * cr1_shop:.1f}%**（「{top_shop_s}」），"
                f"前三店铺合计份额 ≈ **{100 * cr3_shop:.1f}%**（按列表行计，同一 SKU 多行会重复计）。"
            )
        else:
            exec_bullets.append(
                f"竞争结构（{src}，§4）：**店铺** 第一大店铺份额 ≈ **{100 * cr1_shop:.1f}%**（「{top_shop_s}」）。"
            )
    elif not list_export and cr1_deep is not None and top_brand_deep:
        if cr3_deep is not None:
            exec_bullets.append(
                f"竞争结构（无列表导出，§4 用深入合并表）：**品牌** 第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」），"
                f"前三品牌合计份额 ≈ **{100 * cr3_deep:.1f}%**。"
            )
        else:
            exec_bullets.append(
                f"竞争结构（无列表导出，§4 用深入合并表）：**品牌** 第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」）。"
            )
    if (
        list_export
        and len(brands_for_cr) >= min_brand_rows
        and cr1_list_brand is not None
        and top_list_brand
    ):
        if cr3_list_brand is not None:
            exec_bullets.append(
                f"同批列表中**品牌信息有效** **{len(brands_for_cr)}** 条：**品牌** 第一大品牌份额 ≈ **{100 * cr1_list_brand:.1f}%**（「{top_list_brand}」），"
                f"前三品牌合计份额 ≈ **{100 * cr3_list_brand:.1f}%**。"
            )
        else:
            exec_bullets.append(
                f"同批列表中**品牌信息有效** **{len(brands_for_cr)}** 条：**品牌** 第一大品牌份额 ≈ **{100 * cr1_list_brand:.1f}%**（「{top_list_brand}」）。"
            )
    elif list_export and cr1_deep is not None and top_brand_deep and not brands_for_cr:
        exec_bullets.append(
            f"列表导出缺少品牌标题字段，**深入 {n_sku} SKU** 商详品牌第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」），供与 §5 矩阵对照。"
        )
    if pst:
        price_src_short = (
            "（列表全量）"
            if list_export and pst_list.get("n", 0) > 0
            else "（深入样本）"
        )
        exec_bullets.append(
            f"展示价格{price_src_short}：可解析价格 **{pst['n']}** 个观测，区间约 **{pst['min']:.2f}～{pst['max']:.2f}** 元，"
            f"中位数 **{pst.get('median', pst['mean']):.2f}** 元。"
        )
        wb = int(promo_sig.get("rows_with_both_list_and_coupon") or 0)
        sh = promo_sig.get("share_coupon_below_list_when_both")
        med = promo_sig.get("median_discount_pct_when_coupon_below")
        if wb >= 3 and isinstance(sh, (int, float)) and sh >= 0.08 and med is not None:
            exec_bullets.append(
                f"列表侧约 **{100.0 * float(sh):.0f}%** 可对齐行呈现「券后/到手」**低于**「标价」，展示价差中位数约 **{float(med):.1f}%**（**§6.1** 活动与话术摘录）。"
            )
    if multi_feedback_cat and (hits or scen_n_texts > 0):
        exec_bullets.append(
            "评价侧写（关注词、用途/场景）已按 **§5 同款细类** 分节，见 **§8.3**（同图并列）。"
        )
    elif hits:
        top3 = "、".join(f"「{w}」({n})" for w, n in hits.most_common(3))
        exec_bullets.append(f"评价侧写（词频）：{top3}。")
    if scen_n_texts > 0 and scen_counts and not multi_feedback_cat:
        top_s = scen_counts.most_common(4)
        frag = "；".join(f"{lbl} **{n}** 条" for lbl, n in top_s)
        exec_bullets.append(f"用途/场景（评价自述，可多选）：{frag}（有效文本 **{scen_n_texts}** 条）。")
    if api_rc is not None:
        exec_bullets.append(
            f"PC 搜索返回的检索结果规模约 **{api_rc:,}**（站内匹配条数量级，见 §3.2；非零售额口径）。"
        )
    for b in exec_bullets:
        lines.append(f"- {b}")
    if not exec_bullets:
        lines.append("- 当前批次可汇总要点较少（以正文各节实际输出为准）。")

    proxy = _search_list_proxies(search_export_rows) if search_export_rows else {}
    lines.extend(["", "---", "", "## 三、整体市场观察（渠道可见度 proxy · 非官方规模）", ""])
    lines.extend(
        [
            "### 3.1 与「市场规模」的区别",
            "",
            "- **官方/行业市场规模**（如全国零售额、品类增速、渗透率）通常来自 **Euromonitor、行业协会、上市公司年报、券商研报** 等；**不能**用京东搜索返回条数或 SKU 数直接等同。",
            "- **§3.2** 使用搜索接口返回的**检索结果规模**字段；**§3.3～3.4** 描述本次导出的列表行、去重 SKU/店铺及列表价，用作 **proxy（参照）**，外推全市场需谨慎。",
            "",
            "### 3.2 接口返回的检索规模",
            "",
        ]
    )
    if api_rc is not None:
        lines.append(
            f"- 根据本批次保存的搜索原始响应解析：监测词「**{keyword}**」下，平台申报的检索匹配规模约 **{api_rc:,}**。"
        )
        if api_list_kw:
            lines.append(
                f"- 同批响应中的列表关键词：**{api_list_kw}**（可与监测词对照是否一致）。"
            )
        if len(api_rc_uniques) > 1:
            nums = "、".join(f"{u:,}" for u in api_rc_uniques)
            lines.append(
                f"- 注：多份原始响应中该规模字段曾出现不同取值（{nums}），正文取**众数** **{api_rc:,}**（共 {api_rc_n_values} 次有效读取）。"
            )
        elif api_raw_json_n > 0:
            lines.append(
                f"- 已扫描 **{api_raw_json_n}** 份原始响应并完成读取。"
            )
        lines.extend(
            [
                "- **含义**：平台对该关键词给出的**检索匹配条数量级**，用于感受站内商品池「宽度」；可能含不同类目/规格条目，**不等于**独立 SKU 数、动销或 GMV，且会随索引与运营策略变化。",
                "",
            ]
        )
    else:
        lines.append(
            "*未能从本批次搜索原始响应中解析到有效的检索规模字段（目录缺失、无可用响应或字段为空）。*"
        )
        lines.append("")

    lines.extend(["### 3.3 搜索列表规模（本次抓取范围内的可见 SKU / 店铺）", ""])
    if proxy.get("total_rows", 0) > 0:
        pmin, pmax = proxy.get("page_span") or (None, None)
        span_txt = (
            f"页码（去重）约 **{pmin}～{pmax}** 页"
            if pmin is not None and pmax is not None
            else "页码字段缺失或无法解析"
        )
        lines.extend(
            [
                f"- **列表导出行数**：**{proxy['total_rows']}** 行。",
                f"- **去重 SKU 数**：**{proxy['unique_skus']}**；**去重店铺数**：**{proxy['unique_shops']}**；{span_txt}。",
                f"- **列表中去重叶子类目代码/片段数**（粗略）：**{proxy['unique_leaf_cats']}**（同一关键词下品类宽度 proxy）。",
                "",
            ]
        )
        lpst = proxy.get("list_price_stats") or {}
        lines.extend(["### 3.4 列表端展示价（全导出，非仅深入样本）", ""])
        if lpst:
            lines.extend(
                [
                    f"- 自列表「标价 / 券后价」解析到 **{lpst['n']}** 个数值价；"
                    f"区间约 **{lpst['min']:.2f}～{lpst['max']:.2f}** 元，"
                    f"中位数 **{float(lpst.get('median', lpst['mean'])):.2f}** 元。",
                    "- **说明**：第六章价格统计表已与上表同源（均为列表全量，条件满足时）；若正文第六章标注为合并表样本，则因无可用列表价而退化。深入 SKU 的详情价可与列表价对照。",
                    "",
                ]
            )
        else:
            lines.append("*列表导出中未能解析出数值价格。*")
            lines.append("")
    else:
        lines.append(
            "*未读到可用的搜索列表导出或文件为空；§3.3～3.4 无列表侧数据。*"
        )
        lines.append("")
        lines.extend(["### 3.4 列表端展示价（全导出）", "", "*无列表数据。*", ""])

    if external_rows:
        lines.extend(
            [
                "### 3.5 外部市场规模与行业信息（运行配置摘录）",
                "",
                "以下为本次任务报告调参中维护的**第三方市场摘录**，可与 §3.2 检索规模及 §3.3～3.4 列表参照对照使用；口径与真实性以原出处为准。",
                "",
                "| 指标 | 数值与口径 | 来源 | 年份 |",
                "| --- | --- | --- | --- |",
            ]
        )
        for a, b, c, d in external_rows:
            lines.append(
                f"| {_md_cell(a, 40)} | {_md_cell(b, 48)} | {_md_cell(c, 36)} | {_md_cell(d, 12)} |"
            )
        lines.append("")

    ch4_heading = (
        "## 四、市场与竞争结构（PC 搜索列表全量）"
        if list_export
        else "## 四、市场与竞争结构（深入合并表 · 无列表导出）"
    )
    lines.extend(["", "---", "", ch4_heading, ""])
    if list_export:
        lines.append(
            f"基于**搜索列表导出**共 **{n_structure}** 行，与 §3.3 一致；"
            f"集中度按**列表行**计数（同一 SKU 多次曝光则重复计）。"
        )
    else:
        lines.append(
            f"*未读到可用列表全量行，以下退化为**深入 SKU 合并样本** **{n_structure}** 行。*"
        )
    lines.append("")

    lines.extend(["### 4.1 品牌分布与集中度", ""])
    brand_rows_n = len(brands_for_cr)
    show_list_brand_cr = list_export and brand_rows_n >= min_brand_rows
    show_merged_brand_cr = not list_export and brand_rows_n > 0
    if (show_list_brand_cr or show_merged_brand_cr) and cr1_list_brand is not None:
        lines.extend(
            _embed_chart(
                run_dir,
                "chart_brand_rows_pie.png",
                "品牌列表曝光占比（扇形图；按 strip 后品牌名计数、与结构化摘要 ``list_brand_mix_top`` 同源；"
                "长尾并入「（其余品牌）」；扇形内再合并为「其他」）",
            )
        )
        lines.extend(
            _lines_4_reading_brand(
                cr1=cr1_list_brand,
                cr3=cr3_list_brand,
                top=top_list_brand or "",
                brand_rows_n=brand_rows_n,
                n_structure=n_structure,
            )
        )
        lines.append(
            "*更细的品牌行数分布见本任务「结构化摘要」数据包。*"
        )
    elif list_export:
        lines.append(
            f"*列表导出中店铺/品牌标题有效 **{brand_rows_n}** 条，"
            f"低于建议阈值（≥{min_brand_rows}），品牌集中度未展开。**店铺结构见 §4.2**；"
            f"商详品牌在 **§5**。*"
        )
    else:
        lines.append("*深入子样本无可用品牌字段。*")
    lines.append("")

    lines.extend(["### 4.2 店铺分布与集中度", ""])
    shop_rows_n = len(shops_for_cr)
    if shop_rows_n:
        lines.extend(
            _embed_chart(
                run_dir,
                "chart_shop_rows_pie.png",
                "店铺列表曝光占比（扇形图；按 strip 后店铺名计数、与结构化摘要 ``list_shop_mix_top`` 同源；"
                "长尾并入「（其余店铺）」；扇形内再合并为「其他」）",
            )
        )
        lines.extend(
            _lines_4_reading_shop(
                cr1=cr1_shop,
                cr3=cr3_shop,
                top=top_shop_s or "",
                shop_rows_n=shop_rows_n,
                n_structure=n_structure,
            )
        )
        lines.append(
            "*更细的店铺行数分布见本任务「结构化摘要」数据包。*"
        )
    else:
        lines.append("*无店铺字段。*")
    lines.append("")

    lines.extend(["### 4.3 细分类目分布（深入合并表 · 与 §5 矩阵同口径）", ""])
    if cm_structure and n_sku_matrix > 0:
        lines.extend(
            _embed_chart(
                run_dir,
                "chart_category_mix_pie.png",
                "细类标签分布（扇形图；合并表 ``detail_category_path``，与 §5 同源；"
                "Top 12 以外的细类在数据层并入「（其余细类）」；扇形图内再合并为「其他」）",
            )
        )
        lines.append(
            "*完整类目分布见界面「数据摘要」或简报包中的数据文件。*"
        )
    else:
        lines.append(
            "*深入合并表中无具备可解析 ``detail_category_path`` 细类标签的 SKU，本小节不展示扇形图；请核对商详抓取与合并字段。*"
        )
    lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 五、竞品对比矩阵（按细分类目分组）",
            "",
            "分组**仅**使用合并表列 ``detail_category_path``（商详类目路径）：**三级路径**取中间一段（如 … > **饼干** > 粗粮饼干），"
            "**四级及以上**取倒数第二段（如 … > **面条** > 挂面）。**该列为空**或路径段均为内部编码、**无法解析出可读细类**的 SKU **不进入**本矩阵，亦**不参与**第八章按细类的评价统计。",
            "",
            "**读图方式**：每个细类下为**并列横向条形图**（左：**展示价**（元）；右：**销量**（搜索列表 ``totalSales`` 文案解析件数，如「已售50万+」计为 **50 万**）），"
            "纵轴为**产品标题**（与 ``report_assets/chart_matrix_prices_sales__*.png`` 同源）。**SKU、店铺、配料与评价摘要等明细不列入正文**，见本批次 ``keyword_pipeline_merged.csv``。",
            "",
        ]
    )
    grouped_matrix = _merged_rows_grouped_for_matrix(merged_rows)
    if not grouped_matrix:
        if merged_rows:
            lines.append(
                "*深入合并表有条目，但均无可用 ``detail_category_path``（或路径无法解析为可读细类），故无法生成细类矩阵；"
                "§5～§8 中依赖矩阵的按细类统计相应为空。请核对商详抓取与合并字段。*"
            )
        else:
            lines.append("*无合并表 SKU。*")
        lines.append("")
    for gi, (gname, grows) in enumerate(grouped_matrix):
        lines.append(f"### {gname}（**{len(grows)}** 款）")
        lines.append("")
        mx_chart = _matrix_prices_sales_chart_filename(gname, gi)
        lines.extend(
            _embed_chart(
                run_dir,
                mx_chart,
                f"「{_md_cell(gname, 20)}」· 展示价与销量（totalSales 解析）；纵轴为产品标题。",
            )
        )
        if not (run_dir / "report_assets" / mx_chart).is_file():
            lines.append(
                f"*（尚未生成 ``report_assets/{mx_chart}``：请确认已执行报告出图流程，或重新生成报告。）*"
            )
            lines.append("")
        lines.append("")

    _llm_mx = (llm_matrix_section_md or "").strip()
    if _llm_mx:
        lines.extend(
            [
                "",
                "#### 细类要点归纳（大模型，与上文条形图互补）",
                "",
                "> **说明**：与 §5 相同的细类划分下归纳卖点与配料共性；**具体 SKU、价格与条形图以正文为准**，SKU级明细见合并表 CSV。",
                "",
                _llm_mx,
                "",
            ]
        )

    ch6_price_title = (
        "## 六、价格分析（PC 搜索列表全量）"
        if list_export and pst_list.get("n", 0) > 0
        else "## 六、价格分析（深入 SKU 合并表 · 无可用列表价或未导出列表）"
    )
    lines.extend(["---", "", ch6_price_title, ""])
    lines.append(f"- **统计基础**：{price_analysis_basis_cn}。")
    if (
        list_export
        and pst_list.get("n", 0) > 0
        and pst_merged.get("n", 0) > 0
    ):
        lines.append(
            f"- **对照**：合并表深入样本可解析价 **{pst_merged['n']}** 个观测，中位数约 **{float(pst_merged.get('median', pst_merged['mean'])):.2f}** 元（与上表样本范围不同，仅作对照）。"
        )
    lines.append("")
    if pst:
        price_tbl = [
            "| 统计量 | 数值（元） | 说明 |",
            "| --- | --- | --- |",
            f"| 样本量 | {pst['n']} | 与统计基础一致 |",
            f"| 最小值 | {pst['min']:.2f} | |",
        ]
        if "q1" in pst:
            price_tbl.append(f"| 下四分位 Q1 | {float(pst['q1']):.2f} | |")
        else:
            price_tbl.append("| 下四分位 Q1 | — | 样本不足 4 个 |")
        price_tbl.append(
            f"| 中位数 | {float(pst.get('median', pst['mean'])):.2f} | |"
        )
        if "q3" in pst:
            price_tbl.append(f"| 上四分位 Q3 | {float(pst['q3']):.2f} | |")
        else:
            price_tbl.append("| 上四分位 Q3 | — | 样本不足 4 个 |")
        price_tbl.extend(
            [
                f"| 最大值 | {pst['max']:.2f} | |",
                f"| 均值 | {pst['mean']:.2f} | |",
            ]
        )
        if "stdev" in pst:
            price_tbl.append(f"| 标准差 | {pst['stdev']:.2f} | 离散程度 |")
        lines.extend(price_tbl)
        lines.append("")
        lines.append(
            "**解读提示**：价差大通常反映规格、组合装、品牌溢价或促销差异；B 端定价策略需结合成本与渠道单独建模。"
        )
        lines.append("")
        lines.extend(_markdown_price_promotion_section(promo_sig))
    else:
        lines.append("*当前样本无可用数值价格，本节不展开统计表。*")
        lines.append("")
        lines.extend(_markdown_price_promotion_section(promo_sig))
    lines.append("")

    _llm_pr = (llm_price_groups_section_md or "").strip()
    if _llm_pr:
        lines.extend(
            [
                "",
                "#### 细类价盘要点归纳（大模型，与 §6 量化表互补）",
                "",
                "> **说明**：侧重价带与标价/券后关系的可读叙述；**数值以正文分位数表为准**。",
                "",
                _llm_pr,
                "",
            ]
        )

    _llm_po = (llm_promo_groups_section_md or "").strip()
    if _llm_po:
        lines.extend(
            [
                "",
                "#### 细类促销与活动要点归纳（大模型，与 §6.1 及价盘互补）",
                "",
                "> **说明**：依据合并表「促销摘要」及列表卖点/腰带、榜单类文案等**页面展示摘录**；"
                "归纳券/补贴/新人/榜单曝光等活动形态，**不**替代 §5 的配料/宣称归纳。**具体以页面与 CSV 为准**。",
                "",
                _llm_po,
                "",
            ]
        )

    lines.extend(
        [
            "---",
            "",
            "## 八、消费者反馈与用户画像（按细分类目）",
            "",
            "### 8.1 方法",
            "",
            "- **细类划分**：与 **§5 竞品矩阵** 相同，**仅**依据 ``detail_category_path`` 解析为「饼干 / 西式糕点 / …」等（规则见 §5 章首说明）。",
            "- **归因**：每条评价按其 SKU 对应到深入样本，再映射到该 SKU 所属细类；SKU 不在合并表中的评价单独归入说明性分组；**在合并表中但该 SKU 缺 ``detail_category_path`` 或路径无法解析为可读细类的，该评价不进入按细类统计**（与 §5 排除口径一致）。",
            "- **正负面粗判（§8.2）**：先以关键词规则与图表做粗分；若任务开启 **llm_comment_sentiment**，可附**大模型对抽样原文的主题归因**（尤其负向「用户在抱怨什么」），与词频条形图互补。",
            "- **关注词与使用场景（§8.3）**：对组内评价正文做关注词子串计数（左栏条形图）；对每条有效文本独立扫描**本次任务生效的场景词组**（来自报告调参或系统默认），一条可属多场景，右栏为**占该细类有效文本比例 %**（多标签下可相加 **>** 100%）。二者在 **同一张图左右并列**，与 §5 矩阵细类一一对应。",
            "",
            "### 8.2 评价正负面粗判（关键词规则）",
            "",
            f"- **有效文本条数**：{sentiment_lex.get('text_units', 0)}（与 §8.1 归因口径一致）。",
            f"- **偏正向（仅命中正向词表）**：{sentiment_lex.get('positive_only', 0)} 条；"
            f"**偏负向（仅命中负向词表）**：{sentiment_lex.get('negative_only', 0)} 条；"
            f"**混合（同条兼含正/负词）**：{sentiment_lex.get('mixed_positive_and_negative', 0)} 条；"
            f"**中性或空文本**：{sentiment_lex.get('neutral_or_empty', 0)} 条。",
            "- **说明**：词表为方向性粗判，讽刺、省略与错别字会导致误判；正式结论请**人工抽样**阅读原文。",
        ]
    )
    _scope = (sentiment_lex.get("lexeme_scope_note") or "").strip()
    if _scope:
        lines.append(f"- **词根统计口径**：{_scope}")
    lines.extend(["", ""])
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_sentiment_overview_pie.png",
            "评价语气四象限占比（扇形图；与上表条数一致）",
        )
    )
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_positive_lexemes_bar.png",
            "正向评价里**最常出现的口语短语**（在偏正向或混合评价条内统计；条形图）",
        )
    )
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_negative_lexemes_bar.png",
            "负向评价里**最常出现的口语短语**（在偏负向或混合评价条内统计；条形图）",
        )
    )
    pos_h = sentiment_lex.get("positive_tone_lexeme_hits") or []
    neg_h = sentiment_lex.get("negative_tone_lexeme_hits") or []
    if pos_h:
        frag = "；".join(
            f"「{x.get('word', '')}」{x.get('texts_matched', 0)} 条"
            for x in pos_h[:6]
            if isinstance(x, dict)
        )
        lines.append(f"- **正向语境高频短语（摘要）**：{frag}。")
    if neg_h:
        frag_n = "；".join(
            f"「{x.get('word', '')}」{x.get('texts_matched', 0)} 条"
            for x in neg_h[:6]
            if isinstance(x, dict)
        )
        lines.append(f"- **负向语境高频短语（摘要）**：{frag_n}。")
    _llm_s = (llm_sentiment_section_md or "").strip()
    if _llm_s:
        lines.extend(
            [
                "",
                "#### 大模型深入解读（主题归因，与词频统计互补）",
                "",
                "> **说明**：基于与上节**同一分桶规则**抽样的评价原文，由大模型归纳**用户在说什么**（尤其是负向的具体事由），与上列条数、条形图**互补**；引文以原评论为准。",
                "",
                _llm_s,
            ]
        )
    lines.append("")
    lines.extend(
        [
            "### 8.3 关注词与使用场景（按细类）",
            "",
            "每细类一张**左右并列图**（与 ``report_assets/chart_focus_and_scenarios_bar__*.png`` 同源）："
            "**左**为配置关注词子串命中次数（同一评价可出现多次，为次数而非去重条数）；"
            "**右**为预设场景词组命中占该细类有效文本比例 %（一条可属多场景；多柱比例可相加 **>** 100%）。"
            "统计均基于评价正文（或兜底预览）子串规则，**不等于**购买动机调研结论。",
            "",
        ]
    )
    if not feedback_groups:
        lines.append("*无评价数据可归组。*")
        lines.append("")
    else:
        for gi, (gname, cr_g, texts_g) in enumerate(feedback_groups):
            n_flat = len(cr_g)
            lines.append(f"#### {gname}")
            lines.append("")
            lines.append(
                f"- **本细类逐条评价**：{n_flat} 条；**用于统计的有效文本条数**：{len(texts_g)}。"
            )
            lines.append("")
            hits_g = _group_keyword_hits(cr_g, texts_g, focus_words=focus_words)
            scen_g, scen_ng = _comment_scenario_counts(texts_g, scenario_groups)
            has_focus = any(n > 0 for n in hits_g.values()) if hits_g else False
            has_scen = scen_ng > 0 and any(n > 0 for n in scen_g.values())
            if scen_ng <= 0:
                lines.append("*该细类下无可用评价正文。*")
                lines.append("")
                continue
            if has_focus or has_scen:
                cap = (
                    f"「{_md_cell(gname, 24)}」细类 · 关注词与使用场景（左：关注词命中次数；右：场景占有效文本 %；"
                    f"有效文本 **{scen_ng}** 条）"
                )
                lines.extend(
                    _embed_chart(
                        run_dir,
                        _focus_scenario_combo_bar_filename(gname, gi),
                        cap,
                    )
                )
            else:
                lines.append("*该细类无关注词命中且未命中预设场景词组。*")
                lines.append("")
            if has_scen:
                for para in _scenario_summary_bullets(scen_g, scen_ng):
                    lines.append(para)
                    lines.append("")
            elif scen_ng > 0:
                lines.append("*未命中预设场景词组。*")
                lines.append("")

    _llm_sg = (llm_scenario_groups_section_md or "").strip()
    if _llm_sg:
        lines.extend(
            [
                "",
                "#### 使用场景要点归纳（大模型，与 §8.3 右栏图表互补）",
                "",
                "> **说明**：与 §8.3 **相同**的预设场景词组与子串命中规则；**各场景条数与占比以正文图右栏为准**。",
                "",
                _llm_sg,
                "",
            ]
        )

    _llm_cg = (llm_comment_groups_section_md or "").strip()
    if _llm_cg:
        lines.extend(
            [
                "",
                "#### 细类评价与关注词要点归纳（大模型，与 §8.3 左栏图表互补）",
                "",
                "> **说明**：归纳各细类反馈主题与配置关注词命中；**次数与 §8.3 图左栏以正文为准**。",
                "",
                _llm_cg,
                "",
            ]
        )

    lines.extend(["---", "", "## 九、策略与机会提示（假设清单，待验证）", ""])
    _llm_st = (llm_strategy_opportunities_section_md or "").strip()
    if _llm_st:
        lines.extend(
            [
                "基于本任务结构化摘要（价盘、集中度、评价与场景、促销信号等）的**假设性策略归纳**；数字与明细以前文及 CSV 为准，定稿前请结合贵司成本、渠道与合规复核。",
                "",
                "#### 策略与机会建议（大模型）",
                "",
                _llm_st,
                "",
            ]
        )
    else:
        lines.extend(
            [
                "未生成本节大模型正文：请在任务 `report_config` 中开启 `llm_strategy_opportunities` 并重跑产物，或检查 run 目录下 `strategy_opportunities_llm.json` 是否报错。",
                "",
            ]
        )

    lines.extend(
        [
            "---",
            "",
            "## 附录 A：数据留存说明",
            "",
            "- 本批次**任务输出目录**内保存：搜索列表导出、深入 SKU 合并表、商详与评价相关表格，以及搜索/商详原始响应与运行参数快照，供内部复核与复算。",
            "- 对外演示或转发前请按公司规范做**脱敏**处理。",
            "",
            "---",
            "",
            "*本报告由系统自动汇总生成；定稿前请业务交叉核对数据与结论。*",
            "",
        ]
    )
    return "\n".join(lines)


def _sanitize_json_numbers(obj: Any) -> Any:
    """浮点 NaN/Inf 无法 JSON 序列化，统一转 None 或圆角。"""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 6)
    if isinstance(obj, dict):
        return {k: _sanitize_json_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_json_numbers(x) for x in obj]
    return obj


def build_competitor_brief(
    *,
    run_dir: Path,
    keyword: str,
    merged_rows: list[dict[str, str]],
    search_export_rows: list[dict[str, str]],
    comment_rows: list[dict[str, str]],
    meta: dict[str, Any] | None,
    report_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    与 ``build_competitor_markdown`` 共用统计口径，输出可 JSON 序列化的结构化竞品摘要（**规则驱动**，无 LLM）。
    """
    focus_words, scenario_groups, _ext = resolve_report_tuning(report_config)
    sku_header = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    batch = _run_batch_label(run_dir)
    n_sku = len(merged_rows)
    n_cmt = len(comment_rows)
    n_sku_matrix = sum(1 for r in merged_rows if _competitor_matrix_group_key(r))

    list_export = len(search_export_rows) > 0
    structure_rows = search_export_rows if list_export else merged_rows
    n_structure = len(structure_rows)
    shops_s = _structure_shops(structure_rows, list_export=list_export)
    brands_s = _structure_brands(structure_rows, list_export=list_export)
    shops_for_cr = _structure_names_for_pie_counter(shops_s)
    brands_for_cr = _structure_names_for_pie_counter(brands_s)
    cr1_shop, cr3_shop, top_shop_s, top_shop_share = _brand_cr(shops_for_cr)
    cr1_list_brand, cr3_list_brand, top_list_brand, _ = _brand_cr(brands_for_cr)
    cm_structure = _category_mix(merged_rows, top_k=12)
    min_brand_rows = max(5, int(0.02 * n_structure)) if n_structure else 5

    brands_deep = [
        _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
        for r in merged_rows
        if _cell(r, MERGED_FIELD_TO_CSV_HEADER["detail_brand"], "detail_brand")
    ]
    cr1_deep, cr3_deep, top_brand_deep, top_brand_deep_share = _brand_cr(
        brands_deep
    )
    cr1_hints = cr1_shop if list_export and cr1_shop is not None else cr1_deep

    pst_merged = _price_stats_extended(_collect_prices(merged_rows))
    pst_list = (
        _price_stats_extended(_collect_prices(search_export_rows))
        if list_export
        else {}
    )
    pst = (
        pst_list
        if list_export and pst_list.get("n", 0) > 0
        else pst_merged
    )
    price_stats_source = (
        "pc_search_export_all_rows"
        if list_export and pst_list.get("n", 0) > 0
        else "keyword_pipeline_merged"
    )
    promo_rows_brief = (
        search_export_rows
        if list_export and pst_list.get("n", 0) > 0
        else merged_rows
    )
    price_promotion_signals = _analyze_price_promotions(promo_rows_brief)

    hits = _comment_keyword_hits(comment_rows, focus_words)
    if not hits:
        blob = _merge_comment_previews(merged_rows)
        for w in focus_words:
            if len(w) < 2:
                continue
            n = blob.count(w)
            if n:
                hits[w] += n

    comment_texts = _iter_comment_text_units(comment_rows, merged_rows)
    comment_sentiment_lexicon = _comment_sentiment_lexicon(comment_texts)
    scen_counts, scen_n_texts = _comment_scenario_counts(
        comment_texts, scenario_groups
    )

    (
        api_rc,
        api_list_kw,
        api_rc_uniques,
        api_raw_json_n,
        _api_rc_n_values,
    ) = _pc_search_result_count_from_raw(run_dir)

    proxy = _search_list_proxies(search_export_rows) if search_export_rows else {}

    hints = _strategy_hints(
        cr1=cr1_hints,
        pst=pst,
        hits=hits,
        n_comments=n_cmt,
        scen_counts=scen_counts,
        scen_n_texts=scen_n_texts,
    )

    matrix_groups: list[dict[str, Any]] = []
    for gname, mrows in _merged_rows_grouped_for_matrix(merged_rows):
        items: list[dict[str, str]] = []
        for row in mrows:
            items.append(
                {
                    "sku_id": _cell(row, sku_header),
                    "title": _cell(row, title_h),
                    "brand": _cell(
                        row,
                        MERGED_FIELD_TO_CSV_HEADER["detail_brand"],
                        "detail_brand",
                    ),
                    "list_price_show": _cell(
                        row, *_LIST_SHOW_PRICE_CELL_KEYS
                    ),
                    "coupon_or_detail_price": _cell(
                        row,
                        _COUPON_SHOW_PRICE_KEY,
                        _LEGACY_COUPON_SHOW_PRICE_KEY,
                    ),
                    "detail_price_final": _cell(row, *_DETAIL_PRICE_FINAL_CSV_KEYS),
                    "shop": _cell(row, *_MERGED_SHOP_CELL_KEYS),
                    "category": _detail_category_path_cell(row),
                    "selling_point": _cell(
                        row, _SELLING_POINT_KEY, _LEGACY_SELLING_POINT_KEY
                    )[:240],
                    "comment_fuzzy": _cell(row, *_COMMENT_FUZZ_KEYS),
                    "total_sales": merged_csv_effective_total_sales(row),
                }
            )
        matrix_groups.append(
            {"group": gname, "sku_count": len(items), "skus": items}
        )

    feedback_by_group: list[dict[str, Any]] = []
    usage_scenarios_by_matrix_group: list[dict[str, Any]] = []
    for gi, (gname, cr, tu) in enumerate(
        _consumer_feedback_by_matrix_group(
            merged_rows=merged_rows,
            comment_rows=comment_rows,
            sku_header=sku_header,
        )
    ):
        gh = _group_keyword_hits(cr, tu, focus_words=focus_words)
        scen_g, scen_n_g = _comment_scenario_counts(tu, scenario_groups)
        slug_fb = _scenario_group_asset_slug(gname, gi)
        feedback_by_group.append(
            {
                "group": gname,
                "matrix_group_index": gi,
                "chart_slug": slug_fb,
                "comment_rows": len(cr),
                "effective_comment_text_units": len(tu),
                "focus_keyword_hits": [
                    {"word": w, "count": n} for w, n in gh.most_common(24)
                ],
                "scenarios_top": [
                    {
                        "scenario": s,
                        "count": n,
                        "share_of_text_units": (
                            n / scen_n_g if scen_n_g else 0.0
                        ),
                    }
                    for s, n in scen_g.most_common(6)
                ]
                if scen_n_g
                else [],
            }
        )
        if scen_n_g > 0 and scen_g:
            usage_scenarios_by_matrix_group.append(
                {
                    "group": gname,
                    "matrix_group_index": gi,
                    "chart_slug": slug_fb,
                    "effective_text_units": scen_n_g,
                    "scenarios": [
                        {
                            "scenario": s,
                            "count": int(n),
                            "share_of_text_units": (
                                float(n) / scen_n_g if scen_n_g else 0.0
                            ),
                        }
                        for s, n in scen_g.most_common()
                        if n > 0
                    ],
                }
            )

    meta_slice: dict[str, Any] = {}
    if meta:
        for k in (
            "page_start",
            "page_to",
            "max_skus_config",
            "pc_search_export_rows",
            "merged_rows",
            "scenario_filter_enabled",
            "merged_csv_mode",
        ):
            if k in meta:
                meta_slice[k] = meta[k]

    list_brand_block: dict[str, Any] | None
    if len(brands_for_cr) >= min_brand_rows:
        list_brand_block = {
            "first_share": cr1_list_brand,
            "top_three_combined_share": cr3_list_brand,
            "top_label": top_list_brand,
        }
    else:
        list_brand_block = None

    out: dict[str, Any] = {
        "schema_version": 1,
        "keyword": keyword,
        "batch_label": batch,
        "run_dir": str(run_dir.resolve()),
        "scope": {
            "merged_sku_count": n_sku,
            "comment_flat_rows": n_cmt,
            "structure_source_rows": n_structure,
            "uses_pc_search_list_export": list_export,
            "category_mix_source": "keyword_pipeline_merged",
            "category_mix_valid_matrix_sku_count": n_sku_matrix,
        },
        "meta": meta_slice or None,
        "pc_search_raw": {
            "result_count_consensus": api_rc,
            "list_keyword": api_list_kw or None,
            "result_count_uniques": api_rc_uniques,
            "raw_json_files_scanned": api_raw_json_n,
        },
        "list_visibility_proxy": proxy,
        "concentration": {
            "shops_from_list": {
                "first_share": cr1_shop,
                "top_three_combined_share": cr3_shop,
                "top_label": top_shop_s,
                "top_share_pct": top_shop_share,
            },
            "list_brand_field": list_brand_block,
            "detail_brand_among_merged": {
                "first_share": cr1_deep,
                "top_three_combined_share": cr3_deep,
                "top_label": top_brand_deep,
                "top_share_pct": top_brand_deep_share,
            },
        },
        "category_mix_top": [
            {"label": lbl, "count": cnt} for lbl, cnt in cm_structure
        ],
        "list_brand_mix_top": [
            {"label": k, "count": v}
            for k, v in _counter_mix_top_rows_with_remainder(
                brands_s,
                top_n=24,
                remainder_label="（其余品牌）",
            )
        ],
        "list_shop_mix_top": [
            {"label": k, "count": v}
            for k, v in _counter_mix_top_rows_with_remainder(
                shops_s,
                top_n=24,
                remainder_label="（其余店铺）",
            )
        ],
        "price_stats": pst,
        "price_stats_source": price_stats_source,
        "price_stats_merged_sample": pst_merged,
        "price_stats_list_export": pst_list if list_export else {},
        "price_promotion_signals": price_promotion_signals,
        "comment_focus_keywords": [
            {"word": w, "count": n} for w, n in hits.most_common(24)
        ],
        "usage_scenarios": [
            {
                "scenario": lbl,
                "count": n,
                "share_of_text_units": (
                    n / scen_n_texts if scen_n_texts else 0.0
                ),
            }
            for lbl, n in scen_counts.most_common(16)
        ],
        "usage_scenarios_denominator": scen_n_texts,
        "usage_scenarios_by_matrix_group": usage_scenarios_by_matrix_group,
        "strategy_hints": hints,
        "matrix_by_group": matrix_groups,
        "consumer_feedback_by_matrix_group": feedback_by_group,
        "comment_sentiment_lexicon": comment_sentiment_lexicon,
        "notes": [
            "与在线分析报告各章统计口径一致；关注词与场景以任务中的分析规则为准（子串命中统计，非深度主题模型）。",
            "价格来自页面展示字段抽取，含促销与规格差异；促销与标价对齐等为启发式摘录，仅供对照。",
            "评价语气为关键词粗判，非深度学习情感模型。",
            "「集中度」中：最大一家占比、前三名合计占比为小数（如 0.12 表示约 12%），对应列表或深入样本中的相关行。",
        ],
    }
    return _sanitize_json_numbers(out)


def main() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    existing = _resolve_existing_run_dir(EXISTING_RUN_DIR)
    meta_path_early = (existing / kpl.FILE_RUN_META_JSON) if existing else None
    meta_early: dict[str, Any] | None = None
    if meta_path_early and meta_path_early.is_file():
        try:
            meta_early = json.loads(meta_path_early.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta_early = None

    if existing:
        if not existing.is_dir():
            print(f"[竞品报告] EXISTING_RUN_DIR 不是目录: {existing}", file=sys.stderr)
            sys.exit(2)
        kw = (KEYWORD or "").strip() or _infer_keyword(existing, meta_early)
        if not kw:
            print(
                "[竞品报告] 仅分析已有目录时，请配置 KEYWORD，或保留 run_meta.json 的 keyword，"
                "或使目录名为 YYYYMMDD_HHMMSS_关键词",
                file=sys.stderr,
            )
            sys.exit(2)
        run_dir = existing
        print(f"[竞品报告] 使用已有目录（不抓取）: {run_dir}", file=sys.stderr)
    else:
        kw = (KEYWORD or "").strip()
        if not kw:
            print("[竞品报告] 全量抓取时请在本文件顶部配置 KEYWORD", file=sys.stderr)
            sys.exit(2)

        backup: dict[str, Any] = {}
        try:
            if OVERRIDE_MAX_SKUS is not None:
                backup["MAX_SKUS"] = kpl.MAX_SKUS
                kpl.MAX_SKUS = max(1, int(OVERRIDE_MAX_SKUS))
            if OVERRIDE_PAGE_START is not None:
                backup["PAGE_START"] = kpl.PAGE_START
                kpl.PAGE_START = max(1, int(OVERRIDE_PAGE_START))
            if OVERRIDE_PAGE_TO is not None:
                backup["PAGE_TO"] = kpl.PAGE_TO
                kpl.PAGE_TO = max(1, int(OVERRIDE_PAGE_TO))

            print(f"[竞品报告] 关键词={kw!r}，开始流水线…", file=sys.stderr)
            run_dir = kpl.main(keyword=kw)
        finally:
            for name, val in backup.items():
                setattr(kpl, name, val)

    merged_path = run_dir / kpl.FILE_MERGED_CSV
    comments_path = run_dir / kpl.FILE_COMMENTS_FLAT_CSV
    meta_path = run_dir / kpl.FILE_RUN_META_JSON

    _, merged_rows = _read_csv_rows(merged_path)
    _, search_export_rows = _read_csv_rows(run_dir / kpl.FILE_PC_SEARCH_CSV)
    _, comment_rows = _read_csv_rows(comments_path)
    meta: dict[str, Any] | None = meta_early if existing else None
    if meta is None and meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = None

    md = build_competitor_markdown(
        run_dir=run_dir,
        keyword=kw,
        merged_rows=merged_rows,
        search_export_rows=search_export_rows,
        comment_rows=comment_rows,
        meta=meta,
    )
    out_md = run_dir / "competitor_analysis.md"
    out_md.write_text(md, encoding="utf-8")
    print(f"[竞品报告] 运行目录: {run_dir}", file=sys.stderr)
    print(f"[竞品报告] 已写: {out_md}", file=sys.stderr)


if __name__ == "__main__":
    main()
