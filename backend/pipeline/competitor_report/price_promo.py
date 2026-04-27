"""列表/合并行上的标价与券后价差统计，及第六章 6.1 Markdown 片段。"""
from __future__ import annotations

import statistics
from typing import Any

from .constants import (
    _COUPON_SHOW_PRICE_KEY,
    _JD_LIST_PRICE_KEY,
    _LEGACY_COUPON_SHOW_PRICE_KEY,
    _LEGACY_JD_LIST_PRICE_KEY,
    _LEGACY_RANK_TAGLINE_KEY,
    _LEGACY_SELLING_POINT_KEY,
    _ORIGINAL_LIST_PRICE_KEY,
    _RANK_TAGLINE_KEY,
    _SELLING_POINT_KEY,
)
from .csv_io import _cell, _float_price


def _analyze_price_promotions(rows: list[dict[str, str]]) -> dict[str, Any]:
    """
    从列表或合并行中归纳「标价 vs 券后/到手」等价差信号，
    供第六章第一节与结构化摘要使用（按**页面展示价**字段归纳，非结算实付）。
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
        "promo_keyword_row_hits_top": [],
    }


def _markdown_price_promotion_section(p: dict[str, Any]) -> list[str]:
    """第六章第一节：优惠活动与价差信号（Markdown 行列表）。"""
    lines: list[str] = [
        "### 6.1 优惠活动与价差信号（页面展示摘录）",
        "",
        "- **统计范围**：与上节价量统计**同一批行**；比较的是列表/合并表中的**展示标价**与**展示券后/到手价**（字段见表头），"
        "反映页面呈现的活动与券信息，**不等于**用户结算实付或历史最低价。",
        "",
    ]
    wb = int(p.get("rows_with_both_list_and_coupon") or 0)
    if wb <= 0:
        lines.append(
            "- **标价与券后价可对齐比较**的有效行不足，本节以展示价与券后/到手字段的可得信息为主。"
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
                "- **价差**：存在「券后低于标价」样本，但条数较少，未给出稳健分位数；建议结合第五章矩阵中的单品对照。"
            )
        lines.append("")
        oa = int(p.get("rows_original_price_above_list_price") or 0)
        if oa > 0:
            lines.append(
                f"- **划线原价高于当前标价** 的行约 **{oa}** 条（常见「划线价 + 当前价」促销陈列，具体以页面为准）。"
            )
            lines.append("")
    lines.append(
        "- **说明**：本节**不对**列表「卖点/腰带」等字段做预设促销关键词行级统计；"
        "活动形态归纳以第六章「细类促销与活动要点归纳」为准（若已生成）。"
    )
    lines.append("")
    return lines


__all__ = [
    "_analyze_price_promotions",
    "_markdown_price_promotion_section",
]
