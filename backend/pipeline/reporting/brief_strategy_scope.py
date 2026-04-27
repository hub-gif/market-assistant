"""按矩阵分组（细类）收窄 competitor brief，使策略生成输入仅含所选分组数据。"""
from __future__ import annotations

import copy
from collections import Counter
from typing import Any

from pipeline.competitor_report.csv_io import _collect_prices
from pipeline.competitor_report.price_promo import _analyze_price_promotions
from pipeline.competitor_report.price_stats import _price_stats_extended


def list_matrix_groups_for_api(brief: dict[str, Any]) -> list[dict[str, Any]]:
    """供前端下拉：矩阵分组名称与索引（与 ``matrix_by_group`` 顺序一致）。"""
    mg = brief.get("matrix_by_group")
    if not isinstance(mg, list):
        return []
    out: list[dict[str, Any]] = []
    for i, g in enumerate(mg):
        if not isinstance(g, dict):
            continue
        name = (g.get("group") or "").strip() or "—"
        skus = g.get("skus") if isinstance(g.get("skus"), list) else []
        out.append(
            {
                "index": i,
                "group": name,
                "sku_count": int(g.get("sku_count") or len(skus)),
            }
        )
    return out


def resolve_strategy_matrix_group_index(
    brief: dict[str, Any],
    *,
    matrix_group_index: int | None = None,
    matrix_group_label: str | None = None,
) -> tuple[int | None, str | None]:
    """
    解析请求中的矩阵分组。

    返回 ``(index, error)``：
    - ``index is None`` 且 ``error is None``：未指定收窄（使用完整 brief）；
    - ``index`` 为 ``int``：有效分组下标；
    - ``error`` 非空：参数与 brief 不一致。
    """
    mg = brief.get("matrix_by_group")
    if not isinstance(mg, list) or not mg:
        if matrix_group_index is not None or (
            matrix_group_label and str(matrix_group_label).strip()
        ):
            return None, "当前任务 brief 中无 matrix_by_group，无法按细类收窄"
        return None, None

    label = (matrix_group_label or "").strip()
    has_idx = matrix_group_index is not None
    has_lbl = bool(label)

    if not has_idx and not has_lbl:
        return None, None

    if has_idx:
        idx = int(matrix_group_index)
        if idx < 0 or idx >= len(mg):
            return None, f"strategy_matrix_group_index 须在 0～{len(mg) - 1} 之间"

    if has_lbl and not has_idx:
        for i, g in enumerate(mg):
            if not isinstance(g, dict):
                continue
            if (g.get("group") or "").strip() == label:
                return i, None
        return None, f"未找到矩阵分组「{label}」"

    assert has_idx
    idx = int(matrix_group_index)
    g0 = mg[idx]
    gname = (g0.get("group") or "").strip() if isinstance(g0, dict) else ""
    if has_lbl and gname != label:
        return None, (
            f"strategy_matrix_group_index={idx} 对应分组「{gname}」，"
            f"与 strategy_matrix_group「{label}」不一致"
        )
    return idx, None


def _sku_rows_for_prices(skus: list[dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for s in skus:
        if not isinstance(s, dict):
            continue
        out.append({k: str(v) if v is not None else "" for k, v in s.items()})
    return out


def _mix_top_rows(
    skus: list[dict[str, Any]], key: str, *, top_n: int = 24
) -> list[dict[str, Any]]:
    c: Counter[str] = Counter()
    for s in skus:
        if not isinstance(s, dict):
            continue
        v = (s.get(key) or "").strip() or "（未标注）"
        c[v] += 1
    rows: list[dict[str, Any]] = []
    for lab, cnt in c.most_common(top_n):
        rows.append({"label": lab, "count": cnt})
    remainder = sum(cnt for _, cnt in c.most_common()[top_n:])
    if remainder > 0:
        rows.append({"label": "（其余）", "count": remainder})
    return rows


def _category_mix_from_skus(skus: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c: Counter[str] = Counter()
    for s in skus:
        if not isinstance(s, dict):
            continue
        cat = (s.get("category") or "").strip()
        if not cat:
            continue
        parts = [p.strip() for p in cat.split(">") if p.strip()]
        leaf = parts[-1] if parts else cat
        c[leaf] += 1
    return [{"label": k, "count": v} for k, v in c.most_common(24)]


def _concentration_brand_shop_from_skus(skus: list[dict[str, Any]]) -> dict[str, Any]:
    """用矩阵内 SKU 粗算集中度（与全站列表口径不同，仅供分组内对照）。"""
    brands = [
        (s.get("brand") or "").strip()
        for s in skus
        if isinstance(s, dict) and (s.get("brand") or "").strip()
    ]
    shops = [
        (s.get("shop") or "").strip()
        for s in skus
        if isinstance(s, dict) and (s.get("shop") or "").strip()
    ]

    def _block(labels: list[str]) -> dict[str, Any]:
        if not labels:
            return {
                "first_share": 0.0,
                "top_three_combined_share": 0.0,
                "top_label": "—",
                "top_share_pct": "0%",
            }
        ct = Counter(labels)
        n = len(labels)
        top_lab, top_n = ct.most_common(1)[0]
        top3 = sum(x for _, x in ct.most_common(3))
        return {
            "first_share": top_n / n,
            "top_three_combined_share": top3 / n,
            "top_label": top_lab,
            "top_share_pct": f"{100.0 * top_n / n:.1f}%",
        }

    return {
        "shops_from_list": _block(shops),
        "list_brand_field": None,
        "detail_brand_among_merged": _block(brands),
    }


def filter_brief_for_strategy_matrix_group(
    brief: dict[str, Any],
    *,
    matrix_group_index: int,
) -> dict[str, Any]:
    """
    深拷贝 ``brief``，仅保留 ``matrix_by_group[matrix_group_index]`` 及其对齐的
    ``consumer_feedback_by_matrix_group`` / ``usage_scenarios_by_matrix_group``，
    并重算与样本相关的价盘、类目与集中度（避免仍混入全关键词列表口径）。
    """
    b = copy.deepcopy(brief)
    mg = b.get("matrix_by_group")
    if not isinstance(mg, list) or matrix_group_index < 0 or matrix_group_index >= len(
        mg
    ):
        return b

    chosen = mg[matrix_group_index]
    if not isinstance(chosen, dict):
        return b

    gname = (chosen.get("group") or "").strip() or "—"
    skus = chosen.get("skus") if isinstance(chosen.get("skus"), list) else []
    n_skus = len(skus)

    b["matrix_by_group"] = [
        {
            "group": gname,
            "sku_count": int(chosen.get("sku_count") or n_skus),
            "skus": skus,
        }
    ]

    def _pick_by_group(
        items: Any,
    ) -> list[dict[str, Any]]:
        if not isinstance(items, list):
            return []
        out: list[dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            if (it.get("group") or "").strip() == gname:
                out.append(dict(it))
                break
            gi = it.get("matrix_group_index")
            if gi is not None and int(gi) == matrix_group_index:
                out.append(dict(it))
                break
        return out

    fb = b.get("consumer_feedback_by_matrix_group")
    fb_one = _pick_by_group(fb)
    b["consumer_feedback_by_matrix_group"] = fb_one

    ub = b.get("usage_scenarios_by_matrix_group")
    b["usage_scenarios_by_matrix_group"] = _pick_by_group(ub)

    if fb_one:
        f0 = fb_one[0]
        denom = int(f0.get("effective_comment_text_units") or 0)
        if denom <= 0:
            denom = int(f0.get("comment_rows") or 0)
        b["comment_focus_keywords"] = []
        b["usage_scenarios"] = []
        b["usage_scenarios_denominator"] = denom
    else:
        b["comment_focus_keywords"] = []
        b["usage_scenarios"] = []
        b["usage_scenarios_denominator"] = 0

    rows_for_price = _sku_rows_for_prices(
        [s for s in skus if isinstance(s, dict)]
    )
    prices = _collect_prices(rows_for_price)
    pst_merged = _price_stats_extended(prices)
    b["price_stats_merged_sample"] = pst_merged
    b["price_stats"] = dict(pst_merged) if pst_merged else {}
    b["price_stats_source"] = "strategy_scope_matrix_group_skus"
    b["price_stats_list_export"] = {}

    b["category_mix_top"] = _category_mix_from_skus(
        [s for s in skus if isinstance(s, dict)]
    )
    b["list_brand_mix_top"] = _mix_top_rows(
        [s for s in skus if isinstance(s, dict)], "brand"
    )
    b["list_shop_mix_top"] = _mix_top_rows(
        [s for s in skus if isinstance(s, dict)], "shop"
    )

    b["concentration"] = _concentration_brand_shop_from_skus(
        [s for s in skus if isinstance(s, dict)]
    )

    sc = b.get("scope")
    if isinstance(sc, dict):
        sc2 = dict(sc)
        sc2["merged_sku_count"] = n_skus
        if fb_one:
            sc2["comment_flat_rows"] = int(fb_one[0].get("comment_rows") or 0)
        b["scope"] = sc2

    b["strategy_scope_applied"] = {
        "matrix_group_index": matrix_group_index,
        "group": gname,
        "original_matrix_group_count": len(mg),
    }

    notes = b.get("notes")
    extra = (
        "策略生成已按矩阵分组收窄：下文统计与矩阵仅针对「"
        + gname
        + "」内 SKU；与全关键词搜索列表、全样本评价总量不同口径。"
    )
    if isinstance(notes, list):
        b["notes"] = [extra] + [n for n in notes if isinstance(n, str)]
    else:
        b["notes"] = [extra]

    b["price_promotion_signals"] = _analyze_price_promotions(rows_for_price)
    b["strategy_hints"] = []

    b["list_visibility_proxy"] = {
        "total_rows": n_skus,
        "unique_skus": n_skus,
        "_strategy_scope_note": "矩阵所选分组内 SKU 数，非全关键词列表导出口径。",
    }

    return b


__all__ = [
    "filter_brief_for_strategy_matrix_group",
    "list_matrix_groups_for_api",
    "resolve_strategy_matrix_group_index",
]
