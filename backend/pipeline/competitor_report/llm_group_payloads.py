"""按细类矩阵分组的 LLM 载荷（矩阵/价盘/促销/评价/场景）。"""
from __future__ import annotations

from collections import Counter
from typing import Any

from pipeline.csv.schema import JD_SEARCH_CSV_HEADERS, MERGED_FIELD_TO_CSV_HEADER

from .comment_sentiment import _comment_keyword_hits
from .constants import (
    _COMMENT_CSV_BODY,
    _COMMENT_CSV_SKU,
    _COUPON_SHOW_PRICE_KEY,
    _DETAIL_PRICE_FINAL_CSV_KEYS,
    _LEGACY_COUPON_SHOW_PRICE_KEY,
    _LEGACY_RANK_TAGLINE_KEY,
    _LEGACY_SELLING_POINT_KEY,
    _LIST_SHOW_PRICE_CELL_KEYS,
    _MERGED_SHOP_CELL_KEYS,
    _RANK_TAGLINE_KEY,
    _SELLING_POINT_KEY,
)
from .csv_io import _cell, _collect_prices, _md_cell
from .ingredients import _ingredients_from_product_attributes, _ingredients_single_line
from .matrix_group import _competitor_matrix_group_key, _merged_rows_grouped_for_matrix
from .price_stats import _price_stats_extended


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
    """单条 SKU：合并表「促销摘要」「榜单排名」「榜单类文案」摘录，供促销 LLM 用（不含列表卖点/腰带列，避免固定词表匹配的粗口径）。"""
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


__all__ = [
    "build_comment_groups_llm_payload",
    "build_matrix_groups_llm_payload",
    "build_price_groups_llm_payload",
    "build_promo_groups_llm_payload",
    "build_scenario_groups_llm_payload",
    "_comment_scenario_counts",
    "_group_keyword_hits",
    "_listing_price_snippet_for_llm",
    "_matrix_excerpt_line_for_llm",
    "_promo_snippet_for_llm",
    "_text_hits_scenario_triggers",
]
