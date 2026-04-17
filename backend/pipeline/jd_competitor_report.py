# -*- coding: utf-8 -*-
"""
关键词 → 调用 ``jd_keyword_pipeline`` 全链路采集 → 生成 **标准化竞品分析报告**（Markdown）。

报告结构对齐常见竞品分析框架：研究范围与方法、执行摘要、**整体市场观察（列表可见度参考）**、
市场与竞争结构、**按细分类目分组的竞品对比矩阵**、价格分析（含规则化价差/活动信号与可选 **细类价盘·促销** 大模型归纳）、**按细分类目的消费者反馈与用户画像**、**策略与机会提示**（以大模型归纳为主，可选）与附录；并明确数据边界。
若运行配置中提供了外部市场规模摘录（``EXTERNAL_MARKET_TABLE_ROWS``），则追加对应表格小节；否则不输出占位行。

依赖：全量抓取时与 ``crawler_copy/jd_pc_search/jd_keyword_pipeline.py`` 相同（Node、h5st、Playwright、``common/jd_cookie.txt``）。
**仅复用已有目录生成报告时**不需要跑浏览器，只需该目录下已有 CSV / ``run_meta.json``。

本模块位于 ``pipeline``（解析与报告）；爬虫实现仅在 ``crawler_copy/jd_pc_search``。

用法：

- **重新抓取并出报告**：``EXISTING_RUN_DIR = None``，配置 ``KEYWORD``（及可选 ``OVERRIDE_*``），在 ``backend`` 目录下执行
  ``python -m pipeline.jd_competitor_report``；或沿用爬虫目录下的兼容入口 ``python jd_competitor_report.py``（见该文件说明）。
- **只分析已有批次**：将 ``EXISTING_RUN_DIR`` 设为 ``pipeline_runs/<时间戳>_<关键词>/`` 的绝对或相对路径（相对当前工作目录），
  再执行同一命令；**不重新抓取**。关键词优先用本文件 ``KEYWORD``，否则读 ``run_meta.json`` 的 ``keyword``，再否则从目录名
  ``YYYYMMDD_HHMMSS_<词>`` 推断。

流水线其余参数（评论分页、延迟等）仍在 ``jd_keyword_pipeline.py`` 顶部配置。

输出：在对应运行目录下覆盖写入 ``competitor_analysis.md``。
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# 竞品报告依赖爬虫副本中的 ``jd_keyword_pipeline``（采集）；本文件归属 pipeline（解析与成稿）。
_BACKEND = Path(__file__).resolve().parent.parent
_CRAWLER_JD = _BACKEND / "crawler_copy" / "jd_pc_search"
if str(_CRAWLER_JD) not in sys.path:
    sys.path.insert(0, str(_CRAWLER_JD))

import jd_keyword_pipeline as kpl  # noqa: E402
from pipeline.csv_schema import (  # noqa: E402
    COMMENT_CSV_COLUMNS,
    JD_SEARCH_CSV_HEADERS,
    MERGED_FIELD_TO_CSV_HEADER,
    merged_csv_effective_total_sales,
)

from pipeline.competitor_report.config import *  # noqa: F403
from pipeline.competitor_report.constants import *  # noqa: F403
from pipeline.competitor_report.csv_io import *  # noqa: F403
from pipeline.competitor_report.price_promo import (  # noqa: E402
    _analyze_price_promotions,
    _markdown_price_promotion_section,
)
from pipeline.competitor_report.comment_sentiment import (  # noqa: E402
    build_comment_sentiment_llm_payload,
    _comment_keyword_hits,
    _comment_sentiment_lexicon,
    _iter_comment_text_units,
    _iter_comment_text_units_and_scores,
    _merge_comment_previews,
    _parse_comment_score,
)
from pipeline.competitor_report.llm_group_payloads import (  # noqa: E402
    build_comment_groups_llm_payload,
    build_matrix_groups_llm_payload,
    build_price_groups_llm_payload,
    build_promo_groups_llm_payload,
    build_scenario_groups_llm_payload,
    _comment_scenario_counts,
    _group_keyword_hits,
    _text_hits_scenario_triggers,
)
from pipeline.competitor_report.matrix_group import (  # noqa: E402
    _category_mix,
    _competitor_matrix_group_key,
    _merged_rows_grouped_for_matrix,
)
from pipeline.competitor_report.price_stats import _price_stats_extended  # noqa: E402
from pipeline.competitor_report.consumer_feedback import (  # noqa: E402
    _comment_lines_with_product_context,
    _consumer_feedback_by_matrix_group,
    _sku_to_matrix_group_map,
)
from pipeline.competitor_report.list_mix import (  # noqa: E402
    _brand_cr,
    _counter_mix_top_rows_with_remainder,
    _search_list_proxies,
    _structure_brands,
    _structure_names_for_pie_counter,
    _structure_shops,
)
from pipeline.competitor_report.matrix_md import (  # noqa: E402
    _competitor_matrix_md_line,
    _matrix_ingredients_cell,
)
from pipeline.competitor_report.report_md_helpers import (  # noqa: E402
    _embed_chart,
    _focus_scenario_combo_bar_filename,
    _lines_4_reading_brand,
    _lines_4_reading_shop,
    _matrix_prices_sales_chart_filename,
    _mermaid_pie_focus_keywords,
    _scenario_group_asset_slug,
    _scenario_summary_bullets,
    _strategy_hints,
)
from pipeline.competitor_report.run_context import (  # noqa: E402
    _infer_keyword,
    _pc_search_result_count_from_raw,
    _resolve_existing_run_dir,
    _run_batch_label,
)

# ---------------------------------------------------------------------------
# 运行配置（按需改这里；与 pipeline.competitor_report.constants 中默认关注词等配合使用）
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
    chapter8_text_mining_probe_section_md: str | None = None,
) -> str:
    focus_words, scenario_groups, external_rows = resolve_report_tuning(report_config)
    _ch8_probe_sec = (chapter8_text_mining_probe_section_md or "").strip()
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
    # §4.3 类目分布：深入合并表（与 §5 竞品矩阵同一细类划分，非搜索列表行）
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

    comment_texts, comment_scores = _iter_comment_text_units_and_scores(
        comment_rows, merged_rows
    )
    sentiment_lex = _comment_sentiment_lexicon(comment_texts, comment_scores)
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
        f"- **分析对象**：本次采集流程选取的 **{n_sku}** 个 SKU（搜索排序靠前子样本，非全站普查）。",
    ]
    if n_sku:
        n_sku_nop = n_sku - n_sku_pathed
        n_sku_unparsed = n_sku_pathed - n_sku_matrix
        lines.append(
            f"- **细类分析范围**：**{n_sku_matrix}** 个 SKU 具备参与**第五至第八章**分析所需的**商品详情页类目路径**"
            f"（且能读出常见细类名称，如饼干、挂面等）；另有 **{n_sku_nop}** 个商品缺少该信息、"
            f"**{n_sku_unparsed}** 个虽有路径但读不出细类名称，**未纳入**细类矩阵与按细类的评价统计。"
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
            "- **价格**：自页面「标价 / 券后价 / 详情价」等抽取的**展示价**，含促销与规格差异，**不等于**出厂价或成本。**第六章** 在具备可用的搜索列表导出时，优先以**列表全量**统计；否则使用**已深入 SKU** 的合并数据；**第六章第一节** 归纳标价与券后价差等**列表侧展示价差信号**（不对卖点/腰带字段做预设关键词扫描）。",
            "- **品牌/店铺集中度（第四章）**：有列表全量时按列表行计店铺与品牌占比；无列表导出时按深入 SKU 合并表估算。",
            "- **评价主题词**：对评价正文做**预设词表子串计数**，非分词主题模型，适合扫方向，**需抽样人工验证**。",
            "- **用途/场景**：对每条评价独立判断是否命中预设场景词；一条可计入多个场景，统计的是「提及该场景的评价条数」而非用户数。",
            (
                "- **用户画像（第八章）**：正负面粗判含**口语短语**级摘录；**第八章第三节**另用分词、词频与主题模型等对评论做**补充分析**（与**第八章第二节**条形图口径不同、互为补充），可选词云并由大模型归纳要点。"
                if _ch8_probe_sec
                else "- **用户画像（第八章）**：正负面粗判含**口语短语**级摘录；关注词与场景**仅按细类**以**同图左右并列**展示（左为关注词命中次数，右为场景占有效文本 **%**）；见**第八章第三节**。"
            ),
            "- **细类划分（第五至第八章）**：**仅**依据合并表中的**商品详情页类目路径**；该信息缺失或无法读出细类名称的 SKU **不参与**竞品矩阵与按细类评价统计（相关评价条亦**不进入**按细类图表）。",
            "- **检索结果规模**：来自京东 PC 搜索返回的「结果条数」类指标，表示平台侧申报的匹配数量级，**不等于**动销、库存或独立 SKU 数。",
            "",
            "### 1.4 主要局限",
            "",
            "- 仅覆盖 **京东 PC**，不含天猫、抖音、线下、B2B 原料端。",
            "- 样本量由本次抓取上限与搜索页数决定，**结论外推需谨慎**。",
            "- 详情配料与宣称以页面展示为准，**与真实配方可能不一致**（合规与实测另议）。",
            (
                "- **行业零售额、TAM、CAGR 等**：无法从本批次数据推导；本报告已纳入任务中配置的第三方摘录，见 **第三章第五节**。"
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
                f"竞争结构（{src}，第四章）：**店铺** 第一大店铺份额 ≈ **{100 * cr1_shop:.1f}%**（「{top_shop_s}」），"
                f"前三店铺合计份额 ≈ **{100 * cr3_shop:.1f}%**（按列表行计，同一 SKU 多行会重复计）。"
            )
        else:
            exec_bullets.append(
                f"竞争结构（{src}，第四章）：**店铺** 第一大店铺份额 ≈ **{100 * cr1_shop:.1f}%**（「{top_shop_s}」）。"
            )
    elif not list_export and cr1_deep is not None and top_brand_deep:
        if cr3_deep is not None:
            exec_bullets.append(
                f"竞争结构（无列表导出，第四章用深入合并表）：**品牌** 第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」），"
                f"前三品牌合计份额 ≈ **{100 * cr3_deep:.1f}%**。"
            )
        else:
            exec_bullets.append(
                f"竞争结构（无列表导出，第四章用深入合并表）：**品牌** 第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」）。"
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
            f"列表导出缺少品牌标题字段，**深入 {n_sku} SKU** 商详品牌第一大品牌份额 ≈ **{100 * cr1_deep:.1f}%**（「{top_brand_deep}」），供与第五章矩阵对照。"
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
                f"列表侧约 **{100.0 * float(sh):.0f}%** 可对齐行呈现「券后/到手」**低于**「标价」，展示价差中位数约 **{float(med):.1f}%**（**第六章第一节** 活动与话术摘录）。"
            )
    if multi_feedback_cat and (hits or scen_n_texts > 0):
        exec_bullets.append(
            "评价侧写（关注词、用途/场景）已按**第五章同一细类划分**分节，见**第八章第三节**（同图并列）。"
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
            f"PC 搜索返回的检索结果规模约 **{api_rc:,}**（站内匹配条数量级，见第三章第二节；**不是**零售额或动销统计）。"
        )
    for b in exec_bullets:
        lines.append(f"- {b}")
    if not exec_bullets:
        lines.append("- 当前批次可汇总要点较少（以正文各节实际输出为准）。")

    proxy = _search_list_proxies(search_export_rows) if search_export_rows else {}
    lines.extend(["", "---", "", "## 三、整体市场观察（渠道可见度参考，非官方市场规模）", ""])
    lines.extend(
        [
            "### 3.1 与「市场规模」的区别",
            "",
            "- **官方/行业市场规模**（如全国零售额、品类增速、渗透率）通常来自 **Euromonitor、行业协会、上市公司年报、券商研报** 等；**不能**用京东搜索返回条数或 SKU 数直接等同。",
            "- **第三章第二节** 使用搜索接口返回的**结果条数**；**第三章第三、四节** 描述本次导出的列表行、去重 SKU/店铺及列表价，仅作**参照**，外推全市场需谨慎。",
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
                f"- **列表中去重叶子类目代码/片段数**（粗略）：**{proxy['unique_leaf_cats']}**（同一关键词下品类宽度的参考）。",
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
            "*未读到可用的搜索列表导出或文件为空；第三章第三、四节无列表侧数据。*"
        )
        lines.append("")
        lines.extend(["### 3.4 列表端展示价（全导出）", "", "*无列表数据。*", ""])

    if external_rows:
        lines.extend(
            [
                "### 3.5 外部市场规模与行业信息（运行配置摘录）",
                "",
                "以下为本次任务报告调参中维护的**第三方市场摘录**，可与第三章第二节检索规模及第三章第三、四节列表参照对照使用；**指标含义与真实性以原出处为准**。",
                "",
                "| 指标 | 数值与说明 | 来源 | 年份 |",
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
            f"基于**搜索列表导出**共 **{n_structure}** 行，与第三章第三节一致；"
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
                "品牌列表曝光占比（扇形图；按整理后的品牌名计数，与结构化摘要中的品牌占比统计一致；"
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
            f"低于建议阈值（≥{min_brand_rows}），品牌集中度未展开。**店铺结构见第四章第二节**；"
            f"商详品牌在**第五章**。*"
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
                "店铺列表曝光占比（扇形图；按整理后的店铺名计数，与结构化摘要中的店铺占比统计一致；"
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

    lines.extend(["### 4.3 细分类目分布（深入合并表 · 与第五章矩阵同一细类划分）", ""])
    if cm_structure and n_sku_matrix > 0:
        lines.extend(
            _embed_chart(
                run_dir,
                "chart_category_mix_pie.png",
                "细类标签分布（扇形图；依据合并表中的商品详情页类目路径，与第五章一致；"
                "Top 12 以外的细类在统计时并入「（其余细类）」；扇形图内再合并为「其他」）",
            )
        )
        lines.append(
            "*完整类目分布见界面「数据摘要」或简报包中的数据文件。*"
        )
    else:
        lines.append(
            "*深入合并表中无具备可解析商品详情页类目路径的 SKU，本小节不展示扇形图；请核对商详抓取与合并字段。*"
        )
    lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 五、竞品对比矩阵（按细分类目分组）",
            "",
            "分组**仅**依据合并表中的**商品详情页类目路径**（京东商详中的类目层级）：**三级路径**取中间一段（如 … > **饼干** > 粗粮饼干），"
            "**四级及以上**取倒数第二段（如 … > **面条** > 挂面）。**路径缺失**或各段均为内部编码、**读不出常见细类名称**的 SKU **不进入**本矩阵，亦**不参与**第八章按细类的评价统计。",
            "",
            "**读图方式**：每个细类下为**并列横向条形图**（左：**展示价**（元）；右：**销量**（来自搜索列表页「已售」等销量文案，如「已售50万+」计为 **50 万**）），"
            "纵轴为**产品标题**（与本节各附图一致）。**SKU、店铺、配料与评价摘要等明细不列入正文**，详见本批次导出的合并数据表。",
            "",
        ]
    )
    grouped_matrix = _merged_rows_grouped_for_matrix(merged_rows)
    if not grouped_matrix:
        if merged_rows:
            lines.append(
                "*深入合并表有条目，但均无可用商品详情页类目路径（或路径无法解析为可读细类），故无法生成细类矩阵；"
                "第五至第八章中依赖矩阵的按细类统计相应为空。请核对商详抓取与合并字段。*"
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
                f"「{_md_cell(gname, 20)}」· 展示价与销量（页面「已售」销量文案）；纵轴为产品标题。",
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
                "> **说明**：与第五章相同的细类划分下归纳卖点与配料共性；**具体 SKU、价格与条形图以正文为准**，SKU 级明细见合并表 CSV。",
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
                "#### 细类价盘要点归纳（大模型，与第六章量化表互补）",
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
                "#### 细类促销与活动要点归纳（大模型，与第六章第一节及价盘互补）",
                "",
                "> **说明**：依据合并表「促销摘要」及榜单相关字段（如「榜单排名」「榜单类文案」）等**页面展示摘录**；"
                "不采用列表「卖点/腰带」类字段作归纳依据（多为固定词表匹配，口径偏粗）。归纳券/补贴/新人/榜单曝光等活动形态，**不**替代第五章的配料/宣称归纳。**具体以页面与 CSV 为准**。",
                "",
                _llm_po,
                "",
            ]
        )

    _sm_score = sentiment_lex.get("method") == "score_then_lexeme"
    _sec82_title = (
        "### 8.2 评价正负面粗判（评分优先 + 关键词回退）"
        if _sm_score
        else "### 8.2 评价正负面粗判（关键词规则）"
    )
    _sec82_block: list[str] = [
        "---",
        "",
        "## 八、消费者反馈与用户画像（按细分类目）",
        "",
        "### 8.1 方法",
        "",
        "- **细类划分**：与**第五章「竞品矩阵」**相同，**仅**依据合并表中的**商品详情页类目路径**解析为「饼干 / 西式糕点 / …」等（规则见第五章开头说明）。",
        "- **归因**：每条评价按其 SKU 对应到深入样本，再映射到该 SKU 所属细类；SKU 不在合并表中的评价单独归入说明性分组；**在合并表中但该 SKU 缺少类目路径或读不出细类名称的，该评价不进入按细类统计**（与第五章**同一条排除规则**）。",
        "- **正负面粗判（第八章第二节）**：若评价含有效「评分」列则**先按星级**粗分正负与中评，再在对应子集内统计口语短语；无评分时仍按关键词子串；若任务开启**大模型评价情感分析**，可附**大模型对抽样原文的主题归因**，与条形图互补。",
        (
            "- **文本补充分析（第八章第三节）**：本任务已用中文分词与统计工具做了开放词表分析（词频、关键词突出度、词对共现、主题归纳等，可选词云），与**第八章第二节**规则词表条形图**不同**、**互补**；**不再**输出原「关注词次数 + 场景占比」左右并列条图。"
            if _ch8_probe_sec
            else "- **关注词与使用场景（第八章第三节）**：对组内评价正文做关注词子串计数（左栏条形图）；对每条有效文本独立扫描**本次任务生效的场景词组**（来自报告调参或系统默认），一条可属多场景，右栏为**占该细类有效文本比例 %**（多标签下可相加 **>** 100%）。二者在 **同一张图左右并列**，与第五章矩阵细类一一对应。"
        ),
        "",
        _sec82_title,
        "",
        f"- **有效文本条数**：{sentiment_lex.get('text_units', 0)}（与第八章第一节**归因规则**一致）。",
    ]
    if _sm_score:
        _sec82_block.append(
            "- **正负面粗判规模**：本批存在有效「评分」时——**1～2 星**计为偏负向，**4～5 星**计为偏正向，**3 星**计为中评，**空文本**计为中性；"
            "无评分的条仍按关键词子串划分；「混合」仅在**无评分**且同条兼含正/负关键词时出现。"
        )
    _sec82_block.extend(
        [
            f"- **偏正向**：{sentiment_lex.get('positive_only', 0)} 条"
            + ("（主要为 4～5 星）" if _sm_score else "（仅命中正向词表）")
            + "；"
            f"**偏负向**：{sentiment_lex.get('negative_only', 0)} 条"
            + ("（主要为 1～2 星）" if _sm_score else "（仅命中负向词表）")
            + "；"
            f"**混合**：{sentiment_lex.get('mixed_positive_and_negative', 0)} 条"
            + ("（无评分且同条兼含正/负关键词）" if _sm_score else "（同条兼含正/负词）")
            + "；"
            f"**中性或空文本**：{sentiment_lex.get('neutral_or_empty', 0)} 条"
            + ("（含 3 星中评及无关键词命中）" if _sm_score else "")
            + "。",
            "- **说明**："
            + (
                "星级与正文可能不一致（如五星长文吐槽）；口语短语条形图仅在对应星级子集内统计；正式结论请**人工抽样**阅读原文。"
                if _sm_score
                else "词表为方向性粗判，讽刺、省略与错别字会导致误判；正式结论请**人工抽样**阅读原文。"
            ),
        ]
    )
    lines.extend(_sec82_block)
    _scope = (sentiment_lex.get("lexeme_scope_note") or "").strip()
    if _scope:
        lines.append(f"- **词根统计说明**：{_scope}")
    lines.extend(["", ""])
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_sentiment_overview_pie.png",
            "评价正负面粗判规模（扇形图；与上表条数一致）",
        )
    )
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_positive_lexemes_bar.png",
            (
                "正向评价里**最常出现的口语短语**（在 **4～5 星** 评价条内统计；条形图）"
                if _sm_score
                else "正向评价里**最常出现的口语短语**（在偏正向或混合评价条内统计；条形图）"
            ),
        )
    )
    lines.extend(
        _embed_chart(
            run_dir,
            "chart_negative_lexemes_bar.png",
            (
                "负向评价里**最常出现的口语短语**（在 **1～2 星** 评价条内统计；条形图）"
                if _sm_score
                else "负向评价里**最常出现的口语短语**（在偏负向或混合评价条内统计；条形图）"
            ),
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
                "> **说明**：基于与上节**同一套评分优先或关键词归类规则**抽样的评价原文，由大模型归纳**用户在说什么**（尤其是负向的具体事由），与上列条数、条形图**互补**；引文以原评论为准。",
                "",
                _llm_s,
            ]
        )
    lines.append("")
    if _ch8_probe_sec:
        lines.extend(
            [
                "### 8.3 评论文本补充分析（词频、关键词与共现、主题归纳）",
                "",
                "> **说明**：与**第八章第二节**口语短语条形图（规则词表）**口径不同**、**互补**；**不再**输出本章原「关注词 + 场景」左右并列条图；插图位于本批次报告附图文件夹中。",
                "",
                _ch8_probe_sec,
                "",
            ]
        )
    else:
        # 仅当未嵌入第八章第三节补充分析（_ch8_probe_sec 为空）时：原「关注词 + 场景」条图与逐细类段落
        lines.extend(
            [
                "### 8.3 关注词与使用场景（按细类）",
                "",
                "每细类一张**左右并列图**（与报告附图文件夹中的 ``chart_focus_and_scenarios_bar__*.png`` 同源）："
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
                    "#### 使用场景要点归纳（大模型，与第八章第三节右栏图表互补）",
                    "",
                    "> **说明**：与第八章第三节**相同**的预设场景词组与子串命中规则；**各场景条数与占比以正文图右栏为准**。",
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
                    "#### 细类评价与关注词要点归纳（大模型，与第八章第三节左栏图表互补）",
                    "",
                    "> **说明**：归纳各细类反馈主题与配置关注词命中；**次数与第八章第三节图左栏以正文为准**。",
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
    与 ``build_competitor_markdown`` 共用**同一套统计规则**，输出可 JSON 序列化的结构化竞品摘要（**规则驱动**，无 LLM）。
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

    comment_texts, comment_scores = _iter_comment_text_units_and_scores(
        comment_rows, merged_rows
    )
    comment_sentiment_lexicon = _comment_sentiment_lexicon(
        comment_texts, comment_scores
    )
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
            "与在线分析报告各章**计数规则**一致；关注词与场景以任务中的分析规则为准（子串命中统计，非深度主题模型）。",
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
