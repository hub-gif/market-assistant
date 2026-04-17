"""报告 Markdown 片段：Mermaid、场景摘要、规则策略提示、插图路径与解读段落。"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

from .csv_io import _md_cell


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


__all__ = [
    "_embed_chart",
    "_focus_scenario_combo_bar_filename",
    "_lines_4_reading_brand",
    "_lines_4_reading_shop",
    "_matrix_prices_sales_chart_filename",
    "_mermaid_pie_focus_keywords",
    "_scenario_group_asset_slug",
    "_scenario_summary_bullets",
    "_strategy_hints",
]
