"""
市场策略 Markdown 草稿：侧重**策略制定框架**（目标、战场、定位、支柱、行动），
基于同任务结构化摘要与可选业务备注规则生成；附录为关键数据速览。

后续可接 LLM 润色；当前无模型调用，便于验收与追溯。
"""
from __future__ import annotations

import math
from typing import Any

from .brief_concentration import (
    concentration_first_share,
    concentration_top_three_share,
)


def _esc(s: Any) -> str:
    t = "" if s is None else str(s).strip()
    return t.replace("\r\n", "\n").replace("\r", "\n")


def _pct(x: Any) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return "—"
        return f"{100 * v:.1f}%"
    except (TypeError, ValueError):
        return "—"


def _num(x: Any) -> str:
    if x is None:
        return "—"
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return "—"
        if x == int(x):
            return str(int(x))
        return f"{x:.2f}"
    return str(x)


def _cr_narrative(label: str, cr1: Any, cr3: Any, top: Any) -> str | None:
    """从集中度生成一句策略向描述，无数据则返回 None（正文避免英文缩写）。"""
    try:
        c1 = float(cr1) if cr1 is not None else None
    except (TypeError, ValueError):
        c1 = None
    if c1 is None and not (top or "").strip():
        return None
    top_s = _esc(top) or "—"
    if "店铺" in label:
        w1, w3 = "第一大店铺约占列表行的", "前三大店铺合计约占"
    elif "品牌" in label:
        w1, w3 = "第一大品牌约占", "前三大品牌合计约占"
    else:
        w1, w3 = "第一大主体约占", "前三大合计约占"
    if c1 is not None:
        if c1 >= 0.4:
            tone = "偏高，头部资源集中"
        elif c1 >= 0.25:
            tone = "中等，存在可争夺空间"
        else:
            tone = "相对分散，差异化切入点可能更多"
        return (
            f"- **{label}**：{w1} **{_pct(cr1)}**，{w3} **{_pct(cr3)}**；"
            f"当前头部为「{top_s}」。*粗判：{tone}。*"
        )
    return f"- **{label}**：头部为「{top_s}」（缺少占比时可结合列表与商详数据补全）。"


def _goal_bullet(label: str, user_val: str, placeholder: str) -> str:
    v = _esc(user_val).strip()
    if v:
        return f"- **{label}**：{v}"
    return f"- **{label}**：*（{placeholder}）*"


def _pillar_cell(user_val: str) -> str:
    v = _esc(user_val).strip()
    return v if v else "*待填*"


def _pos_mark(choice: str, key: str) -> str:
    return "[x]" if choice == key else "[ ]"


def _risk_line(checked: bool, text: str) -> str:
    mark = "[x]" if checked else "[ ]"
    return f"- {mark} {text}"


def build_strategy_draft_markdown(
    *,
    job_id: int,
    keyword: str,
    brief: dict[str, Any],
    business_notes: str = "",
    generated_at_iso: str = "",
    strategy_decisions: dict[str, Any] | None = None,
) -> str:
    """生成可下载的 Markdown：策略框架为主，附录为数据速览。"""
    d = strategy_decisions or {}
    pos = _esc(d.get("positioning_choice") or "").strip()
    kw = _esc(brief.get("keyword")) or _esc(keyword) or "—"
    lines: list[str] = [
        f"# 市场策略制定草稿 · 「{kw}」",
        "",
        "> 本稿用于**辅助制定市场策略**；由规则根据本批次结构化摘要与业务备注生成，**非大模型自由发挥**，定稿前请业务修订。",
        "",
    ]
    if generated_at_iso:
        lines.append(f"> **生成时间**：{_esc(generated_at_iso)}  ·  **任务 ID**：{job_id}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 一、战略背景与目标（请业务补全）",
            "",
            _goal_bullet("本品角色", str(d.get("product_role") or ""), "新品 / 追赶 / 防守 / 拓品类 …"),
            _goal_bullet("时间范围", str(d.get("time_horizon") or ""), "如：本季度 / 未来 12 周"),
            _goal_bullet(
                "成功标准（可量化）",
                str(d.get("success_criteria") or ""),
                "如：搜索位次、转化率、声量、复购 …",
            ),
            _goal_bullet("非目标（明确不做什么）", str(d.get("non_goals") or ""), "可选"),
            "",
        ]
    )

    scope = brief.get("scope") or {}
    merged_n = scope.get("merged_sku_count")
    comm_n = scope.get("comment_flat_rows")
    lines.extend(
        [
            "## 二、战场界定（监测语境）",
            "",
            f"- **监测关键词 / 货架语境**：{kw}",
            f"- **批次**：{_esc(brief.get('batch_label')) or '—'}",
        ]
    )
    if merged_n is not None or comm_n is not None:
        lines.append(
            f"- **深入样本规模**：深入 SKU ≈ {_num(merged_n)}；评价扁平条数 ≈ {_num(comm_n)}。"
            "*策略含义：样本越大，以下「假设」越需抽样复核原评论。*"
        )
    bf = _esc(d.get("battlefield_one_line") or "").strip()
    if bf:
        lines.append(f"- **一句话战场**：{bf}")
    else:
        lines.append(
            "- **一句话战场**：*（请用业务语言写：我们在哪个需求场景、与谁抢同一批用户？）*"
        )
    lines.append("")

    conc = brief.get("concentration") or {}
    shops = conc.get("shops_from_list") or {}
    dbrand = conc.get("detail_brand_among_merged") or {}
    lines.extend(["## 三、竞争格局 → 策略含义", ""])
    n_shop = _cr_narrative(
        "列表侧店铺集中度",
        concentration_first_share(shops),
        concentration_top_three_share(shops),
        shops.get("top_label"),
    )
    n_brand = _cr_narrative(
        "深入样本内品牌集中度",
        concentration_first_share(dbrand),
        concentration_top_three_share(dbrand),
        dbrand.get("top_label"),
    )
    if n_shop:
        lines.append(n_shop)
    if n_brand:
        lines.append(n_brand)
    if not n_shop and not n_brand:
        lines.append("*本摘要未含集中度指标，请结合本批次竞争结构数据补全后再写判断。*")
    lines.extend(
        [
            "",
            "**可下判断的提问（自测）**",
            "",
            "- 若头部已占稳心智，本品是**侧翼**还是**正面替代**？",
            "- 店铺/品牌分散时，是否适合用**细分场景**或**内容教育**切入？",
            "",
        ]
    )
    stance = _esc(d.get("competitive_stance") or "").strip()
    stance_line = {
        "flank": "- **本品倾向**：倾向**侧翼切入**，避免与头部正面硬碰。",
        "head_on": "- **本品倾向**：倾向**正面替代**，对标头部主战场。",
        "both": "- **本品倾向**：计划**分层推进**（部分场景侧翼、部分场景正面）。",
        "undecided": "- **本品倾向**：**尚未拍板**，需在会议中对齐后再定主战场叙事。",
    }.get(stance)
    if stance_line:
        lines.append(stance_line)
        lines.append("")

    mix = brief.get("category_mix_top") or []
    if mix:
        lines.append("### 类目结构提示（Top）")
        lines.append("")
        lines.append("*以下仅作「货架长什么样」的速记。*")
        for item in mix[:6]:
            if isinstance(item, dict):
                lines.append(f"- {_esc(item.get('label'))}：{_num(item.get('count'))}")
        lines.append("")

    pst = brief.get("price_stats") or {}
    lines.extend(["## 四、价格带与定位选项（启发式）", ""])
    if pst.get("n"):
        src = _esc(brief.get("price_stats_source")) or "—"
        lines.extend(
            [
                f"- **价格来源**：{src}，有效价样本 n = {_num(pst.get('n'))}。",
                f"- **展示价区间**：{_num(pst.get('min'))} ～ {_num(pst.get('max'))}；**中位数** {_num(pst.get('median'))}。",
                "",
                "**定位选项（请勾一条或改写，并写明理由）**",
                "",
                f"- {_pos_mark(pos, 'top')} **贴顶**：对标中高位或头部价位带，强调品质/成分/背书。",
                f"- {_pos_mark(pos, 'mid')} **卡腰**：围绕中位数一带，强调性价比与场景匹配。",
                f"- {_pos_mark(pos, 'entry')} **下探**：贴近区间下限，强调入门与拉新（注意毛利与品牌调性）。",
                f"- {_pos_mark(pos, 'different')} **另起带**：刻意避开主价格带，用规格/组合/服务差异化。",
                "",
            ]
        )
    else:
        lines.append("*摘要中无价带统计，请结合本批次价格相关数据补全后再填上表。*")
        lines.append("")
        lines.extend(
            [
                "**定位选项（请勾一条或改写，并写明理由）**",
                "",
                f"- {_pos_mark(pos, 'top')} **贴顶**：对标中高位或头部价位带，强调品质/成分/背书。",
                f"- {_pos_mark(pos, 'mid')} **卡腰**：围绕中位数一带，强调性价比与场景匹配。",
                f"- {_pos_mark(pos, 'entry')} **下探**：贴近区间下限，强调入门与拉新（注意毛利与品牌调性）。",
                f"- {_pos_mark(pos, 'different')} **另起带**：刻意避开主价格带，用规格/组合/服务差异化。",
                "",
            ]
        )

    ckw = brief.get("comment_focus_keywords") or []
    usc = brief.get("usage_scenarios") or []
    lines.extend(["## 五、用户需求与场景 — 可写成策略的假设", ""])
    lines.append(
        "*下列由关注词/场景**计数**转化而来，是「待验证假设」而非结论；请结合评价原文抽样修订。*"
    )
    lines.append("")
    if ckw:
        for item in ckw[:8]:
            if isinstance(item, dict):
                w = _esc(item.get("word"))
                c = _num(item.get("count"))
                lines.append(
                    f"- **假设**：用户决策中「{w}」被频繁提及（约 {c} 次统计命中）—— "
                    f"*可追问：本品故事是否正面回应？传播关键词是否覆盖？*"
                )
    if usc:
        for item in usc[:6]:
            if isinstance(item, dict):
                sc = _esc(item.get("scenario"))
                cn = _num(item.get("count"))
                sh = _pct(item.get("share_of_text_units"))
                lines.append(
                    f"- **场景命题**：「{sc}」在预设场景中约 {cn} 条、约占 {sh} 文本单元—— "
                    f"*可追问：主图/详情/客服话术是否对齐该场景？*"
                )
    if not ckw and not usc:
        lines.append("*摘要中无关注词/场景组结果，请补全评论侧分析后再写本节。*")
    lines.append("")

    hints = brief.get("strategy_hints") or []
    lines.extend(
        [
            "## 六、机会方向与策略支柱（草案）",
            "",
            "### 规则引擎提示（来自摘要 `strategy_hints`）",
            "",
        ]
    )
    if hints:
        for h in hints:
            lines.append(f"- {_esc(h)}")
    else:
        lines.append("*（当前无自动线索，请结合本批次结论手写 3～5 条机会）*")
    pp = str(d.get("pillar_product") or "")
    pr = str(d.get("pillar_price") or "")
    pch = str(d.get("pillar_channel") or "")
    pcm = str(d.get("pillar_comm") or "")
    lines.extend(
        [
            "",
            "### 策略支柱 — 请业务逐项填空",
            "",
            "| 支柱 | 本品打算怎么做 | 与头部差异 | 证据 / 出处 |",
            "|------|----------------|------------|-------------|",
            f"| 产品 | {_pillar_cell(pp)} | *待填* | *§* |",
            f"| 价格 | {_pillar_cell(pr)} | *待填* | *§* |",
            f"| 渠道/触点 | {_pillar_cell(pch)} | *待填* | *§* |",
            f"| 传播与内容 | {_pillar_cell(pcm)} | *待填* | *§* |",
            "",
        ]
    )

    rk = bool(d.get("ack_risk_keywords"))
    rp = bool(d.get("ack_risk_price"))
    rc = bool(d.get("ack_risk_concentration"))
    lines.extend(
        [
            "## 七、风险与待证伪",
            "",
            _risk_line(rk, "关注词/场景是否**以偏概全**？（需原评论抽样）"),
            _risk_line(rp, "价格带是否含大促/异常挂价？（需核对清洗规则）"),
            _risk_line(rc, "列表集中度与深入样本品牌是否**矛盾**？（需解释渠道差异）"),
            "",
        ]
    )

    notes = _esc(business_notes)
    lines.extend(
        [
            "## 八、业务约束与内部判断",
            "",
            (notes if notes else "*（未填写。建议补充：渠道红线、价位策略、竞品对标名单、预算量级等。）*"),
            "",
        ]
    )

    lines.extend(
        [
            "## 九、建议下一步（策略向）",
            "",
            "- [ ] 开会对齐：**§一** 目标与 **§八** 约束，确认 1～2 条主策略命题。",
            "- [ ] 为 **§六** 策略支柱表格每一行各找 **1 条数据证据**（注明出处）。",
            "- [ ] 产出 **12 周节奏表**（里程碑 + 负责人），与本品排期挂钩。",
            "- [ ] 定义 **3 个可观测指标**（周或双周复盘）。",
            "",
            "---",
            "",
            "## 附录 · 本任务关键数据速览",
            "",
            f"- **关键词**：{kw}  ·  **摘要版本**：v{_num(brief.get('schema_version'))}",
        ]
    )
    meta = brief.get("meta")
    meta_labels = {
        "page_start": "起始页",
        "page_to": "采集至页",
        "max_skus_config": "SKU 上限",
        "scenario_filter_enabled": "场景筛选",
    }
    if isinstance(meta, dict) and meta:
        bits = []
        for k in ("page_start", "page_to", "max_skus_config", "scenario_filter_enabled"):
            if k in meta:
                label = meta_labels.get(k, k)
                bits.append(f"{label}={_esc(meta.get(k))}")
        if bits:
            lines.append(f"- **采集参数快照**：{'; '.join(bits)}")
    raw = brief.get("pc_search_raw") or {}
    if raw.get("result_count_consensus") is not None:
        lines.append(
            f"- **列表申报规模（resultCount）**：{_num(raw.get('result_count_consensus'))}"
        )
    lines.extend(
        [
            "",
            "*同目录含本批次 CSV 与分析产出，可对照使用。*",
            "",
            "---",
            "",
            "*本稿由工作台「市场策略制定」生成；与同任务结构化分析数据一致。*",
            "",
        ]
    )
    return "\n".join(lines)
