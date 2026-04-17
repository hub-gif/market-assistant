"""
市场策略 Markdown 草稿：**规则骨架**（占位 + 少量数据摘录），供业务与大模型成稿对齐。

- 决策在「策略生成」表单完成；未填项由大模型结合摘要与报告节选补全。
- 骨架刻意短、可执行；避免与成稿重复的「假设 / 待验证」套话。
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


def report_uses_chapter8_text_mining_probe(report_config: dict[str, Any] | None) -> bool:
    """
    与任务 ``report_config`` 中 ``chapter8_text_mining_probe`` 一致；未显式设置时默认 ``True``
    （与 ``jd.runner.get_default_report_config`` 一致）。
    为 ``True`` 时，策略稿第五节不再逐条列举关注词/场景子串命中，以免与当前报告正文口径冲突。
    """
    if not isinstance(report_config, dict):
        return True
    if "chapter8_text_mining_probe" in report_config:
        return bool(report_config.get("chapter8_text_mining_probe"))
    return True


def build_strategy_draft_markdown(
    *,
    job_id: int,
    keyword: str,
    brief: dict[str, Any],
    business_notes: str = "",
    generated_at_iso: str = "",
    strategy_decisions: dict[str, Any] | None = None,
    report_config: dict[str, Any] | None = None,
) -> str:
    """生成可下载的 Markdown：策略骨架为主，附录为数据速览。"""
    use_ch8_probe = report_uses_chapter8_text_mining_probe(report_config)
    d = strategy_decisions or {}
    pos = _esc(d.get("positioning_choice") or "").strip()
    kw = _esc(brief.get("keyword")) or _esc(keyword) or "—"
    lines: list[str] = [
        f"# 市场策略制定草稿 · 「{kw}」",
        "",
        "> **骨架说明**：本页为**规则骨架**（占位与少量摘录）。勾选大模型生成时，在骨架与数据上写**短、可执行**成稿。"
        "**决策在策略生成表单完成**；未填项由模型结合本任务摘要与报告节选补全，**成稿不再写「请再选 / 请决策」式套话**。",
        "",
    ]
    if generated_at_iso:
        lines.append(f"> **生成时间**：{_esc(generated_at_iso)}  ·  **任务 ID**：{job_id}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## 一、目标与边界",
            "",
            _goal_bullet("本品角色", str(d.get("product_role") or ""), "新品 / 追赶 / 防守 / 拓品类 …"),
            _goal_bullet("时间范围", str(d.get("time_horizon") or ""), "如：本季度 / 未来 12 周"),
            _goal_bullet(
                "成功标准（可量化）",
                str(d.get("success_criteria") or ""),
                "如：搜索位次、转化率、声量、复购 …",
            ),
            _goal_bullet("非目标", str(d.get("non_goals") or ""), "明确不做什么（可选）"),
            _goal_bullet(
                "目标客群",
                str(d.get("audience_segment") or ""),
                "一句话：为谁、什么场景（可选）",
            ),
            _goal_bullet(
                "主要对标",
                str(d.get("competitor_reference") or ""),
                "品牌或价位带参照（可选）",
            ),
            _goal_bullet(
                "资源与预算备注",
                str(d.get("resource_notes") or ""),
                "人力、投放、产能等量级（可选）",
            ),
            "",
        ]
    )

    scope = brief.get("scope") or {}
    merged_n = scope.get("merged_sku_count")
    comm_n = scope.get("comment_flat_rows")
    lines.extend(
        [
            "## 二、战场与样本",
            "",
            f"- **监测关键词 / 货架语境**：{kw}",
            f"- **批次**：{_esc(brief.get('batch_label')) or '—'}",
        ]
    )
    if merged_n is not None or comm_n is not None:
        lines.append(
            f"- **深入样本**：深入 SKU ≈ {_num(merged_n)}；评价扁平条数 ≈ {_num(comm_n)}。"
        )
    bf = _esc(d.get("battlefield_one_line") or "").strip()
    if bf:
        lines.append(f"- **一句话战场**：{bf}")
    else:
        lines.append(
            "- **一句话战场**：*（在哪个需求场景、与谁抢同一批用户？）*"
        )
    lines.append("")

    conc = brief.get("concentration") or {}
    shops = conc.get("shops_from_list") or {}
    dbrand = conc.get("detail_brand_among_merged") or {}
    lines.extend(["## 三、竞争结构（摘录）", ""])
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
        lines.append("*本摘要未含集中度指标，请结合本批次竞争结构数据补全。*")
    lines.extend(
        [
            "",
            "- **自测**：头部已占稳心智时，侧翼还是正面替代？分散时是否用细分场景或内容教育切入？",
            "",
        ]
    )
    stance = _esc(d.get("competitive_stance") or "").strip()
    stance_line = {
        "flank": "- **本品倾向**：侧翼切入，避免与头部正面硬碰。",
        "head_on": "- **本品倾向**：正面替代，对标头部主战场。",
        "both": "- **本品倾向**：分层推进（部分场景侧翼、部分场景正面）。",
        "undecided": "- **本品倾向**：*（表单未选；成稿时据数据写清倾向）*",
    }.get(stance)
    if stance_line:
        lines.append(stance_line)
        lines.append("")

    mix = brief.get("category_mix_top") or []
    if mix:
        lines.append("### 类目结构（Top）")
        lines.append("")
        for item in mix[:6]:
            if isinstance(item, dict):
                lines.append(f"- {_esc(item.get('label'))}：{_num(item.get('count'))}")
        lines.append("")

    pst = brief.get("price_stats") or {}
    lines.extend(["## 四、价格带与定位（业务已选）", ""])
    if pst.get("n"):
        src = _esc(brief.get("price_stats_source")) or "—"
        lines.extend(
            [
                f"- **价格来源**：{src}，有效价样本 n = {_num(pst.get('n'))}。",
                f"- **展示价区间**：{_num(pst.get('min'))} ～ {_num(pst.get('max'))}；**中位数** {_num(pst.get('median'))}。",
                "",
                "**主定位（与表单一致）**",
                "",
                f"- {_pos_mark(pos, 'top')} **贴顶**：中高位或头部价位带，强调品质/成分/背书。",
                f"- {_pos_mark(pos, 'mid')} **卡腰**：围绕中位数一带，强调性价比与场景匹配。",
                f"- {_pos_mark(pos, 'entry')} **下探**：贴近区间下限，强调入门与拉新（注意毛利与调性）。",
                f"- {_pos_mark(pos, 'different')} **另起带**：避开主价格带，用规格/组合/服务差异化。",
                "",
            ]
        )
    else:
        lines.append("*摘要中无价带统计，请结合本批次价格数据补全。*")
        lines.append("")
        lines.extend(
            [
                "**主定位（与表单一致）**",
                "",
                f"- {_pos_mark(pos, 'top')} **贴顶**：中高位或头部价位带，强调品质/成分/背书。",
                f"- {_pos_mark(pos, 'mid')} **卡腰**：围绕中位数一带，强调性价比与场景匹配。",
                f"- {_pos_mark(pos, 'entry')} **下探**：贴近区间下限，强调入门与拉新（注意毛利与调性）。",
                f"- {_pos_mark(pos, 'different')} **另起带**：避开主价格带，用规格/组合/服务差异化。",
                "",
            ]
        )

    ckw = brief.get("comment_focus_keywords") or []
    usc = brief.get("usage_scenarios") or []
    lines.extend(["## 五、用户与评论侧", ""])
    if use_ch8_probe:
        lines.extend(
            [
                "*当前任务报告以**第八章评论侧文本挖掘**为主呈现时，此处**不**逐条罗列子串命中次数（与报告 §8 口径一致）。*",
                "",
                "- **需求焦点**：*（骨架占位；成稿写 1～2 句可执行结论）*",
                "- **场景侧重**：*（骨架占位）*",
                "- **传播切入点**：*（骨架占位）*",
                "",
            ]
        )
    else:
        lines.append("*下列为关注词/场景**统计摘录**（口径同本批次摘要）；成稿时写成具体动作，勿重复统计句。*")
        lines.append("")
        if ckw:
            for item in ckw[:8]:
                if isinstance(item, dict):
                    w = _esc(item.get("word"))
                    c = _num(item.get("count"))
                    lines.append(
                        f"- 「{w}」：子串统计命中约 **{c}** 次（口径同报告关注词）。"
                    )
        if usc:
            for item in usc[:6]:
                if isinstance(item, dict):
                    sc = _esc(item.get("scenario"))
                    cn = _num(item.get("count"))
                    sh = _pct(item.get("share_of_text_units"))
                    lines.append(
                        f"- 场景「{sc}」：约 **{cn}** 条，约占 **{sh}** 文本单元（预设场景分组）。"
                    )
        if not ckw and not usc:
            lines.append("*摘要中无关注词/场景组，请结合评论侧分析补全本节。*")
    lines.append("")

    hints = brief.get("strategy_hints") or []
    lines.extend(
        [
            "## 六、机会与策略支柱",
            "",
            "### 摘要提示（`strategy_hints`）",
            "",
        ]
    )
    if hints:
        for h in hints:
            lines.append(f"- {_esc(h)}")
    else:
        lines.append("*（当前无自动线索）*")
    pp = str(d.get("pillar_product") or "")
    pr = str(d.get("pillar_price") or "")
    pch = str(d.get("pillar_channel") or "")
    pcm = str(d.get("pillar_comm") or "")
    lines.extend(
        [
            "",
            "### 策略支柱（表单已填优先）",
            "",
            "| 支柱 | 本品动作 | 与头部差异 | 证据 / 出处 |",
            "|------|----------|------------|-------------|",
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
    rk_kw = (
        "评论侧归纳是否以偏概全？（需原评论抽样）"
        if use_ch8_probe
        else "关注词/场景统计是否以偏概全？（需原评论抽样）"
    )
    lines.extend(
        [
            "## 七、风险核对项",
            "",
            _risk_line(rk, rk_kw),
            _risk_line(rp, "价格带是否含大促/异常挂价？（需核对清洗规则）"),
            _risk_line(rc, "列表集中度与深入样本品牌是否不一致？（需解释渠道差异）"),
            "",
        ]
    )

    notes = _esc(business_notes)
    lines.extend(
        [
            "## 八、业务约束与备注",
            "",
            (notes if notes else "*（未填写业务备注。）*"),
            "",
        ]
    )

    lines.extend(
        [
            "## 九、下一步（可执行）",
            "",
            "- [ ] 对齐 **§一** 目标与 **§八** 约束，锁定 1～2 条主命题。",
            "- [ ] 为 **§六** 支柱各补 **1 条数据证据** + **12 周内可交付动作**（负责人 + 时间）。",
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
