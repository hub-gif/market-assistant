"""
市场策略 Markdown 草稿：**规则骨架**（占位 + 必要表单项；不铺陈与报告同构的统计摘录），供业务与大模型成稿对齐。

- 决策在「策略生成」表单完成；未填项由大模型结合摘要与报告节选补全。
- 骨架刻意短、可执行；避免与成稿重复的「假设 / 待验证」套话。
"""
from __future__ import annotations

import math
from typing import Any

def _esc(s: Any) -> str:
    t = "" if s is None else str(s).strip()
    return t.replace("\r\n", "\n").replace("\r", "\n")


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


def _goal_bullet(label: str, user_val: str, placeholder: str) -> str:
    v = _esc(user_val).strip()
    if v:
        return f"- **{label}**：{v}"
    return f"- **{label}**：*（{placeholder}）*"


def _pillar_cell(user_val: str) -> str:
    v = _esc(user_val).strip()
    return v if v else "*待填*"


def _price_position_llm_hint(pos: str) -> str:
    """给 LLM 的 §8.2 提示：单行取向，避免成稿复刻四选项勾选表单。"""
    k = (pos or "").strip()
    if k == "top":
        core = "当前表单取向为贴顶：锚定中高位或头部价位带。"
    elif k == "mid":
        core = "当前表单取向为卡腰：围绕监测价带中位数一带。"
    elif k == "entry":
        core = "当前表单取向为下探：贴近监测价带区间下限。"
    elif k == "different":
        core = "当前表单取向为另起带：以规格、组合或服务形成差异化价位。"
    else:
        core = "表单未勾选价位取向；请结合 `structured_brief` 价带与业务判断补全。"
    return (
        f"- {core} 成稿 §8.2 仅用**连贯叙述句**展开定价逻辑；**禁止**四选项勾选清单、"
        "并排「贴顶/卡腰/下探/另起带」问卷式排版、「（表单勾选）」及类似内部提示语。"
    )


def _price_position_display_line(pos: str) -> str:
    """下载稿 §8.2：单行交待表单价位取向，不铺陈四勾选清单。"""
    k = (pos or "").strip()
    if k == "top":
        core = "**贴顶**（锚定中高位或头部价位带）"
    elif k == "mid":
        core = "**卡腰**（围绕监测价带中位数一带）"
    elif k == "entry":
        core = "**下探**（贴近监测价带区间下限）"
    elif k == "different":
        core = "**另起带**（规格/组合或服务差异化价位）"
    else:
        core = "（表单未勾选；请结合价带与业务判断）"
    return (
        f"- **价位取向（表单）**：{core}。"
        " 成稿 §8.2 用连贯叙述展开即可，不必复刻四选项勾选排版。"
    )


def _nine_ten_markdown_blocks(
    *,
    rk: bool,
    rp: bool,
    rc: bool,
    for_llm_input: bool,
) -> list[str]:
    """§九、§十：叙述式分条，避免勾选问卷体不利阅读。"""
    items: list[tuple[str, str, bool]] = [
        (
            "评论与归纳口径",
            "评论侧归纳是否存在以偏概全，宜结合原评论抽样核实。",
            rk,
        ),
        (
            "价格带与清洗规则",
            "价格带是否包含大促或异常挂价，宜核对数据清洗规则。",
            rp,
        ),
        (
            "列表曝光与深入样本",
            "列表侧集中度与深入样本中的品牌结构是否不一致，宜说明渠道或口径差异。",
            rc,
        ),
    ]
    out: list[str] = [
        "## 九、风险、假设与待验证",
        "",
    ]
    if for_llm_input:
        out.append(
            "*§9 须用**短段落或分条叙述**写风险、假设与验证或应对；**禁止** `[ ]`/`[x]` 勾选、问卷式排版，"
            "或仅堆疑问句而无动作。*"
        )
        out.append("")
    else:
        out.append(
            "*成稿：每条风险带**应对动作或验证计划**；下列为业务表单关注点。*"
        )
        out.append("")
    for title, body, checked in items:
        tag = (
            " *（业务已在表单中勾选「已知晓」，成稿须优先写清验证或应对。）*"
            if checked
            else ""
        )
        out.append(f"- **{title}**：{body}{tag}")
    out.append("")
    if not for_llm_input:
        out.extend(["*业务备注见下节。*", ""])
    out.extend(
        [
            "## 十、下一步与节奏",
            "",
        ]
    )
    if for_llm_input:
        out.append(
            "*§10 须列**可执行动作**（可补负责人/时间），与 §2.1 / §六 优先级一致；**禁止** `[ ]` 待办勾选格式。*"
        )
        out.append("")
    else:
        out.append("*成稿：可执行任务清单；可补负责人与时间。*")
        out.append("")
    out.extend(
        [
            "- 锁定主推款与对标，并完成法务与合规核对。",
            "- 统一对外数据口径与话术。",
            "- 下轮监测更新后迭代策略。",
            "",
        ]
    )
    return out


def filter_strategy_hints_for_ch8_probe(hints: Any) -> list[str]:
    """
    当报告以 **第八章文本挖掘** 为主呈现评论侧时，规则引擎的 ``strategy_hints`` 中仍可能含
    「关注词出现较多」「预设场景占比」类句子（与 §8 主口径冲突）。此处剔除，避免进入策略底稿与 LLM。
    """
    if not isinstance(hints, list):
        return []
    out: list[str] = []
    for h in hints:
        s = _esc(h) if h is not None else ""
        if not s.strip():
            continue
        if "评价文本中「" in s and "等主题出现较多" in s:
            continue
        if "用途/场景中「" in s and "有效评价自述" in s:
            continue
        out.append(s)
    return out if out else [
        "（与「关注词/预设场景条形图」相关的自动提示已省略；用户洞察请以报告 §8 文本挖掘及第五至第八章细类归纳为准；可执行策略以「策略制定」按细类生成为准。）"
    ]


def report_uses_chapter8_text_mining_probe(report_config: dict[str, Any] | None) -> bool:
    """
    与任务 ``report_config`` 中 ``chapter8_text_mining_probe`` 一致；未显式设置时默认 ``True``
    （与 ``jd.runner.get_default_report_config`` 一致）。
    用于 §1.2 短指引分支及对 ``strategy_hints`` 的过滤：开启探针时与子串命中枚举相关的自动线索会被压掉；
    关闭时 §1.2 仍提示「简报不附带预设关注词/场景子串统计枚举」。
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
    our_product_profile: str = "",
    generated_at_iso: str = "",
    strategy_decisions: dict[str, Any] | None = None,
    report_config: dict[str, Any] | None = None,
    for_llm_input: bool = False,
) -> str:
    """生成可下载的 Markdown：与「六主轴 + 品牌四线」示例稿同构的规则骨架，附录为数据速览。

    ``for_llm_input=True`` 时供大模型归纳用：弱化源码/路径/任务 ID 等痕迹，减少对外成稿误复述。
    """
    use_ch8_probe = report_uses_chapter8_text_mining_probe(report_config)
    d = strategy_decisions or {}
    pos = _esc(d.get("positioning_choice") or "").strip()
    kw = _esc(brief.get("keyword")) or _esc(keyword) or "—"
    batch = _esc(brief.get("batch_label")) or "—"
    if for_llm_input:
        lines: list[str] = [
            f"# 「{kw}」",
            "",
        ]
        if generated_at_iso:
            lines.append(
                f"> **生成时间**：{_esc(generated_at_iso)}（归纳用，**勿写入对外成稿正文**）"
            )
            lines.append("")
        lines.append(
            "> **规划 §1.1（归纳用）**：成稿须以「怎么做」为主体，不以「是什么」铺陈代替策略；摘要「阶段重点」须 1～2 条执行句；第一章背景控制在少量结论句。**勿将本行写入对外正文。**"
        )
        lines.append("")
    else:
        lines = [
            f"# 市场策略制定草稿 · 「{kw}」",
            "",
            "> **骨架说明**：本页为规则骨架；结构与 [`docs/demo`](docs/demo) 示例一致。**全局禁止编造**见 `generate_strategy.py` 中 `STRATEGY_DATA_RULES`。",
            "",
        ]
        if generated_at_iso:
            lines.append(f"> **生成时间**：{_esc(generated_at_iso)}  ·  **任务 ID**：{job_id}")
            lines.append("")

    scope = brief.get("scope") or {}
    merged_n = scope.get("merged_sku_count")
    comm_n = scope.get("comment_flat_rows")

    pr_role = _esc(d.get("product_role") or "").strip()
    bf_line = _esc(d.get("battlefield_one_line") or "").strip()
    aud = _esc(d.get("audience_segment") or "").strip()
    th = _esc(d.get("time_horizon") or "").strip()
    sc = _esc(d.get("success_criteria") or "").strip()

    def _scope_cell(val: str, placeholder: str) -> str:
        return val if val else f"*（{placeholder}）*"

    scope_prelude = (
        [
            "*回答：**这份策略是针对什么、在什么边界里做的**——属「立项靶心」，不是执行摘要。表单已填则写成短句；未填保留占位，**勿**编造。*",
            "*业务侧在动策略稿之前，应先对齐本节；阶段目标类型以业务内部规划口径为准（若有）。*",
            "",
        ]
        if not for_llm_input
        else []
    )
    _table_main_cat = (
        "*（待填：如饼干线 / 面包线 / 多线并行；未定写「待业务定类」）*"
        if not for_llm_input
        else "*（待确认）*"
    )
    sgt = _esc(d.get("stage_goal_type") or "").strip()
    _goal_type_placeholder = (
        "待确认：可填如拉新尝试、做销量与转化、守份额与复购、新品试水等业务阶段目标；与业务内部口径对齐"
        if for_llm_input
        else "成稿从业务侧阶段目标类型口径择一或组合，并与下列成功标准一致"
    )
    _table_goal_type = _scope_cell(sgt, _goal_type_placeholder)
    _summary_user_side = (
        "- **用户侧**：*（结论句：讨论焦点与负向主题；按细类分句；勿复述报告统计摘录。）*"
        if not for_llm_input
        else "- **用户侧**：—"
    )
    _summary_stage = (
        "- **阶段重点**：*（1～2 条可执行动作，点明类目/主推线。）*"
        if not for_llm_input
        else "- **阶段重点**：—"
    )
    lines.extend(
        [
            "## 策略范围与前提（生成前先对齐）",
            "",
            *scope_prelude,
            "| 须明确项 | 填写或待确认 |",
            "|----------|----------------|",
            f"| **监测任务（数据同源）** | 关键词「{kw}」；批次 **{batch}**；与同任务《竞品分析报告》一致 |",
            f"| **策略服务对象（本品角色）** | {_scope_cell(pr_role, '待填：新品 / 追赶 / 防守 / 拓品类 …')} |",
            f"| **一句话战场** | {_scope_cell(bf_line, '在哪个需求场景、与谁抢同一批用户？')} |",
            f"| **目标客群/场景** | {_scope_cell(aud, '可选')} |",
            f"| **主推类目/细类** | {_table_main_cat} |",
            f"| **本阶段策略目标类型** | {_table_goal_type} |",
            f"| **时间范围** | {_scope_cell(th, '如本季度 / 未来 12 周')} |",
            f"| **成功标准（可量化）** | {_scope_cell(sc, '搜索位次、转化、复购等')} |",
            "",
            "---",
            "",
            "## 摘要",
            "",
            f"- **范围与样本**：监测词「{kw}」；批次 **{batch}**；"
            + (
                f"深入 SKU ≈ {_num(merged_n)}；评价条数 ≈ {_num(comm_n)}。"
                if merged_n is not None or comm_n is not None
                else "样本规模见附录。"
            ),
            _summary_user_side,
            _summary_stage,
            "",
            "## 一、顾客是谁",
            "",
            "### 1.1 人群与决策路径",
            "",
            f"- **检索与货架语境**：{kw}；批次 {batch}。",
        ]
    )
    bf = _esc(d.get("battlefield_one_line") or "").strip()
    if bf:
        lines.append(f"- **一句话战场**：{bf}")
    else:
        lines.append("- **一句话战场**：*（在哪个需求场景、与谁抢同一批用户？）*")
    lines.extend(
        [
            (
                "- **典型路径**：搜索 → 列表比价 → 商详与配料 → 评价 → 下单/复购。"
                if for_llm_input
                else "- **典型路径**：*（成稿：搜索 → 列表比价 → 详情与配料 → 评价 → 下单/复购。）*"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿写清谁在何任务下检索与决策、主攻类目/细类。*",
                    "",
                ]
            ),
            "### 1.2 细类讨论焦点（评论文本分析）",
            "",
        ]
    )
    if use_ch8_probe:
        _sec12 = (
            "*评论侧以报告**第八章文本挖掘**为准；成稿在此按**细类各一两句**写讨论焦点，勿铺陈词频次数、共现、类目条数表等与报告重复的摘录。*"
            if for_llm_input
            else "*评论侧见报告第八章文本挖掘；成稿按细类各一两句，勿铺陈词频、共现、类目条数表等摘录。*"
        )
    else:
        _sec12 = (
            "*简报不附带预设关注词/场景子串统计枚举；评论侧见报告第八章及原文抽样；成稿按细类各一两句，勿铺陈统计摘录。*"
            if for_llm_input
            else "*简报中**不再**附带预设关注词/场景子串统计；评论侧见报告第八章；成稿按细类各一两句，勿铺陈与报告重复的统计摘录。*"
        )
    lines.append(_sec12)
    lines.append("")

    lines.extend(
        [
            (
                "### 1.3 本品聚焦"
                if for_llm_input
                else "### 1.3 本品聚焦（占位）"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿写清本期**主攻人群/场景/类目或主推细类**与 §2.1「类目/细类」列的对应关系。*",
                    "",
                ]
            ),
            _goal_bullet("本品角色", str(d.get("product_role") or ""), "新品 / 追赶 / 防守 / 拓品类 …"),
            _goal_bullet(
                "本阶段策略目标类型",
                str(d.get("stage_goal_type") or ""),
                "与业务内部阶段目标口径对齐，或自填（如拉新尝试、做销量）",
            ),
            _goal_bullet(
                "目标客群",
                str(d.get("audience_segment") or ""),
                "为谁、什么场景（可选）",
            ),
            _goal_bullet(
                "主要对标",
                str(d.get("competitor_reference") or ""),
                "品牌或价位带参照（可选）",
            ),
            "",
        ]
    )
    opp = (our_product_profile or "").strip()
    if opp:
        opp_esc = _esc(opp)
        lines.extend(
            [
                "- **业务侧本品依据**（产品手册或内部共识摘要；**辅助**监测与报告：仅界定**本品**可引用的成分、功效、人群与宣称边界；**策略主干**仍须来自上方监测任务与 `structured_brief`/报告节选；成稿须**综合两者**，**勿**以大段照抄本节代替 §2 及后续战术；与竞品监测数据分源）：",
                "",
                opp_esc,
                "",
            ]
        )

    lines.extend(
        [
            "## 二、产品价值与用户痛点",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*本节仅 §2.1 一表；痛点与 brief/报告可核对；多类目分行；勿编造用户引语。*",
                    "",
                ]
            ),
            "### 2.1 针对痛点要怎么做",
            "",
            "| 类目/细类（本决策适用） | 用户痛点（简述） | 策略动作 | 具体怎么做（触点/话术/规格/渠道） | 如何验证 |",
            "|--------------------------|------------------|----------|-----------------------------------|----------|",
            "| *（如：饼干线 / 西式糕点 / 全池仅当可解释）* | *（口感/分量/价格信任等）* | *（动词句）* | *（可执行）* | *（指标或抽样）* |",
            "| | | | | |",
            "",
        ]
    )

    raw_hints = brief.get("strategy_hints") or []
    hints = (
        filter_strategy_hints_for_ch8_probe(raw_hints)
        if use_ch8_probe
        else (list(raw_hints) if isinstance(raw_hints, list) else [])
    )
    if hints:
        lines.append(
            "**监测摘要自动线索**" if for_llm_input else "**摘要自动线索（`strategy_hints`）**"
        )
        lines.append("")
        for h in hints:
            lines.append(f"- {_esc(h)}")
        lines.append("")

    pst = brief.get("price_stats") or {}
    lines.extend(
        [
            "## 三、为什么要买「这款产品」",
            "",
            "### 3.1 购买者视角：为何要选这一款（依据与理由）",
            "",
        ]
    )
    if for_llm_input:
        # 检索量级、价带统计已在 structured_brief/报告；勿写入 rules，避免模型复述进策略正文。
        lines.append("")
    else:
        raw = brief.get("pc_search_raw") or {}
        if raw.get("result_count_consensus") is not None:
            rc = _num(raw.get("result_count_consensus"))
            lines.append(
                f"- **站内检索匹配条数量级**：{rc}（列表 resultCount，非销售额口径）。"
            )
        elif merged_n is not None:
            lines.append(f"- **深入监测样本 SKU 数**：{_num(merged_n)}。")
        else:
            lines.append("- **检索与样本尺度**：*（成稿结合摘要与监测范围。）*")
        lines.append("")
        if pst.get("n"):
            src = _esc(brief.get("price_stats_source")) or "—"
            src_disp = src
            lines.extend(
                [
                    f"- **本批样本价带**：来源 {src_disp}，n = {_num(pst.get('n'))}；"
                    f"区间 {_num(pst.get('min'))}～{_num(pst.get('max'))}；中位数 {_num(pst.get('median'))}。",
                    "",
                ]
            )
        else:
            lines.append(
                "*摘要中无价带统计，成稿可结合本批次价格数据在本节补一句价位锚点；**勿**重复 §2 已写的应对动作。*"
            )
            lines.append("")
        lines.append(
            "- **购买理由**：*（成稿：**购买者视角**——买家为何选这一款；承接上列依据与 §2 优先痛点；多细类则分句；**勿**只写品类风口或运营叙事；价带/规格动作已在 §2 表内则此处**勿再展开一遍**。）*"
        )
        lines.append("")

    lines.extend(
        [
            "## 四、为什么要选「这个品牌」",
            "",
            (
                "### 4.1 品牌承诺与调性"
                if for_llm_input
                else "### 4.1 品牌承诺与调性（占位）"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*承诺与调性落到触点；多类目按 §2.1 分句。价位见 §8.2。*",
                    "",
                ]
            ),
            (
                "- **一句话**：*（请写可落到商详/包装/客服等触点的承诺句）*"
                if for_llm_input
                else "- **一句话**：*（占位）*"
            ),
            (
                "- **调性**：*（成稿结合品类、`structured_brief` 与表单写沟通风格；**勿**将本句复制到 §8.1～§8.4 当各节正文。）*"
                if for_llm_input
                else "- **调性**：*（占位；成稿结合品类写清风格，忌全篇与 §8 各节套同一句。）*"
            ),
            "",
            "### 4.2 信任与证据",
            "",
            *(
                ["- *（评价、配料、可核验表述边界。）*", ""]
                if for_llm_input
                else ["- *（成稿：评价、配料、可核验表述边界。）*", ""]
            ),
            *(
                []
                if for_llm_input
                else [
                    "*信任与证据；与 §8.2 价位叙述分开写。*",
                    "",
                ]
            ),
        ]
    )

    lines.extend(
        [
            "## 五、与其它品牌有何不同",
            "",
            (
                "*竞争格局与店铺/品牌集中度见同任务《竞品分析报告》；规则骨架**不**铺陈摘录，成稿**勿**复述「对比对象（摘录）」「店铺分布」「品牌分布」等与报告同构的长段。*"
                if for_llm_input
                else "*竞争格局与集中度见报告；成稿写清与谁对比、差异化与应对，**勿**在此铺陈店铺/品牌占比摘录。*"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "- **环境自测**：头部强势时是侧翼还是正面替代？格局分散时是否用细分场景切入？",
                    "",
                ]
            ),
            (
                "### 5.1 差异化方向"
                if for_llm_input
                else "### 5.1 差异化方向（占位）"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*差异化写清相对竞品多做什么/少做什么；细类不同则分写。*",
                    "",
                ]
            ),
            "| 差异点 | 说明 | 风险 |",
            "|--------|------|------|",
            (
                "| | | |"
                if for_llm_input
                else "| | *待填* | |"
            ),
            "",
        ]
    )
    lines.append("### 5.2 竞争应对")
    lines.append("")
    if not for_llm_input:
        lines.append("*竞争应对：跟价/不跟价时的话术或机制（可简短）。*")
        lines.append("")
    stance = _esc(d.get("competitive_stance") or "").strip()
    stance_line = {
        "flank": "- **本品倾向**：侧翼切入，避免与头部正面硬碰。",
        "head_on": "- **本品倾向**：正面替代，对标头部主战场。",
        "both": "- **本品倾向**：分层推进（部分场景侧翼、部分场景正面）。",
        "undecided": (
            "- **本品倾向**：*（待确认）*"
            if for_llm_input
            else "- **本品倾向**：*（表单未选；成稿时据数据写清倾向）*"
        ),
    }.get(stance)
    if stance_line:
        lines.append(stance_line)
    lines.append("")

    lines.extend(
        [
            "## 六、阶段目标与路径",
            "",
            "### 6.1 本阶段定义",
            "",
            _goal_bullet("时间范围", str(d.get("time_horizon") or ""), "如：本季度 / 未来 12 周"),
            _goal_bullet(
                "成功标准（可量化）",
                str(d.get("success_criteria") or ""),
                "搜索位次、转化、复购等",
            ),
            _goal_bullet("非目标", str(d.get("non_goals") or ""), "明确不做什么（可选）"),
            "",
            "### 6.2 路径",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*路径与 §2.1 对齐；动词句为主。*",
                    "",
                ]
            ),
            _goal_bullet(
                "营销策略",
                str(d.get("marketing_strategy") or ""),
                "传播、活动、投放、内容主线（可选）",
            ),
            _goal_bullet(
                "总体策略",
                str(d.get("general_strategy") or ""),
                "增长/品类/经营总原则（可选）",
            ),
            _goal_bullet(
                "资源与预算备注",
                str(d.get("resource_notes") or ""),
                "人力、投放、产能等（可选）",
            ),
            "",
        ]
    )

    pp = str(d.get("pillar_product") or "")
    pr = str(d.get("pillar_price") or "")
    pch = str(d.get("pillar_channel") or "")
    pcm = str(d.get("pillar_comm") or "")
    tp = str(d.get("tactic_promotion") or "")
    lines.extend(
        [
            "## 七、品牌四线：建设 · 打造 · 运营 · 体验",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*四线对应产品/定价/渠道/传播；每条至少一句落地动作。*",
                    "",
                ]
            ),
            "### 7.1 品牌建设",
            "",
            f"- {_pillar_cell(pp)}",
            "",
            "### 7.2 品牌打造",
            "",
            f"- {_pillar_cell(pr)}",
            "",
            "### 7.3 品牌运营",
            "",
            f"- {_pillar_cell(pch)}",
            "",
            "### 7.4 品牌体验",
            "",
            f"- {_pillar_cell(pcm)}",
            "",
        ]
    )

    if use_ch8_probe and not for_llm_input:
        pst_sig = brief.get("price_promotion_signals") or {}
        has_promo = isinstance(pst_sig, dict) and bool(pst_sig)
        promo_one = (
            "*促销线索须与摘要中的价格/活动信号一致；无则勿编造门槛。*"
            if has_promo
            else "*价差与活动：有则承接摘要；无则勿编造。*"
        )
        lines.extend(["", promo_one, ""])

    lines.extend(
        [
            "## 八、战术支柱",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*四支柱回扣痛点→动作→落地；类目不同则分细类。*",
                    "",
                ]
            ),
            "### 8.1 产品策略",
            "",
            f"- *（表单产品支柱：{_pillar_cell(pp)}）*",
            "",
            "### 8.2 定价策略",
            "",
        ]
    )
    if for_llm_input:
        lines.append(_price_position_llm_hint(pos))
        lines.extend(["", f"- *（表单价格支柱：{_pillar_cell(pr)}）*", ""])
    else:
        lines.extend(
            [
                _price_position_display_line(pos),
                "",
                f"- *（表单价格支柱：{_pillar_cell(pr)}）*",
                "",
            ]
        )
    lines.extend(
        [
            "### 8.3 促销与活动策略",
            "",
            *(
                [
                    "*成稿须写**本品**拟采用的满减/满折或到手价规则（决策句），监测至多一句带过；勿把摘要里的行数占比当正文。*",
                    "",
                ]
                if for_llm_input
                else [
                    "*写清本阶段促销**决策**（拟满减/折扣档、跟价原则）；勿写成报告统计段落。*",
                    "",
                ]
            ),
            f"- *（表单促销策略：{_pillar_cell(tp)}）*",
            "",
            "### 8.4 渠道与传播",
            "",
            f"- *（渠道/传播：{_pillar_cell(pch)} / {_pillar_cell(pcm)}）*",
            "",
        ]
    )

    rk = bool(d.get("ack_risk_keywords"))
    rp = bool(d.get("ack_risk_price"))
    rc = bool(d.get("ack_risk_concentration"))
    lines.extend(_nine_ten_markdown_blocks(rk=rk, rp=rp, rc=rc, for_llm_input=for_llm_input))

    notes = _esc(business_notes)
    lines.extend(
        [
            "### 业务约束与备注",
            "",
            (notes if notes else "*（未填写业务备注。）*"),
            "",
            "---",
            "",
            "## 附录：本任务关键数据一览",
            "",
            f"- **关键词**：{kw}  ·  **批次**：{batch}  ·  **摘要版本**：v{_num(brief.get('schema_version'))}",
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
        if for_llm_input:
            bits_llm: list[str] = []
            ps, pt = meta.get("page_start"), meta.get("page_to")
            if ps is not None and pt is not None:
                bits_llm.append(f"列表页约第 {_esc(ps)}～{_esc(pt)} 页")
            elif ps is not None:
                bits_llm.append(f"列表自第 {_esc(ps)} 页起采集")
            if meta.get("max_skus_config") is not None:
                bits_llm.append(f"深入样本上限约 {_num(meta.get('max_skus_config'))} 个 SKU")
            if meta.get("scenario_filter_enabled"):
                bits_llm.append("已启用场景筛选")
            if bits_llm:
                lines.append(f"- **采集范围**：{'；'.join(bits_llm)}")
        else:
            bits = []
            for k in ("page_start", "page_to", "max_skus_config", "scenario_filter_enabled"):
                if k in meta:
                    label = meta_labels.get(k, k)
                    bits.append(f"{label}={_esc(meta.get(k))}")
            if bits:
                lines.append(f"- **采集参数快照**：{'; '.join(bits)}")
    raw = brief.get("pc_search_raw") or {}
    if raw.get("result_count_consensus") is not None and not for_llm_input:
        lines.append(
            f"- **列表申报规模（resultCount）**：{_num(raw.get('result_count_consensus'))}"
        )
    if for_llm_input:
        lines.extend(
            [
                "",
                "*可与同任务《竞品分析报告》及本批次数据表对照核验。*",
                "",
                "---",
                "",
            ]
        )
    else:
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
