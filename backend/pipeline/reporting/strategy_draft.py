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


def _cr_narrative(
    label: str,
    cr1: Any,
    cr3: Any,
    top: Any,
    *,
    first_share_wording: tuple[str, str] | None = None,
) -> str | None:
    """从集中度生成一句策略向描述，无数据则返回 None（正文避免英文缩写）。

    ``first_share_wording`` 为 ``(第一大句前缀, 前三大句前缀)`` 时覆盖默认措辞（用于矩阵收窄口径，
    避免误写「列表行」）。
    """
    try:
        c1 = float(cr1) if cr1 is not None else None
    except (TypeError, ValueError):
        c1 = None
    if c1 is None and not (top or "").strip():
        return None
    top_s = _esc(top) or "—"
    if first_share_wording is not None:
        w1, w3 = first_share_wording
    elif "店铺" in label:
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


def _shop_unique_sku_basis_lines(shops: dict[str, Any]) -> list[str]:
    """
    ``shops_from_list.unique_sku_basis`` 与竞品报告/摘要一致：按去重 SKU 的店铺集中度对照口径。
    """
    usb = shops.get("unique_sku_basis") if isinstance(shops, dict) else None
    if not isinstance(usb, dict) or not usb.get("n_unique_skus"):
        return []
    u1 = concentration_first_share(usb)
    u3 = concentration_top_three_share(usb)
    utop = _esc(usb.get("top_label") or "")
    if u1 is None or not utop:
        return []
    return [
        f"- **列表侧店铺（按去重 SKU）**：共 **{_num(usb.get('n_unique_skus'))}** 个去重 SKU；"
        f"第一大店铺「{utop}」约占 **{_pct(u1)}**；前三合计 **{_pct(u3)}**。"
        "*（与上行「按列表行」可能因同一 SKU 多行曝光而差异；非销量/市占。）*"
    ]


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
    用于 §1.2 文案分支及对 ``strategy_hints`` 的过滤：开启探针时与子串命中枚举相关的自动线索会被压掉；
    关闭时 §1.2 仍说明「简报不附带预设关注词/场景子串统计」，评论侧以报告第八章探针（若启用）与原文为准。
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
        "- **用户侧**：*（一两句结论即可：讨论焦点与负向主题；**按细类分句**归纳，**勿**混成「全站用户」一句；**勿**展开与报告重复的细类统计、词频。）*"
        if not for_llm_input
        else "- **用户侧**：—"
    )
    _summary_stage = (
        "- **阶段重点**：*（须含 1～2 条**可执行动作**，回扣 §2 优先痛点；**尽量点明适用类目/主推线**；勿仅写「加强运营」。）*"
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
                    "*成稿须与 §2 一致：写清「谁在什么任务下检索、决策」及**主攻类目/细类**（与 §2.1 类目列可对上），为后文「针对痛点怎么做」埋伏笔。*",
                    "",
                ]
            ),
            "### 1.2 细类讨论焦点（评论文本分析）",
            "",
        ]
    )
    if use_ch8_probe:
        if not for_llm_input:
            lines.extend(
                [
                    "*当前任务以**第八章评论侧文本挖掘**为主呈现时，此处**不**逐条罗列关注词子串命中次数。*",
                    "",
                    "- **饼干 / 糕点 / 面点等**：*（骨架占位；成稿**分细类**各一句归纳用户关心点，**勿**合并成模糊「全池」一句；**勿**复述 §8 词频与条数。）*",
                    "",
                ]
            )
    else:
        if not for_llm_input:
            lines.append(
                "*简报中**不再**附带预设关注词/场景子串统计；评论侧请依据同任务《竞品分析报告》**第八章第二节**（文本挖掘探针，若已启用）及抽样原文撰写本节。*"
            )
            lines.append("")

    mix = brief.get("category_mix_top") or []
    if mix:
        lines.append("### 类目结构（摘录）")
        lines.append("")
        for item in mix[:6]:
            if isinstance(item, dict):
                lines.append(f"- {_esc(item.get('label'))}：{_num(item.get('count'))}")
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

    lines.extend(
        [
            "## 二、产品价值与用户痛点",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*本节**仅**用下表写清**针对痛点要怎么做**（**类目** + 痛点简述 + 动作 + 落地 + 验证）。**不再**单设「痛点表 / 价值对表 / 负向归因」子节，避免与 §三、§八重复。*",
                    "",
                    "*「用户痛点（简述）」须与 `structured_brief` / 策略线索 / 报告节选**可核对**；**禁止**编造「用户反馈『……』」式引语，除非原句已出现在上述输入中。*",
                    "",
                    "*「类目/细类」列：写明本行决策**适用于哪一类**（如饼干/面包/全检索池）；多细类须**分行**，**禁止**用一句「全站」覆盖彼此冲突的策略；类目未定可写「待业务定类」并附分类假设。*",
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
    raw = brief.get("pc_search_raw") or {}
    if raw.get("result_count_consensus") is not None:
        lines.append(
            f"- **检索结果量级（需求侧参考，非销售额）**：{_num(raw.get('result_count_consensus'))}（站内匹配条数量级）"
            if for_llm_input
            else f"- **检索结果量级（需求侧参考，非销售额）**：{_num(raw.get('result_count_consensus'))}（列表 resultCount）"
        )
    elif merged_n is not None:
        lines.append(f"- **深入样本 SKU 数（监测范围）**：{_num(merged_n)}")
    else:
        lines.append(
            "- **检索与样本尺度**：—"
            if for_llm_input
            else "- **检索与样本尺度**：*（成稿结合摘要与监测范围。）*"
        )
    lines.append("")
    if pst.get("n"):
        src = _esc(brief.get("price_stats_source")) or "—"
        src_disp = "本监测样本" if for_llm_input and src == "strategy_scope_matrix_group_skus" else src
        lines.extend(
            [
                f"- **价带摘录（支撑购买理由与价位锚点）**：来源 {src_disp}，n = {_num(pst.get('n'))}；"
                f"区间 {_num(pst.get('min'))}～{_num(pst.get('max'))}；中位数 {_num(pst.get('median'))}。",
                "",
            ]
        )
    else:
        if for_llm_input:
            lines.append("- **价带摘录**：监测摘要中暂无统计表，可结合同任务报告补一句与购买理由相关的价位锚点。")
        else:
            lines.append(
                "*摘要中无价带统计，成稿可结合本批次价格数据在本节补一句价位锚点；**勿**重复 §2 已写的应对动作。*"
            )
        lines.append("")
    lines.append(
        "- **购买理由（须站在购买者一侧写）**：用 1～2 句写清**买家为何愿意下单这一款**——解决什么顾虑、在货架上凭什么选它（获得感、可感知利益、价位是否值得等）；上列检索/价带仅作背景，勿喧宾夺主。"
        "可用「用户/消费者」作主语，**禁止**用纯运营口吻（如「适合××叙事切入」「策略上占位」）代替购买动机；**禁止**以品类宏观句收尾而不落到本品可验证点。"
        if for_llm_input
        else "- **购买理由**：*（成稿：**购买者视角**——买家为何选这一款；承接上列依据与 §2 优先痛点；多细类则分句；**勿**只写品类风口或运营叙事；价带/规格动作已在 §2 表内则此处**勿再展开一遍**。）*"
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
                    "*成稿：承诺与调性须能落到**触点**（商详/包装/客服首句等）上的**具体句子**；**若**多类目话术不同，按 §2.1 类目**分句**，勿仅形容词。*",
                    "",
                ]
            ),
            (
                "- **一句话**：*（请写可落到商详/包装/客服等触点的承诺句）*"
                if for_llm_input
                else "- **一句话**：*（占位）*"
            ),
            (
                "- **调性**：透明、可验证、合规控糖叙事。"
                if for_llm_input
                else "- **调性**：透明、可验证、合规控糖叙事（成稿可细化）。"
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
                    "*本节写**用户为何信任、为何愿意选这个品牌**（承诺、证据、合规边界）；**价位阵地**（表单勾选的四类取向）见 **§8.2 定价策略**，勿混写。*",
                    "",
                ]
            ),
        ]
    )

    conc = brief.get("concentration") or {}
    shops = conc.get("shops_from_list") or {}
    dbrand = conc.get("detail_brand_among_merged") or {}
    scope_ap = brief.get("strategy_scope_applied")
    scoped_matrix = isinstance(scope_ap, dict) and bool(scope_ap.get("group"))
    gname_scoped = _esc(scope_ap.get("group")) if scoped_matrix else ""
    lines.extend(
        [
            "## 五、与其它品牌有何不同",
            "",
            "### 5.1 对比对象（摘录）",
            "",
        ]
    )
    if scoped_matrix:
        lines.append(
            "*本任务已按矩阵细类收窄：**下列店铺/品牌占比均按该分组内「深入合并 SKU」条数统计**，"
            "与全关键词 **PC 搜索列表行** 集中度**不是同一口径**；亦非销量或市占。*"
            if not for_llm_input
            else "*集中度：按所选矩阵分组内合并 SKU 条数；非全站列表行。*"
        )
        lines.append("")
    shop_label = (
        f"店铺分布（「{gname_scoped}」内样本 SKU）"
        if scoped_matrix
        else "列表侧店铺集中度"
    )
    brand_label = (
        f"品牌分布（「{gname_scoped}」内样本 SKU）"
        if scoped_matrix
        else "深入样本内品牌集中度"
    )
    scoped_wording: tuple[str, str] | None = (
        ("第一大店铺约占该分组样本 SKU 的", "前三大店铺合计约占")
        if scoped_matrix
        else None
    )
    scoped_brand_wording: tuple[str, str] | None = (
        ("第一大品牌约占该分组样本 SKU 的", "前三大品牌合计约占")
        if scoped_matrix
        else None
    )
    n_shop = _cr_narrative(
        shop_label,
        concentration_first_share(shops),
        concentration_top_three_share(shops),
        shops.get("top_label"),
        first_share_wording=scoped_wording,
    )
    n_brand = _cr_narrative(
        brand_label,
        concentration_first_share(dbrand),
        concentration_top_three_share(dbrand),
        dbrand.get("top_label"),
        first_share_wording=scoped_brand_wording,
    )
    if n_shop:
        lines.append(n_shop)
    for uline in _shop_unique_sku_basis_lines(shops):
        lines.append(uline)
    if n_brand:
        lines.append(n_brand)
    if not n_shop and not n_brand:
        lines.append(
            "- **竞争结构**：监测摘要未含集中度摘录。"
            if for_llm_input
            else "*本摘要未含集中度指标，请结合本批次竞争结构数据补全。*"
        )
    lines.extend(
        [
            "",
            *(
                []
                if for_llm_input
                else [
                    "",
                    "- **环境自测**：头部强势时是侧翼还是正面替代？格局分散时是否用细分场景切入？",
                ]
            ),
            "",
            (
                "### 5.2 差异化方向"
                if for_llm_input
                else "### 5.2 差异化方向（占位）"
            ),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿：相对竞品**多做什么/少做什么**，写**可执行的一步**；**若**差异因细类而异，**分类目**写（非空泛「更好」）。*",
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
    lines.append("### 5.3 竞争应对")
    lines.append("")
    if not for_llm_input:
        lines.append(
            "*成稿：在表单倾向基础上，写清**跟价/不跟价时具体话术或机制**（一句即可）。*"
        )
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
                    "*成稿：路径须与 **§2.1 针对痛点要怎么做** 可对齐；营销/总体策略为**动词句**，回扣痛点；**多类目并行**时**分线**写目标或写清主线/副线。*",
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
    lines.extend(
        [
            "## 七、品牌四线：建设 · 打造 · 运营 · 体验",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*（与表单「4P 策略支柱」对应：产品 / 定价 / 渠道 / 传播。）*",
                    "*成稿：**每条线**至少一句——服务哪类痛点、本阶段**具体做哪一步**；**尽量**与 §2.1「类目/细类」可对上，多类目则**分句**（勿四条同一泛化句）。*",
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
        promo_hint = "*促销与活动线索：须与摘要 `price_promotion_signals` 及第六章/第九章已有归纳一致；无则勿编造具体满减门槛。*"
        lines.extend(
            [
                "",
                promo_hint
                if has_promo
                else "*促销与价差：若摘要或价格信号有归纳则承接；无则勿编造。*",
                "",
            ]
        )

    lines.extend(
        [
            "## 八、战术支柱",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿：四支柱分别回扣 **痛点→动作→落地**（可与 §2.1 呼应，避免纯重复）；**若**产品/定价/促销因类目策略不同，**分细类**写子条，勿一条盖全站。*",
                    "",
                ]
            ),
            "### 8.1 产品策略",
            "",
            f"- *（表单产品支柱：{_pillar_cell(pp)}）*",
            "",
            "### 8.2 定价策略",
            "",
            "**价位阵地取向（表单勾选；与监测价带可对读）**",
            "",
            f"- {_pos_mark(pos, 'top')} **贴顶**：中高位或头部价位带。",
            f"- {_pos_mark(pos, 'mid')} **卡腰**：围绕中位数一带。",
            f"- {_pos_mark(pos, 'entry')} **下探**：贴近区间下限。",
            f"- {_pos_mark(pos, 'different')} **另起带**：规格/组合/服务差异化。",
            "",
            f"- *（表单价格支柱：{_pillar_cell(pr)}）*",
            "",
            "### 8.3 促销与活动策略",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*须写促销**原则**（券/到手价/跟价节奏）；**满减、满折、跨店**等：能引用的写清来源；监测未捕获具体门槛时写「待与运营/后台对齐」，**勿**整节留空，**勿**编造门槛数字。*",
                    "*与 `price_promotion_signals`、报告第六章一致；勿虚构活动。*",
                    "",
                ]
            ),
            "### 8.4 渠道与传播",
            "",
            f"- *（渠道/传播：{_pillar_cell(pch)} / {_pillar_cell(pcm)}）*",
            "",
        ]
    )

    rk = bool(d.get("ack_risk_keywords"))
    rp = bool(d.get("ack_risk_price"))
    rc = bool(d.get("ack_risk_concentration"))
    rk_kw = "评论侧归纳是否以偏概全？（需原评论抽样）"
    lines.extend(
        [
            "## 九、风险、假设与待验证",
            "",
            _risk_line(rk, rk_kw),
            _risk_line(rp, "价格带是否含大促/异常挂价？（需核对清洗规则）"),
            _risk_line(rc, "列表集中度与深入样本品牌是否不一致？（需解释渠道差异）"),
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿：每条风险尽量带**应对动作或验证计划**（抽样、核对规则），勿只列标题。*",
                    "",
                    "*业务备注见下节。*",
                    "",
                ]
            ),
            "## 十、下一步与节奏",
            "",
            *(
                []
                if for_llm_input
                else [
                    "*成稿：下列为**可执行任务**（可补负责人/时间）；与 §2.1 / §六 优先级一致；可含「按类目核对主图/商详与 §2.1 表」类项。*",
                    "",
                ]
            ),
            "- [ ] 锁定主推款与对标；过法务与合规。",
            "- [ ] 统一对外数据口径与话术。",
            "- [ ] 下轮监测更新后迭代策略。",
            "",
        ]
    )

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
    if raw.get("result_count_consensus") is not None:
        lines.append(
            f"- **平台申报检索规模**：{_num(raw.get('result_count_consensus'))}"
            if for_llm_input
            else f"- **列表申报规模（resultCount）**：{_num(raw.get('result_count_consensus'))}"
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
