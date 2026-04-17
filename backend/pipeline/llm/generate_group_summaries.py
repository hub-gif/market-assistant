"""第五至第八章各细类归纳：矩阵/评论/场景/价盘/促销及分块调用。"""
from __future__ import annotations

import json
from typing import Any

from .llm_client import call_llm, estimate_chat_input_tokens, llm_context_window_size

MATRIX_GROUPS_SYSTEM = """你是竞品分析顾问。输入为 JSON：``keyword`` 与 ``groups`` 数组。
每个 group 含 ``group``（细分类目名）、``sku_count``、``price_stats``（该细类深入合并行可解析展示价的 min/max/median/mean/n，与 **第六章「按细类价盘」** 分位数表同源；无则 n=0 或缺字段）、
``lines``（该细类下若干 SKU 的标题/卖点/配料**摘录**，均来自页面抓取拼接，可能截断）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题（不要使用 ``##`` 一级标题）；
- 每段约 **100～200 字**中文：**主体**归纳该细类下**卖点表述共性**、**配料类型/宣称共性**（摘录中无配料则写「配料摘录较少」）；**品牌格局**可一句概括（仅依据摘录中可见品牌/系列，勿编造销量排名）；
- **价带/价位**：若 ``price_stats.n`` 为大于 0 的整数，**仅允许**用该对象里的数值写价带（如 min～max、中位数），且须与 ``price_stats`` **完全一致**，**禁止**写「价格带未明确」「未体现具体价位」「多为中端」等**与上述数值相矛盾**的表述；若 n=0 或无可信数值，**不要猜测价位**，可写一句「深入样本可解析数值价不足，价盘以 **第六章** 表格为准」；
- **禁止**输出 Markdown 表格、禁止逐条复述 SKU 明细表；勿编造功效、认证；
- 若 ``lines`` 很少，明确写「样本较少，归纳供启发」。

总输出约 **800～3500 字**（细类多则偏长）。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


MATRIX_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告第五章末「细类要点归纳」正文（Markdown）。\n\n"
)


def generate_matrix_group_summaries_llm(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    trimmed: list[dict[str, Any]] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        g2 = dict(g)
        ln = g2.get("lines")
        if isinstance(ln, list) and len(ln) > 22:
            g2["lines"] = ln[:22]
        trimmed.append(g2)
    payload = {"keyword": keyword, "groups": trimmed}
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 95_000:
        for g2 in trimmed:
            ln = g2.get("lines")
            if isinstance(ln, list) and len(ln) > 12:
                g2["lines"] = ln[:12]
        raw = json.dumps({"keyword": keyword, "groups": trimmed}, ensure_ascii=False)
    user = MATRIX_GROUPS_USER_PREFIX + raw
    return call_llm(MATRIX_GROUPS_SYSTEM, user)


COMMENT_GROUPS_SYSTEM = """你是用户研究与品类顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（与 第五章矩阵一致的细分类目名）、``comment_flat_rows``、``effective_text_lines``、
``focus_hit_lines``（关注词子串命中摘要，与 第八章第三节 同源）、``sample_text_snippets``（评价短摘录，已截断）。
摘录行通常以 ``【细类：…｜SKU：…｜品名：…｜店铺：…】`` 开头：细类可与本 group 名对照，**品名/SKU/店铺**表示该句具体出自哪条链接；归纳时若引用原话，**须交代是「哪家店、哪条 SKU、哪款品名」上的反馈**，勿只写「有用户说口感差」而不指代产品。
关注词命中为子串统计，可能与句意不一致；**请以整句语义**判断褒贬（如「软硬适中」「没那么甜」常为满意表述，不得据此写成质地问题）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题（不要使用 ``##`` 一级标题）；
- 每段约 **100～220 字**中文：归纳该细类下**消费者在讨论什么**（口感、价格、物流、功效疑虑等）、**关注词命中反映的诉求**；勿编造摘录中未出现的品牌、医学结论；
- **去重与可证（本章仅评论侧）**：本段**只**依据评价/关注词摘录，**禁止**把 ``keyword``、品类常识或商品标题卖点套话写成「用户评价」；**禁止**各细类段首复用同一句总括（如「整体上满足了消费者对低 GI、高蛋白、便携性的需求」）；每段开头句式须**有变化**，并至少一句体现**该细类与相邻细类在讨论焦点上的差异**。**利益/诉求词**（低 GI、高蛋白、便携、代餐、控糖等）**仅当**在 ``sample_text_snippets``、``effective_text_lines`` 或 ``focus_hit_lines`` 的**原文**中可子串命中或可明确同义（便携↔随身、小包装、单片、独立装等）时才写；若上述字段中**未**出现「蛋白」「便携」「随身」「小包装」「单片」等，则**不得**写「高蛋白」「便携性」等；**禁止**为凑齐常见卖点组合而脑补未在输入中出现的词。
- **禁止**输出 Markdown 表格、禁止逐条复述全部评价；
- 若 ``effective_text_lines`` 很少，明确写「样本较少，归纳供启发」。

总输出约 **600～3200 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


COMMENT_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告第八章末「细类评论与关注词要点归纳」正文（Markdown）。\n\n"
)


def generate_comment_group_summaries_llm(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    """
    细类多、评价正文长时 JSON 易超上下文：按档位逐步缩短 ``effective_text_lines`` /
    ``sample_text_snippets`` 直至估算 tokens 低于窗口（与 ``AI_crawler.chat_completion_text`` 预检一致）。
    """

    def _compact_one(
        g: dict[str, Any],
        *,
        eff_n: int,
        eff_max: int,
        sn_n: int,
        sn_max: int,
        fh_n: int,
    ) -> dict[str, Any]:
        g2: dict[str, Any] = {
            "group": g.get("group"),
            "comment_flat_rows": g.get("comment_flat_rows"),
        }
        el = g.get("effective_text_lines")
        if isinstance(el, list):
            g2["effective_text_lines"] = [str(x)[:eff_max] for x in el[:eff_n]]
        else:
            g2["effective_text_lines"] = []
        sn = g.get("sample_text_snippets")
        if isinstance(sn, list):
            g2["sample_text_snippets"] = [str(x)[:sn_max] for x in sn[:sn_n]]
        else:
            g2["sample_text_snippets"] = []
        fh = g.get("focus_hit_lines")
        if isinstance(fh, list):
            g2["focus_hit_lines"] = [str(x) for x in fh[:fh_n]]
        else:
            g2["focus_hit_lines"] = []
        return g2

    ctx = llm_context_window_size()
    budget = ctx - 512 - 256
    # 预留 ``max_tokens=8192`` 的完成空间；网关计输入 tokens 常高于本地粗估
    def _input_ok(system: str, user_p: str) -> bool:
        est = estimate_chat_input_tokens(system, user_p)
        return est < 15_500

    levels: list[tuple[int, int, int, int, int]] = [
        (14, 260, 10, 200, 10),
        (12, 220, 8, 180, 8),
        (10, 180, 8, 160, 6),
        (8, 150, 6, 140, 6),
        (6, 120, 5, 120, 5),
        (5, 100, 4, 100, 4),
        (4, 80, 3, 80, 3),
        (3, 70, 3, 70, 3),
        (3, 50, 2, 60, 2),
    ]
    user = ""
    chosen = levels[-1]
    for level in levels:
        chosen = level
        eff_n, eff_max, sn_n, sn_max, fh_n = level
        trimmed = [
            _compact_one(g, eff_n=eff_n, eff_max=eff_max, sn_n=sn_n, sn_max=sn_max, fh_n=fh_n)
            for g in groups
            if isinstance(g, dict)
        ]
        raw = json.dumps({"keyword": keyword, "groups": trimmed}, ensure_ascii=False)
        if len(raw) > 48_000:
            raw = raw[:44_000] + "\n…\n"
        user = COMMENT_GROUPS_USER_PREFIX + raw
        if _input_ok(COMMENT_GROUPS_SYSTEM, user):
            break
    else:
        tail = "\n\n…（JSON 已截断以适配上下文；仅依据可见字段撰写。）\n"
        eff_n, eff_max, sn_n, sn_max, fh_n = chosen
        trimmed = [
            _compact_one(g, eff_n=eff_n, eff_max=eff_max, sn_n=sn_n, sn_max=sn_max, fh_n=fh_n)
            for g in groups
            if isinstance(g, dict)
        ]
        raw = json.dumps({"keyword": keyword, "groups": trimmed}, ensure_ascii=False)
        room = max(
            2000,
            int((budget - 800) / 0.55)
            - len(COMMENT_GROUPS_SYSTEM)
            - len(COMMENT_GROUPS_USER_PREFIX)
            - len(tail),
        )
        user = COMMENT_GROUPS_USER_PREFIX + raw[: max(1500, room)] + tail
    return call_llm(COMMENT_GROUPS_SYSTEM, user)


SCENARIO_GROUPS_SYSTEM = """你是用户研究与品类顾问。输入为 JSON：``keyword``、``scenario_lexicon``、``groups``。
``scenario_lexicon`` 列出各场景标签及示例触发子串（与报告 **第八章第三节** 右栏统计规则一致）。
``groups`` 每项含 ``group``（与 第五章矩阵一致的细分类目名）、``effective_text_count``（有效评价文本条数）、
``scenario_distribution``（各预设场景的 ``mention_rows`` 与 ``share_of_effective_texts``；**一条评价可计入多场景**；与 第八章第三节 图右栏同源）、
``sample_text_snippets``（摘录行常含细类、SKU、品名、店铺等前缀的短引文，已截断）。
统计为**子串命中**，不是语义主题模型。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该条 ``group`` 字段**完全一致**的细类名作为小节标题；
- 每段约 **100～220 字**：归纳该细类用户**自述的使用场景/用途**结构（哪些场景标签相对突出、多场景叠加是否常见），可点到与其他细类的差异；**所有条数与占比须与 ``scenario_distribution``、``effective_text_count`` 一致**，禁止编造；
- 引用原话时须保留或复述摘录中的店铺/SKU/品名信息，勿虚构；
- **禁止** Markdown 表格、禁止复述全部摘录；若 ``effective_text_count`` 很小，写明「样本较少，归纳供启发」。

总输出约 **600～3200 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


SCENARIO_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告 第八章第三节（右栏：使用场景）之后的「使用场景要点归纳」正文（Markdown）。\n\n"
)


def generate_scenario_group_summaries_llm(
    payload: dict[str, Any], *, keyword: str
) -> str:
    """与 ``generate_comment_group_summaries_llm`` 类似：细类多时长 JSON 按档压缩。"""

    def _compact_group(
        g: dict[str, Any],
        *,
        dist_n: int,
        sn_n: int,
        sn_max: int,
    ) -> dict[str, Any]:
        g2: dict[str, Any] = {
            "group": g.get("group"),
            "effective_text_count": g.get("effective_text_count"),
        }
        dist = g.get("scenario_distribution")
        if isinstance(dist, list):
            g2["scenario_distribution"] = []
            for x in dist[:dist_n]:
                if not isinstance(x, dict):
                    continue
                g2["scenario_distribution"].append(
                    {
                        "scenario": x.get("scenario"),
                        "mention_rows": x.get("mention_rows"),
                        "share_of_effective_texts": x.get(
                            "share_of_effective_texts"
                        ),
                    }
                )
        else:
            g2["scenario_distribution"] = []
        sn = g.get("sample_text_snippets")
        if isinstance(sn, list):
            g2["sample_text_snippets"] = [
                str(x)[:sn_max] for x in sn[:sn_n]
            ]
        else:
            g2["sample_text_snippets"] = []
        return g2

    def _compact_lex(raw: Any, *, max_items: int, trig_n: int) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw[:max_items]:
            if not isinstance(item, dict):
                continue
            tr = item.get("trigger_examples")
            te = (
                [str(x)[:48] for x in tr[:trig_n]]
                if isinstance(tr, list)
                else []
            )
            out.append({"label": item.get("label"), "trigger_examples": te})
        return out

    groups_in = [g for g in (payload.get("groups") or []) if isinstance(g, dict)]
    ctx = llm_context_window_size()
    budget = ctx - 512 - 256

    def _input_ok(system: str, user_p: str) -> bool:
        est = estimate_chat_input_tokens(system, user_p)
        return est < 15_500

    levels: list[tuple[int, int, int, int, int]] = [
        (16, 14, 260, 10, 12),
        (14, 12, 220, 8, 10),
        (12, 10, 180, 8, 8),
        (10, 8, 150, 6, 6),
        (8, 6, 120, 5, 5),
        (6, 5, 100, 4, 4),
        (5, 4, 80, 3, 3),
    ]
    user = ""
    chosen = levels[-1]
    for level in levels:
        chosen = level
        dist_n, sn_n, sn_max, lex_n, trig_n = level
        trimmed_g = [
            _compact_group(g, dist_n=dist_n, sn_n=sn_n, sn_max=sn_max)
            for g in groups_in
        ]
        lex_c = _compact_lex(
            payload.get("scenario_lexicon"),
            max_items=lex_n,
            trig_n=trig_n,
        )
        body = {
            "keyword": keyword,
            "scenario_lexicon": lex_c,
            "groups": trimmed_g,
        }
        raw = json.dumps(body, ensure_ascii=False)
        if len(raw) > 48_000:
            raw = raw[:44_000] + "\n…\n"
        user = SCENARIO_GROUPS_USER_PREFIX + raw
        if _input_ok(SCENARIO_GROUPS_SYSTEM, user):
            break
    else:
        tail = "\n\n…（JSON 已截断以适配上下文；仅依据可见字段撰写。）\n"
        dist_n, sn_n, sn_max, lex_n, trig_n = chosen
        trimmed_g = [
            _compact_group(g, dist_n=dist_n, sn_n=sn_n, sn_max=sn_max)
            for g in groups_in
        ]
        lex_c = _compact_lex(
            payload.get("scenario_lexicon"),
            max_items=lex_n,
            trig_n=trig_n,
        )
        raw = json.dumps(
            {
                "keyword": keyword,
                "scenario_lexicon": lex_c,
                "groups": trimmed_g,
            },
            ensure_ascii=False,
        )
        room = max(
            2000,
            int((budget - 800) / 0.55)
            - len(SCENARIO_GROUPS_SYSTEM)
            - len(SCENARIO_GROUPS_USER_PREFIX)
            - len(tail),
        )
        user = SCENARIO_GROUPS_USER_PREFIX + raw[: max(1500, room)] + tail
    return call_llm(SCENARIO_GROUPS_SYSTEM, user)


PRICE_GROUPS_SYSTEM = """你是定价与渠道顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（细分类目名，与 第五章矩阵、第六章「按细类价盘」小节一致）、``sku_count``、``price_stats``（该细类可解析展示价的 min/max/median/mean/n，与第六章各细类 Markdown 分位数表同源）、
``listing_snippets``（若干「标题｜标价｜券后｜详情价」摘录，来自合并表字段，已截断）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题；
- 每段约 **80～200 字**中文，**只写价盘与价差**：用 ``price_stats`` 概括价带/离散度（如 min～max、中位数、相对集中或拉得开）；用 ``listing_snippets`` 归纳**标价 vs 券后 vs 详情价**是否常一致、是否常见券后低于标价、价差幅度的大致印象；**可一句**联系标题里**显式出现的规格数字**（如克重、件数）解释**价高/价差大是否可能来自大规格或组合装**——仅当摘录里确有数字时写，勿展开成宣称解读；
- **硬性禁止**（本章不是卖点章）：不要列举或归纳「0 蔗糖 / 低 GI / 全麦 / 代餐 / 孕妇 / 控糖」等**营销宣称或场景关键词**；不要写配料、功效、品牌叙事、用户画像；这些若出现应留给报告 **第五章细类要点归纳**。
- **禁止** Markdown 表格、禁止罗列全部 SKU；勿编造未出现的到手价、销量排名；
- 若 ``price_stats`` 中 n=0 或缺失，写「该细类无可解析数值价，从略」。

总输出约 **500～2800 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


PRICE_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告第六章末「细类价盘要点归纳」正文（Markdown）。"
    "本章只写**数值价带与标价/券后/详情价关系**，勿写卖点宣称关键词归纳。\n\n"
)


def generate_price_group_summaries_llm(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    trimmed: list[dict[str, Any]] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        g2 = dict(g)
        sn = g2.get("listing_snippets")
        if isinstance(sn, list) and len(sn) > 14:
            g2["listing_snippets"] = sn[:14]
        trimmed.append(g2)
    payload = {"keyword": keyword, "groups": trimmed}
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 95_000:
        for g2 in trimmed:
            sn = g2.get("listing_snippets")
            if isinstance(sn, list) and len(sn) > 8:
                g2["listing_snippets"] = sn[:8]
        raw = json.dumps({"keyword": keyword, "groups": trimmed}, ensure_ascii=False)
    user = PRICE_GROUPS_USER_PREFIX + raw
    return call_llm(PRICE_GROUPS_SYSTEM, user)


PROMO_GROUPS_SYSTEM = """你是电商促销与价盘顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（细分类目名，与第五章矩阵、第六章一致）、``sku_count``、``rows_with_buyer_promo_text``（该细类合并表中「促销摘要」非空行数）、
``promo_snippets``（若干条摘录：标题 + 促销摘要/榜单排名/榜单类文案等，已截断；**不含**列表卖点/腰带列）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题；
- 每段约 **80～220 字**中文，**只写促销与活动形态**：如券后/满减/百亿补贴/新人包邮/限购提示/「到手价」展示方式、与 **第六章第一节** 规则统计可对照的**活动话术密度**印象；可一句点出**榜单曝光**是否常见、是否与价格带并存；
- **硬性禁止**：不要展开配料、功效、用户画像；不要复述第五章 的配料归纳；不要编造未在摘录中出现的具体金额或活动规则；
- 若该细类 ``rows_with_buyer_promo_text`` 为 0 且摘录几乎只有标题，写「该细类缺少购买者侧促销摘要，从略」。

总输出约 **500～2800 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


PROMO_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告第六章「细类促销与活动要点归纳」正文（Markdown）。"
    "依据 ``promo_snippets`` 中的促销摘要与榜单相关摘录，**不写**价带分位数（留给上一小节）。\n\n"
)


def generate_promo_group_summaries_llm(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    trimmed: list[dict[str, Any]] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        g2 = dict(g)
        sn = g2.get("promo_snippets")
        if isinstance(sn, list) and len(sn) > 14:
            g2["promo_snippets"] = sn[:14]
        trimmed.append(g2)
    payload = {"keyword": keyword, "groups": trimmed}
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 95_000:
        for g2 in trimmed:
            sn = g2.get("promo_snippets")
            if isinstance(sn, list) and len(sn) > 8:
                g2["promo_snippets"] = sn[:8]
        raw = json.dumps({"keyword": keyword, "groups": trimmed}, ensure_ascii=False)
    user = PROMO_GROUPS_USER_PREFIX + raw
    return call_llm(PROMO_GROUPS_SYSTEM, user)


def _join_chunked_group_markdown(parts: list[str]) -> str:
    """按细类多次调用 LLM 后的片段拼接（顺序与 ``groups`` 一致）。"""
    return "\n\n".join(p.strip() for p in parts if (p or "").strip())


def generate_matrix_group_summaries_llm_chunked(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    """与 ``generate_matrix_group_summaries_llm`` 等价输出结构，但**每个矩阵细类单独**请求一次网关。"""
    clean = [g for g in groups if isinstance(g, dict)]
    if not clean:
        return ""
    parts = [
        generate_matrix_group_summaries_llm([g], keyword=keyword) for g in clean
    ]
    return _join_chunked_group_markdown(parts)


def generate_price_group_summaries_llm_chunked(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    clean = [g for g in groups if isinstance(g, dict)]
    if not clean:
        return ""
    parts = [
        generate_price_group_summaries_llm([g], keyword=keyword) for g in clean
    ]
    return _join_chunked_group_markdown(parts)


def generate_promo_group_summaries_llm_chunked(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    clean = [g for g in groups if isinstance(g, dict)]
    if not clean:
        return ""
    parts = [
        generate_promo_group_summaries_llm([g], keyword=keyword) for g in clean
    ]
    return _join_chunked_group_markdown(parts)


def generate_comment_group_summaries_llm_chunked(
    groups: list[dict[str, Any]], *, keyword: str
) -> str:
    clean = [g for g in groups if isinstance(g, dict)]
    if not clean:
        return ""
    parts = [
        generate_comment_group_summaries_llm([g], keyword=keyword) for g in clean
    ]
    return _join_chunked_group_markdown(parts)


def generate_scenario_group_summaries_llm_chunked(
    payload: dict[str, Any], *, keyword: str
) -> str:
    """``scenario_lexicon`` 每轮原样附带，``groups`` 每次只含一个细类。"""
    groups_in = [g for g in (payload.get("groups") or []) if isinstance(g, dict)]
    if not groups_in:
        return ""
    lex = payload.get("scenario_lexicon")
    base: dict[str, Any] = {
        "scenario_lexicon": lex if isinstance(lex, list) else [],
    }
    parts = [
        generate_scenario_group_summaries_llm(
            {**base, "groups": [g]},
            keyword=keyword,
        )
        for g in groups_in
    ]
    return _join_chunked_group_markdown(parts)
