"""策略稿润色与第九章策略机会归纳。"""
from __future__ import annotations

import json
import os
from typing import Any

from ..reporting.brief_compact import compact_brief_for_llm
from ..reporting.strategy_draft import build_strategy_draft_markdown
from .llm_client import call_llm, estimate_chat_input_tokens, llm_context_window_size

STRATEGY_SYSTEM = """你是市场策略顾问，根据**结构化监测摘要**与业务侧填写的**决策字段**，把「规则底稿」润色为可读的策略 Markdown。

**规则**：
- 输入 JSON 含 `rules_draft_markdown`（规则引擎生成的底稿，与同任务数据一致）、`structured_brief`（摘要子集）、`strategy_decisions`、`business_notes` 等；
- **不得编造**输入中不存在的销量、占比、价格数字；若底稿与摘要中有数字，须保持一致；表述集中度时用「第一大……份额」「前三家合计」等中文，**不要用** CR1、CR3 等缩写；
- 若 `structured_brief` 含 `matrix_overview_for_llm` 或矩阵相关字段，策略中应**呼应**细分类目分组与竞品矩阵结论，不得无故删光矩阵相关建议；
- 可调整段落衔接、标题层级、列表与表格呈现，使更易读；可补充「建议」「待业务确认」类表述，但不虚构竞品名称或数据；
- **仅输出** Markdown 正文（不要 ``` 围栏包裹全文）。"""

STRATEGY_USER_PREFIX = """请基于以下 JSON 输出最终策略稿（Markdown）。\n\n"""


def generate_strategy_draft_markdown_llm(
    *,
    job_id: int,
    keyword: str,
    brief: dict[str, Any],
    business_notes: str,
    generated_at_iso: str,
    strategy_decisions: dict[str, Any],
) -> str:
    rules_md = build_strategy_draft_markdown(
        job_id=job_id,
        keyword=keyword,
        brief=brief,
        business_notes=business_notes,
        generated_at_iso=generated_at_iso,
        strategy_decisions=strategy_decisions,
    )
    compact = compact_brief_for_llm(brief, max_chars=80_000)
    payload = {
        "job_id": job_id,
        "keyword": keyword,
        "generated_at_iso": generated_at_iso,
        "strategy_decisions": strategy_decisions,
        "business_notes": business_notes,
        "structured_brief": compact,
        "rules_draft_markdown": rules_md,
    }
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 500_000:
        payload["rules_draft_markdown"] = rules_md[:200_000] + "\n\n…（底稿过长已截断，请勿编造截断后内容）\n"
        raw = json.dumps(payload, ensure_ascii=False)
    user = STRATEGY_USER_PREFIX + raw
    return call_llm(STRATEGY_SYSTEM, user)


STRATEGY_OPPORTUNITIES_SYSTEM = """你是 B 端市场与增长顾问。输入 JSON 含 ``keyword``、``competitor_brief``（与本任务规则报告同源的结构化摘要，可能经裁剪，并含 ``matrix_overview_for_llm``），以及可选 ``prior_chapter_llm_narratives``（本报告 **第五至第八章** 已生成的大模型归纳节选，与正文**同源**）。

请输出 **Markdown 正文**（不要用 ``` 围栏包裹），将**直接嵌入**宿主文档中**已存在章节标题之下**的小节，读者已知当前处于「策略与机会」相关章节。

**与前文分析严格对齐（硬性，优先于自由发挥）**：
- **定性主题**（各细类讨论焦点、正负向体验、场景与关注词归纳、配料/卖点叙事、促销形态描述等）须与 ``prior_chapter_llm_narratives`` 中已出现的表述**方向一致**，**禁止**另写一套与节选**明显矛盾**的品类判断、品牌举例或用户痛点主题。
- **定量与可核验事实**（价带分位数、店铺/品牌占比、条数、评论统计字段等）**以** ``competitor_brief`` **为准**；若节选与 brief 数字冲突，**采纳 brief**，且勿复述与数字冲突的节选句。
- 若某键未出现在 ``prior_chapter_llm_narratives`` 或内容为空，则该维度**不得**编造与可能存在的报告其他章冲突的细节；仅依据 ``competitor_brief`` 或明确写「输入中未体现」。
- **转化与体验**小节：正负向体验线索须**优先呼应** ``sec8_2_sentiment_theme_attribution`` 与 **第八章第三节 侧**节选（``sec8_3_comment_focus_summaries`` 或 ``sec8_3_text_mining_probe``，视何者存在）；**禁止**将节选未提及的具体抱怨/品类问题写成**主要结论**；可写「假设：待结合业务验证」。

**标题与措辞（硬性）**：
- **禁止**在正文开头或任何位置重复宿主已有章名/小节名，包括但不限于：「第九章」「第9章」「九、」「策略与机会提示」「策略与机会建议」「策略与机会」等；**不要**自造 ``##`` 一级标题；
- 小节标题**仅允许**使用业务主题式 ``####``（如下所列），从第一句起就进入实质内容。

**必须遵守**：
- **数字与事实**：价格分位数、集中度份额、条数、占比等**只能**来自 ``competitor_brief`` 中已有字段；**禁止编造**未出现的品牌销量、具体 GMV、未给出的到手价；
- **语气**：分节给出**可操作的假设性建议**（定价区间思路、应对齐的差异化观测点、应规避的风险、促销与机制设计线索、转化与详情页/评价侧改进方向），每条建议用「假设：」「待验证：」等标明不确定性；
- **结构**：至少使用 ``####`` 组织以下主题（可合并子条，但须覆盖）：**定价与价带**、**差异化与应对齐的优势**、**风险与避免项**、**促销与活动机制**、**转化与体验**；
- **促销与活动机制（硬性）**：该节**必须优先依据** ``competitor_brief.price_promotion_signals``（券后/标价、价差等，若存在），并与 ``prior_chapter_llm_narratives.sec6_promo_group_summaries``（若有）**不矛盾**，给出**假设性**机制建议。**禁止**编造具体满减门槛、红包面额、补贴比例；**禁止**在输入中完全未出现任何列表侧价差或促销归纳信号时，仍写一大段具体「要做满减发红包」而无「输入中未捕获此类信号」的说明。
- **转化与体验（硬性）**：须**同时**写清正向与负向；**禁止**使用「占比均超过 130 次」等**语义不通或混用次数/占比**的表述；数字表述须与 ``competitor_brief`` 一致。
- **禁止**：不要写完整报告目录；不要复述「研究范围与方法」；不要使用 CR1/CR3 缩写（用「第一大……份额」「前三家合计」）；不要输出与输入矛盾的价带描述。

篇幅约 **900～3200 字**（数据丰富可偏长）。"""


STRATEGY_OPPORTUNITIES_USER_PREFIX = (
    "请根据以下 JSON 撰写策略归纳正文（Markdown）。"
    "``competitor_brief`` 为结构化摘要；若含 ``prior_chapter_llm_narratives``，则为 第五至第八章 大模型归纳节选，须与策略正文对齐。"
    "宿主报告已含章节标题，**勿在输出中写第九章或「策略与机会」类标题**。\n\n"
)


def _truncate_strategy_narrative(text: str, max_chars: int) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    if len(s) <= max_chars:
        return s
    return (
        s[: max_chars - 80].rstrip()
        + "\n\n…（前文各章归纳节选已截断；请勿编造截断后内容。）\n"
    )


def _strategy_prompt_fits_context(system: str, user: str) -> bool:
    """若为 False，``chat_completion_text`` 会在发请求前因过长而抛错。"""
    est = estimate_chat_input_tokens(system, user)
    ctx = llm_context_window_size()
    buf = 256
    return est < ctx - buf - 256


def _strategy_completion_avail_tokens(system: str, user: str) -> int:
    """
    与 ``AI_crawler.chat_completion_text`` 中 ``avail = context_window - input_est - buf`` 一致，
    即本次调用实际可用于 **completion** 的上限（随后还会与 ``max_tokens`` 取 min）。
    若该值过小，长文会在句中被截断（例如「转化与体验」末段不完整）。
    """
    est = estimate_chat_input_tokens(system, user)
    ctx = llm_context_window_size()
    buf = 256
    return ctx - est - buf


def _min_strategy_completion_tokens() -> int:
    raw = (os.environ.get("MA_STRATEGY_MIN_COMPLETION_TOKENS") or "2048").strip()
    try:
        return max(256, int(raw))
    except ValueError:
        return 2048


def _strategy_prompt_ok_for_call(system: str, user: str, *, min_completion_tokens: int) -> bool:
    return _strategy_prompt_fits_context(
        system, user
    ) and _strategy_completion_avail_tokens(system, user) >= min_completion_tokens


def generate_strategy_opportunities_llm(
    brief: dict[str, Any],
    *,
    keyword: str,
    chapter_llm_narratives: dict[str, str] | None = None,
) -> str:
    """
    基于 ``build_competitor_brief`` 全量摘要，生成策略与机会小节正文（不含章名，由宿主 Markdown 加标题）。

    ``chapter_llm_narratives`` 为与本报告 第五至第八章 同源的大模型正文节选，键名稳定（见 runner 传入），用于与策略段严格对齐。
    """
    narr_in = {
        k: v
        for k, v in (chapter_llm_narratives or {}).items()
        if isinstance(v, str) and v.strip()
    }
    sys_prompt = STRATEGY_OPPORTUNITIES_SYSTEM

    def _user_from_payload(p: dict[str, Any]) -> str:
        return STRATEGY_OPPORTUNITIES_USER_PREFIX + json.dumps(p, ensure_ascii=False)

    min_comp = _min_strategy_completion_tokens()
    min_comp_relaxed = max(256, min_comp // 2)

    for cap_brief, cap_narr in (
        (48_000, 2_800),
        (42_000, 2_200),
        (36_000, 1_700),
        (30_000, 1_300),
        (26_000, 950),
        (22_000, 700),
        (18_000, 500),
        (16_000, 400),
        (14_000, 320),
        (12_000, 260),
        (10_000, 200),
    ):
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        narratives = {
            k: _truncate_strategy_narrative(v, cap_narr) for k, v in narr_in.items()
        }
        payload: dict[str, Any] = {
            "keyword": keyword,
            "competitor_brief": compact,
        }
        if narratives:
            payload["prior_chapter_llm_narratives"] = narratives
        user = _user_from_payload(payload)
        if _strategy_prompt_ok_for_call(sys_prompt, user, min_completion_tokens=min_comp):
            return call_llm(sys_prompt, user)

    for cap_brief in (40_000, 32_000, 26_000, 20_000, 16_000, 14_000, 12_000, 10_000):
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        payload = {"keyword": keyword, "competitor_brief": compact}
        user = _user_from_payload(payload)
        if _strategy_prompt_ok_for_call(sys_prompt, user, min_completion_tokens=min_comp):
            return call_llm(sys_prompt, user)

    for cap_brief in (14_000, 12_000, 10_000, 8_000):
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        payload = {"keyword": keyword, "competitor_brief": compact}
        user = _user_from_payload(payload)
        if _strategy_prompt_ok_for_call(sys_prompt, user, min_completion_tokens=min_comp_relaxed):
            return call_llm(sys_prompt, user)

    compact = compact_brief_for_llm(brief, max_chars=8_000)
    payload = {"keyword": keyword, "competitor_brief": compact}
    user = _user_from_payload(payload)
    return call_llm(sys_prompt, user)
