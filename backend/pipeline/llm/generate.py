"""
竞品报告 / 策略稿的**大模型生成**：通过 ``crawler_copy/jd_pc_search/AI_crawler`` 的
``chat_completion_text`` 调用，与配料识别共用网关与密钥。
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from django.conf import settings

from ..reporting.brief_compact import compact_brief_for_llm
from ..reporting.strategy_draft import build_strategy_draft_markdown


def _ensure_ai_crawler_path() -> None:
    root = Path(settings.CRAWLER_JD_ROOT).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"爬虫副本目录不存在: {root}")
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)


def _call_llm(system_prompt: str, user_prompt: str) -> str:
    _ensure_ai_crawler_path()
    import AI_crawler as ac  # noqa: WPS433

    raw = ac.chat_completion_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    return ac.strip_outer_markdown_fence(raw)


REPORT_SYSTEM = """你是业务与产品读者顾问。输入 JSON 含 `keyword`、`competitor_brief`（可能经裁剪）、
`matrix_overview_for_llm`（按细分类目的 SKU 数与品牌样本）。

你的输出将**嵌入在规则报告第八章末**（作为「### 8.5 …」的正文，系统已加小节标题与说明），**紧接在**
消费者反馈 §8.1～8.3 **之后**、第九章策略**之前**。因此写的是**具体分析型补充**，不是篇首速读块。

所有数字、占比、条数、品牌名、价格区间等**必须严格来自输入 JSON**，禁止编造未在输入中出现的定量结论。

**硬性禁止**：
- 正文中**勿**写「第九章」「策略与机会」等与宿主文档已有标题**重复**的章名、小节名或起首套话；本段小节仅用 ``####`` 业务主题；
- **不要**使用「## 一」「## 八」等会打乱宿主文档的顶级章节号；请使用 ``####`` 或必要时 ``###`` 作为本段内小节标题；
- **不要**输出完整报告目录或复述「研究范围与方法」长章；
- **不要**撰写 Markdown 表格版「竞品对比矩阵」或罗列 SKU 明细——**正文已含矩阵**，此处只做分组级语义归纳；
- **不要使用** CR1、CR3 等集中度缩写作主表述；集中度请用「第一大店铺/品牌份额」「前三家合计份额」；输入中的英文字段名勿照抄进正文，请写成中文业务用语。

**请输出**（仅输出将置于 §8.5 下的正文，不要自造「### 8.5」标题行）：
- **Markdown**，约 **800～1500 字**；
- 建议用 ``####`` 组织：**执行摘要级要点**、**竞争与价盘**、**用户声量与负向事由**（须归纳用户在抱怨什么类型的问题，而非只堆关键词）、**后续可验证动作（假设）**（不写第九章目录或重复策略章内容）；
- 若有 `comment_sentiment_lexicon`，概括正/负向粗判局限；**负向**写清事由类型（口感、价格、物流等）；
- **归因与引语（硬性）**：`consumer_feedback_by_matrix_group` 与 `comment_sentiment_lexicon` 等均为**跨 SKU/跨店铺的关键词子串或条数统计**，**不能**单独据此推断「某一店铺某一单品」的结论。  
  - **具体体验句式（含口感、包装等）**须以正文 **§8.2** 中带 ``【细类｜SKU｜品名｜店铺】`` 前缀的抽样为准；本段**不要**新增无前缀、无店铺/品名/SKU 指向的「」引语。  
  - 若写口感、包装、物流、价格等**聚合**维度，须**写明统计范围**（如「在已合并的评价文本中，『物流』『价格』类关键词命中较多，为全样本子串计数」）；可结合 `matrix_overview_for_llm` 谈细类结构；**可一句**引导读者「见 §8.2 按店铺/品名的负向举例」。  
  - 若 §8.2 已归纳带店铺与 SKU 的负向主题，本段**只做执行摘要级收束**，勿重复编造新引文。  
- 语气专业、中文；某类信息在输入中缺失时**一句带过数据缺口**即可，**禁止**输出「本段未提供该项」等套话占位。"""

REPORT_USER_PREFIX = """请根据以下 JSON 撰写上文所述 §8.5 嵌入段落（Markdown 正文，勿加 ### 8.5 标题）。\n\n"""


def _estimated_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    """与 ``AI_crawler._estimate_chat_input_tokens`` 一致，用于在调用前预判上下文。"""
    total_chars = len(system_prompt or "") + len(user_prompt or "")
    return int(total_chars * 0.55) + 512


def _llm_context_window_size() -> int:
    raw = (
        os.environ.get("LLM_CONTEXT_WINDOW")
        or os.environ.get("OPENAI_CONTEXT_WINDOW")
        or "32768"
    ).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return 32768


def generate_competitor_report_markdown_llm(brief: dict[str, Any], keyword: str) -> str:
    # 与 AI_crawler.chat_completion_text 一致：input_est 须 < ctx - buf - 256，否则拒调
    ctx = _llm_context_window_size()
    buf = 256
    input_budget = ctx - buf - 256

    caps = (88_000, 64_000, 48_000, 34_000, 24_000, 16_000, 11_000, 7_500)
    user = ""
    for max_chars in caps:
        compact = compact_brief_for_llm(brief, max_chars=max_chars)
        payload = {
            "keyword": keyword,
            "competitor_brief": compact,
            "matrix_overview_for_llm": compact.get("matrix_overview_for_llm") or [],
        }
        raw = json.dumps(payload, ensure_ascii=False)
        user = REPORT_USER_PREFIX + raw
        if _estimated_chat_input_tokens(REPORT_SYSTEM, user) < input_budget:
            return _call_llm(REPORT_SYSTEM, user)

    # 仍过大：截断 user JSON（保留 matrix_overview 在 compact 内已尽量精简）
    tail = "\n\n…（JSON 已截断以适配上下文；仅依据可见字段撰写，勿编造截断外数字。）\n"
    room = max(0, int((input_budget - 800) / 0.55) - len(REPORT_SYSTEM) - len(REPORT_USER_PREFIX) - len(tail))
    if room < 2000:
        room = 2000
    user = (REPORT_USER_PREFIX + raw[:room] + tail) if raw else (REPORT_USER_PREFIX + "{}" + tail)
    return _call_llm(REPORT_SYSTEM, user)


SENTIMENT_LLM_SYSTEM = """你是电商/食品类用户研究助手。输入 JSON 含：

- ``comment_sentiment_lexicon``：子串词表统计（与报告条形图**同一计数方式**，**仅作定量参考**；子串命中≠说话人态度）。
- ``positive_lexeme_hits_top`` / ``negative_lexeme_hits_top``：短语级命中摘要（同源）。
- ``sentiment_bucket_method``：恒为 ``keyword_substring_heuristic``；``sample_reviews_positive_biased`` / ``negative`` / ``mixed_tone`` 是按该词表**机械归类**的抽样，**可能与整句真实褒贬不一致**（例如「软硬适中」曾被误归负向）。
- **``sample_reviews_semantic_pool``**（若有）：本批评价经去重后的**随机/洗牌抽样**，覆盖未命中任一关键词的句子。**归纳正/负向体验、引用「」短引文时，优先以此池与上述各列表中的原文为准，自行结合语境理解**：转折、对比（如「没那么甜」「软硬适中」）、先抑后扬/先扬后抑整句态度；**不得以子串是否命中负面词来断言该句为抱怨**。

每条样本通常以 ``【细类：…｜SKU：…｜品名：…｜店铺：…】`` 开头，表示 **§5 细类、SKU、品名、店铺**；写归纳与「」引文时须能还原「哪家店、哪条 SKU、哪款品名」，或保留前缀，**禁止**无指代地写「用户普遍…」。

**硬性要求**：
- **仅输出 Markdown 正文**（不要用 ``` 围栏包裹全文）；
- **不要编造**样本中未出现的具体事实、品牌、价格、医学功效；
- **定量数字**（条数、占比、lexicon 各字段）须与 ``comment_sentiment_lexicon`` **一致**，勿编造；
- **定性归纳**（满意点/抱怨点、引语是否算差评）：以**整句语义**为准；若某句在语义上为褒义或中性描述，**不得**放入「质地差、口感硬」等负向归因；若词表归类结果与句意冲突，**以句意为准**，并在「使用注意」点明「关键词归类**仅反映子串计数，不作态度判断**」。
- 若某措辞**未**出现在任一抽样原文（含前缀后正文）中，**禁止**用引号写成直接引语。
- **不要**只复述「某词出现 N 次」——条形图已展示；你的价值是**语义归纳**。

**建议结构**（使用四级标题 ``####``）：
1. ``#### 正向体验主题``：3～6 条；概括满意点（口感、甜度、性价比等），**尽量**用「」引用 ``sample_reviews_semantic_pool`` 或其它样本中**语义确为正面**的短句（勿把对比褒义句当差评例子）。
2. ``#### 负向评价主题归因``：**核心段落**。依据你读后判定为**确有不满**的句子，归纳 **4～8 个**问题维度（口味、质地、价格、物流等）。引文优先取自句意确为批评的原文（可来自任一档位键，不限于 ``sample_reviews_negative_biased``）；引文须含 ``【细类…｜…店铺…】`` 或同义店铺+品名/SKU。
3. ``#### 混合评价中的典型张力``（可选）：同一评价里褒贬并存时，说明在争什么；若无则略写。
4. ``#### 使用注意``：关键词子串统计的局限、``sample_reviews_semantic_pool`` 与词表归类的差异、抽样截断、非医学结论。

总字数约 **700～1600 字**，简体中文，语气客观。"""


def generate_comment_sentiment_analysis_llm(payload: dict[str, Any]) -> str:
    """基于 lexicon 统计 + 语义池与按词表归类的抽样，生成 §8.2 大模型解读段落（Markdown）。"""
    p = dict(payload)
    raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        # 超长时优先压缩关键词归类样本，再压缩语义池；尽量保留 semantic_pool 条数略多
        for k, cap, maxlen in (
            ("sample_reviews_positive_biased", 6, 180),
            ("sample_reviews_mixed_tone", 4, 180),
            ("sample_reviews_negative_biased", 14, 200),
            ("sample_reviews_semantic_pool", 30, 340),
        ):
            lst = p.get(k)
            if isinstance(lst, list):
                p[k] = [str(x)[:maxlen] for x in lst[:cap]]
        raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        raw = raw[:82_000] + "\n\n…（输入过长已截断，请勿编造截断外内容）\n"
    user = "请根据以下 JSON 按系统说明输出 Markdown：\n\n" + raw
    return _call_llm(SENTIMENT_LLM_SYSTEM, user)


def split_competitor_report_for_bridges(
    md: str, *, max_excerpt: int = 1200
) -> dict[str, dict[str, str]]:
    """
    按「## 一、」…「## 九、」切分规则报告；**只返回正文中实际出现的章**（略去未输出的章）。
    每键含完整标题行与正文摘录（过长截断）。
    """
    pat = re.compile(r"^## ([一二三四五六七八九])、([^\n]*)$", re.MULTILINE)
    matches = list(pat.finditer(md))
    out: dict[str, dict[str, str]] = {}
    for i, m in enumerate(matches):
        key = m.group(1)
        rest = m.group(2)
        title = f"## {key}、{rest}"
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(md)
        body = md[start:end].strip()
        exc = body[:max_excerpt]
        if len(body) > max_excerpt:
            exc += "\n\n…（本节摘录已截断）\n"
        out[key] = {"title": title, "excerpt": exc}
    return out


def _parse_llm_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```\s*$", "", raw)
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", raw)
    if m:
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else {}
        except json.JSONDecodeError:
            pass
    return {}


def _normalize_section_bridge_map(d: dict[str, Any]) -> dict[str, str]:
    allowed = frozenset("一二三四五六七八九")
    out: dict[str, str] = {}
    for k, v in d.items():
        if not isinstance(k, str) or len(k) != 1 or k not in allowed:
            continue
        if isinstance(v, str) and v.strip():
            out[k] = v.strip()
    return out


BRIDGE_SECTIONS_SYSTEM = """你是竞品监测报告的**章节衔接**撰稿助手。

**输入 JSON** 含：
- ``keyword``：监测词；
- ``competitor_brief``：与本报告一致的**结构化摘要**（已裁剪体积）；
- ``sections``：键为汉字「一」～「九」，每项含 ``title``（该章完整二级标题行）与 ``excerpt``（该章正文开头摘录，可能已截断）。

**任务**：为 **sections 中出现的每一键** 各写一段 **衔接性分析**（帮读者从摘要与摘录过渡到读该章表格/图），并与 ``competitor_brief`` 中的数字与结论一致。

**硬性要求**：
- **仅输出一个 UTF-8 JSON 对象**（不要用 markdown 代码围栏包裹整段输出）；
- 键必须为「一」「二」…「九」之一，且 **只对输入 sections 里存在的键** 给出字符串值；可省略无材料的键；
- 每个值为 **Markdown 片段**（约 3～10 句中文），**禁止**使用 ``## `` 开头的行（不要写新的二级章标题）；可使用 ``###`` / ``####`` 或加粗小标题；
- 所有**定量表述**须能在 ``competitor_brief`` 或对应 ``excerpt`` 中找到依据，**禁止编造** SKU 数、份额、价格；
- 不要复述整章表格；不要写「详见下文矩阵」以外的空洞套话；可点出该章阅读重点（如价盘带、矩阵细类、评价规则局限等）。"""


def generate_section_bridges_llm(
    *,
    keyword: str,
    brief: dict[str, Any],
    sections: dict[str, dict[str, str]],
) -> dict[str, str]:
    """一次 LLM 调用，返回各章衔接 Markdown 片段（键：一～九）。"""
    if not sections:
        return {}
    compact = compact_brief_for_llm(brief, max_chars=100_000)
    sec: dict[str, dict[str, str]] = {
        k: {"title": v.get("title", ""), "excerpt": v.get("excerpt", "")}
        for k, v in sections.items()
        if isinstance(v, dict)
    }
    for max_exc in (1200, 900, 600, 400, 280):
        for v in sec.values():
            ex = v.get("excerpt") or ""
            if len(ex) > max_exc:
                v["excerpt"] = ex[:max_exc] + "\n…\n"
        payload = {
            "keyword": keyword,
            "competitor_brief": compact,
            "sections": sec,
        }
        raw = json.dumps(payload, ensure_ascii=False)
        if len(raw) <= 92_000:
            break
    user = "请严格按系统说明，**只输出一个 JSON 对象**（键为一～九，值为 Markdown 字符串）：\n\n" + raw
    text = _call_llm(BRIDGE_SECTIONS_SYSTEM, user)
    return _normalize_section_bridge_map(_parse_llm_json_object(text))


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
    return _call_llm(STRATEGY_SYSTEM, user)


MATRIX_GROUPS_SYSTEM = """你是竞品分析顾问。输入为 JSON：``keyword`` 与 ``groups`` 数组。
每个 group 含 ``group``（细分类目名）、``sku_count``、``price_stats``（该细类深入合并行可解析展示价的 min/max/median/mean/n，与 **§6「按细类价盘」** 分位数表同源；无则 n=0 或缺字段）、
``lines``（该细类下若干 SKU 的标题/卖点/配料**摘录**，均来自页面抓取拼接，可能截断）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题（不要使用 ``##`` 一级标题）；
- 每段约 **100～200 字**中文：**主体**归纳该细类下**卖点表述共性**、**配料类型/宣称共性**（摘录中无配料则写「配料摘录较少」）；**品牌格局**可一句概括（仅依据摘录中可见品牌/系列，勿编造销量排名）；
- **价带/价位**：若 ``price_stats.n`` 为大于 0 的整数，**仅允许**用该对象里的数值写价带（如 min～max、中位数），且须与 ``price_stats`` **完全一致**，**禁止**写「价格带未明确」「未体现具体价位」「多为中端」等**与上述数值相矛盾**的表述；若 n=0 或无可信数值，**不要猜测价位**，可写一句「深入样本可解析数值价不足，价盘以 **§6** 表格为准」；
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
    return _call_llm(MATRIX_GROUPS_SYSTEM, user)


COMMENT_GROUPS_SYSTEM = """你是用户研究与品类顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（与 §5 矩阵一致的细分类目名）、``comment_flat_rows``、``effective_text_lines``、
``focus_hit_lines``（关注词子串命中摘要，与 §8.3 同源）、``sample_text_snippets``（评价短摘录，已截断）。
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

    ctx = _llm_context_window_size()
    budget = ctx - 512 - 256
    # 预留 ``max_tokens=8192`` 的完成空间；网关计输入 tokens 常高于本地粗估
    def _input_ok(system: str, user_p: str) -> bool:
        est = _estimated_chat_input_tokens(system, user_p)
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
    return _call_llm(COMMENT_GROUPS_SYSTEM, user)


SCENARIO_GROUPS_SYSTEM = """你是用户研究与品类顾问。输入为 JSON：``keyword``、``scenario_lexicon``、``groups``。
``scenario_lexicon`` 列出各场景标签及示例触发子串（与报告 **§8.3** 右栏统计规则一致）。
``groups`` 每项含 ``group``（与 §5 矩阵一致的细分类目名）、``effective_text_count``（有效评价文本条数）、
``scenario_distribution``（各预设场景的 ``mention_rows`` 与 ``share_of_effective_texts``；**一条评价可计入多场景**；与 §8.3 图右栏同源）、
``sample_text_snippets``（摘录行常含细类、SKU、品名、店铺等前缀的短引文，已截断）。
统计为**子串命中**，不是语义主题模型。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该条 ``group`` 字段**完全一致**的细类名作为小节标题；
- 每段约 **100～220 字**：归纳该细类用户**自述的使用场景/用途**结构（哪些场景标签相对突出、多场景叠加是否常见），可点到与其他细类的差异；**所有条数与占比须与 ``scenario_distribution``、``effective_text_count`` 一致**，禁止编造；
- 引用原话时须保留或复述摘录中的店铺/SKU/品名信息，勿虚构；
- **禁止** Markdown 表格、禁止复述全部摘录；若 ``effective_text_count`` 很小，写明「样本较少，归纳供启发」。

总输出约 **600～3200 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


SCENARIO_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告 §8.3（右栏：使用场景）之后的「使用场景要点归纳」正文（Markdown）。\n\n"
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
    ctx = _llm_context_window_size()
    budget = ctx - 512 - 256

    def _input_ok(system: str, user_p: str) -> bool:
        est = _estimated_chat_input_tokens(system, user_p)
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
    return _call_llm(SCENARIO_GROUPS_SYSTEM, user)


PRICE_GROUPS_SYSTEM = """你是定价与渠道顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（细分类目名，与 §5 矩阵、§6「按细类价盘」小节一致）、``sku_count``、``price_stats``（该细类可解析展示价的 min/max/median/mean/n，与 §6 各细类 Markdown 分位数表同源）、
``listing_snippets``（若干「标题｜标价｜券后｜详情价」摘录，来自合并表字段，已截断）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题；
- 每段约 **80～200 字**中文，**只写价盘与价差**：用 ``price_stats`` 概括价带/离散度（如 min～max、中位数、相对集中或拉得开）；用 ``listing_snippets`` 归纳**标价 vs 券后 vs 详情价**是否常一致、是否常见券后低于标价、价差幅度的大致印象；**可一句**联系标题里**显式出现的规格数字**（如克重、件数）解释**价高/价差大是否可能来自大规格或组合装**——仅当摘录里确有数字时写，勿展开成宣称解读；
- **硬性禁止**（本章不是卖点章）：不要列举或归纳「0 蔗糖 / 低 GI / 全麦 / 代餐 / 孕妇 / 控糖」等**营销宣称或场景关键词**；不要写配料、功效、品牌叙事、用户画像；这些若出现应留给报告 **§5 细类要点归纳**。
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
    return _call_llm(PRICE_GROUPS_SYSTEM, user)


PROMO_GROUPS_SYSTEM = """你是电商促销与价盘顾问。输入为 JSON：``keyword`` 与 ``groups``。
每个 group 含 ``group``（细分类目名，与 §5 矩阵、§6 一致）、``sku_count``、``rows_with_buyer_promo_text``（该细类合并表中「促销摘要」非空行数）、
``promo_snippets``（若干条摘录：标题 + 促销摘要/购买者侧榜单/列表卖点与腰带等，已截断）。

请**为每个细类**输出一小段 Markdown（全部 groups 都要写，顺序与输入一致）：
- 以 ``#### `` + 与该 group 字段**完全一致**的细类名作为小节标题；
- 每段约 **80～220 字**中文，**只写促销与活动形态**：如券后/满减/百亿补贴/新人包邮/限购提示/「到手价」展示方式、与 **§6.1** 规则统计可对照的**活动话术密度**印象；可一句点出**榜单/腰带**是否常见、是否与价格带并存；
- **硬性禁止**：不要展开配料、功效、用户画像；不要复述 §5 的配料归纳；不要编造未在摘录中出现的具体金额或活动规则；
- 若该细类 ``rows_with_buyer_promo_text`` 为 0 且摘录几乎只有标题，写「该细类缺少购买者侧促销摘要，从略」。

总输出约 **500～2800 字**。仅输出正文 Markdown，不要用代码围栏包裹全文。"""


PROMO_GROUPS_USER_PREFIX = (
    "请根据以下 JSON 撰写竞品报告第六章「细类促销与活动要点归纳」正文（Markdown）。"
    "依据 ``promo_snippets`` 中的促销摘要与列表文案，**不写**价带分位数（留给上一小节）。\n\n"
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
    return _call_llm(PROMO_GROUPS_SYSTEM, user)


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


STRATEGY_OPPORTUNITIES_SYSTEM = """你是 B 端市场与增长顾问。输入 JSON 含 ``keyword`` 与 ``competitor_brief``（与本任务规则报告同源的结构化摘要，可能经裁剪，并含 ``matrix_overview_for_llm``）。

请输出 **Markdown 正文**（不要用 ``` 围栏包裹），将**直接嵌入**宿主文档中**已存在章节标题之下**的小节，读者已知当前处于「策略与机会」相关章节。

**标题与措辞（硬性）**：
- **禁止**在正文开头或任何位置重复宿主已有章名/小节名，包括但不限于：「第九章」「第9章」「九、」「策略与机会提示」「策略与机会建议」「策略与机会」等；**不要**自造 ``##`` 一级标题；
- 小节标题**仅允许**使用业务主题式 ``####``（如下所列），从第一句起就进入实质内容。

**必须遵守**：
- **数字与事实**：价格分位数、集中度份额、条数、占比等**只能**来自输入 JSON 中已有字段；**禁止编造**未出现的品牌销量、具体 GMV、未给出的到手价；
- **语气**：分节给出**可操作的假设性建议**（定价区间思路、应对齐的差异化观测点、应规避的风险、促销与机制设计线索、转化与详情页/评价侧改进方向），每条建议用「假设：」「待验证：」等标明不确定性；
- **结构**：至少使用 ``####`` 组织以下主题（可合并子条，但须覆盖）：**定价与价带**、**差异化与应对齐的优势**、**风险与避免项**、**促销与活动机制**、**转化与体验**；
- **转化与体验（硬性）**：该节须**同时**写清 **正向体验**（如详情呈现、规格可读性、评价中反复被肯定的点、有助于信任与下单的线索，**仅依据** brief 中可见字段）与 **负向体验/摩擦**（如评价侧抱怨主题、体验短板、可能损害转化的信号及**待验证**的改进方向）；不得只写一侧；无足够依据时写明「输入中信号不足」而非编造；
- **禁止**：不要写完整报告目录；不要复述「研究范围与方法」；不要使用 CR1/CR3 缩写（用「第一大……份额」「前三家合计」）；不要输出与输入矛盾的价带描述。

篇幅约 **900～3200 字**（数据丰富可偏长）。"""


STRATEGY_OPPORTUNITIES_USER_PREFIX = (
    "请根据以下 JSON 撰写策略归纳正文（Markdown）。宿主报告已含章节标题，**勿在输出中写第九章或「策略与机会」类标题**。\n\n"
)


def generate_strategy_opportunities_llm(
    brief: dict[str, Any], *, keyword: str
) -> str:
    """
    基于 ``build_competitor_brief`` 全量摘要，生成策略与机会小节正文（不含章名，由宿主 Markdown 加标题）。
    """
    compact = compact_brief_for_llm(brief, max_chars=100_000)
    payload = {"keyword": keyword, "competitor_brief": compact}
    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 110_000:
        compact = compact_brief_for_llm(brief, max_chars=65_000)
        payload = {"keyword": keyword, "competitor_brief": compact}
        raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 110_000:
        compact = compact_brief_for_llm(brief, max_chars=40_000)
        payload = {"keyword": keyword, "competitor_brief": compact}
        raw = json.dumps(payload, ensure_ascii=False)
    user = STRATEGY_OPPORTUNITIES_USER_PREFIX + raw
    return _call_llm(STRATEGY_OPPORTUNITIES_SYSTEM, user)
