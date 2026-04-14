"""
竞品报告 / 策略稿的**大模型生成**：通过 ``crawler_copy/jd_pc_search/AI_crawler`` 的
``chat_completion_text`` 调用，与配料识别共用网关与密钥。
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from django.conf import settings

from .brief_compact import compact_brief_for_llm
from .strategy_draft import build_strategy_draft_markdown


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
消费者反馈 §8.1～8.4 **之后**、第九章策略**之前**。因此写的是**具体分析型补充**，不是篇首速读块。

所有数字、占比、条数、品牌名、价格区间等**必须严格来自输入 JSON**，禁止编造未在输入中出现的定量结论。

**硬性禁止**：
- **不要**使用「## 一」「## 八」等会打乱宿主文档的顶级章节号；请使用 ``####`` 或必要时 ``###`` 作为本段内小节标题；
- **不要**输出完整报告目录或复述「研究范围与方法」长章；
- **不要**撰写 Markdown 表格版「竞品对比矩阵」或罗列 SKU 明细——**正文已含矩阵**，此处只做分组级语义归纳；
- **不要使用** CR1、CR3 等英文缩写；集中度请用「第一大品牌份额」「前三品牌合计份额」。

**请输出**（仅输出将置于 §8.5 下的正文，不要自造「### 8.5」标题行）：
- **Markdown**，约 **800～1500 字**；
- 建议用 ``####`` 组织：**执行摘要级要点**、**竞争与价盘**、**用户声量与负向事由**（须归纳用户在抱怨什么类型的问题，而非只堆关键词）、**与第九章衔接的策略边界**；
- 若有 `comment_sentiment_lexicon`，概括正/负向粗判局限；**负向**写清事由类型（口感、价格、物流等）；
- **归因与引语（硬性）**：`consumer_feedback_by_matrix_group` 与 `comment_sentiment_lexicon` 等均为**跨 SKU/跨店铺的关键词子串或条数统计**，**不能**单独据此推断「某一店铺某一单品」的结论。  
  - **具体体验句式（含口感、包装等）**须以正文 **§8.2** 中带 ``【细类｜SKU｜品名｜店铺】`` 前缀的抽样为准；本段**不要**新增无前缀、无店铺/品名/SKU 指向的「」引语。  
  - 若写口感、包装、物流、价格等**聚合**维度，须点明口径（如「在已合并的评价文本中，『物流』『价格』类关键词命中较多，为全样本子串计数」）；可结合 `matrix_overview_for_llm` 谈细类结构；**可一句**引导读者「见 §8.2 按店铺/品名的负向举例」。  
  - 若 §8.2 已归纳带店铺与 SKU 的负向主题，本段**只做执行摘要级收束**，勿重复编造新引文。  
- 语气专业、中文；缺失项写「本段未提供该项」而非猜测。"""

REPORT_USER_PREFIX = """请根据以下 JSON 撰写上文所述 §8.5 嵌入段落（Markdown 正文，勿加 ### 8.5 标题）。\n\n"""


def generate_competitor_report_markdown_llm(brief: dict[str, Any], keyword: str) -> str:
    # 与章节衔接等 LLM 步骤一致控制体积；默认 350k 易触发网关超时/拒收导致 502
    compact = compact_brief_for_llm(brief, max_chars=100_000)
    payload = {
        "keyword": keyword,
        "competitor_brief": compact,
        "matrix_overview_for_llm": compact.get("matrix_overview_for_llm") or [],
    }
    user = REPORT_USER_PREFIX + json.dumps(payload, ensure_ascii=False)
    return _call_llm(REPORT_SYSTEM, user)


SENTIMENT_LLM_SYSTEM = """你是电商/食品类用户研究助手。输入 JSON 含：
- ``comment_sentiment_lexicon``：关键词规则下的条数与短语命中（粗判，非深度学习）；
- ``positive_lexeme_hits_top`` / ``negative_lexeme_hits_top``：短语级命中摘要（与条形图同源）；
- ``sample_reviews_*``：按同一规则从评价中抽样的短文（已截断），**仅可依据这些原文与 lexicon 数字归纳**。
  每条样本通常以 ``【细类：…｜SKU：…｜品名：…｜店铺：…】`` 开头，表示该句评价对应的 **§5 矩阵细类**、**具体 SKU/商品标题**与**店铺**；写归纳与引用「」短引文时**须让读者能回答「哪家店、哪条 SKU、哪款品名」**——或保留该前缀，或在同一句内用「店铺名 + 品名/SKU」复述一致信息，**禁止**把多条样本混成「用户普遍」却不交代是哪一店哪一品。

**硬性要求**：
- **仅输出 Markdown 正文**（不要用 ``` 围栏包裹全文）；
- **不要编造**样本中未出现的具体事实、品牌、价格、医学功效；
- 条数、占比等**定量表述须与** ``comment_sentiment_lexicon`` **一致**，勿与样本矛盾；
- 若某具体措辞（如「口感偏硬」）**未**出现在任一 ``sample_reviews_*`` 字符串（含前缀后的正文）中，**禁止**用引号写出该句或暗示为直接引语；仅可写「口感相关抱怨在样本/词表中较集中」等聚合表述。
- **不要**只复述「某词出现 N 次」——词频条形图已在报告正文；你的价值是**语义层归纳**：用户在说什么、不满/满意的具体事由是什么。

**建议结构**（使用四级标题 ``####``）：
1. ``#### 正向体验主题``：3～6 条；每条用一句话概括一类满意点（如口感、甜度、饱腹、性价比、物流），**尽量**在句末用简短「」引用样本中的原话片段佐证（无合适原话则省略引号，勿杜撰）。
2. ``#### 负向评价主题归因``：**核心段落**。在「偏负向」与「混合」样本中归纳 **4～8 个具体问题维度**（示例维度，按需选用：口味/难吃/怪味、过甜或寡淡、质地口感、价格与促销、包装破损、物流时效、真伪与效期、与宣传不符、健康/功效疑虑等）。每个维度下用 1～2 条列表项写清「用户具体在抱怨什么」，并**尽量**附上来自 ``sample_reviews_negative_biased`` 或 ``sample_reviews_mixed_tone`` 的「」短引文（引文内**须含** ``【细类…｜…店铺…】`` 前缀，或明确写出与前缀一致的**店铺 + 品名/SKU**）；若某维度在样本中几乎无依据则不要硬写。
3. ``#### 混合评价中的典型张力``（可选）：若 ``sample_reviews_mixed_tone`` 非空，用 2～4 条说明同一条评价里正负并存时在讨论什么（如「认可低糖但嫌口感」）；否则写一句「本批混合样本较少，从略」。
4. ``#### 使用注意``：1～3 句说明：关键词分桶的局限、抽样与截断、与医学/功效结论无关等。

总字数约 **700～1600 字**，简体中文，语气客观。"""


def generate_comment_sentiment_analysis_llm(payload: dict[str, Any]) -> str:
    """基于规则分桶抽样评价 + lexicon 统计，生成 §8.2 大模型解读段落（Markdown）。"""
    p = dict(payload)
    raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        # 超长时优先压缩正向与混合，保留更多负向样本以利主题归因
        for k, cap, maxlen in (
            ("sample_reviews_positive_biased", 8, 200),
            ("sample_reviews_mixed_tone", 6, 200),
            ("sample_reviews_negative_biased", 18, 220),
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
    按「## 一、」…「## 九、」切分规则报告，供大模型按章写衔接分析。
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
- **不得编造**输入中不存在的销量、占比、价格数字；若底稿与摘要中有数字，须保持一致；表述集中度时用「第一大品牌份额」等中文，**不要用** CR1、CR3 缩写；
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
