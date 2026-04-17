"""第八章情感解读、报告分章、章节衔接等 LLM。"""
from __future__ import annotations

import json
import re
from typing import Any

from ..reporting.brief_compact import compact_brief_for_llm
from .llm_client import call_llm

SENTIMENT_LLM_SYSTEM = """你是电商/食品类用户研究助手。输入 JSON 含：

- ``comment_sentiment_lexicon``：子串词表统计（与报告条形图**同一计数方式**，**仅作定量参考**；子串命中≠说话人态度）。
- ``positive_lexeme_hits_top`` / ``negative_lexeme_hits_top``：短语级命中摘要（同源）。
- ``sentiment_bucket_method``：``score_then_lexeme`` 表示**先按 1～5 星分桶**（无评分行再按关键词）；``keyword_substring_heuristic`` 表示**仅关键词**分桶；与条形图一致。``sample_reviews_positive_biased`` / ``negative`` / ``mixed_tone`` 按该规则**机械归类**的抽样，**可能与整句真实褒贬不一致**（例如「软硬适中」曾被误归负向）。
- **``sample_reviews_semantic_pool``**（若有）：本批评价经去重后的**随机/洗牌抽样**（来自全部有效条，不限于某一象限）。**归纳正/负向体验、引用「」短引文时，优先以此池与上述各列表中的原文为准，自行结合语境理解**：转折、对比（如「没那么甜」「软硬适中」）、先抑后扬/先扬后抑整句态度；**不得以子串是否命中负面词来断言该句为抱怨**。

每条样本通常以 ``【细类：…｜SKU：…｜品名：…｜店铺：…】`` 开头，表示 **第五章细类、SKU、品名、店铺**；写归纳与「」引文时须能还原「哪家店、哪条 SKU、哪款品名」，或保留前缀，**禁止**无指代地写「用户普遍…」。

**硬性要求**：
- **仅输出 Markdown 正文**（不要用 ``` 围栏包裹全文）；
- **不要编造**样本中未出现的具体事实、品牌、价格、医学功效；
- **定量数字**（条数、占比、lexicon 各字段）须与 ``comment_sentiment_lexicon`` **一致**，勿编造；
- **定性归纳**（满意点/抱怨点、引语是否算差评）：以**整句语义**为准；若某句在语义上为褒义或中性描述，**不得**放入「质地差、口感硬」等负向归因；若词表归类结果与句意冲突，**以句意为准**，并在「使用注意」点明「关键词归类**仅反映子串计数，不作态度判断**」。
- **负向主题优先级（硬性）**：写「主要」「集中」「突出」类抱怨前，**必须对照** ``negative_lexeme_hits_top`` 各短语的 ``texts_matched``：若「口感硬/咬不动/发硬」等**预设短语命中为 0 或明显低于**其它维度（如分量、少、物流），**不得**把质地硬写成首要负向主题；若抽样原文与语义池里**反复出现**「分量少、太少、不够吃」等而预设短语未列出，仍须**单独归纳**（用户常用生活化表述，不必与预设表完全一致）。
- 若某措辞**未**出现在任一抽样原文（含前缀后正文）中，**禁止**用引号写成直接引语。
- **不要**只复述「某词出现 N 次」——条形图已展示；你的价值是**语义归纳**。

**建议结构**（使用四级标题 ``####``）：
1. ``#### 正向体验主题``：3～6 条；概括满意点（口感、甜度、性价比等），**尽量**用「」引用 ``sample_reviews_semantic_pool`` 或其它样本中**语义确为正面**的短句（勿把对比褒义句当差评例子）。
2. ``#### 负向评价主题归因``：**核心段落**。依据你读后判定为**确有不满**的句子，归纳 **4～8 个**问题维度（须覆盖**质地、分量/规格、价格、物流、包装**等中在原文中**实际出现**的类别，勿只写质地）。引文优先取自句意确为批评的原文（可来自任一档位键，不限于 ``sample_reviews_negative_biased``）；引文须含 ``【细类…｜…店铺…】`` 或同义店铺+品名/SKU。
3. ``#### 混合评价中的典型张力``（可选）：同一评价里褒贬并存时，说明在争什么；若无则略写。
4. ``#### 使用注意``：关键词子串统计的局限、``sample_reviews_semantic_pool`` 与词表归类的差异、抽样截断、非医学结论。

总字数约 **700～1600 字**，简体中文，语气客观。"""


def generate_comment_sentiment_analysis_llm(payload: dict[str, Any]) -> str:
    """基于 lexicon 统计 + 语义池与按词表归类的抽样，生成 第八章第二节 大模型解读段落（Markdown）。"""
    p = dict(payload)
    raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
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
    return call_llm(SENTIMENT_LLM_SYSTEM, user)


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
    text = call_llm(BRIDGE_SECTIONS_SYSTEM, user)
    return _normalize_section_bridge_map(_parse_llm_json_object(text))
