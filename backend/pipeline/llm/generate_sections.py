"""评价情感 LLM（可选）：默认**不**写入竞品报告；供独立调用或历史任务兼容。"""
from __future__ import annotations

import json
import re
from typing import Any

from ..reporting.brief_compact import compact_brief_for_llm
from .llm_client import call_llm

SENTIMENT_LLM_SYSTEM = """你是电商/食品类用户研究助手。输入 JSON **仅**含开放语义材料（**不含**预设子串词表、不含机械分桶样本列表）：

- **``sample_reviews_semantic_pool``**：本批评价去重后的**洗牌抽样**原文（可含 ``【细类：…｜SKU：…｜品名：…｜店铺：…】`` 前缀）。**归纳正/负向体验、写「」短引文时只依据本池与 JSON 中其它明文字段**，结合整句语境（转折、反讽、先抑后扬等）；**禁止**凭单一敏感词断言整句为差评。
- **``text_unit_count``** / **``unique_attributed_snippets_count``**：条数统计，勿编造。
- **``star_rating_distribution``**（**若有**）：有评价星级数据时，各档条数（``score_1_2`` / ``score_3`` / ``score_4_5`` / ``no_score``）。**仅作辅助**：低星多不自动等于「口感硬」等具体抱怨主题，须回到原文语义；**禁止**在输出中复述已废弃的「预设短语命中」「lexeme_hits」等口径。
- **``semantic_pool_note``**：字段说明，遵循即可。

**硬性要求**：
- **仅输出 Markdown 正文**（不要用 ``` 围栏包裹全文）；
- **不要编造**样本中未出现的具体事实、品牌、价格、医学功效；
- **定量**：若输入含 ``star_rating_distribution``，其中数字须与 JSON **一致**；``text_unit_count`` 与池子规模须自洽，勿编造；
- **定性**：负向主题**只写**你在原文中读后能站稳的抱怨；若池中**几乎没有**明确批评句，须如实写「本批抽样内负向语义证据有限」，**禁止**为凑结构编造「口感硬」等未在引文中出现的典型抱怨。
- 某措辞**未**出现在任一抽样原文（含前缀后正文）中，**禁止**用引号写成直接引语。
- **不要**在输出里提及「预设词表」「子串命中」「lexeme」「关键词分桶」等已废弃机制。

**建议结构**（使用四级标题 ``####``）：
1. ``#### 正向体验主题``：3～6 条；尽量用「」引用池中**语义确为正面**的短句。
2. ``#### 负向评价主题归因``：依据原文归纳；证据不足时简短说明，勿硬写。
3. ``#### 混合评价中的典型张力``（可选）：若无则略。
4. ``#### 使用注意``：抽样截断、星级与语义可能不一致、非医学结论。

**篇幅**：若 JSON 含 ``matrix_group_focus``，约 **500～1200 字**；否则约 **700～1600 字**。简体中文，语气客观。"""


def generate_comment_sentiment_analysis_llm(payload: dict[str, Any]) -> str:
    """基于开放语义池（及可选星级分布）生成评价正/负向主题归纳（Markdown）。"""
    p = dict(payload)
    scope_note = ""
    mg = p.get("matrix_group_focus")
    if isinstance(mg, str) and mg.strip():
        scope_note = (
            f"\n\n【范围】以下评价与统计**仅**来自细类「{mg.strip()}」；"
            "正向/负向主题须贴合**该细类**语境，勿笼统写成「全关键词下用户普遍…」。\n"
        )
    raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        lst = p.get("sample_reviews_semantic_pool")
        if isinstance(lst, list):
            p["sample_reviews_semantic_pool"] = [
                str(x)[:280] for x in lst[:24]
            ]
        raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        raw = raw[:82_000] + "\n\n…（输入过长已截断，请勿编造截断外内容）\n"
    user = "请根据以下 JSON 按系统说明输出 Markdown：" + scope_note + "\n\n" + raw
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
- **店铺/自营相关（硬性）**：**禁止**编造「京东自营 SKU 占比」「自营占比超 X%」「POP/第三方占比」等**未在输入中出现的**具体比例或款数；若 ``competitor_brief.concentration.shops_from_list`` 有数据，写店铺集中度时须与之一致，并区分 **按列表行** 与 **按去重 SKU**（见 ``unique_sku_basis``），**禁止**将列表曝光写成「市场份额」或笼统「SKU 占比」。
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
