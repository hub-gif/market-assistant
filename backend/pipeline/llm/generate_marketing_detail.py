"""策略稿 → 核心信息卡 → 商详文案包（两步 LLM，JSON 输出）。"""
from __future__ import annotations

import json
from typing import Any

from .llm_client import call_llm

_MAX_STRATEGY_CHARS = 28_000

CORE_CARD_SYSTEM = """你是电商商详与营销文案顾问。根据用户提供的「策略稿全文」与结构化决策、业务备注，输出**仅一段 UTF-8 JSON 对象**（不要 Markdown 代码围栏，不要前后说明文字）。

**硬性**：
- 事实、数字、功效、检测结论、销量、评价原文：**仅可**来自输入；**禁止**编造未出现的品牌名、数据、「用户说」引语。
- 食品/健康相关：**禁止**治疗承诺与夸大疗效；无依据写「输入未体现」或「待法务确认」。
- 句子短、可落地；兼顾**购买者决策**与商详写手可用性。

**JSON 键（须全部出现，值为字符串；无内容用空串）**：
- one_liner_value：一句话价值主张（买家能得到什么）
- buyer_job_to_be_done：购买者的任务或情境（一句）
- key_pain_or_desire：核心痛点或欲望（与策略一致）
- why_this_product：为何要选这一款（相对同类，一句）
- proof_or_trust_angle：信任或证明角度（无依据写「输入未体现」）
- differentiation_vs_alternatives：与替代方案相比的差异（一句）
- price_value_framing：价位与价值感如何表述（与策略价位可对读；无则「待业务确认」）
- compliance_taboos：表述禁区摘要（来自业务备注或策略风险）
- open_points_for_business：待业务补充（无则空串）
"""

DETAIL_PACK_SYSTEM = """你是京东商详向文案手。输入为已定稿的「核心信息卡」JSON 与关键词。请输出**仅一段 UTF-8 JSON 对象**（不要 Markdown 代码围栏）。

**硬性**：
- **仅可**依据核心信息卡展开；**禁止**新增数字、功效、认证、评价引语、竞品具体名（除非信息卡里已有）。
- 购买者视角，短句；禁止输出 JSON 键名英文给最终读者（值全部为中文商详可用文案）。
- 不要泄露「核心信息卡」「策略稿」等内部词。

**JSON 键（须全部出现）**：
- listing_titles：字符串数组，4～6 条商品短标题备选（每条约 30 字内）
- listing_subtitle：一条列表副文案（约 60 字内）
- detail_headline：商详首屏下 lead，1～2 句
- selling_bullets：字符串数组，5～8 条卖点（每条约 40 字内）
- spec_sidebar_lines：字符串数组，0～3 条参数区旁短句（可空数组）
- faq：对象数组，每项含 question、answer 字符串，3～5 组；答句不得超出信息卡承诺
"""


def _truncate_strategy(md: str) -> tuple[str, bool]:
    t = (md or "").strip()
    if len(t) <= _MAX_STRATEGY_CHARS:
        return t, False
    return t[: _MAX_STRATEGY_CHARS].rstrip() + "\n\n…（策略正文已截断，以下同）\n", True


def _parse_llm_json(raw: str) -> dict[str, Any]:
    s = (raw or "").strip()
    if not s:
        raise ValueError("大模型返回为空")
    try:
        out = json.loads(s)
    except json.JSONDecodeError:
        i = s.find("{")
        j = s.rfind("}")
        if i >= 0 and j > i:
            out = json.loads(s[i : j + 1])
        else:
            raise ValueError("大模型返回不是合法 JSON") from None
    if not isinstance(out, dict):
        raise ValueError("大模型 JSON 须为对象")
    return out


def generate_core_info_card(
    *,
    keyword: str,
    strategy_markdown: str,
    strategy_decisions: dict[str, Any] | None,
    business_notes: str,
) -> dict[str, Any]:
    md, truncated = _truncate_strategy(strategy_markdown)
    payload = {
        "keyword": keyword,
        "strategy_markdown": md,
        "strategy_markdown_truncated": truncated,
        "strategy_decisions": strategy_decisions or {},
        "business_notes": (business_notes or "").strip(),
    }
    user = (
        "请根据以下 JSON 输出核心信息卡（仅 JSON 对象）：\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    raw = call_llm(CORE_CARD_SYSTEM, user)
    return _parse_llm_json(raw)


def generate_detail_page_pack(
    *,
    keyword: str,
    core_info_card: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "keyword": keyword,
        "core_info_card": core_info_card,
    }
    user = "请根据以下 JSON 输出商详包（仅 JSON 对象）：\n" + json.dumps(
        payload, ensure_ascii=False
    )
    raw = call_llm(DETAIL_PACK_SYSTEM, user)
    return _parse_llm_json(raw)


def generate_marketing_detail_pack(
    *,
    keyword: str,
    strategy_markdown: str,
    strategy_decisions: dict[str, Any] | None = None,
    business_notes: str = "",
) -> dict[str, Any]:
    core = generate_core_info_card(
        keyword=keyword,
        strategy_markdown=strategy_markdown,
        strategy_decisions=strategy_decisions,
        business_notes=business_notes,
    )
    pack = generate_detail_page_pack(keyword=keyword, core_info_card=core)
    return {
        "core_info_card": core,
        "detail_page_pack": pack,
    }
