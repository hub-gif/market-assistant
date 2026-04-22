"""策略稿 → 核心信息卡 → 营销内容多触点文案（两步 LLM，JSON 输出）。"""
from __future__ import annotations

import json
from typing import Any

from .llm_client import call_llm

_MAX_STRATEGY_CHARS = 28_000

CORE_CARD_SYSTEM = """你是电商营销内容顾问。根据用户提供的「策略稿全文」与结构化决策、业务备注，输出**仅一段 UTF-8 JSON 对象**（不要 Markdown 代码围栏，不要前后说明文字）。

**硬性**：
- 事实、数字、功效、检测结论、销量、评价原文：**仅可**来自输入；**禁止**编造未出现的品牌名、数据、「用户说」引语。
- 食品/健康相关：**禁止**治疗承诺与夸大疗效；无依据写「输入未体现」或「待法务确认」。
- 句子短、可落地；兼顾**购买者决策**与**列表/商详/主图等多触点**上架可用性。
- **读者第一眼须知道在卖什么**：禁止通篇只有「价值感」「信任」「体验」而**不出现可识别的品类/形态**（如饼干、燕麦、奶粉、饮料等）。若输入未给出具体 SKU 名，仍须写清**类目 + 形态/规格层级**（如「低 GI 方向早餐饼干（待业务定款）」），不得用「优质好物」「健康之选」等**无品类**的句子糊弄本条。

**JSON 键（须全部出现，值为字符串；无内容用空串）**：
- what_we_sell：**卖的是什么**（必填，建议 25～80 字）。写清**品类 + 主推形态/规格或适用场景**，让读者**不读策略稿**也能回答「你们在卖哪种货」。**仅可**综合策略稿、`strategy_decisions`（尤其 **pillar_product**、battlefield_one_line、audience_segment、marketing_strategy）、`business_notes` 与 `keyword` 监测语境中已出现的信息；若 `pillar_product` 非空须与之**不矛盾**。无具体商品名时须明确写「待业务补充主推 SKU/品名」，并保留类目词（可与关键词监测范围对读）。
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

DETAIL_PACK_SYSTEM = """你是京东场景营销内容写手。输入为已定稿的「核心信息卡」JSON 与关键词。请输出**仅一段 UTF-8 JSON 对象**（不要 Markdown 代码围栏）。

**硬性**：
- **仅可**依据核心信息卡展开；**禁止**新增数字、功效、认证、评价引语、竞品具体名（除非信息卡里已有）。
- 购买者视角，短句；禁止输出 JSON 键名英文给最终读者（值全部为中文**多触点上架**可用文案）。
- 不要泄露「核心信息卡」「策略稿」等内部词。
- **每条 listing_titles、listing_subtitle、detail_headline、selling_bullets 的前两条**均须让读者能识别**在卖什么品类/什么货**（须与信息卡 **what_we_sell** 一致，可缩写但**禁止**偷换品类或只剩空洞形容词）。若信息卡 `what_we_sell` 已写品类，文案中**至少一处**直接出现该类目词或同义可识别表述。
- **文生图/文生视频提示词**：须为**可直接复制**到常见文生图、文生视频模型的**中文**描述；**仅可**依据信息卡已有事实与品类，**禁止**在提示词里写「策略稿」「信息卡」「JSON」等元话语；**禁止**要求生成未授权的具体品牌 Logo、真实包装上的可辨认商标、带疗效承诺的贴片字。
- **文生图须「有货、有卖点画面」**（硬性）：
  - 从信息卡 **what_we_sell**、**one_liner_value**、**key_pain_or_desire**、**why_this_product**、**differentiation_vs_alternatives**、**price_value_framing** 中提炼 **1～3 条可画出来的卖点**，写入 ``text_to_image_prompt_main``；**场景图** ``text_to_image_prompt_scene`` 非空时须保留**至少 1 处**同款质地或品类辨识（非空场景图时）。
  - **禁止**整段只有「白底」「居中」「电商主图」「健康食品」等空壳，而**不出现具体货态**（形态、切片、包装类型、手持/摆放方式至少择一）。
  - **口感/质地类**（如松软、酥脆、绵密、有嚼劲）：**必须**写成**可见结构**，不能只写一次形容词了事。例：**松软**→「吐司切片横截面气孔细腻、边缘微翘显蓬松」「轻按后缓慢回弹」「手撕开可见柔软内里」；**酥脆**→「饼干断面层次清晰、碎屑自然」。信息卡未提质地则**不写**，勿编造。
  - **配料/品类视觉**（如全麦）：可写「麸皮颗粒隐约可见」「浅褐全麦外皮」等，**禁止**疗效字幕、血糖仪、前后对比治病画面。

**JSON 键（须全部出现）**：
- listing_titles：字符串数组，4～6 条商品短标题备选（每条约 30 字内；**每条须含可识别品类或品名线索**，禁止 6 条全是「安心之选」类）
- listing_subtitle：一条列表副文案（约 60 字内）
- detail_headline：商品详情页首屏下 lead，1～2 句（**首句须点明卖的是什么货**，再写价值）
- selling_bullets：字符串数组，5～8 条卖点（每条约 40 字内）
- spec_sidebar_lines：字符串数组，0～3 条参数区旁短句（可空数组）
- faq：对象数组，每项含 question、answer 字符串，3～5 组；答句不得超出信息卡承诺
- traceability_note：**依据与边界**（必填，2～4 句）。用业务可读中文说明：本包与信息卡中**哪些承诺方向一致**、**哪些表述须业务或法务核对**、**输入未体现的不得对外宣称**；**禁止**新数字、新功效、新认证。
- main_image_three_points：字符串数组，**恰好 3 条**，主图/首图用超短句（每条建议 6～14 字）；须与 **what_we_sell** 品类一致，可来自卖点压缩，禁止空泛口号
- live_or_short_hook：一条直播或短视频开场钩句（≤40 字）；同一事实约束
- customer_service_opening：一条客服首句/欢迎语建议（≤50 字）；同一事实约束
- text_to_image_prompt_main：字符串，**主图/首图**文生图提示词（建议 **100～260** 字）。**必须**依次包含：① **具体货态**（与 **what_we_sell** 一致的品类+形态，如全麦吐司切片摞放、独立小包饼干）；② **至少一条质地/卖点的视觉化描写**（与信息卡一致，参见上文「松软→截面/按压/手撕」等）；③ **构图与背景**（如白底居中、轻微投影）；④ **光影**（柔和棚拍、写实）；⑤ **规避**（无 Logo、无疗效字、无竞品名）。**英文模型**可关键风格词括注英文。
- text_to_image_prompt_scene：字符串，**场景/生活方式**备选图（建议 **80～200** 字）：早餐桌、手持、厨房台面等；**须含**与主图**同一品类**的清晰货态，并**至少一处**质地或食用情境（如蒸汽、刀切截面、蘸牛奶）。与主图完全重复则宁可缩短但保留情境差分。无合适场景时 ``""``。
- text_to_video_prompt：字符串，文生视频提示词（建议 **100～260** 字），竖屏 9:16、**5～15 秒**。**须**含 **1 个能体现质地或卖点的镜头**（如慢镜撕开吐司见柔软内里、刀切截面特写、轻捏回弹），与信息卡卖点一致；另写开场与转场（推近/平移）。**禁止**疗效字幕、未授权标识；可「无对白」或「一句中性口播」。
"""

# 第二步 JSON 完整键表；模型漏键或旧落盘缺字段时由 ``normalize_detail_page_pack`` 补齐。
_DETAIL_PAGE_PACK_DEFAULTS: dict[str, Any] = {
    "listing_titles": [],
    "listing_subtitle": "",
    "detail_headline": "",
    "selling_bullets": [],
    "spec_sidebar_lines": [],
    "faq": [],
    "traceability_note": "",
    "main_image_three_points": [],
    "live_or_short_hook": "",
    "customer_service_opening": "",
    "text_to_image_prompt_main": "",
    "text_to_image_prompt_scene": "",
    "text_to_video_prompt": "",
}

_DETAIL_PAGE_PACK_LIST_KEYS: frozenset[str] = frozenset(
    {
        "listing_titles",
        "selling_bullets",
        "spec_sidebar_lines",
        "main_image_three_points",
        "faq",
    }
)


def normalize_detail_page_pack(data: dict[str, Any]) -> dict[str, Any]:
    """保证 ``detail_page_pack`` 含全部约定键，避免模型漏输出或旧 JSON 缺字段。"""
    out: dict[str, Any] = dict(data)
    for key in _DETAIL_PAGE_PACK_DEFAULTS:
        v = out.get(key)
        if key in _DETAIL_PAGE_PACK_LIST_KEYS:
            if isinstance(v, list):
                continue
            out[key] = []
            continue
        if v is None:
            out[key] = ""
        elif not isinstance(v, str):
            out[key] = str(v)
    return out


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
    user = (
        "请根据以下 JSON 输出营销内容多触点文案（**仅**一段 JSON 对象）。\n"
        "**必填键名（缺一不可，勿省略）**：listing_titles, listing_subtitle, detail_headline, "
        "selling_bullets, spec_sidebar_lines, faq, traceability_note, main_image_three_points, "
        "live_or_short_hook, customer_service_opening, text_to_image_prompt_main, "
        "text_to_image_prompt_scene, text_to_video_prompt。\n"
        "**文生图/视频**：须让「货」和卖点**看得见**（如松软→截面气孔、手撕/按压回弹）；禁止整段只有白底健康食品而无具体形态与质地描写。\n"
        "输入数据：\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    raw = call_llm(DETAIL_PACK_SYSTEM, user)
    return normalize_detail_page_pack(_parse_llm_json(raw))


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
