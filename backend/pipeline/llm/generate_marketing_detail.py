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
- **对照式理由（不编造）**：当策略或备注能概括「普通/常规同类」的典型痛点（如升糖快、甜腻、纤维低、易饿）时，**why_this_product** 与 **differentiation_vs_alternatives** 须用**对照**写清「为何选本品」；**禁止**捏造未出现的品牌、检测值、具体「高/低百分之几」等。无对标素材时写本品独特点，并在 **open_points_for_business** 提示可补充的对照数据或检测依据（若无则空串）。

**JSON 键（须全部出现，值为字符串；无内容用空串）**：
- what_we_sell：**卖的是什么**（必填，建议 25～80 字）。写清**品类 + 主推形态/规格或适用场景**，让读者**不读策略稿**也能回答「你们在卖哪种货」。**仅可**综合策略稿、`strategy_decisions`（尤其 **pillar_product**、battlefield_one_line、audience_segment、marketing_strategy）、`business_notes` 与 `keyword` 监测语境中已出现的信息；若 `pillar_product` 非空须与之**不矛盾**。无具体商品名时须明确写「待业务补充主推 SKU/品名」，并保留类目词（可与关键词监测范围对读）。
- one_liner_value：一句话价值主张（买家能得到什么）
- buyer_job_to_be_done：购买者的任务或情境（一句）
- key_pain_or_desire：核心痛点或欲望（与策略一致）
- why_this_product：为何要选这一款（**优先** 1～2 句写相对**常规/普通同类**的核心理由，可用泛称如「普通甜面包」「常见饼干」；可从蛋白、膳食纤维、饱腹感、GI 或糖负担、口感、包装形态、配料表等**择输入已支持**的维度；无对照素材则写本品独特点）
- proof_or_trust_angle：信任或证明角度（无依据写「输入未体现」）
- differentiation_vs_alternatives：与替代方案相比的差异（**须含**与常规同类对照的一句话结论；营养数字、GI、每百克含量等**仅可**复述输入已有内容）
- price_value_framing：价位与价值感如何表述（与策略价位可对读；无则「待业务确认」）
- compliance_taboos：表述禁区摘要（来自业务备注或策略风险）
- open_points_for_business：待业务补充（无则空串）
"""

DETAIL_PACK_SYSTEM = """你是京东场景营销内容写手。输入为已定稿的「核心信息卡」JSON 与关键词。请输出**仅一段 UTF-8 JSON 对象**（不要 Markdown 代码围栏）。

**硬性**：
- **仅可**依据核心信息卡展开；**禁止**新增数字、功效、认证、评价引语、竞品具体名（除非信息卡里已有）。
- 购买者视角，短句；禁止输出 JSON 键名英文给最终读者（值全部为中文**多触点上架**可用文案）。
- 不要泄露「核心信息卡」「策略稿」等内部词。
- **更丰富≠编造**：可增加条数与段落，但**每一条**须能从信息卡对应字段找到方向；无依据处写「输入未体现」「待业务核对」，**禁止**为凑字数新增数字、销量、认证、评价引语、具体竞品名。
- **对照式表达（写厚但不编造）**：学习优质商详「先对比再购买」。**detail_headline** 在首句点明品类后，**至少 1 句**用「相对普通/常规同类（泛称，禁止编造品牌）」讲清差异或价值；无依据时用中性句或「具体对比数值待包装/检测与业务核对」。**selling_bullets** 中 **至少 3 条**须为**可感知的对照卖点**，从蛋白、膳食纤维、饱腹感、口感质地、配料/清洁标签、包装控量或便携、GI/糖负担等角度择信息卡**已支持**的项；信息卡未提的维度**不硬写**。**detail_mid_story_paragraphs** 中 **至少 1 段**用「为何不满足于普通同类」叙事，仍须紧扣信息卡，禁止新数字与编造用户故事。
- **每条 listing_titles、listing_subtitle、detail_headline、selling_bullets 的前两条**均须让读者能识别**在卖什么品类/什么货**（须与信息卡 **what_we_sell** 一致，可缩写但**禁止**偷换品类或只剩空洞形容词）。若信息卡 `what_we_sell` 已写品类，文案中**至少一处**直接出现该类目词或同义可识别表述。
- **文生图/文生视频提示词**：须为**可直接复制**到常见文生图、文生视频模型的**中文**描述；**仅可**依据信息卡已有事实与品类，**禁止**在提示词里写「策略稿」「信息卡」「JSON」等元话语；**禁止**要求生成未授权的具体品牌 Logo、真实包装上的可辨认商标、带疗效承诺的贴片字。
- **文生图须「有货、有卖点画面」**（硬性）：
  - 从信息卡 **what_we_sell**、**one_liner_value**、**key_pain_or_desire**、**why_this_product**、**differentiation_vs_alternatives**、**price_value_framing** 中提炼 **1～3 条可画出来的卖点**，写入 ``text_to_image_prompt_main``；**场景图** ``text_to_image_prompt_scene`` 非空时须保留**至少 1 处**同款质地或品类辨识（非空场景图时）。
  - **禁止**整段只有「白底」「居中」「电商主图」「健康食品」等空壳，而**不出现具体货态**（形态、切片、包装类型、手持/摆放方式至少择一）。
  - **口感/质地类**（如松软、酥脆、绵密、有嚼劲）：**必须**写成**可见结构**，不能只写一次形容词了事。例：**松软**→「吐司切片横截面气孔细腻、边缘微翘显蓬松」「轻按后缓慢回弹」「手撕开可见柔软内里」；**酥脆**→「饼干断面层次清晰、碎屑自然」。信息卡未提质地则**不写**，勿编造。
  - **配料/品类视觉**（如全麦）：可写「麸皮颗粒隐约可见」「浅褐全麦外皮」等，**禁止**疗效字幕、血糖仪、前后对比治病画面。

**JSON 键（须全部出现）**：
- listing_titles：字符串数组，**6～9** 条商品短标题备选（每条约 30 字内；**每条须含可识别品类或品名线索**，禁止多条全是空洞套话；其中 **2～3 条**可在有依据时含「相对更…/更少…/不腻」等**对照**表述；其余侧重场景/质地/配料/人群）
- listing_subtitle：一条列表副文案（约 **60～100** 字内，信息不足则取下限；**鼓励**含一句与常规同类对照的价值，无依据则省略）
- detail_headline：商品详情页首屏下 lead，**2～4 句**（**首句须点明卖的是什么货**；**至少 1 句**为相对常规同类的对照或价值；总长约 **80～200** 字）
- selling_bullets：字符串数组，**8～12** 条卖点（每条约 **40 字内**；**至少 3 条**为「本品 vs 常规同类」式差异；整体须覆盖：品类形态、口感/质地（若信息卡有）、蛋白/纤维/饱腹/GI 或糖负担（**仅信息卡有则写**）、配料/健康表述（合规）、包装/规格（若信息卡有）、场景、信任点、与常规品差异等**不同角度**，**禁止** 12 条重复同一句话换说法）
- spec_sidebar_lines：字符串数组，**0～5** 条参数区旁短句（可空数组）
- faq：对象数组，每项含 question、answer 字符串，**5～8** 组；答句不得超出信息卡承诺；其中 **1～2** 组宜为「和普通/常规××有什么不同」类（××用泛称）；可含「怎么保存」「适合谁」
- detail_mid_story_paragraphs：字符串数组，**2～4 段**详情页**首屏之后**的中段叙事；每段 **70～150** 字；**仅**展开信息卡已有卖点与 `what_we_sell`，可分段讲「适合谁—怎么吃—为何值得」；**禁止**新数字、新功效、编造用户故事
- usage_and_pairing_tips：字符串数组，**2～5** 条食用场景、保存提示、搭配建议（如早餐配牛奶）；信息卡未写保存条件则写「输入未体现具体保质期与保存要求，上架前请核对包装」类中性句，**禁止**编造保质期天数
- short_graphic_post_variants：字符串数组，**3～5** 条短图文/种草贴变体；每条 **45～110** 字；须**首句或次句**点明品类；适合复制到站内动态；**禁止**销量名次、虚假好评引语
- live_script_bullets：字符串数组，**4～7** 条直播或短视频**可照读要点**（每条约 **15～40** 字）；按顺序像口播提纲；**禁止**医疗承诺与未证实数据；可与 `live_or_short_hook` 呼应但勿逐句重复
- traceability_note：**依据与边界**（必填，2～4 句）。用业务可读中文说明：本包与信息卡中**哪些承诺方向一致**、**哪些表述须业务或法务核对**、**输入未体现的不得对外宣称**；**禁止**新数字、新功效、新认证。
- main_image_three_points：字符串数组，**恰好 3 条**，主图/首图用超短句（每条建议 6～14 字）；须与 **what_we_sell** 品类一致，可来自卖点压缩，禁止空泛口号
- live_or_short_hook：一条直播或短视频开场钩句（≤40 字）；同一事实约束
- customer_service_opening：一条客服首句/欢迎语建议（≤50 字）；同一事实约束
- text_to_image_prompt_main：字符串，**主图/首图**文生图提示词（建议 **100～260** 字）。**必须**依次包含：① **具体货态**（与 **what_we_sell** 一致的品类+形态，如全麦吐司切片摞放、独立小包饼干）；② **至少一条质地/卖点的视觉化描写**（与信息卡一致，参见上文「松软→截面/按压/手撕」等）；③ **构图与背景**（如白底居中、轻微投影）；④ **光影**（柔和棚拍、写实）；⑤ **规避**（无 Logo、无疗效字、无竞品名）。**英文模型**可关键风格词括注英文。
- text_to_image_prompt_scene：字符串，**场景/生活方式**备选图（建议 **80～200** 字）：早餐桌、手持、厨房台面等；**须含**与主图**同一品类**的清晰货态，并**至少一处**质地或食用情境（如蒸汽、刀切截面、蘸牛奶）。与主图完全重复则宁可缩短但保留情境差分。无合适场景时 ``""``。
- text_to_video_prompt：字符串，文生视频提示词（建议 **100～260** 字），竖屏 9:16、**5～15 秒**。**须**含 **1 个能体现质地或卖点的镜头**（如慢镜撕开吐司见柔软内里、刀切截面特写、轻捏回弹），与信息卡卖点一致；另写开场与转场（推近/平移）。**禁止**疗效字幕、未授权标识；可「无对白」或「一句中性口播」。
"""

# 第一步核心信息卡：必填字符串键（与 ``CORE_CARD_SYSTEM`` 一致）；漏键时补空串。
_CORE_INFO_CARD_STRING_KEYS: tuple[str, ...] = (
    "what_we_sell",
    "one_liner_value",
    "buyer_job_to_be_done",
    "key_pain_or_desire",
    "why_this_product",
    "proof_or_trust_angle",
    "differentiation_vs_alternatives",
    "price_value_framing",
    "compliance_taboos",
    "open_points_for_business",
)


def normalize_core_info_card(data: dict[str, Any]) -> dict[str, Any]:
    """保证 ``core_info_card`` 含全部约定键（字符串）；避免模型或少字段旧 JSON 缺一漏万。"""
    out: dict[str, Any] = dict(data)
    for key in _CORE_INFO_CARD_STRING_KEYS:
        v = out.get(key)
        if v is None:
            out[key] = ""
        elif isinstance(v, str):
            continue
        else:
            out[key] = str(v)
    return out


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
    "detail_mid_story_paragraphs": [],
    "usage_and_pairing_tips": [],
    "short_graphic_post_variants": [],
    "live_script_bullets": [],
}

_DETAIL_PAGE_PACK_LIST_KEYS: frozenset[str] = frozenset(
    {
        "listing_titles",
        "selling_bullets",
        "spec_sidebar_lines",
        "main_image_three_points",
        "faq",
        "detail_mid_story_paragraphs",
        "usage_and_pairing_tips",
        "short_graphic_post_variants",
        "live_script_bullets",
    }
)


def _normalize_faq_items(v: Any) -> list[dict[str, str]]:
    """统一为 ``[{question, answer}, ...]``；模型偶发混用键名或漏字段时尽量可消费。"""
    if not isinstance(v, list):
        return []
    out: list[dict[str, str]] = []
    for item in v:
        if not isinstance(item, dict):
            continue
        q = item.get("question", item.get("q"))
        a = item.get("answer", item.get("a"))
        qs = (q if isinstance(q, str) else str(q)) if q is not None else ""
        as_ = (a if isinstance(a, str) else str(a)) if a is not None else ""
        if qs.strip() or as_.strip():
            out.append({"question": qs, "answer": as_})
    return out


def normalize_detail_page_pack(data: dict[str, Any]) -> dict[str, Any]:
    """保证 ``detail_page_pack`` 含全部约定键，避免模型漏输出或旧 JSON 缺字段。"""
    out: dict[str, Any] = dict(data)
    for key in _DETAIL_PAGE_PACK_DEFAULTS:
        v = out.get(key)
        if key == "faq":
            out["faq"] = _normalize_faq_items(v)
            continue
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
    return normalize_core_info_card(_parse_llm_json(raw))


def generate_detail_page_pack(
    *,
    keyword: str,
    core_info_card: dict[str, Any],
) -> dict[str, Any]:
    core = normalize_core_info_card(core_info_card)
    anchor = (core.get("what_we_sell") or "").strip()
    payload = {
        "keyword": keyword,
        "core_info_card": core,
    }
    user = (
        "请根据以下 JSON 输出营销内容多触点文案（**仅**一段 JSON 对象）。\n"
        + (
            f"**品类锚点（各触点须一致、勿偷换类目）**：{anchor}\n\n"
            if anchor
            else "**品类锚点**：以信息卡 what_we_sell 为准，各触点勿偷换类目。\n\n"
        )
        + "**必填键名（缺一不可，勿省略）**：listing_titles, listing_subtitle, detail_headline, "
        "selling_bullets, spec_sidebar_lines, faq, detail_mid_story_paragraphs, usage_and_pairing_tips, "
        "short_graphic_post_variants, live_script_bullets, traceability_note, main_image_three_points, "
        "live_or_short_hook, customer_service_opening, text_to_image_prompt_main, "
        "text_to_image_prompt_scene, text_to_video_prompt。\n"
        "**丰富度**：在遵守信息卡前提下尽量写满条数与段落；**禁止**为凑字编造。\n"
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
