"""
竞品报告 / 策略稿的**大模型生成**：通过 ``crawler_copy/jd_pc_search/AI_crawler`` 的
``chat_completion_text`` 调用，与配料识别共用网关与密钥。
"""
from __future__ import annotations

import json
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


REPORT_SYSTEM = """你撰写一段**短小的「速读与策略补充」**，插在完整规则报告**之前**供读者扫读。读者为业务与产品。

**输入**：JSON 含 `keyword`、`competitor_brief`（可能经裁剪）、`matrix_overview_for_llm`（按细分类目的 SKU 数与品牌样本）。
所有数字、占比、条数、品牌名、价格区间等**必须严格来自输入 JSON**，禁止编造未在输入中出现的定量结论。

**硬性禁止**：
- **不要**输出完整报告目录或重复「研究范围与方法」等长章结构；
- **不要**撰写 Markdown 表格版「竞品对比矩阵」或罗列 SKU 明细——**正文报告已含完整矩阵**，此处仅可概括分组级结论（细类名、SKU 数、主要品牌来自 `matrix_overview_for_llm` / brief）；
- **不要**写「matrix_by_group 已省略」「仅保留代表性品牌」等免责声明，也不要引导读者认为明细缺失；
- **不要使用** CR1、CR3 等英文缩写；集中度请用「第一大品牌份额」「前三品牌合计份额」。

**请输出**（仅输出正文，不要前言后语）：
- 使用 **Markdown**，控制在约 **800～1500 字**；
- 建议小节标题（二级）：**执行摘要要点**、**竞争与价盘速读**、**用户声量与关注点**、**策略提示与数据边界**；
- 若有 `comment_sentiment_lexicon`，概括正/负向粗判与局限（非深度学习）；
- 语气专业、中文；缺失项写「本摘要未提供该项」而非猜测。"""

REPORT_USER_PREFIX = """请根据以下 JSON 撰写完整竞品分析报告（Markdown 正文）。\n\n"""


def generate_competitor_report_markdown_llm(brief: dict[str, Any], keyword: str) -> str:
    compact = compact_brief_for_llm(brief)
    payload = {
        "keyword": keyword,
        "competitor_brief": compact,
        "matrix_overview_for_llm": compact.get("matrix_overview_for_llm") or [],
    }
    user = REPORT_USER_PREFIX + json.dumps(payload, ensure_ascii=False)
    return _call_llm(REPORT_SYSTEM, user)


SENTIMENT_LLM_SYSTEM = """你是电商/食品类用户研究助手。输入 JSON 含：
- ``comment_sentiment_lexicon``：关键词规则下的条数与短语命中（粗判，非深度学习）；
- ``sample_reviews_*``：按同一规则从评价中抽样的短文（已截断），**仅可依据这些原文与 lexicon 数字归纳**。

**硬性要求**：
- **仅输出 Markdown 正文**（不要用 ``` 围栏包裹全文）；
- **不要编造**样本中未出现的具体事实、品牌、价格、医学功效；
- 条数、占比等**定量表述须与** ``comment_sentiment_lexicon`` **一致**，勿与样本矛盾。

**建议结构**（使用四级标题 ``####``）：
1. ``#### 正向要点归纳``：3～6 条要点，概括满意点（口感、甜度、包装、物流、性价比等）；
2. ``#### 负向与风险点归纳``：3～6 条要点；
3. ``#### 使用注意``：1～2 句说明样本量、抽样局限、与关键词规则可能不一致之处。

总字数约 **400～900 字**，简体中文，语气客观。"""


def generate_comment_sentiment_analysis_llm(payload: dict[str, Any]) -> str:
    """基于规则分桶抽样评价 + lexicon 统计，生成 §8.2 大模型解读段落（Markdown）。"""
    p = dict(payload)
    raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        for k in (
            "sample_reviews_positive_biased",
            "sample_reviews_negative_biased",
            "sample_reviews_mixed_tone",
        ):
            lst = p.get(k)
            if isinstance(lst, list):
                p[k] = [str(x)[:140] for x in lst[:8]]
        raw = json.dumps(p, ensure_ascii=False)
    if len(raw) > 88_000:
        raw = raw[:82_000] + "\n\n…（输入过长已截断，请勿编造截断外内容）\n"
    user = "请根据以下 JSON 按系统说明输出 Markdown：\n\n" + raw
    return _call_llm(SENTIMENT_LLM_SYSTEM, user)


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
