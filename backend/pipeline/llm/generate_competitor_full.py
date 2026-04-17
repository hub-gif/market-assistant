"""竞品报告 8.5 节：整篇大模型补充（基于结构化 brief）。"""
from __future__ import annotations

import json
from typing import Any

from ..reporting.brief_compact import compact_brief_for_llm
from .llm_client import call_llm, estimate_chat_input_tokens, llm_context_window_size

REPORT_SYSTEM = """你是业务与产品读者顾问。输入 JSON 含 `keyword`、`competitor_brief`（可能经裁剪）、
`matrix_overview_for_llm`（按细分类目的 SKU 数与品牌样本）。

你的输出将**嵌入在规则报告第八章末**（作为「### 8.5 …」的正文，系统已加小节标题与说明），**紧接在**
消费者反馈（第八章第一至三节）**之后**、第九章策略**之前**。因此写的是**具体分析型补充**，不是篇首速读块。

所有数字、占比、条数、品牌名、价格区间等**必须严格来自输入 JSON**，禁止编造未在输入中出现的定量结论。

**硬性禁止**：
- 正文中**勿**写「第九章」「策略与机会」等与宿主文档已有标题**重复**的章名、小节名或起首套话；本段小节仅用 ``####`` 业务主题；
- **不要**使用「## 一」「## 八」等会打乱宿主文档的顶级章节号；请使用 ``####`` 或必要时 ``###`` 作为本段内小节标题；
- **不要**输出完整报告目录或复述「研究范围与方法」长章；
- **不要**撰写 Markdown 表格版「竞品对比矩阵」或罗列 SKU 明细——**正文已含矩阵**，此处只做分组级语义归纳；
- **不要使用** CR1、CR3 等集中度缩写作主表述；集中度请用「第一大店铺/品牌份额」「前三家合计份额」；输入中的英文字段名勿照抄进正文，请写成中文业务用语。

**请输出**（仅输出将置于 8.5 小节 下的正文，不要自造「### 8.5」标题行）：
- **Markdown**，约 **800～1500 字**；
- 建议用 ``####`` 组织：**执行摘要级要点**、**竞争与价盘**、**用户声量与负向事由**（须归纳用户在抱怨什么类型的问题，而非只堆关键词）、**后续可验证动作（假设）**（不写第九章目录或重复策略章内容）；
- 若有 `comment_sentiment_lexicon`，概括正/负向粗判局限；**负向**写清事由类型（口感、价格、物流等）；
- **归因与引语（硬性）**：`consumer_feedback_by_matrix_group` 与 `comment_sentiment_lexicon` 等均为**跨 SKU/跨店铺的关键词子串或条数统计**，**不能**单独据此推断「某一店铺某一单品」的结论。  
  - **具体体验句式（含口感、包装等）**须以正文 **第八章第二节** 中带 ``【细类｜SKU｜品名｜店铺】`` 前缀的抽样为准；本段**不要**新增无前缀、无店铺/品名/SKU 指向的「」引语。  
  - 若写口感、包装、物流、价格等**聚合**维度，须**写明统计范围**（如「在已合并的评价文本中，『物流』『价格』类关键词命中较多，为全样本子串计数」）；可结合 `matrix_overview_for_llm` 谈细类结构；**可一句**引导读者「见第八章第二节按店铺/品名的负向举例」。  
  - 若 第八章第二节 已归纳带店铺与 SKU 的负向主题，本段**只做执行摘要级收束**，勿重复编造新引文。  
- 语气专业、中文；某类信息在输入中缺失时**一句带过数据缺口**即可，**禁止**输出「本段未提供该项」等套话占位。"""

REPORT_USER_PREFIX = """请根据以下 JSON 撰写上文所述 8.5 小节 嵌入段落（Markdown 正文，勿加 ### 8.5 标题）。\n\n"""


def generate_competitor_report_markdown_llm(brief: dict[str, Any], keyword: str) -> str:
    ctx = llm_context_window_size()
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
        if estimate_chat_input_tokens(REPORT_SYSTEM, user) < input_budget:
            return call_llm(REPORT_SYSTEM, user)

    tail = "\n\n…（JSON 已截断以适配上下文；仅依据可见字段撰写，勿编造截断外数字。）\n"
    room = max(0, int((input_budget - 800) / 0.55) - len(REPORT_SYSTEM) - len(REPORT_USER_PREFIX) - len(tail))
    if room < 2000:
        room = 2000
    user = (REPORT_USER_PREFIX + raw[:room] + tail) if raw else (REPORT_USER_PREFIX + "{}" + tail)
    return call_llm(REPORT_SYSTEM, user)
