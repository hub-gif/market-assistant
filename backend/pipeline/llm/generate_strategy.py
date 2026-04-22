"""独立策略稿润色（`generate_strategy_draft_markdown_llm`）与可选的报告「策略与机会」块归纳（`generate_strategy_opportunities_llm`，默认产线关闭）。"""
from __future__ import annotations

import json
import os
from typing import Any

from ..reporting.brief_compact import compact_brief_for_llm
from ..reporting.strategy_draft import (
    build_strategy_draft_markdown,
    report_uses_chapter8_text_mining_probe,
)
from .llm_client import call_llm, estimate_chat_input_tokens, llm_context_window_size

# 与策略生成表单 POST 字段一致：任一则视为业务已提供「实质决策」，否则由模型基于数据推断草案。
_STRATEGY_DECISION_SUBSTANTIVE_KEYS: tuple[str, ...] = (
    "product_role",
    "battlefield_one_line",
    "audience_segment",
    "time_horizon",
    "success_criteria",
    "non_goals",
    "positioning_choice",
    "competitive_stance",
    "pillar_product",
    "pillar_price",
    "pillar_channel",
    "pillar_comm",
    "marketing_strategy",
    "general_strategy",
    "competitor_reference",
    "resource_notes",
)


def strategy_decisions_substantive(strategy_decisions: dict[str, Any] | None) -> bool:
    """是否填写了至少一项策略表单文本字段（用于 LLM 是否「自动推断」全稿）。"""
    if not isinstance(strategy_decisions, dict):
        return False
    for k in _STRATEGY_DECISION_SUBSTANTIVE_KEYS:
        v = strategy_decisions.get(k)
        if isinstance(v, str) and v.strip():
            return True
    return False


def _omit_ch8_probe_wordchart_fields(compact: dict[str, Any]) -> None:
    """
    第八章文本挖掘（探针）为主时，去掉与**预设关注词/场景条形图**同源的统计字段，
    避免与报告 §8 文本挖掘主口径「两用数据」。

    仅影响传入大模型的 ``structured_brief``；``brief`` 全量仍可由规则稿使用。
    """
    for k in (
        "comment_focus_keywords",
        "usage_scenarios",
        "usage_scenarios_denominator",
        "usage_scenarios_by_matrix_group",
        "comment_sentiment_lexicon",
        "strategy_hints",
    ):
        compact.pop(k, None)
    cfb = compact.get("consumer_feedback_by_matrix_group")
    if not isinstance(cfb, list):
        return
    slim: list[Any] = []
    for g in cfb:
        if not isinstance(g, dict):
            slim.append(g)
            continue
        slim.append(
            {
                k: v
                for k, v in g.items()
                if k not in ("focus_keyword_hits", "scenarios_top")
            }
        )
    compact["consumer_feedback_by_matrix_group"] = slim


STRATEGY_DATA_RULES = """**全局禁止编造（硬性）**：下列条款**同时**适用于 ① **独立策略稿**全文；② 若任务仍生成的**宿主报告内** ``####`` 策略归纳块（JSON 含 ``competitor_brief``）。**默认产线**下报告内第九章大模型长文已关闭，**独立策略稿不以该块为默认事实源**。
- **事实与数字**：销量、GMV、占比、价带、条数、份额、券面额、满减/满折门槛、到手价、店铺/品牌计数与排名、SKU 数、接口返回量等，**仅可**来自**本次调用输入 JSON** 已给出的字段。**独立策略稿**侧为：`structured_brief`、`rules_draft_markdown` 内摘录、**可选** `report_strategy_excerpt`（**默认多为空**，见 ``load_report_strategy_excerpt``）、**可选** `report_matrix_group_evidence_md`（与同任务报告第五～第八章细类归纳同源）、`strategy_decisions`、`business_notes`。**报告内嵌策略块**侧为 ``competitor_brief``、可选 ``prior_chapter_llm_narratives``。**禁止**凭空新增、改口径或写成「已监测证实」而无字段支撑。
- **主体与名称**：**禁止**引入上述输入中**未出现**的**具体**品牌名、店铺名、SKU 名、商品标题作为**事实陈述**；若 `strategy_decisions`/备注/brief/节选已含则可写；否则用「头部/同类竞品」等泛称或「待业务指定对标」。
- **用户侧表述**：**禁止**虚构评价原文、访谈引语、带引号的「用户说…」；细则见下文「§2 针对痛点要怎么做」表**痛点简述**列。
- **促销与活动**：**禁止**编造活动名、具体规则、补贴比例；细则见下文促销与第八章探针相关条款。
- **策略动作与落地结果**：可写「建议」「假设」「待验证」的动作方向，**不得**编造「已执行」「已上线」「数据显示转化率/复购提升」等**无输入依据**的结果。
- **信息不足**：须写「输入未体现」「待核对」「假设：」「待验证：」，**禁止**用确定语气掩盖缺失依据。
- **与 §2.1「类目/细类」列一致（全文）**：除 §2.1 表格外，**摘要、一、三～八**凡写策略动作、阶段重点、资源分配、差异化或竞争应对，**优先**标明适用**类目/细类**；多细类策略冲突时**分条**写。**禁止**用「全站用户」「整体上一句」覆盖与 §2.1 已分行决策**矛盾**的表述。

**与竞品分析报告的分工（硬性）**：
- 宿主报告已含样本量、价带分布、词频/共现、矩阵、第八章文本挖掘等**统计分析**。策略稿**不得**重复展开同类内容：不重写词频表、细类评论条数罗列、统计方法说明、与报告图表逐条复述。
- **允许**：用一两句**结论性**话概括用户侧/评论侧要点；必要时写「详见同任务《竞品分析报告》§× / 附录」。
- **必须**把篇幅放在**策略**：§2「针对痛点要怎么做」、后文战术与节奏（**勿**与报告重复统计展开）。

**数据与口径（硬性，与宿主分析报告同源输入）**：
- **§2「针对痛点要怎么做」表（反捏造 + 分类目，硬性）**：
  - **「类目/细类（本决策适用）」列**：须与 `structured_brief` 中类目混排、矩阵分组、§1.2 细类讨论或 `strategy_decisions` 已选战场**可对上**；**禁止**编造未出现的类目名。**多细类并存**（如饼干 vs 面包）时，**必须分行**分策，**禁止**用「全站用户」「整体策略」等**泛化**一句覆盖彼此冲突的动作。**若**类目或主推线尚不确定，该行可写「待业务定类」或「假设：优先××线」，并说明**分类决策依据或待补信息**；仍须避免与数据明显矛盾。
  - **「用户痛点（简述）」列**：**禁止**书写「用户反馈『……』」「评价称『……』」等**带引号的逐字原话**，除非该片段在 `structured_brief`、`strategy_hints`、`report_strategy_excerpt` 或 `business_notes` 中**已出现相同或明显包含**的文本；否则一律**不得**用引号假装引用。
  - 若输入仅有主题级信号（关注词、负向归因方向、价差行数等），痛点简述应写**可追溯归纳**，例如「与 brief 中 ×× 字段一致」「与报告第八章/节选已归纳的 ×× 主题一致」「监测摘要见 `strategy_hints` 第 n 条」，或写「**待原评论抽样核实**」——**禁止**把合理推测写成「用户已明确说……」的事实口吻。
  - **禁止**凭空发明痛点行（如「配料相似」「卖点雷同」「性价比一般」）作为**已监测结论**；此类表述仅当 `structured_brief`、节选或备注中**确有同类主题或措辞**时方可写入，否则不写或标为待验证假设。
- **不得编造**销量、GMV、未在 `structured_brief` 与底稿中出现的占比或价格；底稿与摘要中的数字须保持一致。
- **店铺集中度**仅可依据 `structured_brief.concentration` 与底稿，并区分**列表行**与**去重 SKU**；用「第一大……份额」「前三家合计」等中文，**不要用** CR1、CR3。
- **禁止编造**「京东自营 SKU 占比」「自营超 X%」等摘要中未给出的定量句。
- **矩阵**：若 `structured_brief` 含矩阵相关字段，须**呼应**细分类目与竞品矩阵结论，不得无故删光。
- **第八章文本挖掘探针（当 JSON 中 `chapter8_text_mining_probe` 为真时）**：
  - **禁止**将「关注词子串命中次数」「预设场景分组条数/占比」当作评论侧主论据。
  - 用户洞察、负向归因须与 **§8 文本挖掘** 及可选节选一致；促销与券价差须与 `price_promotion_signals`、报告**第六章**及 brief 已给字段一致（**默认**无宿主报告内长文策略节选时，**禁止**以「第九章已写」为凭据编造具体规则）；**禁止**编造满减门槛或补贴比例。
- **可选 `report_strategy_excerpt`**：**默认多为空**。非空时战略方向与该节选不明显矛盾；**不得**把节选与 `structured_brief` 均未出现的数字当作事实。**为空时**以 `structured_brief`、`report_matrix_group_evidence_md`（若有）与底稿/表单为准，**禁止**编造「宿主报告策略章已断言的」具体结论或虚假背书。"""

STRATEGY_SYSTEM = f"""你是市场策略顾问，根据**结构化监测摘要**与业务侧填写的**决策字段**，把「规则底稿」写成**短、可执行**的策略 Markdown **独立成稿**。

**输入**：`rules_draft_markdown`（规则骨架，**六主轴 + 品牌四线**结构，与 `docs/demo` 市场策略稿示例同构）、`structured_brief`、`strategy_decisions`、`business_notes`；**可选** `report_strategy_excerpt`（**默认多为空**，遗留或历史任务可能非空）；**可选** **`report_matrix_group_evidence_md`**（与所选细类对齐的宿主报告第五～八章归纳摘录——**主对齐源之一**）。

**与细类收窄及遗留节选（硬性）**：
- 当 JSON 含 `report_matrix_group_evidence_md` 且非空时：
  - **定性主题**（用户讨论焦点、卖点/配料叙事、负向体验类型、场景与关注词归纳方向等）须与该节选及 `structured_brief` **方向一致**，**禁止**另写一套与节选**明显矛盾**的品类判断。
  - **数字、份额、价带、条数**仍以 **`structured_brief` 为准**；节选与 brief 数字冲突时**采纳 brief**，勿复述冲突数字句。
- **`report_strategy_excerpt`**：**默认产线多为空**（报告内全任务大模型策略长文已弃用）。非空时仅作**弱参考**；写**收窄细类**策略时仍以 `structured_brief` + `report_matrix_group_evidence_md`（若有）为主，**不得**把全关键词池结论套成该细类已证实事实。**默认为空时禁止**写「报告第九章已归纳…」类虚假背书。

{STRATEGY_DATA_RULES}

**规划核心 §1.1（硬性）：策略要写「怎么做」，不能只写「是什么」**  
（与 `docs/planning/策略生成-框架确定.md` §1.1 一致；违反则成稿不合格。）
- **读者测试**：业务读者读完**摘要、一、三～八**任一大节后，应能回答至少一项：**谁（团队/渠道）在何触点、针对哪类用户或哪条痛点、采取什么动作、如何验收或待验证什么**。若某段只能回答「市场/品类/价带是什么样」而**没有**紧随或嵌入的「故本阶段须…」「优先…」类**动词句**，须改写或删并，**禁止**以形势描述段作为该节主体。
- **背景上限**：**一、顾客是谁** 的 1.1 与 1.2 **禁止**扩写成第二份分析报告：合计**至多约五句**结论性背景（谁搜、关心什么、分细类一句）；价带分位、样本量拆解、词频/方法一句带过或写「详见同任务《竞品分析报告》」，**禁止**多段连续铺陈数据。
- **摘要**：在「范围与样本」「用户侧」各**一句**可接受后，**阶段重点**必须是 **1～2 条完整执行句**，每条须含**可识别动作**（如统一商详第几屏表述、主图试点、规格命名、客服首句、跟价/不跟价说明等）之一，**禁止**单独使用「加强运营」「把握机会」「提升体验」「深化心智」等无主体、无触点、无痛点指向的套话。
- **§2.1 表**：监测已支撑**多个**痛点或细类维度时，**至少两行**有实质内容（非空、非整格「待填」）；**策略动作**与**具体怎么做**两列须以**动词短语或短句**开头，**禁止**两列长期只有形容词、名词标签或泛化口号。
- **§六～§八**：每一 numbered 小节（如 §6.2、§7.x、§8.x）须含**至少一条**可指回 §2.1 某一行的落地动作（可口头合并叙述）；**禁止**仅用「强化品牌/优化体验/夯实基础」等名词堆叠而无**谁做、在哪做、做哪一步**。
- **反例（禁止作为节内主要篇幅）**：「当前品类呈现…」「市场整体…」「用户日益注重健康」等**纯判断句串**而无后续「因此我方本阶段…」；若保留背景，**一句**后必须接执行句。

**落实范围**：上文「全局禁止编造」适用于**摘要、一至十、附录**的每一句话与表格每一格；**不得**因章节不同而放宽。

**对外成稿与禁止技术泄露（硬性）**：
- 正文须为**可直接对业务或合作方阅读**的正式策略文档（对外前仍须按需脱敏）。**禁止**出现：反引号代码体、JSON 键名、英文字段名、内部数据结构名、源码或仓库路径、类文件名、「任务 ID」「工作台」「规则骨架」等系统痕迹；**禁止**照抄底稿中以 *成稿：*、*回答：*、*占位*、*骨架* 开头的**元说明句**，须改写为正式业务表述。
- **禁止**在正文使用**写作指导式**套话（读者不应看到「作者须知」）：例如「须与 §2/§2.1 一致」「与 §2.1 类目列可对上」「为后文……埋伏笔」「回扣 §2」「承接 §2」「勿重复 §×」等——须直接写**实质策略内容**，勿解释章节之间如何对齐。
- **一级标题**用文书式，例如 `# 「{{keyword}}」市场策略建议书（草案）`，其中 `keyword` 取自本消息 JSON 的同名字段；**勿**使用「草稿」「底稿」「归纳用」等对内用词。
- **附录**中的采集范围等信息用**中文短句**（如「列表页约采集第 3～10 页」），**禁止** `page_start=` 等键值对或英文键名。

**业务决策未填写时的成稿义务（当 JSON 中 `strategy_decisions_substantive` 为 false 时）**：
- 视为未提交表单决策：须基于监测摘要、细类报告节选、底稿数据表及 `business_notes`（若有）**主动推断**一套连贯的**假设性**策略；在「策略范围与前提」写清推断前提（「假设：」「待业务确认：」），对阶段目标类型给出 **A～E 类选项及你从数据中归纳的推荐倾向**（不得只列选项而无立场）。
- **§2.1 针对痛点要怎么做**须含**多行实质内容**，覆盖监测已支撑的主要细类与痛点，**禁止**整表留白或满篇 *（待填）*。
- 仍须遵守全局禁止编造：数字、品牌、店铺、用户原话、活动规则仅可来自输入依据；无依据处用假设语气。

**决策边界（硬性）**：
- **当 `strategy_decisions_substantive` 为 true 时**：业务已在 `strategy_decisions` 中填写的项（角色、时间、成功标准、战场一句话、定位勾选、竞争倾向、四柱、目标客群/对标/资源备注、**营销策略**与**总体策略**等）视为**已定决策**：成稿须**落实为具体执行句**，**不得**改写成相反结论或再要求用户「请选择」。
- **当 `strategy_decisions_substantive` 为 true** 而部分表单项仍为空或占位：结合监测摘要与节选**补全为可执行表述**，与数据方向一致。
- **当 `strategy_decisions_substantive` 为 false 时**：适用上文「业务决策未填写时的成稿义务」，**禁止**以「请先填表」类表述搪塞全篇。
- **成稿阶段避免**：反复「请业务决策」；不确定时在 §2.1 用「类目/细类」+「假设：」「待业务确认：」**写清**，**禁止**只写泛化一句。

**输出结构与阅读顺序（须与 `rules_draft_markdown` 章节一致，勿另起目录）**：
**策略范围与前提（生成前先对齐）** → **摘要** → **一、顾客是谁**（含人群与路径、细类讨论、本品聚焦）→ **二、产品价值与用户痛点**（**仅 §2.1 针对痛点要怎么做** 表，**勿**再设独立「痛点表/价值对表/负向归因」子节）→ **三、为什么要买「这款产品」**（**仅 §3.1 购买者视角：为何要选这一款（依据与理由）**；**无 §3.2**，转化与价带应对已在 §2 表内则**勿重复**）→ **四、为什么要选「这个品牌」** → **五、与其它品牌有何不同** → **六、阶段目标与路径** → **七、品牌四线**（建设·打造·运营·体验）→ **八、战术支柱**（产品/定价/促销/渠道与传播）→ **九、风险、假设与待验证** → **十、下一步与节奏**（含业务备注）→ **附录**。
可微调小节标题用语，**不得**删减上述逻辑块或把「诊断数据」与「落地动作」顺序颠倒；**禁止**私自恢复已删除的小节（如 §3.2）。

**语气**：面向业务读者，避免 CR1、心智等内部缩写；**勿在成稿中反复强调「对齐某报告第几章」**，以策略表述为主。

**策略表述硬性（痛点 → 怎么做，须覆盖全书，不得只写 §2～§8 部分章节）**：
- **总原则**：成稿**不是**第二份分析报告，也**不是**市场形势说明书。每条重要内容应能回答：**针对哪条用户痛点、在哪条类目/细类下**（与 **§2.1** 表对应）、**我们采取什么动作**、**在具体触点怎么做**（商详/主图/短视频/客服/规格/价格呈现等）、**如何验证**（若适用）。**「是什么」仅作每节不超过一两句的铺垫；「怎么做」须占可策略论述篇幅的主体。**
- **§2.1 针对痛点要怎么做**（若底稿已有表头）须**填写实质内容**；全稿**动作总锚**为 §2.1。若无表，须在 **§二** 或 **§八** 用等价分条写清「痛点—动作—落地—验证」。

**分节要求（与底稿章节一一对应，勿省略）**：
- **策略范围与前提**：回答「**这份策略是针对什么做的**」（监测任务、本品角色、战场、主推类目、**本阶段目标类型**、时间、成功标准）。与业务表单及备注对齐；`strategy_decisions_substantive` 为 false 时须写清假设前提与推荐目标类型（可含 A～E 选项），为 true 时未填项不得与已填决策矛盾。**禁止**与后文 §2.1、§六 自相矛盾。
- **摘要**：除范围样本外，**阶段重点**须含 1～2 条**可执行动作**，指向优先痛点（非空泛「加强运营」）；须与上文「策略范围与前提」边界一致（**勿**在正文写「回扣 §2」「承接上文」等指导语）。
- **一、顾客是谁**：**禁止**重复报告中的细类词频、分品类样本量展开、文本挖掘方法；用 **少量结论句**（谁搜、关心什么、决策场景）；**1.3 本品聚焦**须写清本期主攻人群/场景/细类（**勿**写「与 §2.1 可对上」类作者提示）。
- **二**：**仅 §2.1** 一张表：「类目/细类（本决策适用）| 用户痛点（简述）| 策略动作 | 具体怎么做 | 如何验证」。须覆盖监测已支撑的主要维度（**按类目分行**，口感/质地分线、分量/规格、信任与价格等，依数据取舍）；**类目列 + 痛点简述列**遵守「§2 表」条款。**禁止**再写独立「痛点与证据表」「价值对表」「负向归因」子节（与 §二 重复的内容一律并入本表或删去）。
- **三**：**仅 §3.1**，标题与底稿一致为**购买者视角：为何要选这一款（依据与理由）**。全文须站在**购买者**一侧：写其在浏览/比价时**为何值得把这一款放进购物车**（解决什么具体问题、相对同类获得感、价位是否可接受、信任点是什么），可用「用户/消费者」作主语。**先**保留或转述输入中已有**检索/样本与价带**（作买家决策背景，勿大段铺陈），**随后**用 1～2 句落到**购买动机**。**禁止**用运营/品牌单方口吻替代买家逻辑（如「适合××叙事切入」「策略上占位」「品类时机好」作为收尾而不说买家得到什么）。**禁止**以只适用于整个品类的宏观句作为**唯一或最后**结论；宏观背景若写，**必须**收束到「因此**买家**更愿为这一款付费」的可验证点（规格/配料/口感/价位等须与输入可对读）。可结合 brief 写价带锚点一句。**禁止**写 §3.2「转化障碍与应对」；若与购买相关的障碍与应对已在 §2.1 表内，§3.1 **勿再复述**。
- **四**：品牌承诺与调性须能落到**可感知触点**（如商详第几屏、包装、客服首句），避免只有形容词。
- **五**：§5.2 差异化、§5.3 竞争应对须写清**相对竞品多做什么/少做什么、具体一步动作**。
- **六**：成功标准与 §6.2 路径须与 **§2.1** 动作**可对齐或合并叙述**；营销/总体策略句须为**动词导向**。
- **七**：品牌四线**每一条**至少一句：**服务哪类痛点、本周/本阶段具体做哪一步**。
- **八**：四支柱**每一支柱**须回扣 **痛点→动作→落地**（可与 §2.1 合并叙述，避免重复堆砌）。
- **九**：在表单风险勾选之外，**每条风险**尽量带**应对动作或验证计划**（抽样、核对规则），勿只列风险标题。
- **十**：下一步清单须为**可执行任务**（可含负责人/时间占位），与 §2.1 或 §六 优先级一致；可含「按类目核对主图/商详与 §2.1」类项。

**全书与 §2.1 类目列对齐（防泛化，与上条「全文一致」配套）**：
- **摘要**：阶段重点中的可执行动作**尽量**点明适用类目或主推线。
- **四、五**：品牌承诺、差异化、竞争应对若因细类而异，**分款/分类目**写。
- **六**：路径与成功标准若多类目并行，**分线**写 KPI 或写清主线/副线。
- **七**：品牌四线每条宜**可指回** §2.1 某类目行；若四线共用全池，须一句交代**共用前提**。
- **八**：四支柱下若产品/定价/促销策略因类目不同，**分子条**（如「饼干：…」「面包：…」），勿与 §2.1 矛盾。
- **九**：可写「主推类目未定」「多线话术不一致」等风险及验证方式。

**口感/质地与细类（禁止「一词盖全站」）**：
- 监测中「**酥脆**」与「**松软**」等可能**同时**高频，通常对应**不同细类**（如饼干 vs 面包/糕点）或不同场景。成稿须**按主推细类或分产品线**表述：饼干线策略与酥脆/饱腹等对齐，面包/糕点线与松软/早餐等对齐；若多线并存须**分款分句**，**禁止**只写「要做松软」而忽略酥脆主导的细类，除非 `structured_brief`、表单或业务备注已明确**仅**推该线。
- **产品策略句**须能指回：**本品是哪一类、解决哪条口感预期**，避免与数据里另一细类的主导词打架。

**促销：满减、满折、券（≠ 不管；≠ 编造）**：
- **必须**在 **§八.3 促销与活动策略**（及必要时 §七.3）写清：与 `price_promotion_signals`、报告第六章已归纳的**券、标价与到手价差、常见活动形态**如何承接（跟价节奏、规则透明、不与数据矛盾）；**禁止**因「没编出具体数字」就整节不写促销。
- **禁止编造**输入中未出现的**具体**满减门槛、满额折扣、每满减金额；若摘要/报告未捕获某类机制，须明确写「**监测未捕获具体满减/满折规则，上架前须与运营及后台活动对齐后再对外宣称**」，并可列**待补信息**（如：是否参加跨店满减、店铺券类型）。
- **区分**：「策略上跟券、保到手价透明」是成稿义务；「具体满 300 减 40」只能来自已有数据。

**输出**：仅 Markdown 正文（不要 ``` 围栏）；须收束各小节与全文，勿中途截断。"""

STRATEGY_USER_PREFIX = (
    "请基于以下 JSON 输出最终策略稿（Markdown），正文须为对外可读正式文档，不得泄露 JSON 键名、字段名、源码路径或底稿中的编写提示语。\n"
    "输出前自检：全文不得包含输入中未出现的具体数字、品牌/店铺名、用户引语与活动规则；不确定处须写「假设」「监测未体现」或「待业务核对」。\n"
    "输出前自检（规划 §1.1）：摘要「阶段重点」是否为 1～2 条含动作+触点（或时间窗口）的执行句；第一章是否未写成长篇市场白皮书；§2.1 是否至少两行实质且「动作/怎么做」列为动词句；§六～§八 每节是否至少一条可落地的「谁在哪做什么」。若否，先改再输出。\n"
    "若 JSON 中 `strategy_decisions_substantive` 为 false：你须基于监测摘要与细类报告节选**主动推断**完整策略草案（含 §2.1 多行实质内容），"
    "在「策略范围与前提」标明假设前提，并对阶段目标给出 A～E 类型选项及**推荐倾向**；禁止全文停留在待填占位。\n"
    "若 `strategy_decisions_substantive` 为 true：已填表单项视为已定须落实；空项结合数据补全，并与后文一致。\n\n"
)


def generate_strategy_draft_markdown_llm(
    *,
    job_id: int,
    keyword: str,
    brief: dict[str, Any],
    business_notes: str,
    generated_at_iso: str,
    strategy_decisions: dict[str, Any],
    report_strategy_excerpt: str | None = None,
    report_matrix_group_evidence_md: str | None = None,
    report_config: dict[str, Any] | None = None,
) -> str:
    """
    ``report_strategy_excerpt``：可选；由 ``load_report_strategy_excerpt`` 加载（见 ``reporting.report_strategy_excerpt``）。**默认产线**下多为空；非空多见于历史任务或曾显式开启报告内策略 LLM 的落盘。

    ``report_matrix_group_evidence_md``：按所选矩阵细类从 ``competitor_analysis.md`` 抽取的第五～第八章大模型小节摘录（见
    ``reporting.report_matrix_group_evidence.load_report_matrix_group_evidence_markdown``）；用于与收窄后的 ``structured_brief`` 一并支撑策略叙事。
    """
    rules_md = build_strategy_draft_markdown(
        job_id=job_id,
        keyword=keyword,
        brief=brief,
        business_notes=business_notes,
        generated_at_iso=generated_at_iso,
        strategy_decisions=strategy_decisions,
        report_config=report_config,
        for_llm_input=True,
    )
    excerpt_raw = (report_strategy_excerpt or "").strip()
    group_evidence_raw = (report_matrix_group_evidence_md or "").strip()
    sys_prompt = STRATEGY_SYSTEM
    min_comp = _min_strategy_completion_tokens()
    min_comp_relaxed = max(256, min_comp // 2)

    def _payload_and_user(
        *,
        compact_max: int,
        excerpt_max: int,
        rules_max: int | None,
    ) -> str:
        compact = compact_brief_for_llm(brief, max_chars=compact_max)
        if report_uses_chapter8_text_mining_probe(report_config):
            compact = dict(compact)
            _omit_ch8_probe_wordchart_fields(compact)
        ex = (
            _truncate_strategy_narrative(excerpt_raw, excerpt_max)
            if excerpt_raw
            else ""
        )
        ev_max = min(24_000, max(3_000, excerpt_max + excerpt_max // 2))
        gm = (
            _truncate_strategy_narrative(group_evidence_raw, ev_max)
            if group_evidence_raw
            else ""
        )
        if rules_max is None:
            rd = rules_md
        else:
            rd = _truncate_rules_draft_md(rules_md, rules_max)
        payload: dict[str, Any] = {
            "job_id": job_id,
            "keyword": keyword,
            "generated_at_iso": generated_at_iso,
            "strategy_decisions": strategy_decisions,
            "strategy_decisions_substantive": strategy_decisions_substantive(
                strategy_decisions
            ),
            "business_notes": business_notes,
            "structured_brief": compact,
            "rules_draft_markdown": rd,
            "report_strategy_excerpt": ex,
            "report_matrix_group_evidence_md": gm,
            "chapter8_text_mining_probe": bool(
                report_uses_chapter8_text_mining_probe(report_config)
            ),
        }
        if report_uses_chapter8_text_mining_probe(report_config):
            payload["structured_brief_omission_note"] = (
                "已启用第八章文本挖掘（探针为主）：structured_brief 已省略「关注词/场景子串计数」、按细类 feedback 中的 focus_keyword_hits/scenarios_top、"
                "``strategy_hints`` 等；报告已**不再**输出 ``comment_sentiment_lexicon``（星级子集预设口语短语）及同口径图。**不得**再以这类子串计数、短语条形图或预设场景占比作为论据。"
                "用户与评论侧须依报告 §8 文本挖掘归纳及 `report_matrix_group_evidence_md`；**促销、满减、券价差**须与报告第六章、`price_promotion_signals` 及 brief 已给字段一致；若 `report_strategy_excerpt` 非空则勿与其明显矛盾。**默认**节选为空，勿编造「报告策略长文已写明的」具体活动规则。"
            )
        raw = json.dumps(payload, ensure_ascii=False)
        if len(raw) > 500_000:
            payload["rules_draft_markdown"] = _truncate_rules_draft_md(rd, 200_000)
            raw = json.dumps(payload, ensure_ascii=False)
        return STRATEGY_USER_PREFIX + raw

    for cap_brief, cap_excerpt, cap_rules in (
        (80_000, 24_000, None),
        (64_000, 20_000, None),
        (48_000, 17_000, None),
        (36_000, 14_000, None),
        (28_000, 11_000, None),
        (22_000, 9_000, None),
        (18_000, 7_000, None),
        (14_000, 5_000, None),
        (12_000, 4_000, 220_000),
        (10_000, 3_500, 180_000),
        (10_000, 3_000, 150_000),
        (9_000, 2_500, 120_000),
        (8_000, 2_000, 100_000),
        (8_000, 2_000, 70_000),
    ):
        user = _payload_and_user(
            compact_max=cap_brief,
            excerpt_max=cap_excerpt,
            rules_max=cap_rules,
        )
        if _strategy_prompt_ok_for_call(
            sys_prompt, user, min_completion_tokens=min_comp
        ):
            return call_llm(sys_prompt, user)

    for cap_brief, cap_excerpt, cap_rules in (
        (10_000, 2_000, 55_000),
        (8_000, 1_500, 45_000),
        (7_000, 1_200, 35_000),
    ):
        user = _payload_and_user(
            compact_max=cap_brief,
            excerpt_max=cap_excerpt,
            rules_max=cap_rules,
        )
        if _strategy_prompt_ok_for_call(
            sys_prompt, user, min_completion_tokens=min_comp_relaxed
        ):
            return call_llm(sys_prompt, user)

    user = _payload_and_user(compact_max=6_000, excerpt_max=1_000, rules_max=28_000)
    return call_llm(sys_prompt, user)


STRATEGY_OPPORTUNITIES_SYSTEM = (
    STRATEGY_DATA_RULES
    + """

你是 B 端市场与增长顾问。输入 JSON 含 ``keyword``、``competitor_brief``（与本任务规则报告同源的结构化摘要，可能经裁剪，并含 ``matrix_overview_for_llm``），以及可选 ``prior_chapter_llm_narratives``（本报告 **第五至第八章** 已生成的大模型归纳节选，与正文**同源**）。

请输出 **Markdown 正文**（不要用 ``` 围栏包裹），将**直接嵌入**宿主文档中**已存在章节标题之下**的小节，读者已知当前处于「策略与机会」相关章节。

**（与独立下载策略稿的关系）**：独立策略稿使用「摘要→一～十→附录」的六主轴结构；本节**不是**完整策略稿，仅输出下列 ``####`` 主题块，避免与宿主「九、策略与机会提示」等**已存在标题**字面重复。

**全局禁止编造**见上文 `STRATEGY_DATA_RULES` 段首；**本节每个 ``####`` 块**均须遵守（含不得虚构用户引语、未在 ``competitor_brief`` 出现的品牌名与数字）。

**与前文分析严格对齐（硬性，优先于自由发挥）**：
- **定性主题**（各细类讨论焦点、正负向体验、场景与关注词归纳、配料/卖点叙事、促销形态描述等）须与 ``prior_chapter_llm_narratives`` 中已出现的表述**方向一致**，**禁止**另写一套与节选**明显矛盾**的品类判断、品牌举例或用户痛点主题。
- **定量与可核验事实**（价带分位数、店铺/品牌占比、条数、评论统计字段等）**以** ``competitor_brief`` **为准**；若节选与 brief 数字冲突，**采纳 brief**，且勿复述与数字冲突的节选句。
- 若某键未出现在 ``prior_chapter_llm_narratives`` 或内容为空，则该维度**不得**编造与可能存在的报告其他章冲突的细节；仅依据 ``competitor_brief`` 或明确写「输入中未体现」。
- **转化与体验**小节：正负向体验线索须**优先呼应** **第八章第二节 侧**节选（``sec8_3_comment_focus_summaries`` 或 ``sec8_3_text_mining_probe``，视何者存在；内部键名仍沿用 ``sec8_3_*``）；**禁止**将节选未提及的具体抱怨/品类问题写成**主要结论**；可写「假设：待结合业务验证」。

**标题与措辞（硬性）**：
- **禁止**在正文开头或任何位置重复宿主已有章名/小节名，包括但不限于：「第9章」「九、」「策略与机会提示」「策略与机会建议」「策略与机会」等（勿与报告固定章节标题撞车）；**不要**自造 ``##`` 一级标题；
- 小节标题**仅允许**使用业务主题式 ``####``（如下所列），从第一句起就进入实质内容。

**必须遵守**：
- **数字与事实**：价格分位数、集中度份额、条数、占比等**只能**来自 ``competitor_brief`` 中已有字段；**禁止编造**未出现的品牌销量、具体 GMV、未给出的到手价；
- **店铺类型占比（硬性）**：**禁止**编造「京东自营 SKU 占比」「自营款数占比超 X%」等表述，除非 ``competitor_brief`` 中 ``concentration.shops_from_list`` / ``list_shop_mix_top`` 等字段**已出现**对应店铺名与计数；若写第一大店铺份额，须与 ``shops_from_list`` 一致，并区分 **列表行** 与 **去重 SKU**（``unique_sku_basis``），**禁止**写成全渠道市占或模糊「SKU 占比」。
- **语气**：分节给出**可操作的假设性建议**（定价区间思路、应对齐的差异化观测点、应规避的风险、促销与机制设计线索、转化与详情页/评价侧改进方向），每条建议用「假设：」「待验证：」等标明不确定性；
- **结构**：至少使用 ``####`` 组织以下主题（可合并子条，但须覆盖）：**定价与价带**、**差异化与应对齐的优势**、**风险与避免项**、**促销与活动机制**、**转化与体验**；
- **促销与活动机制（硬性）**：该节**必须优先依据** ``competitor_brief.price_promotion_signals``（券后/标价、价差等，若存在），并与 ``prior_chapter_llm_narratives.sec6_promo_group_summaries``（若有）**不矛盾**，给出**假设性**机制建议。**禁止**编造具体满减门槛、红包面额、补贴比例；**禁止**在输入中完全未出现任何列表侧价差或促销归纳信号时，仍写一大段具体「要做满减发红包」而无「输入中未捕获此类信号」的说明。
- **转化与体验（硬性）**：须**同时**写清正向与负向；**禁止**使用「占比均超过 130 次」等**语义不通或混用次数/占比**的表述；数字表述须与 ``competitor_brief`` 一致。
- **禁止**：不要写完整报告目录；不要复述「研究范围与方法」；不要使用 CR1/CR3 缩写（用「第一大……份额」「前三家合计」）；不要输出与输入矛盾的价带描述。

篇幅约 **900～3200 字**（数据丰富可偏长）。"""
)


STRATEGY_OPPORTUNITIES_USER_PREFIX = (
    "请根据以下 JSON 撰写策略归纳正文（Markdown）。"
    "``competitor_brief`` 为结构化摘要；若含 ``prior_chapter_llm_narratives``，则为 第五至第八章 大模型归纳节选，须与策略正文对齐。"
    "宿主报告已含「策略与机会」相关章节标题，**勿在输出中重复「九、」「策略与机会」类章名或小节名**。"
    "输出前自检：不得编造 brief 与节选未出现的数字、品牌/店铺名、用户引语与活动规则；不确定须用「假设：」「待验证：」「输入未体现」。\n\n"
)


def _truncate_rules_draft_md(text: str, max_chars: int) -> str:
    """规则策略底稿过长时截断，避免 JSON 与 completion 预算挤占输出。"""
    s = (text or "").strip()
    if not s:
        return ""
    if len(s) <= max_chars:
        return s
    return (
        s[: max_chars - 80].rstrip()
        + "\n\n…（规则底稿已截断，请勿编造截断后内容。）\n"
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
