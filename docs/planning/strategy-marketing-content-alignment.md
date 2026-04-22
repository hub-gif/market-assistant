# 规划留痕：市场策略稿 · 营销内容 · 与竞品报告对齐

**成稿日期**：2026-04-17  
**目的**：划分**报告 / 策略 / 营销**三层职责：报告与简报管监测与论证；**可执行策略**以**独立策略稿（策略制定）**交付，与同任务 `brief`、报告第五～八章矩阵/评论侧归纳等**同源、不编造**；**营销内容只把已定稿的策略叙事翻成可对外触点文案**，不在营销管线里二次灌入报告原文。  
**关联代码（现状）**：

| 能力 | 位置 |
|------|------|
| 规则策略底稿 | `pipeline/reporting/strategy_draft.py` → `build_strategy_draft_markdown` |
| 策略稿 LLM 润色 | `pipeline/llm/generate_strategy.py` → `generate_strategy_draft_markdown_llm`（payload 可选含 `report_strategy_excerpt`，**默认多为空**） |
| 报告「九、策略与机会提示」节选加载（遗留/兼容） | `pipeline/reporting/report_strategy_excerpt.py` → `load_report_strategy_excerpt` |
| 报告内第九章大模型长文（**默认关闭**） | `generate_strategy_opportunities_llm`；runner 默认 `llm_strategy_opportunities: false`，正文第九章为固定读者说明（见 `jd_report._strategy_opportunities_reader_fixed_lines`） |
| 策略稿 API / 导出 | `pipeline/views/job_report_views.py` → `JobStrategyDraftView` |
| 营销内容生成（核心信息卡 + 多触点文案） | `pipeline/llm/generate_marketing_detail.py`、`JobMarketingDetailPackView` |
| 简报与压缩 | `pipeline/reporting/brief_compact.py`（**策略/报告链路**；非营销管线默认输入） |
| Markdown→Word/PDF | `pipeline/reporting/md_document_export.py` |

---

## 1. 原则（事实源与禁止项）

| 维度 | 要求 |
|------|------|
| **事实源** | 仅允许来自：同任务 `build_competitor_brief` 产物、已落盘的 `competitor_analysis.md` 中与策略论证相关的既定小节、各章 LLM 节选 JSON、`strategy_hints`、按细类收窄时的 **`report_matrix_group_evidence_md`**（与报告第五～八章同源）。**不再**把「报告第九章大模型长文」当作默认事实源；历史任务若存在 `strategy_opportunities_llm.json.markdown` 可按加载规则视为可选补充。 |
| **策略稿** | 以规则底稿为骨架；**先**通过「策略范围与前提」与规划文档「启动前」对齐**针对什么做策略**；LLM 仅做可读性润色与衔接，**不得新增** brief/底稿中不存在的数字、品牌、销量、价格。全文还须遵守 `STRATEGY_DATA_RULES` 段首「**全局禁止编造**」（用户引语、活动规则、无依据的落地结果等）。 |
| **报告内「第九章」** | **默认产线**：竞品报告在「九、策略与机会提示」下**不**再附加全任务大模型策略长文，仅为短引导（指向「策略制定」按细类生成）。若调试或历史配置显式开启 `llm_strategy_opportunities` 并落盘正文，仍须遵守 `STRATEGY_OPPORTUNITIES_SYSTEM`（`generate_strategy.py`）中的硬性条款。 |
| **营销内容** | **仅表达层**：输入为**已定稿策略稿 Markdown + 表单决策 + 业务备注**（及后续可选受众/渠道等），**不新增事实**。**不默认**并入 `compact_brief`、报告第八/九章节选或其它报告正文。与监测数据的一致性由**策略稿所消费的 brief / 矩阵节选 / 表单**先收束；若营销稿与业务认知不符，应修订策略输入，而非在营销接口再拼报告正文。 |

---

## 2. 策略稿：与宿主报告及数据同源（阶段 S1，**已修订口径**）

**目标**：独立下载的策略稿与同任务**简报与报告中的监测结论**一致（计数、价带、矩阵细类、第五～八章归纳等），**不编造**；**不再**以「必须与报告内第九章大模型段落方向一致」作为默认验收口径。

**实现要点（与代码一致）**：

1. **`load_report_strategy_excerpt(run_dir)`**（兼容字段）：若 `strategy_opportunities_llm.json` 含非空 **`markdown`**（通常仅历史任务或显式开启 LLM 第九章时），则载入；若仅有空壳 JSON，**不再**回退截取 `competitor_analysis.md`，避免把第九章固定读者说明误当策略正文。否则再尝试从 `competitor_analysis.md` 截取 `## 九、策略与机会提示` 至 `## 附录` 之前。  
2. **`STRATEGY_SYSTEM`**：当 `report_strategy_excerpt` **非空**时，润色稿须与该节选**不明显矛盾**；若与 `business_notes` / `strategy_decisions` 冲突，须在成稿中交代依据（如业务备注优先）。**默认**节选为空：成稿以 `structured_brief` + `report_matrix_group_evidence_md` + 底稿与表单为准，**不得**编造「报告第九章已写明的」具体结论。  
3. **API**：`POST /api/jobs/{id}/strategy-draft/` 仍返回 `report_strategy_excerpt_source`、`report_strategy_excerpt_chars`（`generator=rules` 时亦返回），便于核对当前任务是否仍存在遗留节选。  
4. 产品侧：`generator: rules | llm` 仍为既有行为；规则版作审计底稿。

**验收**：抽样任务核对策略稿数字与 brief、矩阵节选可对读；**不再**要求「第九章要点 ↔ 策略稿 bullet」一一对应（默认无第九章长文）。

---

## 3. 营销内容：仅生成营销稿（阶段 S2～S3）

**边界（产品定论）**

- **只做一件事**：把**策略已定稿的叙事**转成**可上架/可多触点使用的文案**（核心信息卡、商详与列表侧、主图要点、短视频钩句、客服首句、依据与边界等）。  
- **输入**：`strategy_markdown`、`strategy_decisions`、`business_notes`（与现网 `JobMarketingDetailPackView` 一致）；后续可增**可选** `audience_segment`、`channels` 等，**仍不得**作为新事实来源。  
- **刻意不做**：在营销请求里**默认拼接** `compact_brief`、报告 `competitor_analysis.md` 第八/九章节选或其它报告正文——避免「半篇报告 + 半篇卖点」的混杂产出；策略稿已承载与数据同源的叙事时，营销层信任该输入。

**提示词硬性约束（与实现对齐）**

- 事实、数字、功效、引语**仅可**来自策略稿与表单/备注中已出现内容；食品/健康等合规禁区同策略侧原则。  
- 输出中保留 **依据与边界** 类字段，提醒对外宣称限度（**相对策略承诺**，非相对整份 PDF 报告再摘一层）。

**输出与留痕**

- 现网：`run_dir/marketing/marketing_detail_pack_v1.json`；可下载 / Word / PDF 由前端与 `export-document` 支持。第二步 JSON 在**不编造**前提下偏**丰富**：更多标题/卖点/FAQ、详情中段叙事、食用搭配、短图文变体、直播要点提纲，以及 **文生图/文生视频** 可复制提示词（见 `generate_marketing_detail.py`）。

**与旧稿差异说明**

- 本文件早期版本曾设想营销与 brief/报告节选**强绑定**；经产品收敛，**以本节边界为准**，不再将「必选 report_excerpts」作为默认架构。

---

## 4. 实施顺序

| 阶段 | 内容 | 产出 |
|------|------|------|
| **S1** | 策略稿 payload 含可选 `report_strategy_excerpt`；与宿主数据同源以 brief + 矩阵节选为主；第九章长文默认弃用 | ✅ 已合并：`report_strategy_excerpt.py`（含空壳 json 不回退）、API 响应字段；默认节选为空 |
| **S2** | 营销模块 v1：策略驱动两步 LLM + 落盘 + Word/PDF；**不**默认并入报告节选 | ✅ 方向与现网 `generate_marketing_detail` 一致，细节以代码为准 |
| **S3** | 前端：策略预览入口、可选受众/渠道、载入上次生成等 | 产品闭环（按需排期） |
| **S4**（可选） | 轻量校验：输出中数字与 brief 同源性启发式检查 | 降低明显幻觉 |

---

## 5. 风险与边界

- LLM 无法 100% 杜绝编造，**规则底稿 + brief + 人工抽检**仍为默认。  
- 营销内容需保留「不替代合规/法务审核」类免责声明（可与报告附录表述一致）。

---

## 6. 修订记录

| 日期 | 说明 |
|------|------|
| 2026-04-17 | 首版：对齐原则、S1～S4、代码锚点、API 示意。 |
| 2026-04-18 | S1 落地：`load_report_strategy_excerpt`、`STRATEGY_SYSTEM` 对齐条款、`strategy_opportunities_llm.json.markdown`、策略稿 API 响应字段。 |
| 2026-04-22 | 收敛营销边界（§3）；**修订 S1**：默认产线弃用报告内第九章大模型长文，策略稿与数据对齐以 **brief + 第五～八章节选** 为主，`report_strategy_excerpt` 为遗留/空默认；更新 §1、§2、§4、能力表。 |

后续变更请在本表追加一行，并在正文相应章节修改。
