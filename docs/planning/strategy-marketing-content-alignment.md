# 规划留痕：市场策略稿 · 营销内容 · 与竞品报告对齐

**成稿日期**：2026-04-17  
**目的**：将「策略稿与报告第九章同源、不编造」与「营销内容面向受众痛点与兴趣、仍对齐报告、不编造」落成可执行路线，供产品/研发后续查阅与迭代。  
**关联代码（现状）**：

| 能力 | 位置 |
|------|------|
| 规则策略底稿 | `pipeline/reporting/strategy_draft.py` → `build_strategy_draft_markdown` |
| 策略稿 LLM 润色 | `pipeline/llm/generate_strategy.py` → `generate_strategy_draft_markdown_llm`（payload 含 `report_strategy_excerpt`） |
| 第九章节选加载（S1） | `pipeline/reporting/report_strategy_excerpt.py` → `load_report_strategy_excerpt` |
| 报告第九章策略归纳 | `generate_strategy_opportunities_llm`（与 `competitor_brief` + 各章节选对齐） |
| 策略稿 API / 导出 | `pipeline/views/job_report_views.py` → `JobStrategyDraftView` |
| 简报与压缩 | `pipeline/reporting/brief_compact.py` |
| Markdown→Word/PDF | `pipeline/reporting/md_document_export.py` |

---

## 1. 原则（事实源与禁止项）

| 维度 | 要求 |
|------|------|
| **事实源** | 仅允许来自：同任务 `build_competitor_brief` 产物、已落盘的 `competitor_analysis.md` 中与策略相关的既定小节、各章 LLM 节选 JSON、`strategy_hints`、第九章策略相关落盘（如 `strategy_opportunities_llm.json`）。 |
| **策略稿** | 以规则底稿为骨架；LLM 仅做可读性润色与衔接，**不得新增** brief/底稿中不存在的数字、品牌、销量、价格。 |
| **报告第九章** | 继续遵守 `STRATEGY_OPPORTUNITIES_SYSTEM`（`generate_strategy.py`）中的硬性条款：brief 优先、与 `prior_chapter_llm_narratives` 不矛盾、假设性表述等。 |
| **营销内容** | 视为**表达层**：不新增事实；把同一批证据改写成面向某类受众的叙事；可核验结论须能指回 brief 或报告节选。 |

---

## 2. 策略稿：与报告内「策略建议」对齐（阶段 S1）— **已落地**

**目标**：独立下载的策略稿与宿主报告第九章**方向一致**，避免与已发布报告矛盾。

**实现要点（与代码一致）**：

1. **`load_report_strategy_excerpt(run_dir)`**：优先读 `strategy_opportunities_llm.json` 的 **`markdown`**（runner 在第九章大模型成功时写入）；否则从 **`competitor_analysis.md`** 截取 `## 九、策略与机会提示` 至 `## 附录` 之前。  
2. **`STRATEGY_SYSTEM`**：当 `report_strategy_excerpt` 非空时，润色稿须与节选**战略方向一致**；若 `business_notes` / `strategy_decisions` 与节选冲突，须标注「与报告第九章策略归纳不一致之处见业务备注」类表述。节选为空时不得编造第九章结论。  
3. **API**：`POST /api/jobs/{id}/strategy-draft/` 响应增加 `report_strategy_excerpt_source`、`report_strategy_excerpt_chars`（`generator=rules` 时亦返回，便于核对是否加载到节选）。  
4. 产品侧：`generator: rules | llm` 仍为既有行为；规则版作审计底稿。

**验收**：抽样任务核对「第九章要点 ↔ 策略稿 bullet」可对应；数字仅来自 brief/底稿。

---

## 3. 营销内容：受众痛点与兴趣，仍对齐报告（阶段 S2～S3）

**定位**：新建管线（建议 `pipeline/reporting/marketing_content.py` + `pipeline/llm/generate_marketing.py`），与策略、报告**共用同一 brief**，并强制附带「依据」块。

**输入（约束）**

- `compact_brief_for_llm(brief)`（或可溯源子集）；  
- **必选** `report_excerpts`：至少含 **第九章策略相关段落** + **第八章评论/文本挖掘摘要中的一小段**（从已生成 md 或 JSON 截取，设长度上限）；  
- **必选** `audience_profile`：如 `{ "segment": "...", "pain_points_hint": "可选业务补充" }` —— 仅作叙述角度，**不作为新事实来源**；  
- **可选** `channels`：如朋友圈短文案、详情页卖点结构、投放标题等，控制输出形态。

**提示词硬性约束**

- 禁止出现 brief / `report_excerpts` 未出现的品牌名、价格、销量、功效承诺；  
- 痛点与兴趣须表述为「基于监测摘要已出现的主题/评价方向」的转述；  
- 建议结构：**依据摘要**（可溯源） + **受众表达** + **不可用/待核实**（输入未体现的，列明不得对外宣称）。

**输出与留痕**

- 建议落盘 `run_dir/marketing/marketing_pack.md`（或等价路径）；  
- 任务侧记录 artifact（文件名、时间、输入摘要 hash），便于追溯。

**API 示意**

- `POST /api/jobs/{id}/generate-marketing/`  
- Body：`audience_segment`、`channels[]`、`business_notes`（可选）。

---

## 4. 实施顺序

| 阶段 | 内容 | 产出 |
|------|------|------|
| **S1** | 策略稿 payload 增加 `report_strategy_excerpt`，收紧 `STRATEGY_SYSTEM` | ✅ 已合并：`report_strategy_excerpt.py`、runner 落盘 `markdown`、OpenAPI 响应字段 |
| **S2** | 营销模块 v1：单模板 + 强约束提示词 + 落盘 + 复用 Word/PDF 导出 | 可演示的营销初稿 |
| **S3** | 前端：任务页入口、受众与渠道、生成历史 | 产品闭环 |
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

后续变更请在本表追加一行，并在正文相应章节修改。
