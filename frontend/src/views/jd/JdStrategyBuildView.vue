<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { refreshJobs, useJobs, api } from '../../composables/useJobs'
import {
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'

const route = useRoute()
const router = useRouter()
const { jobs } = useJobs()

const selectedId = ref('')
const businessNotes = ref('')
const err = ref('')
const genInFlight = generationInFlightKey()
const STRATEGY_PREFIX = 'strategy-draft:'
const strategyDraftPendingJobId = computed(() => {
  for (const k of genInFlight.value) {
    if (k.startsWith(STRATEGY_PREFIX)) return k.slice(STRATEGY_PREFIX.length)
  }
  return null
})
const strategyGeneratingAny = computed(() => strategyDraftPendingJobId.value != null)
const strategyGeneratingThisTask = computed(
  () =>
    strategyDraftPendingJobId.value != null &&
    strategyDraftPendingJobId.value === selectedId.value,
)
const strategyGeneratingOtherTask = computed(
  () =>
    strategyDraftPendingJobId.value != null &&
    strategyDraftPendingJobId.value !== selectedId.value,
)
/** 勾选则本次仅规则稿（不调用大模型）；默认不勾选即走大模型 */
const rulesOnlyThisRun = ref(false)

/** 与竞品矩阵细类一致；空字符串表示不收窄（全关键词样本） */
const strategyMatrixScope = ref('')
const matrixGroups = ref([])
const briefMatrixLoading = ref(false)
const briefMatrixErr = ref('')

const decisions = reactive({
  product_role: '',
  time_horizon: '',
  success_criteria: '',
  non_goals: '',
  battlefield_one_line: '',
  positioning_choice: '',
  competitive_stance: '',
  pillar_product: '',
  pillar_price: '',
  pillar_channel: '',
  pillar_comm: '',
  audience_segment: '',
  competitor_reference: '',
  resource_notes: '',
  marketing_strategy: '',
  general_strategy: '',
  ack_risk_keywords: false,
  ack_risk_price: false,
  ack_risk_concentration: false,
})

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const positioningOptions = [
  { value: '', label: '暂不勾选（文稿中均为空选）' },
  { value: 'top', label: '贴顶' },
  { value: 'mid', label: '卡腰' },
  { value: 'entry', label: '下探' },
  { value: 'different', label: '另起带' },
]

/** 对应后端 competitive_stance / 成稿 §5.3：与头部或主竞品「怎么打」，非价位阵地、亦非泛指的「进市场」。 */
const stanceOptions = [
  { value: '', label: '暂不填写' },
  { value: 'flank', label: '侧翼切入（避开头部主战场）' },
  { value: 'head_on', label: '正面替代（对标头部主战场）' },
  { value: 'both', label: '分层推进（侧翼 + 正面并行）' },
  { value: 'undecided', label: '尚未拍板' },
]

function buildPayload() {
  const generator = rulesOnlyThisRun.value ? 'rules' : 'llm'
  return {
    generator,
    business_notes: businessNotes.value,
    product_role: decisions.product_role,
    time_horizon: decisions.time_horizon,
    success_criteria: decisions.success_criteria,
    non_goals: decisions.non_goals,
    battlefield_one_line: decisions.battlefield_one_line,
    positioning_choice: decisions.positioning_choice,
    competitive_stance: decisions.competitive_stance,
    pillar_product: decisions.pillar_product,
    pillar_price: decisions.pillar_price,
    pillar_channel: decisions.pillar_channel,
    pillar_comm: decisions.pillar_comm,
    audience_segment: decisions.audience_segment,
    competitor_reference: decisions.competitor_reference,
    resource_notes: decisions.resource_notes,
    marketing_strategy: decisions.marketing_strategy,
    general_strategy: decisions.general_strategy,
    ack_risk_keywords: decisions.ack_risk_keywords,
    ack_risk_price: decisions.ack_risk_price,
    ack_risk_concentration: decisions.ack_risk_concentration,
    ...(strategyMatrixScope.value
      ? { strategy_matrix_group: strategyMatrixScope.value }
      : {}),
  }
}

const STORAGE_KEY = (id) => `ma_strategy_draft_${id}`

function formatJobOption(j) {
  const t = j.created_at
  const tail = t ? String(t).replace('T', ' ').slice(0, 16) : ''
  return tail ? `#${j.id} · ${j.keyword} · ${tail}` : `#${j.id} · ${j.keyword}`
}

async function loadList() {
  try {
    await refreshJobs()
  } catch {
    /* ignore */
  }
}

async function loadMatrixGroupsForJob(id) {
  matrixGroups.value = []
  strategyMatrixScope.value = ''
  briefMatrixErr.value = ''
  if (!id) return
  briefMatrixLoading.value = true
  try {
    const r = await api(`/api/jobs/${id}/competitor-brief/`)
    const text = await r.text()
    if (!r.ok) {
      try {
        briefMatrixErr.value = JSON.parse(text).detail || text
      } catch {
        briefMatrixErr.value = text || `HTTP ${r.status}`
      }
      return
    }
    const data = JSON.parse(text)
    const mg = data.matrix_groups
    matrixGroups.value = Array.isArray(mg) ? mg : []
    const saved = sessionStorage.getItem(`ma_strategy_scope_${id}`)
    if (saved && matrixGroups.value.some((g) => g.group === saved)) {
      strategyMatrixScope.value = saved
    }
  } catch (e) {
    briefMatrixErr.value = String(e)
  } finally {
    briefMatrixLoading.value = false
  }
}

async function generateAndGoPreview() {
  const id = selectedId.value
  if (!id) return
  err.value = ''
  const key = `${STRATEGY_PREFIX}${id}`
  await withGenerationInFlight(key, async () => {
    try {
      const r = await api(`/api/jobs/${id}/strategy-draft/`, {
        method: 'POST',
        body: JSON.stringify(buildPayload()),
      })
      const text = await r.text()
      if (!r.ok) {
        try {
          const j = JSON.parse(text)
          err.value = j.detail || text
        } catch {
          err.value = text || `HTTP ${r.status}`
        }
        return
      }
      const j = JSON.parse(text)
      sessionStorage.setItem(
        STORAGE_KEY(id),
        JSON.stringify({
          markdown: j.markdown || '',
          keyword: j.keyword || '',
          generated_at: j.generated_at || '',
        }),
      )
      router.push({ path: '/jd/strategy-view', query: { job: id } })
    } catch (e) {
      err.value = String(e)
    }
  })
}

onMounted(loadList)

watch(selectedId, (id) => {
  loadMatrixGroupsForJob(id)
})

watch(strategyMatrixScope, (v) => {
  const jid = selectedId.value
  if (!jid) return
  if (v) sessionStorage.setItem(`ma_strategy_scope_${jid}`, v)
  else sessionStorage.removeItem(`ma_strategy_scope_${jid}`)
})

watch(
  () => route.query.job,
  (j) => {
    if (j) selectedId.value = String(j)
  },
  { immediate: true },
)

watch(
  successJobs,
  (list) => {
    if (selectedId.value) return
    if (route.query.job) return
    if (list.length) selectedId.value = String(list[0].id)
  },
  { immediate: true },
)
</script>

<template>
  <div>
    <section class="ma-card">
      <h2>策略生成</h2>
      <p class="hint-top">
        选择<strong>已成功</strong>任务，先选顶部<strong>矩阵细类</strong>（对应 §0 主推类目）。下方表单区块<strong>按成稿章节顺序</strong>排列（§0→§1.3→§5→§6→§7～§8→§9→§10）；§2～§4
        无单独表单项，由监测与模型写入 §2.1 / §3.1 / §4.1。生成结果见
        <RouterLink to="/jd/strategy-view">策略稿预览</RouterLink>。<strong>已填项</strong>进底稿并由大模型落实；<strong>未填项</strong>可由模型结合数据推断。
      </p>


      <div class="toolbar">
        <label class="chk-inline">
          <input v-model="rulesOnlyThisRun" type="checkbox" />
          本次仅生成规则稿（不做大模型全文润色，更快、不调用智能服务）
        </label>
      </div>
      <div class="toolbar">
        <label class="sel-label">任务</label>
        <select v-model="selectedId" class="job-select">
          <option value="" disabled>请选择任务</option>
          <option v-for="j in successJobs" :key="j.id" :value="String(j.id)">
            {{ formatJobOption(j) }}
          </option>
        </select>
        <button
          type="button"
          class="ma-btn ma-btn-primary"
          :disabled="!selectedId || strategyGeneratingAny || briefMatrixLoading"
          @click="generateAndGoPreview"
        >
          {{ strategyGeneratingThisTask ? '生成中…' : '生成并前往预览' }}
        </button>
      </div>
      <div v-if="selectedId" class="toolbar toolbar-stack">
        <label class="sel-label">主推类目（矩阵细类）</label>
        <select
          v-model="strategyMatrixScope"
          class="job-select"
          :disabled="briefMatrixLoading || strategyGeneratingAny"
        >
          <option value="">全部分类（不收窄 · 与全关键词监测样本一致）</option>
          <option v-for="g in matrixGroups" :key="g.index" :value="g.group">
            {{ g.group }}（{{ g.sku_count }} 款）
          </option>
        </select>
        <span v-if="briefMatrixLoading" class="ma-muted">正在加载矩阵分组…</span>
        <span v-else class="ma-muted ma-hint-sub"
          >对应 §0 前提表中「主推类目/细类」及报告第五章矩阵；收窄后监测摘要与报告节选仅针对该细类。</span
        >
      </div>
      <p v-if="briefMatrixErr" class="ma-err">{{ briefMatrixErr }}</p>
      <p v-if="strategyGeneratingOtherTask" class="ma-warn-banner">
        任务 #{{ strategyDraftPendingJobId }} 的策略稿正在生成中，请稍候再切换任务或重复提交。
      </p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>

      <fieldset class="fieldset">
        <legend>§0 策略范围与前提</legend>
        <p class="fieldset-hint">
          下列内容写入成稿首段「<strong>策略范围与前提</strong>」表格。监测词、批次由任务自动带出；<strong>主推类目/细类</strong>以顶部「矩阵细类」为准（与报告矩阵一致）。「本品角色、战场、客群」与 §1.3 列表同源，在底稿中会一并出现。
        </p>
        <label class="fld">
          <span>本品角色（策略服务对象）</span>
          <input
            v-model="decisions.product_role"
            type="text"
            placeholder="对应表中「策略服务对象」：如追赶型 / 新品 / 防守 / 拓品类"
          />
        </label>
        <label class="fld fld-block">
          <span>一句话战场</span>
          <textarea
            v-model="decisions.battlefield_one_line"
            rows="2"
            placeholder="对应表中「一句话战场」：在什么需求场景、与谁争同一批检索与购买用户"
          />
        </label>
        <label class="fld fld-block">
          <span>目标客群 / 场景</span>
          <input
            v-model="decisions.audience_segment"
            type="text"
            placeholder="对应表中「目标客群/场景」：为谁、在什么情境下买（可选）"
          />
        </label>
        <label class="fld">
          <span>时间范围</span>
          <input
            v-model="decisions.time_horizon"
            type="text"
            placeholder="对应 §6.1「时间范围」与前提表：如本季度 / 未来 12 周"
          />
        </label>
        <label class="fld fld-block">
          <span>成功标准（可量化）</span>
          <textarea
            v-model="decisions.success_criteria"
            rows="2"
            placeholder="对应 §6.1「成功标准」与前提表：如搜索位次、转化、复购等可验证指标"
          />
        </label>
        <label class="fld fld-block">
          <span>非目标</span>
          <textarea
            v-model="decisions.non_goals"
            rows="2"
            placeholder="对应 §6.1「非目标」：本阶段明确不做的边界（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>§1.3 一、顾客是谁 · 本品聚焦</legend>
        <p class="fieldset-hint">
          成稿「<strong>§1.3 本品聚焦</strong>」中除角色、客群外的<strong>最后一项</strong>。角色与客群请在上文 §0 填写；此处只补<strong>主要对标</strong>，便于 §5 差异化与全文叙述对齐同一参照系。
        </p>
        <label class="fld fld-block">
          <span>主要对标（§1.3 · 与 §0 战场一致时最有效）</span>
          <input
            v-model="decisions.competitor_reference"
            type="text"
            placeholder="如：具体头部品牌名、或同一价位带的标杆 SKU / 价格带；无则写「待业务指定」或留空由模型泛化"
          />
        </label>
      </fieldset>

      <div class="form-skip-note" role="note">
        <strong>§2～§4</strong>（二、产品价值与用户痛点 · 三、为什么要买这款产品 · 四、为什么要选这个品牌）本页<strong>不设表单项</strong>：§2.1
        痛点表、§3.1、§4.1 等由<strong>监测摘要、报告节选与大模型</strong>按底稿结构填写；你可通过顶部矩阵收窄与「业务备注」影响归纳范围。
      </div>

      <fieldset class="fieldset">
        <legend>§5.3 五、与其它品牌有何不同 · 竞争应对</legend>
        <p class="fieldset-hint">
          写入成稿 <strong>§5.3</strong>「本品倾向」：与 §1.3 主要对标/头部交锋时，优先<strong>侧翼</strong>还是<strong>正面</strong>等。<strong>价位阵地</strong>（贴顶/卡腰/下探）属于 §8.2，在下方填写，勿与此处混淆。
        </p>
        <label class="fld fld-block">
          <span>面对竞品时的主打法（落入 §5.3 正文）</span>
          <select v-model="decisions.competitive_stance" class="job-select full">
            <option v-for="o in stanceOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>§6 六、阶段目标与路径</legend>
        <p class="fieldset-hint">
          §0 已填「时间 / 成功标准 / 非目标」会进入 <strong>§6.1 本阶段定义</strong>；此处三项写入 <strong>§6.2 路径</strong> 下的分条叙述，请用<strong>可执行的动词句</strong>，并与 §2.1 痛点动作方向一致（多细类可分句）。
        </p>
        <label class="fld fld-block">
          <span>营销策略（§6.2 · 路径）</span>
          <textarea
            v-model="decisions.marketing_strategy"
            rows="3"
            placeholder="写入 §6.2「营销策略」：传播、活动、投放、内容主线；尽量写清阶段重点而非口号（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>总体策略（§6.2 · 路径）</span>
          <textarea
            v-model="decisions.general_strategy"
            rows="3"
            placeholder="写入 §6.2「总体策略」：增长 / 品类 / 经营总原则，与上文战场与非目标不矛盾（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>资源与预算备注（§6.2 · 路径）</span>
          <textarea
            v-model="decisions.resource_notes"
            rows="2"
            placeholder="写入 §6.2「资源与预算备注」：人力、投放、产能约束，供模型写节奏与优先级（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>§7～§8 七、品牌四线 · 八、战术支柱</legend>
        <p class="fieldset-hint">
          底稿会把下列内容<strong>各写两遍</strong>：<strong>§7.1～§7.4</strong>（建设/打造/运营/体验四行）与 <strong>§8.1～§8.4</strong>（战术支柱）。价位阵地单选只进 <strong>§8.2</strong> 勾选区；<strong>§8.3 促销</strong>无单独表单项，由监测与模型写。第四章「品牌承诺与调性」仍由模型依据数据写，本页不单独列。
        </p>
        <label class="fld fld-block">
          <span>产品（§7.1 品牌建设 · §8.1 产品策略）</span>
          <textarea
            v-model="decisions.pillar_product"
            rows="2"
            placeholder="同一文案进入 §7.1 与 §8.1：规格、配方/功能叙事、与 §2.1 可挂钩的产品动作（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>价位阵地（仅 §8.2 定价策略 · 单选）</span>
          <select v-model="decisions.positioning_choice" class="job-select full">
            <option v-for="o in positioningOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
        <label class="fld fld-block">
          <span>定价补充（§7.2 品牌打造 · §8.2 定价策略）</span>
          <textarea
            v-model="decisions.pillar_price"
            rows="2"
            placeholder="同一叙述进入 §7.2 与 §8.2「表单价格支柱」：在价位阵地之外的到手价呈现、跟价/避战原则、与大促关系等（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>渠道与触点（§7.3 品牌运营 · §8.4 渠道与传播 · 渠道侧）</span>
          <textarea
            v-model="decisions.pillar_channel"
            rows="2"
            placeholder="同一叙述进入 §7.3 与 §8.4：货架、店铺类型、站内路径、触点优先级等（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>传播与内容（§7.4 品牌体验 · §8.4 渠道与传播 · 传播侧）</span>
          <textarea
            v-model="decisions.pillar_comm"
            rows="2"
            placeholder="同一叙述进入 §7.4 与 §8.4：内容形态、达人/自播、搜索承接与话术方向等（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>§9 九、风险、假设与待验证（确认知晓）</legend>
        <p class="fieldset-hint">
          与成稿 <strong>§9</strong> 风险意识一致；勾选表示已了解数据局限（不影响生成，便于业务自检）。
        </p>
        <label class="chk">
          <input v-model="decisions.ack_risk_keywords" type="checkbox" />
          关注词 / 场景可能以偏概全（需原评论抽样）
        </label>
        <label class="chk">
          <input v-model="decisions.ack_risk_price" type="checkbox" />
          价格带可能含大促或异常挂价（需核对清洗与计价规则）
        </label>
        <label class="chk">
          <input v-model="decisions.ack_risk_concentration" type="checkbox" />
          列表集中度与深入样本品牌可能矛盾（需解释差异）
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>§10 十、下一步与节奏 · 业务备注</legend>
        <p class="fieldset-hint">
          写入成稿 <strong>§10</strong>「下一步与节奏」中的<strong>业务约束与备注</strong>段落；与 §0～§8 表单字段不同，此处自由叙述红线、禁忌与组织判断，模型会融入收尾清单。
        </p>
        <label class="fld fld-block">
          <span>业务备注（§10 正文）</span>
          <textarea
            v-model="businessNotes"
            rows="4"
            placeholder="如：法务/合规表述边界、渠道限价、禁止对标表述、预算与人力硬约束；会进入 §10 而非替换 §2～§8 结构"
          />
        </label>
      </fieldset>
    </section>
  </div>
</template>

<style scoped>
.hint-top {
  margin: 0 0 1rem;
  font-size: 0.88rem;
  color: #4b5563;
  line-height: 1.55;
}
.hint-flow {
  margin-top: -0.6rem;
  padding-top: 0.5rem;
  border-top: 1px solid #e5e7eb;
  font-size: 0.84rem;
}
.hint-top a,
.hint-top :deep(a) {
  color: #2563eb;
  font-weight: 500;
}
.toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
}
.toolbar-stack {
  flex-direction: column;
  align-items: stretch;
}
.toolbar-stack .sel-label {
  margin-bottom: -0.25rem;
}
.sel-label {
  font-size: 0.85rem;
  font-weight: 500;
  color: #374151;
}
.job-select {
  flex: 1;
  min-width: 220px;
  padding: 0.5rem 0.65rem;
  border-radius: 6px;
  border: 1px solid #d1d5db;
  font: inherit;
}
.job-select.full {
  width: 100%;
  min-width: 0;
  box-sizing: border-box;
}
.ma-muted {
  color: #64748b;
}
.ma-warn-banner {
  margin: 0.5rem 0 0;
  padding: 0.5rem 0.75rem;
  font-size: 0.86rem;
  line-height: 1.45;
  color: #92400e;
  background: #fffbeb;
  border: 1px solid #fcd34d;
  border-radius: 6px;
}
.fieldset {
  margin: 1.25rem 0 0;
  padding: 0.85rem 1rem 1rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fafafa;
}
.fieldset legend {
  padding: 0 0.35rem;
  font-size: 0.88rem;
  font-weight: 600;
  color: #1f2937;
}
.fieldset-hint {
  margin: 0 0 0.5rem;
  font-size: 0.8rem;
  line-height: 1.5;
  color: #6b7280;
}
.ma-hint-sub {
  display: block;
  margin-top: 0.35rem;
  font-size: 0.8rem;
  line-height: 1.45;
}
.fld {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  margin-top: 0.65rem;
}
.fld:first-of-type {
  margin-top: 0.35rem;
}
.fld-block {
  width: 100%;
}
.fld span {
  font-size: 0.82rem;
  font-weight: 500;
  color: #4b5563;
}
.fld input[type='text'],
.fld textarea {
  width: 100%;
  box-sizing: border-box;
  padding: 0.5rem 0.65rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.88rem;
}
.fld textarea {
  resize: vertical;
  min-height: 52px;
}
.chk {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.86rem;
  color: #374151;
  line-height: 1.45;
  cursor: pointer;
}
.chk input {
  margin-top: 0.2rem;
}
.chk-inline {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  font-size: 0.86rem;
  color: #374151;
  line-height: 1.45;
  cursor: pointer;
  width: 100%;
  margin-bottom: 0.25rem;
}
.chk-inline input {
  margin-top: 0.2rem;
}
.form-skip-note {
  margin: 1rem 0 0;
  padding: 0.65rem 0.85rem;
  font-size: 0.82rem;
  line-height: 1.5;
  color: #4b5563;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
}
.form-skip-note strong {
  color: #334155;
}
</style>
