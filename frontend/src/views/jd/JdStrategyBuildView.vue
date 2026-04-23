<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { refreshJobs, useJobs, api } from '../../composables/useJobs'
import {
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'
import {
  loadStrategyMatrixScope,
  saveStrategyDraftRecord,
  saveStrategyMatrixScope,
} from '../../lib/strategyDraftStorage'

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
  stage_goal_type: '',
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

/** 对应后端 competitive_stance：与头部或主竞品「怎么打」，非价位阵地、亦非泛指的「进市场」。 */
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
    stage_goal_type: decisions.stage_goal_type,
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
    const saved = loadStrategyMatrixScope(id)
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
      saveStrategyDraftRecord(id, {
        markdown: j.markdown || '',
        keyword: j.keyword || '',
        generated_at: j.generated_at || '',
        last_request: buildPayload(),
      })
      router.push({ path: '/jd/strategy-view', query: { job: id } })
    } catch (e) {
      err.value = String(e)
    }
  })
}

function onStorageScopeSync(ev) {
  const prefix = 'ma_strategy_scope_'
  if (!ev.key || !ev.key.startsWith(prefix)) return
  const jid = ev.key.slice(prefix.length)
  if (jid !== String(selectedId.value)) return
  const v = loadStrategyMatrixScope(jid)
  if (v && matrixGroups.value.some((g) => g.group === v)) {
    strategyMatrixScope.value = v
  } else if (!v) {
    strategyMatrixScope.value = ''
  }
}

onMounted(() => {
  loadList()
  if (typeof window !== 'undefined') {
    window.addEventListener('storage', onStorageScopeSync)
  }
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    window.removeEventListener('storage', onStorageScopeSync)
  }
})

watch(selectedId, (id) => {
  loadMatrixGroupsForJob(id)
})

watch(strategyMatrixScope, (v) => {
  const jid = selectedId.value
  if (!jid) return
  saveStrategyMatrixScope(jid, v)
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
        选择<strong>已成功</strong>任务，先选顶部<strong>矩阵细类</strong>（主推类目，与报告矩阵一致）。策略稿与矩阵选择保存在本机 <strong>localStorage</strong>，同域名下可跨标签查看；与其它页面的耗时任务通过全局任务锁同步。下方字段按策略文档常见顺序排列；成稿里的小节标题与编号由系统自动对应。有关痛点、购买理由、品牌承诺等由监测与模型撰写，本页主要收集<strong>业务决策与战术要点</strong>。生成结果见
        <RouterLink to="/jd/strategy-view">策略稿预览</RouterLink>。<strong>已填项</strong>进入底稿并由大模型落实；<strong>未填项</strong>可由模型结合数据推断。
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
          >与策略稿中的主推类目及报告矩阵一致；收窄后监测摘要与报告节选仅针对该细类。</span
        >
      </div>
      <p v-if="briefMatrixErr" class="ma-err">{{ briefMatrixErr }}</p>
      <p v-if="strategyGeneratingOtherTask" class="ma-warn-banner">
        任务 #{{ strategyDraftPendingJobId }} 的策略稿正在生成中，请稍候再切换任务或重复提交。
      </p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>

      <fieldset class="fieldset">
        <legend>策略范围与前提</legend>
        <p class="fieldset-hint">
          界定本次策略的任务边界与阶段目标。监测词、批次由任务自动带出；<strong>主推类目</strong>以顶部「矩阵细类」为准。角色、<strong>本阶段策略目标类型</strong>、战场、客群会进入策略稿开篇；目标类型填好后，成稿会按你的表述落实。并与下一栏「主要对标」衔接。
        </p>
        <label class="fld">
          <span>本品角色（策略服务对象）</span>
          <input
            v-model="decisions.product_role"
            type="text"
            placeholder="如：追赶型 / 新品 / 防守 / 拓品类"
          />
        </label>
        <label class="fld fld-block">
          <span>本阶段策略目标类型</span>
          <textarea
            v-model="decisions.stage_goal_type"
            rows="2"
            placeholder="如：心智占位、转化与复购爬坡、份额防守等（可与内部规划文档中的阶段目标类型选项对齐）；不填则由系统在成稿中结合数据推断"
          />
        </label>
        <label class="fld fld-block">
          <span>一句话战场</span>
          <textarea
            v-model="decisions.battlefield_one_line"
            rows="2"
            placeholder="在什么需求场景、与谁争夺同一批检索与购买用户"
          />
        </label>
        <label class="fld fld-block">
          <span>目标客群 / 场景</span>
          <input
            v-model="decisions.audience_segment"
            type="text"
            placeholder="为谁、在什么情境下买（可选）"
          />
        </label>
        <label class="fld">
          <span>时间范围</span>
          <input
            v-model="decisions.time_horizon"
            type="text"
            placeholder="如：本季度 / 未来 12 周（与后文阶段目标一致）"
          />
        </label>
        <label class="fld fld-block">
          <span>成功标准（可量化）</span>
          <textarea
            v-model="decisions.success_criteria"
            rows="2"
            placeholder="如：搜索位次、转化、复购等可验证指标"
          />
        </label>
        <label class="fld fld-block">
          <span>非目标</span>
          <textarea
            v-model="decisions.non_goals"
            rows="2"
            placeholder="本阶段明确不做的边界（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>本品聚焦 · 主要对标</legend>
        <p class="fieldset-hint">
          角色与客群已在上文填写；此处补充<strong>主要对标</strong>（品牌或价位参照），便于后文写差异与竞争应对时对齐同一参照系。
        </p>
        <label class="fld fld-block">
          <span>主要对标</span>
          <input
            v-model="decisions.competitor_reference"
            type="text"
            placeholder="如：具体头部品牌、或同价位标杆；与上文战场一致时最有效。可写「待业务指定」或留空"
          />
        </label>
      </fieldset>

      <div class="form-skip-note" role="note">
        <strong>自动撰写部分</strong>：用户痛点、购买理由、品牌承诺与调性等内容<strong>不在本页填写</strong>，将由监测摘要、报告节选与大模型写入策略稿；可通过顶部矩阵收窄与文末「业务备注」影响范围。
      </div>

      <fieldset class="fieldset">
        <legend>与竞品的应对方式</legend>
        <p class="fieldset-hint">
          面对头部或主竞品时，优先<strong>侧翼</strong>还是<strong>正面</strong>等。下方「价位阵地」回答在哪条价格带上打，与这里不是一回事。
        </p>
        <label class="fld fld-block">
          <span>面对竞品时的主打法</span>
          <select v-model="decisions.competitive_stance" class="job-select full">
            <option v-for="o in stanceOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>阶段目标与路径（补充）</legend>
        <p class="fieldset-hint">
          上文「时间、成功标准、非目标」会进入阶段定义；此处填写营销策略、总体策略与资源备注。尽量用<strong>可执行的动词句</strong>，并与痛点动作方向一致（多细类可分句）。
        </p>
        <label class="fld fld-block">
          <span>营销策略</span>
          <textarea
            v-model="decisions.marketing_strategy"
            rows="3"
            placeholder="传播、活动、投放、内容主线；写清阶段重点而非口号（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>总体策略</span>
          <textarea
            v-model="decisions.general_strategy"
            rows="3"
            placeholder="增长 / 品类 / 经营总原则；与上文战场与非目标不矛盾（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>资源与预算备注</span>
          <textarea
            v-model="decisions.resource_notes"
            rows="2"
            placeholder="人力、投放、产能约束；便于成稿写节奏与优先级（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>品牌四线与战术动作</legend>
        <p class="fieldset-hint">
          下列内容会在策略稿中用于<strong>品牌四线</strong>与<strong>战术支柱</strong>相关段落（系统会自动落到对应小节）。价位阵地为单选；促销与活动细节无单独表单项，由监测与模型归纳。品牌承诺与调性由模型依据数据撰写。
        </p>
        <label class="fld fld-block">
          <span>产品</span>
          <textarea
            v-model="decisions.pillar_product"
            rows="2"
            placeholder="规格、配方或功能叙事、计划中的产品动作（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>价位阵地（单选）</span>
          <select v-model="decisions.positioning_choice" class="job-select full">
            <option v-for="o in positioningOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
        <label class="fld fld-block">
          <span>定价（补充说明）</span>
          <textarea
            v-model="decisions.pillar_price"
            rows="2"
            placeholder="在价位阵地之外：到手价呈现、跟价或避战原则、与大促关系等（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>渠道与触点</span>
          <textarea
            v-model="decisions.pillar_channel"
            rows="2"
            placeholder="货架、店铺类型、站内路径、触点优先级等（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>传播与内容</span>
          <textarea
            v-model="decisions.pillar_comm"
            rows="2"
            placeholder="内容形态、达人/自播、搜索承接与话术方向等（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>数据与样本风险（确认知晓）</legend>
        <p class="fieldset-hint">
          勾选表示了解以下数据局限（不影响生成，仅供自检）。
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
        <legend>业务备注</legend>
        <p class="fieldset-hint">
          法务红线、渠道约束、组织与预算等自由补充，会进入策略稿收尾部分；不替换正文结构，也不替代上方已填的决策字段。
        </p>
        <label class="fld fld-block">
          <span>业务备注</span>
          <textarea
            v-model="businessNotes"
            rows="4"
            placeholder="如法务/合规表述边界、渠道限价、禁止对标表述、预算与人力硬约束等（可选）"
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
