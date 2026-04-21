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

const stanceOptions = [
  { value: '', label: '暂不填写' },
  { value: 'flank', label: '倾向侧翼切入' },
  { value: 'head_on', label: '倾向正面替代' },
  { value: 'both', label: '分层推进（侧翼 + 正面）' },
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
        选择<strong>已成功</strong>任务，在下方选择<strong>策略类目</strong>（与竞品矩阵细类一致，对应成稿中的主推细类；选「全部分类」则与全关键词样本一致）。<strong>默认</strong>使用大模型在规则底稿与同任务监测摘要基础上润色成稿（需服务端已配置网关）。可勾选「本次仅生成规则稿」跳过智能润色。策略配置与「分析报告生成」的<strong>报告配置</strong>相互独立。收窄类目时，成稿会并入该细类在宿主报告<strong>第五～第八章</strong>的大模型归纳节选。提交后跳转
        <RouterLink to="/jd/strategy-view">策略稿预览</RouterLink>
        。<strong>已填项</strong>写入底稿并由大模型落实；<strong>未填项</strong>可由大模型结合监测数据推断（仍受「禁止编造」约束）。
      </p>
      <p class="hint-top hint-flow">
        <strong>与成稿目录对齐</strong>（对应下载策略稿 Markdown）：「策略范围与前提」→「摘要」→「一、顾客是谁」→「二、针对痛点要怎么做」（主要由数据与模型归纳）→「三～八、购买理由 / 品牌 / 竞品 / 阶段路径 / 品牌四线 / 战术支柱」→「九、风险」→「十、下一步」→「业务约束与备注」→「附录」。下方表单按<strong>同一条目顺序</strong>分组，便于逐项填写。
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
          >与成稿「策略范围与前提」中的主推细类及报告第五章矩阵一致；收窄后监测摘要与报告节选仅针对该细类。</span
        >
      </div>
      <p v-if="briefMatrixErr" class="ma-err">{{ briefMatrixErr }}</p>
      <p v-if="strategyGeneratingOtherTask" class="ma-warn-banner">
        任务 #{{ strategyDraftPendingJobId }} 的策略稿正在生成中，请稍候再切换任务或重复提交。
      </p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>

      <fieldset class="fieldset">
        <legend>策略范围与前提 · 摘要依据</legend>
        <p class="fieldset-hint">
          对应成稿章节「<strong>策略范围与前提</strong>」与「<strong>一、顾客是谁 · 本品聚焦</strong>」表格；监测词与批次由任务自动带出。
        </p>
        <label class="fld">
          <span>本品角色（策略服务对象）</span>
          <input v-model="decisions.product_role" type="text" placeholder="如：追赶型 / 新品 / 防守 / 拓品类" />
        </label>
        <label class="fld fld-block">
          <span>一句话战场</span>
          <textarea
            v-model="decisions.battlefield_one_line"
            rows="2"
            placeholder="在哪个需求场景、与谁争夺同一批检索与购买用户？"
          />
        </label>
        <label class="fld fld-block">
          <span>目标客群 / 场景</span>
          <input
            v-model="decisions.audience_segment"
            type="text"
            placeholder="为谁、什么场景（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>主要对标</span>
          <input
            v-model="decisions.competitor_reference"
            type="text"
            placeholder="品牌或价位带参照（可选）"
          />
        </label>
        <label class="fld">
          <span>时间范围</span>
          <input v-model="decisions.time_horizon" type="text" placeholder="如：本季度 / 未来 12 周" />
        </label>
        <label class="fld fld-block">
          <span>成功标准（可量化）</span>
          <textarea v-model="decisions.success_criteria" rows="2" placeholder="如：搜索位次、转化、复购等（对应「本阶段策略目标」）" />
        </label>
        <label class="fld fld-block">
          <span>非目标</span>
          <textarea v-model="decisions.non_goals" rows="2" placeholder="明确本阶段不做什么（可选）" />
        </label>
        <label class="fld fld-block">
          <span>资源与预算备注</span>
          <textarea
            v-model="decisions.resource_notes"
            rows="2"
            placeholder="人力、投放、产能等（可选；亦进入「六、阶段目标与路径」）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>四、为什么要选「这个品牌」— 主定位</legend>
        <p class="fieldset-hint">对应成稿「<strong>四、为什么要选这个品牌</strong>」中与价位带相关的定位勾选。</p>
        <label class="fld fld-block">
          <span>主定位（选一条）</span>
          <select v-model="decisions.positioning_choice" class="job-select full">
            <option v-for="o in positioningOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>五、与其它品牌有何不同 — 竞争倾向</legend>
        <p class="fieldset-hint">对应成稿「<strong>五、与其它品牌有何不同 · 竞争应对</strong>」。</p>
        <label class="fld fld-block">
          <span>本品竞争倾向</span>
          <select v-model="decisions.competitive_stance" class="job-select full">
            <option v-for="o in stanceOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>六、阶段目标与路径</legend>
        <p class="fieldset-hint">
          对应「<strong>六、阶段目标与路径</strong>」；「二、针对痛点要怎么做」表格主要由监测与模型归纳，本页不单独列项。
        </p>
        <label class="fld fld-block">
          <span>营销策略</span>
          <textarea
            v-model="decisions.marketing_strategy"
            rows="3"
            placeholder="传播、活动、投放、内容主线（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>总体策略</span>
          <textarea
            v-model="decisions.general_strategy"
            rows="3"
            placeholder="增长 / 品类 / 经营总原则（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>七 · 八、品牌四线与战术支柱（4P）</legend>
        <p class="fieldset-hint">
          对应成稿「<strong>七、品牌四线</strong>」与「<strong>八、战术支柱</strong>」（产品 / 定价 / 渠道与传播）。
        </p>
        <label class="fld fld-block">
          <span>产品</span>
          <textarea v-model="decisions.pillar_product" rows="2" placeholder="产品侧动作或差异（可选）" />
        </label>
        <label class="fld fld-block">
          <span>定价</span>
          <textarea v-model="decisions.pillar_price" rows="2" placeholder="价格与价值呈现（可选）" />
        </label>
        <label class="fld fld-block">
          <span>渠道与触点</span>
          <textarea v-model="decisions.pillar_channel" rows="2" placeholder="渠道、货架与触点（可选）" />
        </label>
        <label class="fld fld-block">
          <span>传播与内容</span>
          <textarea v-model="decisions.pillar_comm" rows="2" placeholder="传播、内容、沟通（可选）" />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>九、风险、假设与待验证（确认知晓）</legend>
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
        <legend>业务约束与备注</legend>
        <p class="fieldset-hint">
          对应成稿「<strong>十、下一步与节奏</strong>」下的<strong>业务约束与备注</strong>；供法务、渠道红线、内部判断等补充。
        </p>
        <label class="fld fld-block">
          <span>业务备注</span>
          <textarea
            v-model="businessNotes"
            rows="4"
            placeholder="渠道红线、价位策略、竞品对标、预算与组织约束等"
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
</style>
