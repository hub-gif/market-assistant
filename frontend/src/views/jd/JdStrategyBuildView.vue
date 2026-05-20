<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import { refreshJobs, useJobs, api } from '../../composables/useJobs'
import {
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'
import {
  loadStrategyDraftRecord,
  loadStrategyMatrixScope,
  saveStrategyDraftRecord,
  saveStrategyMatrixScope,
} from '../../lib/strategyDraftStorage'
const route = useRoute()
const router = useRouter()
const { jobs } = useJobs()

const selectedId = ref('')
/** 与监测分源：入底稿 §1.3，作本品事实边界；成稿应结合报告数据写策略，非单独成篇产品文案 */
const ourProductProfile = ref('')
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
  tactic_promotion: '',
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
    our_product_profile: ourProductProfile.value,
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
    tactic_promotion: decisions.tactic_promotion,
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

/** 与 backend ``pipeline/strategy_decision_keys.STRATEGY_DECISION_FIELD_NAMES`` 一致 */
const SAVED_DECISION_KEYS = [
  'product_role',
  'stage_goal_type',
  'time_horizon',
  'success_criteria',
  'non_goals',
  'battlefield_one_line',
  'positioning_choice',
  'competitive_stance',
  'pillar_product',
  'pillar_price',
  'pillar_channel',
  'pillar_comm',
  'tactic_promotion',
  'audience_segment',
  'competitor_reference',
  'resource_notes',
  'marketing_strategy',
  'general_strategy',
  'ack_risk_keywords',
  'ack_risk_price',
  'ack_risk_concentration',
]

/**
 * 从本任务上次已保存的「生成请求」恢复表单，使用户决策与成稿/再次提交一致。
 */
function applyDecisionsFromSavedRecord(jobId) {
  if (!jobId) return
  const rec = loadStrategyDraftRecord(String(jobId))
  const lr = rec?.last_request
  if (!lr || typeof lr !== 'object') return
  for (const k of SAVED_DECISION_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(lr, k)) continue
    if (k.startsWith('ack_')) {
      decisions[k] = Boolean(lr[k])
    } else {
      const v = lr[k]
      decisions[k] = v == null || typeof v === 'boolean' ? '' : String(v)
    }
  }
  if (typeof lr.our_product_profile === 'string') {
    ourProductProfile.value = lr.our_product_profile
  } else {
    ourProductProfile.value = ''
  }
  if (typeof lr.business_notes === 'string') {
    businessNotes.value = lr.business_notes
  } else {
    businessNotes.value = ''
  }
  if (lr.generator === 'rules' || lr.generator === 'llm') {
    rulesOnlyThisRun.value = lr.generator === 'rules'
  }
}

function formatJobOption(j) {
  const t = j.created_at
  const tail = t ? String(t).replace('T', ' ').slice(0, 16) : ''
  return tail ? `${j.id} · ${j.keyword} · ${tail}` : `${j.id} · ${j.keyword}`
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

watch(selectedId, async (id) => {
  await loadMatrixGroupsForJob(id)
  if (id) {
    applyDecisionsFromSavedRecord(String(id))
    const rec = loadStrategyDraftRecord(String(id))
    const mg = rec?.last_request?.strategy_matrix_group
    if (typeof mg === 'string' && mg.trim() && matrixGroups.value.some((g) => g.group === mg)) {
      strategyMatrixScope.value = mg
    }
  }
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
      <p class="hint-top">选成功任务与矩阵细类；填写的进成稿，未填的可由模型补。</p>

      <div class="toolbar">
        <label class="chk-inline">
          <input v-model="rulesOnlyThisRun" type="checkbox" />
          仅规则稿（更快，不调智能服务）
        </label>
      </div>
      <div class="toolbar">
        <label class="sel-label">任务</label>
        <div class="toolbar-task-select-wrap">
          <el-select
            v-model="selectedId"
            class="jd-toolbar-el-select"
            placeholder="请选择任务"
            filterable
            placement="bottom-start"
          >
            <el-option
              v-for="j in successJobs"
              :key="j.id"
              :label="formatJobOption(j)"
              :value="String(j.id)"
            />
          </el-select>
        </div>
        <el-button
          type="primary"
          :disabled="!selectedId || strategyGeneratingAny || briefMatrixLoading"
          @click="generateAndGoPreview"
        >
          {{ strategyGeneratingThisTask ? '生成中…' : '生成并预览' }}
        </el-button>
      </div>
      <div v-if="selectedId" class="toolbar toolbar-stack toolbar-matrix">
        <label class="sel-label">主推类目（矩阵细类）</label>
        <el-select
          v-model="strategyMatrixScope"
          class="jd-matrix-el-select"
          placeholder="全部分类（不收窄）"
          :disabled="briefMatrixLoading || strategyGeneratingAny"
          placement="bottom-start"
        >
          <el-option label="全部分类（不收窄）" :value="''" />
          <el-option
            v-for="g in matrixGroups"
            :key="g.group"
            :label="`${g.group}（${g.sku_count} 款）`"
            :value="g.group"
          />
        </el-select>
        <span v-if="briefMatrixLoading" class="ma-muted">加载细类中…</span>
        <span v-else class="ma-muted ma-hint-sub">收窄后仅针对该细类。</span>
      </div>
      <p v-if="briefMatrixErr" class="ma-err">{{ briefMatrixErr }}</p>
      <p v-if="strategyGeneratingOtherTask" class="ma-warn-banner">
        任务 {{ strategyDraftPendingJobId }} 生成中，请稍候。
      </p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先完成一次采集。</p>

      <fieldset class="fieldset">
        <legend>策略范围与前提</legend>
        <p class="fieldset-hint">边界与阶段目标；主推类目以顶部细类为准。</p>
        <label class="fld">
          <span>本品角色（策略服务对象）</span>
          <el-input
            v-model="decisions.product_role"
            clearable
            placeholder="如：追赶型 / 新品 / 防守 / 拓品类"
          />
        </label>
        <label class="fld fld-block">
          <span>本阶段策略目标类型</span>
          <el-input
            v-model="decisions.stage_goal_type"
            type="textarea"
            :rows="2"
            placeholder="如：让更多人愿意尝试购买、把销量和转化做起来、稳住老顾客和份额、先验证新品是否卖得动……按你公司本阶段真实目标写一句即可；不填则由系统在成稿中结合数据推断"
          />
        </label>
        <label class="fld fld-block">
          <span>一句话战场</span>
          <el-input
            v-model="decisions.battlefield_one_line"
            type="textarea"
            :rows="2"
            placeholder="在什么需求场景、与谁争夺同一批检索与购买用户"
          />
        </label>
        <label class="fld fld-block">
          <span>目标客群 / 场景</span>
          <el-input
            v-model="decisions.audience_segment"
            clearable
            placeholder="为谁、在什么情境下买（可选）"
          />
        </label>
        <label class="fld">
          <span>时间范围</span>
          <el-input
            v-model="decisions.time_horizon"
            clearable
            placeholder="如：本季度 / 未来 12 周（与后文阶段目标一致）"
          />
        </label>
        <label class="fld fld-block">
          <span>成功标准（可量化）</span>
          <el-input
            v-model="decisions.success_criteria"
            type="textarea"
            :rows="2"
            placeholder="如：搜索位次、转化、复购等可验证指标"
          />
        </label>
        <label class="fld fld-block">
          <span>非目标</span>
          <el-input
            v-model="decisions.non_goals"
            type="textarea"
            :rows="2"
            placeholder="本阶段明确不做的边界（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>本品聚焦 · 主要对标</legend>
        <p class="fieldset-hint">主要对标品牌或价位（可选）。</p>
        <label class="fld fld-block">
          <span>主要对标</span>
          <el-input
            v-model="decisions.competitor_reference"
            clearable
            placeholder="如：具体头部品牌、或同价位标杆；与上文战场一致时最有效。可写「待业务指定」或留空"
          />
        </label>
      </fieldset>

      <div class="form-skip-note" role="note">痛点、理由等由数据与模型写入；可用细类收窄与文末备注影响范围。</div>

      <fieldset class="fieldset">
        <legend>与竞品的应对方式</legend>
        <p class="fieldset-hint">与主竞品的主打法；与下栏「价位阵地」不同。</p>
        <label class="fld fld-block">
          <span>面对竞品时的主打法</span>
          <el-select
            v-model="decisions.competitive_stance"
            class="jd-ep-block"
            placement="bottom-start"
          >
            <el-option
              v-for="o in stanceOptions"
              :key="o.value || 'empty'"
              :label="o.label"
              :value="o.value"
            />
          </el-select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>阶段目标与路径（补充）</legend>
        <p class="fieldset-hint">营销与总体策略、资源备注；宜写可执行句。</p>
        <label class="fld fld-block">
          <span>营销策略</span>
          <el-input
            v-model="decisions.marketing_strategy"
            type="textarea"
            :rows="3"
            placeholder="传播、活动、投放、内容主线；写清阶段重点而非口号（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>总体策略</span>
          <el-input
            v-model="decisions.general_strategy"
            type="textarea"
            :rows="3"
            placeholder="增长 / 品类 / 经营总原则；与上文战场与非目标不矛盾（可选）"
          />
        </label>
        <label class="fld fld-block">
          <span>资源与预算备注</span>
          <el-input
            v-model="decisions.resource_notes"
            type="textarea"
            :rows="2"
            placeholder="人力、投放、产能约束；便于成稿写节奏与优先级（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>品牌四线：建设 · 打造 · 运营 · 体验</legend>
        <label class="fld fld-block">
          <span>品牌建设</span>
          <el-input v-model="decisions.pillar_product" type="textarea" :rows="2" placeholder="选填" />
        </label>
        <label class="fld fld-block">
          <span>品牌打造</span>
          <el-input v-model="decisions.pillar_price" type="textarea" :rows="2" placeholder="选填" />
        </label>
        <label class="fld fld-block">
          <span>品牌运营</span>
          <el-input v-model="decisions.pillar_channel" type="textarea" :rows="2" placeholder="选填" />
        </label>
        <label class="fld fld-block">
          <span>品牌体验</span>
          <el-input v-model="decisions.pillar_comm" type="textarea" :rows="2" placeholder="选填" />
        </label>
      </fieldset>

      <fieldset class="fieldset fieldset-tactic-pillars">
        <legend>战术支柱</legend>
        <div class="tactic-sec">
          <h4 class="tactic-sec-t">产品策略</h4>
          <label class="fld fld-block fld-tight">
            <el-input v-model="decisions.pillar_product" type="textarea" :rows="2" placeholder="选填" />
          </label>
        </div>
        <div class="tactic-sec">
          <h4 class="tactic-sec-t">定价策略</h4>
          <label class="fld fld-block">
            <span>价位阵地</span>
            <el-select
              v-model="decisions.positioning_choice"
              class="jd-ep-block"
              placement="bottom-start"
            >
              <el-option
                v-for="o in positioningOptions"
                :key="o.value || 'empty'"
                :label="o.label"
                :value="o.value"
              />
            </el-select>
          </label>
          <label class="fld fld-block">
            <span>补充说明</span>
            <el-input v-model="decisions.pillar_price" type="textarea" :rows="2" placeholder="选填" />
          </label>
        </div>
        <div class="tactic-sec">
          <h4 class="tactic-sec-t">促销与活动策略</h4>
          <label class="fld fld-block fld-tight">
            <el-input v-model="decisions.tactic_promotion" type="textarea" :rows="2" placeholder="选填" />
          </label>
        </div>
        <div class="tactic-sec">
          <h4 class="tactic-sec-t">渠道与传播</h4>
          <div class="tactic-ch-row">
            <label class="fld fld-block">
              <span>渠道</span>
              <el-input v-model="decisions.pillar_channel" type="textarea" :rows="2" placeholder="选填" />
            </label>
            <label class="fld fld-block">
              <span>传播</span>
              <el-input v-model="decisions.pillar_comm" type="textarea" :rows="2" placeholder="选填" />
            </label>
          </div>
        </div>
      </fieldset>

      <fieldset class="fieldset">
        <legend>数据与样本风险（确认知晓）</legend>
        <p class="fieldset-hint">勾选即知悉数据局限。</p>
        <el-checkbox v-model="decisions.ack_risk_keywords" class="chk-ep">
          关注词 / 场景可能以偏概全（需原评论抽样）
        </el-checkbox>
        <el-checkbox v-model="decisions.ack_risk_price" class="chk-ep">
          价格带可能含大促或异常挂价（需核对清洗与计价规则）
        </el-checkbox>
        <el-checkbox v-model="decisions.ack_risk_concentration" class="chk-ep">
          列表集中度与深入样本品牌可能矛盾（需解释差异）
        </el-checkbox>
      </fieldset>

      <fieldset class="fieldset">
        <legend>本品说明</legend>
        <p class="fieldset-hint">
          与监测摘要分源：写入底稿 §1.3，作<strong>本品事实与宣称边界</strong>；成稿须与报告数据<strong>结合</strong>写策略，勿单靠大段产品说明。留空且配好手册 PDF 时按任务关键词摘录。
        </p>
        <label class="fld fld-block">
          <span>本品 / 手册要点</span>
          <el-input
            v-model="ourProductProfile"
            type="textarea"
            :rows="6"
            placeholder="如：核心功效、成分宣称边界、目标人群、价位与渠道定位、与手册一致的表述约束等（可选）"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>业务备注</legend>
        <p class="fieldset-hint">合规、渠道、预算等补充，附在成稿末。</p>
        <label class="fld fld-block">
          <span>业务备注</span>
          <el-input
            v-model="businessNotes"
            type="textarea"
            :rows="4"
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
.toolbar-task-select-wrap {
  flex: 1 1 auto;
  min-width: 10rem;
  max-width: 20rem;
}
.toolbar-stack .sel-label {
  margin-bottom: -0.25rem;
}
.toolbar-matrix :deep(.jd-matrix-el-select.el-select) {
  align-self: flex-start;
}
.chk-ep {
  display: flex;
  align-items: flex-start;
  margin-top: 0.5rem;
  width: 100%;
}
.chk-ep:first-of-type {
  margin-top: 0.35rem;
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
.fieldset-tactic-pillars .tactic-sec {
  margin-top: 0.65rem;
  padding-top: 0.7rem;
  border-top: 1px solid #e5e7eb;
}
.fieldset-tactic-pillars .tactic-sec:first-of-type {
  margin-top: 0.25rem;
  padding-top: 0;
  border-top: none;
}
.tactic-sec-t {
  margin: 0 0 0.4rem;
  font-size: 0.88rem;
  font-weight: 600;
  color: #374151;
}
.tactic-ch-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.5rem 1rem;
  align-items: start;
}
/* 与单列 .fld 的 margin 规则冲突：同排两列曾出现 0.35rem vs 0.65rem 顶距，导致错位 */
.tactic-ch-row .fld {
  margin-top: 0;
}
@media (max-width: 640px) {
  .tactic-ch-row {
    grid-template-columns: 1fr;
  }
  .tactic-ch-row .fld + .fld {
    margin-top: 0.5rem;
  }
}
.fld-tight {
  margin-top: 0.15rem;
}
</style>
