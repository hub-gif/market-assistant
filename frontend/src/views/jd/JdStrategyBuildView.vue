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
const useLlm = ref(false)

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
  ack_risk_keywords: false,
  ack_risk_price: false,
  ack_risk_concentration: false,
})

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
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
  return {
    generator: useLlm.value ? 'llm' : 'rules',
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
    ack_risk_keywords: decisions.ack_risk_keywords,
    ack_risk_price: decisions.ack_risk_price,
    ack_risk_concentration: decisions.ack_risk_concentration,
  }
}

const STORAGE_KEY = (id) => `ma_strategy_draft_${id}`

async function loadList() {
  try {
    await refreshJobs()
  } catch {
    /* ignore */
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
        选择<strong>已成功</strong>任务，在下方填空与勾选。<strong>未勾选</strong>下方选项时，由系统规则生成策略底稿；<strong>勾选「使用大模型生成」</strong>后，由大模型在底稿与数据摘要基础上成稿（服务端需已配置并可用）。提交后跳转到
        <RouterLink to="/jd/strategy-view">策略稿预览</RouterLink>
        。数据与
        <RouterLink to="/jd/analysis-view">同任务分析产出</RouterLink>
        一致。未填项在文稿中仍保留占位提示。
      </p>

      <div class="toolbar">
        <label class="chk-inline">
          <input v-model="useLlm" type="checkbox" />
          使用大模型生成（服务端需已配置并可用）
        </label>
      </div>
      <div class="toolbar">
        <label class="sel-label">任务</label>
        <select v-model="selectedId" class="job-select">
          <option value="" disabled>请选择任务</option>
          <option v-for="j in successJobs" :key="j.id" :value="String(j.id)">
            #{{ j.id }} · {{ j.keyword }} · {{ j.run_dir?.split(/[/\\]/).pop() || '' }}
          </option>
        </select>
        <button
          type="button"
          class="ma-btn ma-btn-primary"
          :disabled="!selectedId || strategyGeneratingAny"
          @click="generateAndGoPreview"
        >
          {{ strategyGeneratingThisTask ? '生成中…' : '生成并前往预览' }}
        </button>
      </div>
      <p v-if="strategyGeneratingOtherTask" class="ma-warn-banner">
        任务 #{{ strategyDraftPendingJobId }} 的策略稿正在生成中，请稍候再切换任务或重复提交。
      </p>

      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        任务目录：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>

      <fieldset class="fieldset">
        <legend>一、战略背景与目标</legend>
        <label class="fld">
          <span>本品角色</span>
          <input v-model="decisions.product_role" type="text" placeholder="如：追赶 / 新品 / 防守" />
        </label>
        <label class="fld">
          <span>时间范围</span>
          <input v-model="decisions.time_horizon" type="text" placeholder="如：本季度 / 未来 12 周" />
        </label>
        <label class="fld fld-block">
          <span>成功标准（可量化）</span>
          <textarea v-model="decisions.success_criteria" rows="2" placeholder="如：搜索位次、转化率…" />
        </label>
        <label class="fld fld-block">
          <span>非目标</span>
          <textarea v-model="decisions.non_goals" rows="2" placeholder="明确不做什么（可选）" />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>二、战场（一句话）</legend>
        <label class="fld fld-block">
          <span>一句话战场</span>
          <textarea
            v-model="decisions.battlefield_one_line"
            rows="2"
            placeholder="在哪个需求场景、与谁抢同一批用户？"
          />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>三、竞争态势自判</legend>
        <label class="fld fld-block">
          <span>本品倾向</span>
          <select v-model="decisions.competitive_stance" class="job-select full">
            <option v-for="o in stanceOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>四、价格带定位选项（勾选一条）</legend>
        <label class="fld fld-block">
          <span>主定位</span>
          <select v-model="decisions.positioning_choice" class="job-select full">
            <option v-for="o in positioningOptions" :key="o.value || 'empty'" :value="o.value">
              {{ o.label }}
            </option>
          </select>
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>六、策略支柱 — 本品打算怎么做（可先填一列）</legend>
        <label class="fld fld-block">
          <span>产品</span>
          <textarea v-model="decisions.pillar_product" rows="2" />
        </label>
        <label class="fld fld-block">
          <span>价格</span>
          <textarea v-model="decisions.pillar_price" rows="2" />
        </label>
        <label class="fld fld-block">
          <span>渠道 / 触点</span>
          <textarea v-model="decisions.pillar_channel" rows="2" />
        </label>
        <label class="fld fld-block">
          <span>传播与内容</span>
          <textarea v-model="decisions.pillar_comm" rows="2" />
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>七、风险确认（已知晓则勾选）</legend>
        <label class="chk">
          <input v-model="decisions.ack_risk_keywords" type="checkbox" />
          关注词 / 场景可能以偏概全（需原评论抽样）
        </label>
        <label class="chk">
          <input v-model="decisions.ack_risk_price" type="checkbox" />
          价格带可能含大促或异常挂价（需核对口径）
        </label>
        <label class="chk">
          <input v-model="decisions.ack_risk_concentration" type="checkbox" />
          列表集中度与深入样本品牌可能矛盾（需解释差异）
        </label>
      </fieldset>

      <fieldset class="fieldset">
        <legend>八、业务约束与内部判断</legend>
        <label class="fld fld-block">
          <span>业务备注</span>
          <textarea
            v-model="businessNotes"
            rows="4"
            placeholder="渠道红线、价位策略、竞品对标、预算量级等"
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
.run-dir-note {
  margin: 0.75rem 0 0;
  font-size: 0.8rem;
  line-height: 1.5;
}
.run-dir-path {
  display: block;
  margin-top: 0.35rem;
  font-size: 0.75rem;
  word-break: break-all;
  color: #475569;
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
