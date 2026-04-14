<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import {
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'
import { RouterLink } from 'vue-router'
import ReportConfigFormFields from '../../components/ReportConfigFormFields.vue'
import { refreshJobs, useJobs, api, reportConfigDefaultsUrl } from '../../composables/useJobs'
import { useReportConfigForm } from '../../composables/useReportConfigForm'

const { jobs } = useJobs()
const selectedId = ref('')
/** 勾选则本次重新生成不走整篇大模型合并（仍先跑规则引擎落盘） */
const useRulesOnly = ref(false)
const regenErr = ref('')
const genInFlight = generationInFlightKey()
const REGEN_PREFIX = 'regenerate-report:'
const regenPendingJobId = computed(() => {
  const k = genInFlight.value
  if (!k || !k.startsWith(REGEN_PREFIX)) return null
  return k.slice(REGEN_PREFIX.length)
})
const regenBusyThisTask = computed(
  () => regenPendingJobId.value != null && regenPendingJobId.value === selectedId.value,
)
/** 任意任务正在重新生成时都应禁用按钮，避免切换页签后 selectedId 被重置导致误判可点 */
const regenBusyAny = computed(() => regenPendingJobId.value != null)
const regenBusyOtherTask = computed(
  () => regenPendingJobId.value != null && regenPendingJobId.value !== selectedId.value,
)

const {
  focusWordRows,
  scenarioGroups,
  marketRows,
  applyFromApiConfig,
  buildPayload,
  addFocusRow,
  removeFocusRow,
  addScenarioRow,
  removeScenarioRow,
  addMarketRow,
  removeMarketRow,
} = useReportConfigForm()

const reportConfigErr = ref('')
const reportConfigSaveLoading = ref(false)
const reportConfigDefaultsLoading = ref(false)
const advancedJsonText = ref('')

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
)

function syncReportConfigFromJob(j) {
  const cfg =
    j && typeof j.report_config === 'object' && j.report_config !== null ? j.report_config : {}
  applyFromApiConfig(cfg)
}

async function loadReportConfigDefaults() {
  reportConfigErr.value = ''
  reportConfigDefaultsLoading.value = true
  try {
    const r = await api(reportConfigDefaultsUrl())
    const text = await r.text()
    if (!r.ok) {
      try {
        const j = JSON.parse(text)
        reportConfigErr.value = j.detail || text
      } catch {
        reportConfigErr.value = text || `HTTP ${r.status}`
      }
      return
    }
    applyFromApiConfig(JSON.parse(text))
  } catch (e) {
    reportConfigErr.value = String(e)
  } finally {
    reportConfigDefaultsLoading.value = false
  }
}

async function saveReportConfigToJob() {
  const id = selectedId.value
  if (!id) return
  reportConfigErr.value = ''
  const parsed = buildPayload()
  reportConfigSaveLoading.value = true
  try {
    const r = await api(`/api/jobs/${id}/`, {
      method: 'PATCH',
      body: JSON.stringify({ report_config: parsed }),
    })
    const text = await r.text()
    if (!r.ok) {
      try {
        const j = JSON.parse(text)
        reportConfigErr.value =
          typeof j === 'object' && j !== null
            ? JSON.stringify(j, null, 2)
            : j.detail || text
      } catch {
        reportConfigErr.value = text || `HTTP ${r.status}`
      }
      return
    }
    const updated = JSON.parse(text)
    const idx = jobs.value.findIndex((x) => x.id === updated.id)
    if (idx >= 0) jobs.value[idx] = updated
    syncReportConfigFromJob(updated)
  } catch (e) {
    reportConfigErr.value = String(e)
  } finally {
    reportConfigSaveLoading.value = false
  }
}

function onAdvancedJsonToggle(ev) {
  const el = ev.target
  if (el instanceof HTMLDetailsElement && el.open) {
    advancedJsonText.value = JSON.stringify(buildPayload(), null, 2)
  }
}

function applyAdvancedJsonToForm() {
  reportConfigErr.value = ''
  try {
    const j = JSON.parse(advancedJsonText.value.trim() || '{}')
    if (j === null || typeof j !== 'object' || Array.isArray(j)) {
      reportConfigErr.value = '内容须为 JSON 对象'
      return
    }
    applyFromApiConfig(j)
    advancedJsonText.value = JSON.stringify(buildPayload(), null, 2)
  } catch {
    reportConfigErr.value = '无法解析：请检查 JSON 格式'
  }
}

async function loadList() {
  try {
    await refreshJobs()
  } catch {
    /* ignore */
  }
}

async function regenerateReport() {
  const id = selectedId.value
  if (!id) return
  regenErr.value = ''
  const key = `${REGEN_PREFIX}${id}`
  await withGenerationInFlight(key, async () => {
    try {
      const r = await api(`/api/jobs/${id}/regenerate-report/`, {
        method: 'POST',
        body: JSON.stringify({
          generator: useRulesOnly.value ? 'rules' : 'llm',
        }),
      })
      const text = await r.text()
      if (!r.ok) {
        try {
          const j = JSON.parse(text)
          regenErr.value = j.detail || text
        } catch {
          regenErr.value = text || `HTTP ${r.status}`
        }
        return
      }
      const updated = JSON.parse(text)
      const idx = jobs.value.findIndex((x) => x.id === updated.id)
      if (idx >= 0) jobs.value[idx] = updated
    } catch (e) {
      regenErr.value = String(e)
    }
  })
}

onMounted(loadList)

watch(selectedId, async () => {
  reportConfigErr.value = ''
  const id = selectedId.value
  if (!id) return
  try {
    const r = await api(`/api/jobs/${id}/`)
    if (r.ok) {
      const j = await r.json()
      const idx = jobs.value.findIndex((x) => x.id === j.id)
      if (idx >= 0) jobs.value[idx] = j
      syncReportConfigFromJob(j)
    }
  } catch {
    /* ignore */
  }
})

watch(
  successJobs,
  (list) => {
    if (!selectedId.value && list.length) selectedId.value = String(list[0].id)
  },
  { immediate: true },
)
</script>

<template>
  <div>
    <section class="ma-card">
      <h2>分析报告生成</h2>
      <p class="hint-top">
        选择<strong>已成功</strong>的任务，调整下方统计规则后点「保存以上设置」，再点「重新生成报告」。默认<strong>先规则引擎</strong>写出统计稿，再<strong>合并大模型补充</strong>（需网关与密钥）；不重新爬取。各章评价解读等开关由「填入推荐示例」或「高级 JSON」中的
        <code>llm_*</code> 字段控制。
        阅读与下载请至
        <RouterLink to="/jd/analysis-view">报告查看</RouterLink>。
      </p>

      <div class="toolbar">
        <label class="chk-inline chk-rules-only">
          <input v-model="useRulesOnly" type="checkbox" />
          本次仅用规则引擎（跳过整篇大模型合并，更快、不调 LLM 全文接口）
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
          :disabled="!selectedId || regenBusyAny"
          title="不重新爬取，仅根据本批次已有数据更新报告文件"
          @click="regenerateReport"
        >
          {{ regenBusyThisTask ? '生成中…' : '重新生成报告' }}
        </button>
      </div>
      <p v-if="regenBusyOtherTask" class="ma-warn-banner">
        任务 #{{ regenPendingJobId }} 的报告正在重新生成中，请稍候再切换任务或重复提交。
      </p>

      <div v-if="selectedId" class="report-config-block">
        <h3 class="report-config-title">报告里的评价统计怎么算</h3>
        <p class="hint-top report-config-hint">
          关注词、场景词组、外部市场表等<strong>可以不改</strong>：留空并保存即沿用内置规则。大模型相关布尔项（如
          <code>llm_comment_sentiment</code>、<code>llm_section_bridges</code>）不再单独占勾选框：若任务里已有，会在保存时保留；要改请展开「高级 JSON」。
        </p>
        <div class="report-config-actions">
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            :disabled="reportConfigDefaultsLoading"
            @click="loadReportConfigDefaults"
          >
            {{ reportConfigDefaultsLoading ? '加载中…' : '填入推荐示例' }}
          </button>
          <button
            type="button"
            class="ma-btn ma-btn-primary"
            :disabled="reportConfigSaveLoading"
            @click="saveReportConfigToJob"
          >
            {{ reportConfigSaveLoading ? '保存中…' : '保存以上设置' }}
          </button>
        </div>

        <ReportConfigFormFields
          :focus-word-rows="focusWordRows"
          :scenario-groups="scenarioGroups"
          :market-rows="marketRows"
          @add-focus="addFocusRow"
          @remove-focus="removeFocusRow"
          @add-scenario="addScenarioRow"
          @remove-scenario="removeScenarioRow"
          @add-market="addMarketRow"
          @remove-market="removeMarketRow"
        />

        <details class="rc-advanced" @toggle="onAdvancedJsonToggle">
          <summary>高级：用 JSON 编辑（一般不需要）</summary>
          <p class="rc-help">
            打开时会根据上面表单生成内容；改完后点「写回表单」再保存。可在此加入
            <code>llm_comment_sentiment</code>、<code>llm_section_bridges</code>、<code>llm_matrix_group_summaries</code>
            等布尔字段（须为 <code>true</code>/<code>false</code>）。页顶「重新生成报告」默认已使用
            <code>generator:&quot;llm&quot;</code>；若只要规则稿请勾选「本次仅用规则引擎」。
          </p>
          <textarea v-model="advancedJsonText" class="report-config-editor" rows="10" spellcheck="false" />
          <button type="button" class="ma-btn ma-btn-secondary rc-add" @click="applyAdvancedJsonToForm">将 JSON 写回表单</button>
        </details>

        <p v-if="reportConfigErr" class="ma-err">{{ reportConfigErr }}</p>
      </div>

      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        本任务输出目录：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>

      <p v-if="regenErr" class="ma-err">{{ regenErr }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>
    </section>
  </div>
</template>

<style scoped>
.hint-top {
  margin: 0 0 1rem;
  font-size: 0.88rem;
  color: #4b5563;
  line-height: 1.5;
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
  margin-bottom: 0.5rem;
}
.chk-rules-only {
  width: auto;
  max-width: 100%;
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
.run-dir-note {
  margin: 0.85rem 0 0;
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
.report-config-block {
  margin: 1rem 0 0;
  padding: 1rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fafafa;
}
.report-config-title {
  margin: 0 0 0.35rem;
  font-size: 1rem;
  font-weight: 600;
  color: #1f2937;
}
.report-config-hint {
  margin-bottom: 0.65rem;
}
.report-config-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-bottom: 0.65rem;
}
.report-config-editor {
  width: 100%;
  box-sizing: border-box;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.78rem;
  line-height: 1.45;
  padding: 0.65rem 0.75rem;
  border-radius: 6px;
  border: 1px solid #d1d5db;
  resize: vertical;
  min-height: 200px;
  margin: 0.5rem 0;
}
.rc-advanced {
  margin-top: 1rem;
  padding-top: 0.75rem;
  border-top: 1px dashed #d1d5db;
}
.rc-advanced summary {
  cursor: pointer;
  font-size: 0.85rem;
  color: #6b7280;
  user-select: none;
}
.rc-advanced[open] summary {
  margin-bottom: 0.5rem;
}
.rc-help {
  margin: 0 0 0.65rem;
  font-size: 0.82rem;
  color: #6b7280;
  line-height: 1.5;
}
.rc-add {
  margin-top: 0.5rem;
  font-size: 0.85rem;
}
</style>
