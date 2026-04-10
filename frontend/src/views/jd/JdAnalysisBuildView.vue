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
const useLlm = ref(false)
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
        body: JSON.stringify({ generator: useLlm.value ? 'llm' : 'rules' }),
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
        选择<strong>已成功</strong>的任务，调整报告统计规则后保存。<strong>未勾选</strong>下方选项时，按固定统计规则生成报告；<strong>勾选「使用大模型生成」</strong>后，由大模型根据本批次摘要撰写全文（通常更慢且可能计费）。均不重新爬取。
        阅读与下载请至
        <RouterLink to="/jd/analysis-view">报告查看</RouterLink>。
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
          :disabled="!selectedId || regenBusyThisTask"
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
          下面三项都<strong>可以不改</strong>：留空并保存，表示沿用系统内置规则。请先点「保存以上设置」，再点「重新生成报告」（需要大模型时先勾选页面上方对应选项）。
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
          <p class="rc-help">打开时会根据上面表单生成内容；改完后点「写回表单」再保存。</p>
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
