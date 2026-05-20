<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import {
  clearRegenerateReportInFlightOnly,
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'
import { RouterLink } from 'vue-router'
import ReportConfigFormFields from '../../components/ReportConfigFormFields.vue'
import { refreshJobs, useJobs, api, reportConfigDefaultsUrl } from '../../composables/useJobs'
import { useJobStore } from '../../stores/jobs'
import { useReportConfigForm } from '../../composables/useReportConfigForm'

const { jobs } = useJobs()
const selectedId = ref('')
/** 勾选则本次只出规则统计稿（仍先跑规则落盘，不做全文智能润色） */
const useRulesOnly = ref(false)
const regenErr = ref('')
const genInFlight = generationInFlightKey()
const REGEN_PREFIX = 'regenerate-report:'
const regenPendingJobId = computed(() => {
  for (const k of genInFlight.value) {
    if (k.startsWith(REGEN_PREFIX)) return k.slice(REGEN_PREFIX.length)
  }
  return null
})
const regenBusyThisTask = computed(
  () => regenPendingJobId.value != null && regenPendingJobId.value === selectedId.value,
)
/** 任意任务正在重新生成时都应禁用按钮，避免切换页签后 selectedId 被重置导致误判可点 */
const regenBusyAny = computed(() => regenPendingJobId.value != null)
const regenBusyOtherTask = computed(
  () => regenPendingJobId.value != null && regenPendingJobId.value !== selectedId.value,
)

const { marketRows, applyFromApiConfig, buildPayload, addMarketRow, removeMarketRow } =
  useReportConfigForm()

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
    useJobStore().mergeJob(updated)
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

function clearLocalRegenLock() {
  regenErr.value = ''
  clearRegenerateReportInFlightOnly()
}

async function regenerateReport() {
  const id = selectedId.value
  if (!id) return
  regenErr.value = ''
  const key = `${REGEN_PREFIX}${id}`
  try {
    await withGenerationInFlight(key, async () => {
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
      useJobStore().mergeJob(updated)
    })
  } catch (e) {
    regenErr.value = String(e)
  }
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
      useJobStore().mergeJob(j)
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
      <h2>报告生成</h2>
      <p class="hint-top">
        成功任务 → 保存设置 → 重新生成；不抓数。成稿见
        <RouterLink to="/jd/analysis-view">报告预览</RouterLink>；细则用「推荐示例」或「高级选项」。
      </p>

      <div class="toolbar">
        <label class="chk-inline chk-rules-only">
          <input v-model="useRulesOnly" type="checkbox" />
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
              :label="`${j.id} · ${j.keyword}`"
              :value="String(j.id)"
            />
          </el-select>
        </div>
        <el-button
          type="primary"
          :disabled="!selectedId || regenBusyAny"
          title="不重新抓数，仅重算本批次报告"
          @click="regenerateReport"
        >
          {{ regenBusyThisTask ? '生成中…' : '重新生成报告' }}
        </el-button>
        <el-button
          v-if="regenBusyAny"
          title="仅清本页「生成中」标记；后端若仍在跑请勿点"
          @click="clearLocalRegenLock"
        >
          清除误锁（本地）
        </el-button>
      </div>
      <p v-if="!successJobs.length" class="hint-top">
        尚无成功任务，请先在「任务」里跑通一次采集。
      </p>
      <p v-else-if="regenBusyAny" class="hint-top">
        本页正记录「生成中」。若已结束可点「清除误锁（本地）」后重试。
      </p>
      <p v-if="regenBusyOtherTask" class="ma-warn-banner">
        任务 {{ regenPendingJobId }} 生成中，请稍候。
      </p>

      <div v-if="selectedId" class="report-config-block">
        <h3 class="report-config-title">报告配置</h3>
        <div class="report-config-actions">
          <el-button
            :disabled="reportConfigDefaultsLoading"
            @click="loadReportConfigDefaults"
          >
            {{ reportConfigDefaultsLoading ? '加载中…' : '填入推荐示例' }}
          </el-button>
          <el-button
            type="primary"
            :disabled="reportConfigSaveLoading"
            @click="saveReportConfigToJob"
          >
            {{ reportConfigSaveLoading ? '保存中…' : '保存以上设置' }}
          </el-button>
        </div>

        <ReportConfigFormFields
          :market-rows="marketRows"
          @add-market="addMarketRow"
          @remove-market="removeMarketRow"
        />

        <details class="rc-advanced" @toggle="onAdvancedJsonToggle">
          <summary>高级选项</summary>
          <p class="rc-help">与表单同步；改后先写回再保存。纯规则稿勾页顶。</p>
          <textarea v-model="advancedJsonText" class="report-config-editor" rows="10" spellcheck="false" />
          <el-button class="rc-add" @click="applyAdvancedJsonToForm">将配置写回表单</el-button>
        </details>

        <p v-if="reportConfigErr" class="ma-err">{{ reportConfigErr }}</p>
      </div>

      <p v-if="regenErr" class="ma-err">{{ regenErr }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先完成一次采集。</p>
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
.toolbar-task-select-wrap {
  flex: 1 1 auto;
  min-width: 10rem;
  max-width: 20rem;
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
