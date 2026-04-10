<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import MarkdownPreview from '../../components/MarkdownPreview.vue'
import { refreshJobs, useJobs, exportStrategyDocument } from '../../composables/useJobs'

const route = useRoute()
const router = useRouter()
const { jobs } = useJobs()

const selectedId = ref('')
const draftMd = ref('')
const draftMeta = ref(null)
const viewMode = ref('render')
const exportErr = ref('')
const exportBusy = ref(false)

const STORAGE_KEY = (id) => `ma_strategy_draft_${id}`

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
)

function loadDraft() {
  const id = selectedId.value
  if (!id) {
    draftMd.value = ''
    draftMeta.value = null
    return
  }
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY(id))
    if (!raw) {
      draftMd.value = ''
      draftMeta.value = null
      return
    }
    const o = JSON.parse(raw)
    draftMd.value = o.markdown || ''
    draftMeta.value = {
      keyword: o.keyword || '',
      generated_at: o.generated_at || '',
    }
  } catch {
    draftMd.value = ''
    draftMeta.value = null
  }
}

async function loadList() {
  try {
    await refreshJobs()
  } catch {
    /* ignore */
  }
}

function downloadDraftMd() {
  if (!draftMd.value || !selectedId.value) return
  const blob = new Blob([draftMd.value], { type: 'text/markdown;charset=utf-8' })
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = `job_${selectedId.value}_strategy_draft.md`
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

async function exportStrategyFmt(fmt) {
  if (!draftMd.value || !selectedId.value) return
  exportErr.value = ''
  exportBusy.value = true
  try {
    await exportStrategyDocument(selectedId.value, draftMd.value, fmt)
  } catch (e) {
    exportErr.value = String(e)
  } finally {
    exportBusy.value = false
  }
}

function goBuildSameJob() {
  const id = selectedId.value
  if (id) {
    router.push({ path: '/jd/strategy-build', query: { job: id } })
  } else {
    router.push('/jd/strategy-build')
  }
}

function syncSelectionFromRouteAndJobs() {
  if (route.query.job) {
    selectedId.value = String(route.query.job)
    return
  }
  if (!selectedId.value && successJobs.value.length) {
    selectedId.value = String(successJobs.value[0].id)
  }
}

onMounted(async () => {
  await loadList()
  syncSelectionFromRouteAndJobs()
  loadDraft()
})

watch(
  () => route.query.job,
  (j) => {
    if (!j) return
    const s = String(j)
    if (s !== selectedId.value) {
      selectedId.value = s
      loadDraft()
    }
  },
)

watch(selectedId, (id) => {
  loadDraft()
  const want = id ? String(id) : ''
  if (String(route.query.job || '') !== want) {
    router.replace({ path: '/jd/strategy-view', query: want ? { job: want } : {} })
  }
})

watch(successJobs, (list) => {
  if (selectedId.value) return
  if (route.query.job) return
  if (list.length) {
    selectedId.value = String(list[0].id)
    loadDraft()
  }
})
</script>

<template>
  <div>
    <section class="ma-card">
      <h2>策略稿预览</h2>
      <p class="hint-top">
        选择在<strong>策略生成</strong>页已生成过的任务查看文稿（保存在本浏览器会话内）。需要改决策请回到
        <RouterLink to="/jd/strategy-build">策略生成</RouterLink>
        重新提交。分析数据见
        <RouterLink to="/jd/analysis-view">报告查看</RouterLink>。
      </p>

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
          class="ma-btn ma-btn-secondary"
          :disabled="!draftMd"
          @click="downloadDraftMd"
        >
          下载文稿
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!draftMd || !selectedId || exportBusy"
          @click="exportStrategyFmt('docx')"
        >
          {{ exportBusy ? '导出中…' : '导出 Word' }}
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!draftMd || !selectedId || exportBusy"
          @click="exportStrategyFmt('pdf')"
        >
          导出 PDF
        </button>
        <button type="button" class="ma-btn ma-btn-primary" @click="goBuildSameJob">
          去策略生成
        </button>
      </div>

      <p v-if="draftMeta?.generated_at" class="meta-line ma-muted">
        生成时间：{{ draftMeta.generated_at }}
        <template v-if="draftMeta.keyword"> · 关键词：{{ draftMeta.keyword }}</template>
      </p>
      <p v-if="exportErr" class="ma-err">{{ exportErr }}</p>
      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        任务目录：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务。</p>
      <p v-else-if="selectedId && !draftMd" class="ma-muted empty-hint">
        当前任务尚无已生成的策略稿。请先在「策略生成」填写并点击「生成并前往预览」。
      </p>
    </section>

    <section v-if="draftMd" class="ma-card preview-card">
      <div class="preview-head">
        <h2>预览</h2>
        <div class="tabs">
          <button type="button" :class="{ on: viewMode === 'render' }" @click="viewMode = 'render'">
            渲染
          </button>
          <button type="button" :class="{ on: viewMode === 'raw' }" @click="viewMode = 'raw'">
            原文
          </button>
        </div>
      </div>
      <div v-if="viewMode === 'render'" class="md-box">
        <MarkdownPreview :source="draftMd" />
      </div>
      <pre v-else class="raw-md">{{ draftMd }}</pre>
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
.meta-line {
  margin: 0.5rem 0 0;
  font-size: 0.82rem;
}
.run-dir-note {
  margin: 0.5rem 0 0;
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
.empty-hint {
  margin-top: 0.75rem;
  line-height: 1.5;
}
.ma-err {
  color: #b91c1c;
  font-size: 0.9rem;
  margin: 0.5rem 0 0;
}
.preview-card {
  margin-top: 1rem;
}
.preview-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
}
.preview-head h2 {
  margin: 0;
}
.tabs {
  display: flex;
  gap: 0.35rem;
}
.tabs button {
  border: 1px solid #e5e7eb;
  background: #f9fafb;
  padding: 0.35rem 0.85rem;
  border-radius: 6px;
  font-size: 0.85rem;
  cursor: pointer;
  color: #4b5563;
}
.tabs button.on {
  background: #2563eb;
  border-color: #2563eb;
  color: #fff;
}
.md-box {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  max-height: min(75vh, 920px);
  overflow: auto;
  background: #fff;
}
.raw-md {
  margin: 0;
  max-height: min(75vh, 920px);
  overflow: auto;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.78rem;
  line-height: 1.5;
  white-space: pre-wrap;
  padding: 1rem;
  background: #fafafa;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
}
</style>
