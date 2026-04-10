<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import MarkdownPreview from '../../components/MarkdownPreview.vue'
import {
  refreshJobs,
  useJobs,
  downloadUrl,
  api,
  previewUrl,
  jobCompetitorBriefUrl,
  downloadCompetitorBriefPack,
  jobExportReportDocumentUrl,
} from '../../composables/useJobs'
import {
  generationInFlightKey,
  withGenerationInFlight,
} from '../../composables/useGenerationInFlight'

const { jobs } = useJobs()
const selectedId = ref('')
const reportMd = ref('')
const err = ref('')
const viewMode = ref('render')
const briefJson = ref('')
const briefErr = ref('')
const briefCopyOk = ref(false)
const packErr = ref('')

/** 将 Markdown 中的 report_assets 相对路径转为可访问的 API URL（在线预览插图） */
function reportMdWithAssetUrls(md, jobId) {
  if (!md || !jobId) return md
  return md.replace(/\]\((report_assets\/[^)]+)\)/g, (_, rel) => {
    const q = encodeURIComponent(rel)
    return `](/api/jobs/${jobId}/report-asset/?path=${q})`
  })
}

const reportMdForPreview = computed(() =>
  reportMdWithAssetUrls(reportMd.value, selectedId.value),
)

const genInFlight = generationInFlightKey()
const K_PREVIEW = 'preview-report:'
const K_BRIEF = 'competitor-brief:'
const K_PACK = 'brief-pack:'
function genKeyMatches(prefix) {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value === `${prefix}${id}`
}
const loading = computed(() => genKeyMatches(K_PREVIEW))
const briefLoading = computed(() => genKeyMatches(K_BRIEF))
const packLoading = computed(() => genKeyMatches(K_PACK))
const viewInFlightOtherJobId = computed(() => {
  const k = genInFlight.value
  if (!k) return null
  const i = k.lastIndexOf(':')
  if (i < 0) return null
  const jid = k.slice(i + 1)
  if (jid === selectedId.value) return null
  return jid
})

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
)

async function loadList() {
  try {
    await refreshJobs()
  } catch {
    /* ignore */
  }
}

async function loadReport() {
  reportMd.value = ''
  err.value = ''
  const id = selectedId.value
  if (!id) return
  await withGenerationInFlight(`${K_PREVIEW}${id}`, async () => {
    try {
      const r = await api(previewUrl(id, 'report'))
      if (!r.ok) {
        const t = await r.text()
        err.value = t
        if (r.status === 404) {
          err.value =
            (t && t.length < 400 ? t : '报告文件不存在。') +
            ' 若数据已在批次目录中，可到「报告生成」页点击「重新生成报告」（不重新爬取）。'
        }
        return
      }
      reportMd.value = await r.text()
    } catch (e) {
      err.value = String(e)
    }
  })
}

async function loadCompetitorBrief() {
  briefJson.value = ''
  briefErr.value = ''
  briefCopyOk.value = false
  const id = selectedId.value
  if (!id) return
  await withGenerationInFlight(`${K_BRIEF}${id}`, async () => {
    try {
      const r = await api(jobCompetitorBriefUrl(id))
      const text = await r.text()
      if (!r.ok) {
        try {
          const j = JSON.parse(text)
          briefErr.value = j.detail || text
        } catch {
          briefErr.value = text || `HTTP ${r.status}`
        }
        return
      }
      const j = JSON.parse(text)
      briefJson.value = JSON.stringify(j, null, 2)
    } catch (e) {
      briefErr.value = String(e)
    }
  })
}

async function copyBriefJson() {
  if (!briefJson.value) return
  try {
    await navigator.clipboard.writeText(briefJson.value)
    briefCopyOk.value = true
    setTimeout(() => {
      briefCopyOk.value = false
    }, 2000)
  } catch {
    briefErr.value = '复制失败（浏览器权限）'
  }
}

function downloadBriefJson() {
  if (!briefJson.value || !selectedId.value) return
  const blob = new Blob([briefJson.value], { type: 'application/json;charset=utf-8' })
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = `job_${selectedId.value}_structured_summary.json`
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

async function downloadBriefPack() {
  const id = selectedId.value
  if (!id) return
  packErr.value = ''
  await withGenerationInFlight(`${K_PACK}${id}`, async () => {
    try {
      await downloadCompetitorBriefPack(id)
    } catch (e) {
      packErr.value = String(e)
    }
  })
}

onMounted(loadList)

watch(selectedId, async () => {
  briefJson.value = ''
  briefErr.value = ''
  packErr.value = ''
  const id = selectedId.value
  if (!id) return
  try {
    const r = await api(`/api/jobs/${id}/`)
    if (r.ok) {
      const j = await r.json()
      const idx = jobs.value.findIndex((x) => x.id === j.id)
      if (idx >= 0) jobs.value[idx] = j
    }
  } catch {
    /* ignore */
  }
  loadReport()
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
      <h2>分析报告查看</h2>
      <p class="hint-top">
        选择<strong>已成功</strong>的任务，在线阅读报告或下载。
        流水线生成报告时会<strong>自动</strong>基于<strong>全部评价正文</strong>分块调用大模型扩展关注词，并写入统计图（PNG，见「二点五」章与简报包 <code>report_assets</code>）。
        <strong>一键下载简报包</strong>含报告稿、统计图、结构化 JSON、要点摘录。
        需要改规则或重算，请至
        <RouterLink to="/jd/analysis-build">报告生成</RouterLink>。
      </p>

      <div class="toolbar">
        <label class="sel-label">任务</label>
        <select v-model="selectedId" class="job-select">
          <option value="" disabled>请选择任务</option>
          <option v-for="j in successJobs" :key="j.id" :value="String(j.id)">
            #{{ j.id }} · {{ j.keyword }} · {{ j.run_dir?.split(/[/\\]/).pop() || '' }}
          </option>
        </select>
        <button type="button" class="ma-btn ma-btn-secondary" :disabled="!selectedId || loading" @click="loadReport">
          {{ loading ? '加载中…' : '重新加载报告' }}
        </button>
        <a
          class="ma-btn ma-btn-secondary dl-link"
          :class="{ disabled: !selectedId }"
          :href="selectedId ? downloadUrl(selectedId, 'report') : '#'"
          target="_blank"
          rel="noreferrer"
          @click="(e) => { if (!selectedId) e.preventDefault() }"
        >
          下载报告
        </a>
        <a
          class="ma-btn ma-btn-secondary dl-link"
          :class="{ disabled: !selectedId }"
          :href="selectedId ? jobExportReportDocumentUrl(selectedId, 'docx') : '#'"
          target="_blank"
          rel="noreferrer"
          @click="(e) => { if (!selectedId) e.preventDefault() }"
        >
          导出 Word
        </a>
        <a
          class="ma-btn ma-btn-secondary dl-link"
          :class="{ disabled: !selectedId }"
          :href="selectedId ? jobExportReportDocumentUrl(selectedId, 'pdf') : '#'"
          target="_blank"
          rel="noreferrer"
          @click="(e) => { if (!selectedId) e.preventDefault() }"
        >
          导出 PDF
        </a>
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!selectedId || briefLoading || loading"
          title="生成与报告相同统计口径的结构化数据"
          @click="loadCompetitorBrief"
        >
          {{ briefLoading ? '摘要加载中…' : '加载结构化摘要' }}
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-primary"
          :disabled="!selectedId || packLoading || loading || briefLoading"
          title="ZIP：报告稿、结构化数据、要点摘录、说明"
          @click="downloadBriefPack"
        >
          {{ packLoading ? '打包中…' : '一键下载简报包' }}
        </button>
      </div>
      <p v-if="viewInFlightOtherJobId" class="ma-warn-banner">
        任务 #{{ viewInFlightOtherJobId }} 仍有请求进行中；当前页切换任务后若按钮已恢复，请等待该任务完成或返回对应任务查看。
      </p>

      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        本任务输出目录（原始表格复核请至「库内数据浏览」）：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>

      <p v-if="briefErr" class="ma-err">{{ briefErr }}</p>
      <p v-if="packErr" class="ma-err">{{ packErr }}</p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>
    </section>

    <section v-if="briefJson" class="ma-card preview-card">
      <div class="preview-head">
        <h2>结构化竞品摘要</h2>
        <div class="tabs">
          <button type="button" class="ma-btn ma-btn-secondary brief-tool" @click="copyBriefJson">
            {{ briefCopyOk ? '已复制' : '复制' }}
          </button>
          <button type="button" class="ma-btn ma-btn-secondary brief-tool" @click="downloadBriefJson">下载文件</button>
        </div>
      </div>
      <p class="hint-top brief-hint">与上方报告统计口径一致的数据汇总，可复制或下载给其它工具使用。</p>
      <pre class="raw-md brief-json">{{ briefJson }}</pre>
    </section>

    <section v-if="reportMd" class="ma-card preview-card">
      <div class="preview-head">
        <h2>预览</h2>
        <div class="tabs">
          <button type="button" :class="{ on: viewMode === 'render' }" @click="viewMode = 'render'">渲染</button>
          <button type="button" :class="{ on: viewMode === 'raw' }" @click="viewMode = 'raw'">原文</button>
        </div>
      </div>
      <div v-if="viewMode === 'render'" class="md-box">
        <MarkdownPreview :source="reportMdForPreview" />
      </div>
      <pre v-else class="raw-md">{{ reportMd }}</pre>
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
.dl-link {
  text-decoration: none;
  display: inline-flex;
  align-items: center;
  box-sizing: border-box;
}
.dl-link.disabled {
  pointer-events: none;
  opacity: 0.5;
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
  white-space: pre;
  padding: 1rem;
  background: #fafafa;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
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
.brief-tool {
  font-size: 0.85rem;
  padding: 0.35rem 0.75rem;
}
.brief-hint {
  margin-top: -0.25rem;
}
.brief-json {
  max-height: min(50vh, 560px);
}
</style>
