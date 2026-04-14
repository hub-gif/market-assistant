<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import MarkdownPreview from '../../components/MarkdownPreview.vue'

/** 将结构化摘要转为非技术用户可读的条目（不出现 cr1 等字段名）。 */
function pctShare(x) {
  if (x == null || x === '') return '—'
  const n = Number(x)
  if (Number.isNaN(n)) return '—'
  return `${(n * 100).toFixed(1)}%`
}

function briefHumanSummary(j) {
  const rows = []
  if (!j || typeof j !== 'object') return rows
  if (j.keyword) rows.push({ label: '搜索关键词', value: String(j.keyword) })
  if (j.batch_label) rows.push({ label: '批次', value: String(j.batch_label) })
  const sc = j.scope
  if (sc && typeof sc === 'object') {
    if (sc.merged_sku_count != null)
      rows.push({ label: '深入采集的商品款数（SKU）', value: String(sc.merged_sku_count) })
    if (sc.comment_flat_rows != null)
      rows.push({ label: '评价条数', value: String(sc.comment_flat_rows) })
    if (sc.structure_source_rows != null)
      rows.push({ label: '列表/结构统计所用行数', value: String(sc.structure_source_rows) })
    if (sc.uses_pc_search_list_export === true)
      rows.push({ label: '是否含搜索列表全量', value: '是' })
  }
  const conc = j.concentration
  if (conc && typeof conc === 'object') {
    const shops = conc.shops_from_list
    if (shops && typeof shops === 'object') {
      if (shops.top_label && shops.cr1 != null) {
        rows.push({
          label: '第一大店铺（占列表行比例）',
          value: `${pctShare(shops.cr1)} · ${shops.top_label}`,
        })
      }
      if (shops.cr3 != null) {
        rows.push({
          label: '前三大店铺合计（占列表行比例）',
          value: pctShare(shops.cr3),
        })
      }
    }
    const lb = conc.list_brand_field
    if (lb && typeof lb === 'object') {
      if (lb.top_label && lb.cr1 != null) {
        rows.push({
          label: '第一大品牌（列表侧，按行）',
          value: `${pctShare(lb.cr1)} · ${lb.top_label}`,
        })
      }
      if (lb.cr3 != null) {
        rows.push({
          label: '前三大品牌合计（列表侧）',
          value: pctShare(lb.cr3),
        })
      }
    }
    const db = conc.detail_brand_among_merged
    if (db && typeof db === 'object') {
      if (db.top_label && db.cr1 != null) {
        rows.push({
          label: '第一大品牌（深入样本）',
          value: `${pctShare(db.cr1)} · ${db.top_label}`,
        })
      }
      if (db.cr3 != null) {
        rows.push({
          label: '前三大品牌合计（深入样本）',
          value: pctShare(db.cr3),
        })
      }
    }
  }
  const p = j.price_stats
  if (p && typeof p === 'object' && p.n > 0) {
    rows.push({ label: '价格统计·样本量', value: String(p.n) })
    if (p.median != null)
      rows.push({ label: '价格统计·中位数（元）', value: Number(p.median).toFixed(2) })
    if (p.mean != null)
      rows.push({ label: '价格统计·平均（元）', value: Number(p.mean).toFixed(2) })
  }
  const src = j.price_stats_source
  if (src === 'pc_search_export_all_rows')
    rows.push({ label: '价格统计·数据来源', value: '搜索列表全量' })
  else if (src === 'keyword_pipeline_merged')
    rows.push({ label: '价格统计·数据来源', value: '深入采集合并表' })
  return rows
}
import {
  refreshJobs,
  useJobs,
  downloadUrl,
  api,
  previewUrl,
  jobCompetitorBriefUrl,
  downloadCompetitorBriefPack,
  exportReportDocument,
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
const briefData = ref(null)
const briefErr = ref('')
const briefCopyOk = ref(false)
const packErr = ref('')
const exportDocErr = ref('')
/** 正在导出的格式：docx | pdf | null */
const exportDocFmt = ref(null)

async function exportReportFmt(fmt) {
  const id = selectedId.value
  if (!id) return
  exportDocErr.value = ''
  exportDocFmt.value = fmt
  try {
    await exportReportDocument(id, fmt)
  } catch (e) {
    exportDocErr.value = String(e?.message || e)
  } finally {
    exportDocFmt.value = null
  }
}

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
  return genInFlight.value.includes(`${prefix}${id}`)
}
const loading = computed(() => genKeyMatches(K_PREVIEW))
const briefLoading = computed(() => genKeyMatches(K_BRIEF))
const packLoading = computed(() => genKeyMatches(K_PACK))
const viewInFlightOtherJobId = computed(() => {
  const sid = selectedId.value
  if (!sid) return null
  for (const k of genInFlight.value) {
    const i = k.lastIndexOf(':')
    if (i < 0) continue
    const jid = k.slice(i + 1)
    if (jid && jid !== sid) return jid
  }
  return null
})

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
)

const briefHumanRows = computed(() => briefHumanSummary(briefData.value))

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
  briefData.value = null
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
          briefErr.value = text || `请求失败（${r.status}）`
        }
        return
      }
      const j = JSON.parse(text)
      briefData.value = j
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
  briefData.value = null
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
        若开启大模型，系统会在后台根据评价正文补充<strong>关注词</strong>与<strong>使用场景</strong>标签，并生成报告中的统计图（与报告插图章节对应）。
        <strong>一键下载简报包</strong>内含：报告正文、插图文件夹、机器整理的<strong>数据摘要</strong>、以及便于扫读的<strong>要点摘录</strong>。
        需要改分析规则或重新出稿，请至
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
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!selectedId || exportDocFmt || loading"
          @click="exportReportFmt('docx')"
        >
          {{ exportDocFmt === 'docx' ? '导出中…' : '导出 Word' }}
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!selectedId || exportDocFmt || loading"
          @click="exportReportFmt('pdf')"
        >
          {{ exportDocFmt === 'pdf' ? '导出中…' : '导出 PDF' }}
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!selectedId || briefLoading || loading"
          title="加载与报告数字一致的数据摘要（可先读易读版，再展开原始格式）"
          @click="loadCompetitorBrief"
        >
          {{ briefLoading ? '摘要加载中…' : '加载数据摘要' }}
        </button>
        <button
          type="button"
          class="ma-btn ma-btn-primary"
          :disabled="!selectedId || packLoading || loading || briefLoading"
          title="下载压缩包：报告、配图、数据与说明"
          @click="downloadBriefPack"
        >
          {{ packLoading ? '打包中…' : '一键下载简报包' }}
        </button>
      </div>
      <p v-if="viewInFlightOtherJobId" class="ma-warn-banner">
        任务 #{{ viewInFlightOtherJobId }} 仍有请求进行中；当前页切换任务后若按钮已恢复，请等待该任务完成或返回对应任务查看。
      </p>

      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        本任务在本机上的结果文件夹（表格明细可在「库内数据浏览」查看）：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>

      <p v-if="briefErr" class="ma-err">{{ briefErr }}</p>
      <p v-if="packErr" class="ma-err">{{ packErr }}</p>
      <p v-if="exportDocErr" class="ma-err">{{ exportDocErr }}</p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先在「搜索采集」跑通一条流水线。</p>
    </section>

    <section v-if="briefData" class="ma-card preview-card">
      <div class="preview-head">
        <h2>竞品数据摘要（机器整理）</h2>
        <div class="tabs">
          <button type="button" class="ma-btn ma-btn-secondary brief-tool" @click="copyBriefJson">
            {{ briefCopyOk ? '已复制' : '复制原始数据' }}
          </button>
          <button type="button" class="ma-btn ma-btn-secondary brief-tool" @click="downloadBriefJson">下载数据文件</button>
        </div>
      </div>
      <p class="hint-top brief-hint">
        以下数字与上方报告一致，用日常用语列出；需要交给其它系统或技术人员时，可展开下方「原始数据」或复制/下载。
      </p>
      <dl v-if="briefHumanRows.length" class="brief-dl">
        <template v-for="(row, idx) in briefHumanRows" :key="idx">
          <dt>{{ row.label }}</dt>
          <dd>{{ row.value }}</dd>
        </template>
      </dl>
      <p v-else class="ma-muted brief-hint">暂无摘要条目（可能缺少列表或品牌字段）。</p>
      <details class="brief-raw-wrap">
        <summary>展开原始数据（机器可读格式）</summary>
        <pre class="raw-md brief-json">{{ briefJson }}</pre>
      </details>
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
.brief-dl {
  margin: 0.5rem 0 1rem;
  display: grid;
  grid-template-columns: minmax(10rem, 38%) 1fr;
  gap: 0.35rem 1rem;
  font-size: 0.9rem;
  line-height: 1.45;
}
.brief-dl dt {
  margin: 0;
  font-weight: 600;
  color: #374151;
}
.brief-dl dd {
  margin: 0;
  color: #1f2937;
  word-break: break-word;
}
.brief-raw-wrap {
  margin-top: 0.75rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 0.5rem 0.75rem;
  background: #fafafa;
}
.brief-raw-wrap summary {
  cursor: pointer;
  font-size: 0.88rem;
  color: #4b5563;
  user-select: none;
}
.brief-raw-wrap .brief-json {
  margin-top: 0.75rem;
}
</style>
