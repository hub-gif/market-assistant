<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import MarkdownPreview from '../../components/MarkdownPreview.vue'

/** 将结构化摘要转为非技术用户可读的条目（不出现 cr1 等字段名）。 */
function pctShare(x) {
  if (x == null || x === '') return '—'
  const n = Number(x)
  if (Number.isNaN(n)) return '—'
  return `${(n * 100).toFixed(1)}%`
}

/** 集中度块：新键 first_share / top_three_combined_share，旧键 cr1 / cr3 */
function concShare(block, key) {
  if (!block || typeof block !== 'object') return null
  const v0 = key === 'first' ? block.first_share : block.top_three_combined_share
  if (v0 != null && v0 !== '') return v0
  const legacy = key === 'first' ? block.cr1 : block.cr3
  return legacy != null && legacy !== '' ? legacy : null
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
      const cr1 = concShare(shops, 'first')
      const cr3 = concShare(shops, 'top3')
      if (shops.top_label && cr1 != null) {
        rows.push({
          label: '第一大店铺（占列表行比例）',
          value: `${pctShare(cr1)} · ${shops.top_label}`,
        })
      }
      if (cr3 != null) {
        rows.push({
          label: '前三大店铺合计（占列表行比例）',
          value: pctShare(cr3),
        })
      }
      const usb = shops.unique_sku_basis
      if (usb && typeof usb === 'object' && usb.n_unique_skus != null) {
        const u1 = concShare(usb, 'first')
        const u3 = concShare(usb, 'top3')
        if (usb.top_label && u1 != null) {
          rows.push({
            label: '第一大店铺（占去重 SKU 比例）',
            value: `${pctShare(u1)} · ${usb.top_label} · 共 ${usb.n_unique_skus} 个 SKU`,
          })
        }
        if (u3 != null) {
          rows.push({
            label: '前三大店铺合计（占去重 SKU）',
            value: pctShare(u3),
          })
        }
      }
    }
    const lb = conc.list_brand_field
    if (lb && typeof lb === 'object') {
      const l1 = concShare(lb, 'first')
      const l3 = concShare(lb, 'top3')
      if (lb.top_label && l1 != null) {
        rows.push({
          label: '第一大品牌（列表侧，按行）',
          value: `${pctShare(l1)} · ${lb.top_label}`,
        })
      }
      if (l3 != null) {
        rows.push({
          label: '前三大品牌合计（列表侧）',
          value: pctShare(l3),
        })
      }
    }
    const db = conc.detail_brand_among_merged
    if (db && typeof db === 'object') {
      const d1 = concShare(db, 'first')
      const d3 = concShare(db, 'top3')
      if (db.top_label && d1 != null) {
        rows.push({
          label: '第一大品牌（深入样本）',
          value: `${pctShare(d1)} · ${db.top_label}`,
        })
      }
      if (d3 != null) {
        rows.push({
          label: '前三大品牌合计（深入样本）',
          value: pctShare(d3),
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
import {
  analysisBriefCacheKey,
  analysisReportCacheKey,
  persistAnalysisBrief,
  persistAnalysisReportMd,
} from '../../lib/analysisViewStorage'
import { useJobStore } from '../../stores/jobs'

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

const genInFlight = generationInFlightKey()
const K_PREVIEW = 'preview-report:'
const K_BRIEF = 'competitor-brief:'
const K_PACK = 'brief-pack:'
const K_EXPORT = 'export-report:'

function isExporting(fmt) {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value.includes(`${K_EXPORT}${id}:${fmt}`)
}

async function exportReportFmt(fmt) {
  const id = selectedId.value
  if (!id) return
  exportDocErr.value = ''
  await withGenerationInFlight(`${K_EXPORT}${id}:${fmt}`, async () => {
    try {
      await exportReportDocument(id, fmt)
    } catch (e) {
      exportDocErr.value = String(e?.message || e)
    }
  })
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
    let jid = null
    if (k.startsWith(K_PREVIEW)) jid = k.slice(K_PREVIEW.length)
    else if (k.startsWith(K_BRIEF)) jid = k.slice(K_BRIEF.length)
    else if (k.startsWith(K_PACK)) jid = k.slice(K_PACK.length)
    else if (k.startsWith(K_EXPORT)) {
      const rest = k.slice(K_EXPORT.length)
      const m = /^(\d+):/.exec(rest)
      if (m) jid = m[1]
    }
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
            (t && t.length < 400 ? t : '报告文件不存在。') + ' 可在「报告生成」重算（不重新抓数）。'
        }
        return
      }
      const text = await r.text()
      reportMd.value = text
      persistAnalysisReportMd(id, text)
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
      persistAnalysisBrief(id, j)
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

function onAnalysisViewStorage(ev) {
  if (!ev.key || ev.storageArea !== localStorage) return
  const sid = selectedId.value
  if (!sid) return
  if (ev.key === analysisReportCacheKey(sid) && ev.newValue != null) {
    reportMd.value = ev.newValue
  }
  if (ev.key === analysisBriefCacheKey(sid) && ev.newValue) {
    try {
      const j = JSON.parse(ev.newValue)
      briefData.value = j
      briefJson.value = JSON.stringify(j, null, 2)
    } catch {
      /* ignore */
    }
  }
}

onMounted(() => {
  loadList()
  window.addEventListener('storage', onAnalysisViewStorage)
})
onUnmounted(() => {
  window.removeEventListener('storage', onAnalysisViewStorage)
})

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
      useJobStore().mergeJob(j)
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
      <h2>报告预览</h2>
      <p class="hint-top">
        选成功任务即可阅读、导出或打包。改规则或重出稿见
        <RouterLink to="/jd/analysis-build">报告生成</RouterLink>。
      </p>

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
        <el-button :disabled="!selectedId || loading" @click="loadReport">
          {{ loading ? '加载中…' : '重新加载报告' }}
        </el-button>
        <el-button
          tag="a"
          rel="noreferrer"
          target="_blank"
          :href="selectedId ? downloadUrl(selectedId, 'report') : '#'"
          :disabled="!selectedId"
          @click="(e) => { if (!selectedId) e.preventDefault() }"
        >
          下载报告
        </el-button>
        <el-button
          :disabled="!selectedId || isExporting('docx') || isExporting('pdf') || loading"
          @click="exportReportFmt('docx')"
        >
          {{ isExporting('docx') ? '导出中…' : '导出 Word' }}
        </el-button>
        <el-button
          :disabled="!selectedId || isExporting('docx') || isExporting('pdf') || loading"
          @click="exportReportFmt('pdf')"
        >
          {{ isExporting('pdf') ? '导出中…' : '导出 PDF' }}
        </el-button>
        <el-button
          :disabled="!selectedId || briefLoading || loading"
          title="加载与报告一致的数据摘要"
          @click="loadCompetitorBrief"
        >
          {{ briefLoading ? '摘要加载中…' : '加载数据摘要' }}
        </el-button>
        <el-button
          type="primary"
          :disabled="!selectedId || packLoading || loading || briefLoading"
          title="报告、配图与数据打包下载"
          @click="downloadBriefPack"
        >
          {{ packLoading ? '打包中…' : '一键下载简报包' }}
        </el-button>
      </div>
      <p v-if="viewInFlightOtherJobId" class="ma-warn-banner">
        任务 {{ viewInFlightOtherJobId }} 仍在处理中，请稍候或切回该任务。
      </p>

      <p v-if="briefErr" class="ma-err">{{ briefErr }}</p>
      <p v-if="packErr" class="ma-err">{{ packErr }}</p>
      <p v-if="exportDocErr" class="ma-err">{{ exportDocErr }}</p>
      <p v-if="err" class="ma-err">{{ err }}</p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务，请先完成一次采集。</p>
    </section>

    <section v-if="briefData" class="ma-card preview-card">
      <div class="preview-head">
        <h2>竞品数据摘要（机器整理）</h2>
        <div class="brief-tool-row">
          <el-button @click="copyBriefJson">
            {{ briefCopyOk ? '已复制' : '复制原始数据' }}
          </el-button>
          <el-button @click="downloadBriefJson">下载数据文件</el-button>
        </div>
      </div>
      <p class="hint-top brief-hint">与上文报告同源；可展开 JSON 或复制。</p>
      <dl v-if="briefHumanRows.length" class="brief-dl">
        <template v-for="(row, idx) in briefHumanRows" :key="idx">
          <dt>{{ row.label }}</dt>
          <dd>{{ row.value }}</dd>
        </template>
      </dl>
      <p v-else class="ma-muted brief-hint">暂无摘要（数据可能不全）。</p>
      <details class="brief-raw-wrap">
        <summary>原始数据</summary>
        <pre class="raw-md brief-json">{{ briefJson }}</pre>
      </details>
    </section>

    <section v-if="reportMd" class="ma-card preview-card">
      <div class="preview-head">
        <h2>预览</h2>
        <el-radio-group v-model="viewMode" class="ep-view-rg ep-view-rg--end">
          <el-radio-button value="render">渲染</el-radio-button>
          <el-radio-button value="raw">原文</el-radio-button>
        </el-radio-group>
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
.toolbar-task-select-wrap {
  flex: 1 1 auto;
  min-width: 10rem;
  max-width: 20rem;
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
.brief-tool-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  align-items: center;
}
.ep-view-rg {
  flex-shrink: 0;
}
.ep-view-rg--end {
  margin-left: auto;
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
