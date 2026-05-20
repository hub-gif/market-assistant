<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter, RouterLink } from 'vue-router'
import MarkdownPreview from '../../components/MarkdownPreview.vue'
import {
  api,
  refreshJobs,
  useJobs,
  exportStrategyDocument,
} from '../../composables/useJobs'
import { generationInFlightKey, withGenerationInFlight } from '../../composables/useGenerationInFlight'
import {
  loadMarketingDetailPackRecord,
  saveMarketingDetailPackRecord,
} from '../../lib/marketingDetailPackStorage'
import { loadStrategyDraftRecord } from '../../lib/strategyDraftStorage'
import { marketingPackResultToMarkdown } from '../../lib/marketingPackMarkdown'
import { formatApiDateTime, formatSourceLabel } from '../../lib/formatApiDateTime'

const route = useRoute()
const router = useRouter()
const { jobs } = useJobs()

const genInFlight = generationInFlightKey()

const selectedId = ref('')
const draftMd = ref('')
const draftMeta = ref(null)
const viewMode = ref('render')
const exportErr = ref('')
const marketingErr = ref('')
const marketingExportErr = ref('')
const marketingResult = ref(null)
/** 底部「预览」区：策略稿 vs 营销 Markdown（与导出同源） */
const previewDoc = ref('strategy')

const exportBusy = computed(() => {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value.some((k) => String(k).startsWith(`export-strategy:${id}:`))
})

const marketingBusy = computed(() => {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value.includes(`marketing-detail-pack:${id}`)
})

const marketingExportBusy = computed(() => {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value.some((k) => String(k).startsWith(`export-marketing-pack:${id}:`))
})

function isMarketingExporting(fmt) {
  const id = selectedId.value
  if (!id) return false
  return genInFlight.value.includes(`export-marketing-pack:${id}:${fmt}`)
}

function payloadForMarketing(lastRequest) {
  if (!lastRequest || typeof lastRequest !== 'object') {
    return { business_notes: '', strategy_decisions: {} }
  }
  const {
    generator: _g,
    business_notes: bn,
    strategy_matrix_group: _mg,
    ...rest
  } = lastRequest
  return {
    business_notes: (bn || '').trim(),
    strategy_decisions: rest,
  }
}

const successJobs = computed(() =>
  [...jobs.value].filter((j) => j.status === 'success').sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  successJobs.value.find((j) => String(j.id) === selectedId.value),
)

const draftGeneratedTime = computed(() => formatApiDateTime(draftMeta.value?.generated_at))

const marketingTime = computed(() => formatApiDateTime(marketingResult.value?.generated_at))

const marketingSourceText = computed(() => formatSourceLabel(marketingResult.value?.source))

const marketingMd = computed(() => {
  if (!marketingResult.value) return ''
  try {
    return marketingPackResultToMarkdown(marketingResult.value) || ''
  } catch {
    return ''
  }
})

function loadDraft() {
  const id = selectedId.value
  if (!id) {
    draftMd.value = ''
    draftMeta.value = null
    return
  }
  try {
    const o = loadStrategyDraftRecord(id)
    if (!o) {
      draftMd.value = ''
      draftMeta.value = null
      return
    }
    draftMd.value = o.markdown || ''
    draftMeta.value = {
      keyword: o.keyword || '',
      generated_at: o.generated_at || '',
      last_request: o.last_request || null,
    }
  } catch {
    draftMd.value = ''
    draftMeta.value = null
  }
}

function loadMarketingPack() {
  const id = selectedId.value
  if (!id) {
    marketingResult.value = null
    return
  }
  const rec = loadMarketingDetailPackRecord(id)
  marketingResult.value = rec && typeof rec === 'object' ? rec : null
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

function downloadMarketingPackJson() {
  if (!marketingResult.value || !selectedId.value) return
  const blob = new Blob([JSON.stringify(marketingResult.value, null, 2)], {
    type: 'application/json;charset=utf-8',
  })
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = `job_${selectedId.value}_marketing_detail_pack.json`
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

async function exportMarketingPackFmt(fmt) {
  if (!marketingResult.value || !selectedId.value) return
  marketingExportErr.value = ''
  const id = selectedId.value
  const md = marketingPackResultToMarkdown(marketingResult.value)
  if (!md.trim()) {
    marketingExportErr.value = '无可导出的营销内容'
    return
  }
  try {
    await withGenerationInFlight(`export-marketing-pack:${id}:${fmt}`, async () => {
      await exportStrategyDocument(id, md, fmt, 'marketing_detail')
    })
  } catch (e) {
    marketingExportErr.value = String(e)
  }
}

async function generateMarketingDetailPack() {
  if (!draftMd.value || !selectedId.value) return
  marketingErr.value = ''
  marketingExportErr.value = ''
  marketingResult.value = null
  const id = selectedId.value
  const { business_notes, strategy_decisions } = payloadForMarketing(
    draftMeta.value?.last_request,
  )
  try {
    await withGenerationInFlight(`marketing-detail-pack:${id}`, async () => {
      const r = await api(`/api/jobs/${id}/marketing-detail-pack/`, {
        method: 'POST',
        body: JSON.stringify({
          strategy_markdown: draftMd.value,
          business_notes,
          strategy_decisions,
        }),
      })
      const text = await r.text()
      if (!r.ok) {
        try {
          const j = JSON.parse(text)
          marketingErr.value = j.detail || text
        } catch {
          marketingErr.value = text || `HTTP ${r.status}`
        }
        return
      }
      const body = JSON.parse(text)
      marketingResult.value = body
      try {
        saveMarketingDetailPackRecord(id, body)
      } catch {
        /* 忽略存储失败 */
      }
      previewDoc.value = 'marketing'
    })
  } catch (e) {
    marketingErr.value = String(e)
  }
}

async function exportStrategyFmt(fmt) {
  if (!draftMd.value || !selectedId.value) return
  exportErr.value = ''
  const id = selectedId.value
  try {
    await withGenerationInFlight(`export-strategy:${id}:${fmt}`, async () => {
      await exportStrategyDocument(id, draftMd.value, fmt)
    })
  } catch (e) {
    exportErr.value = String(e)
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

function onStorageDraftSync(ev) {
  const prefix = 'ma_strategy_draft_'
  if (!ev.key || !ev.key.startsWith(prefix)) return
  const jid = ev.key.slice(prefix.length)
  if (jid === String(selectedId.value)) loadDraft()
}

function onStorageMarketingSync(ev) {
  const prefix = 'ma_marketing_detail_pack_'
  if (!ev.key || !ev.key.startsWith(prefix)) return
  const jid = ev.key.slice(prefix.length)
  if (jid === String(selectedId.value)) loadMarketingPack()
}

onMounted(async () => {
  await loadList()
  syncSelectionFromRouteAndJobs()
  loadDraft()
  loadMarketingPack()
  if (typeof window !== 'undefined') {
    window.addEventListener('storage', onStorageDraftSync)
    window.addEventListener('storage', onStorageMarketingSync)
  }
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    window.removeEventListener('storage', onStorageDraftSync)
    window.removeEventListener('storage', onStorageMarketingSync)
  }
})

watch(
  () => route.query.job,
  (j) => {
    if (!j) return
    const s = String(j)
    if (s !== selectedId.value) {
      selectedId.value = s
      loadDraft()
      loadMarketingPack()
    }
  },
)

watch(selectedId, (id) => {
  marketingExportErr.value = ''
  previewDoc.value = 'strategy'
  loadDraft()
  loadMarketingPack()
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
    loadMarketingPack()
  }
})
</script>

<template>
  <div>
    <section class="ma-card">
      <h2>策略预览</h2>
      <p class="hint-top">
        文稿存于本机浏览器。改内容去
        <RouterLink to="/jd/strategy-build">策略生成</RouterLink>；数据见
        <RouterLink to="/jd/analysis-view">报告预览</RouterLink>。
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
        <el-button :disabled="!draftMd" @click="downloadDraftMd">下载文稿</el-button>
        <el-button
          :disabled="!draftMd || !selectedId || exportBusy"
          @click="exportStrategyFmt('docx')"
        >
          {{ exportBusy ? '导出中…' : '导出 Word' }}
        </el-button>
        <el-button
          :disabled="!draftMd || !selectedId || exportBusy"
          @click="exportStrategyFmt('pdf')"
        >
          导出 PDF
        </el-button>
        <el-button type="primary" @click="goBuildSameJob">编辑策略</el-button>
        <el-button
          :disabled="!draftMd || !selectedId || marketingBusy"
          @click="generateMarketingDetailPack"
        >
          {{ marketingBusy ? '营销内容生成中…' : '生成营销内容' }}
        </el-button>
      </div>

      <p
        v-if="draftMeta?.generated_at"
        class="meta-line ma-muted"
        :title="draftGeneratedTime.title"
      >
        生成时间：{{ draftGeneratedTime.text }}
        <template v-if="draftMeta.keyword"> · 关键词：{{ draftMeta.keyword }}</template>
      </p>
      <p v-if="exportErr" class="ma-err">{{ exportErr }}</p>
      <p v-if="marketingErr" class="ma-err">{{ marketingErr }}</p>
      <p v-if="marketingExportErr" class="ma-err">{{ marketingExportErr }}</p>
      <div v-if="marketingResult" class="marketing-pack-out">
        <h3 class="marketing-pack-h">营销内容</h3>
        <p class="ma-muted marketing-pack-meta" :title="marketingTime.title">
          {{ marketingTime.text }}<template v-if="marketingSourceText"> · {{ marketingSourceText }}</template>
        </p>

        <div class="toolbar marketing-pack-actions">
          <el-button
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="downloadMarketingPackJson"
          >
            下载 JSON
          </el-button>
          <el-button
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="exportMarketingPackFmt('docx')"
          >
            {{ isMarketingExporting('docx') ? '导出中…' : '营销内容导出 Word' }}
          </el-button>
          <el-button
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="exportMarketingPackFmt('pdf')"
          >
            {{ isMarketingExporting('pdf') ? '导出中…' : '营销内容导出 PDF' }}
          </el-button>
        </div>
      </div>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务。</p>
      <p v-else-if="selectedId && !draftMd" class="ma-muted empty-hint">请先在「策略生成」里生成文稿。</p>
    </section>

    <section v-if="draftMd || marketingMd" class="ma-card preview-card">
      <div class="preview-head">
        <h2>预览</h2>
        <el-radio-group
          v-if="marketingMd"
          v-model="previewDoc"
          class="doc-doc-rg"
        >
          <el-radio-button value="strategy">策略稿</el-radio-button>
          <el-radio-button value="marketing">营销内容</el-radio-button>
        </el-radio-group>
        <el-radio-group
          v-model="viewMode"
          class="ep-view-rg"
          :class="{ 'ep-view-rg--end': !marketingMd }"
        >
          <el-radio-button value="render">渲染</el-radio-button>
          <el-radio-button value="raw">原文</el-radio-button>
        </el-radio-group>
      </div>
      <template v-if="previewDoc === 'strategy' && draftMd">
        <div v-if="viewMode === 'render'" class="md-box">
          <MarkdownPreview :source="draftMd" />
        </div>
        <pre v-else class="raw-md">{{ draftMd }}</pre>
      </template>
      <template v-else-if="previewDoc === 'marketing' && marketingMd">
        <div v-if="viewMode === 'render'" class="md-box">
          <MarkdownPreview :source="marketingMd" />
        </div>
        <pre v-else class="raw-md">{{ marketingMd }}</pre>
      </template>
      <p v-else class="ma-muted preview-fallback">请先生成策略或营销内容。</p>
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
.toolbar-task-select-wrap {
  flex: 1 1 auto;
  min-width: 10rem;
  max-width: 20rem;
}
.meta-line {
  margin: 0.5rem 0 0;
  font-size: 0.82rem;
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
.doc-doc-rg {
  margin-right: auto;
  flex-shrink: 0;
}
.preview-fallback {
  margin: 0;
  font-size: 0.9rem;
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
  white-space: pre-wrap;
  padding: 1rem;
  background: #fafafa;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
}
.marketing-pack-out {
  margin-top: 1rem;
  padding: 0.85rem 1rem;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: #f8fafc;
}
.marketing-pack-h {
  margin: 0 0 0.35rem;
  font-size: 1rem;
  color: #1e293b;
}
.marketing-pack-meta {
  margin: 0 0 0.75rem;
  font-size: 0.8rem;
}
.marketing-pack-disk {
  margin: 0 0 0.65rem;
  font-size: 0.78rem;
  line-height: 1.45;
  max-width: 52rem;
}
.marketing-pack-disk code {
  font-size: 0.85em;
  background: #e2e8f0;
  padding: 0.1em 0.35em;
  border-radius: 4px;
}
.marketing-product-hint {
  margin: 0.5rem 0 0;
  font-size: 0.82rem;
  line-height: 1.45;
  max-width: 52rem;
}
</style>
