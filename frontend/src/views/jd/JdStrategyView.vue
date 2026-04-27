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

function pickDetailPackSubset(pack, keys) {
  if (!pack || typeof pack !== 'object') return null
  const o = {}
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(pack, k)) o[k] = pack[k]
  }
  return Object.keys(o).length ? o : null
}

/** 列表/详情页主文案（与「触点」分开展示） */
const marketingPackDetailList = computed(() =>
  pickDetailPackSubset(marketingResult.value?.detail_page_pack, [
    'listing_titles',
    'listing_subtitle',
    'detail_headline',
    'detail_mid_story_paragraphs',
    'selling_bullets',
    'usage_and_pairing_tips',
    'spec_sidebar_lines',
    'faq',
    'short_graphic_post_variants',
  ]),
)

/** 依据、主图要点、文生图/文生视频提示词、短视频钩句、客服 */
const marketingPackTouchBlock = computed(() =>
  pickDetailPackSubset(marketingResult.value?.detail_page_pack, [
    'traceability_note',
    'main_image_three_points',
    'text_to_image_prompt_main',
    'text_to_image_prompt_scene',
    'text_to_video_prompt',
    'live_or_short_hook',
    'live_script_bullets',
    'customer_service_opening',
  ]),
)

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
      <h2>策略稿预览</h2>
      <p class="hint-top">
        选择在<strong>策略生成</strong>页已生成过的任务查看文稿（保存在本机浏览器 <strong>localStorage</strong>，同域名下可跨标签查看）。需要改决策请回到
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
        <button
          type="button"
          class="ma-btn ma-btn-secondary"
          :disabled="!draftMd || !selectedId || marketingBusy"
          @click="generateMarketingDetailPack"
        >
          {{ marketingBusy ? '营销内容生成中…' : '生成营销内容' }}
        </button>
      </div>

      <p v-if="draftMeta?.generated_at" class="meta-line ma-muted">
        生成时间：{{ draftMeta.generated_at }}
        <template v-if="draftMeta.keyword"> · 关键词：{{ draftMeta.keyword }}</template>
      </p>
      <p v-if="exportErr" class="ma-err">{{ exportErr }}</p>
      <p v-if="marketingErr" class="ma-err">{{ marketingErr }}</p>
      <p v-if="marketingExportErr" class="ma-err">{{ marketingExportErr }}</p>
      <div v-if="marketingResult" class="marketing-pack-out">
        <h3 class="marketing-pack-h">营销内容</h3>
        <p class="ma-muted marketing-pack-meta">
          {{ marketingResult.generated_at }} · {{ marketingResult.source }}
        </p>

        <div class="toolbar marketing-pack-actions">
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="downloadMarketingPackJson"
          >
            下载 JSON
          </button>
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="exportMarketingPackFmt('docx')"
          >
            {{ isMarketingExporting('docx') ? '导出中…' : '营销内容导出 Word' }}
          </button>
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            :disabled="!selectedId || marketingExportBusy || marketingBusy"
            @click="exportMarketingPackFmt('pdf')"
          >
            {{ isMarketingExporting('pdf') ? '导出中…' : '营销内容导出 PDF' }}
          </button>
        </div>
        <details open class="marketing-details">
          <summary>核心信息卡</summary>
          <pre class="marketing-pre">{{ JSON.stringify(marketingResult.core_info_card, null, 2) }}</pre>
        </details>
        <details v-if="marketingPackDetailList" open class="marketing-details">
          <summary>列表与详情页主文案</summary>
          <pre class="marketing-pre">{{ JSON.stringify(marketingPackDetailList, null, 2) }}</pre>
        </details>
        <details v-else-if="marketingResult.detail_page_pack" open class="marketing-details">
          <summary>详情页包字段</summary>
          <pre class="marketing-pre">{{ JSON.stringify(marketingResult.detail_page_pack, null, 2) }}</pre>
        </details>
        <details v-if="marketingPackTouchBlock" open class="marketing-details">
          <summary>依据、主图、文生图/文生视频提示词、钩句与客服</summary>
          <pre class="marketing-pre">{{ JSON.stringify(marketingPackTouchBlock, null, 2) }}</pre>
        </details>
      </div>
      <p v-if="selectedJob?.run_dir" class="run-dir-note ma-muted">
        任务目录：<span class="run-dir-path">{{ selectedJob.run_dir }}</span>
      </p>
      <p v-if="!successJobs.length" class="ma-muted">暂无成功任务。</p>
      <p v-else-if="selectedId && !draftMd" class="ma-muted empty-hint">
        当前任务尚无已生成的策略稿。请先在「策略生成」填写并点击「生成并前往预览」。
      </p>
    </section>

    <section v-if="draftMd || marketingMd" class="ma-card preview-card">
      <div class="preview-head">
        <h2>预览</h2>
        <div v-if="marketingMd" class="tabs doc-tabs">
          <button
            type="button"
            :class="{ on: previewDoc === 'strategy' }"
            @click="previewDoc = 'strategy'"
          >
            策略稿
          </button>
          <button
            type="button"
            :class="{ on: previewDoc === 'marketing' }"
            @click="previewDoc = 'marketing'"
          >
            营销内容
          </button>
        </div>
        <div class="tabs">
          <button type="button" :class="{ on: viewMode === 'render' }" @click="viewMode = 'render'">
            渲染
          </button>
          <button type="button" :class="{ on: viewMode === 'raw' }" @click="viewMode = 'raw'">
            原文
          </button>
        </div>
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
      <p v-else class="ma-muted preview-fallback">
        暂无当前页面对应的文稿（请先生成策略稿或营销内容）。
      </p>
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
.doc-tabs {
  margin-right: auto;
}
.preview-fallback {
  margin: 0;
  font-size: 0.9rem;
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
.marketing-pack-actions {
  margin: 0 0 0.75rem;
  flex-wrap: wrap;
  gap: 0.35rem;
}
.marketing-details {
  margin-bottom: 0.65rem;
}
.marketing-details summary {
  cursor: pointer;
  font-weight: 600;
  font-size: 0.88rem;
  color: #334155;
}
.marketing-pre {
  margin: 0.5rem 0 0;
  padding: 0.65rem;
  font-size: 0.75rem;
  line-height: 1.45;
  overflow: auto;
  max-height: 320px;
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
}
</style>
