<script setup>
import { computed, ref, watch } from 'vue'
import {
  api,
  downloadJobDatasetExport,
  jobDatasetSummaryUrl,
  jobDatasetPageUrl,
} from '../composables/useJobs'

const props = defineProps({
  open: { type: Boolean, default: false },
  job: { type: Object, default: null },
  /** 为 true 时嵌入页面（无遮罩、无关闭），用于「库内数据浏览」独立菜单 */
  embedded: { type: Boolean, default: false },
})

const emit = defineEmits(['close'])

const paneActive = computed(() => !!(props.job && (props.embedded || props.open)))

const SORT_LABELS = {
  row_index: '入库顺序',
  price: '价格',
  sku_id: 'SKU',
  title: '标题',
  leaf_category: '叶类目',
  matrix_group_label: '类目',
  detail_category_path: '类目路径',
  detail_brand: '品牌',
  total_sales: '销量（解析排序）',
  comment_count: '评价量（解析排序）',
}

const tab = ref('search')
const page = ref(1)
const pageSize = ref(30)
const pageJumpDraft = ref(1)
const summary = ref(null)
const list = ref({ results: [], total: 0, page: 1, page_size: 30 })
const loading = ref(false)
const err = ref('')
const commentSkuFilter = ref('')
const sortField = ref('row_index')
const sortOrder = ref('asc')
/** 类目（§5 矩阵），对应接口参数 report_group */
const reportGroup = ref('')
/** 店铺名精确筛选，对应接口参数 shop；选项来自摘要 shop_options */
const selectedShop = ref('')
const priceMin = ref('')
const priceMax = ref('')
const detailCategoryQ = ref('')
const exportPanelOpen = ref(false)
const exportLoading = ref(false)
const exportErr = ref('')

function onBackdrop(e) {
  if (e.target === e.currentTarget) emit('close')
}

function handleBackdrop(e) {
  if (props.embedded) return
  onBackdrop(e)
}

const sortOptions = computed(() => {
  const h = summary.value?.dataset_sort_help
  let keys = ['row_index']
  if (h) {
    if (tab.value === 'search') keys = h.search?.length ? h.search : keys
    else if (tab.value === 'detail') keys = h.detail?.length ? h.detail : keys
    else if (tab.value === 'merged') keys = h.merged?.length ? h.merged : keys
    else keys = h.comments?.length ? h.comments : ['row_index']
  }
  return keys.map((k) => ({ value: k, label: SORT_LABELS[k] || k }))
})

const categoryOptions = computed(() => summary.value?.category_options || [])
const shopOptions = computed(() => summary.value?.shop_options || [])

const displayColumns = computed(() => {
  const s = summary.value
  let cols = []
  if (s) {
    if (tab.value === 'search') cols = s.search_columns || []
    else if (tab.value === 'detail') cols = s.detail_columns || []
    else if (tab.value === 'comments') cols = s.comment_columns || []
    else if (tab.value === 'merged') cols = s.merged_columns || []
  }
  if (cols.length > 0) return cols
  const rows = list.value.results
  if (!rows?.length) return []
  const row = rows[0]
  if (!row || typeof row !== 'object') return []
  const skip = new Set(['id', 'row_index'])
  return Object.keys(row)
    .filter((k) => !skip.has(k))
    .map((key) => ({ key, label: key }))
})

function cellText(row, key) {
  const v = row[key]
  if (v == null || v === '') return '—'
  return String(v)
}

/** 与入库字段名一致：这些列存的是可展示的图片 URL（可多段、分号分隔等） */
const IMAGE_FIELD_KEYS = new Set([
  'image',
  'large_pic_urls',
  'detail_main_image',
  'shop_logo',
])

function normalizePossibleUrl(s) {
  const t = String(s).trim()
  if (!t) return ''
  if (t.startsWith('//')) return `https:${t}`
  return t
}

/** 从单元格原文中抽出 http(s) 或 // 图片链接（评论图、多图等） */
function extractImageUrlsFromRaw(raw) {
  if (raw == null || raw === '') return []
  const s = String(raw)
  const out = []
  const seen = new Set()
  for (const m of s.matchAll(/(?:https?:)?\/\/[^\s;|'"<>()[\]\\]+/gi)) {
    let u = m[0].replace(/[,;.)'"\]]+$/g, '')
    u = normalizePossibleUrl(u)
    if (u.length > 14 && !seen.has(u)) {
      seen.add(u)
      out.push(u)
    }
  }
  return out.slice(0, 6)
}

function cellImageUrls(row, key) {
  if (!IMAGE_FIELD_KEYS.has(key)) return []
  return extractImageUrlsFromRaw(row[key])
}

function onThumbError(e) {
  const el = e.target
  if (el && el instanceof HTMLImageElement) {
    el.style.display = 'none'
  }
}

async function loadSummary() {
  if (!props.job?.id) return
  const r = await api(jobDatasetSummaryUrl(props.job.id))
  if (r.ok) summary.value = await r.json()
  else summary.value = null
}

async function refreshList() {
  if (!props.job?.id || !paneActive.value) return
  loading.value = true
  err.value = ''
  try {
    await loadSummary()
    const opts =
      tab.value === 'comments'
        ? { skuId: commentSkuFilter.value.trim() }
        : {
            sort: sortField.value,
            order: sortOrder.value,
            reportGroup: reportGroup.value.trim(),
            shop: selectedShop.value.trim(),
            priceMin: priceMin.value,
            priceMax: priceMax.value,
            detailCategoryQ: detailCategoryQ.value.trim(),
          }
    const url = jobDatasetPageUrl(
      props.job.id,
      tab.value,
      page.value,
      pageSize.value,
      opts,
    )
    const r = await api(url)
    if (!r.ok) {
      err.value = await r.text()
      return
    }
    list.value = await r.json()
    pageJumpDraft.value = list.value.page || page.value
  } catch (e) {
    err.value = String(e)
  } finally {
    loading.value = false
  }
}

watch(
  () => [props.embedded, props.open, props.job?.id],
  () => {
    if (paneActive.value) {
      tab.value = 'search'
      page.value = 1
      sortField.value = 'row_index'
      sortOrder.value = 'asc'
      reportGroup.value = ''
      selectedShop.value = ''
      priceMin.value = ''
      priceMax.value = ''
      detailCategoryQ.value = ''
      commentSkuFilter.value = ''
      err.value = ''
      summary.value = null
      exportPanelOpen.value = false
      exportErr.value = ''
    }
  },
)

watch(tab, () => {
  page.value = 1
  exportPanelOpen.value = false
  sortField.value = 'row_index'
  sortOrder.value = 'asc'
  reportGroup.value = ''
  selectedShop.value = ''
  priceMin.value = ''
  priceMax.value = ''
  detailCategoryQ.value = ''
})

watch(
  [
    sortField,
    sortOrder,
    reportGroup,
    selectedShop,
    priceMin,
    priceMax,
    detailCategoryQ,
  ],
  () => {
    if (paneActive.value && props.job && tab.value !== 'comments') page.value = 1
  },
)

watch(
  [
    paneActive,
    () => props.job?.id,
    tab,
    page,
    commentSkuFilter,
    sortField,
    sortOrder,
    reportGroup,
    selectedShop,
    priceMin,
    priceMax,
    detailCategoryQ,
  ],
  () => {
    if (paneActive.value && props.job) refreshList()
  },
)

const totalPages = () => {
  const t = list.value.total || 0
  const ps = list.value.page_size || pageSize.value
  return Math.max(1, Math.ceil(t / ps) || 1)
}

function prevPage() {
  if (page.value > 1) page.value -= 1
}

function nextPage() {
  if (page.value < totalPages()) page.value += 1
}

function goToPage() {
  const tp = totalPages()
  let n = Math.round(Number(pageJumpDraft.value))
  if (!Number.isFinite(n)) n = 1
  page.value = Math.min(Math.max(1, n), tp)
  pageJumpDraft.value = page.value
}

const exportPanelTitle = computed(() => {
  const m = { search: '搜索', detail: '商详', comments: '评论', merged: '整合宽表' }
  return `当前表（${m[tab.value] || tab.value}）`
})

function toggleExportPanel() {
  exportErr.value = ''
  exportPanelOpen.value = !exportPanelOpen.value
}

function cancelExport() {
  exportPanelOpen.value = false
  exportErr.value = ''
}

async function runExport(format) {
  if (!props.job?.id || !exportPanelOpen.value) return
  const kind = tab.value
  exportLoading.value = true
  exportErr.value = ''
  try {
    await downloadJobDatasetExport(props.job.id, kind, format)
    exportPanelOpen.value = false
  } catch (e) {
    exportErr.value = String(e?.message || e)
  } finally {
    exportLoading.value = false
  }
}
</script>

<template>
  <Teleport to="body" :disabled="embedded">
    <div
      v-if="job && (embedded || open)"
      :class="embedded ? 'embedded-root' : 'overlay'"
      @click="handleBackdrop"
    >
      <div
        :class="embedded ? 'modal modal-embedded' : 'modal'"
        role="dialog"
        :aria-modal="!embedded"
        @click.stop
      >
        <header class="head">
          <div>
            <h3>库内数据 · 任务 #{{ job.id }} · {{ job.keyword }}</h3>
            <p v-if="summary" class="sub">
              搜索 {{ summary.search_rows }} 行 · 商详 {{ summary.detail_rows }} 行 · 评价
              {{ summary.comment_rows }} 条 · 整合 {{ summary.merged_rows ?? 0 }} 行 ·
              仅展示全表至少有一格有值的列（与导出一致）
            </p>
          </div>
          <button
            v-if="!embedded"
            type="button"
            class="close"
            aria-label="关闭"
            @click="emit('close')"
          >
            ×
          </button>
        </header>

        <div class="toolbar">
          <div class="tabs">
            <button type="button" :class="{ on: tab === 'search' }" @click="tab = 'search'">搜索结果</button>
            <button type="button" :class="{ on: tab === 'detail' }" @click="tab = 'detail'">商详结果</button>
            <button type="button" :class="{ on: tab === 'comments' }" @click="tab = 'comments'">评论结果</button>
            <button type="button" :class="{ on: tab === 'merged' }" @click="tab = 'merged'">整合宽表</button>
          </div>
          <div class="exports">
            <button
              type="button"
              class="exp-btn"
              :class="{ on: exportPanelOpen }"
              :disabled="exportLoading"
              @click="toggleExportPanel"
            >
              导出当前表
            </button>
          </div>
        </div>

        <div v-if="exportPanelOpen" class="export-panel">
          <p class="export-panel-title">
            {{ exportPanelTitle }} — 选择导出类型
          </p>
          <div class="export-formats">
            <button
              type="button"
              class="ma-btn ma-btn-secondary exp-fmt"
              :disabled="exportLoading"
              @click="runExport('json')"
            >
              JSON
            </button>
            <button
              type="button"
              class="ma-btn ma-btn-secondary exp-fmt"
              :disabled="exportLoading"
              @click="runExport('csv')"
            >
              CSV
            </button>
            <button
              type="button"
              class="ma-btn ma-btn-secondary exp-fmt"
              :disabled="exportLoading"
              @click="runExport('xlsx')"
            >
              Excel
            </button>
            <button type="button" class="ma-btn ma-btn-secondary exp-cancel" :disabled="exportLoading" @click="cancelExport">
              取消
            </button>
          </div>
          <p v-if="exportLoading" class="export-status">正在生成文件…</p>
          <p v-if="exportErr" class="export-err">{{ exportErr }}</p>
        </div>

        <div class="toolbar2">
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            title="重新拉取摘要与当前页（仅刷新界面数据）"
            :disabled="loading || exportLoading"
            @click="refreshList"
          >
            {{ loading ? '刷新中…' : '刷新' }}
          </button>
          <template v-if="tab === 'comments'">
            <label class="sku-filter">
              按 SKU 筛选
              <input v-model="commentSkuFilter" type="text" placeholder="可选" class="sku-input" />
            </label>
          </template>
        </div>

        <div v-if="tab !== 'comments'" class="toolbar-filters">
          <label class="filter-item">
            排序
            <select v-model="sortField" class="filter-select">
              <option v-for="o in sortOptions" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
          </label>
          <label class="filter-item">
            顺序
            <select v-model="sortOrder" class="filter-select">
              <option value="asc">升序</option>
              <option value="desc">降序</option>
            </select>
          </label>
          <label class="filter-item">
            类目
            <select v-model="reportGroup" class="filter-select wide">
              <option value="">全部</option>
              <option v-for="g in categoryOptions" :key="g" :value="g">{{ g }}</option>
            </select>
          </label>
          <label class="filter-item">
            店铺
            <select v-model="selectedShop" class="filter-select wide">
              <option value="">全部</option>
              <option v-for="s in shopOptions" :key="s" :value="s">{{ s }}</option>
            </select>
          </label>
          <template v-if="tab === 'detail' || tab === 'merged'">
            <label class="filter-item">
              类目路径包含
              <input
                v-model="detailCategoryQ"
                type="search"
                class="filter-input wide"
                placeholder="模糊匹配商详类目路径"
                list="detail-cat-dl"
              />
              <datalist id="detail-cat-dl">
                <option
                  v-for="p in summary?.detail_category_path_options || []"
                  :key="p"
                  :value="p"
                />
              </datalist>
            </label>
          </template>
          <label class="filter-item">
            价格 ≥
            <input v-model="priceMin" type="number" step="any" class="filter-input narrow" placeholder="最低" />
          </label>
          <label class="filter-item">
            价格 ≤
            <input v-model="priceMax" type="number" step="any" class="filter-input narrow" placeholder="最高" />
          </label>
        </div>

        <div class="table-block">
          <div v-if="loading" class="state state-fill">加载中…</div>
          <p v-else-if="err" class="state err state-fill">{{ err }}</p>
          <div v-else class="table-wrap">
            <table class="data-table">
              <thead>
                <tr>
                  <th class="col-narrow">id</th>
                  <th class="col-narrow">row</th>
                  <th v-for="col in displayColumns" :key="col.key" class="col-dyn" :title="col.label">
                    {{ col.label }}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in list.results" :key="row.id">
                  <td class="num col-narrow">{{ row.id }}</td>
                  <td class="num col-narrow">{{ row.row_index }}</td>
                  <td
                    v-for="col in displayColumns"
                    :key="col.key"
                    class="cell-dyn"
                    :class="{ 'cell-dyn-media': cellImageUrls(row, col.key).length > 0 }"
                  >
                    <div v-if="cellImageUrls(row, col.key).length" class="cell-media">
                      <a
                        v-for="(u, i) in cellImageUrls(row, col.key)"
                        :key="i"
                        :href="u"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="cell-thumb-link"
                        :title="u"
                      >
                        <img
                          :src="u"
                          class="cell-thumb"
                          loading="lazy"
                          referrerpolicy="no-referrer"
                          alt=""
                          @error="onThumbError"
                        />
                      </a>
                    </div>
                    <template v-else>{{ cellText(row, col.key) }}</template>
                  </td>
                </tr>
              </tbody>
            </table>
            <p v-if="!list.results?.length" class="ma-muted empty">本页无数据（可点「刷新」或切换分页 / 表）</p>
          </div>
        </div>

        <footer class="pager">
          <span class="ma-muted"
            >第 {{ list.page || page }} / {{ totalPages() }} 页 · 共 {{ list.total ?? 0 }} 条</span
          >
          <span class="pager-jump">
            <label class="jump-label"
              >跳转
              <input
                v-model.number="pageJumpDraft"
                type="number"
                :min="1"
                :max="totalPages()"
                class="jump-input"
              />
              页</label
            >
            <button type="button" class="ma-btn ma-btn-secondary" @click="goToPage">确定</button>
          </span>
          <button type="button" class="ma-btn ma-btn-secondary" :disabled="page <= 1" @click="prevPage">
            上一页
          </button>
          <button
            type="button"
            class="ma-btn ma-btn-secondary"
            :disabled="page >= totalPages()"
            @click="nextPage"
          >
            下一页
          </button>
        </footer>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.overlay {
  position: fixed;
  inset: 0;
  background: rgb(15 23 42 / 0.45);
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
  box-sizing: border-box;
}
.modal {
  background: #fff;
  border-radius: 12px;
  width: min(98vw, 1400px);
  max-height: min(92vh, 900px);
  min-height: 0;
  min-width: 0;
  display: flex;
  flex-direction: column;
  box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);
  overflow: hidden;
}
.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: 0.85rem 1.1rem;
  border-bottom: 1px solid #e5e7eb;
  flex-shrink: 0;
}
.head h3 {
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
}
.sub {
  margin: 0.35rem 0 0;
  font-size: 0.78rem;
  color: #64748b;
}
.close {
  border: none;
  background: #f3f4f6;
  width: 2rem;
  height: 2rem;
  border-radius: 8px;
  font-size: 1.35rem;
  line-height: 1;
  cursor: pointer;
  color: #374151;
  flex-shrink: 0;
}
.toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  padding: 0.5rem 1rem;
  border-bottom: 1px solid #f1f5f9;
  flex-shrink: 0;
}
.tabs {
  display: flex;
  gap: 0.35rem;
  flex-wrap: wrap;
}
.tabs button {
  border: 1px solid #e5e7eb;
  background: #f9fafb;
  padding: 0.35rem 0.75rem;
  border-radius: 6px;
  font-size: 0.82rem;
  cursor: pointer;
  color: #4b5563;
}
.tabs button.on {
  background: #2563eb;
  border-color: #2563eb;
  color: #fff;
}
.exports {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem;
}
.exp-btn {
  border: 1px solid #cbd5e1;
  background: #fff;
  padding: 0.4rem 0.75rem;
  border-radius: 8px;
  font-size: 0.8rem;
  cursor: pointer;
  color: #334155;
}
.exp-btn:hover:not(:disabled) {
  border-color: #2563eb;
  color: #1d4ed8;
}
.exp-btn.on {
  border-color: #2563eb;
  background: #eff6ff;
  color: #1d4ed8;
}
.exp-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.export-panel {
  padding: 0.65rem 1rem 0.85rem;
  border-bottom: 1px solid #e5e7eb;
  background: #f8fafc;
  flex-shrink: 0;
}
.export-panel-title {
  margin: 0 0 0.5rem;
  font-size: 0.82rem;
  font-weight: 600;
  color: #1e293b;
}
.export-formats {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.45rem;
}
.exp-fmt {
  font-size: 0.8rem;
}
.exp-cancel {
  font-size: 0.8rem;
  margin-left: 0.25rem;
}
.export-status {
  margin: 0.5rem 0 0;
  font-size: 0.78rem;
  color: #64748b;
}
.export-err {
  margin: 0.5rem 0 0;
  font-size: 0.78rem;
  color: #b91c1c;
  white-space: pre-wrap;
}
.export-hint {
  margin: 0.45rem 0 0;
  font-size: 0.72rem;
  line-height: 1.45;
}
.toolbar2 {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.75rem;
  padding: 0.5rem 1rem;
  border-bottom: 1px solid #f1f5f9;
  flex-shrink: 0;
}
.sku-filter {
  font-size: 0.8rem;
  color: #475569;
  display: flex;
  align-items: center;
  gap: 0.35rem;
}
.sku-input {
  padding: 0.35rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  width: 160px;
  font: inherit;
}
.toolbar-filters {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 0.65rem 0.85rem;
  padding: 0.55rem 1rem 0.65rem;
  border-bottom: 1px solid #f1f5f9;
  background: #fafafa;
  flex-shrink: 0;
}
.filter-item {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  font-size: 0.72rem;
  color: #475569;
}
.filter-select,
.filter-input {
  font: inherit;
  font-size: 0.8rem;
  padding: 0.3rem 0.45rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  min-width: 0;
}
.filter-select.wide,
.filter-input.wide {
  min-width: 12rem;
  max-width: 22rem;
}
.filter-input.narrow {
  width: 5.5rem;
}
/* 高度封顶：数据再长也在表格内滚动，不把整块卡片无限撑高 */
.table-block {
  flex: 1 1 auto;
  min-height: 12rem;
  max-height: min(58vh, 40rem);
  min-width: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.state {
  padding: 1.5rem;
  text-align: center;
  color: #6b7280;
}
.state.err {
  color: #b91c1c;
  white-space: pre-wrap;
  text-align: left;
}
.state-fill {
  flex: 1;
  min-height: 0;
  overflow: auto;
}
.table-wrap {
  flex: 1;
  min-height: 0;
  min-width: 0;
  overflow: auto;
  overflow-x: auto;
  overflow-y: auto;
  padding: 0.5rem 1rem;
  -webkit-overflow-scrolling: touch;
}
.data-table {
  width: max-content;
  border-collapse: collapse;
  font-size: 0.7rem;
  table-layout: auto;
}
.data-table th,
.data-table td {
  border: 1px solid #e5e7eb;
  padding: 0.3rem 0.4rem;
  vertical-align: top;
  text-align: left;
}
.data-table th {
  background: #f8fafc;
  font-weight: 600;
  color: #334155;
}
.data-table th.col-narrow {
  width: 3rem;
  max-width: 3.25rem;
  min-width: 2.5rem;
  left: auto;
}
.data-table th.col-dyn {
  max-width: 9.5rem;
  min-width: 3.5rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.col-narrow {
  width: 3rem;
  max-width: 3.25rem;
  min-width: 2.5rem;
  box-sizing: border-box;
}
.col-dyn {
  min-width: 3.5rem;
  max-width: 9.5rem;
}
.cell-dyn {
  font-family: ui-monospace, monospace;
  max-width: 9.5rem;
  overflow-wrap: anywhere;
  word-break: break-word;
  color: #1e293b;
}
.cell-dyn-media {
  max-width: 11rem;
  vertical-align: middle;
}
.cell-media {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  align-items: center;
}
.cell-thumb-link {
  display: inline-block;
  line-height: 0;
  border-radius: 6px;
  overflow: hidden;
  border: 1px solid #e5e7eb;
  background: #f8fafc;
  flex-shrink: 0;
}
.cell-thumb {
  display: block;
  width: 4.5rem;
  height: 4.5rem;
  object-fit: contain;
}
.num {
  white-space: nowrap;
  color: #64748b;
}
.empty {
  margin: 1rem 0;
  text-align: center;
}
.pager {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: flex-end;
  gap: 0.65rem;
  padding: 0.65rem 1rem;
  border-top: 1px solid #e5e7eb;
  flex-shrink: 0;
}
.pager-jump {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.35rem;
}
.jump-label {
  font-size: 0.78rem;
  color: #475569;
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
}
.jump-input {
  width: 3.5rem;
  font: inherit;
  font-size: 0.8rem;
  padding: 0.25rem 0.35rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
}
.ma-muted {
  color: #64748b;
}
.embedded-root {
  width: 100%;
  min-width: 0;
  display: flex;
  flex-direction: column;
}
.modal-embedded {
  width: 100%;
  max-width: none;
  min-width: 0;
  box-shadow: none;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #fff;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
</style>
