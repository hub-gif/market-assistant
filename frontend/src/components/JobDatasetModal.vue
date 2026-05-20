<script setup>
import { computed, ref, watch } from 'vue'
import {
  api,
  downloadJobDatasetExport,
  downloadJobSearchLlmXlsx,
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
const llmExportLoading = ref(false)
const llmExportErr = ref('')

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
  } catch (e) {
    err.value = String(e)
  } finally {
    loading.value = false
  }
}

/** 仅当 embedded / open / 任务 id 变化时重置；勿用返回新数组的 getter，否则父组件重渲染（如任务列表轮询）会与旧值引用不同而误判变化，反复重置标签并打爆接口。 */
const datasetPaneResetKey = computed(
  () =>
    `${props.embedded ? '1' : '0'}:${props.open ? '1' : '0'}:${String(props.job?.id ?? '')}`,
)

watch(datasetPaneResetKey, () => {
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
    llmExportErr.value = ''
  }
})

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
    pageSize,
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

function onPageSizeChange() {
  page.value = 1
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

async function runLlmExport() {
  if (!props.job?.id || props.job.id == null) return
  llmExportErr.value = ''
  llmExportLoading.value = true
  try {
    await downloadJobSearchLlmXlsx(props.job.id)
  } catch (e) {
    llmExportErr.value = String(e?.message || e)
  } finally {
    llmExportLoading.value = false
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
            <h3>库内数据 · 任务 {{ job.id }} · {{ job.keyword }}</h3>
            <p v-if="summary" class="sub">
              搜索 {{ summary.search_rows }} 行 · 商详 {{ summary.detail_rows }} 行 · 评价
              {{ summary.comment_rows }} 条 · 整合 {{ summary.merged_rows ?? 0 }} 行 ·
              仅展示全表至少有一格有值的列（与导出一致）
            </p>
          </div>
          <el-button
            v-if="!embedded"
            type="info"
            text
            class="head-close-ep"
            aria-label="关闭"
            @click="emit('close')"
          >
            ×
          </el-button>
        </header>

        <div class="toolbar">
          <el-radio-group v-model="tab" class="dataset-tab-rg">
            <el-radio-button value="search">搜索结果</el-radio-button>
            <el-radio-button value="detail">商详结果</el-radio-button>
            <el-radio-button value="comments">评论结果</el-radio-button>
            <el-radio-button value="merged">整合宽表</el-radio-button>
          </el-radio-group>
          <div class="exports">
            <el-button
              v-if="tab === 'search'"
              type="success"
              plain
              :disabled="llmExportLoading || exportLoading || !(summary?.search_rows > 0)"
              :title="'调用大模型补充品牌、规格，耗时随行数增加，请耐心等待'"
              @click="runLlmExport"
            >
              {{ llmExportLoading ? '整理表生成中…' : '导出整理表' }}
            </el-button>
            <el-button
              :type="exportPanelOpen ? 'primary' : 'default'"
              plain
              :disabled="exportLoading || llmExportLoading"
              @click="toggleExportPanel"
            >
              导出当前表
            </el-button>
          </div>
        </div>

        <div v-if="llmExportErr" class="toolbar-msg err">{{ llmExportErr }}</div>

        <div v-if="exportPanelOpen" class="export-panel">
          <p class="export-panel-title">
            {{ exportPanelTitle }} — 选择导出类型
          </p>
          <div class="export-formats">
            <el-button :disabled="exportLoading" @click="runExport('json')">JSON</el-button>
            <el-button :disabled="exportLoading" @click="runExport('csv')">CSV</el-button>
            <el-button :disabled="exportLoading" @click="runExport('xlsx')">Excel</el-button>
            <el-button plain :disabled="exportLoading" @click="cancelExport">取消</el-button>
          </div>
          <p v-if="exportLoading" class="export-status">正在生成文件…</p>
          <p v-if="exportErr" class="export-err">{{ exportErr }}</p>
        </div>

        <div class="toolbar2">
          <el-button
            title="重新拉取摘要与当前页（仅刷新界面数据）"
            :disabled="loading || exportLoading"
            @click="refreshList"
          >
            {{ loading ? '刷新中…' : '刷新' }}
          </el-button>
          <template v-if="tab === 'comments'">
            <div class="sku-filter">
              <span class="filter-label">按 SKU 筛选</span>
              <el-input
                v-model="commentSkuFilter"
                clearable
                size="small"
                class="filter-ep-sku"
                placeholder="可选"
              />
            </div>
          </template>
        </div>

        <div v-if="tab !== 'comments'" class="toolbar-filters toolbar-filters-ep">
          <div class="filter-item">
            <span class="filter-label">排序</span>
            <el-select v-model="sortField" filterable size="small" class="filter-ep-select">
              <el-option v-for="o in sortOptions" :key="o.value" :label="o.label" :value="o.value" />
            </el-select>
          </div>
          <div class="filter-item">
            <span class="filter-label">顺序</span>
            <el-select v-model="sortOrder" size="small" class="filter-ep-select order-select">
              <el-option label="升序" value="asc" />
              <el-option label="降序" value="desc" />
            </el-select>
          </div>
          <div class="filter-item">
            <span class="filter-label">类目</span>
            <el-select
              v-model="reportGroup"
              filterable
              clearable
              placeholder="全部"
              size="small"
              class="filter-ep-select wide"
            >
              <el-option v-for="g in categoryOptions" :key="g" :label="g" :value="g" />
            </el-select>
          </div>
          <div class="filter-item">
            <span class="filter-label">店铺</span>
            <el-select
              v-model="selectedShop"
              filterable
              clearable
              placeholder="全部"
              size="small"
              class="filter-ep-select wide"
            >
              <el-option v-for="s in shopOptions" :key="s" :label="s" :value="s" />
            </el-select>
          </div>
          <div v-if="tab === 'detail' || tab === 'merged'" class="filter-item">
            <span class="filter-label">类目路径包含</span>
            <el-input
              v-model="detailCategoryQ"
              type="search"
              clearable
              size="small"
              class="filter-ep-wide"
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
          </div>
          <div class="filter-item">
            <span class="filter-label">价格 ≥</span>
            <el-input v-model="priceMin" type="number" class="filter-ep-narrow" size="small" placeholder="最低" />
          </div>
          <div class="filter-item">
            <span class="filter-label">价格 ≤</span>
            <el-input v-model="priceMax" type="number" class="filter-ep-narrow" size="small" placeholder="最高" />
          </div>
        </div>

        <div class="table-block">
          <div v-if="loading" class="state state-fill">加载中…</div>
          <p v-else-if="err" class="state err state-fill">{{ err }}</p>
          <div v-else class="table-wrap table-wrap-ep">
            <el-table
              v-if="(list.results || []).length"
              :data="list.results"
              row-key="id"
              border
              stripe
              size="small"
              class="job-dataset-ep-table"
              :max-height="500"
            >
              <el-table-column prop="id" label="id" width="70" align="right" fixed />
              <el-table-column prop="row_index" label="row" width="76" align="right" />
              <el-table-column
                v-for="col in displayColumns"
                :key="col.key"
                :label="col.label"
                :min-width="120"
                show-overflow-tooltip
              >
                <template #default="{ row }">
                  <div
                    v-if="cellImageUrls(row, col.key).length"
                    class="cell-media"
                  >
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
                  <span v-else class="cell-text">{{ cellText(row, col.key) }}</span>
                </template>
              </el-table-column>
            </el-table>
            <el-empty
              v-else
              class="table-empty-ep"
              :image-size="64"
              description="本页无数据（可点「刷新」或切换分页 / 表）"
            />
          </div>
        </div>

        <footer class="pager pager-ep">
            <el-pagination
            v-model:current-page="page"
            v-model:page-size="pageSize"
            :page-sizes="[10, 20, 30, 50, 100]"
            :total="list.total ?? 0"
            :disabled="loading"
            layout="total, sizes, prev, pager, next, jumper"
            background
            @size-change="onPageSizeChange"
          />
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
.head-close-ep {
  flex-shrink: 0;
  min-width: 2rem;
  height: 2rem;
  padding: 0;
  font-size: 1.35rem;
  line-height: 1;
  border-radius: 8px;
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
.dataset-tab-rg {
  flex: 1 1 auto;
  min-width: 0;
  flex-wrap: wrap;
}
.exports {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem;
}
.toolbar-msg {
  padding: 0.35rem 1rem 0;
  font-size: 0.78rem;
  color: #b91c1c;
  white-space: pre-wrap;
}
.toolbar-msg.err {
  border-bottom: 1px solid #fecaca;
  background: #fef2f2;
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
  flex-direction: column;
  gap: 0.2rem;
}
.filter-ep-sku {
  width: 10rem;
  max-width: 100%;
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
  min-width: 0;
}
.filter-label {
  line-height: 1.2;
}
.toolbar-filters-ep :deep(.filter-ep-select) {
  min-width: 0;
}
.toolbar-filters-ep :deep(.filter-ep-select.wide) {
  min-width: 12rem;
  max-width: 22rem;
}
.toolbar-filters-ep :deep(.order-select) {
  width: 5.5rem;
}
.toolbar-filters-ep :deep(.filter-ep-wide) {
  min-width: 12rem;
  max-width: 22rem;
}
.toolbar-filters-ep :deep(.filter-ep-narrow) {
  width: 5.75rem;
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
.table-wrap-ep {
  display: flex;
  flex-direction: column;
  align-items: stretch;
}
.table-empty-ep {
  padding: 1.5rem 0.5rem;
  flex: 0 0 auto;
}
.job-dataset-ep-table {
  width: max-content;
  min-width: 100%;
  font-size: 0.7rem;
}
.table-wrap-ep :deep(.job-dataset-ep-table .el-table__cell) {
  vertical-align: top;
  line-height: 1.4;
}
.cell-text {
  display: block;
  font-family: ui-monospace, monospace;
  overflow-wrap: anywhere;
  word-break: break-word;
  color: #1e293b;
  max-width: 14rem;
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
.pager-ep {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  padding: 0.6rem 1rem;
  border-top: 1px solid #e5e7eb;
  flex-shrink: 0;
}
.pager-ep :deep(.el-pagination) {
  flex-wrap: wrap;
  justify-content: center;
  row-gap: 0.35rem;
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
