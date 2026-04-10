<script setup>
import { computed, ref, watch } from 'vue'
import Papa from 'papaparse'
import MarkdownPreview from './MarkdownPreview.vue'

const props = defineProps({
  open: { type: Boolean, default: false },
  title: { type: String, default: '' },
  loading: { type: Boolean, default: false },
  error: { type: String, default: '' },
  rawText: { type: String, default: '' },
  /** merged | pc_search | comments | detail_ware | report */
  fileKind: { type: String, default: '' },
})

const emit = defineEmits(['close'])

/** 表格视图最多展示的数据行（不含表头） */
const MAX_TABLE_ROWS = 500

const viewMode = ref('render')

const isCsv = computed(() =>
  ['merged', 'pc_search', 'comments', 'detail_ware'].includes(props.fileKind),
)

const isMarkdown = computed(() => props.fileKind === 'report')

const csvTable = computed(() => {
  if (!isCsv.value || !props.rawText) return { headers: [], rows: [], truncated: false, parseErrors: [] }
  const parsed = Papa.parse(props.rawText.replace(/^\uFEFF/, ''), {
    skipEmptyLines: 'greedy',
  })
  const data = parsed.data || []
  const errors = parsed.errors || []
  if (!data.length) return { headers: [], rows: [], truncated: false, parseErrors: errors }

  const headers = (data[0] || []).map((c) => (c == null ? '' : String(c)))
  const hLen = headers.length
  const rawBody = data.slice(1).filter((row) => Array.isArray(row) && row.some((c) => String(c || '').trim() !== ''))

  let truncated = false
  let body = rawBody
  if (body.length > MAX_TABLE_ROWS) {
    truncated = true
    body = body.slice(0, MAX_TABLE_ROWS)
  }

  const rows = body.map((row) => {
    const cells = row.map((c) => (c == null ? '' : String(c)))
    if (cells.length < hLen) return [...cells, ...Array(hLen - cells.length).fill('')]
    if (cells.length > hLen) return cells.slice(0, hLen)
    return cells
  })

  return { headers, rows, truncated, parseErrors: errors }
})

watch(
  () => props.open,
  (v) => {
    if (v) {
      if (isMarkdown.value) viewMode.value = 'render'
      else if (isCsv.value) viewMode.value = 'table'
      else viewMode.value = 'raw'
    }
  },
)

watch(
  () => props.fileKind,
  () => {
    if (isMarkdown.value) viewMode.value = 'render'
    else if (isCsv.value) viewMode.value = 'table'
    else viewMode.value = 'raw'
  },
)

function onBackdrop(e) {
  if (e.target === e.currentTarget) emit('close')
}

const parseWarning = computed(() => {
  const e = csvTable.value.parseErrors
  if (!e.length) return ''
  const n = e.length
  const first = e[0]
  const msg = first?.message || '解析警告'
  return n > 1 ? `${msg} 等共 ${n} 条` : msg
})
</script>

<template>
  <Teleport to="body">
    <div v-if="open" class="overlay" @click="onBackdrop">
      <div class="modal" role="dialog" aria-modal="true" @click.stop>
        <header class="head">
          <h3>{{ title }}</h3>
          <button type="button" class="close" aria-label="关闭" @click="emit('close')">×</button>
        </header>

        <div v-if="loading" class="state">加载中…</div>
        <p v-else-if="error" class="state err">{{ error }}</p>
        <template v-else>
          <!-- Markdown：渲染 / 原文 -->
          <div v-if="isMarkdown" class="tabs">
            <button type="button" :class="{ on: viewMode === 'render' }" @click="viewMode = 'render'">
              预览
            </button>
            <button type="button" :class="{ on: viewMode === 'raw' }" @click="viewMode = 'raw'">
              原文
            </button>
          </div>

          <!-- CSV：表格 / 原文 -->
          <div v-else-if="isCsv" class="tabs">
            <button type="button" :class="{ on: viewMode === 'table' }" @click="viewMode = 'table'">
              表格
            </button>
            <button type="button" :class="{ on: viewMode === 'raw' }" @click="viewMode = 'raw'">
              原文
            </button>
          </div>

          <div class="body-scroll">
            <template v-if="isMarkdown && viewMode === 'render'">
              <div class="md-wrap">
                <MarkdownPreview :source="rawText" />
              </div>
            </template>

            <template v-else-if="isCsv && viewMode === 'table' && csvTable.headers.length">
              <p v-if="parseWarning" class="parse-warn">{{ parseWarning }}</p>
              <div class="csv-outer">
                <div class="csv-scroll">
                  <table class="csv-table">
                    <thead>
                      <tr>
                        <th v-for="(h, idx) in csvTable.headers" :key="idx" class="csv-th">
                          {{ h }}
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="(line, ri) in csvTable.rows" :key="ri">
                        <td v-for="(cell, ci) in line" :key="ci" class="csv-td">{{ cell }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
              <p v-if="csvTable.truncated" class="hint">
                已仅展示前 {{ MAX_TABLE_ROWS }} 行数据，完整内容请用「原文」或下载文件。
              </p>
            </template>

            <pre v-else class="raw">{{ rawText }}</pre>
          </div>
        </template>
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
  width: min(98vw, 1440px);
  max-height: min(92vh, 960px);
  display: flex;
  flex-direction: column;
  box-shadow: 0 25px 50px -12px rgb(0 0 0 / 0.25);
}
.head {
  display: flex;
  align-items: center;
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
}
.close:hover {
  background: #e5e7eb;
}
.state {
  padding: 2rem;
  text-align: center;
  color: #6b7280;
}
.state.err {
  color: #b91c1c;
  white-space: pre-wrap;
}
.tabs {
  display: flex;
  gap: 0.35rem;
  padding: 0.5rem 1rem 0;
  flex-shrink: 0;
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
.body-scroll {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  padding: 0.5rem 1rem 1rem;
}
.md-wrap {
  flex: 1;
  min-height: 0;
  overflow: auto;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fff;
}
.parse-warn {
  margin: 0 0 0.5rem;
  font-size: 0.78rem;
  color: #b45309;
  background: #fffbeb;
  padding: 0.35rem 0.5rem;
  border-radius: 6px;
}
.csv-outer {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fafafa;
}
.csv-scroll {
  flex: 1;
  min-height: 200px;
  max-height: min(72vh, 820px);
  overflow: auto;
  overscroll-behavior: contain;
}
.csv-table {
  border-collapse: separate;
  border-spacing: 0;
  font-size: 0.78rem;
  width: max-content;
  min-width: 100%;
  background: #fff;
}
.csv-th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #f1f5f9;
  border-right: 1px solid #e2e8f0;
  border-bottom: 2px solid #cbd5e1;
  padding: 0.45rem 0.65rem;
  text-align: left;
  font-weight: 600;
  color: #334155;
  white-space: nowrap;
  min-width: 5.5rem;
  max-width: 28rem;
  box-shadow: 0 1px 0 #cbd5e1;
}
.csv-td {
  border-right: 1px solid #f1f5f9;
  border-bottom: 1px solid #f1f5f9;
  padding: 0.4rem 0.6rem;
  vertical-align: top;
  white-space: pre-wrap;
  word-break: break-word;
  min-width: 4rem;
  max-width: 36rem;
}
.csv-th:first-child,
.csv-td:first-child {
  position: sticky;
  left: 0;
  z-index: 1;
}
.csv-th:first-child {
  z-index: 4;
  box-shadow:
    1px 0 0 #e2e8f0,
    0 1px 0 #cbd5e1;
}
.csv-td:first-child {
  background: #fff;
  box-shadow: 1px 0 0 #f1f5f9;
}
.raw {
  flex: 1;
  min-height: 200px;
  max-height: min(72vh, 820px);
  overflow: auto;
  margin: 0;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.76rem;
  line-height: 1.5;
  white-space: pre;
  word-break: normal;
  padding: 0.75rem;
  background: #fafafa;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
}
.hint {
  margin: 0.5rem 0 0;
  font-size: 0.75rem;
  color: #6b7280;
  flex-shrink: 0;
}
</style>
