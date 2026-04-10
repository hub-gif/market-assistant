<script setup>
import { onMounted, ref } from 'vue'
import { api, refreshJobs, useJobs, jobConfigHint, jobCancelUrl } from '../../composables/useJobs'

const { jobs } = useJobs()
const loadError = ref('')
const cancelErr = ref('')
const cancellingId = ref(null)

const statusLabels = {
  pending: '待执行',
  running: '执行中',
  success: '成功',
  failed: '失败',
  cancelled: '已终止',
}

function statusLabel(status) {
  return statusLabels[status] || status
}

function canCancel(j) {
  return j.status === 'pending' || j.status === 'running'
}

async function load() {
  loadError.value = ''
  try {
    await refreshJobs()
  } catch (e) {
    loadError.value = String(e)
  }
}

async function requestCancel(jobId) {
  cancelErr.value = ''
  cancellingId.value = jobId
  try {
    const r = await api(jobCancelUrl(jobId), { method: 'POST' })
    const text = await r.text()
    if (!r.ok) {
      try {
        const j = JSON.parse(text)
        cancelErr.value = j.detail || text
      } catch {
        cancelErr.value = text || `HTTP ${r.status}`
      }
      return
    }
    const updated = JSON.parse(text)
    const idx = jobs.value.findIndex((x) => x.id === updated.id)
    if (idx >= 0) jobs.value[idx] = updated
    await refreshJobs()
  } catch (e) {
    cancelErr.value = String(e)
  } finally {
    cancellingId.value = null
  }
}

onMounted(load)
</script>

<template>
  <div>
    <section class="ma-card">
      <h2>任务列表</h2>
      <p class="lead">
        仅展示任务状态与运行目录。执行中或待执行的任务可点<strong>终止</strong>，系统会在可停点结束并尽量保留已采集文件（非瞬间强制结束）。
        入库表浏览、批次 CSV 预览与下载请使用顶部菜单<strong>「库内数据浏览」</strong>；竞品 Markdown
        报告请使用<strong>「报告查看」</strong>（阅读/下载）或<strong>「报告生成」</strong>（改规则并重写）。
      </p>
      <button type="button" class="ma-btn ma-btn-secondary" @click="load">刷新列表</button>
      <p v-if="loadError" class="ma-err">{{ loadError }}</p>
      <p v-if="cancelErr" class="ma-err">{{ cancelErr }}</p>

      <div v-if="jobs.length" class="ma-table-wrap" style="margin-top: 1rem">
        <table class="ma-table results-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>关键词</th>
              <th>状态</th>
              <th>配置</th>
              <th>运行目录</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="j in jobs" :key="j.id">
              <td>{{ j.id }}</td>
              <td>{{ j.keyword }}</td>
              <td>
                <span :class="['ma-badge', j.status]">{{ statusLabel(j.status) }}</span>
                <span
                  v-if="j.cancellation_requested && j.status === 'running'"
                  class="cancel-pending"
                >
                  · 终止处理中
                </span>
              </td>
              <td class="ma-hint" :title="jobConfigHint(j)">{{ jobConfigHint(j) }}</td>
              <td class="ma-mono" :title="j.error_message || undefined">
                {{ j.run_dir || (j.status === 'failed' ? '见错误信息' : '—') }}
              </td>
              <td class="op-cell">
                <button
                  v-if="canCancel(j)"
                  type="button"
                  class="ma-btn ma-btn-secondary btn-cancel"
                  :disabled="cancellingId === j.id"
                  @click="requestCancel(j.id)"
                >
                  {{ cancellingId === j.id ? '提交中…' : '终止' }}
                </button>
                <span v-else class="ma-muted">—</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="ma-muted" style="margin-top: 1rem">暂无任务，请先在「搜索采集」提交。</p>
    </section>
  </div>
</template>

<style scoped>
.lead {
  margin: 0 0 1rem;
  font-size: 0.88rem;
  color: #4b5563;
  line-height: 1.5;
}
.results-table {
  font-size: 0.8rem;
}
.op-cell {
  white-space: nowrap;
}
.btn-cancel {
  font-size: 0.78rem;
  padding: 0.25rem 0.55rem;
}
.cancel-pending {
  font-size: 0.72rem;
  color: #92400e;
}
</style>
