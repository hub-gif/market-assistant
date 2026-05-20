<script setup>
import { onMounted, ref } from 'vue'
import {
  api,
  refreshJobs,
  useJobs,
  jobConfigHint,
  jobConfigTableCell,
  jobCancelUrl,
} from '../../composables/useJobs'
import { useJobStore } from '../../stores/jobs'

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
    useJobStore().mergeJob(updated)
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
    <section class="ma-card ma-card-elevated">
      <div class="ma-card-head">
        <h2>任务</h2>
        <el-button plain @click="load">刷新</el-button>
      </div>
      <p class="ma-one-liner">进行中可终止；数据与报告请用顶部其他入口。</p>
      <p v-if="loadError" class="ma-err">{{ loadError }}</p>
      <p v-if="cancelErr" class="ma-err">{{ cancelErr }}</p>

      <div v-if="jobs.length" class="ma-table-wrap" style="margin-top: 1rem">
        <table class="ma-table results-table">
          <thead>
            <tr>
              <th class="th-narrow">#</th>
              <th>关键词</th>
              <th>状态</th>
              <th>范围</th>
              <th class="th-op">操作</th>
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
              <td
                class="ma-hint"
                :title="jobConfigHint(j)"
              >{{ jobConfigTableCell(j) }}</td>
              <td class="op-cell">
                <el-button
                  v-if="canCancel(j)"
                  :disabled="cancellingId === j.id"
                  @click="requestCancel(j.id)"
                >
                  {{ cancellingId === j.id ? '提交中…' : '终止' }}
                </el-button>
                <span v-else class="ma-muted">—</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="ma-muted" style="margin-top: 1rem">暂无任务，请先在「采集」提交。</p>
    </section>
  </div>
</template>

<style scoped>
.ma-card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.5rem;
}
.ma-card-head h2 {
  margin: 0;
}
.ma-one-liner {
  margin: 0 0 1rem;
  font-size: 0.8rem;
  color: #6b7280;
  line-height: 1.4;
}
.th-narrow {
  width: 3rem;
}
.th-op {
  width: 4.5rem;
  text-align: right;
}
.results-table {
  font-size: 0.8rem;
}
.op-cell {
  white-space: nowrap;
}
.cancel-pending {
  font-size: 0.72rem;
  color: #92400e;
}
</style>
