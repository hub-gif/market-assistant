<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { api, refreshJobs } from '../../composables/useJobs'
import { useJobStore } from '../../stores/jobs'

const router = useRouter()
const jobStore = useJobStore()

const keyword = ref('低GI')
const loading = ref(false)
const error = ref('')

// activeJobId 持久化到 store，切换标签不会丢失
const activeJobId = computed({
  get: () => jobStore.semiAutoJobId,
  set: (v) => jobStore.setSemiAutoJobId(v),
})

// 从任务列表找当前半自动任务
const activeJob = computed(() => {
  if (activeJobId.value == null) return null
  return jobStore.jobs.find((j) => String(j.id) === String(activeJobId.value)) || null
})

const phase = computed(() => activeJob.value?.semiauto_phase || '')
const jobStatus = computed(() => activeJob.value?.status || '')

const phaseLabel = computed(() => {
  const labels = {
    browser_open: '浏览器启动中…',
    waiting_login: '请在浏览器内完成登录',
    listening: '监听中，在浏览器内正常操作即可采集',
    stopping: '正在落盘并入库，请稍候…',
    done: '完成',
  }
  return labels[phase.value] || (jobStatus.value === 'running' ? '处理中…' : '')
})

const isWaitingLogin = computed(() => phase.value === 'waiting_login' && jobStatus.value === 'running')
const isListening = computed(() => phase.value === 'listening' && jobStatus.value === 'running')
const isStopping = computed(() => phase.value === 'stopping' && jobStatus.value === 'running')
const isDone = computed(() => jobStatus.value === 'success')
const isFailed = computed(() => jobStatus.value === 'failed')
const isActive = computed(() => jobStatus.value === 'running')

async function startSemiAuto() {
  error.value = ''
  const kw = keyword.value.trim()
  if (!kw) {
    error.value = '请输入搜索词'
    return
  }
  loading.value = true
  try {
    const r = await api('/api/jobs/semiauto/', {
      method: 'POST',
      body: JSON.stringify({ keyword: kw }),
    })
    if (!r.ok) {
      let t = await r.text()
      try { t = JSON.stringify(JSON.parse(t), null, 2) } catch { /* keep */ }
      error.value = t
      return
    }
    const job = await r.json()
    jobStore.mergeJob(job)
    activeJobId.value = job.id  // 写入 store，标签切换不丢
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

async function confirmLogin() {
  if (!activeJobId.value) return
  error.value = ''
  try {
    const r = await api(`/api/jobs/${activeJobId.value}/semiauto/confirm-login/`, { method: 'POST' })
    if (!r.ok) {
      const t = await r.text()
      error.value = t || `HTTP ${r.status}`
    }
  } catch (e) {
    error.value = String(e)
  }
}

const restartBusy = ref(false)

async function restartListen() {
  if (!activeJobId.value) return
  error.value = ''
  restartBusy.value = true
  try {
    const r = await api(`/api/jobs/${activeJobId.value}/semiauto/restart-listen/`, { method: 'POST' })
    if (!r.ok) {
      const t = await r.text()
      error.value = t || `HTTP ${r.status}`
    }
  } catch (e) {
    error.value = String(e)
  } finally {
    restartBusy.value = false
  }
}

async function stopTask() {
  if (!activeJobId.value) return
  error.value = ''
  try {
    const r = await api(`/api/jobs/${activeJobId.value}/semiauto/stop/`, { method: 'POST' })
    if (!r.ok) {
      const t = await r.text()
      error.value = t || `HTTP ${r.status}`
    }
  } catch (e) {
    error.value = String(e)
  }
}

function goToResults() {
  router.push('/jd/results')
}

function resetTask() {
  activeJobId.value = null
  error.value = ''
}

onMounted(async () => {
  // 刷新任务列表；若 semiAutoJobId 对应的任务仍在运行，store 会自动启动轮询
  try { await refreshJobs() } catch { /* 忽略 */ }
})
</script>

<template>
  <div>
    <!-- 启动区 -->
    <section v-if="!activeJobId" class="ma-card ma-card-elevated">
      <h2>半自动采集</h2>
      <p class="sa-lead">打开浏览器，手动登录京东后开始监听；前端按钮控制确认登录与结束任务。</p>

      <div class="sa-block">
        <label class="sa-label">搜索词 / 采集标签</label>
        <el-input
          v-model="keyword"
          class="sc-ep sc-ep--wide"
          placeholder="例如：低GI"
          clearable
          @keyup.enter="startSemiAuto"
        />
      </div>

      <el-button type="primary" :disabled="loading" @click="startSemiAuto">
        {{ loading ? '启动中…' : '启动半自动' }}
      </el-button>
      <p v-if="error" class="ma-err">{{ error }}</p>
    </section>

    <!-- 监听控制区 -->
    <section v-else class="ma-card ma-card-elevated">
      <h2>半自动监听</h2>

      <!-- 进度步骤 -->
      <div class="sa-steps">
        <div class="sa-step" :class="{ active: phase === 'browser_open', done: ['waiting_login','listening','stopping','done'].includes(phase) }">
          <span class="sa-step-dot" />
          <span>浏览器启动</span>
        </div>
        <div class="sa-step" :class="{ active: phase === 'waiting_login', done: ['listening','stopping','done'].includes(phase) }">
          <span class="sa-step-dot" />
          <span>等待登录</span>
        </div>
        <div class="sa-step" :class="{ active: phase === 'listening', done: ['stopping','done'].includes(phase) }">
          <span class="sa-step-dot" />
          <span>监听中</span>
        </div>
        <div class="sa-step" :class="{ active: phase === 'stopping', done: phase === 'done' || isDone }">
          <span class="sa-step-dot" />
          <span>落盘入库</span>
        </div>
        <div class="sa-step" :class="{ done: isDone }">
          <span class="sa-step-dot" />
          <span>完成</span>
        </div>
      </div>

      <!-- 当前状态文案 -->
      <p v-if="phaseLabel" class="sa-phase-label">{{ phaseLabel }}</p>
      <p v-if="activeJob?.error_message" class="ma-err">{{ activeJob.error_message }}</p>

      <!-- 操作按钮区 -->
      <div class="sa-actions">
        <el-button
          v-if="isWaitingLogin"
          type="primary"
          @click="confirmLogin"
        >
          确认登录
        </el-button>

        <el-button
          v-if="isListening"
          type="warning"
          plain
          :loading="restartBusy"
          @click="restartListen"
        >
          重启监听
        </el-button>

        <el-button
          v-if="isListening"
          type="danger"
          @click="stopTask"
        >
          结束任务
        </el-button>

        <el-button
          v-if="isStopping"
          type="info"
          disabled
        >
          处理中，请稍候…
        </el-button>

        <el-button
          v-if="isDone"
          type="success"
          @click="goToResults"
        >
          查看任务
        </el-button>

        <el-button
          v-if="isFailed"
          plain
          @click="goToResults"
        >
          查看任务详情
        </el-button>

        <el-button
          v-if="isDone || isFailed"
          plain
          @click="resetTask"
        >
          重新开始
        </el-button>
      </div>

      <!-- 任务 ID 信息 -->
      <p class="ma-muted sa-meta">
        任务 #{{ activeJobId }} · 搜索词：{{ activeJob?.keyword || keyword }}
        <template v-if="isDone"> · 已完成</template>
        <template v-else-if="isFailed"> · 失败</template>
        <template v-else-if="isActive"> · 进行中</template>
      </p>
      <p v-if="error" class="ma-err">{{ error }}</p>

      <!-- 完成后导航提示 -->
      <div v-if="isDone" class="sa-done-hint">
        数据已入库，可前往
        <el-button text type="primary" @click="router.push('/jd/dataset')">数据</el-button>
        或
        <el-button text type="primary" @click="router.push('/jd/analysis-build')">报告生成</el-button>
        查看结果。
      </div>
    </section>
  </div>
</template>

<style scoped>
.sa-lead {
  margin: 0 0 1.1rem;
  font-size: 0.82rem;
  color: #6b7280;
  line-height: 1.5;
}
.sa-block {
  margin-bottom: 1rem;
}
.sa-label {
  display: block;
  font-size: 0.88rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 0.4rem;
}
.sa-actions {
  display: flex;
  gap: 0.75rem;
  flex-wrap: wrap;
  margin: 1.25rem 0 0.5rem;
}
.sa-meta {
  margin-top: 0.75rem;
  font-size: 0.82rem;
  color: #64748b;
}
.sa-phase-label {
  margin: 0.85rem 0 0;
  font-size: 0.95rem;
  color: #1d4ed8;
  font-weight: 500;
}
.sa-done-hint {
  margin-top: 1rem;
  font-size: 0.88rem;
  color: #374151;
  display: flex;
  align-items: center;
  gap: 0.2rem;
  flex-wrap: wrap;
}

/* 进度步骤 */
.sa-steps {
  display: flex;
  align-items: center;
  gap: 0;
  margin: 0.25rem 0 0.5rem;
  flex-wrap: wrap;
}
.sa-step {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.82rem;
  color: #9ca3af;
  padding: 0.3rem 0.5rem;
}
.sa-step:not(:last-child)::after {
  content: '›';
  margin-left: 0.3rem;
  color: #d1d5db;
}
.sa-step-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #d1d5db;
  flex-shrink: 0;
}
.sa-step.active {
  color: #1d4ed8;
  font-weight: 600;
}
.sa-step.active .sa-step-dot {
  background: #3b82f6;
}
.sa-step.done {
  color: #16a34a;
}
.sa-step.done .sa-step-dot {
  background: #22c55e;
}
</style>
