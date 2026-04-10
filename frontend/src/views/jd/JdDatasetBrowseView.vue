<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import JobDatasetModal from '../../components/JobDatasetModal.vue'
import { api, refreshJobs, useJobs } from '../../composables/useJobs'

const { jobs } = useJobs()
const selectedId = ref('')
const loadError = ref('')

const jobOptions = computed(() =>
  [...jobs.value].sort((a, b) => b.id - a.id),
)

const selectedJob = computed(() =>
  jobOptions.value.find((j) => String(j.id) === selectedId.value),
)

function optionLabel(j) {
  const tail = j.run_dir ? j.run_dir.split(/[/\\]/).pop() : ''
  return `#${j.id} · ${j.keyword} · ${j.status}${tail ? ` · ${tail}` : ''}`
}

async function load() {
  loadError.value = ''
  try {
    await refreshJobs()
  } catch (e) {
    loadError.value = String(e)
  }
}

async function refreshSelectedJob() {
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
}

onMounted(load)

watch(
  jobOptions,
  (list) => {
    if (!list.length) {
      selectedId.value = ''
      return
    }
    if (!list.some((j) => String(j.id) === String(selectedId.value))) {
      selectedId.value = String(list[0].id)
    }
  },
  { immediate: true },
)

watch(selectedId, () => {
  refreshSelectedJob()
})
</script>

<template>
  <div class="dataset-page">
    <section class="ma-card top-bar">
      <div class="top-row">
        <h2 class="title">库内数据浏览</h2>
        <button type="button" class="ma-btn ma-btn-secondary btn-refresh" @click="load">刷新任务列表</button>
      </div>
      <p class="lead">
        选择任务后浏览已入库的搜索、商详、评价与<strong>整合宽表</strong>（合并表按列拆分入库，与
        <code>keyword_pipeline_merged.csv</code> lean 列一致）；各 Tab 下「导出当前表」可导出 JSON / CSV /
        Excel。竞品报告请在「报告查看」阅读或「报告生成」重新生成。
      </p>
      <p v-if="loadError" class="ma-err">{{ loadError }}</p>

      <div v-if="jobOptions.length" class="picker">
        <label class="sel-label">任务</label>
        <select v-model="selectedId" class="job-select">
          <option v-for="j in jobOptions" :key="j.id" :value="String(j.id)">
            {{ optionLabel(j) }}
          </option>
        </select>
      </div>
      <p v-else class="ma-muted">暂无任务，请先在「搜索采集」提交。</p>
    </section>

    <section v-if="selectedJob" class="ma-card panel-card">
      <h3 class="panel-title">入库数据</h3>
      <JobDatasetModal :job="selectedJob" embedded :open="true" />
    </section>
  </div>
</template>

<style scoped>
.dataset-page {
  width: 100%;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
.top-bar {
  padding: 0.85rem 1.1rem;
  margin-bottom: 0;
  flex-shrink: 0;
  min-width: 0;
}
.top-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
}
.title {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}
.btn-refresh {
  font-size: 0.82rem;
}
.lead {
  margin: 0.5rem 0 0;
  font-size: 0.82rem;
  color: #4b5563;
  line-height: 1.5;
}
.lead code {
  font-size: 0.76rem;
  background: #f1f5f9;
  padding: 0.05rem 0.3rem;
  border-radius: 4px;
}
.picker {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.65rem;
  margin-top: 0.65rem;
}
.sel-label {
  font-size: 0.85rem;
  font-weight: 500;
  color: #374151;
}
.job-select {
  flex: 1;
  min-width: 260px;
  padding: 0.45rem 0.6rem;
  border-radius: 8px;
  border: 1px solid #d1d5db;
  font: inherit;
}
.panel-card {
  margin-top: 0;
  padding: 0;
  min-width: 0;
  width: 100%;
  display: flex;
  flex-direction: column;
}
.panel-title {
  margin: 0;
  padding: 0.6rem 1rem;
  font-size: 0.92rem;
  font-weight: 600;
  border-bottom: 1px solid #e5e7eb;
  background: #f8fafc;
  flex-shrink: 0;
}
.panel-card :deep(.embedded-root) {
  width: 100%;
  min-width: 0;
  display: flex;
  flex-direction: column;
}
.panel-card :deep(.modal-embedded) {
  width: 100%;
  max-height: none;
}
.ma-muted {
  color: #64748b;
}
</style>
