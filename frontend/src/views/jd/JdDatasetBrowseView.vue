<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import JobDatasetModal from '../../components/JobDatasetModal.vue'
import { api, refreshJobs, useJobs } from '../../composables/useJobs'
import { useJobStore } from '../../stores/jobs'

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
  return `${j.id} · ${j.keyword} · ${j.status}`
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
      useJobStore().mergeJob(j)
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
        <h2 class="title">数据浏览</h2>
        <el-button @click="load">刷新</el-button>
      </div>
      <p class="lead">选任务后按标签查看表数据，可导出。长文见「报告预览」或「报告生成」。</p>
      <p v-if="loadError" class="ma-err">{{ loadError }}</p>

      <div v-if="jobOptions.length" class="picker">
        <label class="sel-label">任务</label>
        <div class="picker-el-wrap">
          <el-select
            v-model="selectedId"
            class="jd-toolbar-el-select"
            placeholder="请选择任务"
            filterable
            placement="bottom-start"
          >
            <el-option
              v-for="j in jobOptions"
              :key="j.id"
              :label="optionLabel(j)"
              :value="String(j.id)"
            />
          </el-select>
        </div>
      </div>
      <p v-else class="ma-muted">暂无任务，请先在「采集」提交。</p>
    </section>

    <section v-if="selectedJob" class="ma-card panel-card">
      <h3 class="panel-title">数据表</h3>
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
.picker-el-wrap {
  flex: 1 1 auto;
  min-width: 10rem;
  max-width: 20rem;
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
