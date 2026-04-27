/**
 * 任务列表：与「搜索采集 / 报告 / 策略」等页共享，列表轮询只保留一路定时器。
 */
import { defineStore } from 'pinia'

function jsonFetch(path, opts = {}) {
  return fetch(path, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
  })
}

function isActiveJobStatus(status) {
  return status === 'pending' || status === 'running'
}

/** API 与路由里 id 可能是 number，表单 v-model 常为 string */
function sameJobId(a, b) {
  if (a == null || b == null) return false
  return a === b || String(a) === String(b)
}

let jobsListPollTimer = null

function stopJobsListPoll() {
  if (jobsListPollTimer != null) {
    clearInterval(jobsListPollTimer)
    jobsListPollTimer = null
  }
}

export const useJobStore = defineStore('ma-jobs', {
  state: () => ({
    jobs: [],
  }),

  actions: {
    _syncJobsListPoll() {
      const hasActive = this.jobs.some((j) => isActiveJobStatus(j.status))
      if (!hasActive) {
        stopJobsListPoll()
        return
      }
      if (jobsListPollTimer != null) return
      jobsListPollTimer = setInterval(() => {
        useJobStore().fetchJobsListQuietly()
      }, 3000)
    },

    setJobs(list) {
      this.jobs = Array.isArray(list) ? list : []
      this._syncJobsListPoll()
    },

    /**
     * 用单条任务详情写回列表（与 PATCH 轮询、详情 GET 对齐）。
     * @param {Record<string, unknown>} updated
     */
    mergeJob(updated) {
      if (!updated || updated.id == null) return
      const idx = this.jobs.findIndex((x) => sameJobId(x.id, updated.id))
      if (idx >= 0) {
        this.jobs.splice(idx, 1, updated)
        this._syncJobsListPoll()
      }
    },

    async fetchJobsListQuietly() {
      try {
        const r = await jsonFetch('/api/jobs/')
        if (r.ok) {
          this.setJobs(await r.json())
        }
      } catch {
        /* 忽略网络错误，下一轮再试 */
      }
    },

    async refreshJobs() {
      const r = await jsonFetch('/api/jobs/')
      if (!r.ok) throw new Error(await r.text())
      this.setJobs(await r.json())
    },
  },
})
