/**
 * 全局任务状态：耗时生成/导出/下载等「进行中」锁，跨标签页用 localStorage 同步。
 */
import { defineStore } from 'pinia'

const LS_KEY = 'ma_tasks_inflight'
const LS_TS = 'ma_tasks_inflight_ts'
/** 含 LLM 的请求可能较久；超时后视为未进行，避免按钮永久禁用 */
const TTL_MS = 45 * 60 * 1000
const LEGACY_SS_KEY = 'ma_generation_inflight'
const LEGACY_SS_TS = 'ma_generation_inflight_ts'

function isAmbiguousClientFailure(err) {
  if (err == null) return false
  const name = err.name || ''
  if (name === 'AbortError') return true
  const msg = String(err.message || err)
  return /Failed to fetch|NetworkError|Load failed|ERR_NETWORK|INTERNET_DISCONNECTED|aborted|cancel/i.test(
    msg,
  )
}

function migrateLegacySessionStorage() {
  if (typeof sessionStorage === 'undefined' || typeof localStorage === 'undefined') return
  try {
    if (localStorage.getItem(LS_KEY)) return
    const raw = sessionStorage.getItem(LEGACY_SS_KEY)
    const ts = sessionStorage.getItem(LEGACY_SS_TS)
    if (!raw) return
    localStorage.setItem(LS_KEY, raw)
    if (ts != null) localStorage.setItem(LS_TS, ts)
  } catch {
    /* ignore */
  }
}

function readKeysFromLocalStorage() {
  migrateLegacySessionStorage()
  if (typeof localStorage === 'undefined') return []
  try {
    const raw = localStorage.getItem(LS_KEY)
    const ts = localStorage.getItem(LS_TS)
    if (!raw || ts == null) return []
    const t = Number(ts)
    if (!Number.isFinite(t) || Date.now() - t > TTL_MS) {
      localStorage.removeItem(LS_KEY)
      localStorage.removeItem(LS_TS)
      return []
    }
    try {
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) return parsed.filter((x) => typeof x === 'string' && x)
      if (typeof parsed === 'string') return [parsed]
      return []
    } catch {
      return raw ? [raw] : []
    }
  } catch {
    return []
  }
}

function writeKeysToLocalStorage(keys) {
  if (typeof localStorage === 'undefined') return
  try {
    if (keys.length) {
      localStorage.setItem(LS_KEY, JSON.stringify(keys))
      localStorage.setItem(LS_TS, String(Date.now()))
    } else {
      localStorage.removeItem(LS_KEY)
      localStorage.removeItem(LS_TS)
    }
  } catch {
    /* 隐私模式 / 配额 */
  }
}

export const useTaskStore = defineStore('ma-tasks', {
  state: () => ({
    inFlightKeys: readKeysFromLocalStorage(),
  }),

  actions: {
    hydrateFromLocalStorage() {
      this.inFlightKeys = readKeysFromLocalStorage()
    },

    _persist() {
      writeKeysToLocalStorage(this.inFlightKeys)
    },

    addKey(key) {
      if (this.inFlightKeys.includes(key)) return
      this.inFlightKeys = [...this.inFlightKeys, key]
      this._persist()
    },

    removeKey(key) {
      this.inFlightKeys = this.inFlightKeys.filter((k) => k !== key)
      this._persist()
    },

    clearAll() {
      this.inFlightKeys = []
      this._persist()
    },

    clearRegenerateReportOnly() {
      const next = this.inFlightKeys.filter((k) => !String(k).startsWith('regenerate-report:'))
      this.inFlightKeys = next
      this._persist()
    },

    /**
     * @param {string} key
     * @param {() => Promise<T>} fn
     * @returns {Promise<T>}
     */
    async withInFlight(key, fn) {
      this.addKey(key)
      try {
        const out = await fn()
        this.removeKey(key)
        return out
      } catch (e) {
        if (!isAmbiguousClientFailure(e)) {
          this.removeKey(key)
        }
        throw e
      }
    },
  },
})
