import { ref } from 'vue'

const STORAGE_KEY = 'ma_generation_inflight'
const STORAGE_TS = 'ma_generation_inflight_ts'
/** 含 LLM 的重新生成可能较久；超时后视为未进行，避免按钮永久禁用 */
const TTL_MS = 45 * 60 * 1000

function readPersisted() {
  if (typeof sessionStorage === 'undefined') return null
  try {
    const k = sessionStorage.getItem(STORAGE_KEY)
    const ts = sessionStorage.getItem(STORAGE_TS)
    if (!k || ts == null) return null
    const t = Number(ts)
    if (!Number.isFinite(t) || Date.now() - t > TTL_MS) {
      sessionStorage.removeItem(STORAGE_KEY)
      sessionStorage.removeItem(STORAGE_TS)
      return null
    }
    return k
  } catch {
    return null
  }
}

function writePersisted(k) {
  if (typeof sessionStorage === 'undefined') return
  try {
    if (k) {
      sessionStorage.setItem(STORAGE_KEY, k)
      sessionStorage.setItem(STORAGE_TS, String(Date.now()))
    } else {
      sessionStorage.removeItem(STORAGE_KEY)
      sessionStorage.removeItem(STORAGE_TS)
    }
  } catch {
    /* 隐私模式 / 配额 */
  }
}

/**
 * 长耗时生成类 POST/GET 的「进行中」标记。
 * 除模块级 ref 外写入 sessionStorage，避免 Vite HMR / 整页刷新后丢失，
 * 从而出现「切换页签后重新生成又可点」的假象。
 *
 * key 示例：`strategy-draft:12`、`regenerate-report:12`、`preview-report:12`
 */
const inFlightKey = ref(readPersisted())

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key !== STORAGE_KEY && e.key !== STORAGE_TS) return
    inFlightKey.value = readPersisted()
  })
}

export function generationInFlightKey() {
  return inFlightKey
}

/**
 * @param {string} key
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withGenerationInFlight(key, fn) {
  inFlightKey.value = key
  writePersisted(key)
  try {
    return await fn()
  } finally {
    if (inFlightKey.value === key) {
      inFlightKey.value = null
      writePersisted(null)
    }
  }
}
