import { ref } from 'vue'

const STORAGE_KEY = 'ma_generation_inflight'
const STORAGE_TS = 'ma_generation_inflight_ts'
/** 含 LLM 的重新生成可能较久；超时后视为未进行，避免按钮永久禁用 */
const TTL_MS = 45 * 60 * 1000

/**
 * @returns {string[]}
 */
function readPersistedKeys() {
  if (typeof sessionStorage === 'undefined') return []
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    const ts = sessionStorage.getItem(STORAGE_TS)
    if (!raw || ts == null) return []
    const t = Number(ts)
    if (!Number.isFinite(t) || Date.now() - t > TTL_MS) {
      sessionStorage.removeItem(STORAGE_KEY)
      sessionStorage.removeItem(STORAGE_TS)
      return []
    }
    try {
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) return parsed.filter((x) => typeof x === 'string' && x)
      if (typeof parsed === 'string') return [parsed]
      return []
    } catch {
      /* 旧版：存的是 JSON 字符串化的单个 key，或非法 JSON 时按原字符串当作一个 key */
      return raw ? [raw] : []
    }
  } catch {
    return []
  }
}

/**
 * @param {string[]} keys
 */
function writePersistedKeys(keys) {
  if (typeof sessionStorage === 'undefined') return
  try {
    if (keys.length) {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(keys))
      sessionStorage.setItem(STORAGE_TS, String(Date.now()))
    } else {
      sessionStorage.removeItem(STORAGE_KEY)
      sessionStorage.removeItem(STORAGE_TS)
    }
  } catch {
    /* 隐私模式 / 配额 */
  }
}

/** 多个耗时请求可并行：例如「重新生成报告」未完成时又点了「报告预览」，不得互相覆盖。 */
const inFlightKeys = ref(readPersistedKeys())

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key !== STORAGE_KEY && e.key !== STORAGE_TS) return
    inFlightKeys.value = readPersistedKeys()
  })
}

/**
 * 当前进行中的请求 key 列表（同一 key 不会重复）。
 * key 示例：`strategy-draft:12`、`regenerate-report:12`、`preview-report:12`
 */
export function generationInFlightKey() {
  return inFlightKeys
}

function addInFlightKey(key) {
  const cur = inFlightKeys.value
  if (cur.includes(key)) return
  inFlightKeys.value = [...cur, key]
  writePersistedKeys(inFlightKeys.value)
}

function removeInFlightKey(key) {
  inFlightKeys.value = inFlightKeys.value.filter((k) => k !== key)
  writePersistedKeys(inFlightKeys.value)
}

/**
 * 切换页签/隐藏标签时浏览器可能中止 fetch；服务端可能仍在执行，不应移除该 key。
 */
function isAmbiguousClientFailure(err) {
  if (err == null) return false
  const name = err.name || ''
  if (name === 'AbortError') return true
  const msg = String(err.message || err)
  return /Failed to fetch|NetworkError|Load failed|ERR_NETWORK|INTERNET_DISCONNECTED|aborted|cancel/i.test(
    msg,
  )
}

/**
 * @param {string} key
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withGenerationInFlight(key, fn) {
  addInFlightKey(key)
  try {
    const out = await fn()
    removeInFlightKey(key)
    return out
  } catch (e) {
    if (!isAmbiguousClientFailure(e)) {
      removeInFlightKey(key)
    }
    throw e
  }
}
