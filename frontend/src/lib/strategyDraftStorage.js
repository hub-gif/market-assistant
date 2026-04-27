/**
 * 策略稿与会话字段：localStorage 主存，便于跨标签；首次读取时从 sessionStorage 迁移旧数据。
 */

const DRAFT_PREFIX = 'ma_strategy_draft_'
const SCOPE_PREFIX = 'ma_strategy_scope_'

function draftKey(jobId) {
  return `${DRAFT_PREFIX}${jobId}`
}

function scopeKey(jobId) {
  return `${SCOPE_PREFIX}${jobId}`
}

/**
 * @param {string} jobId
 * @returns {Record<string, unknown>|null}
 */
export function loadStrategyDraftRecord(jobId) {
  if (!jobId) return null
  const key = draftKey(jobId)
  try {
    let raw = localStorage.getItem(key)
    if (!raw && typeof sessionStorage !== 'undefined') {
      raw = sessionStorage.getItem(key)
      if (raw) {
        try {
          localStorage.setItem(key, raw)
        } catch {
          /* 配额：保留 session 可读 */
        }
      }
    }
    if (!raw) return null
    return JSON.parse(raw)
  } catch {
    return null
  }
}

/**
 * @param {string} jobId
 * @param {Record<string, unknown>} record
 */
export function saveStrategyDraftRecord(jobId, record) {
  if (!jobId) return
  const key = draftKey(jobId)
  const payload = JSON.stringify(record)
  try {
    localStorage.setItem(key, payload)
  } catch {
    try {
      sessionStorage.setItem(key, payload)
    } catch {
      /* ignore */
    }
    return
  }
  try {
    sessionStorage.removeItem(key)
  } catch {
    /* ignore */
  }
}

/**
 * @param {string} jobId
 * @returns {string}
 */
export function loadStrategyMatrixScope(jobId) {
  if (!jobId) return ''
  const key = scopeKey(jobId)
  try {
    let v = localStorage.getItem(key)
    if (v == null && typeof sessionStorage !== 'undefined') {
      v = sessionStorage.getItem(key)
      if (v != null) {
        try {
          localStorage.setItem(key, v)
        } catch {
          /* */
        }
      }
    }
    return v || ''
  } catch {
    return ''
  }
}

/**
 * @param {string} jobId
 * @param {string} groupLabel empty = clear
 */
export function saveStrategyMatrixScope(jobId, groupLabel) {
  if (!jobId) return
  const key = scopeKey(jobId)
  try {
    if (groupLabel) {
      localStorage.setItem(key, groupLabel)
      try {
        sessionStorage.setItem(key, groupLabel)
      } catch {
        /* */
      }
    } else {
      localStorage.removeItem(key)
      try {
        sessionStorage.removeItem(key)
      } catch {
        /* */
      }
    }
  } catch {
    try {
      if (groupLabel) sessionStorage.setItem(key, groupLabel)
      else sessionStorage.removeItem(key)
    } catch {
      /* */
    }
  }
}
