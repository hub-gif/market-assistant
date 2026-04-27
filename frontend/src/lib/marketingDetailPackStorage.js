/**
 * 营销内容包（marketing-detail-pack API 返回的 JSON）持久化，与策略稿分键存放。
 * 主存 localStorage，与 strategyDraftStorage 相同迁移思路。
 */

const PREFIX = 'ma_marketing_detail_pack_'

function key(jobId) {
  return `${PREFIX}${jobId}`
}

/**
 * @param {string} jobId
 * @returns {Record<string, unknown> | null}
 */
export function loadMarketingDetailPackRecord(jobId) {
  if (!jobId) return null
  const k = key(jobId)
  try {
    let raw = localStorage.getItem(k)
    if (!raw && typeof sessionStorage !== 'undefined') {
      raw = sessionStorage.getItem(k)
      if (raw) {
        try {
          localStorage.setItem(k, raw)
        } catch {
          /* */
        }
      }
    }
    if (!raw) return null
    const o = JSON.parse(raw)
    return o && typeof o === 'object' ? o : null
  } catch {
    return null
  }
}

/**
 * @param {string} jobId
 * @param {Record<string, unknown>} pack
 */
export function saveMarketingDetailPackRecord(jobId, pack) {
  if (!jobId || !pack || typeof pack !== 'object') return
  const k = key(jobId)
  const payload = JSON.stringify(pack)
  try {
    localStorage.setItem(k, payload)
  } catch {
    try {
      sessionStorage.setItem(k, payload)
    } catch {
      /* */
    }
    return
  }
  try {
    sessionStorage.removeItem(k)
  } catch {
    /* */
  }
}
