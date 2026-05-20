/**
 * 将 API 返回的 ISO 时间格式化为易读的本地时间；title 中给出 UTC 对照，便于核对「准不准」。
 * @param {string | null | undefined} iso
 * @returns {{ text: string, title: string }}
 */
export function formatApiDateTime(iso) {
  if (iso == null || String(iso).trim() === '') {
    return { text: '—', title: '' }
  }
  const raw = String(iso).trim()
  const d = new Date(raw)
  if (Number.isNaN(d.getTime())) {
    return { text: raw, title: '无法解析为时间，以下为原文' }
  }
  const text = new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(d)
  const utc = d.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC')
  return {
    text,
    title: `接口时间多为 UTC。UTC：${utc}；上行为本机时区换算，与系统时钟一致。`,
  }
}

const SOURCE_LABELS = {
  llm_marketing_detail_pack_v1: 'LLM 营销内容包 v1',
}

/**
 * @param {string | null | undefined} source
 * @returns {string}
 */
export function formatSourceLabel(source) {
  if (source == null || source === '') return ''
  const k = String(source)
  return SOURCE_LABELS[k] || k
}
