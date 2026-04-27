/**
 * 分析报告页：报告正文与数据摘要的 localStorage 缓存，供跨标签页通过 storage 事件同步。
 */

export function analysisReportCacheKey(jobId) {
  return `ma_analysis_report_${jobId}`
}

export function analysisBriefCacheKey(jobId) {
  return `ma_analysis_brief_${jobId}`
}

/**
 * @param {string | number} jobId
 * @param {string} md
 */
export function persistAnalysisReportMd(jobId, md) {
  if (typeof localStorage === 'undefined' || !jobId || md == null) return
  try {
    localStorage.setItem(analysisReportCacheKey(jobId), String(md))
  } catch {
    /* 配额 / 隐私模式 */
  }
}

/**
 * @param {string | number} jobId
 * @param {unknown} briefObj 可 JSON 序列化的摘要对象
 */
export function persistAnalysisBrief(jobId, briefObj) {
  if (typeof localStorage === 'undefined' || !jobId || briefObj == null) return
  try {
    localStorage.setItem(analysisBriefCacheKey(jobId), JSON.stringify(briefObj))
  } catch {
    /* ignore */
  }
}
