import { storeToRefs } from 'pinia'
import { useJobStore } from '../stores/jobs'

export function api(path, opts = {}) {
  return fetch(path, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
  })
}

export async function refreshJobs() {
  return useJobStore().refreshJobs()
}

export function jobCancelUrl(jobId) {
  return `/api/jobs/${jobId}/cancel/`
}

export function downloadUrl(jobId, name) {
  return `/api/jobs/${jobId}/download/?name=${name}`
}

export function previewUrl(jobId, name) {
  return `/api/jobs/${jobId}/preview/?name=${name}`
}

export function jobDatasetSummaryUrl(jobId) {
  return `/api/jobs/${jobId}/dataset/summary/`
}

export function jobCompetitorBriefUrl(jobId) {
  return `/api/jobs/${jobId}/competitor-brief/`
}

export function jobCompetitorBriefPackUrl(jobId) {
  return `/api/jobs/${jobId}/competitor-brief-pack/`
}

/** 竞品报告 Markdown → Word/PDF（服务端读 run_dir 下 competitor_analysis.md） */
export function jobExportReportDocumentUrl(jobId, fmt = 'docx') {
  return `/api/jobs/${jobId}/export-document/?kind=report&fmt=${encodeURIComponent(fmt)}`
}

/** 竞品报告 GET 导出 Word/PDF（blob 下载，失败时解析服务端 JSON 提示） */
export async function exportReportDocument(jobId, fmt = 'docx') {
  const url = jobExportReportDocumentUrl(jobId, fmt)
  const r = await fetch(url)
  const ct = r.headers.get('Content-Type') || ''
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try {
      if (ct.includes('application/json')) {
        const j = await r.json()
        msg = typeof j?.detail === 'string' ? j.detail : JSON.stringify(j)
      } else {
        const t = await r.text()
        if (t) msg = t.length > 500 ? `${t.slice(0, 500)}…` : t
      }
    } catch {
      /* keep msg */
    }
    throw new Error(msg)
  }
  const blob = await r.blob()
  const filename =
    filenameFromContentDisposition(r.headers.get('Content-Disposition')) ||
    `job_${jobId}_competitor_report.${fmt}`
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = filename
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

/**
 * 策略稿或营销内容 Markdown → Word/PDF
 * @param {'strategy' | 'marketing_detail'} [kind]
 */
export async function exportStrategyDocument(jobId, markdown, fmt = 'docx', kind = 'strategy') {
  const r = await api(`/api/jobs/${jobId}/export-document/`, {
    method: 'POST',
    body: JSON.stringify({ kind, fmt, markdown }),
  })
  const ct = r.headers.get('Content-Type') || ''
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try {
      if (ct.includes('application/json')) {
        const j = await r.json()
        msg = typeof j?.detail === 'string' ? j.detail : JSON.stringify(j)
      } else {
        const t = await r.text()
        if (t) msg = t.length > 500 ? `${t.slice(0, 500)}…` : t
      }
    } catch {
      /* keep msg */
    }
    throw new Error(msg)
  }
  const blob = await r.blob()
  const fallback =
    kind === 'marketing_detail'
      ? `job_${jobId}_marketing_detail_pack.${fmt}`
      : `job_${jobId}_strategy_draft.${fmt}`
  const filename =
    filenameFromContentDisposition(r.headers.get('Content-Disposition')) || fallback
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = filename
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

export async function downloadCompetitorBriefPack(jobId) {
  const url = jobCompetitorBriefPackUrl(jobId)
  const r = await fetch(url)
  const ct = r.headers.get('Content-Type') || ''
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try {
      if (ct.includes('application/json')) {
        const j = await r.json()
        msg = j.detail || JSON.stringify(j)
      } else {
        const t = await r.text()
        if (t) msg = t.length > 500 ? `${t.slice(0, 500)}…` : t
      }
    } catch {
      /* keep msg */
    }
    throw new Error(msg)
  }
  const blob = await r.blob()
  let filename =
    filenameFromContentDisposition(r.headers.get('Content-Disposition')) ||
    `job_${jobId}_competitor_brief_pack.zip`
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = filename
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

export function reportConfigDefaultsUrl() {
  return '/api/report-config-defaults/'
}

export function strategyConfigDefaultsUrl() {
  return '/api/strategy-config-defaults/'
}

/**
 * @param {Record<string, string | number | undefined> | string} [opts] 筛选参数对象；兼容旧调用：传入字符串视为 comments 的 sku_id
 */
export function jobDatasetPageUrl(jobId, kind, page = 1, pageSize = 50, opts = {}) {
  const o = typeof opts === 'string' ? { skuId: opts } : opts || {}
  const p = new URLSearchParams({
    page: String(page),
    page_size: String(pageSize),
  })
  const sku = o.skuId ?? o.sku_id
  if (sku) p.set('sku_id', String(sku))
  if (o.sort) p.set('sort', String(o.sort))
  if (o.order) p.set('order', String(o.order))
  const rg =
    o.reportGroup ?? o.report_group ?? o.categoryNormId ?? o.category_norm_id
  if (rg !== undefined && rg !== null && String(rg).trim() !== '')
    p.set('report_group', String(rg).trim())
  const shop = o.shop ?? o.shop_name ?? o.shopQ ?? o.shop_q
  if (shop !== undefined && shop !== null && String(shop).trim() !== '')
    p.set('shop', String(shop).trim())
  const pmin = o.priceMin ?? o.price_min
  if (pmin !== undefined && pmin !== null && String(pmin).trim() !== '')
    p.set('price_min', String(pmin).trim())
  const pmax = o.priceMax ?? o.price_max
  if (pmax !== undefined && pmax !== null && String(pmax).trim() !== '')
    p.set('price_max', String(pmax).trim())
  const dcq = o.detailCategoryQ ?? o.detail_category_q
  if (dcq !== undefined && dcq !== null && String(dcq).trim() !== '')
    p.set('detail_category_q', String(dcq).trim())
  return `/api/jobs/${jobId}/dataset/${kind}/?${p.toString()}`
}

export function jobExportUrl(jobId, kind, exportFmt) {
  return `/api/jobs/${jobId}/export/?kind=${encodeURIComponent(kind)}&export_fmt=${encodeURIComponent(exportFmt)}`
}

function filenameFromContentDisposition(header) {
  if (!header) return null
  const star = /filename\*=UTF-8''([^;\s]+)/i.exec(header)
  if (star) {
    try {
      return decodeURIComponent(star[1].trim())
    } catch {
      return star[1].trim()
    }
  }
  const q = /filename="([^"]+)"/i.exec(header)
  if (q) return q[1]
  const plain = /filename=([^;\s]+)/i.exec(header)
  return plain ? plain[1].replace(/^"|"$/g, '') : null
}

export async function downloadJobDatasetExport(jobId, kind, exportFmt) {
  const url = jobExportUrl(jobId, kind, exportFmt)
  const r = await fetch(url)
  const ct = r.headers.get('Content-Type') || ''
  if (!r.ok) {
    let msg = `HTTP ${r.status}`
    try {
      if (ct.includes('application/json')) {
        const j = await r.json()
        msg = j.detail || JSON.stringify(j)
      } else {
        const t = await r.text()
        if (t) msg = t.length > 500 ? `${t.slice(0, 500)}…` : t
      }
    } catch {
      /* keep msg */
    }
    throw new Error(msg)
  }
  const blob = await r.blob()
  let filename =
    filenameFromContentDisposition(r.headers.get('Content-Disposition')) ||
    `job_${jobId}_export.${exportFmt === 'xlsx' ? 'xlsx' : exportFmt === 'csv' ? 'csv' : 'json'}`
  const u = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = u
  a.download = filename
  a.rel = 'noopener'
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(u)
}

export function jobConfigHint(j) {
  const parts = []
  if (j.page_start != null || j.page_to != null) {
    parts.push(`页 ${j.page_start ?? '—'}–${j.page_to ?? '—'}`)
  }
  if (j.max_skus != null) parts.push(`SKU≤${j.max_skus}`)
  if (j.pipeline_run_dir) {
    const s = j.pipeline_run_dir
    parts.push(s.length > 24 ? `目录:${s.slice(0, 24)}…` : `目录:${s}`)
  }
  if (j.cookie_file_path) parts.push('Cookie:文件')
  if (j.inline_cookie_used) parts.push('Cookie:粘贴')
  if (j.request_delay) parts.push(`延迟:${j.request_delay}`)
  if (j.list_pages) parts.push(`评页:${j.list_pages}`)
  if (j.pvid) parts.push('pvid')
  if (j.scenario_filter_enabled === true) parts.push('筛选:开')
  if (j.scenario_filter_enabled === false) parts.push('筛选:关')
  return parts.length ? parts.join(' · ') : '默认'
}

export function useJobs() {
  const store = useJobStore()
  const { jobs } = storeToRefs(store)
  return {
    jobs,
    refreshJobs,
    downloadUrl,
    jobConfigHint,
  }
}
