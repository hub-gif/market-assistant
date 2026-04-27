import { ref } from 'vue'

/**
 * 表单未单独展示的布尔项：从任务读入后在「保存」时原样写回，避免误清空。
 * （与 backend ``validate_report_config_body`` 允许的键一致。）
 */
const REPORT_CONFIG_PASSTHROUGH_BOOL_KEYS = [
  'llm_comment_sentiment',
  'llm_matrix_group_summaries',
  'llm_price_group_summaries',
  'llm_promo_group_summaries',
  'llm_strategy_opportunities',
  'llm_comment_group_summaries',
  'llm_group_summaries_chunk_by_matrix',
  'chapter8_text_mining_probe',
  'chapter8_text_mining_probe_live_llm',
  'chapter8_text_mining_probe_llm_chunked',
  'chapter8_text_mining_probe_wordcloud',
]

const REPORT_CONFIG_PASSTHROUGH_INT_KEYS = [
  'chapter8_probe_min_texts',
  'chapter8_probe_lda_topics',
  'chapter8_probe_top_k_words',
  'chapter8_probe_cooc_vocab',
  'chapter8_probe_cooc_pairs',
  'chapter8_probe_wordcloud_max',
]

/**
 * 报告调参表单（与后端 report_config 字段对应），面向非技术用户。
 */
export function useReportConfigForm() {
  const marketRows = ref([
    { indicator: '', value_and_scope: '', source: '', year: '' },
  ])
  /** 表单未编辑的项，从任务配置读入后随保存写回 */
  const passthroughBools = ref({})
  const passthroughInts = ref({})

  function resetToEmpty() {
    marketRows.value = [{ indicator: '', value_and_scope: '', source: '', year: '' }]
    passthroughBools.value = {}
    passthroughInts.value = {}
  }

  /**
   * @param {Record<string, unknown>|null|undefined} cfg
   */
  function applyFromApiConfig(cfg) {
    if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) {
      resetToEmpty()
      return
    }

    const er = cfg.external_market_table_rows
    if (Array.isArray(er) && er.length) {
      marketRows.value = er.map((row) => {
        if (Array.isArray(row) && row.length >= 4) {
          return {
            indicator: String(row[0] ?? ''),
            value_and_scope: String(row[1] ?? ''),
            source: String(row[2] ?? ''),
            year: String(row[3] ?? ''),
          }
        }
        if (row && typeof row === 'object' && !Array.isArray(row)) {
          return {
            indicator: String(row.indicator ?? ''),
            value_and_scope: String(row.value_and_scope ?? ''),
            source: String(row.source ?? ''),
            year: String(row.year ?? ''),
          }
        }
        return { indicator: '', value_and_scope: '', source: '', year: '' }
      })
    } else {
      marketRows.value = [{ indicator: '', value_and_scope: '', source: '', year: '' }]
    }

    const passB = {}
    for (const k of REPORT_CONFIG_PASSTHROUGH_BOOL_KEYS) {
      if (Object.prototype.hasOwnProperty.call(cfg, k)) passB[k] = Boolean(cfg[k])
    }
    passthroughBools.value = passB

    const passI = {}
    for (const k of REPORT_CONFIG_PASSTHROUGH_INT_KEYS) {
      if (Object.prototype.hasOwnProperty.call(cfg, k)) {
        const v = cfg[k]
        if (typeof v === 'number' && Number.isFinite(v)) passI[k] = Math.trunc(v)
      }
    }
    passthroughInts.value = passI
  }

  /** @returns {Record<string, unknown>} 可 PATCH 到后端的 report_config；全空则为 {} */
  function buildPayload() {
    const out = {}

    const rows = marketRows.value
      .map((r) => ({
        indicator: (r.indicator || '').trim(),
        value_and_scope: (r.value_and_scope || '').trim(),
        source: (r.source || '').trim(),
        year: (r.year || '').trim(),
      }))
      .filter((r) => r.indicator || r.value_and_scope || r.source || r.year)
    if (rows.length) {
      out.external_market_table_rows = rows.map((r) => ({
        indicator: r.indicator,
        value_and_scope: r.value_and_scope,
        source: r.source,
        year: r.year,
      }))
    }

    Object.assign(out, passthroughBools.value)
    Object.assign(out, passthroughInts.value)
    return out
  }

  function addMarketRow() {
    marketRows.value.push({
      indicator: '',
      value_and_scope: '',
      source: '',
      year: '',
    })
  }
  function removeMarketRow(i) {
    if (marketRows.value.length > 1) marketRows.value.splice(i, 1)
    else {
      const z = marketRows.value[0]
      z.indicator = ''
      z.value_and_scope = ''
      z.source = ''
      z.year = ''
    }
  }

  return {
    marketRows,
    passthroughBools,
    passthroughInts,
    resetToEmpty,
    applyFromApiConfig,
    buildPayload,
    addMarketRow,
    removeMarketRow,
  }
}
