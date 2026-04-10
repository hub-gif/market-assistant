import { ref } from 'vue'

/** 触发词分隔：逗号、顿号、中文逗号、换行 */
const TRIGGER_SPLIT = /[,，、\n\r]+/u

function splitTriggers(text) {
  if (!text || typeof text !== 'string') return []
  return text
    .split(TRIGGER_SPLIT)
    .map((s) => s.trim())
    .filter(Boolean)
}

/**
 * 报告调参表单（与后端 report_config 字段对应），面向非技术用户。
 */
export function useReportConfigForm() {
  const focusWordRows = ref([{ text: '' }])
  const scenarioGroups = ref([{ label: '', triggersText: '' }])
  const marketRows = ref([
    { indicator: '', value_and_scope: '', source: '', year: '' },
  ])

  function resetToEmpty() {
    focusWordRows.value = [{ text: '' }]
    scenarioGroups.value = [{ label: '', triggersText: '' }]
    marketRows.value = [{ indicator: '', value_and_scope: '', source: '', year: '' }]
  }

  /**
   * @param {Record<string, unknown>|null|undefined} cfg
   */
  function applyFromApiConfig(cfg) {
    if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) {
      resetToEmpty()
      return
    }

    const w = cfg.comment_focus_words
    if (Array.isArray(w) && w.length) {
      focusWordRows.value = w
        .map((x) => ({ text: String(x ?? '').trim() }))
        .filter((r) => r.text)
      if (!focusWordRows.value.length) focusWordRows.value = [{ text: '' }]
    } else {
      focusWordRows.value = [{ text: '' }]
    }

    const sg = cfg.comment_scenario_groups
    if (Array.isArray(sg) && sg.length) {
      scenarioGroups.value = sg.map((item) => {
        let label = ''
        let triggers = []
        if (Array.isArray(item) && item.length >= 2) {
          label = String(item[0] ?? '').trim()
          const tr = item[1]
          triggers = Array.isArray(tr) ? tr.map((t) => String(t ?? '').trim()).filter(Boolean) : []
        } else if (item && typeof item === 'object' && !Array.isArray(item)) {
          label = String(item.label ?? '').trim()
          const tr = item.triggers
          triggers = Array.isArray(tr) ? tr.map((t) => String(t ?? '').trim()).filter(Boolean) : []
        }
        return {
          label,
          triggersText: triggers.join('、'),
        }
      })
      if (!scenarioGroups.value.length) scenarioGroups.value = [{ label: '', triggersText: '' }]
    } else {
      scenarioGroups.value = [{ label: '', triggersText: '' }]
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
  }

  /** @returns {Record<string, unknown>} 可 PATCH 到后端的 report_config；全空则为 {} */
  function buildPayload() {
    const out = {}

    const words = focusWordRows.value.map((r) => (r.text || '').trim()).filter(Boolean)
    if (words.length) out.comment_focus_words = words

    const groups = scenarioGroups.value
      .map((g) => ({
        label: (g.label || '').trim(),
        triggers: splitTriggers(g.triggersText || ''),
      }))
      .filter((g) => g.label && g.triggers.length)
    if (groups.length) {
      out.comment_scenario_groups = groups.map((g) => ({
        label: g.label,
        triggers: g.triggers,
      }))
    }

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

    return out
  }

  function addFocusRow() {
    focusWordRows.value.push({ text: '' })
  }
  function removeFocusRow(i) {
    if (focusWordRows.value.length > 1) focusWordRows.value.splice(i, 1)
    else focusWordRows.value[0].text = ''
  }

  function addScenarioRow() {
    scenarioGroups.value.push({ label: '', triggersText: '' })
  }
  function removeScenarioRow(i) {
    if (scenarioGroups.value.length > 1) scenarioGroups.value.splice(i, 1)
    else {
      scenarioGroups.value[0].label = ''
      scenarioGroups.value[0].triggersText = ''
    }
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
    focusWordRows,
    scenarioGroups,
    marketRows,
    resetToEmpty,
    applyFromApiConfig,
    buildPayload,
    addFocusRow,
    removeFocusRow,
    addScenarioRow,
    removeScenarioRow,
    addMarketRow,
    removeMarketRow,
  }
}
