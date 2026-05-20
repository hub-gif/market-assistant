/**
 * 将营销内容 API 返回的 JSON 转为可导出 Word/PDF 的 Markdown（中文小标题）。
 */

import { formatApiDateTime, formatSourceLabel } from './formatApiDateTime'

const CORE_LABELS = {
  what_we_sell: '卖的是什么（本品）',
  one_liner_value: '一句话价值主张',
  buyer_job_to_be_done: '购买者任务与情境',
  key_pain_or_desire: '核心痛点或欲望',
  why_this_product: '为何要选这一款',
  proof_or_trust_angle: '信任或证明角度',
  differentiation_vs_alternatives: '与替代方案的差异',
  price_value_framing: '价位与价值感表述',
  compliance_taboos: '表述禁区摘要',
  open_points_for_business: '待业务补充',
}

function escLine(s) {
  if (s == null || s === '') return '—'
  return String(s).replace(/\r\n/g, '\n').trim() || '—'
}

function pushCoreCard(lines, card) {
  if (!card || typeof card !== 'object') {
    lines.push('（无核心信息卡数据）')
    lines.push('')
    return
  }
  for (const [en, zh] of Object.entries(CORE_LABELS)) {
    lines.push(`### ${zh}`)
    lines.push('')
    lines.push(escLine(card[en]))
    lines.push('')
  }
}

function pushBullets(lines, title, arr) {
  lines.push(`### ${title}`)
  lines.push('')
  if (!Array.isArray(arr) || !arr.length) {
    lines.push('—')
    lines.push('')
    return
  }
  let any = false
  for (const item of arr) {
    const t = escLine(item)
    if (t !== '—') {
      lines.push(`- ${t}`)
      any = true
    }
  }
  if (!any) lines.push('—')
  lines.push('')
}

function pushFaq(lines, faq) {
  lines.push('### 买家问答')
  lines.push('')
  if (!Array.isArray(faq) || !faq.length) {
    lines.push('—')
    lines.push('')
    return
  }
  let n = 0
  for (const item of faq) {
    if (!item || typeof item !== 'object') continue
    const q = escLine(item.question)
    const a = escLine(item.answer)
    if (q === '—' && a === '—') continue
    n += 1
    lines.push(`#### 问 ${n}：${q === '—' ? '（未提供）' : q}`)
    lines.push('')
    lines.push(a)
    lines.push('')
  }
  if (n === 0) {
    lines.push('—')
    lines.push('')
  }
}

function pushDetailPack(lines, pack) {
  if (!pack || typeof pack !== 'object') {
    lines.push('（无多触点营销文案数据）')
    lines.push('')
    return
  }
  lines.push('### 依据与边界')
  lines.push('')
  lines.push(escLine(pack.traceability_note))
  lines.push('')
  pushBullets(lines, '商品短标题备选', pack.listing_titles)
  lines.push('### 列表副文案')
  lines.push('')
  lines.push(escLine(pack.listing_subtitle))
  lines.push('')
  lines.push('### 商品详情页首屏引导')
  lines.push('')
  lines.push(escLine(pack.detail_headline))
  lines.push('')
  pushBullets(lines, '详情页中段叙事', pack.detail_mid_story_paragraphs)
  pushBullets(lines, '卖点列表', pack.selling_bullets)
  pushBullets(lines, '食用场景与搭配建议', pack.usage_and_pairing_tips)
  pushBullets(lines, '参数区旁短句', pack.spec_sidebar_lines)
  pushFaq(lines, pack.faq)
  pushBullets(lines, '短图文/种草贴变体', pack.short_graphic_post_variants)
  pushBullets(lines, '主图三要点', pack.main_image_three_points)
  lines.push('### 文生图提示词（主图）')
  lines.push('')
  lines.push(escLine(pack.text_to_image_prompt_main))
  lines.push('')
  lines.push('### 文生图提示词（场景/备选）')
  lines.push('')
  lines.push(escLine(pack.text_to_image_prompt_scene))
  lines.push('')
  lines.push('### 文生视频提示词（短视频）')
  lines.push('')
  lines.push(escLine(pack.text_to_video_prompt))
  lines.push('')
  lines.push('### 直播/短视频钩句')
  lines.push('')
  lines.push(escLine(pack.live_or_short_hook))
  lines.push('')
  pushBullets(lines, '直播/短视频要点提纲', pack.live_script_bullets)
  lines.push('### 客服首句建议')
  lines.push('')
  lines.push(escLine(pack.customer_service_opening))
  lines.push('')
}

/**
 * @param {Record<string, unknown>} result marketing-detail-pack API 的 JSON 体
 * @returns {string}
 */
export function marketingPackResultToMarkdown(result) {
  if (!result || typeof result !== 'object') return ''
  const lines = []
  const jobId = result.job_id ?? ''
  const kw = result.keyword ?? ''
  const gen = formatApiDateTime(result.generated_at)
  const genAt = gen.text
  const src = formatSourceLabel(result.source) || (result.source ?? '')
  lines.push('# 营销内容')
  lines.push('')
  const srcPart = src ? ` · ${src}` : ''
  lines.push(`> 任务 ${jobId} · 关键词：${kw} · ${genAt}${srcPart}`)
  lines.push('')
  lines.push('## 核心信息卡')
  lines.push('')
  pushCoreCard(lines, result.core_info_card)
  lines.push('## 多触点文案（列表/详情页/主图等）')
  lines.push('')
  pushDetailPack(lines, result.detail_page_pack)
  return lines.join('\n').trim() + '\n'
}
