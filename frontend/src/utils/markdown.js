import { marked } from 'marked'

marked.setOptions({
  gfm: true,
  breaks: true,
})

export function renderMarkdownToHtml(src) {
  const text = (src || '').replace(/^\uFEFF/, '')
  if (!text.trim()) return ''
  return marked.parse(text)
}
