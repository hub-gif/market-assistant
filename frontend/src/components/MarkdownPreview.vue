<script setup>
import { computed } from 'vue'
import DOMPurify from 'dompurify'
import { renderMarkdownToHtml } from '../utils/markdown'

const props = defineProps({
  source: { type: String, default: '' },
})

const safeHtml = computed(() => {
  const raw = renderMarkdownToHtml(props.source)
  if (!raw) return ''
  return DOMPurify.sanitize(raw, {
    ADD_ATTR: ['target', 'rel'],
  })
})
</script>

<template>
  <article class="markdown-body md-preview-root" v-html="safeHtml" />
</template>

<style scoped>
.md-preview-root {
  box-sizing: border-box;
  max-width: none;
  padding: 0.5rem 0.25rem 1rem;
  font-size: 15px;
  line-height: 1.65;
  color: #1f2328;
}

/* 宽表格、代码块横向滚动（贴近常见 MD 预览） */
.md-preview-root :deep(.markdown-body table),
.md-preview-root :deep(table) {
  display: block;
  width: max-content;
  max-width: 100%;
  overflow-x: auto;
  border-spacing: 0;
  border-collapse: collapse;
  margin: 1em 0;
}

.md-preview-root :deep(pre) {
  overflow-x: auto;
  padding: 12px 14px;
  border-radius: 8px;
  font-size: 0.86em;
  line-height: 1.5;
}

.md-preview-root :deep(img) {
  max-width: 100%;
  height: auto;
}
</style>
