<script setup>
import { computed, onMounted } from 'vue'
import { useRoute, useRouter, RouterLink, RouterView } from 'vue-router'
import { refreshJobs } from '../composables/useJobs'

const route = useRoute()
const router = useRouter()

const currentPlatform = computed(() => {
  const p = route.path
  if (p.startsWith('/jd')) return 'jd'
  if (p.startsWith('/tb')) return 'tb'
  return ''
})

const jdTabs = [
  { to: '/jd/semiauto', label: '半自动', key: 'semiauto' },
  { to: '/jd/search', label: '采集', key: 'search' },
  { to: '/jd/results', label: '任务', key: 'results' },
  { to: '/jd/dataset', label: '数据', key: 'dataset' },
  { to: '/jd/analysis-build', label: '报告生成', key: 'analysis-build' },
  { to: '/jd/analysis-view', label: '报告预览', key: 'analysis-view' },
  { to: '/jd/strategy-build', label: '策略生成', key: 'strategy-build' },
  { to: '/jd/strategy-view', label: '策略预览', key: 'strategy-view' },
]

function selectPlatform(id) {
  if (id === 'jd') router.push('/jd/search')
  if (id === 'tb') router.push('/tb')
}

onMounted(async () => {
  try {
    await refreshJobs()
  } catch {
    /* 后端未起时忽略 */
  }
})
</script>

<template>
  <div class="shell">
    <aside class="sidenav">
      <div class="brand">
        <span class="brand-mark">M</span>
        <div class="brand-text">
          <strong>Market-Assistant</strong>
          <span>市场助手</span>
        </div>
      </div>
      <p class="nav-label">平台</p>
      <nav class="platform-nav">
        <el-button
          text
          class="plat-item"
          :class="{ 'plat-item--active': currentPlatform === 'jd' }"
          @click="selectPlatform('jd')"
        >
          <span class="plat-icon jd">京</span>
          <span class="plat-name">京东商城</span>
          <span class="plat-tag">可用</span>
        </el-button>
        <el-button
          text
          class="plat-item"
          :class="{ 'plat-item--active': currentPlatform === 'tb' }"
          @click="selectPlatform('tb')"
        >
          <span class="plat-icon tb">淘</span>
          <span class="plat-name">淘宝天猫</span>
          <span class="plat-tag soon">筹备</span>
        </el-button>
      </nav>
    </aside>

    <div class="main">
      <header class="topbar">
        <div class="topbar-inner">
          <h1 class="page-title">{{ route.meta.title || 'Market-Assistant' }}</h1>
          <p v-if="currentPlatform === 'jd'" class="page-desc">
            围绕关键词任务：采集与跑数、报告与策略生成、预览与数据浏览
          </p>
          <p v-else-if="currentPlatform === 'tb'" class="page-desc">多平台能力规划中</p>
        </div>
      </header>

      <div v-if="currentPlatform === 'jd'" class="func-tabs">
        <RouterLink
          v-for="t in jdTabs"
          :key="t.key"
          :to="t.to"
          class="func-tab"
          :class="{ active: route.meta.tab === t.key }"
        >
          {{ t.label }}
        </RouterLink>
      </div>

      <main
        class="content"
        :class="{ 'content-wide': currentPlatform === 'jd' || route.meta.wide }"
      >
        <RouterView />
      </main>
    </div>
  </div>
</template>

<style scoped>
.shell {
  display: flex;
  flex: 1;
  min-height: 0;
  width: 100%;
  font-family: system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
  background: linear-gradient(165deg, #f1f5f9 0%, #f8fafc 45%, #eef2f7 100%);
  color: #111827;
}
.sidenav {
  width: 240px;
  flex-shrink: 0;
  background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
  color: #e2e8f0;
  padding: 1.25rem 0;
  display: flex;
  flex-direction: column;
  border-right: 1px solid rgb(0 0 0 / 0.2);
}
.brand {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0 1.25rem 1.5rem;
  border-bottom: 1px solid rgb(255 255 255 / 0.08);
  margin-bottom: 1rem;
}
.brand-mark {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 800;
  font-size: 1.1rem;
  color: #fff;
}
.brand-text {
  display: flex;
  flex-direction: column;
  gap: 0.1rem;
}
.brand-text strong {
  font-size: 0.95rem;
  letter-spacing: -0.02em;
}
.brand-text span {
  font-size: 0.72rem;
  color: #94a3b8;
}
.nav-label {
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: #64748b;
  padding: 0 1.25rem;
  margin: 0 0 0.5rem;
}
.platform-nav {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  padding: 0 0.75rem;
}
.plat-item {
  display: flex;
  align-items: center;
  gap: 0.65rem;
  width: 100%;
  height: auto;
  min-height: unset;
  padding: 0.65rem 0.75rem;
  border: none;
  border-radius: 10px;
  margin: 0;
  justify-content: flex-start;
  font: inherit;
  color: #cbd5e1;
  background: transparent !important;
  text-align: left;
  transition: background 0.15s, color 0.15s;
}
.plat-item:hover {
  color: #f1f5f9 !important;
  background: rgb(255 255 255 / 0.06) !important;
}
.plat-item--active {
  color: #fff !important;
  background: rgb(59 130 246 / 0.25) !important;
}
.plat-icon {
  width: 34px;
  height: 34px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 700;
  font-size: 0.85rem;
}
.plat-icon.jd {
  background: #b91c1c;
  color: #fff;
}
.plat-icon.tb {
  background: #ea580c;
  color: #fff;
}
.plat-name {
  flex: 1;
  font-size: 0.9rem;
  font-weight: 500;
}
.plat-tag {
  font-size: 0.65rem;
  padding: 0.15rem 0.4rem;
  border-radius: 4px;
  background: rgb(34 197 94 / 0.25);
  color: #86efac;
}
.plat-tag.soon {
  background: rgb(148 163 184 / 0.2);
  color: #94a3b8;
}
.main {
  flex: 1;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
.topbar {
  background: #fff;
  border-bottom: 1px solid #e5e7eb;
  padding: 1.1rem 1.75rem 0.85rem;
}
.page-title {
  margin: 0;
  font-size: 1.35rem;
  font-weight: 700;
  letter-spacing: -0.03em;
}
.page-desc {
  margin: 0.45rem 0 0;
  max-width: 42em;
  font-size: 0.875rem;
  line-height: 1.55;
  color: #475569;
  letter-spacing: 0.01em;
}
.func-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.15rem;
  padding: 0 1.75rem;
  background: rgb(255 255 255 / 0.82);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid rgb(0 0 0 / 0.06);
}
.func-tab {
  padding: 0.6rem 0.85rem;
  font-size: 0.84rem;
  font-weight: 500;
  color: #64748b;
  text-decoration: none;
  border-bottom: 2px solid transparent;
  margin-bottom: -1px;
  border-radius: 8px 8px 0 0;
  transition: color 0.15s, border-color 0.15s, background 0.15s;
}
.func-tab:hover {
  color: #1d4ed8;
  background: rgb(241 245 249 / 0.9);
}
.func-tab.active {
  color: #1d4ed8;
  border-bottom-color: #3b82f6;
  background: #fff;
}
.content {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  overflow-x: hidden;
  padding: 1.25rem 1.75rem 2rem;
  max-width: 1200px;
  width: 100%;
  min-width: 0;
  margin: 0 auto;
  box-sizing: border-box;
  -webkit-overflow-scrolling: touch;
}
.content.content-wide {
  max-width: min(96vw, 1680px);
}
</style>
