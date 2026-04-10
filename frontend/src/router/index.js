import { createRouter, createWebHistory } from 'vue-router'
import AppShell from '../layouts/AppShell.vue'
import JdSearchView from '../views/jd/JdSearchView.vue'
import JdResultsView from '../views/jd/JdResultsView.vue'
import JdDatasetBrowseView from '../views/jd/JdDatasetBrowseView.vue'
import JdAnalysisView from '../views/jd/JdAnalysisView.vue'
import JdAnalysisBuildView from '../views/jd/JdAnalysisBuildView.vue'
import JdStrategyView from '../views/jd/JdStrategyView.vue'
import JdStrategyBuildView from '../views/jd/JdStrategyBuildView.vue'
import TbPlaceholderView from '../views/TbPlaceholderView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      component: AppShell,
      children: [
        { path: '', redirect: '/jd/search' },
        {
          path: 'jd/search',
          name: 'jd-search',
          component: JdSearchView,
          meta: { platform: 'jd', tab: 'search', title: '搜索采集' },
        },
        {
          path: 'jd/results',
          name: 'jd-results',
          component: JdResultsView,
          meta: { platform: 'jd', tab: 'results', title: '任务列表' },
        },
        {
          path: 'jd/dataset',
          name: 'jd-dataset',
          component: JdDatasetBrowseView,
          meta: {
            platform: 'jd',
            tab: 'dataset',
            title: '库内数据浏览',
          },
        },
        {
          path: 'jd/analysis',
          redirect: { path: '/jd/analysis-view' },
        },
        {
          path: 'jd/analysis-build',
          name: 'jd-analysis-build',
          component: JdAnalysisBuildView,
          meta: { platform: 'jd', tab: 'analysis-build', title: '报告生成' },
        },
        {
          path: 'jd/analysis-view',
          name: 'jd-analysis-view',
          component: JdAnalysisView,
          meta: { platform: 'jd', tab: 'analysis-view', title: '报告查看' },
        },
        {
          path: 'jd/strategy',
          redirect: { path: '/jd/strategy-view' },
        },
        {
          path: 'jd/strategy-build',
          name: 'jd-strategy-build',
          component: JdStrategyBuildView,
          meta: { platform: 'jd', tab: 'strategy-build', title: '策略生成' },
        },
        {
          path: 'jd/strategy-view',
          name: 'jd-strategy-view',
          component: JdStrategyView,
          meta: { platform: 'jd', tab: 'strategy-view', title: '策略稿预览' },
        },
        {
          path: 'tb',
          name: 'tb',
          component: TbPlaceholderView,
          meta: { platform: 'tb', title: '淘宝 / 天猫' },
        },
      ],
    },
  ],
})

export default router
