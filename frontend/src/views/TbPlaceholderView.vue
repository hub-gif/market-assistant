<script setup>
import { RouterLink } from 'vue-router'
</script>

<template>
  <div class="ma-card placeholder">
    <h2>淘宝 / 天猫</h2>
    <p class="lead">该平台适配与数据采集能力仍在规划中。</p>
    <p class="ma-muted">后续将在此提供与京东侧一致的搜索、结果与营销分析流程。</p>
    <RouterLink to="/jd/search" class="back">← 返回京东商城</RouterLink>
  </div>
</template>

<style scoped>
.placeholder {
  max-width: 520px;
}
.lead {
  font-size: 1rem;
  color: #374151;
  margin: 0 0 0.75rem;
}
.back {
  display: inline-block;
  margin-top: 1.25rem;
  color: #2563eb;
  text-decoration: none;
  font-weight: 500;
}
.back:hover {
  text-decoration: underline;
}
</style>
