<script setup>
defineProps({
  marketRows: { type: Array, required: true },
})
defineEmits(['add-market', 'remove-market'])
</script>

<template>
  <div>

    <div class="rc-section">
      <h4 class="rc-subtitle">1. 外部市场信息（可选）</h4>
      <p class="rc-help">若手边有第三方市场规模、增速等摘录，可填在表里，报告会多一节说明；不需要可整表留空。</p>
      <div class="rc-market-wrap">
        <table class="rc-market">
          <colgroup>
            <col class="rc-col-ind" />
            <col class="rc-col-val" />
            <col class="rc-col-src" />
            <col class="rc-col-year" />
            <col class="rc-col-act" />
          </colgroup>
          <thead>
            <tr>
              <th>指标</th>
              <th>数值与说明</th>
              <th>来源</th>
              <th>年份</th>
              <th />
            </tr>
          </thead>
          <tbody>
            <tr v-for="(r, i) in marketRows" :key="'m' + i">
              <td><input v-model="r.indicator" type="text" class="rc-input rc-td" placeholder="可选" /></td>
              <td><input v-model="r.value_and_scope" type="text" class="rc-input rc-td" placeholder="可选" /></td>
              <td><input v-model="r.source" type="text" class="rc-input rc-td" placeholder="可选" /></td>
              <td><input v-model="r.year" type="text" class="rc-input rc-td" placeholder="可选" /></td>
              <td><button type="button" class="ma-btn ma-btn-secondary rc-mini" @click="$emit('remove-market', i)">删行</button></td>
            </tr>
          </tbody>
        </table>
      </div>
      <button type="button" class="ma-btn ma-btn-secondary rc-add" @click="$emit('add-market')">添加一行</button>
    </div>
  </div>
</template>

<style scoped>
.rc-section {
  margin: 1.1rem 0;
  padding-top: 0.75rem;
  border-top: 1px solid #e5e7eb;
}
.rc-section:first-of-type {
  border-top: none;
  padding-top: 0;
}
.rc-subtitle {
  margin: 0 0 0.35rem;
  font-size: 0.95rem;
  font-weight: 600;
  color: #374151;
}
.rc-help {
  margin: 0 0 0.65rem;
  font-size: 0.82rem;
  color: #6b7280;
  line-height: 1.5;
}
.rc-input {
  flex: 1;
  min-width: 140px;
  padding: 0.45rem 0.55rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.88rem;
  box-sizing: border-box;
}
.rc-input.rc-td {
  flex: none;
  display: block;
  min-width: 0;
  width: 100%;
  max-width: 100%;
  font-size: 0.8rem;
}
.rc-mini {
  font-size: 0.8rem;
  padding: 0.3rem 0.55rem;
  flex-shrink: 0;
}
.rc-add {
  margin-top: 0.5rem;
  font-size: 0.85rem;
}
.rc-market-wrap {
  overflow-x: auto;
  margin-bottom: 0.35rem;
}
.rc-market {
  width: 100%;
  max-width: 100%;
  table-layout: fixed;
  border-collapse: collapse;
  font-size: 0.8rem;
}
.rc-col-ind {
  width: 18%;
}
.rc-col-val {
  width: 34%;
}
.rc-col-src {
  width: 26%;
}
.rc-col-year {
  width: 12%;
}
.rc-col-act {
  width: 5.25rem;
}
.rc-market th,
.rc-market td {
  border: 1px solid #e5e7eb;
  padding: 0.35rem;
  vertical-align: middle;
  text-align: left;
  min-width: 0;
  overflow: hidden;
  word-wrap: break-word;
}
.rc-market td:last-child {
  overflow: visible;
  text-align: center;
  vertical-align: middle;
}
.rc-market th {
  background: #f3f4f6;
  font-weight: 600;
  color: #374151;
  white-space: nowrap;
}
.rc-market th:last-child {
  width: 5.25rem;
}
</style>
