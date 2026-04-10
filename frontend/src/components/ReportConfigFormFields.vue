<script setup>
defineProps({
  focusWordRows: { type: Array, required: true },
  scenarioGroups: { type: Array, required: true },
  marketRows: { type: Array, required: true },
})
defineEmits([
  'add-focus',
  'remove-focus',
  'add-scenario',
  'remove-scenario',
  'add-market',
  'remove-market',
])
</script>

<template>
  <div>
    <div class="rc-section">
      <h4 class="rc-subtitle">1. 评价里要统计的「关注词」</h4>
      <p class="rc-help">报告会数这些词在评价里出现了多少次（适合看大家常提什么，例如口感、控糖、价格等）。</p>
      <div class="rc-rows">
        <div v-for="(row, i) in focusWordRows" :key="'f' + i" class="rc-inline">
          <input v-model="row.text" type="text" class="rc-input" placeholder="输入一个词，如：控糖" />
          <button type="button" class="ma-btn ma-btn-secondary rc-mini" @click="$emit('remove-focus', i)">删除</button>
        </div>
      </div>
      <button type="button" class="ma-btn ma-btn-secondary rc-add" @click="$emit('add-focus')">添加词</button>
    </div>

    <div class="rc-section">
      <h4 class="rc-subtitle">2. 用途 / 场景分组</h4>
      <p class="rc-help">
        每一组有一个<strong>场景名称</strong>，和若干<strong>触发词</strong>。若一条评价里出现了其中任意一个词，这条评价就会算进该场景（一条评价可以同时属于多个场景）。触发词请用顿号、逗号或换行分开。
      </p>
      <div v-for="(g, i) in scenarioGroups" :key="'s' + i" class="rc-scenario-card">
        <label class="rc-label">场景名称</label>
        <input v-model="g.label" type="text" class="rc-input" placeholder="如：早餐 / 代餐" />
        <label class="rc-label">触发词</label>
        <textarea
          v-model="g.triggersText"
          class="rc-textarea"
          rows="2"
          placeholder="如：早餐、代餐、早饭（可用顿号或换行分隔）"
        />
        <button type="button" class="ma-btn ma-btn-secondary rc-mini" @click="$emit('remove-scenario', i)">删除本组</button>
      </div>
      <button type="button" class="ma-btn ma-btn-secondary rc-add" @click="$emit('add-scenario')">添加场景组</button>
    </div>

    <div class="rc-section">
      <h4 class="rc-subtitle">3. 外部市场信息（可选）</h4>
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
.rc-rows {
  display: flex;
  flex-direction: column;
  gap: 0.45rem;
}
.rc-inline {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem;
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
/* 表内输入：不占 flex，宽度受列约束，避免撑进邻列 */
.rc-input.rc-td {
  flex: none;
  display: block;
  min-width: 0;
  width: 100%;
  max-width: 100%;
  font-size: 0.8rem;
}
.rc-textarea {
  width: 100%;
  max-width: 100%;
  box-sizing: border-box;
  padding: 0.45rem 0.55rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.88rem;
  resize: vertical;
  margin: 0.35rem 0 0.5rem;
}
.rc-label {
  display: block;
  font-size: 0.8rem;
  font-weight: 500;
  color: #4b5563;
  margin-top: 0.35rem;
}
.rc-label:first-of-type {
  margin-top: 0;
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
.rc-scenario-card {
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 0.75rem 0.85rem;
  margin-bottom: 0.65rem;
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
