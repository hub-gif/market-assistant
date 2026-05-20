<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api, refreshJobs, useJobs } from '../../composables/useJobs'

const router = useRouter()
const { jobs } = useJobs()

const keyword = ref('低GI')
const maxSkus = ref('')
const pageStart = ref('')
const pageTo = ref('')
const pipelineRunDir = ref('')
const cookieFilePath = ref('')
const cookieText = ref('')
const pvid = ref('')
const requestDelay = ref('')
const listPages = ref('')
const scenarioFilter = ref('')

const loading = ref(false)
const error = ref('')

function appendOptionalBody(body) {
  const ms = maxSkus.value === '' ? null : Number(maxSkus.value)
  const ps = pageStart.value === '' ? null : Number(pageStart.value)
  const pt = pageTo.value === '' ? null : Number(pageTo.value)
  if (ms != null && !Number.isNaN(ms)) body.max_skus = ms
  if (ps != null && !Number.isNaN(ps)) body.page_start = ps
  if (pt != null && !Number.isNaN(pt)) body.page_to = pt

  const prd = pipelineRunDir.value.trim()
  if (prd) body.pipeline_run_dir = prd
  const cfp = cookieFilePath.value.trim()
  if (cfp) body.cookie_file_path = cfp
  const ct = cookieText.value.trim()
  if (ct) body.cookie_text = ct
  const pv = pvid.value.trim()
  if (pv) body.pvid = pv
  const rd = requestDelay.value.trim()
  if (rd) body.request_delay = rd
  const lp = listPages.value.trim()
  if (lp) body.list_pages = lp
  if (scenarioFilter.value === 'on') body.scenario_filter_enabled = true
  if (scenarioFilter.value === 'off') body.scenario_filter_enabled = false
}

async function submitJob() {
  error.value = ''
  loading.value = true
  const body = { keyword: keyword.value.trim(), platform: 'jd' }
  appendOptionalBody(body)

  const r = await api('/api/jobs/', { method: 'POST', body: JSON.stringify(body) })
  loading.value = false
  if (!r.ok) {
    let t = await r.text()
    try {
      t = JSON.stringify(JSON.parse(t), null, 2)
    } catch {
      /* keep */
    }
    error.value = t
    return
  }
  try {
    await refreshJobs()
  } catch (e) {
    error.value = String(e)
    return
  }
  router.push('/jd/results')
}
</script>

<template>
  <div>
    <section class="ma-card ma-card-elevated">
      <h2>新建采集</h2>
      <p class="lead">搜索词与范围；展开项可配登录与高级参数。</p>

      <div class="sc-block">
        <label class="sc-main-label">搜索词</label>
        <el-input v-model="keyword" class="sc-ep sc-ep--wide" placeholder="例如：低GI" clearable />
      </div>

      <div class="sc-section">
        <h3 class="sc-title">范围（可选）</h3>
        <p class="sc-help">留空则使用默认；需要限制页数或数量时再填。</p>
        <div class="sc-grid-3">
          <div class="sc-field">
            <label class="sc-label">最多采多少款</label>
            <el-input
              v-model="maxSkus"
              class="sc-ep"
              type="number"
              :min="1"
              placeholder="留空＝默认"
            />
          </div>
          <div class="sc-field">
            <label class="sc-label">列表起始页</label>
            <el-input v-model="pageStart" class="sc-ep" type="number" :min="1" placeholder="如 1" />
          </div>
          <div class="sc-field">
            <label class="sc-label">列表结束页</label>
            <el-input v-model="pageTo" class="sc-ep" type="number" :min="1" placeholder="如 3" />
          </div>
        </div>
      </div>

      <details class="sc-details">
        <summary>登录与节奏</summary>
        <p class="sc-help">需要登录态或降频时配置；Cookie 可文件或粘贴。</p>
        <div class="sc-block">
          <label class="sc-label">Cookie 文件路径（可选）</label>
          <el-input
            v-model="cookieFilePath"
            class="sc-ep sc-ep--wide"
            placeholder="须在你本机 Low GI 项目根目录之下，例如 common/jd_cookie.txt"
            clearable
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">或粘贴整份 Cookie 文本（可选）</label>
          <el-input
            v-model="cookieText"
            class="sc-ep sc-ep--wide"
            type="textarea"
            :rows="4"
            placeholder="与 jd_cookie.txt 单行相同；或粘贴「Cookie: …」整行"
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">请求间隔（可选）</label>
          <el-input
            v-model="requestDelay"
            class="sc-ep sc-ep--mid"
            placeholder="如 30-60，单位秒；留空＝脚本默认"
            clearable
          />
        </div>
      </details>

      <details class="sc-details">
        <summary>更多（一般留空）</summary>
        <p class="sc-help">高阶参数，可全部留空。</p>
        <div class="sc-block">
          <label class="sc-label">运行结果目录（可选）</label>
          <el-input
            v-model="pipelineRunDir"
            class="sc-ep sc-ep--wide"
            placeholder="相对本仓库根下 data/JD 的子路径；留空则自动生成「时间戳_关键词」目录"
            clearable
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">评价列表翻页范围（可选）</label>
          <el-input
            v-model="listPages"
            class="sc-ep sc-ep--mid"
            placeholder="如 1-2；控制每条 SKU 抓评价时的页数"
            clearable
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">调试编号 PVID（可选）</label>
          <el-input v-model="pvid" class="sc-ep sc-ep--mid" placeholder="一般留空" clearable />
        </div>
        <div class="sc-block">
          <label class="sc-label">列表「应用场景」筛选</label>
          <el-select v-model="scenarioFilter" class="sc-ep sc-ep--mid" placement="bottom-start" clearable>
            <el-option label="不覆盖脚本默认" :value="''" />
            <el-option label="强制开启" value="on" />
            <el-option label="强制关闭" value="off" />
          </el-select>
        </div>
      </details>

      <el-button type="primary" class="sc-submit" :disabled="loading" @click="submitJob">
        {{ loading ? '提交中…' : '启动采集' }}
      </el-button>
      <p v-if="error" class="ma-err">{{ error }}</p>
    </section>

    <p class="ma-muted sc-foot">已有 {{ jobs.length }} 条任务 · 提交后进入任务页</p>
  </div>
</template>

<style scoped>
.lead {
  margin: 0 0 1.1rem;
  font-size: 0.8rem;
  color: #6b7280;
  line-height: 1.45;
}
.sc-foot {
  margin-top: 0.5rem;
  font-size: 0.8rem;
}
.sc-block {
  margin-bottom: 1rem;
}
.sc-section {
  margin: 1.15rem 0 1.25rem;
  padding: 1rem 1.1rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fafafa;
}
.sc-title {
  margin: 0 0 0.35rem;
  font-size: 1rem;
  font-weight: 600;
  color: #1f2937;
}
.sc-help {
  margin: 0 0 0.85rem;
  font-size: 0.82rem;
  color: #6b7280;
  line-height: 1.5;
}
.sc-main-label {
  display: block;
  font-size: 0.88rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 0.4rem;
}
.sc-label {
  display: block;
  font-size: 0.82rem;
  font-weight: 500;
  color: #4b5563;
  margin-bottom: 0.35rem;
}
.sc-grid-3 {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}
.sc-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}
.sc-input {
  width: 100%;
  box-sizing: border-box;
  min-height: 2rem;
  padding: 0.35rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.85rem;
  line-height: 1.35;
}
.sc-input:focus {
  outline: none;
  border-color: #2563eb;
  box-shadow: 0 0 0 1px #2563eb33;
}
.sc-input-wide {
  max-width: 100%;
}
.sc-input-mid {
  max-width: 320px;
}
.sc-textarea {
  width: 100%;
  box-sizing: border-box;
  min-height: 2.75rem;
  padding: 0.4rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.85rem;
  line-height: 1.45;
  resize: vertical;
}
.sc-textarea:focus {
  outline: none;
  border-color: #2563eb;
  box-shadow: 0 0 0 1px #2563eb33;
}
.sc-select {
  max-width: 280px;
  min-height: 2rem;
  padding: 0.35rem 0.5rem;
  border-radius: 6px;
  border: 1px solid #d1d5db;
  font: inherit;
  font-size: 0.85rem;
  line-height: 1.3;
  color: #111827;
  background: #fff;
  box-sizing: border-box;
}
.sc-select:focus {
  outline: none;
  border-color: #2563eb;
  box-shadow: 0 0 0 1px #2563eb33;
}
.sc-tip {
  font-size: 0.75rem;
  color: #9ca3af;
  line-height: 1.35;
}
.sc-details {
  margin: 0.85rem 0;
  padding: 0.65rem 0.85rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fff;
}
.sc-details summary {
  cursor: pointer;
  font-weight: 600;
  font-size: 0.9rem;
  color: #374151;
  user-select: none;
}
.sc-details[open] summary {
  margin-bottom: 0.5rem;
}
.sc-submit {
  margin-top: 1rem;
}
.ma-muted {
  color: #64748b;
  font-size: 0.88rem;
}
</style>
