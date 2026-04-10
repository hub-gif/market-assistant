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
    <section class="ma-card">
      <h2>新建采集任务</h2>
      <p class="lead">
        这里只配置<strong>京东搜索与采集脚本</strong>怎么跑（翻几页、采多少 SKU、Cookie 等）。
      </p>

      <div class="sc-block">
        <label class="sc-main-label">在京东搜什么</label>
        <input
          v-model="keyword"
          type="text"
          class="sc-input sc-input-wide"
          placeholder="例如：低GI（对应 PC 搜索框里的词）"
        />
      </div>

      <div class="sc-section">
        <h3 class="sc-title">采集范围（可选）</h3>
        <p class="sc-help">
          三项都可以<strong>留空</strong>：将完全使用当前爬虫脚本里的默认页数、默认上限。只有需要缩小或放大本次任务时再填。
        </p>
        <div class="sc-grid-3">
          <div class="sc-field">
            <label class="sc-label">最多深入多少个商品</label>
            <input
              v-model="maxSkus"
              type="number"
              min="1"
              class="sc-input"
              placeholder="留空＝脚本默认"
            />
            <span class="sc-tip">会拉商详、写评价样本的上限，不是全站总数。</span>
          </div>
          <div class="sc-field">
            <label class="sc-label">搜索列表从第几页</label>
            <input
              v-model="pageStart"
              type="number"
              min="1"
              class="sc-input"
              placeholder="起始页，如 1"
            />
            <span class="sc-tip">京东排序列表的「逻辑页码」起点。</span>
          </div>
          <div class="sc-field">
            <label class="sc-label">搜索列表到第几页</label>
            <input
              v-model="pageTo"
              type="number"
              min="1"
              class="sc-input"
              placeholder="结束页，如 3"
            />
            <span class="sc-tip">与上一项一起限定本次翻页范围。</span>
          </div>
        </div>
      </div>

      <details class="sc-details">
        <summary>登录与请求节奏（多数环境需要 Cookie）</summary>
        <p class="sc-help">
          若采集经常失败或要登录态，请配置 Cookie：二选一，<strong>粘贴优先于文件</strong>。在浏览器
          DevTools → Network 点开任意 jd.com 请求，复制请求头里的整行 Cookie（带
          <code>Cookie:</code> 前缀也可以）。保存后任务会写入临时文件供整条流水线（含 Node 签包与
          Playwright）使用，<strong>不需要</strong>改仓库里的 jd_cookie.txt。
        </p>
        <div class="sc-block">
          <label class="sc-label">Cookie 文件路径（可选）</label>
          <input
            v-model="cookieFilePath"
            type="text"
            class="sc-input sc-input-wide"
            placeholder="须在你本机 Low GI 项目根目录之下，例如 common/jd_cookie.txt"
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">或粘贴整份 Cookie 文本（可选）</label>
          <textarea
            v-model="cookieText"
            rows="4"
            class="sc-textarea"
            placeholder="与 jd_cookie.txt 单行相同；或粘贴「Cookie: …」整行"
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">请求间隔（可选）</label>
          <input
            v-model="requestDelay"
            type="text"
            class="sc-input sc-input-mid"
            placeholder="如 30-60，单位秒；留空＝脚本默认"
          />
          <span class="sc-tip">适当放慢可降低被风控概率。</span>
        </div>
      </details>

      <details class="sc-details">
        <summary>更多脚本参数（一般不用改）</summary>
        <p class="sc-help">以下对应流水线脚本里的高阶开关；不懂可全部留空。</p>
        <div class="sc-block">
          <label class="sc-label">运行结果目录（可选）</label>
          <input
            v-model="pipelineRunDir"
            type="text"
            class="sc-input sc-input-wide"
            placeholder="相对本仓库根下 data/JD 的子路径；留空则自动生成「时间戳_关键词」目录"
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">评价列表翻页范围（可选）</label>
          <input
            v-model="listPages"
            type="text"
            class="sc-input sc-input-mid"
            placeholder="如 1-2；控制每条 SKU 抓评价时的页数"
          />
        </div>
        <div class="sc-block">
          <label class="sc-label">调试编号 PVID（可选）</label>
          <input v-model="pvid" type="text" class="sc-input sc-input-mid" placeholder="一般留空" />
        </div>
        <div class="sc-block">
          <label class="sc-label">列表「应用场景」筛选</label>
          <select v-model="scenarioFilter" class="sc-select">
            <option value="">不覆盖脚本默认</option>
            <option value="on">强制开启</option>
            <option value="off">强制关闭</option>
          </select>
          <span class="sc-tip">
            与京东搜索列表接口里的「应用场景」筛选有关，和 Cookie、和报告里的「用途/场景」统计<strong>无关</strong>；只配 Cookie
            时请保持「不覆盖脚本默认」即可。
          </span>
        </div>
      </details>

      <button type="button" class="ma-btn ma-btn-primary sc-submit" :disabled="loading" @click="submitJob">
        {{ loading ? '提交中…' : '启动采集并生成报告' }}
      </button>
      <p v-if="error" class="ma-err">{{ error }}</p>
    </section>

    <p class="ma-muted">
      当前队列中约 <strong>{{ jobs.length }}</strong> 条任务记录；提交成功后将跳转到「任务列表」。
    </p>
  </div>
</template>

<style scoped>
.lead {
  margin: 0 0 1.1rem;
  font-size: 0.88rem;
  color: #4b5563;
  line-height: 1.55;
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
  padding: 0.45rem 0.55rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.88rem;
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
  padding: 0.45rem 0.55rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font: inherit;
  font-size: 0.88rem;
  resize: vertical;
}
.sc-select {
  max-width: 280px;
  padding: 0.45rem 0.55rem;
  border-radius: 6px;
  border: 1px solid #d1d5db;
  font: inherit;
  font-size: 0.88rem;
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
