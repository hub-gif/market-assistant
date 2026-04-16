# Market-Assistant（低 GI / 京东采集与竞品分析）

**本目录可作为独立 Git 仓库根目录**：克隆后配置 `.env` 与数据区即可运行工作台（任务、入库、浏览、报告、策略、LLM 等）。爬虫副本在 `backend/crawler_copy/jd_pc_search`，不依赖仓库外的其它目录。

面向「前台事业部」的 Web 工作台：提交京东关键词采集任务、查看流水线产出、**库内分页浏览**已入库的搜索/商详/评价数据、生成竞品分析报告，并支持导出 JSON / CSV / Excel。

**研发对接**：任务产物、状态与 REST 能力见 **docs** 下的流水线输出说明与 OpenAPI 子集。

---

## 技术栈

| 部分 | 说明 |
|------|------|
| 后端 | Django 5 + Django REST Framework，SQLite（可配置路径） |
| 前端 | Vue 3 + Vite 5 + Vue Router，开发时通过 Vite 代理访问 API |
| 采集 | 京东 PC 搜索侧脚本副本（与流水线任务联动） |

---

## 部署

下文**仓库根**指克隆后的项目根目录（含 `backend/`、`frontend/`、`docs/`）。环境变量**只使用仓库根下一份** `.env`（模板为 `.env.example`），勿在 `backend/` 等子目录再建第二份。

### 环境变量（`.env`）

| 文件 | 说明 |
|------|------|
| 仓库根 `.env` | 运行时配置（勿提交 Git）；从 `.env.example` 复制 |
| `.env.example` | 模板，可随仓库分发 |

Django（`backend/config/settings.py`）与 `backend/crawler_copy/jd_pc_search/AI_crawler.py` 均从**仓库根**的 `.env` 加载。

**首次编辑建议：**

1. 复制：`cp .env.example .env`（Windows：`copy .env.example .env`）。
2. 填写 **`DJANGO_SECRET_KEY`**；生产将 **`DJANGO_DEBUG`** 设为 `False`，并配置 **`DJANGO_ALLOWED_HOSTS`**、**`CORS_ALLOWED_ORIGINS`**、**`CSRF_TRUSTED_ORIGINS`**（域名带协议，如 `https://app.example.com`）。
3. **`LOW_GI_PROJECT_ROOT`（可选）**：不设置时，跑批数据默认在仓库根 **`./data/JD/`**（启动 Django 时会创建）；单独数据盘则设为可写绝对路径，数据落在其下 `data/JD/...`。
4. 使用配料图识别、报告/策略 LLM 时：填写 **`OPENAI_*`** 或 **`LLM_*`**。

### 运行环境与依赖

| 层级 | 要求 | 说明 |
|------|------|------|
| 后端 | Python **3.11+** | 建议虚拟环境；依赖见 `backend/requirements.txt` |
| 前端 | **Node.js 18+** | 生产构建建议 `npm ci`（需 `package-lock.json`） |
| 流水线 / 采集 | **Node.js**；按需 **Playwright** | 调用 `backend/crawler_copy/jd_pc_search` 下脚本与子进程 |
| 数据目录 | 磁盘可写 | 默认 `./data/JD/`；或 `LOW_GI_PROJECT_ROOT` |
| Cookie | 本地文件（默认不入库） | `backend/crawler_copy/jd_pc_search/common/jd_cookie.txt`，或按工作台接口配置 |

### 首次安装（后端）

在仓库根进入 `backend/`，创建虚拟环境、安装依赖并迁移：

```bash
cd backend
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
python manage.py migrate
```

若要跑采集流水线：在本机安装 Node / Playwright（与现有开发环境一致），并准备好 Cookie。

### 开发环境（前后端联调）

| 顺序 | 目录 | 命令 | 说明 |
|------|------|------|------|
| 1 | `backend/` | `python manage.py runserver` | 默认 **http://127.0.0.1:8000**，REST 在 **`/api`** |
| 2 | `frontend/` | `npm install`（首次）、`npm run dev` | 默认 **http://127.0.0.1:5173** |

**须先启动后端，再启动前端。** `frontend/vite.config.js` 将 **`/api`** 代理到 `http://127.0.0.1:8000`，浏览器只访问 Vite 地址即可。

其它前端命令：`npm run build`（生产构建）、`npm run preview`（预览构建结果）。

可选：`python manage.py createsuperuser` 后访问 **http://127.0.0.1:8000/admin/**。

### 生产环境（构建与反向代理）

**后端**

1. 与开发相同 Python 版本，在 `backend/` 执行安装依赖与 `migrate`。  
2. 使用 **Gunicorn**（或 uWSGI 等）托管 WSGI，示例：

```bash
cd backend
gunicorn config.wsgi:application --bind 127.0.0.1:8000 --workers 3
```

3. 用 **Nginx**（或其它网关）将 **`/api`**（及如需的 `admin`）反代到上述进程。  
4. `.env` 中 `ALLOWED_HOSTS`、CORS、CSRF 与实际上线域名、协议一致。

**前端**

```bash
cd frontend
npm ci
npm run build
```

将 **`dist/`** 作为静态站点根目录托管。**推荐同源**：同域下静态资源 + `location /api/` 反代到 Gunicorn。若前后端不同域，配置好 CORS/CSRF，并注意 HTTPS 混合内容等问题。

生产**不使用** Vite 开发服务；`vite.config.js` 里的 `proxy` 仅在 `npm run dev` 时生效。

### 部署自检清单

- [ ] 已执行 `python manage.py migrate`  
- [ ] 生产环境已更换密钥，`DEBUG=False`  
- [ ] `ALLOWED_HOSTS` / `CORS_*` / `CSRF_*` 与真实访问地址一致  
- [ ] `data/JD/` 或 `LOW_GI_PROJECT_ROOT` 对应目录可写  
- [ ] 需要流水线时：Node、Playwright、Cookie 已就绪  

---

## 常用功能说明

1. **搜索采集**：创建京东关键词流水线任务（翻页、SKU 上限、Cookie 等；报告统计规则在「报告生成」）。  
2. **任务与结果**：查看任务状态；成功任务可 **库内浏览**、文件预览与下载、导出。  
3. **报告生成**：配置统计规则并重新生成分析报告文件。  
4. **报告查看**：在线预览、单文件下载、加载结构化摘要、**一键下载简报包**（ZIP）。  
5. **结构化摘要**：与报告**同一套计数规则**的规则化 JSON，供联调或其它工具使用。  
6. **市场策略制定**：选成功任务，可选填业务备注，生成策略向 Markdown（目标、战场、定位选项、支柱与行动；规则版、非大模型）。  

任务**成功结束后**会自动执行入库；也可在「库内浏览」里从批次目录重新入库。

---

## API 前缀

开发时 REST 默认在后端根地址下的 **`/api`**；本地通过 Vite 代理访问即可。

---

## 相关文档

均在 **docs** 目录：项目进展与里程碑、流水线输出说明、演示与脱敏、工程说明、OpenAPI 等。

---

## 目录结构（简要）

- **backend**：Django 与任务流水线 API  
- **frontend**：Vue + Vite 工作台  
- **docs**：说明、模板、演示与 OpenAPI 等  
