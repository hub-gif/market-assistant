# Market-Assistant（低 GI / 京东采集与竞品分析）

**本目录可作为独立 Git 仓库根目录**：克隆后只需配置 `.env` 与数据区，即可部署并实现当前工作台全部能力（任务、入库、浏览、报告、策略、LLM 等）。代码不依赖仓库外的 `crawler/` 等目录；爬虫副本在 `backend/crawler_copy/jd_pc_search`。

面向「前台事业部」的 Web 工作台：提交京东关键词采集任务、查看流水线产出、**库内分页浏览**已入库的搜索/商详/评价数据、生成竞品分析报告，并支持导出 JSON / CSV / Excel。

跑批 CSV、`pipeline_runs` 等默认写在**本仓库根目录**下的 `data/JD/`；若数据需放在其它磁盘，可在 `.env` 中设置 **LOW_GI_PROJECT_ROOT** 为绝对路径。

**环境变量**：全栈**只使用一份** `market_assistant/.env`（模板为 `.env.example`），勿在仓库根或其它子目录再建第二份 `.env`。

**研发对接**：任务产物、状态与 REST 能力见项目内 **流水线输出说明** 与 **OpenAPI 子集**；部署与 Git 整理见 **docs/DEPLOY_AND_GIT.md**。

---

## 技术栈

| 部分 | 说明 |
|------|------|
| 后端 | Django 5 + Django REST Framework，SQLite（可配置路径） |
| 前端 | Vue 3 + Vite 5 + Vue Router，开发时通过 Vite 代理访问 API |
| 采集 | 京东 PC 搜索侧脚本副本（与流水线任务联动） |

---

## 环境准备

- **Python** 3.11+（建议虚拟环境）
- **Node.js** 18+（用于前端）
- **唯一**环境文件：在本目录（`market_assistant/`）执行：

```bash
copy .env.example .env
```

编辑 `.env`，至少设置：

- **DJANGO_SECRET_KEY**；生产环境将 **DJANGO_DEBUG** 设为 False，并配置 **ALLOWED_HOSTS** 与 CORS/CSRF。
- **LOW_GI_PROJECT_ROOT**（可选）：不填则数据写在仓库根下 `data/JD/`；单独数据盘时再填绝对路径。
- 若使用配料识别、报告/策略 LLM：在同一文件填写 **OPENAI_*** 或 **LLM_***（与 `AI_crawler` 共用，无需另建 `.env`）。

---

## 启动后端

在后端子目录下执行：

```bash
cd market_assistant/backend

# 安装依赖（建议在 venv 中）
pip install -r requirements.txt

# 数据库迁移
python manage.py migrate

# 开发服务（默认 http://127.0.0.1:8000）
python manage.py runserver
```

管理后台（可选）：创建超级用户后访问 Django 管理地址。

---

## 启动前端

在前端子目录下执行：

```bash
cd market_assistant/frontend

# 首次安装依赖
npm install

# 开发模式（默认 http://127.0.0.1:5173）
npm run dev
```

浏览器打开本地开发地址。开发环境下，前端将 **API** 代理到后端端口，因此需**先启动后端**，再启动前端。

其他脚本：

```bash
npm run build    # 生产构建
npm run preview  # 本地预览构建结果
```

---

## 常用功能说明

1. **搜索采集**：创建京东关键词流水线任务（翻页、SKU 上限、Cookie 等；报告统计规则在「报告生成」）。  
2. **任务与结果**：查看任务状态；成功任务可 **库内浏览**、文件预览与下载、导出。  
3. **报告生成**：配置统计规则并重新生成分析报告文件。  
4. **报告查看**：在线预览、单文件下载、加载结构化摘要、**一键下载简报包**（ZIP）。  
5. **结构化摘要**：与报告同口径的规则化 JSON，供联调或其它工具使用。  
6. **市场策略制定**：选成功任务，可选填业务备注，生成策略向 Markdown（目标、战场、定位选项、支柱与行动；规则版、非大模型）。  

任务**成功结束后**会自动执行入库；也可在「库内浏览」里从批次目录重新入库。

---

## API 前缀

开发时 REST 接口默认在后端根地址下的 **/api**；前端通过同源代理访问即可。

---

## 相关文档

均在项目 **docs** 目录下，主要包括：项目进展与里程碑、流水线输出说明、演示与脱敏、工程说明等。

**部署、Git 仓库整理、单 `.env` 约定**：见 [docs/DEPLOY_AND_GIT.md](docs/DEPLOY_AND_GIT.md)。

---

## 目录结构（简要）

- **backend**：Django 与任务流水线 API  
- **frontend**：Vue + Vite 工作台  
- **docs**：说明、模板、演示与 OpenAPI 等  
