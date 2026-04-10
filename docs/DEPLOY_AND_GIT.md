# 部署与 Git 仓库整理

## 1. 环境变量：仅一份 `.env`

| 文件 | 说明 |
|------|------|
| `market_assistant/.env` | 本地与服务器上的**唯一**配置（密钥、路径、Django、LLM） |
| `market_assistant/.env.example` | 模板，可提交仓库 |

已移除「仓库根目录 `.env`」与「`backend/.env`」第二加载源；Django 与 `crawler_copy/jd_pc_search/AI_crawler.py` 均从 `market_assistant/.env` 读取（`AI_crawler` 在导入时先加载该文件再解析 `LOW_GI_PROJECT_ROOT`）。

部署到新机器：

1. 复制 `market_assistant/.env.example` → `market_assistant/.env`
2. **可选**：设置 `LOW_GI_PROJECT_ROOT` 为单独数据盘的绝对路径；不设置时默认为**本仓库根目录**，数据写在 `./data/JD/`（启动 Django 时会自动创建该目录）
3. 设置 `DJANGO_SECRET_KEY`、`DJANGO_DEBUG=False`、生产域名下的 `DJANGO_ALLOWED_HOSTS` / `CORS_*` / `CSRF_*`
4. 若使用 LLM，填写 `OPENAI_*` 或 `LLM_*`

## 2. 功能是否只需本目录？

**是。** `market_assistant` 内含：

- Django API、流水线任务、入库与导出  
- 前端 Vue 工作台  
- 京东采集脚本副本 `backend/crawler_copy/jd_pc_search`（含 Node/Playwright 子进程调用）

**不要求**仓库外仍存在旧的 `crawler/jd_pc_search`。  

**运行时另需**（部分不在 Git 中）：

- 默认可写目录为仓库根下 `data/JD/`（`.gitignore` 已忽略）；若配置了 `LOW_GI_PROJECT_ROOT` 则数据在该路径下  
- 按任务配置放置 Cookie（路径须在有效数据根之下，如 `common/jd_cookie.txt`）  
- 本机已装 Node、流水线所需的 Playwright 等（与现有一致）

## 3. 远程 Git 只维护 `market_assistant`（推荐两种做法）

> **先备份仓库**，再在副本上操作；改写历史后需与团队约定 **`git push --force`**。

### 方案 A：保留历史，把子目录提成仓库根（git filter-repo）

适用于「当前仓库在上一级 `Low GI/`，只想提交 `market_assistant/` 里的内容且路径变为仓库根」。

1. 安装 [git-filter-repo](https://github.com/newren/git-filter-repo)（需单独安装，不是 Git 自带）。
2. 在**原仓库克隆的副本**中执行：

```bash
cd /path/to/Low-GI-repo-copy
git filter-repo --path market_assistant/ --path-rename market_assistant/:
```

3. 此时仓库根目录即为原 `market_assistant` 下的 `backend/`、`frontend/`、`docs/` 等。
4. 将 `origin` 改为新远程或清空原远程后强制推送：

```bash
git remote add origin <你的新仓库 URL>
git branch -M main
git push -u origin main --force
```

5. 旧远程若废弃，在 Git 平台将旧库归档或删除，避免误用。

### 方案 B：新仓库，不保留旧历史

适用于「从零起一个干净远程，只装当前代码」。

```bash
cd market_assistant
git init
git add .
git commit -m "chore: initial standalone market_assistant"
git remote add origin <新仓库 URL>
git branch -M main
git push -u origin main
```

之后本地开发只在 `market_assistant` 目录内 `git pull` / `git push`。

### 拆库后目录约定

- 克隆下来的**仓库根** = 现在的 `market_assistant`（含 `backend/`、`frontend/`、`docs/`）。  
- 文档中的路径仍写 `market_assistant/.env` 时，在「已拆库」情形下指**仓库根目录下的** `.env`（即 `.env` 在 clone 下来的根上）。

### 已从跟踪中移除误提交文件（旧 monorepo 根目录上执行）

若仍暂时保留大仓库，可在**原根目录**执行：

```bash
git rm -r --cached venv/ 2>/dev/null || true
git rm -r --cached data/ 2>/dev/null || true
git rm -r --cached .idea/ 2>/dev/null || true
git rm --cached .env 2>/dev/null || true
```

## 4. 生产构建（简要）

- **后端**：`pip install -r backend/requirements.txt`，`migrate`，用 gunicorn/uwsgi 等托管 WSGI，前面 Nginx 反代。  
- **前端**：`cd frontend && npm ci && npm run build`，将 `dist/` 由 Nginx 托管静态资源，并把 `/api` 反代到 Django；同时把生产环境的 CORS/CSRF 与 `vite.config.js` 开发代理区分配置（生产一般同源或显式写 API 域名）。

## 5. 与 `LOW_GI_PROJECT_ROOT` 的关系

流水线 CSV、跑批目录默认写在 `LOW_GI_PROJECT_ROOT/data/JD/...`。该路径**可以**在服务器上位于 Web 代码库之外（例如单独数据盘），只要在 `.env` 中指向正确绝对路径即可。
