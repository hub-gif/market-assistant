# 京东半自动 Chrome 插件 + Sidecar

## 流程（对齐 Web 半自动）

1. **启动半自动** → 自动建 `run_dir`、可选建 Django 任务（`waiting_login`）
2. **确认登录** → 开始落盘（`listening`）；此后可翻页、关 popup，**监听不中断**
3. **结束任务** → 解析 CSV + 入库（可取消勾选「自动入库」则只落盘）

## 状态不丢

- `captureEnabled` / `sessionPhase` 存在 **chrome.storage.local**，由 **background** 维护
- 京东 **翻页** 只重载页面 hook（有防重复标记），**不会**重置会话
- popup 打开只做 **软同步**（`/job/status` 更新计数），**不会**把阶段打回 idle

## 启动

```powershell
cd extensions\jd_semiauto_chrome\sidecar
$env:LOW_GI_PROJECT_ROOT = "d:\PythonProject\Low GI\market_assistant"
python __main__.py
```

Chrome 加载 `extension/` → 重载扩展后使用（当前 **0.2.0**）。

## 快速模式（可选）

仍支持旧 API：`POST /session/start` + 指定 `run_dir` 后立即 listening（无登录确认、无自动入库）。

## API

| 路径 | 说明 |
|------|------|
| GET `/job/status` | 阶段、计数、run_dir |
| POST `/job/start` | `{ "keyword", "create_django_job": true }` |
| POST `/job/confirm-login` | 进入监听 |
| POST `/job/stop` | `{ "run_postprocess": true }` |
| POST `/capture/batch` | 扩展批量写 JSON |
