# 落盘契约（与 Playwright 半自动一致）

插件 + sidecar 写入的 JSON 须能被现有盘后脚本消费（**不修改** `backend/crawler_copy` / `backend/pipeline`）。

## 目录

```
{run_dir}/
  list/*.json
  detail/*.json
  comment/*.json
  graphic/*.json
```

## envelope 字段

| 字段 | 必填 | 说明 |
|------|------|------|
| `keyword` | 是 | 任务关键词 |
| `capture_kind` | 是 | `list` / `detail` / `comment` / `graphic` |
| `function_id` | 建议 | URL 中 `functionId` |
| `url` | 是 | 完整请求 URL |
| `status` | 建议 | HTTP 状态，默认 200 |
| `method` | 建议 | GET / POST |
| `parsed` | 是 | 接口 JSON 正文 |
| `resolved_sku` | detail/comment/graphic 建议 | 数位 SKU |
| `list_keyword` | list 可选 | `parsed.data.listKeyWord` |

监听阶段**不要**写：`dedupe_key`、`semiauto_detail_ingredients_text`。

盘后配料与 Playwright 半自动相同：`postprocess_semiauto_capture_json_dirs` 从 `graphic/*.json` 的
`graphicContent`（含 `data-lazyload`、CSS `background-image:url` 等）抽长图 URL 再识配料。

## functionId → kind

| functionId | kind |
|------------|------|
| `pc_search_searchWare` | list（仅当 `parsed.data.wareList` / `wareListPro` 为非空数组；`relwords` 等不落盘） |
| `pc_detailpage_wareBusiness` | detail |
| `getLegoWareDetailComment` | comment（商详首屏预览） |
| `getCommentListPage` | comment（评价弹层/翻页，`POST client.action`） |
| `pc_item_getWareGraphic` | graphic |

## 盘后与入库（现有命令）

```bash
# cwd = backend/crawler_copy
python -m sb_browser.platforms.jd_semiauto.postprocess.run_parse_semiauto_to_csv --dir "<run_dir>"

# cwd = backend
python manage.py ingest_pipeline_dataset --create --run-dir "sb_cdp_api_semiauto/<批次>" --keyword <词>
```

可选 Django shell：`finish_semiauto_after_browser(job_id)`（`pipeline.semiauto_tasks` 已有）。
