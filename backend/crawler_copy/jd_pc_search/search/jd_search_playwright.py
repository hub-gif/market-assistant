# -*- coding: utf-8 -*-
"""
get_h5st（Node）拼 URL + Header，Playwright/Chromium 发 GET（浏览器 TLS，避免 Node 直连 jfe 403）。

响应解析与 ``jd_h5_search_requests.py`` 一致：``_detect_blocked``、
``parse_items_and_pc_search_s_step_from_response_body``（优先读响应里的下一跳 s，否则启发式）、
多页合并去重、CSV / 文件输出。

依赖: pip install playwright && playwright install chromium

用法: 修改下方「运行配置」后执行 ``python jd_search_playwright.py``（无命令行参数）。

多页采集逻辑与落盘辅助函数在 ``collect_pc_search_items.py``，供本脚本与上级 ``jd_keyword_pipeline.py`` 共用。
"""

from __future__ import annotations

import csv
import json
import sys
import time
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

from playwright.sync_api import sync_playwright

from collect_pc_search_items import (
    collect_pc_search_export_rows,
    save_pc_request_record,
    save_pc_search_response_raw,
)
from jd_h5_search_requests import CSV_FIELDS, export_pc_search_request_json, parse_request_delay_range

_JD_PC_SEARCH = Path(__file__).resolve().parents[1]
if str(_JD_PC_SEARCH) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH))
from _low_gi_root import low_gi_project_root  # noqa: E402

# ---------------------------------------------------------------------------
# 运行配置（按需改这里）
# ---------------------------------------------------------------------------
# 路径：副本通过 LOW_GI_PROJECT_ROOT 指向「Low GI」根目录
_PROJECT_ROOT = low_gi_project_root()
_PROJECT_DATA = _PROJECT_ROOT / "data" / "JD"

# QUERY：搜索关键词，写入 pc_search 请求与 Referer
QUERY = "低GI"
# PVID：与 search.jd.com 结果页 URL 中 pvid 一致时填入；空则用 Node 内默认值
PVID = ""
# PAGE_START：起始逻辑页 L（从 1 起）；每逻辑页固定 2 次 pc_search（body.page 为 2L−1、2L）
PAGE_START = 1
# PAGE_TO：结束逻辑页（含）；None 表示只采 PAGE_START 这一逻辑页
PAGE_TO = 10
# PAGE_DELAY_SEC：相邻两个逻辑页之间的休眠秒数（与 REQUEST_DELAY 可叠加）
PAGE_DELAY_SEC = 1.2
# REQUEST_DELAY：每次 pc_search 完成后再发起下一次前的随机等待，如 "30-60"；None 关闭
REQUEST_DELAY = "30-60"
# HEADED：True 有头浏览器，便于调试
HEADED = False
# FORMAT："items" 解析商品列表；"raw" 仅打一包原始 JSON（勿与多页组合）
FORMAT = "items"
# RAW_SINGLE：True 等同只采一包原始响应（与 FORMAT=raw 类同，勿设 PAGE_TO 多页）
RAW_SINGLE = False
# CSV_OUTPUT：True 输出 CSV 列（需 FORMAT=items）；False 输出 JSON 数组
CSV_OUTPUT = True
# OUT_PATH：结果文件；空则 CSV/JSON 打到 stdout
OUT_PATH = str(_PROJECT_DATA / "jd_p1_10_2.csv")
# SAVE_PC_SEARCH_JS_DIR：非空则每次 pc_search 后把响应全文落盘到此目录（对照 Network）
SAVE_PC_SEARCH_JS_DIR = str(_PROJECT_DATA / "pc_raw_p1_10_2")
# PRETTY_RAW_JSON：与 SAVE 目录合用 True 时保存为缩进 .json，否则单行 .js
PRETTY_RAW_JSON = True
# RECORD_REQUESTS_DIR：非空则每次请求写入 URL、query、body、请求头、HTTP 状态等 JSON
RECORD_REQUESTS_DIR = str(_PROJECT_DATA / "pc_requests_p1_10_2")
# FETCH_RETRIES：同一 body.page/s 遇空包或零解析时，除首次外最多再试次数
FETCH_RETRIES = 3
# FETCH_RETRY_DELAY_SEC：上述重试间隔（秒），不走 REQUEST_DELAY
FETCH_RETRY_DELAY_SEC = 3.0
# ---------------------------------------------------------------------------


def _dump_out(text: str, out_path: str | None) -> None:
    if out_path:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


def main() -> None:
    fmt = (FORMAT or "items").strip().lower()
    if fmt not in ("raw", "items"):
        print('FORMAT 须为 "items" 或 "raw"', file=sys.stderr)
        sys.exit(2)
    args = SimpleNamespace(
        q=QUERY,
        pvid=(PVID or "").strip(),
        page=int(PAGE_START),
        page_to=PAGE_TO,
        page_delay=float(PAGE_DELAY_SEC),
        request_delay=REQUEST_DELAY,
        headed=bool(HEADED),
        format=fmt,
        raw=bool(RAW_SINGLE),
        csv=bool(CSV_OUTPUT),
        out=(OUT_PATH or "").strip() or None,
        save_pc_search_js=(SAVE_PC_SEARCH_JS_DIR or "").strip() or None,
        pretty_raw_json=bool(PRETTY_RAW_JSON),
        record_requests=(RECORD_REQUESTS_DIR or "").strip() or None,
        fetch_retries=int(FETCH_RETRIES),
        fetch_retry_delay=float(FETCH_RETRY_DELAY_SEC),
    )

    if args.page_to is not None and args.page_to < args.page:
        print("PAGE_TO 必须大于等于 PAGE_START", file=sys.stderr)
        sys.exit(2)
    if args.raw and args.page_to is not None:
        print("RAW_SINGLE=True 时不要设置多页 PAGE_TO", file=sys.stderr)
        sys.exit(2)
    if args.csv and (args.format == "raw" or args.raw):
        print("CSV_OUTPUT 需与 FORMAT=items 同时使用", file=sys.stderr)
        sys.exit(2)

    req_delay_range: tuple[float, float] | None = None
    if args.request_delay:
        try:
            req_delay_range = parse_request_delay_range(str(args.request_delay).strip())
        except ValueError as e:
            print(f"[京东] REQUEST_DELAY 无效: {e}", file=sys.stderr)
            sys.exit(2)
    if args.fetch_retries < 0:
        print("[京东] FETCH_RETRIES 不能为负", file=sys.stderr)
        sys.exit(2)

    page_start = max(1, args.page)
    pe = args.page_to if args.page_to is not None else page_start
    pe = max(page_start, pe)

    want_raw = args.format == "raw" or args.raw
    save_js_dir = (
        Path(args.save_pc_search_js).resolve()
        if args.save_pc_search_js
        else None
    )
    record_req_dir = (
        Path(args.record_requests).resolve()
        if args.record_requests
        else None
    )
    node_pvid = (args.pvid or "").strip() or None

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        context = browser.new_context()
        try:
            if want_raw:
                data = export_pc_search_request_json(
                    args.q, 1, s=1, pvid=node_pvid
                )
                url = data["url"]
                headers = {str(k): str(v) for k, v in data["headers"].items()}
                resp = context.request.get(url, headers=headers)
                print("HTTP", resp.status, resp.status_text, file=sys.stderr)
                ct = resp.headers.get("content-type", "")
                if ct:
                    print("content-type:", ct, file=sys.stderr)
                body = resp.text()
                raw_seq = 1
                if record_req_dir is not None:
                    save_pc_request_record(
                        record_req_dir,
                        raw_seq,
                        label="raw_single",
                        keyword=args.q,
                        api_page=1,
                        api_s=1,
                        log_ctx="--format raw",
                        url=url,
                        headers=headers,
                        http_status=resp.status,
                        status_text=resp.status_text or "",
                        content_type=ct,
                    )
                if save_js_dir is not None:
                    save_pc_search_response_raw(
                        save_js_dir,
                        raw_seq,
                        body,
                        label="raw_single",
                        req_page=1,
                        req_s=1,
                        pretty=args.pretty_raw_json,
                    )
                try:
                    pretty = json.dumps(json.loads(body), ensure_ascii=False, indent=2)
                    _dump_out(pretty + ("\n" if not pretty.endswith("\n") else ""), args.out)
                except json.JSONDecodeError:
                    text = (body[:8000] if body else "(空)") + "\n"
                    _dump_out(text, args.out)
                return

            export_rows = collect_pc_search_export_rows(
                context,
                args,
                page_start=page_start,
                pe=pe,
                req_delay_range=req_delay_range,
                save_js_dir=save_js_dir,
                record_req_dir=record_req_dir,
                node_pvid=node_pvid,
            )
            if args.csv:
                buf = StringIO()
                w = csv.DictWriter(
                    buf, fieldnames=list(CSV_FIELDS), extrasaction="ignore"
                )
                w.writeheader()
                w.writerows(export_rows)
                csv_text = buf.getvalue()
                if args.out:
                    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
                    Path(args.out).write_text("\ufeff" + csv_text, encoding="utf-8")
                else:
                    sys.stdout.write(csv_text)
            else:
                txt = json.dumps(export_rows, ensure_ascii=False, indent=2)
                _dump_out(txt + "\n", args.out)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
