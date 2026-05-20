# -*- coding: utf-8 -*-
"""Playwright：京东 list/detail/comment/graphic 监听落盘（argv 与 semiauto_tasks 对齐；``--restart-file`` 仅占位）。见 ``--help``。"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# playwright_listen -> jd_semiauto -> platforms -> sb_browser -> crawler_copy
_CRAWLER_COPY = Path(__file__).resolve().parents[4]
if str(_CRAWLER_COPY) not in sys.path:
    sys.path.insert(0, str(_CRAWLER_COPY))

from playwright.sync_api import sync_playwright

from sb_browser.platforms.jd_semiauto.common.low_gi_root import low_gi_project_root
from sb_browser.platforms.jd_semiauto.devtools_txt.devtools_cn_export_parse import (
    sku_from_warebusiness_get_url,
)
from sb_browser.platforms.jd_semiauto.playwright_listen import api_capture_kind as _kind
from sb_browser.platforms.jd_semiauto.seleniumbase_cdp.notes_sink_jd import _slug_kw, _slug_sku

running = True


def stop_handler(_sig, _frame):
    global running
    print("\n正在停止监听...", flush=True)
    running = False


signal.signal(signal.SIGINT, stop_handler)


def _safe_browser_close(browser) -> None:
    """Ctrl+C 或用户先关窗口时，驱动连接可能已断，避免未捕获异常弄脏退出码。"""
    try:
        browser.close()
    except Exception as exc:
        err = str(exc).lower()
        if any(
            s in err
            for s in (
                "connection closed",
                "econnrefused",
                "browser has been closed",
                "target closed",
                "websocket",
            )
        ):
            return
        print(f"[警告] browser.close: {exc}", file=sys.stderr, flush=True)


def _touch_status(run_dir: Path, name: str) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / name).touch()
    except OSError:
        pass


def _run_postprocess(run_dir: Path) -> None:
    from sb_browser.platforms.jd_semiauto.postprocess.run_parse_semiauto_to_csv import run as parse_run

    rd = run_dir.resolve()
    print(f"\n[postprocess] {rd}", flush=True)
    n_list, n_detail, n_comment, n_merged = parse_run(rd)
    print(
        f"[postprocess] done list={n_list} detail={n_detail} comment={n_comment} merged={n_merged}",
        flush=True,
    )


def is_json_response(response) -> bool:
    try:
        content_type = response.headers.get("content-type", "").lower()
        if not content_type:
            return False
        return (
            "application/json" in content_type
            or "text/json" in content_type
            or "+json" in content_type
        )
    except Exception:
        return False


def wait_login_confirm(*, login_file: Path | None, skip: bool) -> None:
    if skip:
        print("[登录] 已跳过等待（--skip-login-wait）", flush=True)
        return
    if login_file is not None:
        lf = login_file.resolve()
        print(f"[登录] 完成登录后请创建文件: {lf}", flush=True)
        while not lf.exists():
            time.sleep(0.3)
        print("[登录] 已确认，开始监听。", flush=True)
        return
    try:
        input("[登录] 在浏览器中完成登录后，按 Enter 开始监听…\n")
    except EOFError:
        pass


def _resolved_sku_for_envelope(kind: str, url: str, parsed: dict) -> str:
    """与 CDP 落盘对齐：非列表类写入 ``resolved_sku``（URL body ``skuId`` 优先）。"""
    if kind == "list":
        return ""
    sk = sku_from_warebusiness_get_url(url)
    if sk:
        return sk
    if not isinstance(parsed, dict):
        return ""
    dat = parsed.get("data")
    if isinstance(dat, dict):
        for key in ("skuId", "wareId"):
            s = str(dat.get(key) or "").strip()
            if s.isdigit() and len(s) >= 5:
                return s
    for key in ("skuId", "wareId"):
        s = str(parsed.get(key) or "").strip()
        if s.isdigit() and len(s) >= 5:
            return s
    return ""


def _list_key_word_from_parsed(data: dict) -> str:
    d = data.get("data")
    if isinstance(d, dict):
        return str(d.get("listKeyWord") or "").strip()
    return ""


def make_response_handler(
    out_dir: Path,
    *,
    keyword: str,
    verbose: bool,
    stats: dict[str, dict[str, int]],
):
    lock = threading.Lock()
    per_kind: dict[str, int] = {"list": 0, "detail": 0, "comment": 0, "graphic": 0}
    safe_kw = _slug_kw(keyword)

    def handle_response(response) -> None:
        try:
            url = response.url
            if _kind.classify_jd_aggregate(url=url, parsed=None) is None and not is_json_response(
                response
            ):
                return

            try:
                data = response.json()
            except Exception:
                return

            kind = _kind.classify_jd_aggregate(url=url, parsed=data)
            if kind is None:
                return

            with lock:
                per_kind[kind] += 1
                n = per_kind[kind]

            resolved_sku = _resolved_sku_for_envelope(kind, url, data)
            sku_part = (
                f"_sku_{_slug_sku(resolved_sku)}"
                if kind in ("detail", "comment", "graphic")
                else ""
            )
            fname = f"jd_{kind}_{n:04d}{sku_part}_kw_{safe_kw}.json"

            sub = out_dir / kind
            sub.mkdir(parents=True, exist_ok=True)
            path = sub / fname

            envelope = {
                "keyword": keyword,
                "capture_kind": kind,
                "resolved_sku": resolved_sku,
                "function_id": _kind.function_id_from_url(url) or "",
                "url": url,
                "status": response.status,
                "method": response.request.method,
                "parsed": data,
            }
            lk = _list_key_word_from_parsed(data)
            if lk:
                envelope["list_keyword"] = lk
            path.write_text(
                json.dumps(envelope, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[jd:{kind}] -> {path}", flush=True)

            if verbose:
                pretty = json.dumps(data, ensure_ascii=False, indent=2)
                print(pretty[:800], flush=True)
                if len(pretty) > 800:
                    print("...", flush=True)
        except Exception as exc:
            print(f"监听异常: {exc}", flush=True)

    stats["per_kind"] = per_kind
    return handle_response


def main() -> int:
    global running

    ap = argparse.ArgumentParser(description="Playwright 京东聚合 API 监听落盘")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="落盘目录；与 --run-dir 二选一；默认 data/JD/playwright_jd_captured/<时间戳>",
    )
    ap.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="任务根目录；写 .status_*（与同目录半自动约定的轮询文件一致；接入任务线程前可手动试跑）",
    )
    ap.add_argument(
        "--stop-file",
        type=Path,
        default=None,
        help="停止标记；默认 <落盘>/.stop_requested",
    )
    ap.add_argument(
        "--login-file",
        type=Path,
        default=None,
        help="登录确认文件（前端通常为 .login_confirmed）",
    )
    ap.add_argument(
        "--keyword",
        default="manual",
        help="任务关键词，写入落盘 JSON（与 SB 监听 seleniumbase_cdp.run_listen_demo 一致）",
    )
    ap.add_argument(
        "--restart-file",
        type=Path,
        default=None,
        help="兼容 semiauto_tasks argv；Playwright 版暂不实现重挂监听，可忽略",
    )
    ap.add_argument("--skip-login-wait", action="store_true", help="跳过登录等待")
    ap.add_argument("--verbose", action="store_true", help="打印 JSON 摘要")
    ap.add_argument(
        "--postprocess",
        action="store_true",
        help="监听结束后：postprocess + 导出四类 CSV（等同 postprocess.run_parse_semiauto_to_csv）",
    )
    args = ap.parse_args()

    if args.run_dir and args.out:
        print("请只指定 --run-dir 或 --out 其中之一", file=sys.stderr)
        return 2

    integration_run_dir = args.run_dir is not None

    if args.run_dir:
        out_dir = args.run_dir.resolve()
    elif args.out:
        out_dir = args.out.resolve()
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = (low_gi_project_root() / "data" / "JD" / "playwright_jd_captured" / ts).resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    stop_path = (args.stop_file or (out_dir / ".stop_requested")).resolve()

    if integration_run_dir:
        _touch_status(out_dir, ".status_waiting_login")

    kw = (args.keyword or "manual").strip() or "manual"
    stats: dict[str, dict[str, int]] = {}
    handle_response = make_response_handler(
        out_dir, keyword=kw, verbose=args.verbose, stats=stats
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://www.jd.com")

        wait_login_confirm(
            login_file=args.login_file,
            skip=args.skip_login_wait,
        )

        if integration_run_dir:
            _touch_status(out_dir, ".status_listening")

        def on_page(new_page):
            print(f"\n[新标签页] {new_page.url!r}", flush=True)
            new_page.on("response", handle_response)

        context.on("page", on_page)
        for existing in context.pages:
            existing.on("response", handle_response)
        page.on("response", handle_response)

        print("\n浏览器已启动", flush=True)
        print(f"落盘目录: {out_dir}", flush=True)
        print(f"停止: Ctrl+C 或创建 {stop_path}", flush=True)
        print("list / detail / comment / graphic 四类入子目录。\n", flush=True)

        while running:
            if stop_path.exists():
                print("\n[结束任务] 已检测到停止标记。", flush=True)
                running = False
                break
            try:
                page.wait_for_timeout(400)
            except Exception:
                break

        _safe_browser_close(browser)

    pk = stats.get("per_kind", {})
    print(
        "已退出 | 落盘统计 list={} detail={} comment={} graphic={}".format(
            pk.get("list", 0),
            pk.get("detail", 0),
            pk.get("comment", 0),
            pk.get("graphic", 0),
        ),
        flush=True,
    )

    if args.postprocess:
        try:
            _run_postprocess(out_dir)
        except Exception as exc:
            print(f"[postprocess] 失败: {exc}", file=sys.stderr, flush=True)
            return 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(0)
