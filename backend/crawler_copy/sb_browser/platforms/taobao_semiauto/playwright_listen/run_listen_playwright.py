# -*- coding: utf-8 -*-
"""Playwright：淘宝响应监听落盘（argv 对齐 JD 半自动任务约定）。

运行：``python -m sb_browser.platforms.taobao_semiauto.playwright_listen.run_listen_playwright --help``
（``cwd`` = ``crawler_copy``）。
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

_CRAWLER_COPY = Path(__file__).resolve().parents[4]
if str(_CRAWLER_COPY) not in sys.path:
    sys.path.insert(0, str(_CRAWLER_COPY))

from playwright.sync_api import sync_playwright

from sb_browser.platforms.taobao_semiauto.common import constants_taobao_semiauto as _cfg
from sb_browser.platforms.taobao_semiauto.common.low_gi_root import tb_playwright_semiauto_capture_root
from sb_browser.platforms.taobao_semiauto.playwright_listen import api_capture_kind as _kind
from sb_browser.platforms.taobao_semiauto.playwright_listen import tb_response_body as _tb_body

running = True


def stop_handler(_sig, _frame):
    global running
    print("\n正在停止监听...", flush=True)
    running = False


signal.signal(signal.SIGINT, stop_handler)


def _safe_browser_close(browser) -> None:
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


def _coarse_capture_candidate(*, url: str) -> bool:
    """避免对全站响应读正文：仅 MTOP host 或疑似商详主文档 URL。"""
    return _kind.should_attempt_tb_response_capture(url or "")


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


def make_response_handler(
    out_dir: Path,
    *,
    keyword: str,
    verbose: bool,
    stats: dict[str, dict[str, int]],
):
    lock = threading.Lock()
    per_kind: dict[str, int] = _kind.kind_counter_template()

    def handle_response(response) -> None:
        try:
            url = response.url or ""
            ct = response.headers.get("content-type") or ""

            if not _coarse_capture_candidate(url=url):
                return

            try:
                full_text = response.text()
            except Exception:
                return

            peek_lim = (
                getattr(_cfg, "SEMI_TB_DETAIL_HTML_PEEK_CHARS", 524288)
                if _kind.is_probable_tb_item_detail_document_url(url)
                else 16384
            )
            if not _kind.looks_like_tb_capture(
                url=url, content_type=ct, body_text=full_text[:peek_lim]
            ):
                return

            parsed, shape = _tb_body.parse_tb_response_body(full_text)
            kind = _kind.classify_taobao_aggregate(
                url=url,
                parsed=parsed,
                body_text=full_text,
                content_type=ct,
                body_shape=shape,
            )
            if kind is None:
                return

            body_store, truncated = _tb_body.store_body_payload(shape=shape, full_text=full_text)

            with lock:
                per_kind[kind] = per_kind.get(kind, 0) + 1
                n = per_kind[kind]

            sub = out_dir / kind
            sub.mkdir(parents=True, exist_ok=True)
            path = sub / f"tb_{kind}_{n:04d}.json"

            envelope: dict[str, object] = {
                "keyword": keyword,
                "capture_kind": kind,
                "api_hint": _kind.api_hint_from_url(url),
                "url": url,
                "status": response.status,
                "method": response.request.method,
                "content_type": ct,
                "body_parse_shape": shape,
                "parsed": parsed,
            }
            if body_store is not None:
                envelope["body_text"] = body_store
                envelope["body_text_truncated"] = truncated

            path.write_text(
                json.dumps(envelope, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[tb:{kind}] -> {path}", flush=True)

            if verbose:
                if parsed is not None:
                    pretty = json.dumps(parsed, ensure_ascii=False, indent=2)
                    print(pretty[:800], flush=True)
                    if len(pretty) > 800:
                        print("...", flush=True)
                else:
                    blob = body_store if isinstance(body_store, str) else full_text
                    print(blob[:800], flush=True)
                    if len(blob) > 800:
                        print("...", flush=True)
        except Exception as exc:
            print(f"监听异常: {exc}", flush=True)

    stats["per_kind"] = per_kind
    return handle_response


def main() -> int:
    global running

    ap = argparse.ArgumentParser(description="Playwright 淘宝 API 监听落盘（list/detail/comment/mtop/unknown）")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help=f"落盘目录；与 --run-dir 二选一；默认 {tb_playwright_semiauto_capture_root()}/<时间戳>",
    )
    ap.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="任务根目录；写 .status_*（与 Django 任务约定一致时可沿用）",
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
        help="登录确认文件（如 .login_confirmed）",
    )
    ap.add_argument("--keyword", default="manual", help="写入 JSON 的任务关键词")
    ap.add_argument(
        "--restart-file",
        type=Path,
        default=None,
        help="兼容 argv；Playwright 版暂不实现重挂，忽略即可",
    )
    ap.add_argument("--skip-login-wait", action="store_true", help="跳过登录等待")
    ap.add_argument("--verbose", action="store_true", help="打印正文/JSON 摘要")
    ap.add_argument(
        "--postprocess",
        action="store_true",
        help="监听结束后：半自动 run 目录导出与京东对齐的四类 CSV（parse_tb_semiauto_to_csv）",
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
        out_dir = (tb_playwright_semiauto_capture_root() / ts).resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    stop_path = (args.stop_file or (out_dir / ".stop_requested")).resolve()

    if integration_run_dir:
        _touch_status(out_dir, ".status_waiting_login")

    kw = (args.keyword or "manual").strip() or "manual"
    stats: dict[str, dict[str, int]] = {}
    handle_response = make_response_handler(
        out_dir, keyword=kw, verbose=args.verbose, stats=stats
    )

    landing = (_cfg.SEMI_DEFAULT_LANDING_URL or "https://www.taobao.com/").strip()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context()
        page = context.new_page()
        page.goto(landing)

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
        print("分类目录：list / detail / comment / mtop / unknown。\n", flush=True)

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
    parts = ", ".join(f"{k}={pk.get(k, 0)}" for k in _kind.kind_counter_template())
    print(f"已退出 | 落盘统计 {parts}", flush=True)

    if args.postprocess:
        try:
            from sb_browser.platforms.taobao_semiauto.postprocess.run_parse_tb_semiauto_to_csv import (
                run as tb_semiauto_parse_run,
            )

            n_list, n_detail, n_comment, n_merged = tb_semiauto_parse_run(out_dir)
            print(
                f"[postprocess] CSV 已导出 list={n_list} detail={n_detail} "
                f"comment={n_comment} merged={n_merged}",
                flush=True,
            )
        except Exception as exc:
            print(f"[postprocess] CSV 导出失败: {exc}", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(0)
