"""
SeleniumBase UC：持续监听 Network.responseReceived（performance 日志方案）。

要点：
  - ``uc=True, uc_cdp_events=True``；用 ``driver.get``，勿用 ``sb.open()`` 主导航（易切 CDP 链）。
  - 监听创建/挂载见 ``sb_browser.performance_response_listen``。
  - **纯手动** Ctrl+T 新标签：通常**不会**进 Chromedriver 的 performance 日志；需要时用 ``window.open``/自动化开标签，或改用 ``cdp_json_listen`` / Playwright。
  - 按 Enter 或 Ctrl+C 结束。

用法：
  python sb_uc_network_listener_demo.py
  python sb_uc_network_listener_demo.py --url about:blank
  python sb_uc_network_listener_demo.py --verbose
  python sb_uc_network_listener_demo.py --two-extra-tabs
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Callable

from seleniumbase import SB

# 仓库根直接运行时补全 crawler_copy
_CRAWLER_COPY = Path(__file__).resolve().parent / "backend" / "crawler_copy"
if _CRAWLER_COPY.is_dir() and str(_CRAWLER_COPY) not in sys.path:
    sys.path.insert(0, str(_CRAWLER_COPY))

from sb_browser.performance_response_listen import (  # noqa: E402
    mount_network_response_listener,
    stop_performance_poller,
)

DEFAULT_START_URL = "https://www.jd.com/"
# --two-extra-tabs：再 window.open 两个页，用于验证「单监听 + 多标签」
DEMO_EXTRA_TAB_URLS = (
    "https://www.amazon.com",
    "https://m.jd.com/",
)
POLL_DEFAULT = 0.05
VERBOSE_CAP = 40


def _interesting(url: str) -> bool:
    u = (url or "").lower()
    return (
        "jd.com" in u
        or "jd.hk" in u
        or "360buyimg.com" in u
        or "360buy" in u
        or "jdpay" in u
        or "jkcsjd.com" in u
        or "amazon" in u
        or "amzn" in u
        or "a2z" in u
        or "httpbin" in u
    )


def make_print_handler(
    *,
    verbose: bool,
    verbose_cap: int,
) -> tuple[Callable[[dict], None], dict[str, int]]:
    st: dict[str, int] = {"hit": 0, "verbose": 0}
    lock = threading.Lock()

    def on_response(event: dict) -> None:
        try:
            p = event.get("params") or {}
            r = p.get("response") or {}
            url = (r.get("url") or "").strip()
            if not url:
                return
            status = r.get("status")
            if _interesting(url):
                with lock:
                    st["hit"] += 1
                    n = st["hit"]
                print(f"[#{n}] {status} {url[:220]}", flush=True)
                return
            if not verbose or not url.lower().startswith("https:"):
                return
            with lock:
                if st["verbose"] >= verbose_cap:
                    return
                st["verbose"] += 1
                vn = st["verbose"]
            print(f"[v{vn}] {status} {url[:220]}", flush=True)
        except Exception as exc:
            print(f"[err] {exc}", flush=True)

    return on_response, st


def _stdin_until_enter(stop: threading.Event) -> None:
    try:
        input("监听中… 按 Enter 结束（也可 Ctrl+C）\n")
    except EOFError:
        pass
    finally:
        stop.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="UC performance 持续监听 demo")
    parser.add_argument(
        "--url",
        default=DEFAULT_START_URL,
        help=f"启动后自动打开（默认 {DEFAULT_START_URL}）",
    )
    parser.add_argument("--verbose", action="store_true", help=f"额外打印前 {VERBOSE_CAP} 条 https 响应")
    parser.add_argument("--poll", type=float, default=POLL_DEFAULT, help="轮询间隔秒")
    parser.add_argument(
        "--two-extra-tabs",
        action="store_true",
        help="首屏加载后用 window.open 再开两标签（httpbin JSON + m.jd.com），单监听应都能收到",
    )
    args = parser.parse_args()

    on_response, stats = make_print_handler(
        verbose=args.verbose,
        verbose_cap=VERBOSE_CAP,
    )

    print("UC + performance：勿用 sb.open；多标签通常一次挂载即可。\n", flush=True)

    stop: threading.Event | None = None
    poller: threading.Thread | None = None

    try:
        with SB(uc=True, uc_cdp_events=True) as sb:
            drv = sb.driver
            stop, poller = mount_network_response_listener(
                drv,
                on_response,
                poll_interval_sec=max(0.02, float(args.poll)),
            )

            u = (args.url or "").strip()
            if u:
                print(f"打开: {u}\n", flush=True)
                drv.get(u)

            if args.two_extra_tabs:
                for extra in DEMO_EXTRA_TAB_URLS:
                    drv.execute_script(
                        "window.open(arguments[0], '_blank', 'noopener');",
                        extra,
                    )
                    time.sleep(1.0)
                try:
                    print(
                        f"[提示] 已再开 {len(DEMO_EXTRA_TAB_URLS)} 个标签，"
                        f"handles={len(drv.window_handles)}\n",
                        flush=True,
                    )
                except Exception:
                    print(
                        f"[提示] 已再开 {len(DEMO_EXTRA_TAB_URLS)} 个标签\n",
                        flush=True,
                    )

            threading.Thread(
                target=_stdin_until_enter,
                args=(stop,),
                daemon=True,
            ).start()

            try:
                while not stop.is_set():
                    time.sleep(0.2)
            except KeyboardInterrupt:
                stop.set()
                print("\n[中断] Ctrl+C", flush=True)

    except KeyboardInterrupt:
        if stop is not None:
            stop.set()

    if stop is not None and poller is not None:
        stop_performance_poller(stop, poller)

    print(
        f"\n结束：匹配过滤的响应 {stats['hit']} 条；verbose {stats['verbose']} 条。",
        flush=True,
    )


if __name__ == "__main__":
    main()
