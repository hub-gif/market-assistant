# -*- coding: utf-8 -*-
"""
只启动 UC Chrome（与 ``run_search_demo`` 同款 ``user_data``），打开 ``about:blank`` 后保持运行；
你**手动关浏览器**或在本终端 **Ctrl+C** 结束即可。

  cd backend\\crawler_copy
  ..\\.venv\\Scripts\\python.exe sb_browser\\platforms\\taobao\\open_browser_cookie_cleanup.py
"""
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_crawler_copy() -> Path:
    root = Path(__file__).resolve().parents[3]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


_bootstrap_crawler_copy()

from sb_browser.browsers.paths import resolve_user_data_dir

from sb_browser.browsers.session import configure_stdio_utf8, get_sb

# 与 run_search_demo 一致：空串则用 ``sb_browser/user_data``
TB_USER_DATA_DIR = ""
TB_HEADLESS = False


def main() -> int:
    configure_stdio_utf8()
    udata = resolve_user_data_dir((TB_USER_DATA_DIR or "").strip() or None)

    print(f"[browser] user_data_dir={udata.resolve()}", file=sys.stderr)
    if TB_HEADLESS:
        print("[browser] 警告：TB_HEADLESS=True 时无可见窗口。", file=sys.stderr)
    print("[browser] 已打开后你可自行输入网址或关窗；终端里 Ctrl+C 也会结束。", file=sys.stderr)

    try:
        with get_sb(user_data_dir_arg=TB_USER_DATA_DIR, headless=bool(TB_HEADLESS)) as sb:
            sb.open("about:blank")
            sb.sleep(0.5)
            while True:
                try:
                    sb.sleep(5.0)
                except BaseException:
                    # 浏览器已被手动关闭等
                    break
    except KeyboardInterrupt:
        print("\n[browser] 退出。", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
