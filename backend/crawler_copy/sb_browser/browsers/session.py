# -*- coding: utf-8 -*-
"""
UC 会话：``get_sb`` 供各平台 ``with`` 使用；``run_browser_flow`` / ``main`` 在本文件末尾改 ``SB_*`` 后直接运行本文件。

不打开任何 URL；站点流程在 ``platforms/``。
"""
from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

try:
    from seleniumbase import SB
except ImportError:
    SB = None  # noqa: N816

from sb_browser.browsers.paths import market_assistant_root, resolve_user_data_dir
from sb_browser.browsers.seleniumbase_uc import sb_persistent_launch_kwargs


def _maybe_hint_user_data_locked(user_data_dir: str | None, exc: BaseException) -> None:
    """Chrome 单用户目录不可多开；失败时给可读提示（不篡改异常类型）。"""
    combined = f"{exc!s}"
    lc = combined.lower()
    lockish = (
        ("user data" in lc and ("already" in lc or "in use" in lc))
        or ("profile" in lc and "lock" in lc)
        or ("无法" in combined and "目录" in combined)
        or ("被占用" in combined)
    )
    if lockish:
        print(
            "[sb_browser] 持久化目录疑似被占用。请：关闭用该目录的 Chrome/EDGE、结束 chromedriver；"
            "或改用另一目录（SB_USER_DATA_DIR / user_data_dir_arg，见 browsers/paths.resolve_user_data_dir）。",
            file=sys.stderr,
        )


def cookie_rows_to_header(rows: list[dict[str, Any]]) -> str:
    """Selenium / Playwright Cookie dict → HTTP ``Cookie`` 请求头字符串。"""
    parts: list[str] = []
    for c in rows:
        name = c.get("name")
        if not name:
            continue
        val = c.get("value")
        parts.append(f"{name}={val if val is not None else ''}")
    return "; ".join(parts)


def configure_stdio_utf8() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


@contextmanager
def get_sb(
    *,
    user_data_dir_arg: str = "",
    headless: bool = False,
    sb_test: bool = True,
    persist_profile: bool | None = None,
) -> Generator[Any, None, None]:
    """
    创建 SeleniumBase ``SB``（UC），供平台脚本::

        with get_sb(user_data_dir_arg="", headless=False, sb_test=True) as sb:
            ...

    ``persist_profile``：

    - ``None``（默认）：与历史行为一致——``user_data_dir_arg`` 为空时使用 ``sb_browser/user_data``。
    - ``False``：**未指定目录则不落盘 Profile**（不向 SB 传 ``user_data_dir``，通常为临时目录）；仅在
      ``user_data_dir_arg`` **非空**时解析并持久化到该路径。适用于京东半自动「不强制固定路径」。
    - ``True``：始终解析并 mkdir（空参数 → 默认持久化目录）。

    ``sb_test=False``：非 pytest 场景长时间挂机时可减轻 SB 测试模式附带行为（半自动监听常用）。
    """
    if SB is None:
        raise RuntimeError("未安装 SeleniumBase。请运行: pip install seleniumbase")

    raw = (user_data_dir_arg or "").strip()

    ud_for_kwargs: Path | None
    lock_hint_path: str = ""

    if persist_profile is False:
        if raw:
            udata = resolve_user_data_dir(raw)
            udata.mkdir(parents=True, exist_ok=True)
            ud_for_kwargs = udata
            lock_hint_path = str(udata.resolve())
        else:
            ud_for_kwargs = None
            lock_hint_path = ""
    else:
        udata = resolve_user_data_dir(raw or None)
        udata.mkdir(parents=True, exist_ok=True)
        ud_for_kwargs = udata
        lock_hint_path = str(udata.resolve())

    kwargs = sb_persistent_launch_kwargs(
        user_data_dir=ud_for_kwargs,
        headless=bool(headless),
        sb_test=bool(sb_test),
    )
    manager = SB(**kwargs)
    try:
        sb = manager.__enter__()
    except Exception as e:
        if lock_hint_path:
            _maybe_hint_user_data_locked(lock_hint_path, e)
        raise
    try:
        yield sb
    finally:
        manager.__exit__(None, None, None)


def run_browser_flow(
    *,
    user_data_dir_arg: str = "",
    headless: bool = False,
    export_cookie_relative: str = "",
) -> int:
    """
    UC + 持久化目录 → 不导航 → 可选导出 Cookie 或常驻（Ctrl+C 结束）。

    导出前若需登录态，请先跑 ``platforms/*`` 或在本机 profile 中完成登录。
    """
    if SB is None:
        print("未安装 SeleniumBase。请运行: pip install seleniumbase", file=sys.stderr)
        return 2

    udata = resolve_user_data_dir(user_data_dir_arg.strip() or None)
    udata.mkdir(parents=True, exist_ok=True)
    root = market_assistant_root()

    print(f"[sb_browser] user_data_dir={udata}", file=sys.stderr)
    print(f"[sb_browser] project_root={root}", file=sys.stderr)
    print("[sb_browser] engine=SeleniumBase(UC)；未打开任何 URL。", file=sys.stderr)

    kwargs = sb_persistent_launch_kwargs(user_data_dir=udata, headless=bool(headless))

    try:
        with SB(**kwargs) as sb:
            export_target = (export_cookie_relative or "").strip()
            if export_target:
                out = (root / export_target).resolve()
                out.parent.mkdir(parents=True, exist_ok=True)
                header = cookie_rows_to_header(sb.driver.get_cookies())
                out.write_text(header, encoding="utf-8")
                print(f"[sb_browser] 已写入 Cookie: {out}", file=sys.stderr)
            else:
                print(
                    "[sb_browser] 浏览器已就绪；平台脚本见 platforms/。Ctrl+C 退出。",
                    file=sys.stderr,
                )
                try:
                    while True:
                        time.sleep(1.0)
                except KeyboardInterrupt:
                    print("[sb_browser] 退出。", file=sys.stderr)
    except Exception as e:
        _maybe_hint_user_data_locked(str(udata.resolve()), e)
        raise
    return 0


# ---------------------------------------------------------------------------
# 仅养 UC / 导出 Cookie：改 ``SB_*`` 后：
#   python backend/crawler_copy/sb_browser/browsers/session.py
#
# 默认 ``sb_browser/user_data`` 被占用时：先关浏览器、结束 chromedriver；或设为本机另一路径
# （绝对路径，或相对项目根的 ``profiles/sb_demo`` 等），避免多脚本同目录并发。
# ---------------------------------------------------------------------------
SB_USER_DATA_DIR = ""
SB_HEADLESS = False
SB_EXPORT_COOKIE_FILE = ""


def main() -> int:
    configure_stdio_utf8()
    return run_browser_flow(
        user_data_dir_arg=SB_USER_DATA_DIR,
        headless=bool(SB_HEADLESS),
        export_cookie_relative=SB_EXPORT_COOKIE_FILE,
    )


if __name__ == "__main__":
    _cc = Path(__file__).resolve().parents[2]
    if str(_cc) not in sys.path:
        sys.path.insert(0, str(_cc))
    raise SystemExit(main())
