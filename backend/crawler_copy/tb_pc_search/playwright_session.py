# -*- coding: utf-8 -*-
"""淘宝 mtop 脚本专用：**Playwright** 临时 / 持久化 Chromium。

持久化 ``user_data_dir`` 与其它用途的 Chromium 数据目录请勿共用路径，以免造成 Cookie / 会话错乱。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from playwright.sync_api import (
    APIRequestContext,
    Browser,
    BrowserContext,
    Playwright,
)

from mtop.constants import USER_AGENT

_DEFAULT_LOCALE = "zh-CN"
_DEFAULT_TIMEZONE_ID = "Asia/Shanghai"
_DEFAULT_VIEWPORT = {"width": 1707, "height": 1067}


def _market_assistant_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        p = Path(raw).expanduser().resolve()
        if not p.is_dir():
            raise RuntimeError(f"LOW_GI_PROJECT_ROOT 无效: {p}")
        return p
    return Path(__file__).resolve().parents[3]


def default_playwright_user_data_dir() -> Path:
    """默认持久化目录：``tb_pc_search/pw_user_data``。"""
    return (Path(__file__).resolve().parent / "pw_user_data").resolve()


def resolve_tb_user_data_dir(config: str | None) -> Path:
    """
    * 空 / None：``default_playwright_user_data_dir()``
    * 绝对路径：resolve
    * 相对路径：相对项目根 ``LOW_GI_PROJECT_ROOT``（或未设置时的仓库根推断）
    """
    s = (config or "").strip()
    if not s:
        return default_playwright_user_data_dir()
    p = Path(s).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (_market_assistant_root() / p).resolve()


def cookie_rows_to_header(rows: list[dict[str, Any]]) -> str:
    """Playwright cookie 行 → HTTP ``Cookie`` 请求头。"""
    parts: list[str] = []
    for c in rows:
        name = c.get("name")
        if not name:
            continue
        val = c.get("value")
        parts.append(f"{name}={val if val is not None else ''}")
    return "; ".join(parts)


@dataclass
class TbChromiumSession:
    """持久化会话仅有 ``context``（``browser`` 为 ``None``）。"""

    browser: Browser | None
    context: BrowserContext

    @property
    def request(self) -> APIRequestContext:
        return self.context.request

    def close(self) -> None:
        if self.browser is not None:
            self.browser.close()
            return
        self.context.close()


def _persistent_context_kwargs(*, user_data_dir: Path, headless: bool) -> dict[str, Any]:
    return {
        "user_data_dir": str(user_data_dir.resolve()),
        "headless": headless,
        "locale": _DEFAULT_LOCALE,
        "timezone_id": _DEFAULT_TIMEZONE_ID,
        "viewport": _DEFAULT_VIEWPORT,
        "user_agent": USER_AGENT,
    }


def launch_persistent_chromium(
    pw: Playwright,
    *,
    user_data_dir: Path,
    headless: bool,
    extra_launch_kwargs: dict[str, Any] | None = None,
) -> TbChromiumSession:
    kw = _persistent_context_kwargs(user_data_dir=user_data_dir, headless=headless)
    if extra_launch_kwargs:
        kw.update(extra_launch_kwargs)
    ctx = pw.chromium.launch_persistent_context(**kw)
    return TbChromiumSession(browser=None, context=ctx)


def launch_ephemeral_chromium_like_search(
    pw: Playwright,
    *,
    headless: bool,
) -> TbChromiumSession:
    browser = pw.chromium.launch(headless=headless)
    ctx = browser.new_context(
        user_agent=USER_AGENT,
        locale=_DEFAULT_LOCALE,
        timezone_id=_DEFAULT_TIMEZONE_ID,
        viewport=_DEFAULT_VIEWPORT,
    )
    return TbChromiumSession(browser=browser, context=ctx)
