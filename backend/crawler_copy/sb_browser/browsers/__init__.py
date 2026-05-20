# -*- coding: utf-8 -*-
"""SeleniumBase UC：路径、启动参数、``get_sb``、``session.main`` 等。"""
from __future__ import annotations

from sb_browser.browsers.constants import (
    DEFAULT_LOCALE,
    DEFAULT_TIMEZONE_ID,
    DEFAULT_USER_AGENT,
    DEFAULT_VIEWPORT,
)
from sb_browser.browsers.paths import (
    default_user_data_dir,
    market_assistant_root,
    package_dir,
    resolve_user_data_dir,
    sb_browser_package_root,
)
from sb_browser.browsers.seleniumbase_uc import sb_persistent_launch_kwargs
from sb_browser.browsers.session import (
    configure_stdio_utf8,
    cookie_rows_to_header,
    get_sb,
    main,
    run_browser_flow,
)

__all__ = [
    "DEFAULT_LOCALE",
    "DEFAULT_TIMEZONE_ID",
    "DEFAULT_USER_AGENT",
    "DEFAULT_VIEWPORT",
    "configure_stdio_utf8",
    "cookie_rows_to_header",
    "default_user_data_dir",
    "get_sb",
    "main",
    "market_assistant_root",
    "package_dir",
    "resolve_user_data_dir",
    "run_browser_flow",
    "sb_browser_package_root",
    "sb_persistent_launch_kwargs",
]
