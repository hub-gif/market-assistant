# -*- coding: utf-8 -*-
"""SeleniumBase UC 启动参数；可选持久化 ``user_data_dir``（不传则由 UC/SB 使用临时 Profile）。"""
from __future__ import annotations

from pathlib import Path
from typing import Any


def sb_persistent_launch_kwargs(
    *,
    user_data_dir: Path | None,
    headless: bool,
    sb_test: bool = True,
) -> dict[str, Any]:
    """构造 ``with SB(**kwargs) as sb:``。

    ``user_data_dir=None``：不向 SB 传入该参数，通常为 **本次临时 Profile**。

    长时间挂机监听场景可将 ``sb_test=False``，减轻 SB 「测试模式」附带行为干扰。
    """
    kw: dict[str, Any] = {
        "uc": True,
        "test": bool(sb_test),
        "browser": "chrome",
        "headless": headless,
        "locale_code": "zh-CN",
    }
    if user_data_dir is not None:
        kw["user_data_dir"] = str(user_data_dir.resolve())
    return kw
