# -*- coding: utf-8 -*-
"""持久化目录与项目根（SeleniumBase UC ``user_data_dir``）。"""
from __future__ import annotations

import os
from pathlib import Path


def sb_browser_package_root() -> Path:
    """``sb_browser`` 包根（本文件位于 ``sb_browser/browsers/``）。"""
    return Path(__file__).resolve().parent.parent


def package_dir() -> Path:
    """兼容旧名，等价于 ``sb_browser_package_root``。"""
    return sb_browser_package_root()


def market_assistant_root() -> Path:
    """market_assistant 仓库根：``LOW_GI_PROJECT_ROOT`` 或自举。"""
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        p = Path(raw).expanduser().resolve()
    else:
        p = sb_browser_package_root().resolve().parents[2]
    if not p.is_dir():
        raise RuntimeError(f"项目根不是有效目录: {p}")
    return p


def default_user_data_dir() -> Path:
    """默认 UC 持久化目录：``sb_browser/user_data``。"""
    return sb_browser_package_root() / "user_data"


def resolve_user_data_dir(config: str | None) -> Path:
    """
    * 空 / None：``default_user_data_dir()``（``sb_browser/user_data``）
    * 绝对路径：resolve
    * 相对路径：相对 ``market_assistant_root()``（可用 ``LOW_GI_PROJECT_ROOT``）

    **目录被占用**（如报错 ``user data directory is already in use``）：先关掉正在使用该
    Profile 的浏览器窗口；任务管理器结束残留的 ``chromedriver.exe``；仍不行则把配置改为
    **另一路径**（新目录或副本），勿多进程共写同一 ``user_data``。
    """
    s = (config or "").strip()
    if not s:
        return default_user_data_dir()
    p = Path(s).expanduser()
    if p.is_absolute():
        return p.resolve()
    return (market_assistant_root() / p).resolve()
