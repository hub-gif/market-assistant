# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

from sb_browser.cdp_json_listen import (
    JsonListenSession,
    attach_sb_cdp_json_listener,
    finalize_json_reads_blocking,
)

from . import constants_xhs as _X


def open_xhs_landing(sb: Any, *, url: str | None = None) -> None:
    """在目标 URL 上启用 CDP Mode（必须带 URL，一次调用，便于 Cookie/Storage 写入当前 Profile）。"""
    u = (url or "").strip() or _X.XHS_DEFAULT_URL
    # 勿先 activate_cdp_mode() 空参再 cdp.open：易造成与 user_data 的存储上下文不一致，设置里「所有网站」可能一直为 0。
    sb.activate_cdp_mode(u)
    sb.sleep(2.5)

def search_keyword(sb: Any, keyword: str, *, after_type_sleep: float = 1.2) -> None:

    kw = (keyword or "").strip()
    if not kw:
        return

    inp = (getattr(_X, "XHS_SELECTOR_SEARCH_INPUT", "") or "").strip()
    if not inp:
        print("[xhs.flows] 请在 constants_xhs 中填写 XHS_SELECTOR_SEARCH_INPUT", flush=True)
        return

    sb.cdp.wait_for_element_visible(inp, timeout=15)
    sb.sleep(0.3)
    sb.cdp.mouse_click(inp)
    sb.sleep(0.35)
    sb.cdp.type(inp, kw)
    sb.sleep(max(0.25, float(after_type_sleep)))

    btn = (getattr(_X, "XHS_SELECTOR_SEARCH_BUTTON", "") or "").strip()
    if btn:
        try:
            sb.cdp.wait_for_element_visible(btn, timeout=3)
            sb.cdp.mouse_click(btn)
            sb.sleep(1.5)
            return
        except Exception:
            pass
    sb.cdp.press_keys(inp, "\n")
    sb.sleep(1.8)


def explore_then_search(
    sb: Any,
    keyword: str,
    *,
    landing_url: str | None = None,
    listen_search_notes_json: bool = True,
    save_search_notes_json: bool = False,
    search_notes_save_dir: str | Path | None = None,
) -> JsonListenSession:
    """CDP 落地页 →（可选：按 path 用 ``cdp_json_listen`` 登记请求）→ 搜索 → ``finalize_json_reads_blocking``。

    ``save_search_notes_json=True`` 且命中时，将响应写入 ``<项目根>/data/XHS/search_notes_raw/``，
    命名风格对齐京东 ``pc_search_*_req_*``（见 ``notes_sink.save_search_notes_captures``）。

    返回 ``tap``，便于读 ``captures`` / ``latest`` / ``last_errors``。
    """
    path = (
        getattr(_X, "XHS_SEARCH_NOTES_API_PATH", "") or ""
    ).strip() or "/api/sns/web/v1/search/notes"

    open_xhs_landing(sb, url=landing_url)
    if listen_search_notes_json:
        # POST 笔记搜索在 DevTools/CDP 下偶见 ResourceType 为 OTHER 等非 XHR/FETCH，勿按类型收窄
        tap = attach_sb_cdp_json_listener(
            sb,
            url_contains=path,
            resource_types=(),
        )
    else:
        tap = JsonListenSession()

    sb.sleep(1.0)
    search_keyword(sb, keyword)
    if listen_search_notes_json:
        finalize_json_reads_blocking(sb, tap)
        if save_search_notes_json and tap.captures:
            from .notes_sink import save_search_notes_captures

            save_search_notes_captures(
                tap.captures,
                keyword=(keyword or "").strip(),
                raw_dir=search_notes_save_dir,
            )
    return tap
