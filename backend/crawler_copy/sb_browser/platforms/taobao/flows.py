# -*- coding: utf-8 -*-
from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from sb_browser.cdp_json_listen import (
    JsonListenSession,
    attach_sb_cdp_json_listener,
    finalize_json_reads_blocking,
)
from sb_browser.cdp_human_motion import browse_scroll_burst, mouse_click_or_human

from . import constants_tb as _T


def _pair_or_default(name: str, default: tuple[float, float]) -> tuple[float, float]:
    p = getattr(_T, name, default)
    if not isinstance(p, (tuple, list)) or len(p) < 2:
        p = default
    a, b = float(p[0]), float(p[1])
    return (min(a, b), max(a, b))


def _pacing_scale() -> float:
    s = float(getattr(_T, "TB_PACING_SCALE", 1.0))
    return max(0.35, min(4.0, s))


def _pair_scaled(name: str, default: tuple[float, float]) -> tuple[float, float]:
    """对人机节奏类区间乘 ``TB_PACING_SCALE``（倍率来自 ``constants_tb``）。"""
    lo, hi = _pair_or_default(name, default)
    m = _pacing_scale()
    return (lo * m, hi * m)


def _sleep_uniform(sb: Any, name: str, default: tuple[float, float]) -> None:
    lo, hi = _pair_scaled(name, default)
    sb.sleep(random.uniform(lo, hi))


def _timeout_uniform(name: str, default: tuple[float, float]) -> float:
    lo, hi = _pair_or_default(name, default)
    return random.uniform(lo, hi)


def _pager_after_click_range(spec: Any, *, default_name: str = "TB_SLEEP_PAGER_AFTER_NEXT_CLICK") -> tuple[float, float]:
    dflt = _pair_scaled(default_name, (2.3, 4.05))
    if spec is None:
        return dflt
    if isinstance(spec, (tuple, list)) and len(spec) >= 2:
        a, b = float(spec[0]), float(spec[1])
        lo, hi = (min(a, b), max(a, b))
    else:
        try:
            c = float(spec)
            lo, hi = (max(0.15, c * 0.76), max(0.25, c * 1.24))
        except (TypeError, ValueError):
            return dflt
    m = _pacing_scale()
    return (lo * m, hi * m)


def _int_pair(name: str, default: tuple[int, int]) -> tuple[int, int]:
    p = getattr(_T, name, default)
    if not isinstance(p, (tuple, list)) or len(p) < 2:
        p = default
    a, b = int(p[0]), int(p[1])
    return (min(a, b), max(a, b))


def _human_click_kwargs() -> dict[str, Any]:
    return {
        "enabled": bool(getattr(_T, "TB_HUMANLIKE_ENABLED", True)),
        "segments": _int_pair("TB_HUMANLIKE_MOUSE_SEGMENTS", (16, 30)),
        "step_pause": _pair_or_default("TB_HUMANLIKE_MOUSE_STEP_PAUSE", (0.007, 0.034)),
        "press_pause": _pair_or_default("TB_HUMANLIKE_MOUSE_PRESS_PAUSE", (0.038, 0.12)),
        "scroll_smooth_chance": float(getattr(_T, "TB_HUMANLIKE_SCROLLINTO_SMOOTH_CHANCE", 0.32)),
        # 滚动入视后与贝塞尔起手之间的略长停顿，随 ``TB_PACING_SCALE`` 略拉伸，利于控频
        "pre_path_sleep": _pair_scaled(
            "TB_HUMANLIKE_MOUSE_PRE_PATH_SLEEP", (0.02, 0.19)
        ),
    }


def _type_search_keyword(sb: Any, inp: str, kw: str) -> None:
    """长词分多段 ``press_keys``，短词不切分。"""
    pto = getattr(_T, "TB_CDP_PRESS_KEYS_TIMEOUT_SEC", None)
    lo, hi = _int_pair("TB_HUMANLIKE_TYPING_CHUNK_CHARS", (2, 5))
    a = max(1, min(lo, hi))
    b = max(a, max(lo, hi))
    pa_lo, pa_hi = _pair_scaled(
        "TB_HUMANLIKE_TYPING_BETWEEN_CHUNK_SLEEP", (0.035, 0.24)
    )
    if len(kw) <= 3:
        if pto is None:
            sb.cdp.press_keys(inp, kw)
        else:
            sb.cdp.press_keys(inp, kw, timeout=float(pto))
        return
    i = 0
    while i < len(kw):
        chunk_len = random.randint(a, b)
        chunk = kw[i : i + chunk_len]
        if not chunk:
            break
        if pto is None:
            sb.cdp.press_keys(inp, chunk)
        else:
            sb.cdp.press_keys(inp, chunk, timeout=float(pto))
        i += len(chunk)
        if i < len(kw):
            sb.sleep(random.uniform(pa_lo, pa_hi))


def _browse_listing_if_cfg(sb: Any, *, after_search: bool = False) -> None:
    """随机分段滚动；``after_search=True`` 时使用 ``TB_HUMANLIKE_AFTER_SEARCH_BROWSE_SCROLL``。"""
    if not bool(getattr(_T, "TB_HUMANLIKE_ENABLED", True)):
        return
    if after_search:
        if not bool(getattr(_T, "TB_HUMANLIKE_AFTER_SEARCH_BROWSE_SCROLL", True)):
            return
    else:
        if not bool(getattr(_T, "TB_HUMANLIKE_AFTER_LANDING_SCROLL", True)):
            return
    browse_scroll_burst(
        sb,
        bursts_range=_int_pair("TB_HUMANLIKE_LISTING_SCROLL_BURSTS", (2, 5)),
        pause_sec=_pair_scaled("TB_HUMANLIKE_SCROLL_PAUSE", (0.09, 0.36)),
        delta_px=_int_pair("TB_HUMANLIKE_SCROLL_DELTA_PX", (96, 412)),
        smooth_scroll_chance=float(getattr(_T, "TB_HUMANLIKE_SCROLL_SMOOTH_CHANCE", 0.36)),
        multi_step_chance=float(getattr(_T, "TB_HUMANLIKE_SCROLL_MULTI_STEP_CHANCE", 0.44)),
        max_micro_steps=int(getattr(_T, "TB_HUMANLIKE_SCROLL_MICRO_STEPS_MAX", 5)),
    )


def open_tb_landing(sb: Any, *, url: str | None = None) -> None:
    """在目标 URL 上启用 CDP Mode（优先带.taobao.com cookie 域的一致落地）。"""
    u = (url or "").strip() or _T.TB_DEFAULT_URL
    sb.activate_cdp_mode(u)
    _sleep_uniform(sb, "TB_SLEEP_AFTER_LANDING_ACTIVATE", (2.1, 3.5))
    _browse_listing_if_cfg(sb, after_search=False)


def search_keyword(sb: Any, keyword: str) -> None:
    kw = (keyword or "").strip()
    if not kw:
        return

    inp = (getattr(_T, "TB_SELECTOR_SEARCH_INPUT", "") or "").strip()
    if not inp:
        print("[tb.flows] 请在 constants_tb 中填写 TB_SELECTOR_SEARCH_INPUT", flush=True)
        return

    tb_in = _timeout_uniform("TB_WAIT_SEARCH_INPUT_VISIBLE_TIMEOUT", (17.5, 24.8))
    sb.cdp.wait_for_element_visible(inp, timeout=max(5.0, tb_in))
    _sleep_uniform(sb, "TB_SLEEP_AFTER_FOCUS_SEARCH_INPUT_PREP", (0.22, 0.54))
    _sleep_uniform(sb, "TB_HUMANLIKE_HESITATION_BEFORE_CLICK", (0.04, 0.31))
    mouse_click_or_human(sb, inp, **_human_click_kwargs())
    _sleep_uniform(sb, "TB_SLEEP_AFTER_CLICK_SEARCH_INPUT", (0.17, 0.44))
    # ``type`` 会先清空再一次性 ``send_keys``；``press_keys`` 为 SeleniumBase CDP 类人逐键节奏（见 sb_cdp.press_keys）
    try:
        sb.cdp.clear_input(inp)
    except BaseException:
        pass
    _type_search_keyword(sb, inp, kw)
    _sleep_uniform(sb, "TB_SLEEP_AFTER_TYPING_KEYWORD", (0.38, 1.08))

    btn = (getattr(_T, "TB_SELECTOR_SEARCH_BUTTON", "") or "").strip()
    if btn:
        try:
            tb_btn = _timeout_uniform("TB_WAIT_SEARCH_BUTTON_VISIBLE_TIMEOUT", (3.2, 5.9))
            sb.cdp.wait_for_element_visible(btn, timeout=max(1.0, tb_btn))
            _sleep_uniform(sb, "TB_HUMANLIKE_HESITATION_BEFORE_CLICK", (0.04, 0.31))
            mouse_click_or_human(sb, btn, **_human_click_kwargs())
            _sleep_uniform(sb, "TB_SLEEP_AFTER_SUBMIT_BUTTON", (1.42, 2.38))
            return
        except Exception:
            pass
    # ``press_keys(..., "\\n")`` 内部逐键节奏，末段发回车提交
    sb.cdp.press_keys(inp, "\n")
    _sleep_uniform(sb, "TB_SLEEP_AFTER_SUBMIT_KEYS", (1.85, 2.95))


def click_tb_search_next_page(sb: Any) -> bool:
    """
    在 **搜索列表页**（如 ``s.taobao.com/search``）点击「下一页」。
    selector 见 ``constants_tb.TB_SELECTOR_PAGER_NEXT`` / ``TB_PAGER_NEXT_SELECTORS``。
    """
    custom = (getattr(_T, "TB_SELECTOR_PAGER_NEXT", "") or "").strip()
    if custom:
        sels = (custom,) + tuple(getattr(_T, "TB_PAGER_NEXT_SELECTORS", ()) or ())
    else:
        sels = tuple(getattr(_T, "TB_PAGER_NEXT_SELECTORS", ()) or ())

    waited = _timeout_uniform("TB_WAIT_PAGER_NEXT_VISIBLE_TIMEOUT", (8.5, 12.9))

    for sel in sels:
        s = (sel or "").strip()
        if not s:
            continue
        try:
            sb.cdp.wait_for_element_visible(s, timeout=max(1.0, waited))
        except Exception:
            continue
        try:
            _sleep_uniform(sb, "TB_HUMANLIKE_HESITATION_BEFORE_CLICK", (0.04, 0.31))
            mouse_click_or_human(sb, s, **_human_click_kwargs())
            return True
        except Exception:
            continue
    return False


def explore_then_search_and_listen(
    sb: Any,
    keyword: str,
    *,
    landing_url: str | None = None,
    listen_mtop_json: bool = True,
    save_mtop_json: bool = False,
    mtop_save_dir: str | Path | None = None,
    mtop_save_only_main_bundle: bool | None = None,
    mtop_save_run_dir_by_time: bool | None = None,
    mtop_export_csv_after_save: bool | None = None,
    pager_max_pages: int | None = None,
    pager_after_click_sleep: float | tuple[float, float] | None = None,
) -> JsonListenSession:
    """CDP 落地 →（可选监听 mtop）→ 类人滚动/点击轨迹 → 搜索 → 多页则每页 ``finalize`` → 可选落盘。

    **监听语义**：仅 ``attach_sb_cdp_json_listener`` / ``finalize_json_reads_blocking`` —— 对页面发起的真实请求，
    在响应到达后 **复制** CDP 缓存里的正文；**不是** 用本地保存的 JSON **重放** 假响应，也 **没有**
    启用 ``Fetch`` 域拦截改包。

    类人行为开关见 ``constants_tb.TB_HUMANLIKE_*``（贝塞尔指针轨迹、`window.scrollBy` 分段浏览）。

    监听 **不会** 在翻页时卸掉：``attach`` 一直有效，每页结束调 ``finalize_json_reads_blocking``
    将新响应正文追加到同一 ``tap.captures``。

    翻页页数见 ``pager_max_pages`` 或 ``constants_tb.TB_PAGER_MAX_PAGES``（含第 1 页）；
    ``TB_PAGER_*`` 配点击「下一页」的 selector；翻页间隔见 ``pager_after_click_sleep`` /
    ``constants_tb.TB_SLEEP_PAGER_AFTER_NEXT_CLICK``（经 ``TB_PACING_SCALE`` 缩放），
    另可加 ``TB_PAGER_INTER_PAGE_EXTRA_SLEEP``。

    监听子串优先 ``constants_tb.TB_MTOP_LISTEN_URL_CONTAINS``（h5api + ``/recommend/2.0/``，
    与 Network GET/jsonp 一致）；缺省时再退回到 ``TB_MTOP_WIRELESS_RECOMMEND_SNIPPET``。
    同 path 下的 ``showTypeControl`` / ``aiNavigation`` 类请求由 ``TB_MTOP_URL_EXCLUDE_FRAGMENTS`` 排除。
    ``resource_types=()``：jsonp/script 等资源类型可能被标为 OTHER 等。
    ``mtop_save_only_main_bundle`` / ``mtop_save_run_dir_by_time`` / ``mtop_export_csv_after_save``：
    ``None`` 时用 ``constants_tb`` 默认值。
    返回 ``tap``。
    """
    snippet = (
        (getattr(_T, "TB_MTOP_LISTEN_URL_CONTAINS", None) or "") or getattr(
            _T,
            "TB_MTOP_WIRELESS_RECOMMEND_SNIPPET",
            "",
        )
        or ""
    ).strip() or (
        "https://h5api.m.taobao.com/h5/mtop.relationrecommend."
        "wirelessrecommend.recommend/2.0/"
    )

    excludes = getattr(_T, "TB_MTOP_URL_EXCLUDE_FRAGMENTS", ()) or ()

    pm = pager_max_pages
    if pm is None:
        pm = int(getattr(_T, "TB_PAGER_MAX_PAGES", 1) or 1)
    pm = max(1, pm)
    if not listen_mtop_json:
        pm = 1

    open_tb_landing(sb, url=landing_url)
    if listen_mtop_json:
        cap = max(200, min(2000, pm * 80))
        tap = attach_sb_cdp_json_listener(
            sb,
            url_contains=snippet,
            url_excludes=excludes,
            resource_types=(),
            max_captures=cap,
        )
    else:
        tap = JsonListenSession()

    _sleep_uniform(sb, "TB_SLEEP_BEFORE_FIRST_SEARCH_ACTION", (0.7, 1.55))
    search_keyword(sb, keyword)
    _browse_listing_if_cfg(sb, after_search=True)

    for page_ix in range(pm):
        if listen_mtop_json:
            finalize_json_reads_blocking(sb, tap)
            print(
                f"[tb.flows] 已处理第 {page_ix + 1}/{pm} 页，mtop 捕获累计 {len(tap.captures)} 条",
                flush=True,
            )
        if page_ix >= pm - 1:
            break
        if not click_tb_search_next_page(sb):
            print(
                "[tb.flows] 未找到可点的「下一页」（或已到末页），提前结束翻页",
                flush=True,
            )
            break
        lo, hi = _pager_after_click_range(pager_after_click_sleep)
        sb.sleep(random.uniform(lo, hi))
        elo, ehi = _pair_scaled(
            "TB_PAGER_INTER_PAGE_EXTRA_SLEEP", (0.0, 0.0)
        )
        if ehi > 0.0:
            sb.sleep(random.uniform(max(0.0, elo), ehi))

    if listen_mtop_json:
        if save_mtop_json and tap.captures:
            from .notes_sink import save_mtop_captures

            save_mtop_captures(
                tap.captures,
                keyword=(keyword or "").strip(),
                raw_dir=mtop_save_dir,
                only_main_bundle=mtop_save_only_main_bundle,
                save_run_dir_by_time=mtop_save_run_dir_by_time,
                export_csv_after_save=mtop_export_csv_after_save,
            )
    return tap
