# -*- coding: utf-8 -*-
"""CDP 类人操作：**贝塞尔指针轨迹 + 分段滚动**。供淘宝等平台 ``flows`` 复用。"""
from __future__ import annotations

import asyncio
import json
import math
import random
from typing import Any, Callable, Sequence

import mycdp.input_ as cdp_input

EvalFn = Callable[[str], Any]


def _eval(sb: Any, expression: str) -> Any:
    fn: EvalFn = getattr(sb.cdp, "evaluate", None)  # noqa: DUO101
    if not callable(fn):
        raise RuntimeError("sb.cdp.evaluate 不可用")
    return fn(expression)


def _viewport_wh(sb: Any) -> tuple[float, float]:
    try:
        r = _eval(
            sb,
            "({w:Math.max(document.documentElement.clientWidth||0,window.innerWidth||1),"
            "h:Math.max(document.documentElement.clientHeight||0,window.innerHeight||1)})",
        )
        if isinstance(r, dict):
            return float(r.get("w", 960)), float(r.get("h", 900))
        if isinstance(r, str):
            o = json.loads(r)
            return float(o["w"]), float(o["h"])
    except BaseException:
        pass
    return 1280.0, 860.0


def scroll_into_center(
    sb: Any, css_selector: str, *, smooth_chance: float = 0.32
) -> bool:
    """``scrollIntoView`` 居中；``smooth_chance`` 为使用 ``behavior: 'smooth'`` 的概率。"""
    j = json.dumps(css_selector)
    use_smooth = random.random() < float(max(0.0, min(1.0, smooth_chance)))
    beh = "smooth" if use_smooth else "instant"
    try:
        _eval(
            sb,
            "(function(sel){const el=document.querySelector(sel);"
            "if(!el)return false;"
            'el.scrollIntoView({block:"center",inline:"nearest",behavior:"'
            + beh
            + '"});'
            "return true;})("
            + j
            + ")",
        )
        if use_smooth:
            sb.sleep(random.uniform(0.14, 0.62))
        return True
    except BaseException:
        return False


def _split_integer_sum(total: int, n: int) -> list[int]:
    """将 ``total`` 拆成 ``n`` 段正整数（同号），各段之和为 ``abs(total)`` 再乘符号。"""
    if n <= 1:
        return [total]
    sign = 1 if total >= 0 else -1
    at = abs(int(total))
    if at == 0:
        return [0] * n
    n = max(2, min(int(n), at))
    if n <= 1:
        return [sign * at]
    splits = sorted(random.sample(range(1, at), n - 1))
    pts = [0] + splits + [at]
    return [sign * (pts[i + 1] - pts[i]) for i in range(len(pts) - 1)]


def _element_center_viewport(sb: Any, css_selector: str) -> tuple[float, float] | None:
    j = json.dumps(css_selector)
    try:
        r = _eval(
            sb,
            "(function(sel){const el=document.querySelector(sel);if(!el)return null;"
            "const r=el.getBoundingClientRect();"
            'return{x:r.left+r.width/2,y:r.top+r.height/2};})('
            + j
            + ")",
        )
        if isinstance(r, dict) and "x" in r:
            return float(r["x"]), float(r["y"])
    except BaseException:
        pass
    return None


def _quad_bezier(
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
    n_seg: int,
) -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    for i in range(n_seg + 1):
        t = i / float(max(1, n_seg))
        u = 1.0 - t
        x = u * u * p0[0] + 2 * u * t * p1[0] + t * t * p2[0]
        y = u * u * p0[1] + 2 * u * t * p1[1] + t * t * p2[1]
        if i:
            jm = random.uniform(1.2, 1.85)
            jitter = random.uniform(-1.35 * jm, 1.35 * jm)
            x += jitter
            y += random.uniform(-1.35 * jm, 1.35 * jm)
        pts.append((x, y))
    return pts


def _start_point_viewport(sb: Any) -> tuple[float, float]:
    w, h = _viewport_wh(sb)
    return (
        random.uniform(w * 0.06, w * 0.94),
        random.uniform(h * 0.07, min(h * 0.93, max(40.0, h - 80))),
    )


def browse_scroll_burst(
    sb: Any,
    *,
    bursts_range: Sequence[int],
    pause_sec: tuple[float, float],
    delta_px: tuple[int, int],
    smooth_scroll_chance: float = 0.36,
    multi_step_chance: float = 0.44,
    max_micro_steps: int = 5,
) -> None:
    """模拟浏览：多次 ``window.scrollBy``；可穿插 ``smooth`` 与单次目标拆成多段。"""
    a, b = int(min(*bursts_range)), int(max(*bursts_range))
    n = random.randint(max(1, a), max(a, b))
    dlo, dhi = int(min(delta_px)), int(max(delta_px))
    plo, phi = float(min(pause_sec)), float(max(pause_sec))
    sc = float(max(0.0, min(1.0, smooth_scroll_chance)))
    mc = float(max(0.0, min(1.0, multi_step_chance)))
    mmax = max(2, min(12, int(max_micro_steps)))

    def _one_scroll_top(dyt: int) -> None:
        beh = "'smooth'" if random.random() < sc else "'instant'"
        try:
            _eval(
                sb,
                "window.scrollBy({top:%d,left:0,behavior:%s});" % (dyt, beh),
            )
        except BaseException:
            raise

    for _ in range(n):
        dy = random.randint(min(dlo, dhi), max(dlo, dhi))
        if random.random() < 0.14:
            dy = -abs(dy)

        chunks: list[int]
        if abs(dy) >= 52 and random.random() < mc:
            ns = random.randint(
                2, min(mmax, max(2, abs(dy) // 40)),
            )
            chunks = _split_integer_sum(dy, ns)
        else:
            chunks = [dy]

        for part in chunks:
            if part == 0:
                continue
            try:
                _one_scroll_top(part)
            except BaseException:
                break
            sb.sleep(random.uniform(plo, phi))


async def _mouse_path_click_async(
    tab: Any,
    *,
    path: list[tuple[float, float]],
    step_pause: tuple[float, float],
    press_pause: tuple[float, float],
) -> None:
    try:
        await tab.aopen()
    except BaseException:
        pass
    plo, phi = float(min(step_pause)), float(max(step_pause))
    for xy in path[:-1]:
        await tab.send(
            cdp_input.dispatch_mouse_event(
                type_="mouseMoved",
                x=float(xy[0]),
                y=float(xy[1]),
            ),
        )
        await asyncio.sleep(random.uniform(plo, phi))
    xf, yf = path[-1]
    hp_lo, hp_hi = float(min(press_pause)), float(max(press_pause))
    await tab.send(
        cdp_input.dispatch_mouse_event(
            type_="mouseMoved",
            x=float(xf),
            y=float(yf),
        ),
    )
    await asyncio.sleep(random.uniform(0.02, hp_lo))
    await tab.send(
        cdp_input.dispatch_mouse_event(
            type_="mousePressed",
            x=float(xf),
            y=float(yf),
            button=cdp_input.MouseButton.LEFT,
            buttons=1,
            click_count=1,
        ),
    )
    await asyncio.sleep(random.uniform(hp_lo, hp_hi))
    await tab.send(
        cdp_input.dispatch_mouse_event(
            type_="mouseReleased",
            x=float(xf),
            y=float(yf),
            button=cdp_input.MouseButton.LEFT,
            buttons=0,
            click_count=1,
        ),
    )


def mouse_click_human_like(
    sb: Any,
    css_selector: str,
    *,
    segments: tuple[int, int],
    step_pause: tuple[float, float],
    press_pause: tuple[float, float],
    scroll_smooth_chance: float = 0.32,
    pre_path_sleep: tuple[float, float] = (0.02, 0.19),
) -> bool:
    """
    先入视窗，再沿二次贝塞尔移动指针并点击；失败时返回 ``False``（调用方可再 ``mouse_click``）。
    """
    scroll_into_center(sb, css_selector, smooth_chance=scroll_smooth_chance)
    plo, phi = float(min(pre_path_sleep)), float(max(pre_path_sleep))
    sb.sleep(random.uniform(plo, phi))
    end = _element_center_viewport(sb, css_selector)
    if end is None:
        return False
    start = _start_point_viewport(sb)
    mid = (
        (start[0] + end[0]) / 2.0 + random.uniform(-140, 140),
        (start[1] + end[1]) / 2.0 + random.uniform(-90, 90),
    )
    ux, uy = -(end[1] - start[1]), (end[0] - start[0])
    ln = math.hypot(ux, uy) or 1.0
    ux /= ln
    uy /= ln
    off = random.uniform(-95.0, 95.0)
    ctrl = (mid[0] + ux * off, mid[1] + uy * off + random.uniform(-28.0, 28.0))
    slo, shi = int(min(segments)), int(max(segments))
    n_seg = random.randint(max(10, slo), max(slo + 1, shi))
    path = _quad_bezier(start, ctrl, end, n_seg)
    loop = sb.cdp.get_event_loop()

    async def _run() -> None:
        tab = sb.cdp.page
        await _mouse_path_click_async(
            tab,
            path=path,
            step_pause=step_pause,
            press_pause=press_pause,
        )

    loop.run_until_complete(_run())
    return True


def mouse_click_or_human(
    sb: Any,
    css_selector: str,
    *,
    enabled: bool,
    segments: tuple[int, int],
    step_pause: tuple[float, float],
    press_pause: tuple[float, float],
    scroll_smooth_chance: float = 0.32,
    pre_path_sleep: tuple[float, float] = (0.02, 0.19),
) -> None:
    if not enabled:
        sb.cdp.mouse_click(css_selector)
        return
    try:
        if mouse_click_human_like(
            sb,
            css_selector,
            segments=segments,
            step_pause=step_pause,
            press_pause=press_pause,
            scroll_smooth_chance=scroll_smooth_chance,
            pre_path_sleep=pre_path_sleep,
        ):
            return
    except BaseException:
        pass
    sb.cdp.mouse_click(css_selector)
