# -*- coding: utf-8 -*-
"""
SeleniumBase **仅**用 ``sb.cdp.page``（同一 CDP WebSocket）按 **已知 URL 子串** 认领请求，操作结束后再拉正文。

**思路**（与「我知道接口 path，在操作里找到对应响应」一致）；**非重放**：不调 ``Fetch.enable`` / 不改写响应，
页面仍走正常网络栈，此处仅在响应完成后 **只读复制** 正文。

1. ``Network.responseReceived``：**同步**把 ``request_id → {url, mime}`` 写入 ``pending``（只登记，不读 body）。可选 ``url_excludes``：完整 URL 中含任一则丢弃（同 path、不同 query）。
2. ``finalize_json_reads_blocking``：对 ``pending`` 每条 ``Network.getResponseBody`` 轮询拉正文。

底层 **SeleniumBase UC** 的 ``Connection.send`` 曾把 **任意** 异常吞掉并 ``aclose()``，导致 ``getResponseBody`` 偶发 CDP 错误时连接被关、后续恒为 ``send(None)``。本模块在 **`sb.cdp.page` 实例** 上绑定补丁（SeleniumBase 禁止改写 ``Connection.send`` **类属性**）。

若仍失败，再查登录、反爬与 ``requestId`` 是否与当前页一致。
"""
from __future__ import annotations

import asyncio
import base64
import itertools
import json
import re
import types
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Pattern, Sequence

import mycdp.network as cdp_network
from websockets.protocol import State


def _patch_tab_send(tab: Any) -> None:
    """任意 ``Connection`` 实例上应用 ProtocolException 保留补丁（可对新标签连接重复调用）。"""
    if tab is None:
        return
    if getattr(tab, "_sb_json_listen_send_patched", False):
        return
    try:
        from seleniumbase.undetected.cdp_driver.connection import (
            Connection as _UCConn,
            Listener as _UCListener,
            ProtocolException,
            Transaction,
        )
    except ImportError:
        return

    async def patched_send(self: Any, cdp_obj: Any, _is_update: bool = True) -> Any:
        await self.aopen()
        if not self.websocket or self.websocket.state is State.CLOSED:
            return
        if getattr(self, "browser", None):
            browser = self.browser
            if getattr(browser, "config", None):
                if browser.config.expert:
                    await self._prepare_expert()
                if browser.config.headless:
                    await self._prepare_headless()
        if not self.listener or not self.listener.running:
            self.listener = _UCListener(self)
        try:
            tx = Transaction(cdp_obj)
            tx.connection = self
            if not self.mapper:
                self.__count__ = itertools.count(0)
            tx.id = next(self.__count__)
            self.mapper.update({tx.id: tx})
            if not _is_update:
                await self._register_handlers()
            await self.websocket.send(tx.message)
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    action="ignore",
                    category=RuntimeWarning,
                    message=".*coroutine.*",
                )
                try:
                    return await tx
                except ProtocolException as e:
                    e.message += f"\ncommand:{tx.method}\nparams:{tx.params}"
                    raise
        except ProtocolException:
            raise
        except Exception:
            await self.aclose()

    patched_send.__doc__ = getattr(_UCConn.send, "__doc__", None)
    setattr(tab, "send", types.MethodType(patched_send, tab))
    setattr(tab, "_sb_json_listen_send_patched", True)


def _ensure_sb_uc_connection_send_preserves_protocol_exception(sb: Any) -> None:
    """对 ``sb.cdp.page`` 实例应用补丁（向后兼容入口）。"""
    _cdp = getattr(sb, "cdp", None)
    tab = getattr(_cdp, "page", None) if _cdp is not None else None
    _patch_tab_send(tab)


@dataclass
class CapturedJsonResponse:
    """单次命中：原始文本 + 解析结果。"""

    url: str
    body_text: str
    parsed: dict[str, Any] | list[Any] | None
    parse_error: str | None = None
    mime_type: str = ""


@dataclass
class JsonListenSession:
    """``attach_sb_cdp_json_listener`` 返回；读 ``captures`` / ``latest`` / ``last_errors``。"""

    captures: list[CapturedJsonResponse] = field(default_factory=list)
    last_errors: list[str] = field(default_factory=list)
    """与 ``attach`` 内 ``pending`` 同一 dict；仅内部使用。"""
    pending_by_request: dict[Any, dict[str, str]] | None = field(default=None, repr=False)
    max_captures: int = 200
    on_capture: Callable[[CapturedJsonResponse], None] | None = field(default=None, repr=False)
    """`(Connection 或等价对象, callback)`；供摘除 ``ResponseReceived`` 以免重复挂载。"""
    listener_detach_pairs: list[tuple[Any, Callable[..., Any]]] = field(
        default_factory=list, repr=False,
    )

    def clear(self) -> None:
        self.captures.clear()
        self.last_errors.clear()

    @property
    def latest(self) -> CapturedJsonResponse | None:
        return self.captures[-1] if self.captures else None

    def _note_error(self, msg: str, *, max_kept: int = 15) -> None:
        self.last_errors.append(msg)
        if len(self.last_errors) > max_kept:
            del self.last_errors[: len(self.last_errors) - max_kept]


def detach_json_listen_session_handlers(session: JsonListenSession) -> None:
    """从 UC ``Connection.handlers``（或等价结构）移除本会话登记的 ``ResponseReceived`` 回调。"""
    pairs = list(getattr(session, "listener_detach_pairs", None) or ())
    for obj, cb in pairs:
        try:
            h = getattr(obj, "handlers", None)
            if h is None:
                continue
            lst = h[cdp_network.ResponseReceived]
            while cb in lst:
                lst.remove(cb)
        except BaseException:
            pass
    session.listener_detach_pairs.clear()


def _norm_needles(url_contains: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(url_contains, str):
        return (url_contains.strip(),) if url_contains.strip() else ()
    return tuple(s.strip() for s in url_contains if (s or "").strip())


def _norm_url_excludes(url_excludes: str | Sequence[str] | None) -> tuple[str, ...]:
    """``url_excludes`` 任一子串命中完整 URL 时丢弃（用于同 path、不同 ``data`` 的干扰请求）。"""
    if url_excludes is None:
        return ()
    if isinstance(url_excludes, str):
        return (url_excludes.strip(),) if url_excludes.strip() else ()
    return tuple(s.strip() for s in url_excludes if (s or "").strip())


def _url_has_any_substring(url: str, needles: tuple[str, ...]) -> bool:
    if not needles:
        return False
    return any(n in url for n in needles)


def _url_matches(
    url: str,
    needles: tuple[str, ...],
    url_regex: Pattern[str] | None,
) -> bool:
    if url_regex is not None and url_regex.search(url):
        return True
    if needles and any(n in url for n in needles):
        return True
    return False


def _decode_body(body: str, base64_encoded: bool) -> str:
    if not base64_encoded:
        return body
    try:
        raw = base64.b64decode(body)
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return body


def _parse_json_text(text: str) -> tuple[dict[str, Any] | list[Any] | None, str | None]:
    s = (text or "").strip()
    if not s:
        return None, "empty body"
    try:
        return json.loads(s), None
    except json.JSONDecodeError as e:
        return None, str(e)


def _append_capture(
    session: JsonListenSession,
    *,
    url: str,
    mime_type: str,
    body_text: str,
) -> bool:
    if len(session.captures) >= max(0, int(session.max_captures)):
        return False
    parsed, perr = _parse_json_text(body_text)
    row = CapturedJsonResponse(
        url=url,
        body_text=body_text,
        parsed=parsed,
        parse_error=perr,
        mime_type=mime_type,
    )
    session.captures.append(row)
    if session.on_capture is not None:
        try:
            session.on_capture(row)
        except Exception:
            pass
    return True


def attach_sb_cdp_json_listener(
    sb: Any,
    *,
    url_contains: str | Sequence[str] | None = None,
    url_regex: str | Pattern[str] | None = None,
    url_excludes: str | Sequence[str] | None = None,
    resource_types: Sequence[cdp_network.ResourceType] | None = (
        cdp_network.ResourceType.XHR,
        cdp_network.ResourceType.FETCH,
    ),
    mime_contains: str | None = None,
    max_captures: int = 200,
    on_capture: Callable[[CapturedJsonResponse], None] | None = None,
) -> JsonListenSession:
    """
    在 ``activate_cdp_mode`` 之后调用。按 URL 子串/正则登记 **ResponseReceived**，写入 ``session.pending_by_request``。

    - ``resource_types``：传 ``()`` 表示不按类型过滤（部分 POST 在 CDP 里标为 OTHER）。
    - ``url_excludes``：**完整 URL** 中含任一则**不登记**（同域名同 path、query 内 ``data`` 不同）。
    - 取 body 必须再调 ``finalize_json_reads_blocking(sb, session)``。
    """
    _ensure_sb_uc_connection_send_preserves_protocol_exception(sb)
    needles = _norm_needles(url_contains or ())
    exclude_needles = _norm_url_excludes(url_excludes)
    pattern: Pattern[str] | None
    if url_regex is None:
        pattern = None
    elif isinstance(url_regex, Pattern):
        pattern = url_regex
    else:
        pattern = re.compile(url_regex)

    if not needles and pattern is None:
        raise ValueError("请至少设置 url_contains 或 url_regex")

    session = JsonListenSession(max_captures=max_captures, on_capture=on_capture)
    pending: dict[Any, dict[str, str]] = {}
    session.pending_by_request = pending

    tab = sb.cdp.page
    loop = sb.cdp.get_event_loop()

    def _rtype_ok(ev: cdp_network.ResponseReceived) -> bool:
        if not resource_types:
            return True
        t = getattr(ev, "type_", None)
        for rt in resource_types:
            if t == rt:
                return True
            tv = getattr(t, "value", t)
            rv = getattr(rt, "value", rt)
            if tv == rv:
                return True
        return False

    def on_response_received(event: cdp_network.ResponseReceived) -> None:
        try:
            resp = event.response
            url = (resp.url or "").strip()
            if not _url_matches(url, needles, pattern):
                return
            if _url_has_any_substring(url, exclude_needles):
                return
            if not _rtype_ok(event):
                return
            mime = (resp.mime_type or "").strip()
            if mime_contains and (mime_contains not in mime):
                return
            rid = event.request_id
            pending[rid] = {"url": url, "mime": mime}
        except BaseException as e:
            session._note_error(f"ResponseReceived: {e!s}")

    sb.cdp.add_handler(cdp_network.ResponseReceived, on_response_received)
    session.listener_detach_pairs.append((sb.cdp, on_response_received))

    async def _enable_network() -> None:
        await tab.send(cdp_network.enable())

    try:
        loop.run_until_complete(_enable_network())
    except BaseException as e:
        session._note_error(f"Network.enable: {e!s}")

    return session


def finalize_json_reads_blocking(
    sb: Any,
    session: JsonListenSession,
    *,
    max_rounds_per_request: int = 36,
    round_sleep_sec: float = 0.1,
    overall_timeout_sec: float | None = None,
    per_send_timeout_sec: float | None = None,
) -> None:
    """
    在点击/搜索等动作结束后调用：只对 **当前仍留在 ``pending`` 里的 ``request_id``**，
    用 ``sb.cdp.page.send(Network.getResponseBody)`` 拉正文（短间隔轮询，直到 body 在 CDP 侧可读）。

    ``listener_pending_meta`` / ``pending_by_request`` 兼容旧字段名（若曾在外部挂过别名 dict）。

    ``overall_timeout_sec``：整体超时（秒）。浏览器/CDP 已断开时 ``tab.send`` 可能永久挂起，
    半自动监听应传入有限值以便跳出循环并执行 ``finally`` 落盘；``None`` 保持旧行为（无整体超时）。

    ``per_send_timeout_sec``：**单次** ``getResponseBody`` 的 ``await tab.send`` 上限（秒）。
    SeleniumBase UC 的 WebSocket ``ping_timeout`` 可达半小时，关窗后若不限制单次 ``send``，
    仅靠外层 ``wait_for(_run)`` 有时仍会被长时间阻塞；``None`` 时默认 **10** 秒。
    """
    _ensure_sb_uc_connection_send_preserves_protocol_exception(sb)

    try:
        from seleniumbase.undetected.cdp_driver.connection import ProtocolException as _UCProtocolException
    except ImportError:
        _UCProtocolException = None  # type: ignore[assignment,misc]

    pending = getattr(session, "pending_by_request", None) or getattr(
        session, "listener_pending_meta", None,
    )
    if pending is None:
        return

    loop = sb.cdp.get_event_loop()
    tab = sb.cdp.page

    if per_send_timeout_sec is not None and float(per_send_timeout_sec) > 0:
        ps_eff = float(per_send_timeout_sec)
    else:
        ps_eff = 10.0
    ps_eff = max(2.0, min(ps_eff, 120.0))

    async def _run() -> None:
        try:
            await asyncio.wait_for(tab.aopen(), timeout=min(15.0, ps_eff + 5.0))
        except TimeoutError:
            session._note_error(f"finalize_json_reads_blocking: tab.aopen 超时（>{min(15.0, ps_eff + 5.0):g}s）")
            return
        except BaseException:
            pass

        await asyncio.sleep(0.15)

        items = list(pending.items())
        if not items:
            return

        for rid, meta in items:
            url = meta.get("url") or ""
            mime = meta.get("mime") or ""
            ok = False
            last_hist = ""

            for rnd in range(max(1, int(max_rounds_per_request))):
                if rnd:
                    await asyncio.sleep(round_sleep_sec)

                raw: Any = None
                try:
                    raw = await asyncio.wait_for(
                        tab.send(cdp_network.get_response_body(rid)),
                        timeout=ps_eff,
                    )
                except TimeoutError:
                    last_hist = f"per_send_timeout>{ps_eff:g}s"
                except BaseException as e:
                    if _UCProtocolException is not None and isinstance(
                        e,
                        _UCProtocolException,
                    ):
                        last_hist = f"cdp:{e!s}"
                        break  # Chrome 明确拒绝（如资源已 GC），不重试
                    else:
                        last_hist = f"exc:{e!s}"
                else:
                    if isinstance(raw, tuple) and len(raw) >= 2:
                        bp, fg = raw[0], raw[1]
                        txt = _decode_body(
                            "" if bp is None else str(bp),
                            bool(fg),
                        )
                        if not _append_capture(
                            session,
                            url=url,
                            mime_type=mime,
                            body_text=txt,
                        ):
                            session._note_error("已达 max_captures，跳过后续正文")
                        pending.pop(rid, None)
                        ok = True
                        break
                    last_hist = (
                        "send(None)" if raw is None else f"bad_ty={type(raw)!r}"
                    )

            if not ok:
                u = url[:120] + ("…" if len(url) > 120 else "")
                session._note_error(
                    f"未拉到正文 「{u}」 末次:{last_hist or '?'}",
                )

    try:
        if overall_timeout_sec is not None and overall_timeout_sec > 0:
            loop.run_until_complete(
                asyncio.wait_for(_run(), timeout=float(overall_timeout_sec)),
            )
        else:
            loop.run_until_complete(_run())
    except TimeoutError:
        session._note_error(
            f"finalize_json_reads_blocking: 整体超时 {overall_timeout_sec}s（浏览器/CDP 可能已不可用）",
        )
    except BaseException as e:
        session._note_error(f"finalize_json_reads_blocking: {e!s}")


def attach_to_tab(
    tab: Any,
    loop: Any,
    session: JsonListenSession,
    *,
    needles: tuple[str, ...] = (),
    exclude_needles: tuple[str, ...] = (),
    resource_types: Sequence[cdp_network.ResourceType] | None = (),
    mime_contains: str | None = None,
) -> None:
    """在任意 ``Connection`` 实例上挂 ``Network.responseReceived`` 监听（多标签支持）。

    供 ``_MultiTabListener.scan_new_tabs`` 对新开标签调用；与
    ``attach_sb_cdp_json_listener`` 对齐，但直接接受 ``(tab, loop)`` 而不依赖 ``sb``。
    取 body 须再调 ``finalize_tab_blocking(tab, loop, session)``。
    """
    _patch_tab_send(tab)
    pending: dict[Any, dict[str, str]] = {}
    session.pending_by_request = pending

    def _rtype_ok(ev: cdp_network.ResponseReceived) -> bool:
        if not resource_types:
            return True
        t = getattr(ev, "type_", None)
        for rt in resource_types:
            if t == rt:
                return True
            if getattr(t, "value", t) == getattr(rt, "value", rt):
                return True
        return False

    def on_response_received(event: cdp_network.ResponseReceived) -> None:
        try:
            resp = event.response
            url = (resp.url or "").strip()
            if needles and not _url_has_any_substring(url, needles):
                return
            if exclude_needles and _url_has_any_substring(url, exclude_needles):
                return
            if not _rtype_ok(event):
                return
            mime = (resp.mime_type or "").strip()
            if mime_contains and mime_contains not in mime:
                return
            rid = event.request_id
            pending[rid] = {"url": url, "mime": mime}
        except BaseException as e:
            session._note_error(f"ResponseReceived(tab): {e!s}")

    tab.add_handler(cdp_network.ResponseReceived, on_response_received)
    session.listener_detach_pairs.append((tab, on_response_received))

    async def _enable() -> None:
        await tab.send(cdp_network.enable())

    try:
        loop.run_until_complete(_enable())
    except BaseException as e:
        session._note_error(f"Network.enable(tab): {e!s}")


def finalize_tab_blocking(
    tab: Any,
    loop: Any,
    session: JsonListenSession,
    *,
    max_rounds_per_request: int = 36,
    round_sleep_sec: float = 0.1,
    overall_timeout_sec: float | None = None,
    per_send_timeout_sec: float | None = None,
) -> None:
    """与 ``finalize_json_reads_blocking`` 逻辑完全一致，但接受显式 ``(tab, loop)``（多标签用）。"""
    _patch_tab_send(tab)

    try:
        from seleniumbase.undetected.cdp_driver.connection import ProtocolException as _UCProtocolException
    except ImportError:
        _UCProtocolException = None  # type: ignore[assignment,misc]

    # 分开判断「从未初始化」和「暂时为空」：
    # {} 空 dict 仍要调 aopen() 保持连接/Listener 活跃，否则 Listener 停摆后不再分发事件
    pending = getattr(session, "pending_by_request", None)
    if pending is None:
        pending = getattr(session, "listener_pending_meta", None)
    if pending is None:
        return  # 从未初始化，跳过

    if per_send_timeout_sec is not None and float(per_send_timeout_sec) > 0:
        ps_eff = float(per_send_timeout_sec)
    else:
        ps_eff = 10.0
    ps_eff = max(2.0, min(ps_eff, 120.0))

    async def _run() -> None:
        # 无论 pending 是否为空都先 aopen()，确保 WebSocket 连接保持打开
        try:
            await asyncio.wait_for(tab.aopen(), timeout=min(15.0, ps_eff + 5.0))
        except TimeoutError:
            # 连接已死：清空 pending 避免下次循环无限重试
            pending.clear()
            session._note_error(f"finalize_tab_blocking: tab.aopen 超时（>{min(15.0, ps_eff + 5.0):g}s），清空 pending")
            return
        except BaseException:
            pass

        # pending 为空：连接/Listener 已保活，本轮无 body 需要拉取
        if not pending:
            return

        await asyncio.sleep(0.15)

        items = list(pending.items())
        if not items:
            return

        for rid, meta in items:
            url = meta.get("url") or ""
            mime = meta.get("mime") or ""
            ok = False
            last_hist = ""

            for rnd in range(max(1, int(max_rounds_per_request))):
                if rnd:
                    await asyncio.sleep(round_sleep_sec)

                raw: Any = None
                try:
                    raw = await asyncio.wait_for(
                        tab.send(cdp_network.get_response_body(rid)),
                        timeout=ps_eff,
                    )
                except TimeoutError:
                    last_hist = f"per_send_timeout>{ps_eff:g}s"
                except BaseException as e:
                    if _UCProtocolException is not None and isinstance(e, _UCProtocolException):
                        # CDP 协议错误：该 rid 不再有效，立即放弃
                        last_hist = f"cdp:{e!s}"
                        pending.pop(rid, None)
                        break
                    else:
                        last_hist = f"exc:{e!s}"
                else:
                    if isinstance(raw, tuple) and len(raw) >= 2:
                        bp, fg = raw[0], raw[1]
                        txt = _decode_body("" if bp is None else str(bp), bool(fg))
                        if not _append_capture(session, url=url, mime_type=mime, body_text=txt):
                            session._note_error("已达 max_captures，跳过后续正文")
                        pending.pop(rid, None)
                        ok = True
                        break
                    last_hist = "send(None)" if raw is None else f"bad_ty={type(raw)!r}"

            if not ok:
                # body 拉取失败，直接放弃（不重试），避免积压阻塞后续请求
                pending.pop(rid, None)
                u = url[:120] + ("…" if len(url) > 120 else "")
                session._note_error(f"未拉到正文（已放弃）「{u}」 末次:{last_hist or '?'}")

    try:
        if overall_timeout_sec is not None and overall_timeout_sec > 0:
            loop.run_until_complete(asyncio.wait_for(_run(), timeout=float(overall_timeout_sec)))
        else:
            loop.run_until_complete(_run())
    except TimeoutError:
        session._note_error(f"finalize_tab_blocking: 整体超时 {overall_timeout_sec}s（浏览器/CDP 可能已不可用）")
    except BaseException as e:
        session._note_error(f"finalize_tab_blocking: {e!s}")


def keepalive_tab(tab: Any, loop: Any) -> None:
    """通过 ``patched_send`` 路径发送 ``Network.enable()``，重启已停摆的 Listener。

    每隔若干秒调用一次即可；``Network.enable()`` 是幂等的，多次调用无副作用。
    """
    _patch_tab_send(tab)

    async def _ping() -> None:
        try:
            await asyncio.wait_for(tab.send(cdp_network.enable()), timeout=5.0)
        except BaseException:
            pass

    try:
        loop.run_until_complete(_ping())
    except BaseException:
        pass
