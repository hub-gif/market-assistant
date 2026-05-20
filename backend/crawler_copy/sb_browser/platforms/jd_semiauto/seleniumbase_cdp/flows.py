# -*- coding: utf-8 -*-
"""SeleniumBase + CDP：api.m.jd.com 监听、多标签挂载与主循环。"""
from __future__ import annotations

import json as _json
import sys
import time
import urllib.request as _urllib_request
from pathlib import Path
from typing import Any, Callable

from sb_browser.cdp_json_listen import (
    CapturedJsonResponse,
    JsonListenSession,
    attach_sb_cdp_json_listener,
    attach_to_tab,
    detach_json_listen_session_handlers,
    finalize_tab_blocking,
    keepalive_tab,
)

from ..common import constants_jd_semiauto as _cfg


def _get_chrome_debug_url(sb: Any) -> str | None:
    """从 driver capabilities 中读取 Chrome DevTools HTTP 基础地址。"""
    try:
        d = getattr(sb, "driver", None)
        if d is None:
            return None
        caps = getattr(d, "capabilities", None) or {}
        addr = (caps.get("goog:chromeOptions") or {}).get("debuggerAddress")
        if addr:
            return f"http://{addr}"
    except Exception:
        pass
    return None


class _MultiTabListener:
    """多标签 JsonListenSession 聚合：captures / last_errors / scan_new_tabs / finalize_all。"""

    def __init__(
        self,
        sb: Any,
        initial_session: JsonListenSession,
        needles: tuple[str, ...],
        max_tab_captures: int,
    ) -> None:
        self._needles = needles
        self._max_tab_captures = max_tab_captures
        self._sb = sb
        self._debug_url = _get_chrome_debug_url(sb)
        self._loop = sb.cdp.get_event_loop()

        initial_conn = sb.cdp.page
        # (targetId, Connection, JsonListenSession)；初始标签 targetId 用 "" 占位
        self._tab_sessions: list[tuple[str, Any, JsonListenSession]] = [
            ("", initial_conn, initial_session)
        ]
        self._known_target_ids: set[str] = set()
        self._destroyed_target_ids: set[str] = set()
        self._shared_errors: list[str] = []
        # 已关闭标签的捕获结果归档（从 _tab_sessions 移除前先转移）
        self._archived_captures: list[CapturedJsonResponse] = []

        # Target.targetCreated 事件队列：handler 把新 targetId 写入，主循环读取
        self._pending_new_targets: set[str] = set()
        self._target_listener_active: bool = False

        # 初始化：把当前已开的 page 标签标记为已知，避免重复挂载
        self._initialize_known_targets()
        # 订阅 Target.targetCreated，实现毫秒级感知新标签
        self._initialize_target_listener(sb)

    def _initialize_known_targets(self) -> None:
        if not self._debug_url:
            return
        try:
            resp = _urllib_request.urlopen(f"{self._debug_url}/json/list", timeout=2)
            for t in _json.loads(resp.read()):
                if t.get("type") == "page":
                    tid = t.get("id") or t.get("targetId") or ""
                    if tid:
                        self._known_target_ids.add(tid)
        except Exception:
            pass

    def _initialize_target_listener(self, sb: Any) -> None:
        """订阅 Target.created/destroyed；失败则只靠 HTTP /json/list 兜底。"""
        try:
            import mycdp.target as _cdp_target
        except ImportError:
            return

        def on_target_created(event: Any) -> None:
            try:
                info = getattr(event, "target_info", None)
                if info is None:
                    return
                if str(getattr(info, "type_", "") or "") != "page":
                    return
                tid = str(getattr(info, "target_id", "") or "")
                if tid and tid not in self._known_target_ids:
                    self._pending_new_targets.add(tid)
            except Exception:
                pass

        def on_target_destroyed(event: Any) -> None:
            try:
                tid = str(getattr(event, "target_id", "") or "")
                if tid:
                    self._destroyed_target_ids.add(tid)
            except Exception:
                pass

        try:
            browser = getattr(getattr(sb, "cdp", None), "browser", None)
            if browser is not None:
                # UC 的 ``Browser`` 本身无 ``add_handler``，事件挂在根 WebSocket ``Connection`` 上。
                root = getattr(browser, "connection", None)
                registrars = (
                    r
                    for r in (root, browser)
                    if r is not None and callable(getattr(r, "add_handler", None))
                )
                reg = next(registrars, None)
                if reg is None:
                    raise AttributeError("'Browser' 与 browser.connection 均无 add_handler")
                reg.add_handler(_cdp_target.TargetCreated, on_target_created)
                reg.add_handler(_cdp_target.TargetDestroyed, on_target_destroyed)
                self._target_listener_active = True
        except Exception as e:
            self._note_error(f"Target 事件订阅失败（降级为 HTTP 轮询）: {e!s}")

    @property
    def captures(self) -> list[CapturedJsonResponse]:
        result: list[CapturedJsonResponse] = list(self._archived_captures)
        for _, _, sess in self._tab_sessions:
            result.extend(sess.captures)
        return result

    @property
    def last_errors(self) -> list[str]:
        all_e: list[str] = list(self._shared_errors)
        for _, _, sess in self._tab_sessions:
            all_e.extend(sess.last_errors)
        seen: set[str] = set()
        unique: list[str] = []
        for e in reversed(all_e):
            if e not in seen:
                seen.add(e)
                unique.append(e)
            if len(unique) >= 15:
                break
        return list(reversed(unique))

    def _note_error(self, msg: str) -> None:
        self._shared_errors.append(msg)
        if len(self._shared_errors) > 15:
            del self._shared_errors[: len(self._shared_errors) - 15]

    def _has_pending(self) -> bool:
        """任意存活标签存在待拉取正文时返回 True。"""
        for tid, _, sess in self._tab_sessions:
            if tid and tid in self._destroyed_target_ids:
                continue
            p = getattr(sess, "pending_by_request", None)
            if p:
                return True
        return False

    def _attach_to_ws_url(self, tid: str, ws_url: str, url_hint: str = "") -> int:
        """创建 Connection 并挂载监听，返回 1（成功）或 0（失败）。"""
        try:
            from seleniumbase.undetected.cdp_driver.connection import Connection

            conn = Connection(ws_url)
            sess = JsonListenSession(max_captures=self._max_tab_captures)
            attach_to_tab(
                conn,
                self._loop,
                sess,
                needles=self._needles,
                resource_types=(),
                mime_contains=None,
            )
            self._tab_sessions.append((tid, conn, sess))
            return 1
        except Exception as e:
            self._note_error(f"新标签挂载失败 [{url_hint}]: {e!s}")
            return 0

    def scan_new_tabs(self, *, force_http: bool = False) -> int:
        """新标签挂载监听；优先事件队列，force_http 时 /json/list 兜底。"""
        count = 0

        # 快速路径：消费 Target.targetCreated 事件队列
        if self._pending_new_targets and self._debug_url:
            ws_base = self._debug_url.replace("http://", "ws://", 1)
            while self._pending_new_targets:
                tid = self._pending_new_targets.pop()
                if tid in self._known_target_ids:
                    continue
                self._known_target_ids.add(tid)
                ws_url = f"{ws_base}/devtools/page/{tid}"
                count += self._attach_to_ws_url(tid, ws_url, url_hint=f"targetId={tid}")

        # 慢速路径：HTTP 轮询兜底（事件订阅降级时必须 force_http=True 才触发）
        if not force_http or not self._debug_url:
            return count
        try:
            resp = _urllib_request.urlopen(f"{self._debug_url}/json/list", timeout=2)
            targets = _json.loads(resp.read())
        except Exception as e:
            self._note_error(f"scan_new_tabs(/json/list): {e!s}")
            return count

        for t in targets:
            if t.get("type") != "page":
                continue
            tid = t.get("id") or t.get("targetId") or ""
            if not tid or tid in self._known_target_ids:
                continue
            self._known_target_ids.add(tid)
            ws_url = t.get("webSocketDebuggerUrl") or ""
            if not ws_url:
                continue
            url_hint = (t.get("url") or "")[:60]
            count += self._attach_to_ws_url(tid, ws_url, url_hint=url_hint)

        return count

    def remount_network_listeners(self, sb: Any) -> int:
        """不关浏览器：卸钩子后主标签重挂并按 /json/list 补挂；返回 scan_new_tabs 增量。"""
        for tid, _conn, sess in list(self._tab_sessions):
            self._archived_captures.extend(sess.captures)
            detach_json_listen_session_handlers(sess)
            pend = getattr(sess, "pending_by_request", None)
            if pend:
                pend.clear()

        main_tid = ""
        try:
            page_conn = getattr(getattr(sb, "cdp", None), "page", None)
            targ = getattr(page_conn, "target", None)
            if targ is not None:
                main_tid = str(
                    getattr(targ, "target_id", "") or getattr(targ, "targetId", "") or "",
                ).strip()
        except Exception:
            pass
        # 仅保留当前主标签 id，使 /json/list 慢路径会对其余已打开页重新 attach（与启动时 _initialize_known_targets 行为对齐）
        self._known_target_ids = {main_tid} if main_tid else set()

        new_sess = attach_sb_cdp_json_listener(
            sb,
            url_contains=self._needles,
            resource_types=(),
            mime_contains=None,
            max_captures=self._max_tab_captures,
        )
        self._tab_sessions = [("", getattr(sb.cdp, "page", None), new_sess)]
        return self.scan_new_tabs(force_http=True)

    def _alive_tab_sessions(self) -> list[tuple[str, Any, JsonListenSession]]:
        return [
            (tid, conn, sess)
            for tid, conn, sess in self._tab_sessions
            if not (tid and tid in self._destroyed_target_ids)
        ]

    def _total_pending_alive(self) -> int:
        return sum(
            len(getattr(sess, "pending_by_request", None) or {})
            for _tid, _conn, sess in self._alive_tab_sessions()
        )

    def _burst_clear_pending_if_strictly_over(self, burst_at: int) -> None:
        """存活 pending 总和若 **大于** burst_at 则先删非保留 URL，必要时再整表清空。burst_at≤0 不启用。"""
        if burst_at <= 0:
            return
        n_before = self._total_pending_alive()
        if n_before <= burst_at:
            return
        raw_keep = getattr(_cfg, "SEMI_JD_PENDING_BURST_PROTECT_URL_SUBSTR", ()) or ()
        keep_subs = tuple(str(s).strip().lower() for s in raw_keep if str(s).strip())

        for _tid, _conn, sess in self._alive_tab_sessions():
            pend = getattr(sess, "pending_by_request", None)
            if not pend:
                continue
            for rid in list(pend.keys()):
                meta = pend.get(rid) or {}
                url = ((meta.get("url") or "") if isinstance(meta, dict) else "").lower()
                if keep_subs and any(sub in url for sub in keep_subs):
                    continue
                pend.pop(rid, None)

        if self._total_pending_alive() > burst_at:
            for _tid, _conn, sess in self._alive_tab_sessions():
                pend = getattr(sess, "pending_by_request", None)
                if pend:
                    pend.clear()
            self._note_error(
                f"pending 积压 {n_before}>{burst_at}，已全量清空（含商详保护后仍超限；本轮未入库正文丢弃）"
            )
            return

        self._note_error(
            f"pending 积压 {n_before}>{burst_at}，已删减非保留请求（商详核心 URL 已尽量保留）"
        )

    def prune_stale_tab_sessions(self) -> int:
        """与 /json/list 对账，移除已关闭页的会话。"""
        if not self._debug_url:
            return 0
        try:
            resp = _urllib_request.urlopen(f"{self._debug_url}/json/list", timeout=3)
            targets = _json.loads(resp.read())
        except Exception as e:
            self._note_error(f"prune_stale_tab_sessions(/json/list): {e!s}")
            return 0
        live: set[str] = set()
        for t in targets:
            if t.get("type") != "page":
                continue
            tid = str(t.get("id") or t.get("targetId") or "").strip()
            if tid:
                live.add(tid)
        removed = 0
        surviving: list[tuple[str, Any, JsonListenSession]] = []
        for tid, conn, sess in self._tab_sessions:
            # 首标签占位：与 sb.cdp.page 绑定，不按 targetId 对账剔除
            if tid == "":
                surviving.append((tid, conn, sess))
                continue
            if tid not in live:
                self._archived_captures.extend(sess.captures)
                pending = getattr(sess, "pending_by_request", None)
                if pending:
                    pending.clear()
                self._known_target_ids.discard(tid)
                self._destroyed_target_ids.discard(tid)
                removed += 1
                continue
            surviving.append((tid, conn, sess))
        if removed:
            self._tab_sessions = surviving
        return removed

    def finalize_all(
        self,
        tout: float,
        per_send: float,
        *,
        stop_file: Path | None = None,
    ) -> None:
        """各存活 tab 拉取 pending；stop_file 出现时提前结束本轮。"""
        burst_at = int(getattr(_cfg, "SEMI_JD_PENDING_BURST_CLEAR_AT", 17) or 0)
        self._burst_clear_pending_if_strictly_over(burst_at)

        prog_every = int(getattr(_cfg, "SEMI_JD_FINALIZE_PROGRESS_EVERY", 8) or 0)
        n_tab = len(self._tab_sessions)
        surviving: list[tuple[str, Any, JsonListenSession]] = []
        done_alive = 0
        sessions_snap = list(self._tab_sessions)
        i = 0
        stopped_early = False
        while i < len(sessions_snap):
            tid, conn, sess = sessions_snap[i]
            if tid and tid in self._destroyed_target_ids:
                # 标签已关闭：归档已捕获数据，清空 pending，从 _tab_sessions 剔除
                self._archived_captures.extend(sess.captures)
                pending = getattr(sess, "pending_by_request", None)
                if pending:
                    pending.clear()
                i += 1
                continue
            if stop_file is not None and stop_file.is_file():
                stopped_early = True
                break
            try:
                finalize_tab_blocking(
                    conn,
                    self._loop,
                    sess,
                    overall_timeout_sec=tout,
                    per_send_timeout_sec=per_send,
                )
            except BaseException as e:
                self._note_error(f"finalize: {e!s}")
            surviving.append((tid, conn, sess))
            done_alive += 1
            if (
                prog_every > 0
                and n_tab >= 12
                and done_alive < n_tab
                and done_alive % prog_every == 0
            ):
                print(
                    f"[jd_semiauto] finalize 进度 {done_alive}/{n_tab}（关页未卸会话时会偏慢；已对账剔除死会话）",
                    file=sys.stderr,
                    flush=True,
                )
            i += 1

        if stopped_early:
            for j in range(i, len(sessions_snap)):
                tid, conn, sess = sessions_snap[j]
                if tid and tid in self._destroyed_target_ids:
                    self._archived_captures.extend(sess.captures)
                    pending = getattr(sess, "pending_by_request", None)
                    if pending:
                        pending.clear()
                    continue
                surviving.append((tid, conn, sess))

        self._tab_sessions = surviving
        self._burst_clear_pending_if_strictly_over(burst_at)


def wait_terminal_confirm_login_after_cdp(
    *,
    status_sink: Callable[[str], None] | None = None,
) -> None:
    """CDP 就绪后终端阻塞等回车；无 TTY 则跳过。"""
    sink = status_sink or (lambda m: print(m, file=sys.stderr, flush=True))
    sink(
        "[jd_semiauto] 浏览器已就绪，请在窗口内完成登录；"
        "登录后在此终端按一次回车，开始挂载 API 监听。"
    )
    try:
        input()
    except EOFError:
        sink("[jd_semiauto] stdin 不可用，跳过登录等待，直接开始监听。")


def _wait_file_signal_for_login(
    login_file: Path,
    *,
    poll_interval: float = 0.5,
    status_sink: Callable[[str], None] | None = None,
) -> None:
    """login_file 出现即视为已登录。"""
    sink = status_sink or (lambda m: print(m, file=sys.stderr, flush=True))
    sink("[jd_semiauto] 浏览器已就绪，等待前端「确认登录」信号…")
    while not login_file.is_file():
        time.sleep(poll_interval)
    sink("[jd_semiauto] 已收到登录确认，开始挂载 API 监听。")


def open_landing_cdp(
    sb: Any,
    *,
    landing_url: str | None = None,
    pause_for_login_confirm: bool | None = None,
    login_file: Path | None = None,
) -> None:
    u = (
        (landing_url or "").strip()
        or str(getattr(_cfg, "SEMI_JD_DEFAULT_LANDING_URL", "") or "").strip()
        or "https://www.jd.com/"
    )
    sb.activate_cdp_mode(u)
    sb.cdp.open(u)
    sec = float(getattr(_cfg, "SEMI_JD_POST_ACTIVATE_SLEEP_SEC", 3.0))
    sb.sleep(max(0.5, sec))

    if pause_for_login_confirm is None:
        pause_for_login_confirm = bool(getattr(_cfg, "SEMI_JD_PAUSE_FOR_LOGIN_CONFIRM", True))
    if pause_for_login_confirm:
        if login_file is not None:
            _wait_file_signal_for_login(login_file)
        else:
            wait_terminal_confirm_login_after_cdp()


def attach_jd_api_listener(sb: Any, *, max_captures: int | None = None) -> _MultiTabListener:
    """当前页挂载监听，返回多标签会话。"""
    raw = getattr(_cfg, "SEMI_JD_LISTEN_URL_CONTAINS", ())
    if isinstance(raw, str):
        needles: tuple[str, ...] = (raw,) if raw.strip() else ()
    else:
        needles = tuple(x for x in raw if str(x).strip())
    if not needles:
        needles = ("api.m.jd.com/api", "api.m.jd.com/?", "api.m.jd.com/client.action")

    mc = (
        int(max_captures)
        if max_captures is not None
        else int(getattr(_cfg, "SEMI_JD_MAX_CAPTURES", 1200))
    )
    max_captures_eff = max(200, mc)

    initial_session = attach_sb_cdp_json_listener(
        sb,
        url_contains=needles,
        resource_types=(),
        mime_contains=None,
        max_captures=max_captures_eff,
    )
    return _MultiTabListener(sb, initial_session, needles, max_captures_eff)


def listen_until_stopped(
    sb: Any,
    tap: _MultiTabListener,
    *,
    stop_file: Path | None = None,
    restart_file: Path | None = None,
    status_sink: Callable[[str], None] | None = None,
    save_sink: Callable[[list[CapturedJsonResponse]], None] | None = None,
) -> None:
    """主循环直至 stop_file 或 Ctrl+C；restart_file 触发 remount。save_sink 为增量落盘。"""
    sink = status_sink or (lambda m: print(m, file=sys.stderr, flush=True))
    p = max(0.1, float(getattr(_cfg, "SEMI_JD_LISTEN_POLL_SEC", 0.3)))
    status_every_sec = float(getattr(_cfg, "SEMI_JD_LISTEN_STATUS_EVERY_SEC", 15.0))
    tout = max(3.0, float(getattr(_cfg, "SEMI_JD_FINALIZE_OVERALL_TIMEOUT_SEC", 10.0)))
    per_send = max(1.0, min(float(getattr(_cfg, "SEMI_JD_FINALIZE_PER_SEND_TIMEOUT_SEC", 3.0)), 30.0))
    scan_floor = float(getattr(_cfg, "SEMI_JD_NEW_TAB_HTTP_SCAN_MIN_SEC", 0.35))
    scan_interval = max(
        max(0.1, scan_floor),
        float(getattr(_cfg, "SEMI_JD_NEW_TAB_SCAN_SEC", 3.0)),
    )

    sink("[jd_semiauto] 开始监听 api.m.jd.com；在浏览器内正常操作即可采集，Ctrl+C 停止落盘。")
    last_status = 0.0
    last_scan = 0.0
    last_keepalive = 0.0
    keepalive_every = 2.0          # 每 2 秒对所有 tab 发一次 Network.enable() 心跳，快速恢复导航后死掉的 Listener
    last_captures_at_status = 0          # 上次状态打印时的捕获数
    last_error_snapshot: list[str] = []  # 上次状态打印时的错误快照

    while True:
        # 检查外部停止信号（文件信号模式）
        if stop_file is not None and stop_file.is_file():
            sink("[jd_semiauto] 收到停止信号，退出监听。")
            break

        if restart_file is not None and restart_file.is_file():
            try:
                restart_file.unlink()
            except OSError:
                pass
            sink("[jd_semiauto] 收到重启监听信号，正在重新挂载 Network 监听（浏览器保持打开）…")
            try:
                n_rem = tap.remount_network_listeners(sb)
                if n_rem:
                    sink(
                        f"[jd_semiauto] 重启后已补挂 {n_rem} 个标签的监听"
                        f"（共 {len(tap._tab_sessions)} 路）。"
                    )
                else:
                    sink(
                        f"[jd_semiauto] 重启后主标签已挂好"
                        f"（当前 {len(tap._tab_sessions)} 路会话）。"
                    )
            except BaseException as e:
                tap._note_error(f"重启监听失败: {e!s}")
                sink(f"[jd_semiauto] 重启监听失败: {e!s}")

        now = time.monotonic()

        # 快速路径：每轮消费 Target.targetCreated 事件队列（无 HTTP 开销）
        # HTTP 兜底：事件订阅正常时每 scan_interval 秒一次；降级时每轮都轮询（≈p 秒）
        effective_scan_interval = p if not tap._target_listener_active else scan_interval
        force_http = (now - last_scan) >= effective_scan_interval
        if force_http:
            last_scan = now
        n_new = tap.scan_new_tabs(force_http=force_http)
        if n_new:
            method = "事件" if tap._target_listener_active else "轮询"
            sink(
                f"[jd_semiauto] 新标签 +{n_new}（{method}感知）已挂载监听"
                f"（共 {len(tap._tab_sessions)} 个标签）。"
            )

        if force_http and bool(
            getattr(_cfg, "SEMI_JD_PRUNE_STALE_TABS_WITH_JSON_LIST", True)
        ):
            pruned = tap.prune_stale_tab_sessions()
            if pruned:
                sink(
                    f"[jd_semiauto] DevTools 对账：移除 {pruned} 个已关闭页的监听会话，"
                    f"剩余 {len(tap._tab_sessions)}。"
                )

        # 每轮都调 finalize_all；_run() 内部 if not pending: return 保证无 pending 时快速返回
        try:
            tap.finalize_all(tout, per_send, stop_file=stop_file)
        except BaseException as e:
            tap._note_error(f"finalize(loop): {e!s}")

        # 定期心跳：通过 patched_send 路径重新发送 Network.enable()，保证 Listener 持续活跃
        if (now - last_keepalive) >= keepalive_every:
            last_keepalive = now
            for tid, conn, sess in tap._tab_sessions:
                if tid and tid in tap._destroyed_target_ids:
                    continue
                try:
                    keepalive_tab(conn, tap._loop)
                except BaseException:
                    pass

        if save_sink:
            save_sink(tap.captures)

        now = time.monotonic()
        if status_every_sec > 0 and (now - last_status) >= status_every_sec:
            last_status = now
            n = len(tap.captures)
            n_tabs = len(tap._tab_sessions)
            tab_info = f"（{n_tabs} 个标签）" if n_tabs > 1 else ""
            n_pending = sum(
                len(getattr(sess, "pending_by_request", None) or {})
                for tid, _, sess in tap._tab_sessions
                if not (tid and tid in tap._destroyed_target_ids)
            )
            pend_info = f"，pending={n_pending}" if n_pending else ""
            sink(f"[jd_semiauto] 监听中{tab_info}… 已累积 {n} 条{pend_info}；Ctrl+C 停止落盘，pending={n_pending}。")
            # 只在错误列表自上次打印后有变化时才打印，避免相同错误反复刷屏
            cur_errors = tap.last_errors[-2:]
            if cur_errors != last_error_snapshot:
                last_error_snapshot = cur_errors
                if cur_errors:
                    sink(f"[jd_semiauto] 近期提示: {'; '.join(cur_errors)}")
            last_captures_at_status = n

        sb.sleep(p)
