# -*- coding: utf-8 -*-
"""
京东半自动：UC + CDP 监听 api.m.jd.com，Ctrl+C 或信号文件后按类型落盘。

  python backend/crawler_copy/sb_browser/platforms/jd_semiauto/run_listen_demo.py

手动模式：打开 jd.com → 等候登录（终端回车确认）→ 挂 API 监听 → Ctrl+C 停止 → 落盘。
前端集成模式：传入 --run-dir / --login-file / --stop-file，用文件信号代替终端交互。

监听常量见 constants_jd_semiauto.py；落盘目录默认 data/JD/sb_cdp_api_semiauto/<时间戳>/。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _bootstrap_crawler_copy() -> Path:
    root = Path(__file__).resolve().parents[3]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


_bootstrap_crawler_copy()

try:
    from seleniumbase import SB as _SB  # noqa: F401
except ImportError:
    _SB = None  # noqa: N816

from sb_browser.browsers.session import configure_stdio_utf8, get_sb
from sb_browser.platforms.jd_semiauto import flows as jd_flows
from sb_browser.platforms.jd_semiauto.notes_sink_jd import JdCaptureSink

# ── 浏览器 ────────────────────────────────────────────────────────────────────
# 留空：每次临时 Profile，关窗后不保留会话；填绝对路径：持久化登录态。
SEMI_USER_DATA_DIR = ""
SEMI_HEADLESS = False
SEMI_SB_TEST_MODE = False

# ── 落地页与采集标签 ──────────────────────────────────────────────────────────
SEMI_LANDING_URL = ""           # 留空使用 constants 里的 SEMI_JD_DEFAULT_LANDING_URL
SEMI_CAPTURE_LABEL = "manual"

# ── 落盘 ──────────────────────────────────────────────────────────────────────
SEMI_SAVE_JSON = True
SEMI_SAVE_DIR = ""              # 留空使用 data/JD/sb_cdp_api_semiauto/
SEMI_SAVE_RUN_TIME_SUBDIR = True

# ── 登录确认 ──────────────────────────────────────────────────────────────────
# True：CDP 就绪后在终端阻塞，登录后按回车再挂监听；False 直接进入监听。
SEMI_PAUSE_FOR_LOGIN_CONFIRM = True


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="京东半自动 CDP 监听")
    p.add_argument("--run-dir", default="", help="输出根目录（绝对路径）；空则沿用脚本默认")
    p.add_argument("--keyword", default="", help="采集标签，写入 JSON 文件名；空则用脚本 SEMI_CAPTURE_LABEL")
    p.add_argument(
        "--login-file",
        default="",
        help="登录确认信号文件路径；文件存在时视为已确认，替代终端回车",
    )
    p.add_argument(
        "--stop-file",
        default="",
        help="停止信号文件路径；文件存在时退出监听，替代 Ctrl+C",
    )
    p.add_argument(
        "--restart-file",
        default="",
        help="重启监听信号；文件出现后子进程会重新挂 Network 监听（不关浏览器），并删除该文件",
    )
    return p.parse_args()


def _write_status(run_dir: Path | None, name: str) -> None:
    """向 run_dir 写入状态文件，供后台线程轮询感知 phase。"""
    if run_dir is None:
        return
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / name).touch()
    except Exception:
        pass


def main() -> int:
    configure_stdio_utf8()
    if _SB is None:
        print("未安装 SeleniumBase。请运行: pip install seleniumbase", file=sys.stderr)
        return 2

    args = _parse_args()

    udata = (SEMI_USER_DATA_DIR or "").strip()
    print(
        f"[jd_semiauto] Profile：{'临时（每次新建）' if not udata else udata} | "
        "在脚本打开的窗口内正常浏览即可采集，Ctrl+C 或停止信号结束落盘。",
        file=sys.stderr, flush=True,
    )

    # 参数覆盖常量
    run_dir_arg = (args.run_dir or "").strip()
    run_dir: Path | None = Path(run_dir_arg) if run_dir_arg else None
    keyword = (args.keyword or SEMI_CAPTURE_LABEL or "manual").strip() or "manual"
    login_file: Path | None = Path(args.login_file) if (args.login_file or "").strip() else None
    stop_file: Path | None = Path(args.stop_file) if (args.stop_file or "").strip() else None
    restart_file: Path | None = Path(args.restart_file) if (args.restart_file or "").strip() else None

    landing = (SEMI_LANDING_URL or "").strip() or None

    # 通知后台线程：即将打开浏览器，等待登录
    _write_status(run_dir, ".status_waiting_login")

    # 增量落盘 sink：监听启动时即锁定时间戳，每次 finalize 后立即写盘
    save_dir = run_dir_arg or (SEMI_SAVE_DIR or "").strip() or None
    sink: JdCaptureSink | None = None
    if bool(SEMI_SAVE_JSON):
        sink = JdCaptureSink(
            keyword=keyword,
            raw_dir=save_dir,
            save_run_dir_by_time=not bool(run_dir_arg),
        )

    with get_sb(
        user_data_dir_arg=udata,
        headless=bool(SEMI_HEADLESS),
        sb_test=bool(SEMI_SB_TEST_MODE),
        persist_profile=False,
    ) as sb:
        jd_flows.open_landing_cdp(
            sb,
            landing_url=landing,
            pause_for_login_confirm=bool(SEMI_PAUSE_FOR_LOGIN_CONFIRM),
            login_file=login_file,
        )
        # 通知后台线程：登录完成，正在监听
        _write_status(run_dir, ".status_listening")
        tap = jd_flows.attach_jd_api_listener(sb)
        try:
            jd_flows.listen_until_stopped(
                sb, tap,
                stop_file=stop_file,
                restart_file=restart_file,
                save_sink=sink.flush if sink else None,
            )
        except KeyboardInterrupt:
            print("[jd_semiauto] Ctrl+C，落盘退出。", file=sys.stderr, flush=True)
        finally:
            if sink:
                # 尾部 flush：处理停止信号到 finally 之间可能还有未落盘的数据
                sink.flush(tap.captures)
                if sink.saved_paths:
                    print(f"[jd_semiauto] 落盘目录: {sink.run_rd}", file=sys.stderr, flush=True)
                else:
                    print("[jd_semiauto] 无捕获条目，跳过落盘。", file=sys.stderr, flush=True)
                if sink.skipped:
                    print(
                        f"[jd_semiauto] 跳过 {sink.skipped} 条无 wareList 的 list 响应。",
                        file=sys.stderr, flush=True,
                    )
            if tap.last_errors:
                tail = "; ".join(tap.last_errors[-3:])
                print(f"[jd_semiauto] CDP 提示（最近）: {tail}", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
