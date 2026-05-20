# -*- coding: utf-8 -*-
"""
淘宝示例：**``get_sb``** → CDP 落地 → 搜索 → **多页可选**监听 mtop 推荐 JSON。

无双引号 CLI；改 ``TB_*`` 与 ``constants_tb.TB_PAGER_*``（翻页）后直接运行::

  python backend/crawler_copy/sb_browser/platforms/taobao/run_search_demo.py

**勿**与 ``tb_pc_search`` 的 Playwright ``pw_user_data`` 共用同一目录；与小红书 ``platforms/xiaohongshu`` 脚本并列。
"""
from __future__ import annotations

import random
import sys
from pathlib import Path
from typing import Any


def _tail_wait_seconds(spec: Any) -> float:
    """结束流程前逗留：二元组 ``random.uniform``；正标量在中心附近抖动；≤0 / None 不等待。"""
    if spec is None:
        return 0.0
    if isinstance(spec, (tuple, list)) and len(spec) >= 2:
        a, b = float(spec[0]), float(spec[1])
        lo, hi = min(a, b), max(a, b)
        return random.uniform(lo, hi) if hi > 0 else 0.0
    try:
        c = float(spec)
    except (TypeError, ValueError):
        return 0.0
    if c <= 0:
        return 0.0
    return random.uniform(c * 0.86, c * 1.16)


def _bootstrap_crawler_copy() -> Path:
    """``taobao`` → ``platforms`` → ``sb_browser`` → ``crawler_copy``。"""
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
from sb_browser.platforms.taobao import flows


TB_USER_DATA_DIR = ""
TB_HEADLESS = False
TB_KEYWORD = "方便面"

TB_LANDING_URL = ""

TB_TAIL_WAIT_S = 60  # 或 ``(40, 95)``；正数标量则每次在约 ±14% 内随机
TB_SAVE_MTOP_JSON = True
TB_SAVE_MTOP_DIR = ""
# True：只写「主样式包」（``parsed.data`` 含 constants_tb.TB_MTOP_SAVE_REQUIRE_DATA_KEYS）；False：全部命中均落盘
TB_SAVE_MTOP_MAIN_ONLY = True
# True：在 ``TB_SAVE_MTOP_DIR``（或默认目录）下再建一层时间子文件夹，与历次运行分开
TB_SAVE_MTOP_RUN_TIME_SUBDIR = True
# True/False：显式开启或关闭落盘后 CSV；None：沿用 ``constants_tb.TB_MTOP_EXPORT_CSV_AFTER_JSON``
TB_SAVE_MTOP_EXPORT_CSV = None
TB_PRINT_MTOP = True

TB_PAGER_MAX_PAGES = None  # None：沿用 ``constants_tb.TB_PAGER_MAX_PAGES``；≥2 时每页后继续点「下一页」并监听
TB_PAGER_AFTER_CLICK_SLEEP = None  # None：用 constants_tb TB_SLEEP_PAGER_AFTER_NEXT_CLICK；或为 ``(min_s, max_s)``；单值则按比例抖动


def main() -> int:
    configure_stdio_utf8()
    if _SB is None:
        print("未安装 SeleniumBase。请运行: pip install seleniumbase", file=sys.stderr)
        return 2

    landing = (TB_LANDING_URL or "").strip() or None

    with get_sb(
        user_data_dir_arg=(TB_USER_DATA_DIR or "").strip(),
        headless=bool(TB_HEADLESS),
    ) as sb:
        d = (TB_SAVE_MTOP_DIR or "").strip()
        tap = flows.explore_then_search_and_listen(
            sb,
            TB_KEYWORD.strip(),
            landing_url=landing,
            save_mtop_json=bool(TB_SAVE_MTOP_JSON),
            mtop_save_dir=d or None,
            mtop_save_only_main_bundle=bool(TB_SAVE_MTOP_MAIN_ONLY),
            mtop_save_run_dir_by_time=bool(TB_SAVE_MTOP_RUN_TIME_SUBDIR),
            mtop_export_csv_after_save=TB_SAVE_MTOP_EXPORT_CSV,
            pager_max_pages=TB_PAGER_MAX_PAGES,
            pager_after_click_sleep=TB_PAGER_AFTER_CLICK_SLEEP,
        )
        if TB_PRINT_MTOP:
            if tap.captures:
                latest = tap.latest
                err = (latest.parse_error or "") if latest else ""
                print(
                    f"[taobao] mtop 响应命中 {len(tap.captures)} 条"
                    + (f"；最近一条 JSON 解析: {err}" if err else ""),
                    file=sys.stderr,
                )
            else:
                err_tail = "; ".join(tap.last_errors[-3:]) if tap.last_errors else ""
                print(
                    "[taobao] 未拦截到 wirelessrecommend mtop（检查登录、选择器与页面是否发该请求）",
                    file=sys.stderr,
                )
                if err_tail:
                    print(f"[taobao] CDP 诊断（最近）: {err_tail}", file=sys.stderr)
        tw = _tail_wait_seconds(TB_TAIL_WAIT_S)
        if tw > 0:
            print(
                f"[taobao] 关闭前随机等待约 {tw:.1f}s（改 TB_TAIL_WAIT_S）。",
                file=sys.stderr,
            )
            sb.sleep(tw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
