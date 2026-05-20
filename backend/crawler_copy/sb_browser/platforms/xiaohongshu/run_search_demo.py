# -*- coding: utf-8 -*-
"""
小红书示例：**``get_sb``** 建会话 → CDP 搜索。命中笔记搜索 API 时默认落盘到仓库根
``data/XHS/search_notes_raw/``（对齐 ``data/JD`` 与 ``data/TB``）。

无双引号 CLI；改 ``TB_*`` 后::

  python backend/crawler_copy/sb_browser/platforms/xiaohongshu/run_search_demo.py

仅开浏览器不调平台：``python backend/crawler_copy/sb_browser/browsers/session.py``。
"""
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap_crawler_copy() -> Path:
    """``xiaohongshu`` → ``platforms`` → ``sb_browser`` → ``crawler_copy``。"""
    root = Path(__file__).resolve().parents[3]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


_bootstrap_crawler_copy()

try:
    from seleniumbase import SB as _SB  # noqa: F401 — 存在性检查
except ImportError:
    _SB = None  # noqa: N816

from sb_browser.browsers.session import configure_stdio_utf8, get_sb
from sb_browser.platforms.xiaohongshu import flows


TB_USER_DATA_DIR = ""
TB_HEADLESS = False
TB_KEYWORD = "低GI"

TB_LANDING_URL = ""

TB_TAIL_WAIT_S = 60
# True：将命中条目写入 LOW_GI 项目根下 data/XHS/search_notes_raw/
TB_SAVE_SEARCH_NOTES_JSON = True
# 非空则覆盖目录（绝对或相对当前工作目录）；空则用 data/XHS/search_notes_raw
TB_SAVE_SEARCH_NOTES_DIR = ""
# 是否在 stderr 简要打印是否命中 ``/api/sns/web/v1/search/notes`` 及条数
TB_PRINT_SEARCH_NOTES = True


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
        d = (TB_SAVE_SEARCH_NOTES_DIR or "").strip()
        tap = flows.explore_then_search(
            sb,
            TB_KEYWORD.strip(),
            landing_url=landing,
            save_search_notes_json=bool(TB_SAVE_SEARCH_NOTES_JSON),
            search_notes_save_dir=d or None,
        )
        if TB_PRINT_SEARCH_NOTES:
            if tap.captures:
                latest = tap.latest
                err = (latest.parse_error or "") if latest else ""
                print(
                    f"[xiaohongshu] 搜索笔记 API 命中 {len(tap.captures)} 条"
                    + (f"；最近一条 JSON 解析: {err}" if err else ""),
                    file=sys.stderr,
                )
            else:
                err_tail = "; ".join(tap.last_errors[-3:]) if tap.last_errors else ""
                print(
                    "[xiaohongshu] 未拦截到 /api/sns/web/v1/search/notes "
                    "（检查登录、选择器与是否真的发出该 POST）",
                    file=sys.stderr,
                )
                if err_tail:
                    print(f"[xiaohongshu] CDP 诊断（最近）: {err_tail}", file=sys.stderr)
        tw = max(0.0, float(TB_TAIL_WAIT_S))
        if tw > 0:
            print(f"[xiaohongshu] 等待 {tw:.0f}s；改 TB_TAIL_WAIT_S。", file=sys.stderr)
            sb.sleep(tw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
