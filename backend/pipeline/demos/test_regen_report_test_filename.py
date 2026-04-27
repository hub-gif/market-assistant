"""
在已有流水线目录上重生成报告，**将产出的 Markdown 以测试文件名落盘**，不长期覆盖 ``competitor_analysis.md``。

- 若目录下已有 ``competitor_analysis.md``：先复制为 ``competitor_analysis__pre_test_<时间戳>.md``，再重生成，再把**新生成**内容复制为 ``competitor_analysis__llmtest_<时间戳>.md``，最后把**原内容**写回 ``competitor_analysis.md``。
- 若原本没有主报告：重生成后复制一份为 ``...__llmtest_...``，主文件即新生成结果。

用法（在 backend 下，已配置 Django 与 .env）::

    python -m pipeline.demos.test_regen_report_test_filename

可选环境变量：``MA_TEST_RUN_DIR`` = 绝对路径，默认自动选 ``data/JD/pipeline_runs`` 下第一个含 ``keyword_pipeline_merged.csv`` 的目录。

默认 ``report_config`` 为**快测**（只开 ``llm_matrix_group_summaries``，其余大模型段关；可按需改脚本）。
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# backend 在 sys.path
_BACK = Path(__file__).resolve().parent.parent.parent
if str(_BACK) not in sys.path:
    sys.path.insert(0, str(_BACK))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django  # noqa: E402

django.setup()

from pipeline.jd.runner import regenerate_competitor_report  # noqa: E402


def _default_run_dir() -> Path:
    custom = (os.environ.get("MA_TEST_RUN_DIR") or "").strip()
    if custom:
        return Path(custom).expanduser().resolve()
    from django.conf import settings

    low = Path((settings.LOW_GI_PROJECT_ROOT or "").strip() or _BACK.parent)
    pr = low / "data" / "JD" / "pipeline_runs"
    if not pr.is_dir():
        raise FileNotFoundError(f"无 pipeline_runs: {pr}")
    for d in sorted(pr.iterdir()):
        if d.is_dir() and (d / "keyword_pipeline_merged.csv").is_file():
            return d.resolve()
    raise FileNotFoundError(f"{pr} 下未找到含 keyword_pipeline_merged.csv 的目录")


def _keyword_from_run_dir(run_dir: Path) -> str:
    meta = run_dir / "run_meta.json"
    if not meta.is_file():
        return ""
    import json

    try:
        return str((json.loads(meta.read_text(encoding="utf-8")) or {}).get("keyword") or "").strip()
    except json.JSONDecodeError:
        return ""


def main() -> int:
    run_dir = _default_run_dir()
    kw = _keyword_from_run_dir(run_dir)
    if not kw:
        print("无法从 run_meta 读取 keyword，请设置环境变量或检查 run_dir", file=sys.stderr)
        return 1
    # 快测：只跑矩阵大模型段；全量可改为 None 走默认
    report_config = {
        "llm_comment_sentiment": False,
        "llm_matrix_group_summaries": True,
        "llm_comment_group_summaries": False,
        "llm_price_group_summaries": False,
        "llm_promo_group_summaries": False,
        "llm_strategy_opportunities": False,
        "chapter8_text_mining_probe": False,
        "chapter8_text_mining_probe_live_llm": False,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    main = run_dir / "competitor_analysis.md"
    pre_bak = run_dir / f"competitor_analysis__pre_test_{ts}.md"
    out_test = run_dir / f"competitor_analysis__llmtest_{ts}.md"
    had_main = main.is_file()
    if had_main:
        shutil.copy2(main, pre_bak)
    print("run_dir:", run_dir, file=sys.stderr)
    print("keyword:", kw, file=sys.stderr)
    print("regenerating (may take a few minutes)...", file=sys.stderr)
    regenerate_competitor_report(str(run_dir), kw, report_config=report_config)
    if not main.is_file():
        print("未生成 competitor_analysis.md", file=sys.stderr)
        return 2
    shutil.copy2(main, out_test)
    if had_main and pre_bak.is_file():
        shutil.copy2(pre_bak, main)
    print("test_output:", out_test)
    if had_main:
        print("restored:", main, "from", pre_bak, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
