"""
在已有流水线目录上重生成报告，**将产出的 Markdown 以测试文件名落盘**，不长期覆盖 ``competitor_analysis.md``。

- 若目录下已有 ``competitor_analysis.md``：先复制为 ``competitor_analysis__pre_test_<时间戳>.md``，再重生成，再把**新生成**内容复制为 ``competitor_analysis__fulltest_<模型简写>_<时间戳>.md``（简写来自当前环境 ``OPENAI_TEXT_MODEL``，若未设则回退 ``OPENAI_VISION_MODEL``），最后把**原内容**写回 ``competitor_analysis.md``。
- 若原本没有主报告：重生成后复制一份为 ``...__fulltest_...``，主文件即新生成结果。

对比两种纯文本模型时，可在**启动进程前**在 shell 中设置 ``OPENAI_TEXT_MODEL=...``（优先于 .env 同项），全量重跑两次，用文件名中的模型段区分；stderr 会打印 ``elapsed_sec``、所用模型名。

用法（在 backend 下，已配置 Django 与 .env）::

    python -m pipeline.demos.test_regen_report_test_filename

可选环境变量：``MA_TEST_RUN_DIR`` = 绝对路径，默认自动选 ``data/JD/pipeline_runs`` 下第一个含 ``keyword_pipeline_merged.csv`` 的目录。

默认 **全量** 报告（``report_config=None``，与线上一致合并默认开关）。快测（只开矩阵 LLM 等）设环境变量 ``MA_TEST_REPORT_FAST=1``。
"""
from __future__ import annotations

import os
import re
import shutil
import sys
import time
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


def _model_label_for_env() -> str:
    return (
        (os.environ.get("OPENAI_TEXT_MODEL") or os.environ.get("OPENAI_VISION_MODEL") or "default")
        .strip()
    )


def _model_slug_for_filename() -> str:
    raw = _model_label_for_env()
    s = raw.replace("/", "__").replace("\\", "_")
    s = re.sub(r'[<>:"|?*]+', "_", s)
    s = re.sub(r"[^\w.\-]+", "_", s).strip("._-") or "model"
    return s[:100]


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
    if (os.environ.get("MA_TEST_REPORT_FAST") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
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
    else:
        report_config = None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _model_slug_for_filename()
    mlabel = _model_label_for_env()
    main = run_dir / "competitor_analysis.md"
    pre_bak = run_dir / f"competitor_analysis__pre_test_{ts}.md"
    out_test = run_dir / f"competitor_analysis__fulltest_{slug}_{ts}.md"
    had_main = main.is_file()
    if had_main:
        shutil.copy2(main, pre_bak)
    print("run_dir:", run_dir, file=sys.stderr)
    print("keyword:", kw, file=sys.stderr)
    print("OPENAI_TEXT_MODEL (effective for text):", mlabel, file=sys.stderr)
    print(
        "regenerating (full config, may take long; set MA_TEST_REPORT_FAST=1 for quick) ...",
        file=sys.stderr,
    )
    t0 = time.perf_counter()
    regenerate_competitor_report(str(run_dir), kw, report_config=report_config)
    elapsed = time.perf_counter() - t0
    print("elapsed_sec:", round(elapsed, 2), file=sys.stderr)
    if not main.is_file():
        print("未生成 competitor_analysis.md", file=sys.stderr)
        return 2
    shutil.copy2(main, out_test)
    if had_main and pre_bak.is_file():
        shutil.copy2(pre_bak, main)
    print("test_output:", out_test)
    print("elapsed_sec:", round(elapsed, 2))
    print("model:", mlabel)
    if had_main:
        print("restored:", main, "from", pre_bak, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
