"""
细类价盘要点归纳：打印 ``generate_price_group_summaries_llm`` 输出（与报告 §6 后大模型段同源）。

  cd backend
  .venv\\Scripts\\python.exe -m pipeline.demos.run_price_groups_llm_demo --job 12 --live
  .venv\\Scripts\\python.exe -m pipeline.demos.run_price_groups_llm_demo --merged "D:/path/keyword_pipeline_merged.csv" --live
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

JCR_ROOT = BACKEND_ROOT / "crawler_copy" / "jd_pc_search"
if str(JCR_ROOT) not in sys.path:
    sys.path.insert(0, str(JCR_ROOT))

import jd_competitor_report as jcr  # noqa: E402
import jd_keyword_pipeline as kpl  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="价盘细类归纳 LLM demo")
    parser.add_argument("--job", type=int, default=None, help="PipelineJob 主键，读 run_dir 下合并表")
    parser.add_argument(
        "--merged",
        type=str,
        default="",
        help="keyword_pipeline_merged.csv 绝对或相对路径",
    )
    parser.add_argument("--keyword", type=str, default="")
    parser.add_argument(
        "--live",
        action="store_true",
        help="调用真实大模型；否则只打印 payload 前两条摘要",
    )
    parser.add_argument(
        "--max-groups",
        type=int,
        default=0,
        help="仅送前 N 个细类给模型（0 表示全部，大任务可设 5 试跑）",
    )
    args = parser.parse_args()

    merged_rows: list[dict[str, str]] = []
    keyword = (args.keyword or "").strip()

    if args.merged:
        mp = Path(args.merged).expanduser().resolve()
        if not mp.is_file():
            print(f"合并表不存在: {mp}", file=sys.stderr)
            sys.exit(1)
        _, merged_rows = jcr._read_csv_rows(mp)
    elif args.job is not None:
        from pipeline.models import PipelineJob  # noqa: WPS433

        job = PipelineJob.objects.filter(pk=args.job).first()
        if not job:
            print(f"无此任务: {args.job}", file=sys.stderr)
            sys.exit(1)
        rd = (job.run_dir or "").strip()
        if not rd:
            print("任务无 run_dir", file=sys.stderr)
            sys.exit(1)
        run_dir = Path(rd).expanduser().resolve()
        mp = run_dir / kpl.FILE_MERGED_CSV
        if not mp.is_file():
            print(f"缺少合并表: {mp}", file=sys.stderr)
            sys.exit(1)
        _, merged_rows = jcr._read_csv_rows(mp)
        if not keyword and (job.keyword or "").strip():
            keyword = str(job.keyword).strip()
    else:
        print("请指定 --job <id> 或 --merged <csv路径>", file=sys.stderr)
        sys.exit(1)

    if not keyword:
        keyword = "竞品监测"

    sku_h = "SKU(skuId)"
    title_h = "标题(wareName)"
    groups = jcr.build_price_groups_llm_payload(
        merged_rows, title_h=title_h, sku_header=sku_h
    )
    print(f"# payload: {len(groups)} 个细类, keyword={keyword}", file=sys.stderr)
    if not groups:
        print("build_price_groups_llm_payload 为空（合并表无行？）", file=sys.stderr)
        sys.exit(1)

    if args.max_groups and args.max_groups > 0:
        groups = groups[: args.max_groups]
        print(f"# 截断为前 {len(groups)} 个细类", file=sys.stderr)

    if not args.live:
        preview = json.dumps(groups[:2], ensure_ascii=False, indent=2)
        print(preview[:6000])
        if len(preview) > 6000:
            print("\n…")
        print("\n加 --live 调用 generate_price_group_summaries_llm", file=sys.stderr)
        return

    from pipeline.llm.generate import generate_price_group_summaries_llm  # noqa: WPS433

    out = generate_price_group_summaries_llm(groups, keyword=keyword)
    print(out)


if __name__ == "__main__":
    main()
