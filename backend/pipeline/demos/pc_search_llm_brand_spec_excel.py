# -*- coding: utf-8 -*-
"""
从 ``pc_search_export.csv``（京东/淘宝等与站内「搜索导出」同构的 CSV）调用大模型抽取**品牌**、**规格摘要**、**叶子类目标签**，并导出 Excel（含源数据表 + 品牌/规格/**分类** 三张统计表；与站内「导出整理表」同源逻辑）。

实现位于 ``pipeline.pc_search_llm_excel``；本文件仅提供命令行入口。

用法（在 ``backend`` 下）::

  python -m pipeline.demos.pc_search_llm_brand_spec_excel -i path/pc_search_export.csv -o out.xlsx
  python -m pipeline.demos.pc_search_llm_brand_spec_excel -i ... -o ... --batch-size 60 --workers 6
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

from pipeline.pc_search_llm_excel import build_from_csv_path_to_path  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="pc_search_export CSV → LLM 品牌/规格/分类 → Excel")
    ap.add_argument("--input", "-i", type=Path, required=True, help="pc_search_export.csv 路径")
    ap.add_argument("--output", "-o", type=Path, required=True, help="输出 .xlsx 路径")
    ap.add_argument("--batch-size", type=int, default=40, help="每批行数（默认 40，上限 120）")
    ap.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行批次数（0=环境变量 PC_SEARCH_LLM_MAX_WORKERS 或默认 8；1=串行）",
    )
    ap.add_argument("--limit", type=int, default=0, help="仅处理前 N 行（0=全部）")
    args = ap.parse_args()
    in_path = args.input.resolve()
    out_path = args.output.resolve()
    if not in_path.is_file():
        print(f"找不到输入文件: {in_path}", file=sys.stderr)
        return 1
    try:
        n = build_from_csv_path_to_path(
            in_path,
            out_path,
            batch_size=args.batch_size,
            max_workers=(args.workers if args.workers > 0 else None),
            limit=args.limit,
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1
    print(f"已写入: {out_path}（{n} 行）", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
