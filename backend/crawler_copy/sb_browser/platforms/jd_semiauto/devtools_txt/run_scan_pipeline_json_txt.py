# -*- coding: utf-8 -*-
"""扫描 pipeline 目录下 DevTools 文本片段，结构化 list/detail/comment/graphic + SKU（不写响应正文）。

运行：``python -m sb_browser.platforms.jd_semiauto.devtools_txt.run_scan_pipeline_json_txt``（``cwd`` = ``crawler_copy``）。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[4]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


_bootstrap()

from sb_browser.platforms.jd_semiauto.common.low_gi_root import low_gi_project_root
from sb_browser.platforms.jd_semiauto.devtools_txt.devtools_cn_export_parse import (
    DEFAULT_RELATIVE_PIPELINE_JSON_DIR,
    scan_devtools_txt_paths,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="扫描京东 DevTools 导出 txt，生成关联 manifest。")
    ap.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="含 *.txt 的目录；默认 <项目根>/data/JD/pipeline_runs/json",
    )
    ap.add_argument(
        "-o",
        "--out",
        type=Path,
        default=None,
        help="写入 manifest.json；默认与 --dir 相同",
    )
    args = ap.parse_args()

    base = low_gi_project_root()
    d = args.dir
    if d is None:
        d = (base / DEFAULT_RELATIVE_PIPELINE_JSON_DIR).resolve()
    else:
        d = d.expanduser().resolve()

    if not d.is_dir():
        print(f"目录不存在: {d}", file=sys.stderr)
        return 2

    txts = sorted(d.glob("*.txt"))
    if not txts:
        print(f"未找到 {d} 下的 .txt", file=sys.stderr)
        return 1

    rows = scan_devtools_txt_paths(txts)
    manifest = {
        "schema_version": 1,
        "source_dir": str(d),
        "entries": rows,
    }
    out = args.out
    if out is None:
        out = d / "jd_devtools_txt_manifest.json"
    else:
        out = out.expanduser().resolve()

    out.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"已写 {out}（{len(rows)} 条）", file=sys.stderr)
    for r in rows:
        print(
            f"  [{r['capture_kind']}] sku={r['resolved_sku'] or '—'} "
            f"fid={r['function_id'] or '—'} ← {r['filename']}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
