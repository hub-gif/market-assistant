# -*- coding: utf-8 -*-
"""从 pc_search dump .js 中按 wareList 顺序导出 sku_id、shortName（每文件一屏槽位）。"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

# 同目录导入
sys.path.insert(0, str(Path(__file__).resolve().parent))
from jd_h5_search_requests import (  # noqa: E402
    JD_SKU_KEYS,
    _jd_flatten_ware,
    _sval_jd,
)


def _short_name(d: dict) -> str:
    d0 = _jd_flatten_ware(d)
    sn = d0.get("shortName")
    if sn is None:
        return ""
    return str(sn).strip()


def _sku_id(d: dict) -> str:
    return _sval_jd(_jd_flatten_ware(d), JD_SKU_KEYS).strip()


def main() -> None:
    p = argparse.ArgumentParser(description="wareList 槽位 → sku_id + shortName CSV")
    p.add_argument(
        "js_files",
        nargs="+",
        type=Path,
        help="按顺序的 dump .js（JSON 一行）",
    )
    p.add_argument(
        "-o",
        "--out",
        type=Path,
        required=True,
        help="输出 CSV 路径",
    )
    args = p.parse_args()

    rows: list[dict[str, str]] = []
    seq = 0
    for fi, path in enumerate(args.js_files, start=1):
        text = path.read_text(encoding="utf-8")
        payload = json.loads(text)
        wl = (payload.get("data") or {}).get("wareList")
        if not isinstance(wl, list):
            raise SystemExit(f"{path}: 无 data.wareList")
        for slot, w in enumerate(wl):
            seq += 1
            if not isinstance(w, dict):
                rows.append(
                    {
                        "seq": str(seq),
                        "sku_id": "",
                        "shortName": "",
                    }
                )
                continue
            rows.append(
                {
                    "seq": str(seq),
                    "sku_id": _sku_id(w),
                    "shortName": _short_name(w),
                }
            )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8-sig", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=["seq", "sku_id", "shortName"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
