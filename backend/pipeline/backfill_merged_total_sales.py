"""
不重新抓搜索：从已有 ``pc_search_raw/*.json``（及 ``.js``）解析 ``totalSales``，
并写回 ``keyword_pipeline_merged.csv`` /可选 ``pc_search_export.csv``。
若原始 JSON 无该字段，则尝试从「销量楼层(commentSalesFloor)」单元格中抽取「已售…」片段。

用法（在 backend 目录下）::

  python pipeline/backfill_merged_total_sales.py --run-dir "../data/JD/pipeline_runs/某批次"
  python pipeline/backfill_merged_total_sales.py --merged "D:/path/keyword_pipeline_merged.csv" --dry-run

"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from pipeline.csv_schema import (  # noqa: E402
    JD_SEARCH_CSV_HEADERS,
    MERGED_FIELD_TO_CSV_HEADER,
)

COL_MERGED_TOTAL = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
COL_MERGED_FLOOR = MERGED_FIELD_TO_CSV_HEADER["comment_sales_floor"]
COL_SKU_MERGED = MERGED_FIELD_TO_CSV_HEADER["sku_id"]

COL_EXPORT_TOTAL = JD_SEARCH_CSV_HEADERS["total_sales"]
COL_EXPORT_FLOOR = JD_SEARCH_CSV_HEADERS["comment_sales_floor"]
COL_SKU_EXPORT = JD_SEARCH_CSV_HEADERS["sku_id"]

# 仅列表响应文件，避免误扫 ``pc_request_*.json`` 请求元数据。
_RAW_GLOBS = ("pc_search_*.json", "pc_search_*.js")


def infer_total_sales_from_sales_floor(cell: str) -> str:
    """
    从「销量楼层」合并列文案中截取可作销量口径的片段（供图表解析件数）。
    例：``good:99%好评 | 已售50万+`` → ``已售50万+``。
    """
    t = (cell or "").strip()
    if not t:
        return ""
    m = re.search(r"已售\s*[\d,，.+]*\s*[万亿]?\s*\+?", t)
    if m:
        return m.group(0).strip()
    m2 = re.search(r"已售\s*[\d,，.+\s万千亿]+", t)
    return m2.group(0).strip() if m2 else ""


def _load_json_payload(path: Path) -> Any | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    jcr_root = BACKEND_ROOT / "crawler_copy" / "jd_pc_search"
    if str(jcr_root) not in sys.path:
        sys.path.insert(0, str(jcr_root))
    try:
        from search.jd_h5_search_requests import _loads_json_or_jsonp  # noqa: WPS433

        return _loads_json_or_jsonp(text)
    except Exception:
        return None


def collect_total_sales_from_pc_search_raw(raw_dir: Path) -> dict[str, str]:
    """
    遍历 ``pc_search_raw``下保存的列表响应，按 SKU 汇总 ``total_sales``（后者覆盖前者）。
    """
    if str(BACKEND_ROOT / "crawler_copy" / "jd_pc_search") not in sys.path:
        sys.path.insert(0, str(BACKEND_ROOT / "crawler_copy" / "jd_pc_search"))
    from search.jd_h5_search_requests import parse_items_from_jd_json_payload  # noqa: WPS433

    out: dict[str, str] = {}
    seen_paths: set[Path] = set()
    for pattern in _RAW_GLOBS:
        for p in sorted(raw_dir.glob(pattern)):
            if p in seen_paths:
                continue
            seen_paths.add(p)
            payload = _load_json_payload(p)
            if payload is None:
                continue
            try:
                rows = parse_items_from_jd_json_payload(
                    payload, keyword="", page=1
                )
            except Exception:
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                sku = str(r.get("sku_id") or "").strip()
                ts = str(r.get("total_sales") or "").strip()
                if sku and ts:
                    out[sku] = ts
    return out


def _insert_column_after(fieldnames: list[str], col: str, after: str) -> list[str]:
    fn = list(fieldnames)
    if col in fn:
        return fn
    if after in fn:
        i = fn.index(after) + 1
        fn.insert(i, col)
        return fn
    # 极旧表：无销量楼层时插在评价量后
    fallback_after = MERGED_FIELD_TO_CSV_HEADER["comment_fuzzy"]
    if fallback_after in fn:
        fn.insert(fn.index(fallback_after) + 1, col)
        return fn
    fn.append(col)
    return fn


def _backfill_rows(
    rows: list[dict[str, str]],
    sku_col: str,
    total_col: str,
    floor_col: str,
    sku_to_total: dict[str, str],
    *,
    use_floor_fallback: bool,
) -> int:
    n = 0
    for row in rows:
        cur = str(row.get(total_col) or "").strip()
        sku = str(row.get(sku_col) or "").strip()
        if not cur and sku:
            ts = sku_to_total.get(sku, "")
            if ts:
                row[total_col] = ts
                cur = ts
                n += 1
        if not cur and use_floor_fallback:
            floor = str(row.get(floor_col) or "").strip()
            inf = infer_total_sales_from_sales_floor(floor)
            if inf:
                row[total_col] = inf
                n += 1
    return n


def backfill_csv_file(
    path: Path,
    *,
    sku_to_total: dict[str, str],
    is_merged: bool,
    use_floor_fallback: bool,
    dry_run: bool,
) -> tuple[int, list[str]]:
    raw = path.read_text(encoding="utf-8-sig")
    lines = raw.splitlines()
    if not lines:
        return 0, []
    reader = csv.DictReader(lines)
    old_fn = reader.fieldnames or []
    if is_merged:
        sku_c, tot_c, fl_c = (
            COL_SKU_MERGED,
            COL_MERGED_TOTAL,
            COL_MERGED_FLOOR,
        )
        fieldnames = _insert_column_after(list(old_fn), tot_c, COL_MERGED_FLOOR)
    else:
        sku_c, tot_c, fl_c = COL_SKU_EXPORT, COL_EXPORT_TOTAL, COL_EXPORT_FLOOR
        fieldnames = _insert_column_after(list(old_fn), tot_c, COL_EXPORT_FLOOR)
    rows = list(reader)
    for r in rows:
        for h in fieldnames:
            r.setdefault(h, "")
    filled = _backfill_rows(
        rows, sku_c, tot_c, fl_c, sku_to_total, use_floor_fallback=use_floor_fallback
    )
    if dry_run:
        return filled, fieldnames
    from io import StringIO

    sio = StringIO()
    w2 = csv.DictWriter(sio, fieldnames=fieldnames, lineterminator="\n")
    w2.writeheader()
    w2.writerows(rows)
    path.write_text("\ufeff" + sio.getvalue(), encoding="utf-8")
    return filled, fieldnames


def _resolve_raw_dir(
    run_dir: Path | None, merged_path: Path | None
) -> Path | None:
    if run_dir is not None:
        rd = (run_dir / "pc_search_raw").resolve()
        if rd.is_dir():
            return rd
    if merged_path is not None:
        rd = (merged_path.parent / "pc_search_raw").resolve()
        if rd.is_dir():
            return rd
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="从 pc_search_raw 补全销量口径列（不重新请求搜索）")
    ap.add_argument("--run-dir", type=Path, default=None, help="批次目录（含 keyword_pipeline_merged.csv 与 pc_search_raw）")
    ap.add_argument("--merged", type=Path, default=None, help="合并表路径（可单独指定）")
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="原始搜索响应目录（默认：run-dir 或 merged 父目录下的 pc_search_raw）",
    )
    ap.add_argument(
        "--also-pc-search-export",
        action="store_true",
        help="同时处理同目录下的 pc_search_export.csv",
    )
    ap.add_argument(
        "--no-floor-fallback",
        action="store_true",
        help="禁用从销量楼层文案推断（仅用原始 JSON 中的 totalSales）",
    )
    ap.add_argument("--dry-run", action="store_true", help="只统计将补全条数，不写文件")
    args = ap.parse_args()

    run_dir = args.run_dir.resolve() if args.run_dir else None
    merged_path = args.merged
    if merged_path is None and run_dir is not None:
        merged_path = run_dir / "keyword_pipeline_merged.csv"
    if merged_path is None or not merged_path.is_file():
        ap.error("请指定有效的 --merged 或含 keyword_pipeline_merged.csv 的 --run-dir")

    merged_path = merged_path.resolve()
    raw_dir = args.raw_dir.resolve() if args.raw_dir else _resolve_raw_dir(run_dir, merged_path)
    sku_map: dict[str, str] = {}
    if raw_dir is not None:
        sku_map = collect_total_sales_from_pc_search_raw(raw_dir)
        print(f"[backfill] 自 {raw_dir} 解析到带 totalSales 的 SKU：{len(sku_map)}", file=sys.stderr)
    else:
        print(
            "[backfill] 未找到 pc_search_raw，将仅尝试销量楼层推断（若未加 --no-floor-fallback）",
            file=sys.stderr,
        )

    use_floor = not args.no_floor_fallback
    n_m, _ = backfill_csv_file(
        merged_path,
        sku_to_total=sku_map,
        is_merged=True,
        use_floor_fallback=use_floor,
        dry_run=args.dry_run,
    )
    print(
        f"[backfill] merged：补全单元格数 {n_m}（空列→有值；dry_run={args.dry_run}）",
        file=sys.stderr,
    )

    if args.also_pc_search_export:
        exp = merged_path.parent / "pc_search_export.csv"
        if exp.is_file():
            n_e, _ = backfill_csv_file(
                exp,
                sku_to_total=sku_map,
                is_merged=False,
                use_floor_fallback=use_floor,
                dry_run=args.dry_run,
            )
            print(
                f"[backfill] pc_search_export：补全单元格数 {n_e}",
                file=sys.stderr,
            )
        else:
            print(f"[backfill] 跳过：无 {exp}", file=sys.stderr)


if __name__ == "__main__":
    main()
