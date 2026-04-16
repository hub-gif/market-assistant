"""从 ``detail_ware_export.csv`` / ``detail/ware_*_response.json`` 补全并规范化 lean ``keyword_pipeline_merged.csv``（列序与 ``csv_schema.MERGED_CSV_COLUMNS`` 一致）。"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

from ..csv_schema import (
    MERGED_CSV_COLUMNS,
    MERGED_FIELD_TO_CSV_HEADER,
    MERGED_LEAN_DETAIL_INTERNAL_KEYS,
    merged_csv_effective_total_sales,
    strip_buyer_ranking_line_prefix,
)
from ..ingest import FILE_DETAIL_WARE_CSV, FILE_MERGED_CSV

HOT_KEY = "榜单类文案"


def _ensure_crawler_detail_path() -> None:
    root = Path(__file__).resolve().parents[2] / "crawler_copy" / "jd_pc_search"
    for sub in ("detail", ""):
        p = root / sub if sub else root
        s = str(p.resolve())
        if s not in sys.path:
            sys.path.insert(0, s)


def write_keyword_pipeline_merged_lean_csv(run_dir: Path) -> tuple[int, Path]:
    """
    读取已有 ``keyword_pipeline_merged.csv``（可缺列），按 lean 宽表列序重写：
    - ``销量展示`` 列与入库一致（``merged_csv_effective_total_sales``）
    - 商详块列优先与 ``detail_ware_export.csv`` 对齐；缺则尝试 ``detail/ware_{sku}_response.json``
    - 「榜单类文案」与「榜单排名」去掉 ``榜单/曝光：`` 前缀
    """
    _ensure_crawler_detail_path()
    from jd_detail_buyer_extraction import (  # noqa: WPS433
        buyer_promo_text_from_profile,
        buyer_ranking_line_from_profile,
        extract_buyer_offer_profile_from_json_text,
    )

    run_dir = run_dir.expanduser().resolve()
    merged_path = run_dir / FILE_MERGED_CSV
    detail_path = run_dir / FILE_DETAIL_WARE_CSV
    detail_dir = run_dir / "detail"

    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表: {merged_path}")
    if not detail_dir.is_dir():
        raise FileNotFoundError(f"缺少 detail 目录: {detail_dir}")

    with merged_path.open(encoding="utf-8-sig", newline="") as f:
        old_rows = list(csv.DictReader(f))

    detail_by_sku: dict[str, dict[str, str]] = {}
    if detail_path.is_file():
        with detail_path.open(encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                sku = (r.get("SKU") or r.get("skuId") or "").strip()
                if sku:
                    detail_by_sku[sku] = {k: str(r.get(k) or "").strip() for k in r}

    h_ts = MERGED_FIELD_TO_CSV_HEADER["total_sales"]
    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    br_h = MERGED_FIELD_TO_CSV_HEADER["buyer_ranking_line"]
    pr_h = MERGED_FIELD_TO_CSV_HEADER["buyer_promo_text"]
    rows_out: list[dict[str, str]] = []

    for row in old_rows:
        out = {col: str(row.get(col) or "").strip() for col in MERGED_CSV_COLUMNS}
        out[h_ts] = merged_csv_effective_total_sales(out)

        if out.get(HOT_KEY):
            out[HOT_KEY] = strip_buyer_ranking_line_prefix(out[HOT_KEY])

        sku = (out.get(sku_h) or "").strip()
        if sku and sku in detail_by_sku:
            d = detail_by_sku[sku]
            for ik in MERGED_LEAN_DETAIL_INTERNAL_KEYS:
                ch = MERGED_FIELD_TO_CSV_HEADER[ik]
                v = (d.get(ch) or d.get(ik) or "").strip()
                if v:
                    out[ch] = v
        elif sku:
            jp = detail_dir / f"ware_{sku}_response.json"
            if jp.is_file():
                text = jp.read_text(encoding="utf-8").strip()
                if text:
                    prof = extract_buyer_offer_profile_from_json_text(text)
                    out[br_h] = buyer_ranking_line_from_profile(prof)
                    out[pr_h] = buyer_promo_text_from_profile(prof)

        out[br_h] = strip_buyer_ranking_line_prefix(out.get(br_h) or "")
        rows_out.append(out)

    with merged_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=list(MERGED_CSV_COLUMNS),
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(rows_out)

    return len(rows_out), merged_path
