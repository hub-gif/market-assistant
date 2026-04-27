"""从 ``run_dir/detail/ware_*_response.json`` 与 ``keyword_pipeline_merged.csv`` 重写 ``detail_ware_export.csv``（lean 列集，不重新抓接口）。"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

from ..csv.schema import MERGED_FIELD_TO_CSV_HEADER
from ..ingest import FILE_DETAIL_WARE_CSV, FILE_MERGED_CSV, SKU_FIELD_MERGED


def _ensure_crawler_copy_path() -> None:
    root = Path(__file__).resolve().parents[2] / "crawler_copy" / "jd_pc_search"
    for sub in ("detail", ""):
        p = root / sub if sub else root
        s = str(p.resolve())
        if s not in sys.path:
            sys.path.insert(0, s)


def regenerate_detail_ware_rows(run_dir: Path) -> list[dict[str, str]]:
    _ensure_crawler_copy_path()
    from jd_detail_buyer_extraction import (  # noqa: WPS433
        buyer_promo_text_from_profile,
        buyer_ranking_line_from_profile,
        extract_buyer_offer_profile_from_json_text,
    )
    from jd_detail_ware_business_requests import (  # noqa: WPS433
        DETAIL_WARE_LEAN_CSV_FIELDNAMES,
        detail_ware_lean_csv_row,
    )

    run_dir = run_dir.expanduser().resolve()
    merged_path = run_dir / FILE_MERGED_CSV
    detail_dir = run_dir / "detail"
    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表: {merged_path}")
    if not detail_dir.is_dir():
        raise FileNotFoundError(f"缺少 detail 目录: {detail_dir}")

    rows_out: list[dict[str, str]] = []
    with merged_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get(SKU_FIELD_MERGED) or "").strip()
            if not sku:
                continue
            jp = detail_dir / f"ware_{sku}_response.json"
            if not jp.is_file():
                continue
            text = jp.read_text(encoding="utf-8")
            ing = (
                row.get(MERGED_FIELD_TO_CSV_HEADER["detail_body_ingredients"])
                or row.get("detail_body_ingredients")
                or ""
            ).strip()
            prof = extract_buyer_offer_profile_from_json_text(text)
            rows_out.append(
                detail_ware_lean_csv_row(
                    sku,
                    200,
                    text,
                    detail_body_ingredients=ing,
                    detail_body_ingredients_source_url="",
                    buyer_ranking_line=buyer_ranking_line_from_profile(prof),
                    buyer_promo_text=buyer_promo_text_from_profile(prof),
                )
            )
    return rows_out


def write_detail_ware_export_csv(run_dir: Path) -> tuple[int, Path]:
    rows = regenerate_detail_ware_rows(run_dir)
    _ensure_crawler_copy_path()
    from jd_detail_ware_business_requests import DETAIL_WARE_LEAN_CSV_FIELDNAMES  # noqa: WPS433

    out = run_dir.expanduser().resolve() / FILE_DETAIL_WARE_CSV
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=list(DETAIL_WARE_LEAN_CSV_FIELDNAMES),
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(rows)
    return len(rows), out
