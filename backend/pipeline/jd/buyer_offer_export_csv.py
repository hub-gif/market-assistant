# -*- coding: utf-8 -*-
"""
将 ``detail_ware_export.csv`` 与 ``detail/ware_*_response.json`` 合并为一张表：
在原 lean 列后追加 ``buyer_ranking_line``、``buyer_promo_text``（促销相关摘要句，用稳定分隔符拼接）。

输出默认写入 ``<run_dir>/buyer_offer_profiles/buyer_offer_with_detail.csv``。
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

# 与 pipeline.ingest / jd_keyword_pipeline 中文件名一致（避免 import ingest 触发 Django）
FILE_DETAIL_WARE_CSV = "detail_ware_export.csv"
# 与 jd_keyword_pipeline.DIR_BUYER_OFFER_PROFILES 一致
DIR_BUYER_OFFER_PROFILES = "buyer_offer_profiles"
FILE_BUYER_OFFER_WITH_DETAIL_CSV = "buyer_offer_with_detail.csv"

def _ensure_crawler_detail_path() -> None:
    # 本文件位于 pipeline/jd/，向上两级为 backend/
    root = Path(__file__).resolve().parents[2] / "crawler_copy" / "jd_pc_search"
    for sub in ("detail", ""):
        p = root / sub if sub else root
        s = str(p.resolve())
        if s not in sys.path:
            sys.path.insert(0, s)


def export_buyer_offer_with_detail_csv(
    run_dir: str | Path,
    *,
    promo_sep: str = " | ",
) -> Path:
    """
    读取 ``run_dir/detail_ware_export.csv`` 与 ``run_dir/detail/ware_{sku}_response.json``，
    写出 ``run_dir/buyer_offer_profiles/buyer_offer_with_detail.csv``。
    """
    _ensure_crawler_detail_path()
    from jd_detail_buyer_extraction import (  # noqa: WPS433
        buyer_promo_text_from_profile,
        buyer_ranking_line_from_profile,
        extract_buyer_offer_profile_from_json_text,
    )
    from jd_detail_ware_business_requests import (  # noqa: WPS433
        DETAIL_WARE_LEAN_CSV_FIELDNAMES,
    )

    run_dir = Path(run_dir).expanduser().resolve()
    src = run_dir / FILE_DETAIL_WARE_CSV
    if not src.is_file():
        raise FileNotFoundError(f"缺少详情汇总表: {src}")
    detail_dir = run_dir / "detail"
    if not detail_dir.is_dir():
        raise FileNotFoundError(f"缺少 detail 目录: {detail_dir}")

    out_dir = run_dir / DIR_BUYER_OFFER_PROFILES
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / FILE_BUYER_OFFER_WITH_DETAIL_CSV

    fieldnames: list[str] = list(DETAIL_WARE_LEAN_CSV_FIELDNAMES)

    with src.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows_out: list[dict[str, str]] = []
        for row in reader:
            sku = str(row.get("SKU") or row.get("skuId") or "").strip()
            base = {
                k: str(row.get(k) or "").strip() for k in DETAIL_WARE_LEAN_CSV_FIELDNAMES
            }
            rline = base.get("榜单排名") or base.get("buyer_ranking_line") or ""
            ptext = base.get("促销摘要") or base.get("buyer_promo_text") or ""
            if (not rline and not ptext) and sku:
                jp = detail_dir / f"ware_{sku}_response.json"
                if jp.is_file():
                    text = jp.read_text(encoding="utf-8").strip()
                    if text:
                        prof = extract_buyer_offer_profile_from_json_text(text)
                        rline = buyer_ranking_line_from_profile(prof)
                        ptext = buyer_promo_text_from_profile(prof, sep=promo_sep)
                        base["榜单排名"] = rline
                        base["促销摘要"] = ptext
            rows_out.append(base)

    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_out)

    return out_path


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) < 1:
        print(
            "用法: python -m pipeline.jd.buyer_offer_export_csv <run_dir>\n"
            "  例: python -m pipeline.jd.buyer_offer_export_csv "
            "data/JD/pipeline_runs/20260413_104252_低GI",
            file=sys.stderr,
        )
        sys.exit(2)
    run_dir = argv[0]
    path = export_buyer_offer_with_detail_csv(run_dir)
    print(path)


if __name__ == "__main__":
    main()
