"""调试：从手册 PDF 抽取全文 / 按关键词摘录；默认在终端打印**完整**摘录（不截断预览）。"""
from __future__ import annotations

import argparse
import os
import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


def main() -> int:
    import django

    django.setup()

    from pathlib import Path

    from django.conf import settings

    from pipeline.reporting.product_manual import (
        excerpt_cap_chars,
        excerpt_text_by_keyword,
        extract_pdf_plain_text,
        load_product_manual_text_from_env,
        pdf_extract_max_raw_chars,
    )

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "keywords",
        nargs="*",
        default=["清肌水光精华液", "水光精华", "美丽加减法"],
        help="依次尝试的关键词（默认如上）",
    )
    ap.add_argument(
        "--write-raw",
        type=str,
        default="",
        metavar="PATH",
        help="将 PDF 抽取的全文写入该文件（UTF-8）",
    )
    ap.add_argument(
        "--excerpt-max",
        type=int,
        default=-1,
        metavar="N",
        help="摘录最大字符数；-1=读环境 MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS（默认不限制）；0=不限制",
    )
    args = ap.parse_args()

    repo = Path(settings.LOW_GI_PROJECT_ROOT).resolve()
    default_pdf = repo / "data" / "beauty-product" / "美丽加减法产品手册.pdf"
    raw = (os.environ.get("MA_STRATEGY_PRODUCT_MANUAL_PDF") or "").strip()
    if raw:
        rp = Path(raw)
        pdf = str(rp.expanduser().resolve()) if rp.is_absolute() else str((repo / rp).resolve())
    else:
        pdf = str(default_pdf.resolve())
    p = Path(pdf).expanduser().resolve()
    if not p.is_file():
        print("PDF 不存在:", p, file=sys.stderr)
        return 1
    os.environ["MA_STRATEGY_PRODUCT_MANUAL_PDF"] = str(p)

    cap_raw = pdf_extract_max_raw_chars()
    print("MA_STRATEGY_PRODUCT_MANUAL_PDF_MAX_RAW →", cap_raw, "(None=不截断)")
    print("MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS →", excerpt_cap_chars(), "(None=不截断)")
    print()

    full = extract_pdf_plain_text(p, max_chars=cap_raw)
    print("=== PDF 全文抽取字符数:", len(full), "===")
    if args.write_raw:
        outp = Path(args.write_raw).expanduser().resolve()
        outp.write_text(full, encoding="utf-8")
        print("已写入:", outp)
        print()

    if args.excerpt_max < 0:
        ex_cap = excerpt_cap_chars()
    elif args.excerpt_max == 0:
        ex_cap = None
    else:
        ex_cap = args.excerpt_max

    for kw in args.keywords:
        chunk = excerpt_text_by_keyword(full, kw, max_chars=ex_cap)
        print(f"=== 关键词 {kw!r} → 摘录长度 {len(chunk)} ===")
        sys.stdout.write(chunk + ("\n" if not chunk.endswith("\n") else ""))
        print()

    kw0 = args.keywords[0]
    if args.excerpt_max < 0:
        merged = load_product_manual_text_from_env(strategy_keyword=kw0)
    else:
        merged = load_product_manual_text_from_env(
            strategy_keyword=kw0,
            excerpt_max_chars=ex_cap,
        )
    print(f"=== load_product_manual_text_from_env({kw0!r}) 长度 {len(merged)} ===")
    sys.stdout.write(merged + ("\n" if merged and not merged.endswith("\n") else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
