"""
竞品报告中与大模型相关的块（与 ``pipeline.jd.runner.write_competitor_analysis_for_run_dir`` 同源）：

- §5 后：``generate_matrix_group_summaries_llm``
- §6 后：``generate_price_group_summaries_llm``、``generate_promo_group_summaries_llm``
- §8.2：``generate_comment_sentiment_analysis_llm``
- §8末细类评价：``generate_comment_group_summaries_llm``
- §8.3 右栏后使用场景：``generate_scenario_group_summaries_llm``
- §9 策略与机会：``generate_strategy_opportunities_llm``（``build_competitor_brief`` + 可选 ``chapter_llm_narratives`` 与各章归纳对齐）
- §8.5 类全文补充（独立长文）：``generate_competitor_report_markdown_llm``

 cd backend
  python -m pipeline.demos.run_report_llm_chapters_demo --run-dir "../data/JD/pipeline_runs/20260413_104252_低GI"
  python -m pipeline.demos.run_report_llm_chapters_demo --run-dir "..." --live
  python -m pipeline.demos.run_report_llm_chapters_demo --run-dir "..." --live --only matrix,price,promo
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Callable

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

JCR_ROOT = BACKEND_ROOT / "crawler_copy" / "jd_pc_search"
if str(JCR_ROOT) not in sys.path:
    sys.path.insert(0, str(JCR_ROOT))

from pipeline.competitor_report import jd_report as jcr  # noqa: E402
import jd_keyword_pipeline as kpl  # noqa: E402

from pipeline.csv_schema import MERGED_FIELD_TO_CSV_HEADER  # noqa: E402
from pipeline.jd.runner import (  # noqa: E402
    get_default_report_config,
    use_chunked_group_summaries_llm,
)


def _load_run(
    run_dir: Path,
) -> tuple[
    str,
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    dict[str, Any] | None,
    dict[str, Any],
]:
    run_dir = run_dir.resolve()
    merged_path = run_dir / kpl.FILE_MERGED_CSV
    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表: {merged_path}")
    _, merged_rows = jcr._read_csv_rows(merged_path)
    _, search_export_rows = jcr._read_csv_rows(run_dir / kpl.FILE_PC_SEARCH_CSV)
    _, comment_rows = jcr._read_csv_rows(run_dir / kpl.FILE_COMMENTS_FLAT_CSV)
    meta_path = run_dir / kpl.FILE_RUN_META_JSON
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = None
    eff_path = run_dir / "effective_report_config.json"
    if eff_path.is_file():
        try:
            eff_rc = json.loads(eff_path.read_text(encoding="utf-8"))
            if not isinstance(eff_rc, dict):
                eff_rc = get_default_report_config()
        except json.JSONDecodeError:
            eff_rc = get_default_report_config()
    else:
        eff_rc = get_default_report_config()
    kw = ""
    if meta and str(meta.get("keyword") or "").strip():
        kw = str(meta.get("keyword")).strip()
    return kw, merged_rows, search_export_rows, comment_rows, meta, eff_rc


def _run_one(
    name: str,
    fn: Callable[[], str],
    *,
    live: bool,
    preview_chars: int,
) -> None:
    print(f"\n{'=' * 60}\n## {name}\n{'=' * 60}", flush=True)
    if not live:
        print("(dry-run：加 --live 将调用大模型)", flush=True)
        return
    try:
        out = fn()
        t = (out or "").strip()
        print(f"ok，长度 {len(t)} 字符", flush=True)
        if t:
            head = t[:preview_chars]
            print(head + ("…\n" if len(t) > preview_chars else "\n"), flush=True)
    except Exception as e:
        print(f"FAIL: {e}", flush=True)
        traceback.print_exc()


def main() -> None:
    parser = argparse.ArgumentParser(description="报告各块 LLM 串联试跑")
    parser.add_argument(
        "--run-dir",
        type=str,
        required=True,
        help="流水线目录（含 keyword_pipeline_merged.csv）",
    )
    parser.add_argument(
        "--keyword",
        type=str,
        default="",
        help="覆盖监测词（默认读 meta.keyword）",
    )
    parser.add_argument("--live", action="store_true", help="真实调用大模型")
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="逗号分隔子集：sentiment,matrix,price,promo,strategy_opp,scenario_groups,comment_groups,report_supplement",
    )
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=400,
        help="--live 时每段打印前 N 字",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser()
    kw, merged, search_rows, comment_rows, meta, eff_rc = _load_run(run_dir)
    keyword = (args.keyword or kw or "竞品监测").strip()

    only = {x.strip().lower() for x in args.only.split(",") if x.strip()}
    all_names = {
        "sentiment",
        "matrix",
        "price",
        "promo",
        "strategy_opp",
        "scenario_groups",
        "comment_groups",
        "report_supplement",
    }
    if not only:
        only = all_names

    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]

    print(
        f"# run_dir={run_dir}\n# keyword={keyword}\n"
        f"# merged_rows={len(merged)} comments={len(comment_rows)} "
        f"list={len(search_rows)}\n# only={only or all_names}",
        flush=True,
    )

    from pipeline.llm.generate import (  # noqa: WPS433
        generate_comment_group_summaries_llm,
        generate_comment_group_summaries_llm_chunked,
        generate_comment_sentiment_analysis_llm,
        generate_competitor_report_markdown_llm,
        generate_matrix_group_summaries_llm,
        generate_matrix_group_summaries_llm_chunked,
        generate_price_group_summaries_llm,
        generate_price_group_summaries_llm_chunked,
        generate_promo_group_summaries_llm,
        generate_promo_group_summaries_llm_chunked,
        generate_strategy_opportunities_llm,
        generate_scenario_group_summaries_llm,
        generate_scenario_group_summaries_llm_chunked,
    )

    chunk_gr = use_chunked_group_summaries_llm(eff_rc)

    if "sentiment" in only:
        comment_units, comment_scores = jcr._iter_comment_text_units_and_scores(
            comment_rows, merged
        )
        attr_units = jcr._comment_lines_with_product_context(
            comment_rows,
            merged,
            sku_header=sku_h,
            title_h=title_h,
        )
        if len(attr_units) != len(comment_units):
            attr_units = list(comment_units)

        def _sent() -> str:
            pl = jcr.build_comment_sentiment_llm_payload(
                comment_units,
                scores=comment_scores,
                attributed_texts=attr_units,
                semantic_pool_max=40,
                shuffle_seed=keyword,
            )
            pl["keyword"] = keyword
            return generate_comment_sentiment_analysis_llm(pl)

        _run_one(
            "§8.2 评价正负面（llm_comment_sentiment）",
            _sent,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            print(
                f"  payload: comment_units={len(comment_units)} "
                f"(需 >=2 条才会在生产流水线里调用)",
                flush=True,
            )

    if "matrix" in only:
        pl_mx = jcr.build_matrix_groups_llm_payload(
            merged, sku_header=sku_h, title_h=title_h
        )

        def _mx() -> str:
            if chunk_gr:
                return generate_matrix_group_summaries_llm_chunked(
                    pl_mx, keyword=keyword
                )
            return generate_matrix_group_summaries_llm(pl_mx, keyword=keyword)

        _run_one("§5 细类要点归纳（matrix）", _mx, live=args.live, preview_chars=args.preview_chars)
        if not args.live:
            print(
                f"  payload groups={len(pl_mx)} chunked_by_matrix={chunk_gr}",
                flush=True,
            )

    if "price" in only:
        pl_pr = jcr.build_price_groups_llm_payload(
            merged, sku_header=sku_h, title_h=title_h
        )

        def _pr() -> str:
            if chunk_gr:
                return generate_price_group_summaries_llm_chunked(
                    pl_pr, keyword=keyword
                )
            return generate_price_group_summaries_llm(pl_pr, keyword=keyword)

        _run_one("§6 细类价盘归纳（price）", _pr, live=args.live, preview_chars=args.preview_chars)
        if not args.live:
            print(
                f"  payload groups={len(pl_pr)} chunked_by_matrix={chunk_gr}",
                flush=True,
            )

    if "promo" in only:
        pl_po = jcr.build_promo_groups_llm_payload(
            merged, sku_header=sku_h, title_h=title_h
        )

        def _po() -> str:
            if chunk_gr:
                return generate_promo_group_summaries_llm_chunked(
                    pl_po, keyword=keyword
                )
            return generate_promo_group_summaries_llm(pl_po, keyword=keyword)

        _run_one(
            "§6 细类促销与活动归纳（promo）",
            _po,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            print(
                f"  payload groups={len(pl_po)} chunked_by_matrix={chunk_gr}",
                flush=True,
            )

    if "strategy_opp" in only:
        brief = jcr.build_competitor_brief(
            run_dir=run_dir,
            keyword=keyword,
            merged_rows=merged,
            search_export_rows=search_rows,
            comment_rows=comment_rows,
            meta=meta,
            report_config=eff_rc,
        )

        def _st() -> str:
            return generate_strategy_opportunities_llm(brief, keyword=keyword)

        _run_one(
            "§9 策略与机会（strategy_opp）",
            _st,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            print(
                f"  brief keys: {len(brief)} top-level fields",
                flush=True,
            )

    if "scenario_groups" in only:
        _, scen_tuple, _ = jcr.resolve_report_tuning(eff_rc)
        fb_s = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=comment_rows,
            sku_header=sku_h,
        )
        pl_sg = jcr.build_scenario_groups_llm_payload(
            feedback_groups=fb_s,
            scenario_groups=scen_tuple,
            merged_rows=merged,
            sku_header=sku_h,
            title_h=title_h,
        )

        def _sg() -> str:
            if chunk_gr:
                return generate_scenario_group_summaries_llm_chunked(
                    pl_sg, keyword=keyword
                )
            return generate_scenario_group_summaries_llm(pl_sg, keyword=keyword)

        _run_one(
            "§8.3 使用场景归纳（scenario_groups）",
            _sg,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            n = len((pl_sg or {}).get("groups") or [])
            print(
                f"  payload groups={n} chunked_by_matrix={chunk_gr}",
                flush=True,
            )

    if "comment_groups" in only:
        fb = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=comment_rows,
            sku_header=sku_h,
        )
        fw_src = eff_rc.get("comment_focus_words") or list(jcr.COMMENT_FOCUS_WORDS)
        fw_tuple = tuple(
            str(x).strip() for x in fw_src if str(x).strip()
        ) or jcr.COMMENT_FOCUS_WORDS
        pl_cg = jcr.build_comment_groups_llm_payload(
            feedback_groups=fb,
            focus_words=fw_tuple,
            merged_rows=merged,
            sku_header=sku_h,
            title_h=title_h,
        )

        def _cg() -> str:
            if chunk_gr:
                return generate_comment_group_summaries_llm_chunked(
                    pl_cg, keyword=keyword
                )
            return generate_comment_group_summaries_llm(pl_cg, keyword=keyword)

        _run_one(
            "§8 细类评价与关注词（comment_groups）",
            _cg,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            print(
                f"  payload groups={len(pl_cg)} chunked_by_matrix={chunk_gr}",
                flush=True,
            )

    if "report_supplement" in only:
        brief = jcr.build_competitor_brief(
            run_dir=run_dir,
            keyword=keyword,
            merged_rows=merged,
            search_export_rows=search_rows,
            comment_rows=comment_rows,
            meta=meta,
            report_config=eff_rc,
        )

        def _rp() -> str:
            return generate_competitor_report_markdown_llm(brief, keyword)

        _run_one(
            "§8.5 类报告补充（generate_competitor_report_markdown_llm）",
            _rp,
            live=args.live,
            preview_chars=args.preview_chars,
        )
        if not args.live:
            print("  使用 build_competitor_brief 全量摘要作为输入", flush=True)


if __name__ == "__main__":
    main()
