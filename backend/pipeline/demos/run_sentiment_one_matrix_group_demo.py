"""
仅针对**单个矩阵细类**试跑「8.3 评价正/负向主题」同源载荷与模型调用，**不**重写整份 competitor_analysis.md。

  cd backend
  python -m pipeline.demos.run_sentiment_one_matrix_group_demo --run-dir "../data/JD/pipeline_runs/20260413_104252_低GI" --group 饼干
  python -m pipeline.demos.run_sentiment_one_matrix_group_demo --run-dir "..." --group 饼干 --live

加 ``--live`` 才会真实请求大模型；否则只打印该细类评价条数与是否找到分组。
产出（仅 ``--live`` 且成功）：``run_dir/sentiment_8_3_one_group__{细类}.md`` 与同目录 ``sentiment_8_3_one_group__{细类}.json`` 元数据。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

JCR_ROOT = BACKEND_ROOT / "crawler_copy" / "jd_pc_search"
if str(JCR_ROOT) not in sys.path:
    sys.path.insert(0, str(JCR_ROOT))

from pipeline.competitor_report import jd_report as jcr  # noqa: E402
import jd_keyword_pipeline as kpl  # noqa: E402

from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER  # noqa: E402


def _safe_filename_part(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return "group"
    return re.sub(r'[<>:"/\\|?*]', "_", t)[:80]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="单细类评价正/负向主题 LLM（8.3 同源），不生成整报告",
    )
    parser.add_argument("--run-dir", type=str, required=True, help="流水线目录")
    parser.add_argument(
        "--group",
        type=str,
        default="饼干",
        help="矩阵细类名，须与第五章/feedback 分组名完全一致（默认：饼干）",
    )
    parser.add_argument("--keyword", type=str, default="", help="覆盖监测词（默认读 run_meta.keyword）")
    parser.add_argument(
        "--live",
        action="store_true",
        help="真实调用大模型并写入 run_dir 下 md/json",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    merged_path = run_dir / kpl.FILE_MERGED_CSV
    if not merged_path.is_file():
        raise SystemExit(f"缺少合并表: {merged_path}")

    _, merged_rows = jcr._read_csv_rows(merged_path)
    _, comment_rows = jcr._read_csv_rows(run_dir / kpl.FILE_COMMENTS_FLAT_CSV)

    meta_path = run_dir / kpl.FILE_RUN_META_JSON
    meta_kw = ""
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta_kw = str(meta.get("keyword") or "").strip()
        except json.JSONDecodeError:
            pass
    keyword = (args.keyword or meta_kw or "监测词").strip()

    want = (args.group or "").strip()
    feedback_groups = jcr._consumer_feedback_by_matrix_group(
        merged_rows=merged_rows,
        comment_rows=comment_rows,
        sku_header=MERGED_FIELD_TO_CSV_HEADER["sku_id"],
    )
    names = [g[0] for g in feedback_groups]
    match: tuple[str, list[dict[str, str]], list[str]] | None = None
    for item in feedback_groups:
        if item[0] == want:
            match = item
            break
    if match is None:
        print(f"未找到细类「{want}」。当前分组：{names}", flush=True)
        raise SystemExit(2)

    gname, cr_g, _tu = match
    sub_merged = [
        r
        for r in merged_rows
        if jcr._competitor_matrix_group_key(r) == gname
    ]
    units, scores = jcr._iter_comment_text_units_and_scores(cr_g, sub_merged)
    print(
        f"run_dir={run_dir}\nkeyword={keyword!r}\ngroup={gname!r}\n"
        f"comment_flat_rows={len(cr_g)} effective_text_units={len(units)}",
        flush=True,
    )
    if len(units) < 2:
        print("有效评价正文不足 2 条，与生产流水线一致将跳过该细类。", flush=True)
        raise SystemExit(3)

    if not args.live:
        print("\n(dry-run：加 --live 将调用大模型并写入 sentiment_8_3_one_group__*.md)", flush=True)
        return

    from pipeline.llm.generate import generate_comment_sentiment_analysis_llm  # noqa: WPS433

    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    attr_units = jcr._comment_lines_with_product_context(
        cr_g,
        merged_rows,
        sku_header=sku_h,
        title_h=title_h,
    )
    if len(attr_units) != len(units):
        attr_units = list(units)

    try:
        pl = jcr.build_comment_sentiment_llm_payload(
            units,
            scores=scores,
            attributed_texts=attr_units,
            max_samples_positive=16,
            max_samples_negative=30,
            max_samples_mixed=10,
            max_chars_per_review=360,
            semantic_pool_max=40,
            shuffle_seed=f"{keyword}|{gname}",
        )
        pl["keyword"] = keyword
        pl["matrix_group_focus"] = gname
        md_one = generate_comment_sentiment_analysis_llm(pl)
    except Exception as e:
        print(f"FAIL: {e}", flush=True)
        traceback.print_exc()
        raise SystemExit(1) from e

    body = f"#### {gname}\n\n{md_one.strip()}\n"
    stem = _safe_filename_part(gname)
    out_md = run_dir / f"sentiment_8_3_one_group__{stem}.md"
    out_md.write_text(
        "### 8.3 评价正/负向主题（按细类 · 大模型）· 单类试跑\n\n"
        f"> keyword={keyword!r}，仅本节为脚本独立产出，未合并进 competitor_analysis.md。\n\n"
        + body,
        encoding="utf-8",
    )
    rec: dict[str, Any] = {
        "schema_version": 1,
        "demo": "run_sentiment_one_matrix_group_demo",
        "group": gname,
        "keyword": keyword,
        "ok": True,
        "chars": len(md_one),
        "markdown_file": out_md.name,
    }
    (run_dir / f"sentiment_8_3_one_group__{stem}.json").write_text(
        json.dumps(rec, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"ok，已写 {out_md.name}（{len(md_one)} 字模型正文）", flush=True)


if __name__ == "__main__":
    main()
