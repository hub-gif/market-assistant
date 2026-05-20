"""
探测：在「矩阵收窄后的 brief」里仅限制 ``matrix_by_group[0].skus`` 数量时，
策略润色一次调用的 ``estimate_chat_input_tokens(system+user)`` 约在何值；
二分查找「保留多少个 SKU」可压到目标估算以下（默认按 32k 网关留足 completion 余量）。

**说明**：
- 不改 ``consumer_feedback_by_matrix_group`` 等大块，仅矩阵 SKU 子采样 + 同步重算价盘/集中度等
  （与 ``filter_brief_for_strategy_matrix_group`` 内对 skus 的衍生字段一致思路）。
- 本地 ``len*0.55+512`` 与网关真实 tokenizer 常有 **10%～15%** 正偏差；可用
  ``--observed-ratio`` 粗算「网关侧输入 token」。

用法（在 backend 目录）::

  python -m pipeline.demos.probe_strategy_sku_threshold_for_context --job-id 30
  python -m pipeline.demos.probe_strategy_sku_threshold_for_context --job-id 30 --target-est 28500
  python -m pipeline.demos.probe_strategy_sku_threshold_for_context --job-id 30 --observed-ratio 1.135
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

from pipeline.reporting.brief_strategy_scope import trim_matrix_group_skus_for_llm


def _estimate_for_n(
    scoped_brief: dict[str, Any],
    n: int,
    job_id: int,
    kw: str,
    gen_at: str,
    sd: dict[str, Any],
    business_notes: str,
    excerpt_raw: str | None,
    evidence_md: str | None,
    rc: dict[str, Any] | None,
    our_product_profile: str,
) -> tuple[int, str]:
    from pipeline.llm.generate_strategy import (
        STRATEGY_SYSTEM,
        resolve_strategy_draft_llm_input_snapshot,
    )
    from pipeline.llm.llm_client import estimate_chat_input_tokens

    b = trim_matrix_group_skus_for_llm(scoped_brief, n)
    _payload, user, tier = resolve_strategy_draft_llm_input_snapshot(
        job_id=job_id,
        keyword=kw,
        brief=b,
        business_notes=business_notes,
        generated_at_iso=gen_at,
        strategy_decisions=sd,
        report_strategy_excerpt=excerpt_raw,
        report_matrix_group_evidence_md=evidence_md,
        report_config=rc,
        our_product_profile=our_product_profile,
    )
    est = estimate_chat_input_tokens(STRATEGY_SYSTEM, user)
    return est, tier


def main() -> int:
    import django

    django.setup()

    from django.utils import timezone

    from pipeline.jd.runner import build_competitor_brief_for_job
    from pipeline.models import JobStatus, PipelineJob
    from pipeline.reporting.brief_strategy_scope import (
        filter_brief_for_strategy_matrix_group,
        list_matrix_groups_for_api,
    )
    from pipeline.reporting.report_matrix_group_evidence import (
        load_report_matrix_group_evidence_markdown,
    )
    from pipeline.reporting.report_strategy_excerpt import load_report_strategy_excerpt
    from pipeline.strategy_decision_keys import empty_strategy_decisions

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--job-id", type=int, default=None)
    ap.add_argument("--run-dir", type=Path, default=None)
    ap.add_argument("--matrix-index", type=int, default=0)
    ap.add_argument("--no-scope", action="store_true")
    ap.add_argument(
        "--target-est",
        type=int,
        default=28500,
        help="estimate_chat_input_tokens 目标上限（偏保守，为真实 tokenizer 留余量）；"
        "32k 网关约可对应 28500～30000 视 completion 与偏差而定",
    )
    ap.add_argument(
        "--observed-ratio",
        type=float,
        default=0.0,
        help="若曾对比过网关 prompt_tokens/本地 est，填比值，如 38001/33485≈1.135，用于打印粗算网关输入",
    )
    ap.add_argument(
        "--our-product-profile",
        type=str,
        default="",
        help="与线上一致：与 MA_STRATEGY_PRODUCT_MANUAL_PDF 合并进底稿 §1.3 本品依据，影响 est",
    )
    ap.add_argument("--sweep-step", type=int, default=0, help=">0 时每隔 step 打一行 n,est，便于画曲线")
    args = ap.parse_args()

    if args.run_dir is not None:
        run_dir_p = args.run_dir.expanduser().resolve()
        if not run_dir_p.is_dir():
            print(f"run_dir 不存在: {run_dir_p}", file=sys.stderr)
            return 1
        rc_path = run_dir_p / "effective_report_config.json"
        meta_path = run_dir_p / "run_meta.json"
        if not rc_path.is_file() or not meta_path.is_file():
            print("缺少 effective_report_config.json 或 run_meta.json", file=sys.stderr)
            return 1
        rc = json.loads(rc_path.read_text(encoding="utf-8"))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        kw = (meta.get("keyword") or "").strip()
        if not kw:
            print("run_meta 无 keyword", file=sys.stderr)
            return 1
        run_dir_s = str(run_dir_p)
        job_id = 0
    else:
        jid = args.job_id
        if jid:
            job = PipelineJob.objects.filter(pk=jid).first()
        else:
            job = (
                PipelineJob.objects.filter(status=JobStatus.SUCCESS)
                .exclude(run_dir="")
                .order_by("-id")
                .first()
            )
        if not job:
            print("无可用任务", file=sys.stderr)
            return 1
        run_dir_s = job.run_dir
        kw = job.keyword
        rc = job.report_config if isinstance(job.report_config, dict) else None
        job_id = job.id

    brief = build_competitor_brief_for_job(run_dir_s, kw, report_config=rc)
    matrix_index = -1 if args.no_scope else args.matrix_index
    scoped_label = ""
    if matrix_index >= 0:
        mg = brief.get("matrix_by_group")
        if not isinstance(mg, list) or matrix_index >= len(mg):
            print("matrix_index 越界", file=sys.stderr)
            return 1
        scoped_label = (mg[matrix_index].get("group") or "").strip()
        scoped = filter_brief_for_strategy_matrix_group(
            brief, matrix_group_index=matrix_index
        )
    else:
        scoped = brief

    mg0 = scoped.get("matrix_by_group")
    if not isinstance(mg0, list) or not mg0:
        print("无 matrix_by_group，无法按 SKU 数探测", file=sys.stderr)
        return 1
    skus_full = mg0[0].get("skus") if isinstance(mg0[0], dict) else None
    full_n = len(skus_full) if isinstance(skus_full, list) else 0
    if full_n < 1:
        print("矩阵分组内 SKU 数为 0", file=sys.stderr)
        return 1

    gen_at = timezone.now().isoformat()
    sd = empty_strategy_decisions()
    excerpt_raw, _ = load_report_strategy_excerpt(run_dir_s)
    excerpt_raw = (excerpt_raw or "").strip() or None
    evidence_md: str | None = None
    if scoped_label:
        em, _ = load_report_matrix_group_evidence_markdown(run_dir_s, scoped_label)
        evidence_md = (em or "").strip() or None

    from pipeline.reporting.product_manual import merged_our_product_profile_for_strategy

    our_profile = merged_our_product_profile_for_strategy(
        user_text=(args.our_product_profile or "").strip(),
        strategy_keyword=(kw or "").strip(),
    )

    def Tiered_est(n: int) -> tuple[int, str]:
        return _estimate_for_n(
            scoped_brief=scoped,
            n=n,
            job_id=job_id,
            kw=kw,
            gen_at=gen_at,
            sd=sd,
            business_notes="",
            excerpt_raw=excerpt_raw,
            evidence_md=evidence_md,
            rc=rc,
            our_product_profile=our_profile,
        )

    est_full, tier_full = Tiered_est(full_n)
    print("job_id:", job_id, "keyword:", kw)
    print("matrix:", scoped_label or "(未收窄)", "分组内 SKU 总数:", full_n)
    print("选用档位（全量 SKU）:", tier_full)
    print("target estimate_chat_input_tokens <=", args.target_est)
    print(
        f"全量 SKU: est={est_full}"
        + (
            f" 粗算网关≈{est_full * args.observed_ratio:.0f}"
            if args.observed_ratio > 0
            else ""
        )
    )

    if args.sweep_step > 0:
        print("\n-- sweep --")
        for n in range(1, full_n + 1, args.sweep_step):
            e, t = Tiered_est(n)
            extra = f" 网关≈{e * args.observed_ratio:.0f}" if args.observed_ratio > 0 else ""
            print(f"n={n:4d} est={e:6d}{extra} tier={t[:60]}…")

    if est_full <= args.target_est:
        print(
            f"\n结论：当前「全量 {full_n} 个 SKU」下 est 已为 {est_full} <= {args.target_est}，"
            "无需为 32k 网关再减 SKU（若仍 400，请对照网关真实 prompt_tokens 调 --target-est 或 --observed-ratio）。"
        )
        return 0

    lo, hi = 1, full_n
    best = 1
    while lo <= hi:
        mid = (lo + hi) // 2
        e, _t = Tiered_est(mid)
        if e <= args.target_est:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    est_best, tier_best = Tiered_est(best)
    if best < full_n:
        est_next, _ = Tiered_est(best + 1)
    else:
        est_next = est_best
    print(
        f"\n结论（仅缩矩阵 SKU，consumer_feedback 等未缩）："
        f"\n  满足 est<={args.target_est} 的最大 SKU 数 ≈ **{best}** / {full_n}"
        f"\n  该点 est={est_best}，档位摘要：{tier_best[:160]}"
    )
    if best < full_n:
        print(f"  best+1 SKU 时 est={est_next}（应 > {args.target_est} 或等于边界）")
    if args.observed_ratio > 0:
        print(
            f"  粗算网关输入：best 点 ≈ {est_best * args.observed_ratio:.0f} token；"
            f"best+1 ≈ {est_next * args.observed_ratio:.0f}"
        )
    groups = list_matrix_groups_for_api(brief)
    print(
        "可选细类：",
        [f"{g.get('index')}:{g.get('group')}" for g in groups[:16]],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())