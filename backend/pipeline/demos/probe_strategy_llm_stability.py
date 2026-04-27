"""
同一组入参连续调用「独立策略稿」LLM 若干次，比较全文是否漂移。

用法（在 backend 目录）::

  python -m pipeline.demos.probe_strategy_llm_stability --run-dir \"D:/.../pipeline_runs/某批次\" --rounds 4

  # 仅**探针/复现时**：用 ``--stability-preset`` 在 **strategy_decisions + business_notes**
  # 上收窄「阶段目标/促销」方向，**不**改动产线 `STRATEGY_SYSTEM` 等提示词工程。

  python -m pipeline.demos.probe_strategy_llm_stability --run-dir \"...\" --stability-preset cold_start_promo

  # 每轮完整稿落盘（UTF-8），便于搜 §8.3、阶段目标等

  python -m pipeline.demos.probe_strategy_llm_stability --run-dir \"...\" --write-md-dir ./probe_rounds

默认 temperature 走 ``AI_crawler.chat_completion_text``（当前默认 0.2），未显式设 seed 时
不同轮次出文可略有差异，属预期。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# 探针专用：经表单同源字段 `strategy_decisions` / `business_notes` 传入，不改编产线系统提示词。
_STABILITY_PRESET_COLD_START_GOAL = "冷启动期，以占品类心智与新客获取为主"
_STABILITY_PRESET_COLD_START_NOTES = (
    "【稳定性探针·测试约定】本批为多次调用对比，请在成稿中："
    "①「本阶段策略目标类型」、§1.3 与 §六 须与上表**本阶段策略目标类型**一致，侧重冷启动、占品类心智与拉新，"
    "避免在缺乏说明时以纯冲量/冲榜/搜索位次为**唯一**主轴；"
    "② §8.3 须给出可照后台配置的**拟定**满减/满折档（可写满[门槛]减[面额]、满[门槛]享[折]数，"
    "并标「待运营按毛利与后台活动核定」），勿仅用「加强大促/做活动」句带过而无档位骨架。"
)


def _empty_strategy_decisions() -> dict[str, Any]:
    return {
        "product_role": "",
        "stage_goal_type": "",
        "time_horizon": "",
        "success_criteria": "",
        "non_goals": "",
        "battlefield_one_line": "",
        "positioning_choice": "",
        "competitive_stance": "",
        "pillar_product": "",
        "pillar_price": "",
        "pillar_channel": "",
        "pillar_comm": "",
        "audience_segment": "",
        "competitor_reference": "",
        "resource_notes": "",
        "marketing_strategy": "",
        "general_strategy": "",
        "ack_risk_keywords": False,
        "ack_risk_price": False,
        "ack_risk_concentration": False,
    }


def _merge_decisions(
    base: dict[str, Any], overlay: dict[str, Any] | None
) -> dict[str, Any]:
    out = dict(base)
    if isinstance(overlay, dict):
        out.update(overlay)
    return out


def _apply_stability_preset(
    preset: str,
    strategy_decisions: dict[str, Any],
    business_notes: str,
) -> tuple[dict[str, Any], str]:
    """
    在**探针**里收窄输入方向；不修改 `generate_strategy` 的 system prompt。

    合并顺序：先由调用方组好 ``strategy_decisions``（可含 --decisions-json），
    再于本处**覆盖** preset 所涉字段，以免与产线「未填表则模型自推」的基线混测。
    """
    sd = dict(strategy_decisions)
    notes = (business_notes or "").strip()
    if preset == "none" or not preset:
        return sd, notes
    if preset == "cold_start_promo":
        sd["stage_goal_type"] = _STABILITY_PRESET_COLD_START_GOAL
        extra = _STABILITY_PRESET_COLD_START_NOTES
        if notes:
            return sd, f"{notes}\n\n{extra}"
        return sd, extra
    raise ValueError(f"unknown stability preset: {preset!r}")


def main() -> int:
    import django

    django.setup()

    from django.utils import timezone

    from pipeline.jd.runner import build_competitor_brief_for_job
    from pipeline.llm.generate_strategy import generate_strategy_draft_markdown_llm
    from pipeline.models import JobStatus, PipelineJob
    from pipeline.reporting.brief_strategy_scope import (
        filter_brief_for_strategy_matrix_group,
    )
    from pipeline.reporting.report_matrix_group_evidence import (
        load_report_matrix_group_evidence_markdown,
    )
    from pipeline.reporting.report_strategy_excerpt import load_report_strategy_excerpt

    p = argparse.ArgumentParser(description=__doc__)
    src = p.add_mutually_exclusive_group()
    src.add_argument("--job-id", type=int, default=None, help="PipelineJob 主键")
    src.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="运行目录；与 --job-id 二选一",
    )
    p.add_argument("--rounds", type=int, default=4, help="连续调用次数（默认 4）")
    p.add_argument(
        "--matrix-index",
        type=int,
        default=0,
        help="矩阵分组下标；-1 表示不收窄",
    )
    p.add_argument(
        "--no-scope",
        action="store_true",
        help="与 --matrix-index -1 相同",
    )
    p.add_argument(
        "--decisions-json",
        type=Path,
        default=None,
        help="覆盖 strategy_decisions 的 JSON 文件",
    )
    p.add_argument(
        "--business-notes",
        type=str,
        default="",
    )
    p.add_argument(
        "--snapshot-job-id",
        type=int,
        default=None,
        help="payload.job_id（仅 --run-dir 时，默认 0）",
    )
    p.add_argument(
        "--head-chars",
        type=int,
        default=800,
        help="每轮打印文首字符数，便于肉眼看方向是否一致",
    )
    p.add_argument(
        "--stability-preset",
        type=str,
        default="none",
        choices=("none", "cold_start_promo"),
        help=(
            "探针专用：none=与线上同（空表则由模型自推）。"
            "cold_start_promo=写入 stage_goal 冷启动/占心智/新客，并在 business_notes 中约定"
            "§8.3 满减/满折拟定档；**不**改产线 system prompt"
        ),
    )
    p.add_argument(
        "--write-md-dir",
        type=Path,
        default=None,
        help=(
            "若指定，则每轮将**完整**策略稿 Markdown 写入该目录，文件名为 round_01.md、round_02.md …"
            "（UTF-8 无 BOM）；目录不存在会创建"
        ),
    )
    args = p.parse_args()

    if args.rounds < 1:
        print("rounds 须 >= 1", file=sys.stderr)
        return 1

    run_dir_s: str
    kw: str
    rc: dict[str, Any] | None
    job_id: int

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
        job_id = int(args.snapshot_job_id) if args.snapshot_job_id is not None else 0
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
            print("无可用任务：请指定 --job-id 或 --run-dir", file=sys.stderr)
            return 1
        run_dir_s = job.run_dir
        kw = job.keyword
        rc = job.report_config if isinstance(job.report_config, dict) else None
        job_id = job.id

    brief = build_competitor_brief_for_job(
        run_dir_s,
        kw,
        report_config=rc,
    )
    matrix_index: int | None = args.matrix_index
    if args.no_scope:
        matrix_index = -1
    scoped_label = ""
    if matrix_index is not None and matrix_index >= 0:
        mg = brief.get("matrix_by_group")
        if isinstance(mg, list) and matrix_index < len(mg):
            scoped_label = (mg[matrix_index].get("group") or "").strip()
            brief = filter_brief_for_strategy_matrix_group(
                brief, matrix_group_index=matrix_index
            )
        else:
            print(f"matrix_index {matrix_index} 超出范围", file=sys.stderr)
            return 1

    sd = _empty_strategy_decisions()
    if args.decisions_json is not None:
        dp = args.decisions_json.expanduser().resolve()
        if not dp.is_file():
            print(f"decisions-json 不存在: {dp}", file=sys.stderr)
            return 1
        loaded = json.loads(dp.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            print("decisions-json 根须为 JSON 对象", file=sys.stderr)
            return 1
        sd = _merge_decisions(sd, loaded)

    try:
        sd, business_notes_effective = _apply_stability_preset(
            args.stability_preset, sd, (args.business_notes or "").strip()
        )
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    gen_at = timezone.now().isoformat()
    report_excerpt, ex_src = load_report_strategy_excerpt(run_dir_s)
    report_excerpt = (report_excerpt or "").strip()

    evidence_md = ""
    evidence_src = "none"
    if scoped_label:
        evidence_md, evidence_src = load_report_matrix_group_evidence_markdown(
            run_dir_s,
            scoped_label,
        )

    print("run_dir:", run_dir_s)
    print("job_id (payload):", job_id)
    print("keyword:", kw)
    print("matrix scope:", scoped_label or "（未收窄）")
    print("rounds:", args.rounds)
    print("stability_preset:", args.stability_preset)
    if args.stability_preset != "none":
        print("stage_goal_type (preset):", sd.get("stage_goal_type", ""))
    print("report_strategy_excerpt:", ex_src, "chars", len(report_excerpt))
    print("report_matrix_group_evidence:", evidence_src, "chars", len((evidence_md or "").strip()))
    print("---")
    print(
        "说明：与线上一致走 generate_strategy_draft_markdown_llm；"
        "温度见 AI_crawler.chat_completion_text 默认；未设 seed 时多次调用可不同。"
    )
    print("---")

    write_dir: Path | None = None
    if args.write_md_dir is not None:
        write_dir = args.write_md_dir.expanduser().resolve()
        write_dir.mkdir(parents=True, exist_ok=True)
        print("write_md_dir:", write_dir)
        print("---")

    hashes: list[str] = []
    for i in range(1, args.rounds + 1):
        md = generate_strategy_draft_markdown_llm(
            job_id=job_id,
            keyword=kw,
            brief=brief,
            business_notes=business_notes_effective,
            generated_at_iso=gen_at,
            strategy_decisions=sd,
            report_strategy_excerpt=report_excerpt or None,
            report_matrix_group_evidence_md=(evidence_md or "").strip() or None,
            report_config=rc,
        )
        digest = hashlib.sha256(md.encode("utf-8")).hexdigest()
        hashes.append(digest)
        if write_dir is not None:
            out_path = write_dir / f"round_{i:02d}.md"
            out_path.write_text(
                (md or "").replace("\r\n", "\n"),
                encoding="utf-8",
                newline="\n",
            )
        head = (md[: args.head_chars]).replace("\r\n", "\n")
        print(f"=== 第 {i} 次 full_sha256={digest} 总长={len(md)} ===")
        if write_dir is not None:
            print(f"  -> 已写: {write_dir / f'round_{i:02d}.md'}")
        print(head)
        if len(md) > args.head_chars:
            print("...")
        print()

    u = len(set(hashes))
    n = len(hashes)
    if u == 1:
        tag = f"{n} 轮全文完全一致"
    elif u == n:
        tag = "各轮全文均不同"
    else:
        tag = "部分轮次撞全文、部分不同"
    print("SUMMARY:", f"不同全文数 = {u} / {n}", f"-> {tag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
