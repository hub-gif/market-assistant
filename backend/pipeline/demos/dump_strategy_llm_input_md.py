"""
导出与「策略大模型润色」一次调用一致的完整输入，生成 Markdown（供对照）。

用法（在项目 backend 目录）::

  python -m pipeline.demos.dump_strategy_llm_input_md [--job-id 12] [--matrix-index 0]

若未指定 --matrix-index，则默认收窄到第一个矩阵分组（与「选第一个细类」等效）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "market_assistant.settings")


def _strategy_decisions_empty() -> dict:
    return {
        "product_role": "",
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


def main() -> int:
    import django

    django.setup()

    from django.utils import timezone

    from pipeline.jd.runner import build_competitor_brief_for_job
    from pipeline.llm.generate_strategy import (
        STRATEGY_SYSTEM,
        STRATEGY_USER_PREFIX,
        _omit_ch8_probe_wordchart_fields,
        _truncate_strategy_narrative,
    )
    from pipeline.models import JobStatus, PipelineJob
    from pipeline.reporting.brief_compact import compact_brief_for_llm
    from pipeline.reporting.brief_strategy_scope import (
        filter_brief_for_strategy_matrix_group,
        list_matrix_groups_for_api,
    )
    from pipeline.reporting.report_matrix_group_evidence import (
        load_report_matrix_group_evidence_markdown,
    )
    from pipeline.reporting.report_strategy_excerpt import load_report_strategy_excerpt
    from pipeline.reporting.strategy_draft import (
        build_strategy_draft_markdown,
        filter_strategy_hints_for_ch8_probe,
        report_uses_chapter8_text_mining_probe,
    )

    p = argparse.ArgumentParser()
    p.add_argument("--job-id", type=int, default=None)
    p.add_argument(
        "--matrix-index",
        type=int,
        default=0,
        help="矩阵分组下标；设为 -1 表示不收窄（全部分类）",
    )
    p.add_argument(
        "--no-scope",
        action="store_true",
        help="与 --matrix-index -1 相同：不收窄 brief、不抽细类报告节选",
    )
    p.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="输出 .md 路径（默认：docs/planning/策略生成-LLM全量输入快照.md）",
    )
    args = p.parse_args()

    backend_dir = Path(__file__).resolve().parents[2]
    repo_root = backend_dir.parent
    default_out = (
        repo_root / "docs" / "planning" / "策略生成-LLM全量输入快照.md"
    )
    out_path = Path(args.output) if args.output else default_out

    job_id = args.job_id
    if job_id:
        job = PipelineJob.objects.filter(pk=job_id).first()
    else:
        job = (
            PipelineJob.objects.filter(status=JobStatus.SUCCESS)
            .exclude(run_dir="")
            .order_by("-id")
            .first()
        )
    if not job:
        print("无可用成功任务（需 run_dir 非空）", file=sys.stderr)
        return 1

    rc = job.report_config if isinstance(job.report_config, dict) else None
    brief = build_competitor_brief_for_job(
        job.run_dir,
        job.keyword,
        report_config=rc,
    )
    matrix_groups = list_matrix_groups_for_api(brief)
    group_names = [g.get("group") for g in matrix_groups if isinstance(g, dict)]
    scoped_label = ""
    matrix_index: int | None = args.matrix_index
    if args.no_scope:
        matrix_index = -1
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

    gen_at = timezone.now().isoformat()
    sd = _strategy_decisions_empty()
    rules_md = build_strategy_draft_markdown(
        job_id=job.id,
        keyword=job.keyword,
        brief=brief,
        business_notes="",
        generated_at_iso=gen_at,
        strategy_decisions=sd,
        report_config=rc,
    )

    excerpt_raw, excerpt_src = load_report_strategy_excerpt(job.run_dir)
    excerpt_raw = (excerpt_raw or "").strip()

    evidence_md = ""
    evidence_src = "none"
    if scoped_label:
        evidence_md, evidence_src = load_report_matrix_group_evidence_markdown(
            job.run_dir,
            scoped_label,
        )

    compact_max = 80_000
    excerpt_max = 24_000
    compact = compact_brief_for_llm(brief, max_chars=compact_max)
    if report_uses_chapter8_text_mining_probe(rc):
        compact = dict(compact)
        _omit_ch8_probe_wordchart_fields(compact)
        if isinstance(compact.get("strategy_hints"), list):
            compact["strategy_hints"] = filter_strategy_hints_for_ch8_probe(
                compact["strategy_hints"]
            )
    ex = (
        _truncate_strategy_narrative(excerpt_raw, excerpt_max)
        if excerpt_raw
        else ""
    )
    ev_max = min(24_000, max(3_000, excerpt_max + excerpt_max // 2))
    gm = (
        _truncate_strategy_narrative(evidence_md.strip(), ev_max)
        if evidence_md
        else ""
    )

    payload: dict = {
        "job_id": job.id,
        "keyword": job.keyword,
        "generated_at_iso": gen_at,
        "strategy_decisions": sd,
        "business_notes": "",
        "structured_brief": compact,
        "rules_draft_markdown": rules_md,
        "report_strategy_excerpt": ex,
        "report_matrix_group_evidence_md": gm,
        "chapter8_text_mining_probe": bool(
            report_uses_chapter8_text_mining_probe(rc)
        ),
    }
    if report_uses_chapter8_text_mining_probe(rc):
        payload["structured_brief_omission_note"] = (
            "已启用第八章文本挖掘（探针为主）：structured_brief 已省略顶层「关注词/场景子串计数」、按细类 feedback 中的 focus_keyword_hits/scenarios_top，"
            "以及「与条形图同源的 strategy_hints 句子」，避免与报告 §8 主口径冲突。**不得**再以这类子串计数或预设场景占比作为论据。"
            "用户与评论侧须依报告 §8 文本挖掘归纳及 `report_matrix_group_evidence_md`；**促销、满减、券价差**须与报告第六章、`price_promotion_signals` 及下方 `report_strategy_excerpt`（第九章）对齐，不得省略报告已写明的活动建议。"
        )

    user_body = STRATEGY_USER_PREFIX + json.dumps(payload, ensure_ascii=False)
    full_chars = len(STRATEGY_SYSTEM) + len(user_body)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "# 策略生成 · 大模型一次调用的「全量输入」快照",
        "",
        "> **生成方式**：本机 `pipeline.demos.dump_strategy_llm_input_md` 按与 "
        "`generate_strategy_draft_markdown_llm` 相同的 payload 组装逻辑导出。",
        "> **与线上一致性**：与真实接口相比，表单字段此处均为空默认；"
        "你只要把当时提交的 `strategy_decisions` / `business_notes` 代入即与线上等价。",
        "",
        "## 快照元数据",
        "",
        f"- **任务 ID**：{job.id}",
        f"- **关键词**：{job.keyword}",
        f"- **run_dir**：`{job.run_dir}`",
        f"- **矩阵分组**：{matrix_index if matrix_index is not None and matrix_index >= 0 else '未收窄（全部分类）'}{f' → 「{scoped_label}」' if scoped_label else ''}",
        f"- **本任务可选细类（节选）**：{group_names[:20]}{'…' if len(group_names) > 20 else ''}",
        f"- **第九章节选来源**：{excerpt_src}，约 {len(ex)} 字符",
        f"- **细类报告节选来源**：{evidence_src}，约 {len(gm)} 字符",
        f"- **System 字符数**：{len(STRATEGY_SYSTEM)}",
        f"- **User 消息字符数**：{len(user_body)}",
        f"- **合计约**：{full_chars} 字符",
        "",
        "---",
        "",
        "## 1. System 提示词（完整 `STRATEGY_SYSTEM`）",
        "",
        "```text",
        STRATEGY_SYSTEM,
        "```",
        "",
        "---",
        "",
        "## 2. User 消息（完整：`STRATEGY_USER_PREFIX` + JSON）",
        "",
        "以下为网关 **user** 角色一次发送的完整字符串（前缀 + 单行 JSON）。",
        "",
        "```text",
        user_body,
        "```",
        "",
        "---",
        "",
        "## 3. 同上 JSON 的排版版（便于人眼查看 `structured_brief` 结构）",
        "",
        "说明：若与第 2 节有任何不一致，以第 2 节（真实入参）为准。",
        "",
        "```json",
        json.dumps(payload, ensure_ascii=False, indent=2),
        "```",
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_path} ({out_path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
