"""
导出「独立策略稿 · 大模型润色」一次调用与生产一致的完整入参（不请求网关）。

与 ``generate_strategy_draft_markdown_llm`` / ``resolve_strategy_draft_llm_input_snapshot``
使用相同的截断阶梯与 ``payload`` 字段（含 ``strategy_decisions_substantive``）。

用法（在 backend 目录）::

  # 按数据库任务（默认取最近成功任务；可指定 job-id）
  python -m pipeline.demos.dump_strategy_llm_input_md [--job-id 12] [--matrix-index 0]

  # 仅磁盘 run_dir（无需 PipelineJob；job_id 写 0 进 JSON，仅影响底稿抬头占位）
  python -m pipeline.demos.dump_strategy_llm_input_md --run-dir \"D:/.../pipeline_runs/某批次\"

  # 与线上一致：从文件载入当时提交的 strategy_decisions
  python -m pipeline.demos.dump_strategy_llm_input_md --run-dir \"...\" --decisions-json decisions.json

  # 指定输出
  python -m pipeline.demos.dump_strategy_llm_input_md --run-dir \"...\" -o path/to/snap.md

  # 只打印摘要、不写文件
  python -m pipeline.demos.dump_strategy_llm_input_md --run-dir \"...\" --no-md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


def _strategy_decisions_empty() -> dict[str, Any]:
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


def _merge_decisions(base: dict[str, Any], overlay: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(base)
    if isinstance(overlay, dict):
        out.update(overlay)
    return out


def main() -> int:
    import django

    django.setup()

    from django.utils import timezone

    from pipeline.jd.runner import build_competitor_brief_for_job
    from pipeline.llm.generate_strategy import (
        STRATEGY_SYSTEM,
        resolve_strategy_draft_llm_input_snapshot,
        _min_strategy_completion_tokens,
    )
    from pipeline.llm.llm_client import estimate_chat_input_tokens
    from pipeline.models import JobStatus, PipelineJob
    from pipeline.reporting.brief_strategy_scope import (
        filter_brief_for_strategy_matrix_group,
        list_matrix_groups_for_api,
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
        help="运行目录（与 job.run_dir 相同结构；与 --job-id 二选一）",
    )
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
        "--decisions-json",
        type=Path,
        default=None,
        help="覆盖 strategy_decisions 的 JSON 对象文件（与线上一致时传入）",
    )
    p.add_argument(
        "--business-notes",
        type=str,
        default="",
        help="与接口 business_notes 一致的业务备注",
    )
    p.add_argument(
        "--snapshot-job-id",
        type=int,
        default=None,
        help="写入 payload.job_id（仅 --run-dir 时有效；默认 0）",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="输出 .md 路径（默认：run_dir/strategy_draft_llm_input_snapshot.md 或 docs/planning/…）",
    )
    p.add_argument(
        "--no-md",
        action="store_true",
        help="不写入 Markdown，仅打印控制台摘要",
    )
    args = p.parse_args()

    backend_dir = Path(__file__).resolve().parents[2]
    repo_root = backend_dir.parent
    default_docs_out = (
        repo_root / "docs" / "planning" / "策略生成-LLM全量输入快照.md"
    )

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
    if args.decisions_json is not None:
        dp = args.decisions_json.expanduser().resolve()
        if not dp.is_file():
            print(f"decisions-json 不存在: {dp}", file=sys.stderr)
            return 1
        try:
            loaded = json.loads(dp.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"decisions-json 非合法 JSON: {e}", file=sys.stderr)
            return 1
        if not isinstance(loaded, dict):
            print("decisions-json 根须为 JSON 对象", file=sys.stderr)
            return 1
        sd = _merge_decisions(sd, loaded)

    excerpt_raw, excerpt_src = load_report_strategy_excerpt(run_dir_s)
    excerpt_raw = (excerpt_raw or "").strip()

    evidence_md = ""
    evidence_src = "none"
    if scoped_label:
        evidence_md, evidence_src = load_report_matrix_group_evidence_markdown(
            run_dir_s,
            scoped_label,
        )

    payload, user_body, tier_note = resolve_strategy_draft_llm_input_snapshot(
        job_id=job_id,
        keyword=kw,
        brief=brief,
        business_notes=(args.business_notes or "").strip(),
        generated_at_iso=gen_at,
        strategy_decisions=sd,
        report_strategy_excerpt=excerpt_raw or None,
        report_matrix_group_evidence_md=evidence_md.strip() or None,
        report_config=rc,
    )

    min_comp = _min_strategy_completion_tokens()
    est_in = estimate_chat_input_tokens(STRATEGY_SYSTEM, user_body)
    full_chars = len(STRATEGY_SYSTEM) + len(user_body)
    rd = payload.get("rules_draft_markdown")
    rd_len = len(rd) if isinstance(rd, str) else 0
    sb = payload.get("structured_brief")
    sb_json_len = len(json.dumps(sb, ensure_ascii=False)) if sb else 0

    print("run_dir:", run_dir_s)
    print("job_id (payload):", job_id)
    print("keyword:", kw)
    print("tier:", tier_note)
    print("MA_STRATEGY_MIN_COMPLETION_TOKENS:", min_comp)
    print("strategy_decisions_substantive:", payload.get("strategy_decisions_substantive"))
    print("matrix scope:", f"{matrix_index} → 「{scoped_label}」" if scoped_label else "未收窄")
    print("report_strategy_excerpt:", excerpt_src, "raw chars:", len(excerpt_raw))
    print("report_matrix_group_evidence:", evidence_src, "chars in payload:", len(payload.get("report_matrix_group_evidence_md") or ""))
    print("STRATEGY_SYSTEM chars:", len(STRATEGY_SYSTEM))
    print("user chars:", len(user_body))
    print("total chars:", full_chars)
    print("estimate_chat_input_tokens:", est_in)
    print("structured_brief JSON len:", sb_json_len)
    print("rules_draft_markdown len (in payload):", rd_len)

    if args.no_md:
        return 0

    if args.output is not None:
        out_path = args.output.expanduser().resolve()
    elif args.run_dir is not None:
        out_path = args.run_dir.expanduser().resolve() / "strategy_draft_llm_input_snapshot.md"
    else:
        out_path = default_docs_out

    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# 独立策略稿 · 大模型一次调用的「全量输入」快照",
        "",
        "> **生成方式**：`pipeline.demos.dump_strategy_llm_input_md` 调用 "
        "`resolve_strategy_draft_llm_input_snapshot`，与 ``generate_strategy_draft_markdown_llm`` "
        "首档通过的 ``payload`` / ``user`` 一致（不请求网关）。",
        "> **与线上一致**：将当时 POST 的 `strategy_decisions`、`business_notes`、`strategy_matrix_group_index` "
        "与本脚本参数对齐即可复现。",
        "",
        "## 快照元数据",
        "",
        f"- **任务 ID（payload.job_id）**：{job_id}",
        f"- **关键词**：{kw}",
        f"- **run_dir**：`{run_dir_s}`",
        f"- **矩阵分组**：{matrix_index if matrix_index is not None and matrix_index >= 0 else '未收窄（全部分类）'}{f' → 「{scoped_label}」' if scoped_label else ''}",
        f"- **本任务可选细类（节选）**：{group_names[:20]}{'…' if len(group_names) > 20 else ''}",
        f"- **选用档位**：{tier_note}",
        f"- **strategy_decisions_substantive**：{payload.get('strategy_decisions_substantive')!r}",
        f"- **第九章节选来源**：{excerpt_src}",
        f"- **细类报告节选来源**：{evidence_src}",
        f"- **MA_STRATEGY_MIN_COMPLETION_TOKENS**：{min_comp}",
        f"- **System 字符数**：{len(STRATEGY_SYSTEM)}",
        f"- **User 消息字符数**：{len(user_body)}",
        f"- **合计约**：{full_chars} 字符",
        f"- **estimate_chat_input_tokens（项目内启发式）**：{est_in}",
        f"- **structured_brief 序列化长度**：{sb_json_len}",
        f"- **rules_draft_markdown（payload 内）字符数**：{rd_len}",
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
