"""
复现「第九章 · 策略与机会」一次 LLM 调用的输入（不请求网关）。

从指定 ``run_dir`` 读取 ``effective_report_config.json``、``run_meta.json``、
``competitor_analysis.md``（切片第五～六章大模型归纳）、``chapter8_text_mining_probe.md``
（与 runner 同源 ``markdown_embed_body_for_competitor_report``），再按
``generate_strategy_opportunities_llm`` 的截断阶梯求首个可通过
``_strategy_prompt_ok_for_call`` 的档位。

可将**网关实际收到的** ``system`` 与 ``user``（user = 前缀 + 单行 JSON）写入 Markdown。

用法（在 backend 目录）::

  python -m pipeline.demos.dump_strategy_opportunities_llm_volume --run-dir \".../某批次\"

  # 指定输出路径
  python -m pipeline.demos.dump_strategy_opportunities_llm_volume --run-dir \"...\" -o path/to/snap.md

  # 只打印体积、不写文件
  python -m pipeline.demos.dump_strategy_opportunities_llm_volume --run-dir \"...\" --no-md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "market_assistant.settings")


def _slice_between(md: str, start: str, end: str) -> str:
    i = md.find(start)
    if i < 0:
        return ""
    j = md.find(end, i + len(start))
    if j < 0:
        return md[i:].strip()
    return md[i:j].strip()


def _resolve_first_ok_payload(
    *,
    brief: dict[str, Any],
    kw: str,
    narr_in: dict[str, str],
    STRATEGY_OPPORTUNITIES_SYSTEM: str,
    STRATEGY_OPPORTUNITIES_USER_PREFIX: str,
    compact_brief_for_llm: Any,
    _truncate_strategy_narrative: Any,
    _strategy_prompt_ok_for_call: Any,
    _min_strategy_completion_tokens: Any,
) -> tuple[
    dict[str, Any],
    str,
    str,
    int,
    int,
    dict[str, int],
    bool,
]:
    """
    返回: payload, user, tier_note, cap_brief, cap_narr, narrative_lens, used_narratives
    """
    min_comp = _min_strategy_completion_tokens()
    min_relaxed = max(256, min_comp // 2)

    tiers = (
        (48_000, 2_800),
        (42_000, 2_200),
        (36_000, 1_700),
        (30_000, 1_300),
        (26_000, 950),
        (22_000, 700),
        (18_000, 500),
        (16_000, 400),
        (14_000, 320),
        (12_000, 260),
        (10_000, 200),
    )

    for cap_brief, cap_narr in tiers:
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        narratives = {
            k: _truncate_strategy_narrative(v, cap_narr) for k, v in narr_in.items()
        }
        payload: dict[str, Any] = {"keyword": kw, "competitor_brief": compact}
        if narratives:
            payload["prior_chapter_llm_narratives"] = narratives
        user = STRATEGY_OPPORTUNITIES_USER_PREFIX + json.dumps(
            payload, ensure_ascii=False
        )
        if _strategy_prompt_ok_for_call(
            STRATEGY_OPPORTUNITIES_SYSTEM, user, min_completion_tokens=min_comp
        ):
            lens = {k: len(v) for k, v in narratives.items()}
            note = (
                f"与 ``generate_strategy_opportunities_llm`` 一致的首档："
                f"`compact_brief` max_chars={cap_brief}，"
                f"`prior_chapter_llm_narratives` 每键截断上限 {cap_narr} 字。"
            )
            return payload, user, note, cap_brief, cap_narr, lens, True

    for cap_brief in (40_000, 32_000, 26_000, 20_000, 16_000, 14_000, 12_000, 10_000):
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        payload = {"keyword": kw, "competitor_brief": compact}
        user = STRATEGY_OPPORTUNITIES_USER_PREFIX + json.dumps(
            payload, ensure_ascii=False
        )
        if _strategy_prompt_ok_for_call(
            STRATEGY_OPPORTUNITIES_SYSTEM, user, min_completion_tokens=min_comp
        ):
            note = (
                "叙事整体过长，已退化为**仅** ``keyword`` + ``competitor_brief``（无 "
                "``prior_chapter_llm_narratives``），"
                f"`max_chars={cap_brief}`。"
            )
            return payload, user, note, cap_brief, 0, {}, False

    for cap_brief in (14_000, 12_000, 10_000, 8_000):
        compact = compact_brief_for_llm(brief, max_chars=cap_brief)
        payload = {"keyword": kw, "competitor_brief": compact}
        user = STRATEGY_OPPORTUNITIES_USER_PREFIX + json.dumps(
            payload, ensure_ascii=False
        )
        if _strategy_prompt_ok_for_call(
            STRATEGY_OPPORTUNITIES_SYSTEM, user, min_completion_tokens=min_relaxed
        ):
            note = (
                "叙事与长 brief 均超出预算，已使用 **relaxed** completion 阈值下的仅 brief 档，"
                f"`max_chars={cap_brief}`。"
            )
            return payload, user, note, cap_brief, 0, {}, False

    cap_brief = 8_000
    compact = compact_brief_for_llm(brief, max_chars=cap_brief)
    payload = {"keyword": kw, "competitor_brief": compact}
    user = STRATEGY_OPPORTUNITIES_USER_PREFIX + json.dumps(payload, ensure_ascii=False)
    note = (
        "所有 ``_strategy_prompt_ok_for_call`` 档位均未通过，与生产代码一致时将仍组装该 user "
        "并调用 ``call_llm``（可能由网关或客户端再报错）。"
        f"`max_chars={cap_brief}`，无叙事。"
    )
    return payload, user, note, cap_brief, 0, {}, False


def main() -> int:
    import django

    django.setup()

    from pipeline.demos.chapter8_text_mining_probe import (
        markdown_embed_body_for_competitor_report,
    )
    from pipeline.jd.runner import build_competitor_brief_for_job
    from pipeline.llm.generate_strategy import (
        STRATEGY_OPPORTUNITIES_SYSTEM,
        STRATEGY_OPPORTUNITIES_USER_PREFIX,
        _min_strategy_completion_tokens,
        _strategy_prompt_ok_for_call,
        _truncate_strategy_narrative,
    )
    from pipeline.llm.llm_client import estimate_chat_input_tokens
    from pipeline.reporting.brief_compact import compact_brief_for_llm

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="pipeline 运行目录（含 competitor_analysis.md 等）",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="输出 Markdown 路径（默认：run_dir/strategy_opportunities_llm_input_snapshot.md）",
    )
    ap.add_argument(
        "--no-md",
        action="store_true",
        help="不写入 Markdown，仅打印控制台摘要",
    )
    args = ap.parse_args()
    run_dir = args.run_dir.expanduser().resolve()
    if not run_dir.is_dir():
        print(f"run_dir 不存在: {run_dir}", file=sys.stderr)
        return 1

    rc_path = run_dir / "effective_report_config.json"
    meta_path = run_dir / "run_meta.json"
    if not rc_path.is_file() or not meta_path.is_file():
        print("缺少 effective_report_config.json 或 run_meta.json", file=sys.stderr)
        return 1

    rc = json.loads(rc_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    kw = (meta.get("keyword") or "").strip()
    if not kw:
        print("run_meta 无 keyword", file=sys.stderr)
        return 1

    brief = build_competitor_brief_for_job(str(run_dir), kw, report_config=rc)

    md_path = run_dir / "competitor_analysis.md"
    if not md_path.is_file():
        print(f"缺少 {md_path.name}", file=sys.stderr)
        return 1
    md = md_path.read_text(encoding="utf-8")

    matrix = _slice_between(
        md,
        "#### 细类要点归纳（大模型",
        "\n---\n\n## 六、价格分析",
    )
    price = _slice_between(
        md,
        "#### 细类价盘要点归纳（大模型",
        "\n\n#### 细类促销与活动要点归纳（大模型",
    )
    promo = _slice_between(
        md,
        "#### 细类促销与活动要点归纳（大模型",
        "\n---\n\n## 八、消费者反馈与用户画像",
    )

    probe_path = run_dir / "chapter8_text_mining_probe.md"
    ch8_embed = ""
    if probe_path.is_file():
        ch8_embed = markdown_embed_body_for_competitor_report(
            probe_path.read_text(encoding="utf-8")
        )

    narr_in: dict[str, str] = {}
    if matrix.strip():
        narr_in["sec5_matrix_group_summaries"] = matrix.strip()
    if price.strip():
        narr_in["sec6_price_group_summaries"] = price.strip()
    if promo.strip():
        narr_in["sec6_promo_group_summaries"] = promo.strip()
    if ch8_embed.strip():
        narr_in["sec8_3_text_mining_probe"] = ch8_embed.strip()

    payload, user, tier_note, cap_brief, cap_narr, narrative_lens, used_narr = (
        _resolve_first_ok_payload(
            brief=brief,
            kw=kw,
            narr_in=narr_in,
            STRATEGY_OPPORTUNITIES_SYSTEM=STRATEGY_OPPORTUNITIES_SYSTEM,
            STRATEGY_OPPORTUNITIES_USER_PREFIX=STRATEGY_OPPORTUNITIES_USER_PREFIX,
            compact_brief_for_llm=compact_brief_for_llm,
            _truncate_strategy_narrative=_truncate_strategy_narrative,
            _strategy_prompt_ok_for_call=_strategy_prompt_ok_for_call,
            _min_strategy_completion_tokens=_min_strategy_completion_tokens,
        )
    )

    sys_prompt = STRATEGY_OPPORTUNITIES_SYSTEM
    sys_len = len(sys_prompt)
    user_len = len(user)
    est_in = estimate_chat_input_tokens(sys_prompt, user)
    min_comp = _min_strategy_completion_tokens()

    print("run_dir:", run_dir)
    print("keyword:", kw)
    print("narrative keys (source):", sorted(narr_in.keys()))
    for k, v in narr_in.items():
        print(f"  raw {k}: {len(v)} chars")
    print("STRATEGY_OPPORTUNITIES_SYSTEM chars:", sys_len)
    print("user chars:", user_len)
    print("system + user chars:", sys_len + user_len)
    print("estimate_chat_input_tokens (internal heuristic):", est_in)
    print("tier:", tier_note)
    if narrative_lens:
        print("narrative lens (after truncate):", narrative_lens)
    print("used prior_chapter_llm_narratives:", used_narr)

    if args.no_md:
        return 0

    out_path = args.output
    if out_path is None:
        out_path = run_dir / "strategy_opportunities_llm_input_snapshot.md"
    else:
        out_path = out_path.expanduser().resolve()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    compact = payload.get("competitor_brief")
    compact_json_len = len(json.dumps(compact, ensure_ascii=False)) if compact else 0

    lines: list[str] = [
        "# 第九章 · 策略与机会 · 大模型真实入参快照",
        "",
        "> **说明**：与一次 ``call_llm(STRATEGY_OPPORTUNITIES_SYSTEM, user)`` 一致。"
        "``user`` = ``STRATEGY_OPPORTUNITIES_USER_PREFIX`` + **单行** ``json.dumps(payload)``（与生产相同，非排版版）。",
        "",
        "## 元数据",
        "",
        f"- **run_dir**：`{run_dir}`",
        f"- **keyword**：{kw}",
        f"- **effective_report_config.llm_strategy_opportunities**：{rc.get('llm_strategy_opportunities')!r}（本快照仍按若开启第九章 LLM 时的输入还原）",
        f"- **MA_STRATEGY_MIN_COMPLETION_TOKENS**：{min_comp}",
        f"- **选用档位说明**：{tier_note}",
        f"- **叙事是否进入 payload**：{'是' if used_narr else '否'}",
        f"- **System 字符数**：{sys_len}",
        f"- **User 字符数**：{user_len}",
        f"- **合计字符数**：{sys_len + user_len}",
        f"- **estimate_chat_input_tokens（项目内启发式）**：{est_in}",
        f"- **competitor_brief 序列化长度**：{compact_json_len}",
        "",
        "---",
        "",
        "## 1. System 消息（完整，角色 system）",
        "",
        "```text",
        sys_prompt,
        "```",
        "",
        "---",
        "",
        "## 2. User 消息（完整，角色 user）",
        "",
        "以下为网关收到的 **整段** user 字符串（前缀 + 单行 JSON）。",
        "",
        "```text",
        user,
        "```",
        "",
        "---",
        "",
        "## 3. 同上 JSON 的排版版（便于阅读；以第 2 节为准）",
        "",
        "```json",
        json.dumps(payload, ensure_ascii=False, indent=2),
        "```",
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")
    kb = out_path.stat().st_size // 1024
    print(f"Wrote {out_path} ({kb} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
