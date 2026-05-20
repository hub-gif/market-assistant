"""
策略润色 LLM 调用前：按与 ``chat_completion_text`` 一致的 **budgeted** token 估算（``LLM_INPUT_TOKEN_BUDGET_PAD``）
压缩上下文（矩阵 SKU、报告摘录、细类证据 MD）。

默认开启（``MA_STRATEGY_CONTEXT_COMPRESS``）；关闭时 ``maybe_compress_strategy_llm_context`` 原样返回。

晚导入 ``generate_strategy``，避免循环依赖；由 ``generate_strategy_draft_markdown_llm`` 在解析快照前调用。
"""
from __future__ import annotations

import os
from typing import Any


def _compress_enabled() -> bool:
    v = (os.environ.get("MA_STRATEGY_CONTEXT_COMPRESS") or "1").strip().lower()
    return v not in ("0", "false", "off", "no")


def strategy_llm_context_compress_enabled() -> bool:
    """
    策略稿 LLM 调用前是否允许按 ``MA_STRATEGY_CONTEXT_COMPRESS`` 做上下文压缩（默认开启）。
    未超启发式目标时不会对 inputs 裁剪，但本函数仍为 True。置 ``MA_STRATEGY_CONTEXT_COMPRESS=0`` 关闭。
    """
    return _compress_enabled()


def _token_slack() -> int:
    raw = (os.environ.get("MA_STRATEGY_CONTEXT_TOKEN_SLACK") or "2048").strip()
    try:
        return max(256, int(raw))
    except ValueError:
        return 2048


def _input_token_target_est() -> int:
    """
    ``budgeted_chat_input_tokens(STRATEGY_SYSTEM, user)`` 的上限目标（含 ``LLM_INPUT_TOKEN_BUDGET_PAD``）。
    未设置 ``MA_STRATEGY_INPUT_TOKEN_TARGET_EST`` 时：``context_window - min_completion - slack``。
    """
    raw = (os.environ.get("MA_STRATEGY_INPUT_TOKEN_TARGET_EST") or "").strip()
    if raw:
        try:
            return max(4096, int(raw))
        except ValueError:
            pass
    from .generate_strategy import _min_strategy_completion_tokens
    from .llm_client import llm_context_window_size

    ctx = llm_context_window_size()
    min_comp = _min_strategy_completion_tokens()
    return max(8000, ctx - min_comp - _token_slack())


def _truncate_md(s: str | None, max_chars: int) -> str | None:
    if not s or not str(s).strip():
        return s
    t = str(s).strip()
    if len(t) <= max_chars:
        return t
    cut = max(0, max_chars - 120)
    return t[:cut].rstrip() + "\n\n…（摘录已压缩以适配上下文，请勿编造截断后内容。）\n"


def _measure_est(
    *,
    brief: dict[str, Any],
    report_strategy_excerpt: str | None,
    report_matrix_group_evidence_md: str | None,
    job_id: int,
    keyword: str,
    business_notes: str,
    our_product_profile: str = "",
    generated_at_iso: str,
    strategy_decisions: dict[str, Any],
    report_config: dict[str, Any] | None,
) -> int:
    from pipeline.openai_gateway.estimate import budgeted_chat_input_tokens

    from .generate_strategy import STRATEGY_SYSTEM, resolve_strategy_draft_llm_input_snapshot

    _p, user, _tier = resolve_strategy_draft_llm_input_snapshot(
        job_id=job_id,
        keyword=keyword,
        brief=brief,
        business_notes=business_notes,
        our_product_profile=our_product_profile,
        generated_at_iso=generated_at_iso,
        strategy_decisions=strategy_decisions,
        report_strategy_excerpt=report_strategy_excerpt,
        report_matrix_group_evidence_md=report_matrix_group_evidence_md,
        report_config=report_config,
    )
    return budgeted_chat_input_tokens(STRATEGY_SYSTEM, user)


def maybe_compress_strategy_llm_context(
    *,
    brief: dict[str, Any],
    job_id: int,
    keyword: str,
    business_notes: str,
    our_product_profile: str = "",
    generated_at_iso: str,
    strategy_decisions: dict[str, Any],
    report_strategy_excerpt: str | None,
    report_matrix_group_evidence_md: str | None,
    report_config: dict[str, Any] | None,
) -> tuple[dict[str, Any], str | None, str | None, str]:
    """
    若启发式输入 token 超过目标，则依次为：
    1) 单组矩阵时二分截断 ``skus`` 数量（尽量少砍）；
    2) 缩短 ``report_matrix_group_evidence_md``、``report_strategy_excerpt``。

    返回 ``(brief, excerpt, evidence_md, note)``；``note`` 为空表示未压缩。
    """
    if not _compress_enabled():
        return brief, report_strategy_excerpt, report_matrix_group_evidence_md, ""

    target = _input_token_target_est()
    kwargs = dict(
        job_id=job_id,
        keyword=keyword,
        business_notes=business_notes,
        our_product_profile=our_product_profile,
        generated_at_iso=generated_at_iso,
        strategy_decisions=strategy_decisions,
        report_config=report_config,
    )

    est0 = _measure_est(
        brief=brief,
        report_strategy_excerpt=report_strategy_excerpt,
        report_matrix_group_evidence_md=report_matrix_group_evidence_md,
        **kwargs,
    )
    if est0 <= target:
        return brief, report_strategy_excerpt, report_matrix_group_evidence_md, ""

    notes: list[str] = []
    from pipeline.reporting.brief_strategy_scope import trim_matrix_group_skus_for_llm

    b_work = brief
    ex = report_strategy_excerpt
    ev = report_matrix_group_evidence_md

    mg = brief.get("matrix_by_group")
    if isinstance(mg, list) and len(mg) == 1 and isinstance(mg[0], dict):
        skus = mg[0].get("skus")
        if isinstance(skus, list) and len(skus) > 1:
            full_n = len(skus)
            lo, hi = 1, full_n
            best = 0
            while lo <= hi:
                mid = (lo + hi) // 2
                b_try = trim_matrix_group_skus_for_llm(brief, mid)
                e_try = _measure_est(
                    brief=b_try,
                    report_strategy_excerpt=ex,
                    report_matrix_group_evidence_md=ev,
                    **kwargs,
                )
                if e_try <= target:
                    best = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            if best > 0:
                b_work = trim_matrix_group_skus_for_llm(brief, best)
                if best < full_n:
                    notes.append(
                        f"策略 LLM：矩阵 SKU 自 {full_n} 压缩至 {best}（启发式输入 token 目标≤{target}）"
                    )
            else:
                b_work = trim_matrix_group_skus_for_llm(brief, 1)
                notes.append(
                    "策略 LLM：矩阵 SKU 暂压至 1 条（启发式仍超窗时将再压摘录）"
                )

    est = _measure_est(
        brief=b_work,
        report_strategy_excerpt=ex,
        report_matrix_group_evidence_md=ev,
        **kwargs,
    )
    rounds = 0
    did_ev = False
    did_ex = False
    while est > target and rounds < 32:
        rounds += 1
        ev_len = len(ev or "")
        ex_len = len(ex or "")
        stepped = False
        if ev_len > 500:
            nev = _truncate_md(ev, max(400, int(ev_len * 0.72)))
            if nev != ev:
                ev = nev
                did_ev = True
                stepped = True
        elif ex_len > 200:
            nex = _truncate_md(ex, max(120, int(ex_len * 0.72)))
            if nex != ex:
                ex = nex
                did_ex = True
                stepped = True
        if not stepped:
            break
        est = _measure_est(
            brief=b_work,
            report_strategy_excerpt=ex,
            report_matrix_group_evidence_md=ev,
            **kwargs,
        )

    if did_ev:
        notes.append("策略 LLM：已压缩细类报告证据摘录")
    if did_ex:
        notes.append("策略 LLM：已压缩 report_strategy_excerpt")

    note = "；".join(notes) if notes else ""
    if est > target and note:
        note += f"（压缩后启发式 est={est}，仍高于目标 {target}；网关若更严请调 MA_STRATEGY_INPUT_TOKEN_TARGET_EST）"
    return b_work, ex, ev, note


__all__ = ["maybe_compress_strategy_llm_context", "strategy_llm_context_compress_enabled"]
