"""
矩阵 / 促销 两组 LLM 的「读超时」多为网关侧：单次请求在 ``LLM_CHAT_TIMEOUT``（默认 600s）
内未返回完整响应即失败。默认配置下 ``llm_group_summaries_chunk_by_matrix=True``，
会为**每个细类矩阵分组各发 1 次** ``chat/completions``，串行执行；任一次超时都会导致
对应的 ``matrix_groups_llm.json`` / ``promo_groups_llm.json`` 记录 error。

本模块**不修改流水线**，仅用于本地用真实合并表做载荷统计或可选冒烟调用。

用法（在 ``backend`` 目录、已配置 Django / .env）::

    # 仅统计每个分块请求的 JSON 字符数与粗算输入 token（不调网关）
    set MA_TEST_REAL_RUN_DIR=D:\\...\\pipeline_runs\\20260413_104252_低GI
    python -m pytest pipeline/tests/test_matrix_promo_llm_real_run_dir.py -v -s

    # 额外对「第一个矩阵分组」发 1 次真实请求（需 OPENAI_* / LLM_* 可用）
    set MA_TEST_REAL_LLM=1
    python -m pytest pipeline/tests/test_matrix_promo_llm_real_run_dir.py::test_first_matrix_group_llm_smoke_optional -v -s
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.competitor_report import jd_report as jcr
from pipeline.competitor_report.llm_group_payloads import (
    build_matrix_groups_llm_payload,
    build_promo_groups_llm_payload,
)
from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER


def _real_run_dir() -> Path | None:
    raw = (os.environ.get("MA_TEST_REAL_RUN_DIR") or "").strip()
    if not raw:
        return None
    p = Path(raw).expanduser().resolve()
    return p if p.is_dir() else None


def _load_merged_rows(run_dir: Path) -> list[dict[str, str]]:
    merged = run_dir / "keyword_pipeline_merged.csv"
    if not merged.is_file():
        pytest.skip(f"无合并表: {merged}")
    _, rows = jcr._read_csv_rows(merged)
    return rows


def test_matrix_promo_chunk_payload_metrics_from_real_run_dir() -> None:
    """
    对真实 run_dir：输出矩阵 / 促销 **按细类分块** 时每一次请求的用户 JSON 规模与粗算 tokens。
    用于对照 ``AI_crawler.chat_completion_text`` 的读超时（与输入长、输出 max_tokens 上限、网关排队均相关）。
    """
    rd = _real_run_dir()
    if rd is None:
        pytest.skip("请设置环境变量 MA_TEST_REAL_RUN_DIR 为流水线目录（含 keyword_pipeline_merged.csv）")

    rows = _load_merged_rows(rd)
    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    kw = (os.environ.get("MA_TEST_REAL_KEYWORD") or "低GI").strip() or "低GI"

    pl_mx = build_matrix_groups_llm_payload(rows, sku_header=sku_h, title_h=title_h)
    pl_po = build_promo_groups_llm_payload(rows, sku_header=sku_h, title_h=title_h)

    assert pl_mx, "矩阵分组载荷为空（无可用细类或合并表无类目路径）"
    assert pl_po, "促销分组载荷为空"

    from pipeline.llm.generate_group_summaries import (
        MATRIX_GROUPS_SYSTEM,
        MATRIX_GROUPS_USER_PREFIX,
        PROMO_GROUPS_SYSTEM,
        PROMO_GROUPS_USER_PREFIX,
    )
    from pipeline.llm.llm_client import estimate_chat_input_tokens

    def _per_chunk_stats(
        *,
        groups: list[dict],
        system: str,
        user_prefix: str,
        label: str,
    ) -> None:
        max_chars = max_est = 0
        worst_name = ""
        for g in groups:
            raw = json.dumps({"keyword": kw, "groups": [g]}, ensure_ascii=False)
            user = user_prefix + raw
            est = estimate_chat_input_tokens(system, user)
            if len(raw) > max_chars:
                max_chars = len(raw)
                max_est = est
                worst_name = str(g.get("group") or "")
        n = len(groups)
        print(
            f"\n[{label}] 分块数={n}（默认 chunk 模式下串行请求数≈{n}）\n"
            f"  单次 user JSON（含 keyword+单组）最大字符数≈{max_chars}\n"
            f"  对应粗算输入 tokens≈{max_est}（与 estimate_chat_input_tokens 一致）\n"
            f"  最大块细类名: {worst_name!r}\n"
            f"  说明: 每块仍可能申请较大 max_tokens；网关生成慢或排队时，单次即可触发 Read timeout。\n"
        )

    _per_chunk_stats(
        groups=pl_mx,
        system=MATRIX_GROUPS_SYSTEM,
        user_prefix=MATRIX_GROUPS_USER_PREFIX,
        label="第五章·矩阵归纳 matrix",
    )
    _per_chunk_stats(
        groups=pl_po,
        system=PROMO_GROUPS_SYSTEM,
        user_prefix=PROMO_GROUPS_USER_PREFIX,
        label="第六章·促销归纳 promo",
    )


def test_first_matrix_group_llm_smoke_optional() -> None:
    """可选：只对第一个矩阵分组调用 1 次网关，验证连通性（默认跳过）。"""
    if (os.environ.get("MA_TEST_REAL_LLM") or "").strip() != "1":
        pytest.skip("仅当 MA_TEST_REAL_LLM=1 时调用真实网关")

    rd = _real_run_dir()
    if rd is None:
        pytest.skip("请设置 MA_TEST_REAL_RUN_DIR")

    rows = _load_merged_rows(rd)
    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    kw = (os.environ.get("MA_TEST_REAL_KEYWORD") or "低GI").strip() or "低GI"

    pl_mx = build_matrix_groups_llm_payload(rows, sku_header=sku_h, title_h=title_h)
    assert pl_mx

    from pipeline.llm.generate_group_summaries import generate_matrix_group_summaries_llm

    out = generate_matrix_group_summaries_llm([pl_mx[0]], keyword=kw)
    assert (out or "").strip(), "模型返回为空"
