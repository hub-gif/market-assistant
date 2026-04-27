"""
用「全量」strategy_decisions fixture 走一遍规则底稿，验证字段贯通（不请求大模型）。

用法（在仓库 `backend` 目录下）::

  python -m pipeline.demos.run_strategy_decisions_full_fixture_demo

- ``fixtures/strategy_decisions_full_lowgi_biscuit.json``：仅 21 项 ``strategy_decisions``，供 ``--decisions-json`` 合并。
- ``fixtures/strategy_draft_request_full_lowgi_biscuit.json``：与 ``POST /api/jobs/…/strategy-draft/`` 相同的**顶栏全字段**（含 ``generator``、``business_notes``、矩阵作用域等）。

与线上一致联调大模型入参时，可配合 ::

    python -m pipeline.demos.dump_strategy_llm_input_md --run-dir <你的run_dir> \\
    --decisions-json pipeline/demos/fixtures/strategy_decisions_full_lowgi_biscuit.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pipeline.strategy_decision_keys import STRATEGY_DECISION_FIELD_NAMES

# 子进程 / 无 Django 时也可仅校验 JSON
_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "strategy_decisions_full_lowgi_biscuit.json"

# 与 ``JobStrategyDraftView`` 组装的 strategy_decisions 键一致（全量联调用）
EXPECTED_DECISION_KEYS: frozenset[str] = frozenset(STRATEGY_DECISION_FIELD_NAMES)


def load_full_fixture() -> dict:
    data = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("fixture 根须为 JSON 对象")
    missing = EXPECTED_DECISION_KEYS - set(data.keys())
    extra = set(data.keys()) - EXPECTED_DECISION_KEYS
    if missing:
        raise ValueError(f"fixture 缺少键: {sorted(missing)}")
    if extra:
        raise ValueError(f"fixture 多余键: {sorted(extra)}")
    return data


def main() -> int:
    sd = load_full_fixture()
    from pipeline.llm.generate_strategy import strategy_decisions_substantive
    from pipeline.reporting.strategy_draft import build_strategy_draft_markdown

    if not strategy_decisions_substantive(sd):
        print("strategy_decisions_substantive: 预期为 True，实际为 False", file=sys.stderr)
        return 1
    brief = {
        "schema_version": 1,
        "keyword": "低GI饼干",
        "batch_label": "demo_fixture",
        "scope": {"merged_sku_count": 2},
        "strategy_hints": ["fixture 联调"],
        "meta": {"page_start": 1, "page_to": 3, "max_skus_config": 100},
        "category_mix_top": [
            {"label": "粗粮饼干", "count": 11},
            {"label": "酥性饼干", "count": 10},
        ],
        "pc_search_raw": {"result_count_consensus": 100000},
        "price_stats": {"n": 21, "min": 14.38, "max": 64.97, "median": 27.97},
    }
    md = build_strategy_draft_markdown(
        job_id=0,
        keyword="低GI饼干",
        brief=brief,
        business_notes="（fixture 演示：可替换为业务备注。）",
        generated_at_iso="2026-01-01T00:00:00+00:00",
        strategy_decisions=sd,
        for_llm_input=False,
        report_config=None,
    )
    if "表单促销策略" not in md or str(sd.get("tactic_promotion", "")) not in md:
        print("成稿中未出现 fixture 的促销决策锚点，请检查 strategy_draft 与 fixture。", file=sys.stderr)
        return 1
    if "卡位监测中位" not in md:
        print("成稿中未出现 fixture 中价格支柱文本。", file=sys.stderr)
        return 1
    print("OK — strategy_decisions_substantive:", strategy_decisions_substantive(sd))
    print("OK — build_strategy_draft_markdown 长度:", len(md), "字符")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
