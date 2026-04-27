"""report_config 中布尔开关为 JSON null 时不应把 LLM 归纳整段关掉。"""
from __future__ import annotations

from unittest import mock

from pipeline.jd.runner import merge_report_config_with_defaults
from pipeline.serializers import validate_report_config_body


def test_validate_report_config_strips_null_bool_flags() -> None:
    out = validate_report_config_body(
        {
            "llm_matrix_group_summaries": None,
            "llm_promo_group_summaries": None,
        }
    )
    assert "llm_matrix_group_summaries" not in out
    assert "llm_promo_group_summaries" not in out


@mock.patch("pipeline.jd.runner.get_default_report_config")
def test_merge_report_config_null_bool_gets_default(mock_def: mock.MagicMock) -> None:
    mock_def.return_value = {
        "llm_matrix_group_summaries": True,
        "llm_promo_group_summaries": True,
        "llm_price_group_summaries": False,
    }
    merged = merge_report_config_with_defaults(
        {"llm_matrix_group_summaries": None, "llm_promo_group_summaries": None}
    )
    assert merged["llm_matrix_group_summaries"] is True
    assert merged["llm_promo_group_summaries"] is True
    assert merged["llm_price_group_summaries"] is False
