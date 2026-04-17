"""策略页独立 ``strategy_config`` 校验。"""
from __future__ import annotations

import pytest
from rest_framework import serializers

from pipeline.serializers import validate_strategy_config_body


def test_validate_strategy_config_merges_default() -> None:
    out = validate_strategy_config_body({})
    assert out["use_llm_default"] is True


def test_validate_strategy_config_unknown_key() -> None:
    with pytest.raises(serializers.ValidationError):
        validate_strategy_config_body({"foo": 1})


def test_validate_strategy_config_use_llm_type() -> None:
    with pytest.raises(serializers.ValidationError):
        validate_strategy_config_body({"use_llm_default": "yes"})
