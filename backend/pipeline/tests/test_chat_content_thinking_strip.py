"""strip_thinking_leaks / normalize_message_content 剥掉混在正文的思考标签。"""
from __future__ import annotations

import pytest

from pipeline.openai_gateway.chat_content import (
    normalize_message_content,
    strip_thinking_leaks_from_model_text,
)

_LT, _GT, _SL = chr(60), chr(62), chr(47)


def _t(open_b: str, close_b: str, inner: str) -> str:
    return _LT + open_b + _GT + inner + _LT + _SL + close_b + _GT


def test_strip_redacted_thinking_block() -> None:
    raw = _t("redacted_thinking", "redacted_thinking", "reasoning") + "pong"
    assert strip_thinking_leaks_from_model_text(raw) == "pong"


def test_strip_redacted_open_think_close() -> None:
    raw = _t("redacted_thinking", "think", "a") + "b"
    assert strip_thinking_leaks_from_model_text(raw) == "b"


def test_strip_think_block() -> None:
    raw = _t("think", "think", "x") + "y"
    assert strip_thinking_leaks_from_model_text(raw) == "y"


def test_normalize_strips() -> None:
    raw = _t("redacted_thinking", "redacted_thinking", "x") + "\nok"
    assert normalize_message_content(raw) == "ok"


def test_preserve_thinking_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MA_LLM_PRESERVE_THINKING_IN_OUTPUT", "1")
    raw = _t("redacted_thinking", "redacted_thinking", "inside") + "after"
    out = strip_thinking_leaks_from_model_text(raw)
    assert "inside" in out
    assert "after" in out
