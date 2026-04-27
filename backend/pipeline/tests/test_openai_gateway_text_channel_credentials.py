"""OPENAI_TEXT_* 与 OPENAI_* 可分开供纯文本使用。"""
from __future__ import annotations

import pytest

from pipeline.openai_gateway.credentials import resolve_text_channel_credentials


def test_text_channel_falls_back_to_openai_when_no_text_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-main")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://gw.example.com/v1")
    monkeypatch.delenv("OPENAI_TEXT_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_TEXT_BASE_URL", raising=False)
    k, b = resolve_text_channel_credentials()
    assert k == "sk-main"
    assert b == "https://gw.example.com/v1"


def test_text_channel_uses_text_key_same_base(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-vision")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://llm.rekeymed.com/v1")
    monkeypatch.setenv("OPENAI_TEXT_API_KEY", "sk-text-only")
    k, b = resolve_text_channel_credentials()
    assert k == "sk-text-only"
    assert b == "https://llm.rekeymed.com/v1"


def test_text_channel_uses_text_base_same_key(monkeypatch: pytest.MonkeyPatch) -> None:
    for _e in (
        "OPENAI_TEXT_API_KEY",
        "LLM_TEXT_API_KEY",
        "LLM_API_KEY",
    ):
        monkeypatch.delenv(_e, raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-shared")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://a.com/v1")
    monkeypatch.setenv("OPENAI_TEXT_BASE_URL", "https://b.com/v1")
    k, b = resolve_text_channel_credentials()
    assert k == "sk-shared"
    assert b == "https://b.com/v1"


def test_explicit_args_win_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://env.com/v1")
    k, b = resolve_text_channel_credentials("sk-arg", "https://arg.com/v1")
    assert k == "sk-arg"
    assert b == "https://arg.com/v1"
