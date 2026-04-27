from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipeline.llm.providers import (
    CrawlerOpenAiCompatibleTextLlm,
    DeepSeekTextLlm,
    KimiMoonshotTextLlm,
    OpenAiOfficialChatGptTextLlm,
    get_text_llm,
    reset_text_llm_client_for_tests,
)


def test_openai_official_complete_text_uses_post_and_returns_content(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_OFFICIAL_API_KEY", "sk-test")

    def _fake_post(
        url: str,
        headers: dict,
        json: dict,
        timeout: object,
    ) -> MagicMock:
        assert "/chat/completions" in url
        assert json["messages"][0]["role"] == "system"
        r = MagicMock()
        r.json.return_value = {"choices": [{"message": {"content": "ok_out"}}]}
        r.raise_for_status = MagicMock()
        return r

    with patch(
        "pipeline.llm.providers.adapters.openai_official_chatgpt.requests.post",
        side_effect=_fake_post,
    ):
        llm = OpenAiOfficialChatGptTextLlm()
        out = llm.complete_text("S", "U", temperature=0.1)
    assert out == "ok_out"


def test_kimi_complete_text_uses_post_and_returns_content(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIMI_API_KEY", "sk-kimi-test")

    def _fake_post(
        url: str,
        headers: dict,
        json: dict,
        timeout: object,
    ) -> MagicMock:
        assert "moonshot" in url or "chat/completions" in url
        assert json["messages"][0]["role"] == "system"
        assert headers.get("Authorization", "").startswith("Bearer sk-kimi-")
        r = MagicMock()
        r.json.return_value = {"choices": [{"message": {"content": "kimi_out"}}]}
        r.raise_for_status = MagicMock()
        return r

    with patch(
        "pipeline.llm.providers.adapters.kimi_moonshot_text.requests.post",
        side_effect=_fake_post,
    ):
        llm = KimiMoonshotTextLlm()
        out = llm.complete_text("S", "U", temperature=0.1)
    assert out == "kimi_out"


def test_deepseek_complete_text_uses_post_and_thinking_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-ds-test")
    # 默认开思考，未显式设 DEEPSEEK_TEXT_MODEL 时用 deepseek-v4-pro
    monkeypatch.delenv("DEEPSEEK_TEXT_MODEL", raising=False)
    monkeypatch.delenv("DEEPSEEK_MODEL", raising=False)

    def _fake_post(
        url: str,
        headers: dict,
        json: dict,
        timeout: object,
    ) -> MagicMock:
        assert "deepseek" in url
        assert "/chat/completions" in url
        assert json["messages"][0]["role"] == "system"
        assert headers.get("Authorization", "").startswith("Bearer sk-ds-")
        assert json.get("model") == "deepseek-v4-pro"
        assert json.get("thinking") == {"type": "enabled"}
        assert json.get("reasoning_effort") == "high"
        assert "temperature" not in json
        r = MagicMock()
        r.json.return_value = {"choices": [{"message": {"content": "ds_out"}}]}
        r.raise_for_status = MagicMock()
        return r

    with patch(
        "pipeline.llm.providers.adapters.deepseek_text.requests.post",
        side_effect=_fake_post,
    ):
        llm = DeepSeekTextLlm()
        out = llm.complete_text("S", "U", temperature=0.1)
    assert out == "ds_out"


def test_deepseek_thinking_off_sends_temperature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-ds-test")
    monkeypatch.setenv("DEEPSEEK_THINKING", "0")
    monkeypatch.setenv("DEEPSEEK_TEXT_MODEL", "deepseek-chat")

    def _fake_post(
        url: str,
        headers: dict,
        json: dict,
        timeout: object,
    ) -> MagicMock:
        assert json.get("model") == "deepseek-chat"
        assert "thinking" not in json
        assert "reasoning_effort" not in json
        assert "temperature" in json
        r = MagicMock()
        r.json.return_value = {"choices": [{"message": {"content": "plain"}}]}
        r.raise_for_status = MagicMock()
        return r

    with patch(
        "pipeline.llm.providers.adapters.deepseek_text.requests.post",
        side_effect=_fake_post,
    ):
        out = DeepSeekTextLlm().complete_text("S", "U", temperature=0.0)
    assert out == "plain"


def test_factory_selects_deepseek_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_text_llm_client_for_tests()
    monkeypatch.setenv("MA_LLM_TEXT_PROVIDER", "deepseek")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-x")
    try:
        c = get_text_llm()
        assert isinstance(c, DeepSeekTextLlm)
    finally:
        reset_text_llm_client_for_tests()
        monkeypatch.delenv("MA_LLM_TEXT_PROVIDER", raising=False)
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)


def test_factory_selects_kimi_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_text_llm_client_for_tests()
    monkeypatch.setenv("MA_LLM_TEXT_PROVIDER", "kimi")
    monkeypatch.setenv("KIMI_API_KEY", "sk-x")
    try:
        c = get_text_llm()
        assert isinstance(c, KimiMoonshotTextLlm)
    finally:
        reset_text_llm_client_for_tests()
        monkeypatch.delenv("MA_LLM_TEXT_PROVIDER", raising=False)
        monkeypatch.delenv("KIMI_API_KEY", raising=False)


def test_factory_selects_openai_official_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_text_llm_client_for_tests()
    monkeypatch.setenv("MA_LLM_TEXT_PROVIDER", "chatgpt")
    monkeypatch.setenv("OPENAI_OFFICIAL_API_KEY", "sk-x")
    try:
        c = get_text_llm()
        assert isinstance(c, OpenAiOfficialChatGptTextLlm)
    finally:
        reset_text_llm_client_for_tests()
        monkeypatch.delenv("MA_LLM_TEXT_PROVIDER", raising=False)
        monkeypatch.delenv("OPENAI_OFFICIAL_API_KEY", raising=False)


def test_factory_unknown_provider_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_text_llm_client_for_tests()
    monkeypatch.setenv("MA_LLM_TEXT_PROVIDER", "no_such_provider")
    try:
        with pytest.raises(ValueError, match="不支持的"):
            get_text_llm()
    finally:
        reset_text_llm_client_for_tests()
        monkeypatch.delenv("MA_LLM_TEXT_PROVIDER", raising=False)


def test_default_provider_is_crawler_when_env_cleared(monkeypatch: pytest.MonkeyPatch) -> None:
    """未设置 MA_LLM_TEXT_PROVIDER 时仍为爬虫副本网关；此处只断言类型，不调真实网络。"""
    reset_text_llm_client_for_tests()
    monkeypatch.delenv("MA_LLM_TEXT_PROVIDER", raising=False)
    c = get_text_llm()
    assert isinstance(c, CrawlerOpenAiCompatibleTextLlm)
    reset_text_llm_client_for_tests()
