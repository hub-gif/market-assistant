"""
OpenAI 官方 `https://api.openai.com`（ChatGPT 系列）`chat/completions` 直连接口。

**凭据与网关与爬虫副本中的自建网关独立**，避免与 `OPENAI_BASE_URL` 指向的兼容网关共用时互相串环境。
通过 `MA_LLM_TEXT_PROVIDER=openai_official`（或 `openai_chatgpt` / `chatgpt`）启用。
"""
from __future__ import annotations

import os
from typing import Any

import requests

from pipeline.openai_gateway.chat_content import normalize_message_content
from pipeline.openai_gateway.estimate import (
    budgeted_chat_input_tokens,
    completion_budget_slack_tokens,
    estimate_chat_input_tokens as estimate_crawler_style_input_tokens,
)

_DEFAULT_BASE = "https://api.openai.com/v1"
_DEFAULT_MODEL = "gpt-4o-mini"
# gpt-4o / 4.1 等常见上限；可按模型在 .env 中覆盖
_DEFAULT_CTX = 128_000

_BUF = 256
_WANT_MAX = 8192


def _read_timeout() -> tuple[float, float]:
    read = 600
    raw = (os.environ.get("OPENAI_OFFICIAL_TIMEOUT") or os.environ.get("LLM_CHAT_TIMEOUT") or os.environ.get("OPENAI_TIMEOUT") or "").strip()
    if raw:
        try:
            read = max(60, int(raw))
        except ValueError:
            pass
    conn = 30.0
    raw_c = (os.environ.get("LLM_CHAT_CONNECT_TIMEOUT") or "").strip()
    if raw_c:
        try:
            conn = max(5.0, float(raw_c))
        except ValueError:
            pass
    return (conn, float(read))


def _resolve_credentials() -> tuple[str, str, str]:
    key = (os.environ.get("OPENAI_OFFICIAL_API_KEY") or "").strip()
    if not key:
        msg = "使用 openai_official 适配器需设置环境变量 OPENAI_OFFICIAL_API_KEY（与自建网关/爬虫副本的 key 可分开）。"
        raise ValueError(msg)
    base = (os.environ.get("OPENAI_OFFICIAL_BASE_URL") or _DEFAULT_BASE).strip().rstrip("/")
    model = (
        os.environ.get("OPENAI_OFFICIAL_TEXT_MODEL")
        or os.environ.get("OPENAI_OFFICIAL_MODEL")
        or _DEFAULT_MODEL
    ).strip()
    return key, base, model


def _context_window() -> int:
    raw = (os.environ.get("OPENAI_OFFICIAL_CONTEXT_WINDOW") or str(_DEFAULT_CTX)).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return _DEFAULT_CTX


def _default_temperature() -> float:
    return 0.2


class OpenAiOfficialChatGptTextLlm:
    """
    直连 OpenAI 官方「Chat Completions」；请求体与 `AI_crawler.chat_completion_text` 同形，
    并在本地做与爬虫网关一致的 `max_tokens` 收紧，减少 400。
    """

    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        api_key, base, model = _resolve_credentials()
        body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": _default_temperature() if temperature is None else float(temperature),
            "max_tokens": _WANT_MAX,
        }
        est = budgeted_chat_input_tokens(system_prompt, user_prompt)
        context_window = _context_window()
        slack = completion_budget_slack_tokens()
        if est >= context_window - _BUF - 256 - slack:
            raise ValueError(
                f"提示词过长（估算输入约 {est} tokens，OPENAI_OFFICIAL_CONTEXT_WINDOW={context_window}），"
                "请缩小输入或调大 OPENAI_OFFICIAL_CONTEXT_WINDOW。"
            )
        avail = context_window - est - _BUF - slack
        want = int(body.get("max_tokens") or _WANT_MAX)
        mt = min(want, max(0, avail))
        if mt < 1:
            raise ValueError(
                f"扣除安全余量（LLM_COMPLETION_CONTEXT_SLACK={slack}）后无 completion 预算；请缩短输入或调整环境变量。"
            )
        body["max_tokens"] = mt
        r = requests.post(
            f"{base}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=_read_timeout(),
        )
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            snippet = ""
            if e.response is not None:
                snippet = (e.response.text or "")[:1200].replace("\r\n", "\n").replace("\n", " ")
            if snippet:
                raise requests.HTTPError(
                    f"{e!s} | body: {snippet}",
                    response=e.response,
                    request=e.request,
                ) from e
            raise
        data = r.json()
        msg = (data.get("choices") or [{}])[0].get("message") or {}
        return normalize_message_content(msg.get("content"))

    def estimate_input_tokens(self, system_prompt: str, user_prompt: str) -> int:
        return estimate_crawler_style_input_tokens(system_prompt, user_prompt)

    def context_window_tokens(self) -> int:
        return _context_window()
