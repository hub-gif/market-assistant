"""
月之暗面 Kimi（Moonshot）OpenAI 兼容 `chat/completions`，**仅用于纯文本**（`call_llm` / 报告 / 策略）。

与 `OPENAI_*` / `LLM_*` 分离，避免与自建网关（配料多模态等）混用同一套 Key。

启用：`MA_LLM_TEXT_PROVIDER=kimi`（或 `moonshot` / `kimi_moonshot`）。

环境变量：`KIMI_API_KEY`（必填）、`KIMI_BASE_URL`（默认 Moonshot 官方 v1）、`KIMI_TEXT_MODEL`、
`KIMI_CONTEXT_WINDOW`、`KIMI_TIMEOUT` 等；见 `.env.example`。
"""
from __future__ import annotations

import os
from typing import Any

import requests

from pipeline.openai_gateway.chat_content import normalize_message_content
from pipeline.openai_gateway.estimate import (
    estimate_chat_input_tokens as estimate_crawler_style_input_tokens,
)

_DEFAULT_BASE = "https://api.moonshot.cn/v1"
_DEFAULT_MODEL = "moonshot-v1-8k"
# 与常见 8k 窗口一致；若使用 moonshot-v1-128k 等请调大 KIMI_CONTEXT_WINDOW
_DEFAULT_CTX = 8192

_BUF = 256
_WANT_MAX = 8192


def _read_timeout() -> tuple[float, float]:
    read = 600
    raw = (
        os.environ.get("KIMI_TIMEOUT")
        or os.environ.get("LLM_CHAT_TIMEOUT")
        or os.environ.get("OPENAI_TIMEOUT")
        or ""
    ).strip()
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


def _resolve_kimi_credentials() -> tuple[str, str, str]:
    key = (
        (os.environ.get("KIMI_API_KEY") or os.environ.get("MOONSHOT_API_KEY") or "").strip()
    )
    if not key:
        raise ValueError(
            "使用 kimi 文本适配器需设置 KIMI_API_KEY（或 MOONSHOT_API_KEY），"
            "与配料/视觉所用 OPENAI_API_KEY 分开配置。"
        )
    base = (os.environ.get("KIMI_BASE_URL") or _DEFAULT_BASE).strip().rstrip("/")
    model = (
        os.environ.get("KIMI_TEXT_MODEL")
        or os.environ.get("KIMI_MODEL")
        or os.environ.get("MOONSHOT_MODEL")
        or _DEFAULT_MODEL
    ).strip()
    return key, base, model


def _context_window() -> int:
    raw = (os.environ.get("KIMI_CONTEXT_WINDOW") or str(_DEFAULT_CTX)).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return _DEFAULT_CTX


def _default_temperature() -> float:
    return 0.2


class KimiMoonshotTextLlm:
    """Kimi OpenAI 兼容文本补全；行为与 `OpenAiOfficialChatGptTextLlm` 同形（max_tokens 预检）。"""

    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        api_key, base, model = _resolve_kimi_credentials()
        body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": _default_temperature() if temperature is None else float(temperature),
            "max_tokens": _WANT_MAX,
        }
        est = estimate_crawler_style_input_tokens(system_prompt, user_prompt)
        context_window = _context_window()
        if est >= context_window - _BUF - 256:
            raise ValueError(
                f"提示词过长（估算输入约 {est} tokens，KIMI_CONTEXT_WINDOW={context_window}），"
                "请缩小输入或换更大上下文的 KIMI_TEXT_MODEL / KIMI_CONTEXT_WINDOW。"
            )
        avail = context_window - est - _BUF
        want = int(body.get("max_tokens") or _WANT_MAX)
        body["max_tokens"] = max(256, min(want, max(avail, 256)))
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
