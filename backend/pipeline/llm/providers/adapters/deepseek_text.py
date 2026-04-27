"""
DeepSeek 官方 OpenAI 兼容 `chat/completions`（`https://api.deepseek.com`），**仅用于纯文本**（`call_llm` / 报告 / 策略）。

与 `OPENAI_*` / 配料多模 分离，独立 `DEEPSEEK_*` 凭据。

启用：`MA_LLM_TEXT_PROVIDER=deepseek`（或 `deep_seek`）。

**思考模式**（与官方「Thinking Mode」一致）：默认可通过 `DEEPSEEK_THINKING=0` 关闭。开启时在请求中携带
`thinking.type=enabled` 与 `reasoning_effort`；不发送 `temperature`（官方在思考模式下忽略采样参数）。
未指定 `DEEPSEEK_TEXT_MODEL` 时，开启思考默认用 `deepseek-v4-pro`，关闭时默认 `deepseek-chat`。
详见 https://api-docs.deepseek.com/guides/thinking_mode
"""
from __future__ import annotations

import os
from typing import Any

import requests

from pipeline.openai_gateway.chat_content import normalize_message_content
from pipeline.openai_gateway.estimate import (
    estimate_chat_input_tokens as estimate_crawler_style_input_tokens,
)

_DEFAULT_BASE = "https://api.deepseek.com/v1"
_DEFAULT_MODEL_NO_THINK = "deepseek-chat"
_DEFAULT_MODEL_THINK = "deepseek-v4-pro"
# 常见 64k 级；若用长上下文/官方调整上限可改 DEEPSEEK_CONTEXT_WINDOW
_DEFAULT_CTX = 64_000

_BUF = 256
_WANT_MAX = 8192


def _read_timeout() -> tuple[float, float]:
    read = 600
    raw = (
        os.environ.get("DEEPSEEK_TIMEOUT")
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


def _thinking_enabled() -> bool:
    v = (os.environ.get("DEEPSEEK_THINKING") or "1").strip().lower()
    if v in ("0", "false", "off", "no", "disabled"):
        return False
    return True


def _reasoning_effort() -> str:
    raw = (os.environ.get("DEEPSEEK_REASONING_EFFORT") or "high").strip().lower()
    if raw in ("max", "high", "low", "medium", "xhigh"):
        if raw in ("low", "medium"):
            return "high"
        if raw == "xhigh":
            return "max"
        return raw
    return "high"


def _default_model() -> str:
    return _DEFAULT_MODEL_THINK if _thinking_enabled() else _DEFAULT_MODEL_NO_THINK


def _resolve_deepseek_credentials() -> tuple[str, str, str]:
    key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not key:
        raise ValueError(
            "使用 deepseek 文本适配器需设置 DEEPSEEK_API_KEY，"
            "与配料/视觉所用 OPENAI_API_KEY 分开配置。"
        )
    base = (os.environ.get("DEEPSEEK_BASE_URL") or _DEFAULT_BASE).strip().rstrip("/")
    model = (os.environ.get("DEEPSEEK_TEXT_MODEL") or os.environ.get("DEEPSEEK_MODEL") or "").strip()
    if not model:
        model = _default_model()
    return key, base, model


def _context_window() -> int:
    raw = (os.environ.get("DEEPSEEK_CONTEXT_WINDOW") or str(_DEFAULT_CTX)).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return _DEFAULT_CTX


def _default_temperature() -> float:
    return 0.2


class DeepSeekTextLlm:
    """DeepSeek `chat/completions`；与 `KimiMoonshotTextLlm` 同形（max_tokens 预检）。"""

    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        api_key, base, model = _resolve_deepseek_credentials()
        think = _thinking_enabled()
        body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": _WANT_MAX,
        }
        if think:
            # 与 OpenAI 官方 Python SDK 合并 extra_body 后一致：思考模式不依赖 temperature
            body["thinking"] = {"type": "enabled"}
            body["reasoning_effort"] = _reasoning_effort()
        else:
            body["temperature"] = _default_temperature() if temperature is None else float(temperature)
        est = estimate_crawler_style_input_tokens(system_prompt, user_prompt)
        context_window = _context_window()
        if est >= context_window - _BUF - 256:
            raise ValueError(
                f"提示词过长（估算输入约 {est} tokens，DEEPSEEK_CONTEXT_WINDOW={context_window}），"
                "请缩小输入或调大 DEEPSEEK_TEXT_MODEL / DEEPSEEK_CONTEXT_WINDOW。"
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
