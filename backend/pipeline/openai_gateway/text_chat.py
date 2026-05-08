"""
OpenAI 兼容 `chat/completions` 纯文本（system + user）。

凭据与基址可经 ``OPENAI_TEXT_API_KEY`` / ``OPENAI_TEXT_BASE_URL`` 与多模/配料的 ``OPENAI_API_KEY`` / ``OPENAI_BASE_URL`` **分开**（可只覆写其中一项，另一项回退到 ``OPENAI_*``）。详见 ``credentials.resolve_text_channel_credentials``。
"""
from __future__ import annotations

import os
from typing import Any

import requests

from .chat_content import normalize_message_content
from .credentials import resolve_text_channel_credentials, resolve_text_model_name
from .estimate import budgeted_chat_input_tokens, completion_budget_slack_tokens
from .timeouts import chat_completion_read_timeout as _chat_completion_timeout


def strip_outer_markdown_fence(text: str) -> str:
    """若模型用 ``` / ```markdown 包裹全文，去掉最外层围栏。"""
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    while lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def chat_completion_text(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
    temperature: float = 0.2,
    max_tokens: int = 8192,
    timeout: int | tuple[float, float] | None = None,
    extra_json: dict[str, Any] | None = None,
) -> str:
    if timeout is None:
        timeout = _chat_completion_timeout()
    k, b = resolve_text_channel_credentials(api_key, base_url)
    m = resolve_text_model_name(model)
    body: dict[str, Any] = {
        "model": m,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if extra_json:
        body.update(extra_json)
    ctx_raw = (
        os.environ.get("LLM_CONTEXT_WINDOW")
        or os.environ.get("OPENAI_CONTEXT_WINDOW")
        or "32768"
    ).strip()
    try:
        context_window = max(4096, int(ctx_raw))
    except ValueError:
        context_window = 32768
    buf = 256
    slack = completion_budget_slack_tokens()
    input_est = budgeted_chat_input_tokens(system_prompt, user_prompt)
    if input_est >= context_window - buf - 256 - slack:
        raise ValueError(
            f"提示词过长（估算输入约 {input_est} tokens，上下文上限 {context_window}），"
            "请缩小报告/摘要输入或换更大上下文的模型；也可设置环境变量 LLM_CONTEXT_WINDOW。"
        )
    avail = context_window - input_est - buf - slack
    want = int(body.get("max_tokens") or max_tokens)
    mt = min(want, max(0, avail))
    if mt < 1:
        raise ValueError(
            f"扣除输入估算与安全余量（LLM_COMPLETION_CONTEXT_SLACK={slack}）后无 completion 预算；"
            "请缩短输入或调大 LLM_CONTEXT_WINDOW / 调低 LLM_COMPLETION_CONTEXT_SLACK。"
        )
    body["max_tokens"] = mt
    r = requests.post(
        f"{b}/chat/completions",
        headers={
            "Authorization": f"Bearer {k}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=timeout,
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
