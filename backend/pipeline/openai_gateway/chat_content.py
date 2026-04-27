"""解析 `chat/completions` 返回的 `message.content`（str 或多段），并剥掉泄漏到正文的「思考/推理」片段。"""
from __future__ import annotations

import os
import re
from typing import Any

# 部分网关/模型在仍开启「思考模式」时，会把推理混进 `content`；内部仍可思考，对下游/报告只返回正文。
_F = re.IGNORECASE | re.DOTALL
_LT, _GT, _SL = (chr(60), chr(62), chr(47))


def _pair(open_body: str, close_body: str) -> re.Pattern[str]:
    o = _LT + open_body + _GT
    c = _LT + _SL + close_body + _GT
    return re.compile(re.escape(o) + r".*?" + re.escape(c), _F)


# 成对整段删尽（可多次出现）；`.*?` 非贪婪跨行
_THINKING_SPAN_RES: list[re.Pattern[str]] = [
    _pair("redacted_thinking", "redacted_thinking"),
    _pair("redacted_thinking", "think"),  # 常见：以 </think> 收束
    _pair("think", "think"),
]


def strip_thinking_leaks_from_model_text(text: str) -> str:
    """
    从模型返回的「可见正文」中移除混在 `content` 里的思考/推理块（不关闭服务端思考，只净化下游看到的内容）。

    调试用：``MA_LLM_PRESERVE_THINKING_IN_OUTPUT=1`` 时不再剥离。
    """
    if not (text and text.strip()):
        return text
    if (os.environ.get("MA_LLM_PRESERVE_THINKING_IN_OUTPUT") or "").strip() in (
        "1",
        "true",
        "yes",
    ):
        return text
    t = str(text)
    for pat in _THINKING_SPAN_RES:
        t = pat.sub("", t)
    return t.strip()


def normalize_message_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return strip_thinking_leaks_from_model_text(content.strip())
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(str(item.get("text") or ""))
                elif "text" in item:
                    parts.append(str(item.get("text") or ""))
            elif isinstance(item, str):
                parts.append(item)
        return strip_thinking_leaks_from_model_text("".join(parts).strip())
    return strip_thinking_leaks_from_model_text(str(content).strip())
