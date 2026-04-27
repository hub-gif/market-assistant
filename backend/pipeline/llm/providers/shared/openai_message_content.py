"""
解析 OpenAI chat.completions 返回的 `message.content`：可能是 str 或 part 列表。
与 `AI_crawler._normalize_chat_content` 行为一致。
"""
from __future__ import annotations

from typing import Any


def normalize_message_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content.strip()
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
        return "".join(parts).strip()
    return str(content).strip()
