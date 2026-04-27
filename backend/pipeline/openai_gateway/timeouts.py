"""(连接, 读) 超时时长：供 ``chat/completions`` 与视觉请求使用。"""
from __future__ import annotations

import os


def chat_completion_read_timeout() -> tuple[float, float]:
    read = 600
    raw = (os.environ.get("LLM_CHAT_TIMEOUT") or os.environ.get("OPENAI_TIMEOUT") or "").strip()
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
