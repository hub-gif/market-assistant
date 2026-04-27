"""
试跑 OpenAI 官方「ChatGPT」文本适配器（不经过完整报告管线）。

准备：在 .env 中设置 MA_LLM_TEXT_PROVIDER=chatgpt 与 OPENAI_OFFICIAL_API_KEY（见 .env.example）。

用法（在 backend 目录下）：

    .venv\\Scripts\\python.exe -m pipeline.demos.try_openai_official_llm
"""
from __future__ import annotations

import os
import sys

if __name__ == "__main__":
    if not (os.environ.get("OPENAI_OFFICIAL_API_KEY") or "").strip():
        print("未设置 OPENAI_OFFICIAL_API_KEY，退出。", file=sys.stderr)
        sys.exit(1)
    # 与生产一致时可在 .env 中设 MA_LLM_TEXT_PROVIDER=chatgpt；本脚本也强制用官方适配器
    from pipeline.llm.providers.adapters.openai_official_chatgpt import OpenAiOfficialChatGptTextLlm

    out = OpenAiOfficialChatGptTextLlm().complete_text(
        "You reply in one short English sentence only.",
        "What is 2+2?",
        temperature=0.0,
    )
    print(out)
