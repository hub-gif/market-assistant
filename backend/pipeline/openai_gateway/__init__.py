"""
OpenAI 兼容网关（`chat/completions`）：纯文本、多模态配料、详情长图逆序等。

与 ``crawler_copy/.../AI_crawler`` 解耦，逻辑唯一来源在 ``pipeline.openai_gateway``。
脚本 ``AI_crawler.py`` 仅作加载 .env 与命令行试跑、并向后兼容重导出名。
"""
from __future__ import annotations

from .chat_content import normalize_message_content
from .credentials import (
    _resolve_credentials,
    resolve_credentials,
    resolve_text_model_name,
)
from .ingredients_op import (
    REASON_NO_BODY_URLS,
    REASON_NO_VISION_API,
    extract_ingredients_from_body_image_urls_reversed,
    extract_ingredients_from_body_image_urls_reversed_with_source,
    extract_ingredients_from_image,
    normalize_ingredients_text_for_csv,
    parse_joined_image_urls,
    sanitize_vision_ingredients_output,
)
from .text_chat import chat_completion_text, strip_outer_markdown_fence

__all__ = [
    "REASON_NO_BODY_URLS",
    "REASON_NO_VISION_API",
    "_resolve_credentials",
    "chat_completion_text",
    "extract_ingredients_from_body_image_urls_reversed",
    "extract_ingredients_from_body_image_urls_reversed_with_source",
    "extract_ingredients_from_image",
    "normalize_ingredients_text_for_csv",
    "normalize_message_content",
    "parse_joined_image_urls",
    "resolve_credentials",
    "resolve_text_model_name",
    "sanitize_vision_ingredients_output",
    "strip_outer_markdown_fence",
]
