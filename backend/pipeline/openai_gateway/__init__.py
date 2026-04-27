"""
OpenAI 兼容网关（`chat/completions`）：纯文本、多模态配料、详情长图逆序等。

逻辑唯一在 ``pipeline.openai_gateway``；单图试跑在 ``python -m pipeline.openai_gateway``（见 ``__main__.py``），不再使用爬虫目录下的已删除脚本名。
"""
from __future__ import annotations

from .chat_content import normalize_message_content
from .credentials import (
    _resolve_credentials,
    resolve_credentials,
    resolve_text_channel_credentials,
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
    "resolve_text_channel_credentials",
    "resolve_text_model_name",
    "sanitize_vision_ingredients_output",
    "strip_outer_markdown_fence",
]
