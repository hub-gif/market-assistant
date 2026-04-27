"""OpenAI 兼容网关：从环境或参数解析 API 凭证与模型名（文本 / 多模态共用）。"""
from __future__ import annotations

import os

from .constants import DEFAULT_MODEL


def _resolve_credentials(
    api_key: str | None,
    base_url: str | None,
    model: str | None,
) -> tuple[str, str, str]:
    key = (
        (api_key or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY") or "").strip()
    )
    base = (
        (base_url or "").strip().rstrip("/")
        or (
            os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL") or ""
        ).strip().rstrip("/")
    )
    m = (
        (model or "").strip()
        or (
            os.environ.get("OPENAI_VISION_MODEL")
            or os.environ.get("LLM_MODEL")
            or DEFAULT_MODEL
        ).strip()
    )
    if not key:
        raise ValueError("请设置环境变量 OPENAI_API_KEY（或 LLM_API_KEY）")
    if not base:
        raise ValueError(
            "请设置环境变量 OPENAI_BASE_URL（或 LLM_BASE_URL），例如 https://your-gateway.com/v1"
        )
    return key, base, m


def resolve_text_model_name(model: str | None = None) -> str:
    m = (model or "").strip()
    if m:
        return m
    for env in (
        "OPENAI_TEXT_MODEL",
        "LLM_TEXT_MODEL",
        "OPENAI_VISION_MODEL",
        "LLM_MODEL",
    ):
        v = (os.environ.get(env) or "").strip()
        if v:
            return v
    return DEFAULT_MODEL


def resolve_credentials(
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
) -> tuple[str, str, str]:
    """与历史 ``AI_crawler._resolve_credentials`` 行为一致，供业务显式预检（如多模态是否可用）。"""
    return _resolve_credentials(api_key, base_url, model)
