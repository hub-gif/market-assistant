"""文本 LLM 客户端协议：业务侧只依赖本接口，具体网关由适配器 + 工厂选择。"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class TextLlmClient(Protocol):
    def complete_text(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float | None = None,
    ) -> str:
        """一次 system + user 的纯文本补全，返回助理正文（无通用后处理，由 `llm_client.call_llm` 统一去围栏等）。"""

    def estimate_input_tokens(self, system_prompt: str, user_prompt: str) -> int:
        """与当次后端的 `max_tokens` 预检/截断策略一致的输入侧 token 保守估算。"""

    def context_window_tokens(self) -> int:
        """当前配置下的上下文 token 上限（与预检、策略模块档位一致）。"""
