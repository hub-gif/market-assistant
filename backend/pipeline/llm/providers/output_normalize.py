"""对模型原始输出做与业务无关的轻量归一化（不修改提示词）。"""
from __future__ import annotations


def strip_outer_markdown_fence(text: str) -> str:
    """若模型用 ``` / ```markdown 包裹全文，去掉最外层围栏。与 `AI_crawler.strip_outer_markdown_fence` 行为一致。"""
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    while lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()
