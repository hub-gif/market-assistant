# -*- coding: utf-8 -*-
"""
从本地图片路径或图片 URL 调用 OpenAI 兼容多模态接口，提取配料表等；并提供**纯文本** ``chat/completions`` 供报告/策略等场景复用。

**实现**在 ``backend/pipeline/openai_gateway``；本文件负责：加载根目录 ``.env``、将 ``backend`` 加入 ``sys.path``、
命令行试跑、以及**同名符号**重导（兼容历史 ``import AI_crawler``）。

环境变量说明见原仓库文档与 ``openai_gateway`` 模块注释。

**运行方式**：在下方改 ``IMAGE_SOURCE`` 后执行 ``python AI_crawler.py``，无需命令行参数。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_MA_ROOT = Path(__file__).resolve().parents[3]


def _load_market_assistant_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    p = _MA_ROOT / ".env"
    if p.is_file():
        load_dotenv(p)


_load_market_assistant_dotenv()
_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from _low_gi_root import low_gi_project_root  # noqa: E402

_PROJECT_ROOT = low_gi_project_root()

import requests  # noqa: E402

from pipeline.openai_gateway import (  # noqa: E402
    REASON_NO_BODY_URLS,
    REASON_NO_VISION_API,
    chat_completion_text,
    extract_ingredients_from_body_image_urls_reversed,
    extract_ingredients_from_body_image_urls_reversed_with_source,
    extract_ingredients_from_image,
    normalize_ingredients_text_for_csv,
    normalize_message_content,
    parse_joined_image_urls,
    resolve_credentials,
    resolve_text_model_name,
    sanitize_vision_ingredients_output,
    strip_outer_markdown_fence,
)
from pipeline.openai_gateway.constants import (  # noqa: E402
    DEFAULT_MODEL,
    DEFAULT_USER_AGENT,
)
from pipeline.openai_gateway.credentials import _resolve_credentials  # noqa: E402
from pipeline.openai_gateway.ingredients_defaults import (  # noqa: E402
    IMAGE_REFERER,
    MAX_TOKENS,
    PROMPT_DEFAULT,
    QWEN_OMNI_TEMPLATE,
    TEMPERATURE,
    USER_PROMPT,
)
from pipeline.openai_gateway.ingredients_op import (  # noqa: E402
    _ingredient_extraction_acceptable,
)

# 与早先脚本一致：供 ``python AI_crawler.py`` 单图试跑；多数字段与 ``ingredients_defaults`` 同义
IMAGE_SOURCE = "https://img30.360buyimg.com/sku/jfs/t1/390444/8/13018/103574/6982e951Fc44d9d7b/00d62ee56189d75d.jpg.avif"
_normalize_chat_content = normalize_message_content


def _estimate_chat_input_tokens(system_prompt: str, user_prompt: str) -> int:
    from pipeline.openai_gateway.estimate import estimate_chat_input_tokens

    return estimate_chat_input_tokens(system_prompt, user_prompt)


def main() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    src = (IMAGE_SOURCE or "").strip()
    if not src:
        print(
            "[AI_crawler] 请在文件顶部设置 IMAGE_SOURCE（图片路径或 URL）后重试。",
            file=sys.stderr,
        )
        sys.exit(2)

    prompt_use = (USER_PROMPT or "").strip() or None
    extra: dict[str, Any] | None = None
    if QWEN_OMNI_TEMPLATE:
        extra = {"chat_template_kwargs": {"enable_thinking": False}}

    try:
        text = extract_ingredients_from_image(
            src,
            user_prompt=prompt_use,
            referer=(IMAGE_REFERER or "https://www.jd.com/").strip(),
            temperature=float(TEMPERATURE),
            max_tokens=int(MAX_TOKENS),
            extra_json=extra,
            prompt_default=PROMPT_DEFAULT,
        )
    except ValueError as e:
        print(f"[AI_crawler] {e}", file=sys.stderr)
        sys.exit(2)
    except requests.HTTPError as e:
        err_body = ""
        if e.response is not None and e.response.text:
            err_body = e.response.text[:1500]
        print(f"[AI_crawler] HTTP 错误: {e}\n{err_body}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[AI_crawler] 失败: {e}", file=sys.stderr)
        sys.exit(1)

    t = (text or "").strip()
    if _ingredient_extraction_acceptable(t):
        print(t)
    else:
        print(
            "【未通过配料表校验】输出须同时包含包装配料表常见结构（如「配料/配料表/原料/食品添加剂」）"
            "与含量或百分比等信息，或为「××（含量≥x%）」形态；纯食材/菜谱备料枚举不会采纳。"
            "与 extract_ingredients_from_body_image_urls_reversed 流水线规则一致。"
        )
        if t:
            print(f"[AI_crawler] 模型原始输出（未采纳）: {t}", file=sys.stderr)


if __name__ == "__main__":
    main()
