# -*- coding: utf-8 -*-
"""
命令行单图配料表试跑（多模态）。实现位于本包，**非**对其它模块的再导出。

在 ``backend`` 目录下执行::

    .venv\\Scripts\\python.exe -m pipeline.openai_gateway

与 Django 一样从**仓库根** ``.env`` 读取 ``OPENAI_*`` / ``LLM_*``。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# 与 ingredients_defaults 同口径；可在此改图 URL/路径
IMAGE_SOURCE = "https://img30.360buyimg.com/sku/jfs/t1/390444/8/13018/103574/6982e951Fc44d9d7b/00d62ee56189d75d.jpg.avif"

_HERE = Path(__file__).resolve()
# openai_gateway -> pipeline -> backend -> 仓库根（.env）
_MA_ROOT = _HERE.parents[3]


def _load_market_assistant_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    p = _MA_ROOT / ".env"
    if p.is_file():
        load_dotenv(p)


def main() -> int:
    _load_market_assistant_dotenv()
    import requests

    from .ingredients_defaults import (
        IMAGE_REFERER,
        MAX_TOKENS,
        PROMPT_DEFAULT,
        QWEN_OMNI_TEMPLATE,
        TEMPERATURE,
        USER_PROMPT,
    )
    from .ingredients_op import _ingredient_extraction_acceptable
    from . import extract_ingredients_from_image

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
            "[openai_gateway] 请在本文件 __main__.py 顶部设置 IMAGE_SOURCE（图片路径或 URL）。",
            file=sys.stderr,
        )
        return 2

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
        print(f"[openai_gateway] {e}", file=sys.stderr)
        return 2
    except requests.HTTPError as e:
        err_body = ""
        if e.response is not None and e.response.text:
            err_body = e.response.text[:1500]
        print(f"[openai_gateway] HTTP 错误: {e}\n{err_body}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[openai_gateway] 失败: {e}", file=sys.stderr)
        return 1

    t = (text or "").strip()
    if _ingredient_extraction_acceptable(t):
        print(t)
        return 0
    print(
        "【未通过配料表校验】输出须同时包含包装配料表常见结构（如「配料/配料表/原料/食品添加剂」）"
        "与含量或百分比等信息，或为「××（含量≥x%）」形态；纯食材/菜谱备料枚举不会采纳。"
        "与 extract_ingredients_from_body_image_urls_reversed 流水线规则一致。"
    )
    if t:
        print(f"[openai_gateway] 模型原始输出（未采纳）: {t}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
