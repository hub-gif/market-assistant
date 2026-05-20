# -*- coding: utf-8 -*-
"""盘后商详配料：图源由调用方传入；监听不调。空 URL 可走网关占位。"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

_LAZYLOAD_RE = re.compile(r'data-lazyload\s*=\s*"([^"]+)"', re.IGNORECASE)
_CSS_BG_URL_RE = re.compile(r"url\s*\(\s*([^)]+)\s*\)", re.I)
_ZB_MINI_VALUE_RE = re.compile(
    r"zbViewWeChatMiniImages[^>]*\bvalue\s*=\s*[\"']([^\"']+)[\"']",
    re.I | re.DOTALL,
)
_IMG_SRC_RE = re.compile(
    r"""<(?:img|image)\b[^>]*\bsrc\s*=\s*(['"])(?P<u>.*?)\1""",
    re.I | re.DOTALL,
)


def normalize_jd_img_url(u: str) -> str:
    """补全京东图文 URL（对齐 ``jd_detail_ware_fetch._normalize_jd_detail_asset_url``）。"""
    s = (u or "").strip()
    if len(s) >= 2 and s[0] in "\"'" and s[-1] == s[0]:
        s = s[1:-1].strip()
    if not s or s.lower().startswith("data:"):
        return ""
    if s.startswith("//"):
        return ("https:" + s)[:900]
    if s.startswith("http://"):
        return ("https://" + s[7:])[:900]
    if s.startswith("https://"):
        return s[:900]
    if s.startswith("/sku/jfs/"):
        return ("https://img30.360buyimg.com" + s)[:900]
    if s.startswith("/cms/jfs/"):
        return ("https://img12.360buyimg.com" + s)[:900]
    if s.startswith("/jfs/"):
        return ("https://img30.360buyimg.com/sku/jfs" + s[4:])[:900]
    if s.startswith("jfs/"):
        return ("https://img30.360buyimg.com/sku/" + s)[:900]
    return s[:900]


def _append_url(raw: str, urls: list[str], seen: set[str]) -> None:
    nu = normalize_jd_img_url(raw)
    if nu and nu not in seen:
        seen.add(nu)
        urls.append(nu)


def _urls_from_graphic_html_fragment(html: str, urls: list[str], seen: set[str]) -> None:
    if not (html or "").strip():
        return
    for m in _LAZYLOAD_RE.finditer(html):
        _append_url(m.group(1), urls, seen)
    for m in _CSS_BG_URL_RE.finditer(html):
        _append_url(m.group(1), urls, seen)
    zm = _ZB_MINI_VALUE_RE.search(html)
    if zm:
        for part in (zm.group(1) or "").split(","):
            _append_url(part.strip(), urls, seen)
    for m in _IMG_SRC_RE.finditer(html):
        _append_url(m.group("u"), urls, seen)


def urls_joined_from_graphic_content_html(html: str) -> str:
    """
    从 ``pc_item_getWareGraphic`` 的 ``graphicContent``（及同类 HTML）抽详情长图 URL。

    支持 ``data-lazyload``、CSS ``background-image:url(...)``、
    ``zbViewWeChatMiniImages`` 的 value、``<img>/<image> src``（与 Playwright 半自动 / 插件落盘同源）。
    """
    urls: list[str] = []
    seen: set[str] = set()
    _urls_from_graphic_html_fragment(html, urls, seen)
    return "; ".join(urls)


def urls_joined_from_graphic_parsed(parsed: Any) -> str:
    """从 graphic 接口 ``parsed`` 全文抽 URL（含 ``graphicInfoList`` 片段）。"""
    if not isinstance(parsed, dict):
        return ""
    urls: list[str] = []
    seen: set[str] = set()
    data = parsed.get("data")
    if isinstance(data, dict):
        _urls_from_graphic_html_fragment(str(data.get("graphicContent") or ""), urls, seen)
        glist = data.get("graphicInfoList")
        if isinstance(glist, list):
            for item in glist:
                if isinstance(item, dict):
                    _urls_from_graphic_html_fragment(str(item.get("html") or ""), urls, seen)
    return "; ".join(urls)


def sku_to_graphic_urls_joined_from_run_dir(run_dir: Path) -> dict[str, str]:
    """
    扫描 ``run_dir/graphic/*.json``，按数位 SKU 汇总长图 URL（与商详回填对齐）。

    同一 SKU 多文件时保留按文件名字典序第一份非空解析（与盘后去重后通常一 SKU 一单份一致）。
    """
    from .capture_dedupe_key import _detail_sku_id  # noqa: WPS433

    rd = Path(run_dir).expanduser().resolve()
    gd = rd / "graphic"
    out: dict[str, str] = {}
    if not gd.is_dir():
        return out
    for path in sorted(gd.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        sku = _detail_sku_id(
            str(blob.get("resolved_sku") or ""),
            str(blob.get("url") or ""),
            blob.get("parsed"),
        )
        if not sku or sku in out:
            continue
        joined = urls_joined_from_graphic_parsed(blob.get("parsed"))
        if joined:
            out[sku] = joined
    return out


def _ensure_backend_on_path() -> Path | None:
    """``cwd=crawler_copy`` 时须能 import ``pipeline``（半自动 SB/Playwright 子进程常见）。"""
    here = Path(__file__).resolve()
    crawler_copy = here.parents[4]
    backend = crawler_copy.parent
    if backend.is_dir() and (backend / "pipeline").is_dir():
        bp = str(backend.resolve())
        if bp not in sys.path:
            sys.path.insert(0, bp)
        return backend
    return None


def detail_ingredients_text_needs_refill(text: str) -> bool:
    """空或「未配置多模态」占位时允许盘后重试（补 .env 后重跑即可）。"""
    t = (text or "").strip()
    if not t:
        return True
    return "未配置多模态 API" in t


def recognize_detail_ingredients_with_urls_joined(urls_joined: str) -> tuple[str, str]:
    """
    调用配料识别；返回 ``(正文, source_url)``。

    ``urls_joined`` 为空时将得到网关定义的「未解析到任何详情长图 URL」说明，不发起视觉请求。
    多 URL 须用 ``\"; \"`` 或换行分隔（与 ``parse_joined_image_urls`` 一致）。
    """
    raw_urls = urls_joined or ""
    try:
        _ensure_backend_on_path()
        import pipeline.openai_gateway.ingredients_op as _ing  # noqa: WPS433

        fn: Any = getattr(
            _ing,
            "extract_ingredients_from_body_image_urls_reversed_with_source",
            None,
        )
        if not callable(fn):
            return ("【半自动配料】网关缺少 extract_ingredients_from_body_image_urls_reversed_with_source。", "")
        text, src = fn(raw_urls)
        out_t = str(text or "").strip()
        out_s = str(src).strip() if src else ""
        return out_t, out_s
    except Exception as e:
        return (f"【半自动配料】识别入口异常（已跳过）：{e}"[:1200], "")
