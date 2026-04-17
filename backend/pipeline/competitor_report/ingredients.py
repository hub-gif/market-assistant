"""配料列清洗：与 ``AI_crawler.normalize_ingredients_text_for_csv`` 等口径对齐。"""
from __future__ import annotations

import re


def _is_ingredient_url_blob(s: str) -> bool:
    """详情主图 URL 串（分号分隔）或单列以 http 开头。"""
    t = (s or "").strip()
    if not t:
        return False
    if t.startswith(("http://", "https://")):
        return True
    head = t[:400]
    if ("https://" in head or "http://" in head) and (
        ";" in t or len(t) > 180 or t.count("http") >= 2
    ):
        return True
    return False


def _ingredients_from_product_attributes(attrs: str) -> str:
    m = re.search(r"配料(?:表)?[:：]\s*([^;；]+)", attrs or "")
    return m.group(1).strip() if m else ""


def _ingredients_single_line(s: str) -> str:
    """与 ``AI_crawler.normalize_ingredients_text_for_csv`` 一致：多行配料压成一行（行间 ``；``），便于表格/CSV。"""
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    if len(lines) <= 1:
        return lines[0] if lines else ""
    return "；".join(lines)


__all__ = [
    "_ingredients_from_product_attributes",
    "_ingredients_single_line",
    "_is_ingredient_url_blob",
]
