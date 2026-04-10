# -*- coding: utf-8 -*-
"""工作台粘贴的 Cookie 规范化（与 jd_cookie.txt 单行内容一致）。"""


def normalize_browser_cookie_paste(raw: str) -> str:
    """
    - 去掉首尾空白
    - 若整段以 ``Cookie:`` 开头（不区分大小写，常见于 DevTools 复制请求头），去掉该前缀
    """
    s = (raw or "").strip()
    if not s:
        return ""
    prefix = "cookie:"
    if s.lower().startswith(prefix):
        return s[len(prefix) :].strip()
    return s
