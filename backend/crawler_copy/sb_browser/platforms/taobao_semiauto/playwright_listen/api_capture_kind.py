# -*- coding: utf-8 -*-
"""淘宝聚合/H5 API 分类：list / detail / comment / mtop / unknown。

- **list**：在满足既定 api/url 与噪声过滤的前提下，在 ``parsed["data"]`` 子树下（含值为 JSON 字符串、嵌套 dict）检索 **itemsArray**。
- **comment**：在满足既定 api/url 的前提下，在 ``parsed["data"]`` 子树下检索 **任一** 评价样例字段（见 ``constants_taobao_semiauto.SEMI_TB_COMMENT_PAYLOAD_MARKER_KEYS``，对照 ``data/TB/sample/commant.txt``）。

自检：``python -m sb_browser.platforms.taobao_semiauto.playwright_listen.api_capture_kind``（cwd=crawler_copy）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from sb_browser.platforms.taobao_semiauto.common import constants_taobao_semiauto as _cfg
from sb_browser.platforms.taobao_semiauto.playwright_listen import tb_response_body as _body

Kind = Literal["list", "detail", "comment", "mtop", "unknown"]

_ALL_KINDS: tuple[Kind, ...] = ("list", "detail", "comment", "mtop", "unknown")


def _maybe_json_obj(s: str) -> Any:
    """将 MTOP 里常见的字符串化 JSON 载荷解出对象。"""
    t = (s or "").strip()
    if not t:
        return None
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return None


def _deep_key_under_mtop_data(node: Any, key: str, *, depth: int) -> bool:
    """在 ``parsed['data']`` 根节点起始的子树上查找 ``key``（dict 递归若干层 / str 二次 json.loads）。"""
    if depth <= 0:
        return False
    if isinstance(node, dict):
        if key in node:
            return True
        for v in node.values():
            if _deep_key_under_mtop_data(v, key, depth=depth - 1):
                return True
        return False
    if isinstance(node, str):
        inner = _maybe_json_obj(node)
        if inner is None:
            return False
        return _deep_key_under_mtop_data(inner, key, depth=depth - 1)
    if isinstance(node, list):
        for it in node:
            if _deep_key_under_mtop_data(it, key, depth=depth - 1):
                return True
    return False


def _tb_mtop_payload_key_present(*, parsed: Any, key: str, max_depth: int = 8) -> bool:
    if max_depth <= 0 or not isinstance(parsed, dict):
        return False
    root = parsed.get("data")
    return _deep_key_under_mtop_data(root, key, depth=max_depth)


def _tb_mtop_payload_any_marker_keys(
    *, parsed: Any, keys: tuple[str, ...], max_depth: int = 12
) -> bool:
    """在 ``parsed['data']`` 子树下是否出现 ``keys`` 中任意键（一次遍历多键，避免重复扫树）。"""
    if max_depth <= 0 or not isinstance(parsed, dict):
        return False
    want = frozenset(keys)
    return _deep_any_keys_under_mtop_data(parsed.get("data"), want, depth=max_depth)


def _deep_any_keys_under_mtop_data(node: Any, want: frozenset[str], *, depth: int) -> bool:
    if depth <= 0 or not node:
        return False
    if isinstance(node, dict):
        if want.intersection(node):
            return True
        for v in node.values():
            if _deep_any_keys_under_mtop_data(v, want, depth=depth - 1):
                return True
        return False
    if isinstance(node, str):
        inner = _maybe_json_obj(node)
        if inner is None:
            return False
        return _deep_any_keys_under_mtop_data(inner, want, depth=depth - 1)
    if isinstance(node, list):
        for it in node:
            if _deep_any_keys_under_mtop_data(it, want, depth=depth - 1):
                return True
    return False


def url_capture_excluded(url: str) -> bool:
    u = (url or "").lower()
    return any(x.lower() in u for x in _cfg.SEMI_PLAYWRIGHT_URL_EXCLUDE_FRAGMENTS)


def _mtop_api_from_parsed(parsed: Any) -> str:
    if not isinstance(parsed, dict):
        return ""
    api = parsed.get("api")
    return api.strip() if isinstance(api, str) else ""


def _normalize_host(netloc: str) -> str:
    h = (netloc or "").strip().lower()
    return h.split("@")[-1]


def is_probable_tb_item_detail_document_url(url: str) -> bool:
    """判断是否可能为商详主文档 URL（用于粗筛与 detail 分类）。

    - ``item.taobao.com`` / ``detail.tmall.com``：须 path 命中典型 ``*.htm``，
      或携带 ``id=`` 的根路径/商品路径（覆盖少数跳转形态）；避免 ``/js/`` 等静态资源。
    - 跨境 host：仅校验 host。
    """
    u = (url or "").strip()
    if not u or url_capture_excluded(u):
        return False
    try:
        p = urlparse(u)
    except Exception:
        return False
    host = _normalize_host(p.netloc or "")
    if not host:
        return False
    path_low = (p.path or "").lower()
    query = p.query or ""
    path_ok = _detail_path_matches(path_low)
    query_ok = _query_has_numeric_item_id(query) and (
        path_low in ("", "/") or "item" in path_low
    )

    for suf in _cfg.SEMI_TB_DETAIL_HOST_SUFFIXES_REQUIRE_PATH:
        suf_l = suf.lower()
        if host == suf_l or host.endswith("." + suf_l):
            return path_ok or query_ok

    for suf in _cfg.SEMI_TB_DETAIL_HOST_SUFFIXES_LOOSE:
        suf_l = suf.lower()
        if host == suf_l or host.endswith("." + suf_l):
            return True

    return False


def _detail_path_matches(path_low: str) -> bool:
    return any(snip in path_low for snip in _cfg.SEMI_TB_DETAIL_PATH_SNIPPETS)


def _query_has_numeric_item_id(query: str) -> bool:
    return "id=" in (query or "").lower()


def _is_tb_mtop_host_url(low_url: str) -> bool:
    return any(h.lower() in low_url for h in _cfg.SEMI_PLAYWRIGHT_JSON_HOST_HINTS)


def should_attempt_tb_response_capture(url: str) -> bool:
    """是否读取 ``response.text()``：MTOP host 或疑似商详文档 URL。"""
    u = (url or "").strip()
    if not u or url_capture_excluded(u):
        return False
    if is_probable_tb_item_detail_document_url(u):
        return True
    return _is_tb_mtop_host_url(u.lower())


def _detail_content_type_maybe_html(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return any(
        x in ct
        for x in (
            "text/html",
            "application/xhtml",
            "application/xhtml+xml",
            "text/plain",
        )
    )


def looks_like_tb_capture(*, url: str, content_type: str = "", body_text: str = "") -> bool:
    """是否在读完正文后保留：商详 HTML（放宽 Content-Type）；或 MTOP host。"""
    u = (url or "").strip()
    if not u or url_capture_excluded(u):
        return False
    low = u.lower()
    peek = body_text or ""

    if is_probable_tb_item_detail_document_url(u):
        if not peek.strip():
            return False
        if _body.looks_like_html_document(peek):
            return True
        pl = peek.lstrip("\ufeff \t\r\n").lower()
        if _detail_content_type_maybe_html(content_type):
            if pl.startswith("<!doctype") or pl.startswith("<html"):
                return True
            scan = pl[:524288]
            if "<html" in scan or "<!doctype" in scan:
                return True
        if not (content_type or "").strip():
            return _body.looks_like_html_document(peek)
        return False

    return _is_tb_mtop_host_url(low)


def _list_capture_noise(*, url: str, parsed: Any) -> bool:
    chunks: list[str] = [url or ""]
    if isinstance(parsed, (dict, list)):
        try:
            chunks.append(json.dumps(parsed, ensure_ascii=False))
        except (TypeError, ValueError):
            chunks.append(str(parsed))
    blob = "\n".join(chunks).lower()
    return any(m.lower() in blob for m in _cfg.SEMI_TB_LIST_CAPTURE_NOISE_MARKERS)


def _tb_comment_payload_looks_like(parsed: Any) -> bool:
    """评价类：`data` 子树内命中样例中的任一 marker 键（含嵌套 / 字符串化 JSON）。"""
    return _tb_mtop_payload_any_marker_keys(
        parsed=parsed, keys=_cfg.SEMI_TB_COMMENT_PAYLOAD_MARKER_KEYS, max_depth=12
    )


def _tb_list_payload_looks_like(parsed: Any) -> bool:
    """列表类：`data`（含多层 / 字符串化）子树内需出现 ``itemsArray``。"""
    return _tb_mtop_payload_key_present(parsed=parsed, key="itemsArray", max_depth=8)


def _classify_list_comment(*, api: str, url: str, parsed: Any) -> Kind | None:
    low_u = (url or "").lower()

    if api:
        if api in _cfg.SEMI_TB_COMMENT_CAPTURE_APIS_EXACT and _tb_comment_payload_looks_like(parsed):
            return "comment"
        if any(s in api for s in _cfg.SEMI_TB_COMMENT_API_SUBSTRINGS) and _tb_comment_payload_looks_like(
            parsed
        ):
            return "comment"

    if _is_tb_mtop_host_url(low_u):
        if any(s.lower() in low_u for s in _cfg.SEMI_TB_COMMENT_CAPTURE_URL_SUBSTRINGS):
            if isinstance(parsed, dict) and _tb_comment_payload_looks_like(parsed):
                return "comment"

    if not api:
        return None
    if api in _cfg.SEMI_TB_LIST_CAPTURE_APIS_EXACT and any(
        s.lower() in low_u for s in _cfg.SEMI_TB_LIST_CAPTURE_URL_SUBSTRINGS
    ):
        if _list_capture_noise(url=url, parsed=parsed):
            return None
        if _tb_list_payload_looks_like(parsed):
            return "list"

    return None


def classify_taobao_aggregate(
    *,
    url: str = "",
    parsed: Any = None,
    body_text: str = "",
    content_type: str = "",
    body_shape: _body.ParseShape | None = None,
) -> Kind | None:
    """按 URL + 解析结果 + 正文形态分类；无法命中则 ``None``（不落盘）。"""
    u = (url or "").strip()
    if not u or url_capture_excluded(u):
        return None
    low = u.lower()
    ct = (content_type or "").lower()
    shape = body_shape

    api = _mtop_api_from_parsed(parsed)
    hit = _classify_list_comment(api=api, url=u, parsed=parsed)
    if hit is not None:
        return hit

    if is_probable_tb_item_detail_document_url(u):
        body = body_text or ""
        if shape == "html" or _body.looks_like_html_document(body):
            return "detail"
        if _detail_content_type_maybe_html(ct) and _body.looks_like_html_document(body):
            return "detail"
        if (
            not (content_type or "").strip()
            and body.strip()
            and _body.looks_like_html_document(body)
        ):
            return "detail"

    if _is_tb_mtop_host_url(low):
        if isinstance(parsed, (dict, list)):
            return "mtop"
        if shape in ("unparsed", "empty") and (body_text or "").strip():
            return "mtop"
        return None

    if isinstance(parsed, (dict, list)):
        return "unknown"
    return None


def api_hint_from_url(url: str) -> str:
    """尽力从 path/query 提取可读摘要。"""
    if not url:
        return ""
    try:
        p = urlparse(url)
        q = (p.query or "")[:120]
        tail = f"?{q}" if q else ""
        return f"{p.netloc}{p.path}{tail}"[:240]
    except Exception:
        return url[:240]


def classify_capture_envelope(obj: dict[str, Any]) -> Kind | None:
    url = (obj.get("url") or "").strip()
    p = obj.get("parsed")
    body = (obj.get("body_text") or "") if isinstance(obj, dict) else ""
    ct = str(obj.get("content_type") or "")
    bs = obj.get("body_parse_shape")
    shape = bs if bs in ("empty", "json", "jsonp", "html", "unparsed") else None
    return classify_taobao_aggregate(
        url=url, parsed=p, body_text=str(body), content_type=ct, body_shape=shape
    )


def kind_counter_template() -> dict[str, int]:
    return {k: 0 for k in _ALL_KINDS}


if __name__ == "__main__":
    _cc = Path(__file__).resolve().parents[4]
    if str(_cc) not in sys.path:
        sys.path.insert(0, str(_cc))
    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        print("用法: python -m ...api_capture_kind <blob.json> [...]", file=sys.stderr)
        raise SystemExit(2)
    for fp in paths:
        if not fp.is_file():
            print(f"skip: {fp}", file=sys.stderr)
            continue
        obj = json.loads(fp.read_text(encoding="utf-8"))
        k = classify_capture_envelope(obj)
        print(f"{k}\t{fp.name}\turl={(obj.get('url') or '')[:80]!r}")
