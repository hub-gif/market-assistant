# -*- coding: utf-8 -*-
"""
拦截到的小红书「搜索笔记」API 响应落盘：**仅**小红书平台脚本引用。

写入 ``data/XHS/search_notes_raw/``，命名对齐京东 ``save_pc_search_response_raw``：
``search_notes_<seq>_req_kw_<slug>_t<stamp>.json``。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from sb_browser.cdp_json_listen import CapturedJsonResponse

from ._low_gi_root import xhs_data_dir


def default_search_notes_raw_dir() -> Path:
    return xhs_data_dir() / "search_notes_raw"


def _slug_kw_for_filename(kw: str, *, max_chars: int = 48) -> str:
    """与京东 ``safe_label`` 类似但更保留中文可读；不可用字符替换为 ``_``。"""
    chunks: list[str] = []
    for c in (kw or "").strip()[:max_chars]:
        if c in '<>:"/\\|?*' or ord(c) < 32:
            chunks.append("_")
        else:
            chunks.append(c)
    s = "".join(chunks).strip("._ ").strip("_")
    return s or "kw"


def save_search_notes_captures(
    captures: list[CapturedJsonResponse],
    *,
    keyword: str,
    raw_dir: Path | str | None = None,
    stamp: str | None = None,
    pretty: bool = True,
) -> list[Path]:
    """
    每个 ``CapturedJsonResponse`` 写一条独立 ``.json``，返回写入路径列表。

    ``raw_dir`` 默认 ``data/XHS/search_notes_raw``；若目录不存在会自动创建。
    """
    if not captures:
        return []
    rd = Path(raw_dir).expanduser().resolve() if raw_dir else default_search_notes_raw_dir()
    rd.mkdir(parents=True, exist_ok=True)
    safe_kw = _slug_kw_for_filename(keyword or "")
    if stamp is None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    paths: list[Path] = []
    for seq, row in enumerate(captures, start=1):
        ext = "json" if pretty else "js"
        path = rd / f"search_notes_{seq:03d}_req_kw_{safe_kw}_t{stamp}.{ext}"
        blob = {
            "keyword": keyword,
            "capture_index": seq,
            "stamp": stamp,
            "url": row.url,
            "mime_type": row.mime_type,
            "parse_error": row.parse_error,
            "parsed": row.parsed,
            "body_text": row.body_text,
        }
        if pretty:
            out = json.dumps(blob, ensure_ascii=False, indent=2) + "\n"
        else:
            out = json.dumps(blob, ensure_ascii=False) + "\n"
        path.write_text(out, encoding="utf-8")
        paths.append(path)
        print(f"[xhs] 已保存笔记搜索接口 JSON：{path}", file=sys.stderr)
    return paths
