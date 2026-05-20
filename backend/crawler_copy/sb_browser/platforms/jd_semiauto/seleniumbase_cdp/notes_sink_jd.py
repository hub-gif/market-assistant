# -*- coding: utf-8 -*-
"""监听中按 kind 增量落盘 JSON；不写 dedupe_key、不配配料（盘后 postprocess 处理）。"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sb_browser.cdp_json_listen import CapturedJsonResponse

from ..common.low_gi_root import jd_semiauto_data_dir
from ..common import constants_jd_semiauto as _cfg
from .jd_capture_classify import classify_jd_capture


def _list_key_word(parsed: Any) -> str:
    """搜索列表响应中的接口关键词（优于任务级 keyword）。"""
    if not isinstance(parsed, dict):
        return ""
    data = parsed.get("data")
    if not isinstance(data, dict):
        return ""
    return str(data.get("listKeyWord") or "").strip()


def _slug_kw(kw: str, *, max_chars: int = 48) -> str:
    chunks: list[str] = []
    for c in (kw or "").strip()[:max_chars]:
        if c in '<>:"/\\|?*' or ord(c) < 32:
            chunks.append("_")
        else:
            chunks.append(c)
    s = "".join(chunks).strip("._ ").strip("_")
    return s or "kw"


def _slug_sku(sku: str) -> str:
    t = (sku or "").strip()
    if t.isdigit() and len(t) <= 24:
        return t
    if not t:
        return "unknown"
    safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in t[:32])
    return safe.strip("_") or "unknown"


def _has_ware_list(parsed: Any) -> bool:
    """list 类型过滤：parsed.data.wareList 须为非空 list。"""
    if not isinstance(parsed, dict):
        return False
    data = parsed.get("data")
    if not isinstance(data, dict):
        return False
    wl = data.get("wareList")
    return isinstance(wl, list) and len(wl) > 0


class JdCaptureSink:
    """增量落盘管理器：每次调用 ``flush`` 只写入尚未保存的条目，维护序号跨批次连续。"""

    def __init__(
        self,
        *,
        keyword: str,
        raw_dir: "Path | str | None" = None,
        stamp: "str | None" = None,
        pretty: bool = True,
        save_run_dir_by_time: bool = True,
    ) -> None:
        self.keyword = keyword
        self.stamp_eff = stamp if stamp is not None else datetime.now().strftime("%Y%m%d_%H%M%S")
        self.pretty = pretty

        base_rd = Path(raw_dir).expanduser().resolve() if raw_dir else jd_semiauto_data_dir()
        self.run_rd = base_rd / self.stamp_eff if save_run_dir_by_time else base_rd
        self.safe_kw = _slug_kw(keyword)

        self._kind_seq: dict[str, int] = {
            "list": 0,
            "detail": 0,
            "comment": 0,
            "graphic": 0,
            "unknown": 0,
        }
        self._saved_ids: set[int] = set()
        self.saved_paths: list[Path] = []
        self.skipped = 0

    def flush(self, captures: "list[CapturedJsonResponse]") -> "list[Path]":
        """保存尚未落盘的条目，返回本次新写的路径列表。"""
        new = [c for c in captures if id(c) not in self._saved_ids]
        if not new:
            return []

        paths: list[Path] = []
        for row in new:
            self._saved_ids.add(id(row))
            kind, resolved_sku, function_id = classify_jd_capture(row)

            if kind == "list" and not _has_ware_list(row.parsed):
                self.skipped += 1
                continue

            self._kind_seq[kind] = self._kind_seq.get(kind, 0) + 1
            idx = self._kind_seq[kind]

            sku_part = (
                f"_sku_{_slug_sku(resolved_sku)}"
                if kind in ("detail", "comment", "graphic")
                else ""
            )
            stem = f"jd_{kind}_{idx:03d}{sku_part}_kw_{self.safe_kw}_t{self.stamp_eff}"

            kind_dir = self.run_rd / kind
            kind_dir.mkdir(parents=True, exist_ok=True)
            path = kind_dir / f"{stem}.json"

            blob: dict[str, Any] = {
                "keyword": self.keyword,
                "capture_kind": kind,
                "resolved_sku": resolved_sku or "",
                "function_id": function_id or "",
                "url": row.url,
                "mime_type": row.mime_type,
                "parse_error": row.parse_error,
                "parsed": row.parsed,
            }
            lk = _list_key_word(row.parsed)
            if lk:
                blob["list_keyword"] = lk
            path.write_text(
                json.dumps(blob, ensure_ascii=False, indent=2) + "\n" if self.pretty
                else json.dumps(blob, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            paths.append(path)
            print(
                f"[jd_semiauto] 已写 {kind}/{path.name}（sku={resolved_sku or '—'}）",
                file=sys.stderr,
            )

        self.saved_paths.extend(paths)
        return paths


def save_jd_api_captures(
    captures: "list[CapturedJsonResponse]",
    *,
    keyword: str,
    raw_dir: "Path | str | None" = None,
    stamp: "str | None" = None,
    pretty: bool = True,
    save_run_dir_by_time: "bool | None" = None,
) -> "list[Path]":
    """一次性落盘全部 captures（兼容旧调用）。内部使用 JdCaptureSink。"""
    if not captures:
        return []
    srv = bool(getattr(_cfg, "SEMI_JD_SAVE_RUN_DIR_BY_TIME", True)) \
        if save_run_dir_by_time is None else save_run_dir_by_time
    sink = JdCaptureSink(
        keyword=keyword, raw_dir=raw_dir, stamp=stamp, pretty=pretty,
        save_run_dir_by_time=srv,
    )
    paths = sink.flush(captures)
    if paths:
        print(f"[jd_semiauto] 落盘目录: {sink.run_rd}", file=sys.stderr)
    if sink.skipped:
        print(f"[jd_semiauto] 跳过 {sink.skipped} 条无 wareList 的 list 响应。", file=sys.stderr)
    return paths
