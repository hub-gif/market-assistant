# -*- coding: utf-8 -*-
"""将 capture envelope 写入 run_dir/{kind}/*.json（契约见 ../contracts/README.md）。"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


_KINDS = frozenset({"list", "detail", "comment", "graphic", "unknown"})


def _project_jd_root() -> Path:
    raw = (os.environ.get("LOW_GI_PROJECT_ROOT") or "").strip().strip('"').strip("'")
    if raw:
        return Path(raw).expanduser().resolve() / "data" / "JD"
    # extensions/jd_semiauto_chrome/sidecar/writer.py -> market_assistant
    ma = Path(__file__).resolve().parents[3]
    return ma / "data" / "JD"


def validate_run_dir_under_jd(run_dir: Path) -> Path:
    rd = run_dir.expanduser().resolve()
    jd = _project_jd_root().resolve()
    if not jd.is_dir():
        raise ValueError(f"京东数据根目录不存在: {jd}（请配置 LOW_GI_PROJECT_ROOT）")
    try:
        rd.relative_to(jd)
    except ValueError as e:
        raise ValueError(f"run_dir 须位于 {jd} 下: {rd}") from e
    rd.mkdir(parents=True, exist_ok=True)
    return rd


def _slug_kw(kw: str, *, max_chars: int = 48) -> str:
    chunks: list[str] = []
    for c in (kw or "").strip()[:max_chars]:
        if c in '<>:"/\\|?*' or ord(c) < 32:
            chunks.append("_")
        else:
            chunks.append(c)
    s = "".join(chunks).strip("._ ").strip("_")
    return s or "kw"


def _parsed_has_ware_list(parsed: dict[str, Any]) -> bool:
    """list 落盘：须 parsed.data.wareList（或 wareListPro）为非空数组。"""
    data = parsed.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return False
    if not isinstance(data, dict):
        return False
    for key in ("wareList", "wareListPro"):
        wl = data.get(key)
        if isinstance(wl, list) and len(wl) > 0:
            return True
    return False


def _slug_sku(sku: str) -> str:
    t = (sku or "").strip()
    if t.isdigit() and len(t) <= 24:
        return t
    if not t:
        return "unknown"
    safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in t[:32])
    return safe.strip("_") or "unknown"


class RunDirWriter:
    def __init__(self, run_dir: Path, *, keyword: str = "manual") -> None:
        self.run_dir = validate_run_dir_under_jd(run_dir)
        self.keyword = (keyword or "manual").strip() or "manual"
        self.safe_kw = _slug_kw(self.keyword)
        self._seq: dict[str, int] = {k: 0 for k in _KINDS}

    def file_counts(self) -> dict[str, int]:
        """各 kind 已写文件序号（与 Playwright 落盘计数类似）。"""
        return {k: int(self._seq.get(k, 0)) for k in ("list", "detail", "comment", "graphic")}

    def write_batch(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        written: list[str] = []
        skipped = 0
        for raw in items:
            try:
                rel = self._write_one(raw)
                if rel:
                    written.append(rel)
                else:
                    skipped += 1
            except Exception:
                skipped += 1
        return {
            "run_dir": str(self.run_dir),
            "written": len(written),
            "skipped": skipped,
            "paths": written[:50],
        }

    def _write_one(self, raw: dict[str, Any]) -> str | None:
        kind = str(raw.get("capture_kind") or "").strip().lower()
        if kind not in _KINDS or kind == "unknown":
            return None
        parsed = raw.get("parsed")
        if not isinstance(parsed, dict):
            return None
        if kind == "list" and not _parsed_has_ware_list(parsed):
            return None
        url = str(raw.get("url") or "")
        resolved = str(raw.get("resolved_sku") or "").strip()
        if not resolved and kind != "list":
            resolved = _resolve_sku_from_url_and_parsed(url, parsed)

        self._seq[kind] = self._seq.get(kind, 0) + 1
        n = self._seq[kind]
        sku_part = f"_sku_{_slug_sku(resolved)}" if kind in ("detail", "comment", "graphic") and resolved else ""
        fname = f"jd_{kind}_{n:04d}{sku_part}_kw_{self.safe_kw}.json"

        sub = self.run_dir / kind
        sub.mkdir(parents=True, exist_ok=True)
        path = sub / fname

        envelope: dict[str, Any] = {
            "keyword": str(raw.get("keyword") or self.keyword),
            "capture_kind": kind,
            "resolved_sku": resolved,
            "function_id": str(raw.get("function_id") or _function_id_from_url(url) or ""),
            "url": url,
            "status": int(raw.get("status") or 200),
            "method": str(raw.get("method") or "GET"),
            "parsed": parsed,
        }
        lk = str(raw.get("list_keyword") or "").strip()
        if not lk:
            data = parsed.get("data")
            if isinstance(data, dict):
                lk = str(data.get("listKeyWord") or "").strip()
        if lk:
            envelope["list_keyword"] = lk

        path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return str(path.relative_to(self.run_dir)).replace("\\", "/")


def _function_id_from_url(url: str) -> str | None:
    if not url:
        return None
    qs = parse_qs(urlparse(url).query)
    for key in ("functionId", "functionid"):
        vals = qs.get(key)
        if vals and vals[0]:
            return vals[0].strip()
    return None


def _jd_url_body_dict(url: str) -> dict[str, Any]:
    try:
        qs = parse_qs(urlparse((url or "").strip()).query, keep_blank_values=False)
        raw = (qs.get("body") or [None])[0]
        if not raw:
            return {}
        obj = json.loads(unquote(str(raw)))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _resolve_sku_from_url_and_parsed(url: str, parsed: dict[str, Any]) -> str:
    bd = _jd_url_body_dict(url)
    for k in ("skuId", "wareId"):
        s = str(bd.get(k) or "").strip()
        if s.isdigit() and len(s) >= 5:
            return s
    dat = parsed.get("data")
    if isinstance(dat, dict):
        for k in ("skuId", "wareId"):
            s = str(dat.get(k) or "").strip()
            if s.isdigit() and len(s) >= 5:
                return s
    for k in ("skuId", "wareId"):
        s = str(parsed.get(k) or "").strip()
        if s.isdigit() and len(s) >= 5:
            return s
    return ""
