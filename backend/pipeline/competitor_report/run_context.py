"""运行目录解析、关键词推断、pc_search_raw 检索规模读取。"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


def _run_batch_label(run_dir: Path) -> str:
    name = run_dir.name
    m = re.match(r"^(\d{8})_(\d{6})_", name)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return name


def _resolve_existing_run_dir(raw: str | Path | None) -> Path | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    p = Path(s).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    return p


def _infer_keyword(run_dir: Path, meta: dict[str, Any] | None) -> str:
    if meta:
        k = str(meta.get("keyword") or "").strip()
        if k:
            return k
    m = re.match(r"^\d{8}_\d{6}_(.+)$", run_dir.name)
    if m:
        return m.group(1).strip()
    return ""


def _pc_search_result_count_from_raw(
    run_dir: Path,
) -> tuple[int | None, str, list[int], int, int]:
    """
    从 ``pc_search_raw/*.json`` 读取 ``data.resultCount``（京东 PC 搜索接口返回的检索命中规模）。
    多文件时取众数；返回 (众数值, data.listKeyWord 首见值, 出现过的不同取值升序, 解析到的样本文件数)。
    """
    raw_dir = run_dir / "pc_search_raw"
    if not raw_dir.is_dir():
        return None, "", [], 0, 0
    counts: list[int] = []
    list_kw = ""
    n_files = 0
    for p in sorted(raw_dir.glob("*.json")):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeError):
            continue
        n_files += 1
        if not isinstance(obj, dict):
            continue
        data = obj.get("data")
        if not isinstance(data, dict):
            continue
        rc = data.get("resultCount")
        val: int | None = None
        if isinstance(rc, int) and not isinstance(rc, bool) and rc >= 0:
            val = rc
        elif isinstance(rc, str) and rc.strip().isdigit():
            val = int(rc.strip())
        if val is not None:
            counts.append(val)
        lk = data.get("listKeyWord")
        if isinstance(lk, str) and lk.strip() and not list_kw:
            list_kw = lk.strip()
    if not counts:
        return None, list_kw, [], n_files, 0
    consensus_rc, _freq = Counter(counts).most_common(1)[0]
    uniques = sorted(set(counts))
    return consensus_rc, list_kw, uniques, n_files, len(counts)


__all__ = [
    "_infer_keyword",
    "_pc_search_result_count_from_raw",
    "_resolve_existing_run_dir",
    "_run_batch_label",
]
