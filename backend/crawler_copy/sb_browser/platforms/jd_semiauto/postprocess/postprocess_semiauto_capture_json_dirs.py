# -*- coding: utf-8 -*-
"""写 CSV 前：回填 dedupe_key、同键只留首份、商详首部补配料。监听阶段不写键。"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

from .capture_dedupe_key import (
    _detail_sku_id,
    semantic_dedupe_key_for_saved_capture_blob,
)
from .semiauto_detail_ingredients import (
    detail_ingredients_text_needs_refill,
    recognize_detail_ingredients_with_urls_joined,
    sku_to_graphic_urls_joined_from_run_dir,
)


def _write_blob(path: Path, blob: dict[str, Any], *, indent: bool = True) -> None:
    body = json.dumps(blob, ensure_ascii=False, indent=2) + "\n" if indent else (
        json.dumps(blob, ensure_ascii=False) + "\n"
    )
    path.write_text(body, encoding="utf-8")


def _pick_dest_free(dest_root: Path, name: str) -> Path:
    dest = dest_root / name
    if not dest.exists():
        return dest
    stem, suf = Path(name).stem, Path(name).suffix
    for n in range(1, 10_000):
        cand = dest_root / f"{stem}__dup{n}{suf}"
        if not cand.exists():
            return cand
    raise OSError(f"无法为归档文件腾出唯一路径: {name}")


def _archive_duplicate_json(path: Path, run_dir: Path, kind: str, stats: dict[str, int]) -> None:
    dest_root = run_dir / "_postprocess_superseded" / kind
    dest_root.mkdir(parents=True, exist_ok=True)
    dest = _pick_dest_free(dest_root, path.name)
    shutil.move(str(path), str(dest))
    k = f"{kind}_duplicate_archived"
    stats[k] = stats.get(k, 0) + 1


def _postprocess_list_comment_unknown(rd: Path, kind: str, stats: dict[str, int]) -> None:
    kd = rd / kind
    if not kd.is_dir():
        return
    seen: set[str] = set()
    for path in sorted(kd.glob("*.json")):
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [post_semiauto] 跳过 {kind}/{path.name}: {e}", file=sys.stderr)
            continue
        dk = semantic_dedupe_key_for_saved_capture_blob(blob)
        if not dk:
            continue
        if dk in seen:
            _archive_duplicate_json(path, rd, kind, stats)
            continue
        seen.add(dk)
        dirty = blob.get("dedupe_key") != dk
        blob["dedupe_key"] = dk
        if dirty:
            _write_blob(path, blob)
            kk = f"{kind}_dedupe_key_written"
            stats[kk] = stats.get(kk, 0) + 1


def postprocess_semiauto_capture_json_dirs(run_dir: Path) -> dict[str, int]:
    stats: dict[str, int] = {}
    rd = Path(run_dir).expanduser().resolve()

    for k in ("list", "comment", "unknown", "graphic"):
        _postprocess_list_comment_unknown(rd, k, stats)

    sku_graphic_urls = sku_to_graphic_urls_joined_from_run_dir(rd)
    seen_detail_keys: set[str] = set()
    dd = rd / "detail"
    if not dd.is_dir():
        stats["detail_dir_missing"] = 1

    if dd.is_dir():
        for path in sorted(dd.glob("*.json")):
            try:
                blob = json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"  [post_semiauto] 跳过 detail/{path.name}: {e}", file=sys.stderr)
                continue
            dk = semantic_dedupe_key_for_saved_capture_blob(blob)
            if not dk:
                continue
            if dk in seen_detail_keys:
                _archive_duplicate_json(path, rd, "detail", stats)
                continue
            seen_detail_keys.add(dk)

            dirty = blob.get("dedupe_key") != dk
            blob["dedupe_key"] = dk

            ing_text = str(blob.get("semiauto_detail_ingredients_text") or "")
            if detail_ingredients_text_needs_refill(ing_text):
                dsku = _detail_sku_id(
                    str(blob.get("resolved_sku") or ""),
                    str(blob.get("url") or ""),
                    blob.get("parsed"),
                )
                urls = sku_graphic_urls.get(dsku, "") if dsku else ""
                if not urls:
                    stats["detail_ingredients_no_graphic_urls"] = (
                        stats.get("detail_ingredients_no_graphic_urls", 0) + 1
                    )
                t, src = recognize_detail_ingredients_with_urls_joined(urls)
                blob["semiauto_detail_ingredients_text"] = t
                blob["semiauto_detail_ingredients_source_url"] = src or ""
                dirty = True
                stats["detail_ingredients_filled"] = (
                    stats.get("detail_ingredients_filled", 0) + 1
                )
            else:
                stats["detail_ingredients_skip_nonempty"] = (
                    stats.get("detail_ingredients_skip_nonempty", 0) + 1
                )

            if dirty:
                _write_blob(path, blob)

    summ = ", ".join(f"{k}={v}" for k, v in sorted(stats.items()) if v)
    print(f"  [post_semiauto] {rd.name}: {summ or 'no updates'}", file=sys.stderr)
    return stats


__all__ = ["postprocess_semiauto_capture_json_dirs"]
