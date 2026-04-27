# -*- coding: utf-8 -*-
"""
评价扁平结果**落盘**（UTF-8 BOM CSV / JSONL 行追加）。

解析见 ``jd_item_comment_parse``。
"""
from __future__ import annotations

import csv
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Any

_BACKEND_ROOT = Path(__file__).resolve().parents[3]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))
from pipeline.csv.schema import COMMENT_CSV_COLUMNS, COMMENT_ROW_DICT_KEYS  # noqa: E402


def write_comments_flat_csv(path: Path | str, rows: list[dict[str, Any]]) -> None:
    """与 COMMENTS_OUT 为 ``.csv`` 时相同格式（UTF-8 BOM），供流水线等复用。"""
    _write_comments_csv(Path(path), rows)


def _write_comments_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = StringIO()
    fn = list(COMMENT_CSV_COLUMNS)
    w = csv.DictWriter(buf, fieldnames=fn, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        line: dict[str, Any] = {}
        for h, api_k in zip(COMMENT_CSV_COLUMNS, COMMENT_ROW_DICT_KEYS):
            if api_k == "largePicURLs":
                line[h] = json.dumps(r.get("largePicURLs") or [], ensure_ascii=False)
            else:
                line[h] = r.get(api_k, "")
        w.writerow(line)
    path.write_text("\ufeff" + buf.getvalue(), encoding="utf-8")


def append_comments_jsonl(f: Any, rows: list[dict[str, Any]]) -> None:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
