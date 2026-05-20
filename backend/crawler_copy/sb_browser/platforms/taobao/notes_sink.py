# -*- coding: utf-8 -*-
"""
淘宝 mtop JSON 落盘（来源：CDP **只读监听** — ``Network.responseReceived`` 登记请求、
``Network.getResponseBody`` 复制浏览器**真实响应正文**，**非** Fetch 改写、**非** 本地 JSON 重放）：**仅**本平台脚本引用。

默认写入 ``data/TB/sb_cdp_mtop_raw/<YYYYMMDDHHMMSS>/``（按运行时间与历次结果拆分），文件名：
``mtop_{seq}_req_kw_{slug}_t{stamp}.json``。

默认可只落「主 SRP 大包」（``parsed.data`` 含 ``iconStyle``），与预加载等小应答区分，
见 ``is_tb_main_wireless_bundle`` 与 ``constants_tb.TB_MTOP_SAVE_*``。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from sb_browser.cdp_json_listen import CapturedJsonResponse

from ._low_gi_root import tb_data_dir


def default_mtop_raw_dir() -> Path:
    return tb_data_dir() / "sb_cdp_mtop_raw"


def is_tb_main_wireless_bundle(
    row: CapturedJsonResponse,
    *,
    require_data_keys: Sequence[str] | None = None,
) -> bool:
    """
    区分「主 SRP 样式大包」（如磁盘上体量大的 ``mtop_007``）与同 path、短 URL 的预加载等小应答：
    要求响应无 JSON 解析错误，且 ``parsed['data']`` 为 dict，并包含 ``require_data_keys``
    （默认取自 ``constants_tb.TB_MTOP_SAVE_REQUIRE_DATA_KEYS``，常为 ``iconStyle``）。

    依赖淘宝当前返回结构；若接口改版，改常量中的键集合。
    """
    if row.parse_error:
        return False
    p = row.parsed
    if not isinstance(p, dict):
        return False
    d = p.get("data")
    if not isinstance(d, dict):
        return False
    keys = require_data_keys
    if keys is None:
        from . import constants_tb as _tc

        keys = getattr(_tc, "TB_MTOP_SAVE_REQUIRE_DATA_KEYS", ("iconStyle",))
    return all(k in d for k in keys)


def _slug_kw_for_filename(kw: str, *, max_chars: int = 48) -> str:
    chunks: list[str] = []
    for c in (kw or "").strip()[:max_chars]:
        if c in '<>:"/\\|?*' or ord(c) < 32:
            chunks.append("_")
        else:
            chunks.append(c)
    s = "".join(chunks).strip("._ ").strip("_")
    return s or "kw"


def save_mtop_captures(
    captures: list[CapturedJsonResponse],
    *,
    keyword: str,
    raw_dir: Path | str | None = None,
    stamp: str | None = None,
    pretty: bool = True,
    only_main_bundle: bool | None = None,
    require_data_keys: Sequence[str] | None = None,
    save_run_dir_by_time: bool | None = None,
    export_csv_after_save: bool | None = None,
) -> list[Path]:
    """写盘；可按 ``only_main_bundle`` 过滤；可按 ``save_run_dir_by_time`` 使用 ``sb_cdp_mtop_raw/<stamp>/``。

    ``export_csv_after_save``：若为 ``None``，则沿用 ``constants_tb.TB_MTOP_EXPORT_CSV_AFTER_JSON``；
    为真时在本次写入的目录合并导出 ``mtop_items_*_t*.csv``（见 ``mtop_json_to_csv``）。
    """
    if not captures:
        return []
    try:
        from . import constants_tb as _tc
    except Exception:
        _tc = None

    stamp_eff = stamp if stamp is not None else datetime.now().strftime("%Y%m%d_%H%M%S")

    if only_main_bundle is None:
        only_main_bundle = bool(
            getattr(_tc, "TB_MTOP_SAVE_ONLY_MAIN_BUNDLE", True) if _tc is not None else True,
        )
    if require_data_keys is None and _tc is not None:
        require_data_keys = tuple(
            getattr(_tc, "TB_MTOP_SAVE_REQUIRE_DATA_KEYS", ("iconStyle",)),
        )
    if save_run_dir_by_time is None:
        save_run_dir_by_time = bool(
            getattr(_tc, "TB_MTOP_SAVE_RUN_DIR_BY_TIME", True)
            if _tc is not None
            else True,
        )
    if export_csv_after_save is None:
        export_csv_after_save = bool(
            getattr(_tc, "TB_MTOP_EXPORT_CSV_AFTER_JSON", True)
            if _tc is not None
            else True,
        )

    rows = list(captures)
    if only_main_bundle:
        rows = [
            r
            for r in rows
            if is_tb_main_wireless_bundle(r, require_data_keys=require_data_keys)
        ]
    if not rows:
        print(
            "[tb] 无符合条件的主包条目可写（可关 TB_MTOP_SAVE_ONLY_MAIN_BUNDLE 或放宽键）",
            file=sys.stderr,
        )
        return []

    base_rd = Path(raw_dir).expanduser().resolve() if raw_dir else default_mtop_raw_dir()
    rd = base_rd / stamp_eff if save_run_dir_by_time else base_rd
    rd.mkdir(parents=True, exist_ok=True)
    if save_run_dir_by_time:
        print(f"[tb] 落盘子目录（按本次运行时间戳）: {rd}", file=sys.stderr)
    safe_kw = _slug_kw_for_filename(keyword or "")

    paths: list[Path] = []
    for seq, row in enumerate(rows, start=1):
        ext = "json" if pretty else "js"
        path = rd / f"mtop_{seq:03d}_req_kw_{safe_kw}_t{stamp_eff}.{ext}"
        blob = {
            "keyword": keyword,
            "capture_index": seq,
            "stamp": stamp_eff,
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
        print(f"[tb] 已保存 mtop JSON：{path}", file=sys.stderr)

    if paths and export_csv_after_save:
        from .mtop_json_to_csv import export_json_paths_to_csv

        try:
            export_json_paths_to_csv(
                paths,
                keyword=keyword,
                stamp=stamp_eff,
                safe_keyword_slug=safe_kw,
            )
        except Exception as e:
            print(f"[tb] mtop CSV 导出失败：{e}", file=sys.stderr)
    return paths
