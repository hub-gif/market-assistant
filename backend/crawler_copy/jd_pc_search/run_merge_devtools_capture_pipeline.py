# -*- coding: utf-8 -*-
"""
将「Chrome DevTools → 复制为文本」导出的 **含完整响应体** 的 ``.txt``，按 ``jd_keyword_pipeline``
与 ``jd_pipeline_export`` 的规则整理为：

- ``pc_search_export.csv``
- ``detail_ware_export.csv``
- ``comments_flat.csv``
- ``keyword_pipeline_merged.csv``
- ``detail/ware_{sku}_response.json``（规范化的商详 JSON，便于与流水线一致再跑购买者摘要）
- ``run_meta.json``

目录内可放多份 txt；按请求 URL / 正文自动分为 **列表 / 详情 / 评论**。若未显式传 ``--keyword``，
会尝试从 ``pc_search_searchWare`` 请求 URL 的 ``keyword`` 参数解码。

用法（在 ``backend/crawler_copy/jd_pc_search`` 下）::

  python run_merge_devtools_capture_pipeline.py --input-dir path/to/json_folder

或指定具体文件::

  python run_merge_devtools_capture_pipeline.py \\
    --list-file jd列表.txt --detail-file 详情.txt --comment-file 评论.txt \\
    --output-dir path/to/out_run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

_BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

_CRAWLER_COPY = Path(__file__).resolve().parents[1]
if str(_CRAWLER_COPY) not in sys.path:
    sys.path.insert(0, str(_CRAWLER_COPY))

_JD_PC_SEARCH_DIR = Path(__file__).resolve().parent
if str(_JD_PC_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH_DIR))

for _sub in ("search", "comment", "detail"):
    _pkg = (_JD_PC_SEARCH_DIR / _sub).resolve()
    if str(_pkg) not in sys.path:
        sys.path.insert(0, str(_pkg))

from jd_detail_buyer_extraction import (  # noqa: E402
    buyer_promo_text_from_profile,
    buyer_ranking_line_from_profile,
    extract_buyer_offer_profile_from_json_text,
)
from jd_detail_ware_parse import (  # noqa: E402
    detail_ware_lean_csv_row,
    format_ware_response_for_save,
    parse_ware_business_response_text,
)
from jd_item_comment_export import write_comments_flat_csv  # noqa: E402
from jd_item_comment_parse import extract_comment_rows_from_parsed  # noqa: E402
from jd_pipeline_export import (  # noqa: E402
    comment_fields_from_rows,
    dedupe_comment_rows,
    finalize_merged_row_for_disk,
    write_detail_ware_csv,
    write_merged_csv,
    write_pc_search_export_csv,
    write_run_meta_json,
)
from jd_h5_search_parse import (  # noqa: E402
    CSV_FIELDS,
    jd_row_to_export,
    parse_items_from_response_body,
)
from pipeline.csv.schema import JD_SEARCH_CSV_HEADERS as JD_EXPORT_COLUMN_HEADERS  # noqa: E402

from sb_browser.platforms.jd_semiauto.devtools_txt.devtools_cn_export_parse import (  # noqa: E402
    extract_request_url_from_devtools_cn_export,
    infer_capture_kind,
    sku_from_warebusiness_get_url,
)


def _extract_largest_json_value(text: str) -> Any | None:
    """从整段 DevTools 文本中取出体积最大的 JSON 对象/数组（通常为接口响应体）。"""
    raw = text or ""
    if not raw.strip():
        return None
    dec = json.JSONDecoder()
    best: Any | None = None
    best_span = 0
    i = 0
    n = len(raw)
    while i < n:
        c = raw[i]
        if c not in "{[":
            i += 1
            continue
        try:
            obj, end = dec.raw_decode(raw, i)
        except json.JSONDecodeError:
            i += 1
            continue
        span = end - i
        if span > best_span and isinstance(obj, (dict, list)):
            best_span = span
            best = obj
        i = end
    return best


def _keyword_from_list_url(url: str) -> str:
    q = parse_qs(urlparse(url).query)
    k = (q.get("keyword") or [""])[0]
    return unquote(str(k).strip()) if k else ""


def _sku_from_comment_post_export(text: str) -> str:
    """
    从 ``fetch(..., { "body": "...functionId=getCommentListPage..." })`` 或纯文本 form 里取 sku。
    """
    m = re.search(r"[%22']sku[%22']\s*:\s*[%22'](\d{5,20})[%22']", text)
    if m:
        return m.group(1)
    m2 = re.search(r'(?:^|[&])sku=(\d{5,20})(?:&|$)', text)
    if m2:
        return m2.group(1)
    m3 = re.search(r'%22sku%22%3A%22(\d{5,20})%22', text)
    if m3:
        return m3.group(1)
    return ""


def _dense_pc_search_export_row(internal: dict[str, str]) -> dict[str, str]:
    ex = jd_row_to_export(internal)
    return {cn: str(ex.get(cn, "") or "") for cn in CSV_FIELDS}


def _classify_txt(path: Path, text: str) -> str:
    url = extract_request_url_from_devtools_cn_export(text)
    k = infer_capture_kind(url).lower().strip()
    if k == "graphic" or "pc_item_getwaregraphic" in (url or "").lower():
        return "graphic"
    if k == "detail" or "pc_detailpage_warebusiness" in (url or "").lower():
        return "detail"
    if (
        k == "comment"
        or "getcommentlistpage" in (url or "").lower()
        or "getlegowaredetailcomment" in (url or "").lower()
    ):
        return "comment"
    if k == "list" or "searchware" in (url or "").lower() or "pc_search" in (url or "").lower():
        return "list"
    huge = _extract_largest_json_value(text)
    if isinstance(huge, dict):
        if huge.get("userInfo") is not None and huge.get("pageConfigVO") is not None:
            return "detail"
        if str(huge.get("code")) == "0" and huge.get("result") is not None:
            if any(
                str(x.get("mId") or "").startswith("commentlist")
                for x in _floors_walk(huge)
            ):
                return "comment"
        if huge.get("data") is not None or "searchWareList" in json.dumps(huge, ensure_ascii=False)[
            :80_000
        ]:
            return "list"
    return k or "unknown"


def _floors_walk(obj: Any) -> list[dict[str, Any]]:
    """浅层遍历可能含 floors 的树，返回见到的 dict。"""
    out: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        out.append(obj)
        fl = obj.get("floors")
        if isinstance(fl, list):
            for x in fl:
                if isinstance(x, dict):
                    out.extend(_floors_walk(x))
        for _k, v in obj.items():
            if _k != "floors" and isinstance(v, (dict, list)):
                out.extend(_floors_walk(v))
    elif isinstance(obj, list):
        for x in obj:
            out.extend(_floors_walk(x))
    return out


def _collect_txts(inp: Path, patterns: tuple[str, ...]) -> list[Path]:
    if not inp.is_dir():
        return []
    got: list[Path] = []
    for pat in patterns:
        got.extend(sorted(inp.glob(pat)))
    # 稳定去重
    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in got:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)
    return uniq


def main() -> int:
    ap = argparse.ArgumentParser(description="合并 DevTools 抓包 txt 为流水线 CSV")
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="扫描目录内的 .txt（与默认命名匹配或全部）",
    )
    ap.add_argument("--list-file", type=Path, action="append", default=[])
    ap.add_argument("--detail-file", type=Path, action="append", default=[])
    ap.add_argument("--comment-file", type=Path, action="append", default=[])
    ap.add_argument("--keyword", type=str, default="", help="流水线关键词（缺省则从列表请求 URL 解析）")
    ap.add_argument("--page", type=int, default=1, help="搜索列表解析所用的逻辑页码")
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="输出目录（默认：<input-dir 父>/merged_<timestamp>_<keyword>）",
    )
    ap.add_argument("--merged-mode", choices=("lean", "full"), default="lean")
    ap.add_argument("--detail-mode", choices=("lean", "full"), default="lean")
    args = ap.parse_args()

    txt_paths: list[Path] = []
    for lst in (
        args.list_file,
        args.detail_file,
        args.comment_file,
    ):
        for p in lst:
            rp = Path(p).expanduser().resolve()
            if rp.is_file():
                txt_paths.append(rp)

    inp_dir = Path(args.input_dir).expanduser().resolve() if args.input_dir else None
    if inp_dir and inp_dir.is_dir():
        for p in _collect_txts(
            inp_dir,
            ("*.txt", "jd列表.txt", "详情.txt", "评论.txt"),
        ):
            if p not in txt_paths and p.resolve() not in {x.resolve() for x in txt_paths}:
                txt_paths.append(p)

    if not txt_paths:
        print("未找到可用的 .txt，请传入 --input-dir 或 *_file。", file=sys.stderr)
        return 2

    by_kind: dict[str, list[tuple[Path, str]]] = {"list": [], "detail": [], "comment": []}
    inferred_kw = (args.keyword or "").strip()

    for p in txt_paths:
        try:
            text = p.read_text(encoding="utf-8")
        except OSError as e:
            print(f"跳过读取失败 {p}: {e}", file=sys.stderr)
            continue
        kind = _classify_txt(p, text)
        url = extract_request_url_from_devtools_cn_export(text)
        if kind == "list" and url and not inferred_kw:
            inferred_kw = _keyword_from_list_url(url)
        elif kind == "unknown":
            print(f"WARN 无法归类（按 unknown 跳过）: {p.name}", file=sys.stderr)
            continue
        if kind != "unknown":
            by_kind.setdefault(kind, []).append((p, text))

    kw = inferred_kw.strip() if not (args.keyword or "").strip() else (args.keyword or "").strip()
    if not kw:
        kw = "unknown_keyword"
        print("WARN 未解析到 keyword，流水线关键词列为 unknown_keyword。", file=sys.stderr)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_kw = re.sub(r'[<>:"/\\\\|?*]', "_", kw)[:48]
    if args.output_dir:
        run_dir = Path(args.output_dir).expanduser().resolve()
    elif inp_dir:
        run_dir = (inp_dir.parent / f"{stamp}_{safe_kw}_merged").resolve()
    else:
        run_dir = (txt_paths[0].parent / f"{stamp}_{safe_kw}_merged").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    detail_dir = run_dir / "detail"
    detail_dir.mkdir(parents=True, exist_ok=True)

    # --- List ---
    internal_rows_all: list[dict[str, str]] = []
    for _p, t in by_kind.get("list") or []:
        page_n = int(args.page or 1)
        rows = parse_items_from_response_body(t, keyword=kw, page=page_n)
        if not rows:
            huge = _extract_largest_json_value(t)
            if huge is not None:
                rows = parse_items_from_response_body(
                    json.dumps(huge, ensure_ascii=False),
                    keyword=kw,
                    page=page_n,
                )
        if not rows:
            print(f"WARN 列表未解析出商品行: {_p}", file=sys.stderr)
            continue
        internal_rows_all.extend(rows)
    # 列表内 SKU 去重保留首次
    seen_sku_list: set[str] = set()
    internal_unique: list[dict[str, str]] = []
    for r in internal_rows_all:
        sid = str(r.get("sku_id") or "").strip()
        if not sid or sid in seen_sku_list:
            continue
        seen_sku_list.add(sid)
        internal_unique.append(r)
    export_rows_full = [_dense_pc_search_export_row(r) for r in internal_unique]

    # --- Detail (sku -> response text + flat) ---
    detail_text_by_sku: dict[str, str] = {}
    for _p, t in by_kind.get("detail") or []:
        url = extract_request_url_from_devtools_cn_export(t)
        sku_hint = sku_from_warebusiness_get_url(url)
        parsed = _extract_largest_json_value(t)
        if parsed is None:
            print(f"WARN 详情未解析到 JSON: {_p}", file=sys.stderr)
            continue
        body_str = json.dumps(parsed, ensure_ascii=False)
        sku = sku_hint
        if not sku and isinstance(parsed, dict):
            wim = parsed.get("wareInfoReadMap") or {}
            if isinstance(wim, dict):
                sku = str(wim.get("product_id") or wim.get("sku_id") or "").strip()
        if not sku:
            print(f"WARN 详情无法判定 SKU（跳过保存）: {_p}", file=sys.stderr)
            continue
        detail_text_by_sku[str(sku)] = body_str

    # --- Comment ---
    comments_by_anchor_sku: dict[str, list[dict[str, Any]]] = {}
    global_comment_accum: list[dict[str, Any]] = []

    def _consume_comment_anchor(anchor_sku: str, payload: dict[str, Any]) -> None:
        rows_raw = extract_comment_rows_from_parsed(anchor_sku, payload)
        rows = dedupe_comment_rows(rows_raw)
        comments_by_anchor_sku.setdefault(str(anchor_sku).strip(), []).extend(rows)

    for _p, t in by_kind.get("comment") or []:
        anchor = _sku_from_comment_post_export(t)
        parsed = _extract_largest_json_value(t)
        if not anchor or not isinstance(parsed, dict):
            print(f"WARN 评论未解析 SKU 或非对象 JSON: {_p}", file=sys.stderr)
            continue
        _consume_comment_anchor(anchor, parsed)
        rows_raw = extract_comment_rows_from_parsed(anchor, parsed)
        rows = dedupe_comment_rows(rows_raw)
        global_comment_accum.extend(rows)

    merged_rows: list[dict[str, str]] = []
    detail_csv_rows: list[dict[str, str]] = []

    buyer_promo_cache: dict[str, tuple[str, str]] = {}
    for cap_sku, d_text in sorted(detail_text_by_sku.items()):
        rline, ptext = "", ""
        if (d_text or "").strip():
            try:
                formatted = format_ware_response_for_save(
                    d_text, normalize=True, sort_keys=True, indent=2
                )
                (detail_dir / f"ware_{cap_sku}_response.json").write_text(
                    formatted + "\n", encoding="utf-8"
                )
                _prof = extract_buyer_offer_profile_from_json_text(formatted)
                rline = buyer_ranking_line_from_profile(_prof)
                ptext = buyer_promo_text_from_profile(_prof)
            except OSError:
                pass
            except Exception:
                pass
        buyer_promo_cache[cap_sku] = (rline, ptext)
        detail_csv_rows.append(
            detail_ware_lean_csv_row(
                cap_sku,
                200,
                d_text or "",
                detail_body_ingredients="",
                buyer_ranking_line=rline,
                buyer_promo_text=ptext,
            )
        )

    listed_skus_set = {
        str(r.get("sku_id") or "").strip() for r in internal_unique if r.get("sku_id")
    }

    def _finalize_one_merged_row(
        merged_local: dict[str, str],
        sku_key: str,
        d_body: str,
    ) -> dict[str, str]:
        ware_flat_loc, _w = parse_ware_business_response_text(d_body or "")
        merged_local.update(ware_flat_loc)
        merged_local["detail_body_ingredients"] = ""
        merged_local["detail_body_ingredients_source_url"] = ""
        rline_loc, ptext_loc = buyer_promo_cache.get(sku_key, ("", ""))
        merged_local["buyer_ranking_line"] = rline_loc
        merged_local["buyer_promo_text"] = ptext_loc
        crows_loc = dedupe_comment_rows(comments_by_anchor_sku.get(sku_key, []) or [])
        merged_local.update(comment_fields_from_rows(crows_loc))
        finalize_merged_row_for_disk(merged_local)
        return merged_local

    for internal in internal_unique:
        sku = str(internal.get("sku_id") or "").strip()
        search_row = _dense_pc_search_export_row(internal)
        merged = {k: str(search_row.get(k) or "") for k in CSV_FIELDS}
        merged["流水线关键词"] = kw

        d_text = detail_text_by_sku.get(sku, "")
        merged_rows.append(_finalize_one_merged_row(merged, sku, d_text))

    orphans = sorted(
        (set(detail_text_by_sku.keys()) | set(comments_by_anchor_sku.keys()))
        - listed_skus_set
    )
    for sku in orphans:
        merged = {cn: "" for cn in CSV_FIELDS}
        merged[JD_EXPORT_COLUMN_HEADERS["sku_id"]] = sku
        merged[JD_EXPORT_COLUMN_HEADERS["keyword"]] = kw
        merged[JD_EXPORT_COLUMN_HEADERS["page"]] = str(int(args.page or 1))
        merged[JD_EXPORT_COLUMN_HEADERS["platform"]] = "京东"
        merged["流水线关键词"] = kw
        merged_rows.append(
            _finalize_one_merged_row(merged, sku, detail_text_by_sku.get(sku, ""))
        )

    # 若无列表数据，则用「仅有详情 / 评论 SKU」兜底生成合并行
    if not merged_rows and (detail_text_by_sku or comments_by_anchor_sku):
        for sku in sorted(
            set(detail_text_by_sku.keys()) | set(comments_by_anchor_sku.keys())
        ):
            merged = {cn: "" for cn in CSV_FIELDS}
            merged[JD_EXPORT_COLUMN_HEADERS["sku_id"]] = sku
            merged[JD_EXPORT_COLUMN_HEADERS["keyword"]] = kw
            merged[JD_EXPORT_COLUMN_HEADERS["page"]] = str(int(args.page or 1))
            merged[JD_EXPORT_COLUMN_HEADERS["platform"]] = "京东"
            merged["流水线关键词"] = kw
            merged_rows.append(
                _finalize_one_merged_row(merged, sku, detail_text_by_sku.get(sku, ""))
            )
    pc_path = run_dir / "pc_search_export.csv"
    merged_path = run_dir / "keyword_pipeline_merged.csv"
    dc_path = run_dir / "detail_ware_export.csv"
    cc_path = run_dir / "comments_flat.csv"

    write_pc_search_export_csv(pc_path, export_rows_full)

    cols_m, ncol_m = write_merged_csv(
        merged_path, merged_rows, merged_csv_mode=args.merged_mode
    )
    cols_d, ncol_d = write_detail_ware_csv(
        dc_path, detail_csv_rows, detail_ware_csv_mode=args.detail_mode
    )
    gc = dedupe_comment_rows(global_comment_accum)
    write_comments_flat_csv(cc_path, gc)

    meta = {
        "source": "run_merge_devtools_capture_pipeline.py",
        "keyword": kw,
        "inputs": [str(p) for p in txt_paths],
        "list_sources": [str(x[0]) for x in by_kind.get("list") or []],
        "detail_skus": sorted(detail_text_by_sku.keys()),
        "comment_anchor_skus": sorted(comments_by_anchor_sku.keys()),
        "merged_rows": len(merged_rows),
        "merged_csv_mode": args.merged_mode,
        "detail_ware_rows": len(detail_csv_rows),
        "comments_flat_rows": len(gc),
        "pc_search_rows": len(export_rows_full),
    }
    write_run_meta_json(run_dir / "run_meta.json", meta)

    print(
        f"已写入 {run_dir}：合并表 {len(merged_rows)} 行 / 详情 CSV {len(detail_csv_rows)} 行 "
        f"/ 评价 {len(gc)} 行 / 列表 {len(export_rows_full)} 行。"
        f"\n  merged 列={ncol_m}; detail_csv 列={ncol_d}",
        file=sys.stderr,
    )
    print(str(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
