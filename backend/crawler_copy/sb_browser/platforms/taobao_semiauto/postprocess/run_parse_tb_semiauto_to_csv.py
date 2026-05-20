# -*- coding: utf-8 -*-
"""淘宝 Playwright 半自动 run_dir → 与京东同结构的 CSV（``pc_search_export`` / ``detail_ware_export`` / ``comments_flat`` / ``keyword_pipeline_merged``）。

**去重口径（淘宝）**：以主商品数字 ID（``item_id`` / 商详 URL 的 ``id`` / 评价的 ``auctionNumId``）为唯一键；
``pc_search_export``、``detail_ware_export``、``keyword_pipeline_merged`` 均为**一商品一行**。``comments_flat`` 仍为一条评价一行。
写出前会将单元格内换行折叠为单行（避免一条记录在文件里占用多行物理行）。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def _bootstrap() -> Path:
    """补齐 ``backend`` / ``jd_pc_search``（search、detail、d 根）/ ``tb_pc_search/mtop``。"""
    cc = Path(__file__).resolve().parents[4]
    backend = cc.parent
    for p in (str(backend), str(cc)):
        if p not in sys.path:
            sys.path.insert(0, p)
    jd = cc / "jd_pc_search"
    sys.path.insert(0, str((jd / "detail").resolve()))
    sys.path.insert(0, str((jd / "search").resolve()))
    sys.path.insert(0, str(jd.resolve()))
    tb_sub = str((cc / "tb_pc_search" / "mtop").resolve())
    if tb_sub not in sys.path:
        sys.path.insert(0, tb_sub)
    return cc


_bootstrap()

import jd_pipeline_export as _jpex  # noqa: E402
from jd_h5_search_parse import CSV_FIELDS, jd_row_to_export  # noqa: E402
from item_extract import parse_items_from_mtop_payload  # noqa: E402
from pipeline.csv.schema import (  # noqa: E402
    COMMENT_CSV_COLUMNS,
    COMMENT_ROW_DICT_KEYS,
    DETAIL_CSV_COLUMNS,
    JD_SEARCH_CSV_HEADERS as JD_EXPORT,
    LEAN_DETAIL_CSV_HEADERS,
    MERGED_LEAN_DETAIL_INTERNAL_KEYS,
)

from sb_browser.platforms.taobao_semiauto.common.low_gi_root import tb_playwright_semiauto_capture_root  # noqa: E402
from sb_browser.platforms.taobao_semiauto.postprocess import tb_semiauto_detail_html as _detail_html  # noqa: E402
from sb_browser.platforms.taobao_semiauto.postprocess import tb_semiauto_item_row as _item_row  # noqa: E402
from sb_browser.platforms.taobao_semiauto.postprocess import tb_semiauto_rate_comment as _rate_cm  # noqa: E402

comment_fields_from_rows = _jpex.comment_fields_from_rows
dedupe_comment_rows = _jpex.dedupe_comment_rows
finalize_merged_row_for_disk = _jpex.finalize_merged_row_for_disk
write_merged_csv = _jpex.write_merged_csv


def _detail_en_to_cn() -> dict[str, str]:
    return dict(zip(MERGED_LEAN_DETAIL_INTERNAL_KEYS, LEAN_DETAIL_CSV_HEADERS))


def _comment_en_to_cn() -> dict[str, str]:
    return dict(zip(COMMENT_ROW_DICT_KEYS, COMMENT_CSV_COLUMNS))


def _csv_cell_one_line(val: object, *, sep: str = " | ") -> str:
    """将单元格中的换行压成单行，避免 CSV 一条记录占用多行物理行。"""
    s = "" if val is None else str(val)
    t = s.replace("\r\n", "\n").replace("\r", "\n")
    chunks = [p.strip() for p in t.split("\n")]
    chunks = [c for c in chunks if c]
    return sep.join(chunks)


def _row_one_line_physical(row: dict[str, str]) -> dict[str, str]:
    return {k: _csv_cell_one_line(v) for k, v in row.items()}


def _load_blobs(kind_dir: Path) -> list[tuple[Path, dict]]:
    out: list[tuple[Path, dict]] = []
    for f in sorted(kind_dir.glob("*.json")):
        try:
            out.append((f, json.loads(f.read_text(encoding="utf-8"))))
        except Exception as exc:
            print(f"  [tb.parse] 跳过 {f.name}: {exc}", file=sys.stderr)
    return out


def _parse_list(run_dir: Path) -> int:
    kind_dir = run_dir / "list"
    if not kind_dir.is_dir():
        return 0
    blobs = _load_blobs(kind_dir)
    if not blobs:
        return 0

    all_rows: list[dict[str, str]] = []
    seen_item: set[str] = set()

    for page_idx, (fpath, blob) in enumerate(blobs, start=1):
        parsed = blob.get("parsed")
        if not isinstance(parsed, dict):
            print(f"  [tb.list] {fpath.name}: parsed 非 dict，跳过", file=sys.stderr)
            continue
        keyword = _item_row.tb_list_keyword_for_parse(blob, parsed)
        for canon in parse_items_from_mtop_payload(parsed):
            jd_internal = _item_row.tb_canonical_item_to_jd_row(
                canon,
                keyword=keyword,
                page=str(page_idx),
                platform="淘宝",
            )
            item_id = (jd_internal.get("item_id") or "").strip()
            if not item_id:
                continue
            if item_id in seen_item:
                continue
            seen_item.add(item_id)
            exp = jd_row_to_export(jd_internal)
            all_rows.append(exp)

    if not all_rows:
        return 0

    out_path = run_dir / "pc_search_export.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(CSV_FIELDS), restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(_row_one_line_physical(r) for r in all_rows)
    print(f"  [tb.list] {len(all_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


def _parse_detail(run_dir: Path) -> int:
    en_to_cn = _detail_en_to_cn()
    detail_cols = DETAIL_CSV_COLUMNS

    kind_dir = run_dir / "detail"
    if not kind_dir.is_dir():
        return 0
    blobs = _load_blobs(kind_dir)
    if not blobs:
        return 0

    all_rows: list[dict[str, str]] = []
    seen_item: set[str] = set()

    for fpath, blob in blobs:
        body = str(blob.get("body_text") or "")
        url = str(blob.get("url") or "")
        flat_en, _sku_guess = _detail_html.tb_detail_lean_flat_from_html(body, url=url)
        item_id = _detail_html.tb_item_id_from_detail_url(url)
        if not item_id:
            continue
        if item_id in seen_item:
            continue
        seen_item.add(item_id)
        # 与京东 CSV 列兼容：一商品一行时 ``SKU`` 列填主商品 ID（与同店多 SKU 场景的「SKU」字面不同，但与 merged/评价锚点一致）
        cn_row: dict[str, str] = {"SKU": item_id}
        for ik, zh in en_to_cn.items():
            cn_row[zh] = str(flat_en.get(ik) or "")
        all_rows.append(cn_row)

    if not all_rows:
        return 0

    out_path = run_dir / "detail_ware_export.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(detail_cols), restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(_row_one_line_physical(r) for r in all_rows)

    print(f"  [tb.detail] {len(all_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


def _parse_comment(run_dir: Path) -> int:
    cmap = _comment_en_to_cn()
    kind_dir = run_dir / "comment"
    if not kind_dir.is_dir():
        return 0
    blobs = _load_blobs(kind_dir)
    all_rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for fpath, blob in blobs:
        sku_base = (
            str(blob.get("resolved_sku") or "").strip() or _rate_cm.auction_num_id_from_h5_capture_blob(blob)
        )
        for cr in _rate_cm.extract_tb_comment_rows_from_blob(blob):
            sku = sku_base or str(cr.get("sku") or "").strip()
            cid = str(cr.get("commentId") or "").strip()
            dedup = f"{sku}:{cid}" if cid else f"{sku}:{id(cr)}"
            if dedup in seen_ids:
                continue
            seen_ids.add(dedup)
            pics: list = cr.get("largePicURLs") or []
            cn_row: dict[str, str] = {}
            for en_key, cn_header in cmap.items():
                if en_key == "largePicURLs":
                    cn_row[cn_header] = "|".join(str(u) for u in pics)
                elif en_key == "sku":
                    cn_row[cn_header] = sku
                else:
                    cn_row[cn_header] = str(cr.get(en_key) or "")
            all_rows.append(cn_row)

    if not all_rows:
        return 0

    out_path = run_dir / "comments_flat.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=list(COMMENT_CSV_COLUMNS),
            restval="",
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(_row_one_line_physical(r) for r in all_rows)

    print(f"  [tb.comment] {len(all_rows)} 条 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


def _list_keyword(run_dir: Path) -> str:
    kind_dir = run_dir / "list"
    if kind_dir.is_dir():
        for f in sorted(kind_dir.glob("*.json")):
            try:
                blob = json.loads(f.read_text(encoding="utf-8"))
                parsed = blob.get("parsed")
                pw = parsed if isinstance(parsed, dict) else {}
                return _item_row.tb_list_keyword_for_parse(blob, pw)
            except Exception:
                continue
    return "manual"


def _read_pc_search_csv_rows(run_dir: Path) -> list[dict[str, str]]:
    p = run_dir / "pc_search_export.csv"
    if not p.is_file():
        return []
    with p.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _detail_caches_tb(run_dir: Path) -> dict[str, dict[str, str]]:
    """主商品 item_id → 英文 lean 商详字段（每商品只保留首条捕获）。"""
    out: dict[str, dict[str, str]] = {}
    kd = run_dir / "detail"
    if not kd.is_dir():
        return out
    seen: set[str] = set()
    for _fp, blob in _load_blobs(kd):
        url = str(blob.get("url") or "")
        body = str(blob.get("body_text") or "")
        flat_en, _sku_guess = _detail_html.tb_detail_lean_flat_from_html(body, url=url)
        item_id = _detail_html.tb_item_id_from_detail_url(url)
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        out[item_id] = flat_en
    return out


def _comments_grouped_tb(run_dir: Path) -> dict[str, list]:
    out: dict[str, list] = {}
    kd = run_dir / "comment"
    if not kd.is_dir():
        return out
    for _fp, blob in _load_blobs(kd):
        sku = (
            str(blob.get("resolved_sku") or "").strip() or _rate_cm.auction_num_id_from_h5_capture_blob(blob)
        )
        if not sku:
            continue
        rows = []
        for cr in _rate_cm.extract_tb_comment_rows_from_blob(blob):
            d = dict(cr)
            d["sku"] = sku
            rows.append(d)
        out.setdefault(sku, []).extend(rows)

    for sku in list(out.keys()):
        out[sku] = dedupe_comment_rows(out[sku] or [])
    return out


def _parse_merged(run_dir: Path) -> int:
    """以主商品 ``item_id``（与商详 URL id、detail 缓存键一致）为锚，每商品一行 merged。"""
    kw = _list_keyword(run_dir)
    search_rows = _read_pc_search_csv_rows(run_dir)
    flat_by_item = _detail_caches_tb(run_dir)
    comments_by_item = _comments_grouped_tb(run_dir)

    # 仅以「主商品 ID」对齐：SKU 列在京东为规格 ID，在淘宝解析里常与 item 混用—合并时只信任「主商品ID」列去重匹配
    search_by_item: dict[str, dict[str, str]] = {}
    for row in search_rows:
        iid = str(row.get(JD_EXPORT["item_id"]) or "").strip()
        if iid:
            search_by_item.setdefault(iid, row)

    merged_rows: list[dict[str, str]] = []

    def _finalize_one(merged_local: dict[str, str], item_key: str) -> None:
        fe = flat_by_item.get(item_key) or {}
        merged_local.update(fe)
        merged_local["buyer_ranking_line"] = str(fe.get("buyer_ranking_line") or "")
        merged_local["buyer_promo_text"] = str(fe.get("buyer_promo_text") or "")
        crows = dedupe_comment_rows(comments_by_item.get(item_key, []) or [])
        merged_local.update(comment_fields_from_rows(crows))
        finalize_merged_row_for_disk(merged_local)

    for item_id in sorted(flat_by_item.keys()):
        row_src = search_by_item.get(item_id)
        if row_src is not None:
            merged = {k: str(row_src.get(k) or "") for k in CSV_FIELDS}
        else:
            merged = {cn: "" for cn in CSV_FIELDS}
            merged[JD_EXPORT["sku_id"]] = item_id
            merged[JD_EXPORT["item_id"]] = item_id
            merged[JD_EXPORT["keyword"]] = kw
            merged[JD_EXPORT["page"]] = "1"
            merged[JD_EXPORT["platform"]] = "淘宝"
        merged["流水线关键词"] = kw
        _finalize_one(merged, item_id)
        merged_rows.append(merged)

    if not merged_rows:
        return 0

    out_path = run_dir / "keyword_pipeline_merged.csv"
    write_merged_csv(
        out_path,
        [_row_one_line_physical(r) for r in merged_rows],
        merged_csv_mode="lean",
    )
    print(f"  [tb.merged] {len(merged_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(merged_rows)


def run(run_dir: Path) -> tuple[int, int, int, int]:
    """返回 (n_list, n_detail, n_comment, n_merged)。"""
    run_dir = run_dir.expanduser().resolve()
    n_list = _parse_list(run_dir)
    n_detail = _parse_detail(run_dir)
    n_comment = _parse_comment(run_dir)
    n_merged = _parse_merged(run_dir)
    return n_list, n_detail, n_comment, n_merged


def main() -> int:
    ap = argparse.ArgumentParser(description="淘宝半自动捕获 JSON → 与京东列对齐的 CSV")
    ap.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="run 目录（含 list/detail/comment）；默认取 TB playwright 下最新时间戳",
    )
    args = ap.parse_args()

    if args.dir:
        run_dir = Path(args.dir).resolve()
    else:
        base = tb_playwright_semiauto_capture_root()
        if not base.is_dir():
            print(f"数据目录不存在: {base}", file=sys.stderr)
            return 2
        cands = [d for d in sorted(base.iterdir(), reverse=True) if d.is_dir()]
        if not cands:
            print(f"未找到 run: {base}", file=sys.stderr)
            return 2
        run_dir = cands[0].resolve()

    if not run_dir.is_dir():
        print(f"run 目录不存在: {run_dir}", file=sys.stderr)
        return 2

    print(f"[tb.parse_semiauto] run: {run_dir}", file=sys.stderr)
    n_list, n_detail, n_comment, n_merged = run(run_dir)
    print(
        f"[tb.parse_semiauto] 完成 list={n_list} detail={n_detail} comment={n_comment} merged={n_merged}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
