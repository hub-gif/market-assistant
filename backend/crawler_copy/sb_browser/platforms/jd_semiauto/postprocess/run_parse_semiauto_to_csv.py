# -*- coding: utf-8 -*-
"""半自动 run_dir：先 ``postprocess_semiauto_capture_json_dirs``，再写 pc_search/detail/comments/merged 四类 CSV。

运行：``python -m sb_browser.platforms.jd_semiauto.postprocess.run_parse_semiauto_to_csv``（``cwd`` = ``crawler_copy``）。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[4]  # -> backend/crawler_copy/
    backend = root.parent
    for p in (root, backend):
        if p.is_dir() and str(p) not in sys.path:
            sys.path.insert(0, str(p))


_bootstrap()

from sb_browser.platforms.jd_semiauto.common.low_gi_root import (  # noqa: E402
    jd_semiauto_data_dir,
    low_gi_project_root,
)


def _load_market_assistant_dotenv() -> None:
    """与 Django settings / pipeline.openai_gateway 一致：盘后配料多模态读仓库根 .env。"""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = low_gi_project_root() / ".env"
    if env_path.is_file():
        load_dotenv(env_path)


_load_market_assistant_dotenv()


def _load_blobs(kind_dir: Path) -> list[tuple[Path, dict]]:
    """按文件名排序读取 kind_dir 下所有 *.json，返回 (path, blob) 列表。"""
    result: list[tuple[Path, dict]] = []
    for f in sorted(kind_dir.glob("*.json")):
        try:
            result.append((f, json.loads(f.read_text(encoding="utf-8"))))
        except Exception as e:
            print(f"  [parse] 跳过 {f.name}: {e}", file=sys.stderr)
    return result


def _list_search_keyword_for_parse(blob: dict, parsed: dict) -> str:
    """搜索 CSV / 合并表「关键词」列：优先 ``data.listKeyWord``，再落盘 ``list_keyword``，再任务 keyword。"""
    lk = str(blob.get("list_keyword") or "").strip()
    if lk:
        return lk
    data = parsed.get("data")
    if isinstance(data, dict):
        from_api = str(data.get("listKeyWord") or "").strip()
        if from_api:
            return from_api
    return str(blob.get("keyword") or "manual").strip() or "manual"


# ── list → pc_search_export.csv ───────────────────────────────────────────────

def _parse_list(run_dir: Path) -> int:
    from jd_pc_search.search.jd_h5_search_parse import (
        CSV_FIELDS,
        jd_row_to_export,
        parse_items_from_jd_json_payload,
    )

    kind_dir = run_dir / "list"
    if not kind_dir.is_dir():
        return 0

    blobs = _load_blobs(kind_dir)
    if not blobs:
        return 0

    all_rows: list[dict[str, str]] = []
    seen_skus: set[str] = set()

    for page_idx, (fpath, blob) in enumerate(blobs, start=1):
        parsed = blob.get("parsed")
        if not isinstance(parsed, dict):
            print(f"  [list] {fpath.name}: parsed 非 dict，跳过", file=sys.stderr)
            continue
        keyword = _list_search_keyword_for_parse(blob, parsed)
        rows = parse_items_from_jd_json_payload(parsed, keyword=keyword, page=page_idx)
        for row in rows:
            sku = (row.get("sku_id") or "").strip()
            if sku and sku in seen_skus:
                continue
            if sku:
                seen_skus.add(sku)
            all_rows.append(jd_row_to_export(row))

    if not all_rows:
        return 0

    out_path = run_dir / "pc_search_export.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(CSV_FIELDS), restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"  [list] {len(all_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


# ── detail → detail_ware_export.csv ───────────────────────────────────────────

# 英文内部字段 → 中文表头（lean 子集，与 pipeline schema.py DETAIL_CSV_COLUMNS 对齐）
_DETAIL_EN_TO_CN: dict[str, str] = {
    "detail_brand": "品牌",
    "detail_price_final": "到手价",
    "detail_shop_name": "店铺名称",
    "detail_category_path": "类目路径",
    "detail_product_attributes": "商品参数",
    "detail_body_ingredients": "配料表",
    "buyer_ranking_line": "榜单排名",
    "buyer_promo_text": "促销摘要",
}
# pipeline detail_ware_export.csv 表头顺序（与 schema.py DETAIL_CSV_COLUMNS 一致）
_DETAIL_CSV_COLUMNS: tuple[str, ...] = (
    "SKU",
    "品牌",
    "到手价",
    "店铺名称",
    "类目路径",
    "商品参数",
    "配料表",
    "榜单排名",
    "促销摘要",
)


def _parse_detail(run_dir: Path) -> int:
    from jd_pc_search.detail.jd_detail_ware_parse import (
        WARE_BUSINESS_MERGE_FIELDNAMES,
        flatten_ware_business,
    )

    kind_dir = run_dir / "detail"
    if not kind_dir.is_dir():
        return 0

    blobs = _load_blobs(kind_dir)
    if not blobs:
        return 0

    all_rows: list[dict[str, str]] = []
    seen_skus: set[str] = set()

    for fpath, blob in blobs:
        sku = str(blob.get("resolved_sku") or "").strip()
        if sku in seen_skus:
            continue
        if sku:
            seen_skus.add(sku)
        parsed = blob.get("parsed")
        flat = flatten_ware_business(parsed)
        if "semiauto_detail_ingredients_text" in blob:
            rec = str(blob.get("semiauto_detail_ingredients_text") or "").strip()
            flat = dict(flat)
            flat["detail_body_ingredients"] = rec or str(flat.get("detail_body_ingredients") or "")
        # 将英文字段映射为 lean 中文表头
        cn_row: dict[str, str] = {"SKU": sku}
        for en_key, cn_header in _DETAIL_EN_TO_CN.items():
            cn_row[cn_header] = str(flat.get(en_key) or "")
        all_rows.append(cn_row)

    if not all_rows:
        return 0

    out_path = run_dir / "detail_ware_export.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(_DETAIL_CSV_COLUMNS), restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"  [detail] {len(all_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


# ── comment → comments_flat.csv ───────────────────────────────────────────────

# pipeline comments_flat.csv 表头（与 schema.py COMMENT_CSV_COLUMNS 一致）
_COMMENT_CSV_COLUMNS: tuple[str, ...] = (
    "SKU",
    "评价ID",
    "用户昵称",
    "评价内容",
    "评价时间",
    "购买次数",
    "晒图链接",
    "评分",
)

# 评价 API 字段 → 中文表头映射（与 schema.py COMMENT_ROW_DICT_KEYS/COMMENT_CSV_COLUMNS 对齐）
_COMMENT_EN_TO_CN: dict[str, str] = {
    "sku": "SKU",
    "commentId": "评价ID",
    "userNickName": "用户昵称",
    "tagCommentContent": "评价内容",
    "commentDate": "评价时间",
    "buyCountText": "购买次数",
    "largePicURLs": "晒图链接",
    "commentScore": "评分",
}


def _parse_comment(run_dir: Path) -> int:
    from jd_pc_search.comment.jd_item_comment_parse import extract_comment_rows_from_parsed

    kind_dir = run_dir / "comment"
    if not kind_dir.is_dir():
        return 0

    blobs = _load_blobs(kind_dir)
    if not blobs:
        return 0

    all_rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for fpath, blob in blobs:
        sku = str(blob.get("resolved_sku") or "").strip()
        parsed = blob.get("parsed")
        for cr in extract_comment_rows_from_parsed(sku, parsed):
            cid = str(cr.get("commentId") or "").strip()
            dedup = f"{sku}:{cid}" if cid else f"{sku}:{id(cr)}"
            if dedup in seen_ids:
                continue
            seen_ids.add(dedup)
            pics: list = cr.get("largePicURLs") or []
            cn_row: dict[str, str] = {}
            for en_key, cn_header in _COMMENT_EN_TO_CN.items():
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
        w = csv.DictWriter(f, fieldnames=list(_COMMENT_CSV_COLUMNS), restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"  [comment] {len(all_rows)} 条 → {out_path.name}", file=sys.stderr)
    return len(all_rows)


# ── merged → keyword_pipeline_merged.csv ─────────────────────────────────────

def _ensure_jd_pc_search_subpaths() -> None:
    """``jd_pipeline_export`` 依赖 ``detail/`` 下脚本的顶层 import，与 ``jd_keyword_pipeline`` 一致。"""
    cc = Path(__file__).resolve().parents[4]
    be = cc.parent
    if str(be) not in sys.path:
        sys.path.insert(0, str(be))
    jd = cc / "jd_pc_search"
    for sub in ("search", "comment", "detail"):
        p = str((jd / sub).resolve())
        if p not in sys.path:
            sys.path.insert(0, p)


def _pipeline_keyword(run_dir: Path) -> str:
    """从首个 list JSON 推导「流水线关键词」（``listKeyWord`` / ``list_keyword`` / 任务 keyword）。"""
    kind_dir = run_dir / "list"
    if kind_dir.is_dir():
        for f in sorted(kind_dir.glob("*.json")):
            try:
                blob = json.loads(f.read_text(encoding="utf-8"))
                parsed = blob.get("parsed")
                pw = parsed if isinstance(parsed, dict) else {}
                return _list_search_keyword_for_parse(blob, pw)
            except Exception:
                continue
    return "manual"


def _read_pc_search_csv_rows(run_dir: Path) -> list[dict[str, str]]:
    p = run_dir / "pc_search_export.csv"
    if not p.is_file():
        return []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _detail_caches_for_merge(run_dir: Path) -> tuple[
    dict[str, dict[str, str]],
    dict[str, tuple[str, str]],
]:
    """各 SKU 一条：商详扁平 + (榜单排名, 促销摘要)。"""
    from jd_pc_search.detail.jd_detail_buyer_extraction import (  # noqa: WPS433
        buyer_promo_text_from_profile,
        buyer_ranking_line_from_profile,
        extract_buyer_offer_profile_from_json_text,
    )
    from jd_pc_search.detail.jd_detail_ware_parse import (  # noqa: WPS433
        flatten_ware_business,
        format_ware_response_for_save,
    )

    flat_by_sku: dict[str, dict[str, str]] = {}
    buyer_cache: dict[str, tuple[str, str]] = {}
    kind_dir = run_dir / "detail"
    if not kind_dir.is_dir():
        return flat_by_sku, buyer_cache
    seen: set[str] = set()
    for _fpath, blob in _load_blobs(kind_dir):
        sku = str(blob.get("resolved_sku") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        parsed = blob.get("parsed")
        if not isinstance(parsed, dict):
            continue
        flat = flatten_ware_business(parsed)
        if "semiauto_detail_ingredients_text" in blob:
            rec = str(blob.get("semiauto_detail_ingredients_text") or "").strip()
            flat = dict(flat)
            flat["detail_body_ingredients"] = rec or str(flat.get("detail_body_ingredients") or "")
        flat_by_sku[sku] = flat
        try:
            txt = format_ware_response_for_save(
                json.dumps(parsed, ensure_ascii=False),
                normalize=True,
                sort_keys=True,
                indent=2,
            )
            prof = extract_buyer_offer_profile_from_json_text(txt)
            buyer_cache[sku] = (
                buyer_ranking_line_from_profile(prof),
                buyer_promo_text_from_profile(prof),
            )
        except Exception:
            buyer_cache[sku] = ("", "")
    return flat_by_sku, buyer_cache


def _comments_grouped_by_sku(run_dir: Path) -> dict[str, list]:
    from jd_pc_search.comment.jd_item_comment_parse import (  # noqa: WPS433
        extract_comment_rows_from_parsed,
    )
    from jd_pc_search.jd_pipeline_export import dedupe_comment_rows  # noqa: WPS433

    out: dict[str, list] = {}
    kind_dir = run_dir / "comment"
    if not kind_dir.is_dir():
        return out
    for _fpath, blob in _load_blobs(kind_dir):
        sku = str(blob.get("resolved_sku") or "").strip()
        parsed = blob.get("parsed")
        if not sku:
            continue
        rows = extract_comment_rows_from_parsed(sku, parsed)
        out.setdefault(sku, []).extend(rows)
    for sku in list(out.keys()):
        out[sku] = dedupe_comment_rows(out[sku] or [])
    return out


def _parse_merged(run_dir: Path) -> int:
    """根据已生成的 pc_search / detail / comment 拼装整合宽表（lean）。

    半自动与全自动列表驱动的合并不同：**以商详为锚**——仅对 ``detail/`` 中已成功解析
    ``wareBusiness`` 的 SKU 输出一行，故行数 **≤ 商详表行数**；列表中有但未开商的 SKU 不出现在整合表。
    评论仅挂载到对应 SKU；无商详仅有评论的 SKU 不单独生成整合行（与「不超商详条数」一致）。
    """
    _ensure_jd_pc_search_subpaths()
    from jd_pc_search.jd_pipeline_export import (  # noqa: WPS433
        SKU_CSV_HEADER,
        comment_fields_from_rows,
        dedupe_comment_rows,
        finalize_merged_row_for_disk,
        write_merged_csv,
    )
    from jd_pc_search.search.jd_h5_search_parse import CSV_FIELDS  # noqa: WPS433
    from pipeline.csv.schema import JD_SEARCH_CSV_HEADERS as JD_EXPORT  # noqa: WPS433

    kw = _pipeline_keyword(run_dir)
    search_rows = _read_pc_search_csv_rows(run_dir)
    flat_by_sku, buyer_cache = _detail_caches_for_merge(run_dir)
    comments_by_sku = _comments_grouped_by_sku(run_dir)

    # 搜索侧：同 SKU 取首条（与 list 解析去重一致）
    search_by_sku: dict[str, dict[str, str]] = {}
    for row in search_rows:
        sku = str(row.get(SKU_CSV_HEADER) or "").strip()
        if sku and sku not in search_by_sku:
            search_by_sku[sku] = row

    merged_rows: list[dict[str, str]] = []

    def _finalize_one(
        merged_local: dict[str, str],
        sku_key: str,
    ) -> None:
        merged_local.update(flat_by_sku.get(sku_key) or {})
        rline, ptext = buyer_cache.get(sku_key, ("", ""))
        merged_local["buyer_ranking_line"] = rline
        merged_local["buyer_promo_text"] = ptext
        crows = dedupe_comment_rows(comments_by_sku.get(sku_key, []) or [])
        merged_local.update(comment_fields_from_rows(crows))
        finalize_merged_row_for_disk(merged_local)

    # 仅以「有商详扁平」的 SKU 为键，保证 len(merged) == 有详情的 SKU 数 ≤ detail 导出行数
    for sku in sorted(flat_by_sku.keys()):
        row_src = search_by_sku.get(sku)
        if row_src is not None:
            merged = {k: str(row_src.get(k) or "") for k in CSV_FIELDS}
        else:
            merged = {cn: "" for cn in CSV_FIELDS}
            merged[JD_EXPORT["sku_id"]] = sku
            merged[JD_EXPORT["keyword"]] = kw
            merged[JD_EXPORT["page"]] = "1"
            merged[JD_EXPORT["platform"]] = "京东"
        merged["流水线关键词"] = kw
        _finalize_one(merged, sku)
        merged_rows.append(merged)

    if not merged_rows:
        return 0

    out_path = run_dir / "keyword_pipeline_merged.csv"
    write_merged_csv(out_path, merged_rows, merged_csv_mode="lean")
    print(f"  [merged] {len(merged_rows)} 行 → {out_path.name}", file=sys.stderr)
    return len(merged_rows)


# ── 入口 ──────────────────────────────────────────────────────────────────────

def run(run_dir: Path) -> tuple[int, int, int, int]:
    """解析指定 run_dir，返回 (n_list, n_detail, n_comment, n_merged)。供外部调用（如 semiauto_tasks.py）。"""
    # 必须用绝对导入：Django 侧 ``importlib.util.spec_from_file_location`` 加载本文件时没有 package 上下文
    from sb_browser.platforms.jd_semiauto.postprocess.postprocess_semiauto_capture_json_dirs import (  # noqa: E402
        postprocess_semiauto_capture_json_dirs,
    )

    postprocess_semiauto_capture_json_dirs(run_dir)
    n_list = _parse_list(run_dir)
    n_detail = _parse_detail(run_dir)
    n_comment = _parse_comment(run_dir)
    n_merged = _parse_merged(run_dir)
    return n_list, n_detail, n_comment, n_merged


def main() -> int:
    ap = argparse.ArgumentParser(description="半自动 JD 捕获 JSON → pipeline 兼容 CSV")
    ap.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="run 目录（含 list/ detail/ comment/ 子目录）；留空自动取最新",
    )
    args = ap.parse_args()

    if args.dir:
        run_dir = Path(args.dir).expanduser().resolve()
    else:
        base = jd_semiauto_data_dir()
        if not base.is_dir():
            print(f"数据目录不存在: {base}", file=sys.stderr)
            return 2
        candidates = [d for d in sorted(base.iterdir(), reverse=True) if d.is_dir()]
        if not candidates:
            print(f"未找到 run 目录: {base}", file=sys.stderr)
            return 2
        run_dir = candidates[0]

    if not run_dir.is_dir():
        print(f"run 目录不存在: {run_dir}", file=sys.stderr)
        return 2

    print(f"[parse_semiauto] run 目录: {run_dir}", file=sys.stderr)

    n_list, n_detail, n_comment, n_merged = run(run_dir)
    print(
        f"[parse_semiauto] 完成 → list={n_list} 商品，detail={n_detail} SKU，comment={n_comment} 条，merged={n_merged} 行",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
