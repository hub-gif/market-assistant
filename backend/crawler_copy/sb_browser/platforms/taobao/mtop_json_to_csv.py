# -*- coding: utf-8 -*-
"""
从落盘的 mtop 原始 JSON（``notes_sink`` 格式）中读取 ``parsed.data.itemsArray``，扁平化后写 **UTF-8 BOM** CSV，
与 JSON 同目录，便于 Excel 打开。

可在一次 JSON 写盘后由 ``save_mtop_captures`` 自动调用，也可单独运行本文件对某目录补导出。
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any


def _strip_html_text(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    t = re.sub(r"<[^>]+>", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _scalar(v: Any, *, max_json: int = 4000) -> str:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    try:
        return json.dumps(v, ensure_ascii=False)[:max_json]
    except TypeError:
        return str(v)[:max_json]


def _sku_id_from_item(it: dict[str, Any]) -> str:
    ep = it.get("extraParams")
    if isinstance(ep, list):
        for pair in ep:
            if isinstance(pair, dict) and pair.get("key") == "skuId":
                v = pair.get("value")
                return str(v).strip() if v is not None else ""
    au = str(it.get("auctionURL") or "")
    m = re.search(r"[?&]skuId=(\d+)", au.replace("&amp;", "&"))
    return m.group(1) if m else ""


def _icons_join_text(icon_list: Any, *, sep: str = " | ") -> str:
    """拼接 ``icons`` 条目里出现的 ``text``（忽略纯图占位）。"""
    if not isinstance(icon_list, list):
        return ""
    out: list[str] = []
    for ic in icon_list:
        if not isinstance(ic, dict):
            continue
        t = ic.get("text")
        if isinstance(t, str) and t.strip():
            out.append(t.strip())
    return sep.join(out)


def _structured_usp_compact(usp: Any) -> str:
    """``structuredUSPInfo`` → ``属性：值`` 用中文分号连接。"""
    if not isinstance(usp, list):
        return ""
    parts: list[str] = []
    for x in usp:
        if not isinstance(x, dict):
            continue
        pn = x.get("propertyName")
        pv = x.get("propertyValueName")
        ps = ""
        pn_s = _scalar(pn).strip()
        pv_s = _scalar(pv).strip()
        if pn_s or pv_s:
            ps = f"{pn_s}:{pv_s}" if pn_s else pv_s
        if ps:
            parts.append(ps)
    return "；".join(parts)


def _format_price_show(v: Any) -> str:
    """
    ``priceShow`` 对象 → 可读一行，例如 ``补贴后 ￥6.9``；
    已是字符串则原样去首尾空白。
    """
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if not isinstance(v, dict):
        return ""
    raw_unit = str(v.get("unit") or "¥").strip() or "¥"
    unit = raw_unit.replace("¥", "￥").replace("\u00a5", "￥")  # 半角¥ → 全角￥
    price_num = str(v.get("price") or "").strip()
    desc = str(v.get("priceDesc") or "").strip()
    pre = str(v.get("preText") or "").strip()
    body = unit + price_num if price_num else unit

    prefixes: list[str] = []
    if pre:
        prefixes.append(pre)
    if desc:
        prefixes.append(desc)

    if not prefixes:
        return body
    head = " ".join(prefixes)
    return f"{head} {body}"


def rows_from_parsed_mtop_blob(blob: dict[str, Any]) -> list[dict[str, str]]:
    """从整段落盘对象（含 ``parsed``）解析商品行。"""
    parsed = blob.get("parsed")
    if not isinstance(parsed, dict):
        return []
    data = parsed.get("data")
    if not isinstance(data, dict):
        return []
    items = data.get("itemsArray")
    if not isinstance(items, list):
        return []
    rows: list[dict[str, str]] = []
    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        shop = it.get("shopInfo")
        shop_title = ""
        if isinstance(shop, dict):
            shop_title = _scalar(shop.get("title") or shop.get("shopTitle") or shop.get("nick"))

        rows.append(
            {
                "list_index": str(idx + 1),
                "sku_id": _sku_id_from_item(it),
                "title": _strip_html_text(_scalar(it.get("title"))),
                "nick": _scalar(it.get("nick")),
                "price": _scalar(it.get("price")),
                "price_show": _format_price_show(it.get("priceShow")),
                "icons_text": _icons_join_text(it.get("icons")),
                "structured_usp": _structured_usp_compact(it.get("structuredUSPInfo")),
                "shop_tag": _scalar(it.get("shopTag")),
                "realSales": _scalar(it.get("realSales")),
                "procity": _scalar(it.get("procity")),
                "isP4p": _scalar(it.get("isP4p")),
                "pic_path": _scalar(it.get("pic_path")),
                "auctionURL": _scalar(it.get("auctionURL")),
                "shop_title": shop_title,
            },
        )
    return rows


def load_saved_mtop_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def export_json_paths_to_csv(
    json_paths: list[Path],
    *,
    keyword: str,
    stamp: str,
    safe_keyword_slug: str,
    out_name: str | None = None,
) -> Path | None:
    """
    将多个 ``mtop_*.json`` 中的 ``itemsArray`` 合并导出为单个 CSV（含 ``search_keyword`` 等）。

    输出路径：``<与第一个 json 同目录>/mtop_items_<slug>_t<stamp>.csv``（``out_name`` 可覆盖全名）。
    """
    paths = [Path(p).resolve() for p in json_paths if p]
    if not paths:
        return None

    all_rows: list[dict[str, str]] = []
    fieldnames: list[str] = [
        "search_keyword",
        "list_index",
        "sku_id",
        "title",
        "nick",
        "price",
        "price_show",
        "icons_text",
        "structured_usp",
        "shop_tag",
        "realSales",
        "procity",
        "isP4p",
        "pic_path",
        "auctionURL",
        "shop_title",
    ]

    for jp in paths:
        try:
            blob = load_saved_mtop_json(jp)
        except Exception as e:
            print(f"[tb.csv] 跳过不可读 JSON {jp}: {e}", file=sys.stderr)
            continue
        kw_blob = _scalar(blob.get("keyword")) or keyword
        for row in rows_from_parsed_mtop_blob(blob):
            row2 = {
                "search_keyword": kw_blob,
                **row,
            }
            all_rows.append(row2)

    if not all_rows:
        print("[tb.csv] 无 itemsArray 数据可导出 CSV", file=sys.stderr)
        return None

    out_dir = paths[0].parent
    if out_name:
        out_path = out_dir / out_name
    else:
        out_path = out_dir / f"mtop_items_{safe_keyword_slug}_t{stamp}.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8-sig", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)

    print(f"[tb.csv] 已导出 {len(all_rows)} 行 → {out_path}", file=sys.stderr)
    return out_path


def _slug_kw_local(kw: str, *, max_chars: int = 48) -> str:
    chunks: list[str] = []
    for c in (kw or "").strip()[:max_chars]:
        if c in '<>:"/\\|?*' or ord(c) < 32:
            chunks.append("_")
        else:
            chunks.append(c)
    s = "".join(chunks).strip("._ ").strip("_")
    return s or "kw"


def export_directory_mtop_jsons_to_csv(run_dir: Path | str, *, pattern: str = "mtop_*.json") -> Path | None:
    """对某次运行目录下所有匹配的 JSON 合并导出（补跑用）。"""
    d = Path(run_dir).expanduser().resolve()
    if not d.is_dir():
        print(f"[tb.csv] 不是目录: {d}", file=sys.stderr)
        return None
    paths = sorted(d.glob(pattern))
    if not paths:
        print(f"[tb.csv] 未找到 {pattern}: {d}", file=sys.stderr)
        return None
    try:
        blob0 = load_saved_mtop_json(paths[0])
    except Exception as e:
        print(f"[tb.csv] 读首文件失败: {e}", file=sys.stderr)
        return None
    kw = str(blob0.get("keyword") or "")
    stamp = str(blob0.get("stamp") or "")
    return export_json_paths_to_csv(
        paths,
        keyword=kw,
        stamp=stamp,
        safe_keyword_slug=_slug_kw_local(kw),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="将 mtop 落盘 JSON 中 itemsArray 导出为 CSV")
    p.add_argument("run_dir", type=str, help="含 mtop_*.json 的运行目录（如 sb_cdp_mtop_raw/20260429_112755/）")
    p.add_argument("--pattern", default="mtop_*.json", help="Glob 文件名模式")
    args = p.parse_args(argv)

    rp = Path(args.run_dir)
    paths = sorted(rp.glob(args.pattern))
    if not paths:
        print("[tb.csv] 未找到匹配 JSON", file=sys.stderr)
        return 2
    export_directory_mtop_jsons_to_csv(rp, pattern=args.pattern)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
