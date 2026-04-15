# -*- coding: utf-8 -*-
"""
京东商品详情页评论（getLegoWareDetailComment）。

由同目录 Node ``jd_export_item_comment_request.js`` 生成 **url + headers**
（含 ParamsSign / h5st，见 ``jd_h5st_item_comment.js``），再用 **Playwright/Chromium**
发 GET（与 ``search/jd_search_playwright.py`` 相同方式，浏览器 TLS，减轻 jfe 403）。

依赖: pip install playwright && playwright install chromium

鉴权 Cookie：由 Node 读入请求头；路径见下方配置项 ``COOKIE_FILE`` / ``COOKIE_OVERRIDE``。


用法（本仓库默认）: 修改下方「运行配置」后 ``python jd_h5_item_comment_requests.py``（无命令行参数）。
"""

from __future__ import annotations

import csv
import json
import random
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from playwright.sync_api import sync_playwright

_JD_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_JD_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_JD_PKG_ROOT))
_BACKEND_ROOT = Path(__file__).resolve().parents[3]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))
from common.jd_delay_utils import parse_request_delay_range
from pipeline.csv_schema import COMMENT_CSV_COLUMNS, COMMENT_ROW_DICT_KEYS  # noqa: E402
from _low_gi_root import low_gi_project_root  # noqa: E402

_JD_COMMENT_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# 运行配置（按需改这里）
# ---------------------------------------------------------------------------
# 路径：副本通过 LOW_GI_PROJECT_ROOT 指向「Low GI」根目录
_PROJECT_ROOT = low_gi_project_root()
_PROJECT_DATA = _PROJECT_ROOT / "data" / "JD"
_JD_COMMON_COOKIE = Path(__file__).resolve().parents[1] / "common" / "jd_cookie.txt"

# SKU：单个商品 ID；与 SKU_FILE 二选一
SKU = "10166848058665"
# SKU_FILE：每行一个 SKU（# 注释）；与 SKU 二选一
SKU_FILE = ""
# COMMENT_NUM：首屏 Lego 请求 body.commentNum（评价条数相关）
COMMENT_NUM = 5
# SHOP_TYPE：body.shopType，一般 "0"
SHOP_TYPE = "0"
# COOKIE_FILE：传给 Node 读 Cookie 的文件路径（与搜索/详情共用 jd_cookie.txt）
COOKIE_FILE = str(_JD_COMMON_COOKIE)
# COOKIE_OVERRIDE：非空则覆盖请求头中的 Cookie
COOKIE_OVERRIDE = ""
# TIMEOUT_SEC：单次 Playwright GET/POST 超时（秒）
TIMEOUT_SEC = 30.0
# REQUEST_DELAY：每次发起新 HTTP 前的随机等待，如 "30-60"；"0-0" 可关闭
REQUEST_DELAY = "30-60"
# OUT_JSONL：网络采集时每条接口一行 JSONL；空则打印 stdout
OUT_JSONL = str(_PROJECT_DATA / "jd_comments.jsonl")
# PRETTY：单 SKU 且无 OUT_JSONL 时是否缩进打印业务 JSON（parsed）
PRETTY = False
# RAISE_HTTP：True 时 HTTP 非 2xx 直接退出进程
RAISE_HTTP = False
# HEADED：True 有头浏览器
HEADED = False
# COMMENTS_OUT：解析后的扁平评价，扩展名 .csv 或 .jsonl；可与采集同时写
COMMENTS_OUT = str(_PROJECT_DATA / "jd_comments_flat.csv")
# FROM_JSONL：非空则离线模式，仅从已存 JSONL 抽评价；须同时设 COMMENTS_OUT，不走浏览器
FROM_JSONL = ""
# WITH_COMMENT_LIST：首屏 Lego 成功后是否继续请求分页评价列表（POST client.action）
WITH_COMMENT_LIST = False
# LIST_PAGES：列表分页规格，如 "1"、"1-5"、"1,3,5"
LIST_PAGES = "1"
# LIST_FUNCTION_ID：列表接口 functionId，须与抓包一致
LIST_FUNCTION_ID = "getCommentListPage"
# LIST_STYLE：非首包分页请求的 style
LIST_STYLE = "1"
# LIST_CATEGORY：可选，手动 body.category（默认从首条评价 maidianInfo 解析）
LIST_CATEGORY = ""
# LIST_FIRST_GUID：可选，手动 firstCommentGuid（默认首屏 commentInfoList[0].guid）
LIST_FIRST_GUID = ""
# ---------------------------------------------------------------------------


def export_item_comment_request_json(
    sku: str,
    *,
    comment_num: int = 5,
    shop_type: str = "0",
    cookie_file: str | None = None,
) -> dict[str, Any]:
    """Node 输出 {url, headers}；h5st 与 body 与商品页抓包一致。"""
    cmd = [
        "node",
        str(_JD_COMMENT_DIR / "jd_export_item_comment_request.js"),
        "--sku",
        str(sku).strip(),
        "--comment-num",
        str(max(1, int(comment_num))),
        "--shop-type",
        str(shop_type),
    ]
    cf = (cookie_file or "").strip()
    if cf:
        cmd.extend(["--cookie-file", cf])
    r = subprocess.run(
        cmd,
        cwd=str(_JD_COMMENT_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if r.returncode != 0:
        print(r.stderr or r.stdout, file=sys.stderr)
        sys.exit(r.returncode or 1)
    return json.loads(r.stdout)


def export_item_comment_page_request_json(
    sku: str,
    *,
    category: str,
    first_guid: str,
    page_num: str,
    is_first: bool,
    function_id: str,
    shop_type: str = "0",
    spu_id: str | None = None,
    style: str = "1",
    cookie_file: str | None = None,
) -> dict[str, Any]:
    """Node 输出 POST client.action：{ url, method, form, headers }。"""
    cmd = [
        "node",
        str(_JD_COMMENT_DIR / "jd_export_item_comment_page_request.js"),
        "--sku",
        str(sku).strip(),
        "--category",
        str(category).strip(),
        "--first-guid",
        str(first_guid).strip(),
        "--page-num",
        str(page_num).strip(),
        "--is-first",
        "true" if is_first else "false",
        "--function-id",
        str(function_id).strip(),
        "--shop-type",
        str(shop_type),
        "--style",
        str(style),
    ]
    if spu_id and str(spu_id).strip():
        cmd.extend(["--spu-id", str(spu_id).strip()])
    cf = (cookie_file or "").strip()
    if cf:
        cmd.extend(["--cookie-file", cf])
    r = subprocess.run(
        cmd,
        cwd=str(_JD_COMMENT_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if r.returncode != 0:
        print(r.stderr or r.stdout, file=sys.stderr)
        sys.exit(r.returncode or 1)
    return json.loads(r.stdout)


def _sleep_between_jd_requests(
    delay_range: tuple[float, float], label: str = "请求间隔"
) -> None:
    """在「上一包已完成、即将发起下一包」时调用；0-0 视为不等待。"""
    lo, hi = delay_range
    if lo <= 0 and hi <= 0:
        return
    sec = random.uniform(lo, hi)
    print(
        f"[京东] {label} sleep {sec:.1f}s（区间 {lo:g}–{hi:g}）",
        file=sys.stderr,
    )
    time.sleep(sec)


def parse_list_pages_spec(spec: str) -> list[str]:
    """
    --list-pages：``1-5`` → 1..5；``1,3,5`` → 单页序列；单数字 ``2`` → [\"2\"]。
    """
    s = (spec or "").strip()
    if not s:
        return ["1"]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    if "-" in s:
        parts = s.split("-", 1)
        lo, hi = int(parts[0].strip()), int(parts[1].strip())
        if lo > hi:
            lo, hi = hi, lo
        return [str(i) for i in range(lo, hi + 1)]
    return [s]


def category_and_first_guid_from_lego(parsed: Any) -> tuple[str, str]:
    """从 getLegoWareDetailComment 的 commentInfoList[0] 取 category（maidianInfo 前缀）与 guid。"""
    if not isinstance(parsed, dict):
        return "", ""
    lst = parsed.get("commentInfoList")
    if not isinstance(lst, list) or not lst:
        return "", ""
    first = lst[0]
    if not isinstance(first, dict):
        return "", ""
    guid = str(first.get("guid") or "").strip()
    maidian = str(first.get("maidianInfo") or "").strip()
    category = maidian.split("_", 1)[0].strip() if maidian else ""
    return category, guid


def _read_sku_lines(path: str) -> list[str]:
    p = Path(path)
    if not p.is_file():
        print(f"文件不存在: {path}", file=sys.stderr)
        sys.exit(2)
    out: list[str] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def _loads_jd_plain_json(text: str) -> Any:
    s = (text or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def _jd_business_ok(parsed: Any) -> bool:
    if not isinstance(parsed, dict):
        return False
    if parsed.get("success") is False:
        return False
    c = parsed.get("code")
    if c is None:
        return True
    return c == 0 or str(c) == "0"


def _clean_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return " ".join(s.split()) if s else ""


def _large_pic_urls_from_picture_list(pil: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(pil, list):
        return out
    for p in pil:
        if not isinstance(p, dict):
            continue
        u = p.get("largePicURL") or p.get("largePicUrl")
        if u:
            t = str(u).strip()
            if t and t not in out:
                out.append(t)
    return out


def _is_jd_single_comment_dict(d: dict) -> bool:
    """区分「一条评价」与标签/楼层等对象（getCommentListPage 里多为 commentInfo 扁平结构）。"""
    cid = d.get("commentId")
    if cid is None or str(cid).strip() == "":
        return False
    if d.get("userNickName") is None and not (
        d.get("tagCommentContent") or d.get("commentData")
    ):
        return False
    return True


def _walk_collect_comment_dicts(obj: Any, acc: list[dict[str, Any]]) -> None:
    """深度遍历 JSON，收集所有像单条评价的 dict（含 Lego 的 commentInfoList 项与列表页的 commentInfo）。"""
    if isinstance(obj, dict):
        if _is_jd_single_comment_dict(obj):
            acc.append(obj)
        for v in obj.values():
            _walk_collect_comment_dicts(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _walk_collect_comment_dicts(x, acc)


def _row_from_comment_dict(sku: str, item: dict[str, Any]) -> dict[str, Any]:
    text = _clean_text(
        item.get("tagCommentContent") or item.get("commentData")
    )
    buy = _clean_text(
        item.get("buyCountText") or item.get("repurchaseInfo")
    )
    date = _clean_text(
        item.get("commentDate") or item.get("newCommentDate")
    )
    return {
        "sku": str(sku).strip(),
        "commentId": str(item.get("commentId") or "").strip(),
        "userNickName": _clean_text(item.get("userNickName")),
        "tagCommentContent": text,
        "commentDate": date,
        "buyCountText": buy,
        "largePicURLs": _large_pic_urls_from_picture_list(
            item.get("pictureInfoList")
        ),
        "commentScore":str(item.get("commentScore") or "").strip(),
    }


def extract_comment_rows_from_parsed(sku: str, parsed: Any) -> list[dict[str, Any]]:
    """
    从整段 parsed 深度遍历抽取评价：
    - getLegoWareDetailComment：commentInfoList / lastCommentInfoList
    - getCommentListPage：result.floors → data 里 { commentInfo: {...} } 已拍平为内层字段，同上
    """
    if not isinstance(parsed, dict):
        return []
    acc: list[dict[str, Any]] = []
    _walk_collect_comment_dicts(parsed, acc)
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for item in acc:
        cid = str(item.get("commentId") or "").strip()
        dedup_key = f"{sku}:{cid}" if cid else f"{sku}:{id(item)}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        rows.append(_row_from_comment_dict(sku, item))
    return rows


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


def write_comments_flat_csv(path: Path | str, rows: list[dict[str, Any]]) -> None:
    """与 ``COMMENTS_OUT`` 为 ``.csv`` 时相同格式（UTF-8 BOM），供流水线等复用。"""
    _write_comments_csv(Path(path), rows)


def _append_comments_jsonl(f, rows: list[dict[str, Any]]) -> None:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _extract_rows_from_jsonl_file(path: Path) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        try:
            rec = json.loads(s)
        except json.JSONDecodeError:
            continue
        sku = str(rec.get("sku") or "").strip()
        parsed = rec.get("parsed")
        all_rows.extend(extract_comment_rows_from_parsed(sku, parsed))
    return all_rows


def main() -> None:
    args = SimpleNamespace(
        sku=(SKU or "").strip(),
        sku_file=(SKU_FILE or "").strip(),
        comment_num=int(COMMENT_NUM),
        shop_type=str(SHOP_TYPE),
        cookie_file=(COOKIE_FILE or "").strip(),
        cookie=(COOKIE_OVERRIDE or "").strip(),
        timeout=float(TIMEOUT_SEC),
        request_delay=(REQUEST_DELAY or "").strip() or "30-60",
        out=(OUT_JSONL or "").strip() or None,
        pretty=bool(PRETTY),
        raise_http=bool(RAISE_HTTP),
        headed=bool(HEADED),
        comments_out=(COMMENTS_OUT or "").strip(),
        from_jsonl=(FROM_JSONL or "").strip(),
        with_comment_list=bool(WITH_COMMENT_LIST),
        list_pages=(LIST_PAGES or "1").strip(),
        list_function_id=(LIST_FUNCTION_ID or "getCommentListPage").strip(),
        list_style=(LIST_STYLE or "1").strip(),
        list_category=(LIST_CATEGORY or "").strip(),
        list_first_guid=(LIST_FIRST_GUID or "").strip(),
    )

    comments_out = args.comments_out
    from_jsonl = args.from_jsonl

    if from_jsonl:
        if not comments_out:
            print("离线模式：请配置 FROM_JSONL 与 COMMENTS_OUT", file=sys.stderr)
            sys.exit(2)
        co_path = Path(comments_out)
        rows = _extract_rows_from_jsonl_file(Path(from_jsonl))
        suf = co_path.suffix.lower()
        if suf == ".csv":
            _write_comments_csv(co_path, rows)
        elif suf == ".jsonl":
            co_path.parent.mkdir(parents=True, exist_ok=True)
            with co_path.open("w", encoding="utf-8") as cf:
                _append_comments_jsonl(cf, rows)
        else:
            print("COMMENTS_OUT 请使用 .csv 或 .jsonl 扩展名", file=sys.stderr)
            sys.exit(2)
        print(f"[京东] 已从 JSONL 抽取 {len(rows)} 条评价 → {co_path}", file=sys.stderr)
        return

    sku_one = args.sku
    sku_file = args.sku_file
    if bool(sku_one) == bool(sku_file):
        print("请只配置其一：SKU 或 SKU_FILE", file=sys.stderr)
        sys.exit(2)

    skus = [sku_one] if sku_one else _read_sku_lines(sku_file)
    if not skus:
        print("SKU 列表为空", file=sys.stderr)
        sys.exit(2)

    rd = args.request_delay
    try:
        delay_range = parse_request_delay_range(rd)
    except ValueError as e:
        print(f"[京东] REQUEST_DELAY 无效: {e}", file=sys.stderr)
        sys.exit(2)

    cookie_file_node = (args.cookie_file or "").strip()
    if cookie_file_node:
        cookie_file_node = str(Path(cookie_file_node).resolve())

    timeout_ms = max(1000, int(args.timeout * 1000))
    cookie_override = (args.cookie or "").strip()

    out_f = None
    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        out_f = outp.open("w", encoding="utf-8")

    comments_path = Path(comments_out) if comments_out else None
    comments_csv_rows: list[dict[str, Any]] = []
    comments_jsonl_f = None
    if comments_path is not None:
        suf = comments_path.suffix.lower()
        if suf not in (".csv", ".jsonl"):
            print("COMMENTS_OUT 请使用 .csv 或 .jsonl 扩展名", file=sys.stderr)
            sys.exit(2)
        comments_path.parent.mkdir(parents=True, exist_ok=True)
        if suf == ".jsonl":
            comments_jsonl_f = comments_path.open("w", encoding="utf-8")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=not args.headed)
            context = browser.new_context()
            try:
                gap_before_next = False
                for sku in skus:
                    if gap_before_next:
                        _sleep_between_jd_requests(delay_range, "请求间隔")

                    pack = export_item_comment_request_json(
                        sku,
                        comment_num=args.comment_num,
                        shop_type=str(args.shop_type),
                        cookie_file=cookie_file_node or None,
                    )
                    url = pack["url"]
                    hdrs = {str(k): str(v) for k, v in pack["headers"].items()}
                    if cookie_override:
                        hdrs["Cookie"] = cookie_override

                    resp = context.request.get(url, headers=hdrs, timeout=timeout_ms)
                    status = resp.status
                    text = resp.text()
                    print(
                        f"[京东] sku={sku} HTTP {status} {resp.status_text or ''}",
                        file=sys.stderr,
                    )

                    if args.raise_http and status // 100 != 2:
                        print(f"[京东] HTTP {status}，--raise-http 已启用", file=sys.stderr)
                        sys.exit(1)

                    parsed = _loads_jd_plain_json(text)
                    http_ok = 200 <= status < 300
                    row = {
                        "sku": sku,
                        "http_status": status,
                        "http_ok": http_ok,
                        "ok": http_ok and _jd_business_ok(parsed),
                        "parsed": parsed,
                        "raw": text if parsed is None else None,
                    }

                    line = json.dumps(row, ensure_ascii=False)
                    if out_f:
                        out_f.write(line + "\n")
                    elif len(skus) == 1 and args.pretty and parsed is not None:
                        sys.stdout.write(
                            json.dumps(parsed, ensure_ascii=False, indent=2) + "\n"
                        )
                    elif len(skus) == 1:
                        sys.stdout.write(line + "\n")
                    else:
                        sys.stdout.write(line + "\n")

                    flat = extract_comment_rows_from_parsed(sku, parsed)
                    if comments_path is not None:
                        if comments_jsonl_f is not None:
                            _append_comments_jsonl(comments_jsonl_f, flat)
                        else:
                            comments_csv_rows.extend(flat)

                    gap_before_next = True

                    if args.with_comment_list:
                        cat, fguid = category_and_first_guid_from_lego(parsed)
                        if (args.list_category or "").strip():
                            cat = (args.list_category or "").strip()
                        if (args.list_first_guid or "").strip():
                            fguid = (args.list_first_guid or "").strip()
                        if not cat or not fguid:
                            print(
                                "[京东] --with-comment-list 跳过：缺少 category 或 firstCommentGuid；"
                                "请确认首屏有评价，或使用 --list-category / --list-first-guid",
                                file=sys.stderr,
                            )
                        else:
                            pages = parse_list_pages_spec(args.list_pages or "1")
                            lfid = (args.list_function_id or "getCommentListPage").strip()
                            lstyle = (args.list_style or "1").strip()
                            for pi, pnum in enumerate(pages):
                                if gap_before_next:
                                    _sleep_between_jd_requests(
                                        delay_range, "列表分页请求间隔"
                                    )
                                is_first = pi == 0
                                pack_p = export_item_comment_page_request_json(
                                    sku,
                                    category=cat,
                                    first_guid=fguid,
                                    page_num=pnum,
                                    is_first=is_first,
                                    function_id=lfid,
                                    shop_type=str(args.shop_type),
                                    cookie_file=cookie_file_node or None,
                                    style=lstyle,
                                )
                                url_p = pack_p["url"]
                                hdrs_p = {
                                    str(k): str(v)
                                    for k, v in pack_p["headers"].items()
                                }
                                if cookie_override:
                                    hdrs_p["Cookie"] = cookie_override
                                form_p = pack_p.get("form") or {}
                                form_pw = {
                                    str(k): str(v) for k, v in form_p.items()
                                }
                                resp_p = context.request.post(
                                    url_p,
                                    headers=hdrs_p,
                                    form=form_pw,
                                    timeout=timeout_ms,
                                )
                                st_p = resp_p.status
                                text_p = resp_p.text()
                                print(
                                    f"[京东] sku={sku} 列表页 pageNum={pnum} "
                                    f"HTTP {st_p} {resp_p.status_text or ''}",
                                    file=sys.stderr,
                                )
                                if args.raise_http and st_p // 100 != 2:
                                    print(
                                        "[京东] 列表分页 --raise-http 已启用",
                                        file=sys.stderr,
                                    )
                                    sys.exit(1)
                                parsed_p = _loads_jd_plain_json(text_p)
                                http_ok_p = 200 <= st_p < 300
                                row_p = {
                                    "sku": sku,
                                    "kind": "comment_list_page",
                                    "page_num": pnum,
                                    "http_status": st_p,
                                    "http_ok": http_ok_p,
                                    "ok": http_ok_p
                                    and _jd_business_ok(parsed_p),
                                    "parsed": parsed_p,
                                    "raw": text_p if parsed_p is None else None,
                                }
                                line_p = json.dumps(row_p, ensure_ascii=False)
                                if out_f:
                                    out_f.write(line_p + "\n")
                                else:
                                    sys.stdout.write(line_p + "\n")

                                flat_p = extract_comment_rows_from_parsed(
                                    sku, parsed_p
                                )
                                if comments_path is not None:
                                    if comments_jsonl_f is not None:
                                        _append_comments_jsonl(
                                            comments_jsonl_f, flat_p
                                        )
                                    else:
                                        comments_csv_rows.extend(flat_p)
                                gap_before_next = True
            finally:
                browser.close()
    finally:
        if out_f:
            out_f.close()
        if comments_jsonl_f:
            comments_jsonl_f.close()
        if comments_path is not None and comments_path.suffix.lower() == ".csv":
            _write_comments_csv(comments_path, comments_csv_rows)


if __name__ == "__main__":
    main()
