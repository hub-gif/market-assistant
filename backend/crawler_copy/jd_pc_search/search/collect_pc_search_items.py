# -*- coding: utf-8 -*-
"""
pc_search 多逻辑页采集（供 ``jd_search_playwright`` 与 ``jd_keyword_pipeline`` 共用）。
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse

class SearchCollectionCancelled(Exception):
    """工作台请求终止：携带已解析、尚未 jd_row_to_export 的累计行。"""

    def __init__(self, partial_rows: list[dict[str, str]]) -> None:
        self.partial_rows = partial_rows
        super().__init__("search collection cancelled")


from jd_h5_search_requests import (
    JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE,
    JD_PC_SEARCH_FALLBACK_S_STEP,
    JD_PC_SEARCH_ITEMS_PER_PAGE,
    _detect_blocked,
    _jd_row_count_for_page,
    export_pc_search_request_json,
    jd_pc_api_body_page_first_pack,
    jd_row_to_export,
    parse_items_and_pc_search_s_step_from_response_body,
    pc_search_response_is_empty_ware_list,
    pc_search_should_retry_fetch,
    pc_search_ware_list_slot_count_from_body,
    sleep_pc_search_request_gap,
)


def _pc_request_record_from_url(url: str) -> dict[str, object]:
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    flat: dict[str, object] = {}
    for k, v in q.items():
        flat[k] = v[0] if len(v) == 1 else v
    body_raw = flat.get("body")
    body_json: object = None
    if isinstance(body_raw, str):
        try:
            body_json = json.loads(unquote(body_raw))
        except json.JSONDecodeError:
            body_json = body_raw
    return {
        "url_host": u.netloc,
        "url_path": u.path,
        "query_params": flat,
        "body_param_json": body_json,
    }


def save_pc_request_record(
    directory: Path,
    seq: int,
    *,
    label: str,
    keyword: str,
    api_page: int,
    api_s: int,
    log_ctx: str,
    url: str,
    headers: dict[str, str],
    http_status: int,
    status_text: str,
    content_type: str,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    safe_label = re.sub(r"[^\w.\-]+", "_", label).strip("_") or "req"
    path = directory / f"pc_request_{seq:03d}_{safe_label}_p{api_page}_s{api_s}.json"
    record: dict[str, object] = {
        "seq": seq,
        "keyword": keyword,
        "log_ctx": log_ctx,
        "api_body_page": api_page,
        "api_body_s": api_s,
        "http_status": http_status,
        "http_status_text": status_text,
        "response_content_type": content_type,
        "request_url_full": url,
        **_pc_request_record_from_url(url),
        "request_headers": headers,
    }
    path.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"[京东] 已保存请求记录: {path}", file=sys.stderr)


def save_pc_search_response_raw(
    directory: Path,
    seq: int,
    body: str,
    *,
    label: str,
    req_page: int,
    req_s: int,
    pretty: bool,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    ext = "json" if pretty else "js"
    safe_label = re.sub(r"[^\w.\-]+", "_", label).strip("_") or "resp"
    path = directory / f"pc_search_{seq:03d}_{safe_label}_req_p{req_page}_s{req_s}.{ext}"
    if pretty:
        try:
            out = json.dumps(json.loads(body), ensure_ascii=False, indent=2) + "\n"
        except json.JSONDecodeError:
            out = body
    else:
        out = body
    path.write_text(out, encoding="utf-8")
    print(f"[京东] 已保存原始响应: {path}", file=sys.stderr)


def collect_pc_search_export_rows(
    context: Any,
    args: SimpleNamespace,
    *,
    page_start: int,
    pe: int,
    req_delay_range: tuple[float, float] | None,
    save_js_dir: Path | None,
    record_req_dir: Path | None,
    node_pvid: str | None,
    cancel_check: Callable[[], bool] | None = None,
    node_cookie_file: str | None = None,
) -> list[dict[str, str]]:
    """
    执行与 ``jd_search_playwright`` 相同的多页 pc_search 逻辑，返回
    ``jd_row_to_export`` 后的行（CSV 表头为键），不写 CSV 文件。
    """
    fetch_seq: list[int] = [0]
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()

    api_page = 1
    api_s = 1
    run_aborted = False
    n_api_requests = 0

    def _fetch_pc_body_pw(
        ap: int,
        as_: int,
        *,
        log_ctx: str = "",
        save_label: str = "fetch",
        apply_request_gap: bool = True,
    ) -> tuple[str, str, int]:
        nonlocal n_api_requests
        if cancel_check is not None and cancel_check():
            raise SearchCollectionCancelled(list(all_rows))
        if apply_request_gap and n_api_requests > 0:
            sleep_pc_search_request_gap(req_delay_range)
        if cancel_check is not None and cancel_check():
            raise SearchCollectionCancelled(list(all_rows))
        n_api_requests += 1
        fetch_seq[0] += 1
        seq_n = fetch_seq[0]
        data = export_pc_search_request_json(
            args.q,
            ap,
            s=as_,
            pvid=node_pvid,
            cookie_file=node_cookie_file,
        )
        if cancel_check is not None and cancel_check():
            raise SearchCollectionCancelled(list(all_rows))
        u = data["url"]
        hdrs = {str(k): str(v) for k, v in data["headers"].items()}
        r = context.request.get(u, headers=hdrs)
        ctx = f" {log_ctx}" if log_ctx else ""
        print(
            f"[京东]{ctx} body.page={ap} body.s={as_} "
            f"HTTP {r.status} {r.status_text}",
            file=sys.stderr,
        )
        ct = r.headers.get("content-type", "") or ""
        if record_req_dir is not None:
            save_pc_request_record(
                record_req_dir,
                seq_n,
                label=save_label,
                keyword=args.q,
                api_page=ap,
                api_s=as_,
                log_ctx=log_ctx,
                url=u,
                headers=hdrs,
                http_status=r.status,
                status_text=r.status_text or "",
                content_type=ct,
            )
        return u, r.text(), seq_n

    max_fetch_tries = max(1, int(args.fetch_retries) + 1)
    retry_pause = max(0.0, float(args.fetch_retry_delay))
    last_s_step = JD_PC_SEARCH_FALLBACK_S_STEP

    for _skip_screen in range(max(0, page_start - 1)):
        for _skip_chunk in range(JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE):
            sl_base = (
                f"skip_screen{_skip_screen + 1}_"
                f"chunk{_skip_chunk + 1}"
            )
            url, body, seq_n = "", "", 0
            _skip_rows: list[dict[str, str]] = []
            s_step = 0
            blocked: str | None = None
            for rt in range(max_fetch_tries):
                sl = sl_base if rt == 0 else f"{sl_base}_retry{rt}"
                if rt > 0:
                    if retry_pause > 0:
                        time.sleep(retry_pause)
                    print(
                        f"[京东] 跳过前序屏 重试 {rt}/{max_fetch_tries - 1} "
                        f"body.page={api_page} body.s={api_s}",
                        file=sys.stderr,
                    )
                url, body, seq_n = _fetch_pc_body_pw(
                    api_page,
                    api_s,
                    log_ctx="跳过前序屏",
                    save_label=sl,
                    apply_request_gap=(rt == 0),
                )
                if save_js_dir is not None:
                    save_pc_search_response_raw(
                        save_js_dir,
                        seq_n,
                        body,
                        label=sl,
                        req_page=api_page,
                        req_s=api_s,
                        pretty=args.pretty_raw_json,
                    )
                blocked = _detect_blocked(body)
                if blocked:
                    break
                _skip_rows, s_step = (
                    parse_items_and_pc_search_s_step_from_response_body(
                        body,
                        keyword=args.q,
                        page=page_start,
                        request_api_page=api_page,
                        request_body_s=api_s,
                    )
                )
                if _skip_rows or s_step > 0:
                    break
                if rt + 1 < max_fetch_tries and pc_search_should_retry_fetch(
                    body, has_rows=bool(_skip_rows), s_step=s_step
                ):
                    continue
                break
            if blocked:
                print(
                    f"[京东] 跳过前序屏时 body.page={api_page} body.s={api_s}：{blocked}",
                    file=sys.stderr,
                )
                print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                run_aborted = True
                break
            if s_step <= 0 and not _skip_rows:
                if pc_search_response_is_empty_ware_list(body):
                    print(
                        "[京东] 跳过前序屏：接口返回空 wareList，停止",
                        file=sys.stderr,
                    )
                    run_aborted = True
                    break
                print(
                    f"[京东] 跳过前序屏 本包仍无有效数据，按 Δs={last_s_step} 强推进游标并继续",
                    file=sys.stderr,
                )
                api_page += 1
                api_s += last_s_step
                continue
            last_s_step = max(last_s_step, s_step)
            api_page += 1
            api_s += s_step
        if run_aborted:
            break

    if not run_aborted:
        expect_after_skip = jd_pc_api_body_page_first_pack(page_start)
        if api_page != expect_after_skip:
            print(
                f"[京东] 警告：跳过前序页后首包 body.page 应为 {expect_after_skip}，当前 {api_page}",
                file=sys.stderr,
            )

    if not run_aborted:
        for user_p in range(page_start, pe + 1):
            page_aborted = False
            expect_first = jd_pc_api_body_page_first_pack(user_p)
            if api_page != expect_first:
                print(
                    f"[京东] 警告：逻辑第{user_p}页 首包 body.page 应为 {expect_first}，当前 {api_page}",
                    file=sys.stderr,
                )
            for _attempt in range(JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE):
                sl_base = (
                    f"logic{user_p}_"
                    f"chunk{_attempt + 1}of{JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE}"
                )
                url, body, seq_n = "", "", 0
                rows: list[dict[str, str]] = []
                s_step = 0
                blocked: str | None = None
                for rt in range(max_fetch_tries):
                    sl = sl_base if rt == 0 else f"{sl_base}_retry{rt}"
                    if rt > 0:
                        if retry_pause > 0:
                            time.sleep(retry_pause)
                        print(
                            f"[京东] 逻辑第{user_p}页 第{_attempt + 1}包 "
                            f"重试 {rt}/{max_fetch_tries - 1} "
                            f"body.page={api_page} body.s={api_s}",
                            file=sys.stderr,
                        )
                    url, body, seq_n = _fetch_pc_body_pw(
                        api_page,
                        api_s,
                        log_ctx=(
                            f"逻辑第{user_p}页 "
                            f"第{_attempt + 1}/{JD_PC_SEARCH_CHUNKS_PER_LOGICAL_PAGE}包"
                        ),
                        save_label=sl,
                        apply_request_gap=(rt == 0),
                    )
                    if save_js_dir is not None:
                        save_pc_search_response_raw(
                            save_js_dir,
                            seq_n,
                            body,
                            label=sl,
                            req_page=api_page,
                            req_s=api_s,
                            pretty=args.pretty_raw_json,
                        )
                    blocked = _detect_blocked(body)
                    if blocked:
                        break
                    rows, s_step = (
                        parse_items_and_pc_search_s_step_from_response_body(
                            body,
                            keyword=args.q,
                            page=user_p,
                            request_api_page=api_page,
                            request_body_s=api_s,
                        )
                    )
                    if rows or s_step > 0:
                        break
                    if rt + 1 < max_fetch_tries and pc_search_should_retry_fetch(
                        body, has_rows=bool(rows), s_step=s_step
                    ):
                        continue
                    break
                if blocked:
                    print(
                        f"[京东] CSV第{user_p}页 "
                        f"body.page={api_page} body.s={api_s}：{blocked}",
                        file=sys.stderr,
                    )
                    print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                    page_aborted = True
                    break

                if not rows and s_step <= 0:
                    if pc_search_response_is_empty_ware_list(body):
                        print(
                            f"[京东] CSV第{user_p}页 "
                            f"body.page={api_page} body.s={api_s}："
                            f"接口空 wareList，无更多商品，停止采集",
                            file=sys.stderr,
                        )
                        page_aborted = True
                        break
                    print(
                        f"[京东] CSV第{user_p}页 "
                        f"body.page={api_page} body.s={api_s}："
                        f"多次重试后仍无商品；按 Δs={last_s_step} 强推进并继续下一包",
                        file=sys.stderr,
                    )
                    print(f"  当前 URL: {url[:160]}…", file=sys.stderr)
                    out_dbg = getattr(args, "out", None)
                    if out_dbg and args.csv:
                        dbg = Path(out_dbg).with_suffix(
                            ".debug.json"
                            if body.lstrip().startswith("{")
                            else ".debug.html"
                        )
                        dbg.write_text(body, encoding="utf-8")
                        print(f"  已保存调试样本: {dbg}", file=sys.stderr)
                    api_page += 1
                    api_s += last_s_step
                    continue

                if not rows and s_step > 0:
                    last_s_step = max(last_s_step, s_step)
                    api_page += 1
                    api_s += s_step
                    continue

                pack_skus = {
                    (r.get("sku_id") or "").strip()
                    for r in rows
                    if (r.get("sku_id") or "").strip()
                }
                dup_vs_accumulated = len(pack_skus & seen)

                n_added = 0
                for r in rows:
                    sku = (r.get("sku_id") or "").strip()
                    if not sku or sku in seen:
                        continue
                    if (
                        _jd_row_count_for_page(all_rows, user_p)
                        >= JD_PC_SEARCH_ITEMS_PER_PAGE
                    ):
                        break
                    seen.add(sku)
                    all_rows.append(r)
                    n_added += 1

                slots = pc_search_ware_list_slot_count_from_body(body)
                slot_s = str(slots) if slots is not None else "?"
                slot_gap = ""
                if slots is not None and len(rows) < slots:
                    slot_gap = (
                        f"，{slots - len(rows)} 个槽为无 SKU 占位（活动/对比卡等）未入库"
                    )
                dup_note = ""
                if dup_vs_accumulated:
                    dup_note = (
                        f"；本包 {dup_vs_accumulated} 个 SKU 与此前已采重复"
                        f"（去重后本包新增 {n_added}）"
                    )
                print(
                    f"[京东] 逻辑第{user_p}页 第{_attempt + 1}包 "
                    f"wareList 槽位={slot_s}，本包解析 {len(rows)} 行"
                    f"{slot_gap}，新增 CSV {n_added}{dup_note}",
                    file=sys.stderr,
                )

                last_s_step = max(last_s_step, s_step)
                api_page += 1
                api_s += s_step

            if page_aborted:
                run_aborted = True
                break
            if user_p < pe:
                if cancel_check is not None and cancel_check():
                    raise SearchCollectionCancelled(list(all_rows))
                time.sleep(max(0.0, args.page_delay))

    if not run_aborted:
        per_page = [
            _jd_row_count_for_page(all_rows, p)
            for p in range(page_start, pe + 1)
        ]
        print(
            f"[京东] 小结：逻辑页 {page_start}–{pe}，"
            f"各页 CSV 行数（按 page 列）{per_page}，"
            f"目标≤{JD_PC_SEARCH_ITEMS_PER_PAGE}/页；"
            f"pc_search {n_api_requests} 次，全局去重合计 {len(all_rows)}。"
            f" 说明：每包列表槽位数以响应 wareList 长度为准（会随场景变化）；"
            f"一屏内多包槽位之和也不是固定值。"
            f"CSV 为 SKU 全局去重行数，不必等于各包槽位之和；"
            f"若「新增 CSV」远小于当包槽位，多为游标重叠（核对 body.s 与上文逐包日志）。",
            file=sys.stderr,
        )

    return [jd_row_to_export(r) for r in all_rows]
