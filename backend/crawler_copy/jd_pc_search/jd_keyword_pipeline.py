# -*- coding: utf-8 -*-
"""
关键词 → 京东 PC 搜索 → 对若干 SKU 拉取详情（pc_detailpage_wareBusiness）与评论（首屏 Lego，
可选继续 ``getCommentListPage`` 分页，与同目录 ``jd_h5_item_comment_requests`` 一致），
合并为一行 CSV（搜索列 + 详情摘要 + 评价摘要）。

依赖：Node（搜索/评论签 h5st）、Playwright、本仓库 ``common/jd_cookie.txt``。

用法：修改下方「运行配置」后，在项目任意目录执行::

  python crawler/jd_pc_search/jd_keyword_pipeline.py

或::

  cd crawler/jd_pc_search && python jd_keyword_pipeline.py

每次运行默认在 ``data/JD/pipeline_runs/<时间戳>_<关键词>/`` 下集中写入：合并表、
PC 搜索导出 CSV、评价扁平 CSV、详情汇总 CSV（``detail_ware_export.csv``）、
各 SKU 规整 JSON（``detail/ware_{sku}_response.json``），以及（可选）pc_search 原始包与请求记录。

合并表 ``keyword_pipeline_merged.csv`` 默认 ``MERGED_CSV_MODE=lean``：搜索全列 + **竞品报告/入库实际用到的商详子集**（见 ``_MERGED_LEAN_DETAIL_FIELDNAMES``）+ 评论摘要；全量商详扁平请设 ``MERGED_CSV_MODE="full"``（``WARE_BUSINESS_MERGE_FIELDNAMES``）。
``detail_ware_export.csv`` 默认 ``DETAIL_WARE_CSV_MODE=lean``，为 ``skuId`` + 与合并表一致的商详子集（品牌/到手价/店铺/类目/参数/配料）；全列请设 ``DETAIL_WARE_CSV_MODE="full"``。
若 ``EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES=True``，配料列为**配料表文本**（``detail_body_ingredients_source_url`` 仅在内存/全量详情 CSV 使用，**不写** lean 合并表）；关闭视觉提取时合并表配料列为 **#detail-main 长图 URL 串**。
默认启用 **应用场景筛选**（``brief_content.txt`` 4.1 中式面点/主食 + 4.2 烘焙）：仅命中关键词的 SKU 进入详情与评论队列；词表见 ``scenario_filter.py``。``SCENARIO_FILTER_ENABLED=False`` 可关闭；``SCENARIO_FILTER_PC_SEARCH_CSV="filtered"`` 可使导出 CSV 与筛选后列表一致。
各 SKU 完整接口 JSON 仍在 ``detail/ware_{sku}_response.json``。

端到端竞品速览 Markdown：配置 ``jd_competitor_report.py`` 顶部 ``KEYWORD`` 后执行 ``python jd_competitor_report.py``（内部调用本模块 ``main(keyword=...)``）。
"""

from __future__ import annotations

import csv
import json
import random
import sys
import time
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# 路径与运行配置
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent
from _low_gi_root import low_gi_project_root  # noqa: E402

_PROJECT_ROOT = low_gi_project_root()
# 京东采集统一目录（与 search / detail / comment 脚本默认一致；副本依赖 LOW_GI_PROJECT_ROOT）
_PROJECT_DATA = _PROJECT_ROOT / "data" / "JD"
_COOKIE_FILE = str((_ROOT / "common" / "jd_cookie.txt").resolve())
# 运行时覆盖（由 Market-Assistant 等在 main() 前设置）：非空则优先于 _COOKIE_FILE / 默认空 override
PIPELINE_COOKIE_FILE = ""
PIPELINE_COOKIE_OVERRIDE = ""

# KEYWORD：搜索词（与 pc_search 一致）
KEYWORD = "低GI"
# PAGE_START / PAGE_TO：逻辑页范围（与 jd_search_playwright 含义相同）
PAGE_START = 1
PAGE_TO = 2
# PVID：可选，与搜索结果页 URL 中 pvid 一致时填写
PVID = ""
# REQUEST_DELAY：pc_search 包间随机等待，如 "30-60"；None 关闭
REQUEST_DELAY = "30-60"
PAGE_DELAY_SEC = 1.2
FETCH_RETRIES = 3
FETCH_RETRY_DELAY_SEC = 3.0
# PIPELINE_RUN_DIR：本次运行输出根目录。空则自动创建
# ``data/JD/pipeline_runs/<YYYYMMDD_HHMMSS>_<关键词>/``；非空则用该路径（相对路径相对 data/JD）
PIPELINE_RUN_DIR = ""
# 工作台「终止任务」：可调用无参，返回 True 时在可停点结束并写出已采集部分（协作式，非杀进程）
PIPELINE_CANCEL_CHECK = None  # Callable[[], bool] | None
# 是否将 pc_search 原始响应 / 请求记录写入运行目录子文件夹（与 jd_search_playwright 一致）
PIPELINE_SAVE_PC_SEARCH_RAW = True
PIPELINE_SAVE_PC_SEARCH_RECORDS = True
# 非空时覆盖上面两项，直接指定目录（与单独跑搜索脚本相同）
SAVE_SEARCH_RAW_DIR = ""
RECORD_SEARCH_REQ_DIR = ""

# MAX_SKUS：搜索去重后，最多对多少个 SKU 继续拉详情+评论（控制总耗时）
MAX_SKUS = 5
# COMMENT_NUM：Lego 接口 body.commentNum（仅首屏条数；更多评价靠分页）
COMMENT_NUM = 5
SHOP_TYPE = "0"
# WITH_COMMENT_LIST：首屏 Lego 成功后是否继续请求评价列表分页（POST client.action）
WITH_COMMENT_LIST = True
# LIST_PAGES：分页规格，如 "1"、"1-5"、"1,3,5"（与 jd_h5_item_comment_requests 相同）
LIST_PAGES = "1-2"
LIST_FUNCTION_ID = "getCommentListPage"
LIST_STYLE = "1"
# LIST_CATEGORY / LIST_FIRST_GUID：一般留空，从首屏 parsed 解析；抓包不一致时再填
LIST_CATEGORY = ""
LIST_FIRST_GUID = ""
# COMMENT_LIST_DELAY：分页请求之间的随机等待；空字符串表示沿用 SKU_STEP_DELAY
COMMENT_LIST_DELAY = ""
# SKU_STEP_DELAY：每个 SKU 内「详情→首评」及步骤间随机等待（秒）
SKU_STEP_DELAY = "4-10"
# 详情：与 jd_detail_ware_business_requests 一致，结果为空或无效时重试
DETAIL_FETCH_MAX_ATTEMPTS = 3
DETAIL_FETCH_RETRY_DELAY_SEC = 2.0
# USE_CHROME：True 使用本机 Chrome
USE_CHROME = True
HEADED = False

# 运行目录内固定文件名（一般无需改）
FILE_MERGED_CSV = "keyword_pipeline_merged.csv"
FILE_PC_SEARCH_CSV = "pc_search_export.csv"
FILE_COMMENTS_FLAT_CSV = "comments_flat.csv"
FILE_DETAIL_WARE_CSV = "detail_ware_export.csv"
FILE_RUN_META_JSON = "run_meta.json"
# MERGED_CSV_MODE：``lean`` 时合并表为搜索全列 + 商详子集（``_MERGED_LEAN_DETAIL_FIELDNAMES``）+ 评论摘要；``full`` 为搜索全列 + ``WARE_BUSINESS_MERGE_FIELDNAMES`` 全量
MERGED_CSV_MODE = "lean"
# DETAIL_WARE_CSV_MODE：``lean`` 时 ``detail_ware_export.csv`` 为 ``skuId`` + lean 商详子集；``full`` 为完整详情扁平列（含 http_status 与各 detail_*）
DETAIL_WARE_CSV_MODE = "lean"
# 应用场景筛选（对齐 brief 4.1 中式面点/主食 + 4.2 烘焙）：仅命中关键词的商品进入详情/评论队列
SCENARIO_FILTER_ENABLED = True
# ``pc_search_export.csv``：``full`` 保留搜索全量；``filtered`` 仅写入命中场景的行（与详情样本一致）
SCENARIO_FILTER_PC_SEARCH_CSV = "full"
# 若启用筛选后无命中行，是否回退为未筛选列表（避免跑空）；False 则仍按空列表继续
SCENARIO_FILTER_FALLBACK_TO_UNFILTERED = True
# True：对 ``meta`` 中 ``detail_body_image_urls`` 从后往前调用 ``AI_crawler``，**首次**校验通过即写入配料；
# 未命中时列内为 ``【未识别到配料】…`` 原因说明（非空串）。需 .env；未配置 API 时写入对应提示。关此开关时该列仍为长图 URL 串。
EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES = True
# ---------------------------------------------------------------------------

# 保证可导入 search / comment / detail 下脚本与 common
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_SEARCH_DIR = _ROOT / "search"
_COMMENT_DIR = _ROOT / "comment"
_DETAIL_DIR = _ROOT / "detail"
for _p in (_SEARCH_DIR, _COMMENT_DIR, _DETAIL_DIR):
    s = str(_p.resolve())
    if s not in sys.path:
        sys.path.insert(0, s)

from collect_pc_search_items import (  # noqa: E402
    SearchCollectionCancelled,
    collect_pc_search_export_rows,
)
from common.jd_delay_utils import parse_request_delay_range  # noqa: E402
from scenario_filter import filter_rows_by_scenario  # noqa: E402
from jd_detail_ware_business_requests import (  # noqa: E402
    DETAIL_WARE_LEAN_CSV_FIELDNAMES,
    WARE_BUSINESS_MERGE_FIELDNAMES,
    WARE_PARSED_CSV_FIELDNAMES,
    _JD_DETAIL_CONTEXT_EXTRA_HEADERS,
    _JD_DETAIL_UA,
    detail_ware_lean_csv_row,
    fetch_ware_business,
    format_ware_response_for_save,
    parse_ware_business_response_text,
    ware_parsed_row,
)
from jd_h5_item_comment_requests import (  # noqa: E402
    category_and_first_guid_from_lego,
    export_item_comment_page_request_json,
    export_item_comment_request_json,
    extract_comment_rows_from_parsed,
    parse_list_pages_spec,
    write_comments_flat_csv,
)
from jd_h5_search_requests import (  # noqa: E402
    CSV_FIELDS,
    JD_EXPORT_COLUMN_HEADERS,
    jd_row_to_export,
)


_SKU_CSV_HEADER = JD_EXPORT_COLUMN_HEADERS["sku_id"]

_MERGED_EXTRA_FIELDS = (
    ["pipeline_keyword"]
    + list(WARE_BUSINESS_MERGE_FIELDNAMES)
    + ["comment_count", "comment_preview"]
)

# lean 合并表·商详块（jd_competitor_report + ingest + 配料）；须与 pipeline/csv_schema.MERGED_LEAN_DETAIL_KEYS 一致
_MERGED_LEAN_DETAIL_FIELDNAMES: tuple[str, ...] = (
    "detail_brand",
    "detail_price_final",
    "detail_shop_name",
    "detail_category_path",
    "detail_product_attributes",
    "detail_body_ingredients",
)

# 合并表精简列：搜索列与 jd_h5_search_requests 一致 + 上表商详子集 + 评论摘要
_MERGED_LEAN_FIELDNAMES: tuple[str, ...] = (
    "pipeline_keyword",
    "SKU(skuId)",
    "主商品ID(wareId)",
    "标题(wareName)",
    "标价(jdPrice,jdPriceText,realPrice)",
    "券后到手价(couponPrice,subsidyPrice,finalPrice.estimatedPrice,priceShow)",
    "原价(oriPrice,originalPrice,marketPrice)",
    "卖点(sellingPoint)",
    "榜单类文案(标签/腰带/标题数组中的榜、TOP 等)",
    "评价量(commentFuzzy)",
    "销量楼层(commentSalesFloor)",
    "店铺名(shopName)",
    "商品链接(toUrl,clickUrl,item.m.jd.com)",
    "主图(imageurl,imageUrl)",
    "规格属性(propertyList,color,catid,shortName)",
    "类目(leafCategory,cid3Name,catid)",
    "搜索词(keyword)",
    "页码(page)",
    *_MERGED_LEAN_DETAIL_FIELDNAMES,
    "comment_count",
    "comment_preview",
)


def _merged_csv_fieldnames() -> list[str]:
    if (MERGED_CSV_MODE or "lean").strip().lower() == "full":
        return list(CSV_FIELDS) + [
            f for f in _MERGED_EXTRA_FIELDS if f not in CSV_FIELDS
        ]
    return list(_MERGED_LEAN_FIELDNAMES)


def _detail_ware_csv_fieldnames() -> list[str]:
    if (DETAIL_WARE_CSV_MODE or "lean").strip().lower() == "full":
        return list(WARE_PARSED_CSV_FIELDNAMES)
    return list(DETAIL_WARE_LEAN_CSV_FIELDNAMES)


def _sleep_range(spec: str, label: str) -> None:
    try:
        lo, hi = parse_request_delay_range(spec)
    except ValueError:
        return
    if hi <= 0 and lo <= 0:
        return
    t = random.uniform(lo, hi)
    print(f"[流水线] {label} 等待 {t:.1f}s", file=sys.stderr)
    time.sleep(t)


def _dedupe_comment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按 commentId 去重（跨首屏 + 多页列表）。"""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        cid = str(r.get("commentId") or "").strip()
        if cid:
            if cid in seen:
                continue
            seen.add(cid)
        out.append(r)
    return out


def _comment_fields_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    previews: list[str] = []
    for r in rows[:8]:
        t = str(r.get("tagCommentContent") or "").strip()
        if t:
            previews.append(t[:400])
    joined = " | ".join(previews)[:4000]
    return {
        "comment_count": str(len(rows)),
        "comment_preview": joined,
    }


def _loads_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _safe_dir_segment(s: str, max_len: int = 48) -> str:
    bad = '<>:"/\\|?*\n\r\t'
    t = "".join("_" if c in bad else c for c in (s or "").strip())[:max_len]
    t = t.strip(" .") or "run"
    return t


def _resolve_pipeline_run_dir(kw: str) -> Path:
    raw = (PIPELINE_RUN_DIR or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = _PROJECT_DATA / p
        return p.resolve()
    stamp = time.strftime("%Y%m%d_%H%M%S")
    seg = _safe_dir_segment(kw)
    return (_PROJECT_DATA / "pipeline_runs" / f"{stamp}_{seg}").resolve()


class PipelineCancelled(Exception):
    """工作台请求终止本次流水线；携带已分配的运行目录（可有部分产出）。"""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir.resolve()
        super().__init__("pipeline cancelled")


def _pipeline_cancel_requested() -> bool:
    fn = PIPELINE_CANCEL_CHECK
    try:
        return fn is not None and callable(fn) and bool(fn())
    except Exception:
        return False


def main(keyword: str | None = None) -> Path:
    """
    跑完整条流水线。``keyword`` 非空时覆盖文件内 ``KEYWORD``；返回本次运行目录。

    供 ``jd_competitor_report`` 等脚本 ``import`` 调用；命令行仍执行 ``main()`` 无参。
    """
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    kw = (keyword if keyword is not None else KEYWORD) or ""
    kw = str(kw).strip()
    if not kw:
        print("[流水线] 请配置 KEYWORD 或传入 keyword 参数", file=sys.stderr)
        sys.exit(2)

    page_start = max(1, int(PAGE_START))
    page_to = PAGE_TO if PAGE_TO is not None else page_start
    page_to = max(page_start, int(page_to))

    req_delay_range: tuple[float, float] | None = None
    if REQUEST_DELAY:
        try:
            req_delay_range = parse_request_delay_range(str(REQUEST_DELAY).strip())
        except ValueError as e:
            print(f"[流水线] REQUEST_DELAY 无效: {e}", file=sys.stderr)
            sys.exit(2)

    run_dir = _resolve_pipeline_run_dir(kw)
    run_dir.mkdir(parents=True, exist_ok=True)
    stop_pipeline = False
    print(f"[流水线] 本次输出目录: {run_dir}", file=sys.stderr)

    if (SAVE_SEARCH_RAW_DIR or "").strip():
        save_js = Path(SAVE_SEARCH_RAW_DIR).expanduser().resolve()
    elif PIPELINE_SAVE_PC_SEARCH_RAW:
        save_js = run_dir / "pc_search_raw"
    else:
        save_js = None

    if (RECORD_SEARCH_REQ_DIR or "").strip():
        record_req = Path(RECORD_SEARCH_REQ_DIR).expanduser().resolve()
    elif PIPELINE_SAVE_PC_SEARCH_RECORDS:
        record_req = run_dir / "pc_search_requests"
    else:
        record_req = None

    node_pvid = (PVID or "").strip() or None

    search_args = SimpleNamespace(
        q=kw,
        page_delay=float(PAGE_DELAY_SEC),
        fetch_retries=int(FETCH_RETRIES),
        fetch_retry_delay=float(FETCH_RETRY_DELAY_SEC),
        pretty_raw_json=True,
        csv=True,
        out="",
    )

    print(f"[流水线] 搜索词={kw!r} 逻辑页 {page_start}–{page_to}", file=sys.stderr)

    merged_rows: list[dict[str, str]] = []
    all_comment_rows: list[dict[str, Any]] = []
    detail_csv_rows: list[dict[str, str]] = []
    launch_kw: dict[str, Any] = {"headless": not HEADED}
    if USE_CHROME:
        launch_kw["channel"] = "chrome"

    _ac_mod: Any = None
    _ingredient_vision_ok = False
    if EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES:
        try:
            import AI_crawler as _ac_mod  # noqa: WPS433

            _ac_mod._resolve_credentials(None, None, None)
            _ingredient_vision_ok = True
        except Exception as e:
            print(
                f"[流水线] 已开启配料视觉提取但未就绪（{e}），"
                f"各 SKU 列 detail_body_ingredients 将写入「未配置 API」类原因说明（非 URL）",
                file=sys.stderr,
            )

    _pcf = (PIPELINE_COOKIE_FILE or "").strip()
    if _pcf:
        _pcfp = Path(_pcf).expanduser().resolve()
        cookie_path = str(_pcfp) if _pcfp.is_file() else None
        if cookie_path is None:
            print(
                f"[流水线] 警告：PIPELINE_COOKIE_FILE 不是有效文件，将回退 common/jd_cookie.txt："
                f"{_pcf!r}",
                file=sys.stderr,
            )
    else:
        cookie_path = None
    if cookie_path is None:
        cookie_path = _COOKIE_FILE if Path(_COOKIE_FILE).is_file() else None
    _cookie_override = (PIPELINE_COOKIE_OVERRIDE or "").strip()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(**launch_kw)
        ctx = browser.new_context()
        try:
            export_rows_full = collect_pc_search_export_rows(
                ctx,
                search_args,
                page_start=page_start,
                pe=page_to,
                req_delay_range=req_delay_range,
                save_js_dir=save_js,
                record_req_dir=record_req,
                node_pvid=node_pvid,
                cancel_check=_pipeline_cancel_requested,
                node_cookie_file=cookie_path,
            )
        except SearchCollectionCancelled as e:
            export_rows_full = [jd_row_to_export(r) for r in e.partial_rows]
            stop_pipeline = True
            print(
                "[流水线] 已按请求在下一请求前终止 PC 搜索（保留已得行）",
                file=sys.stderr,
            )
        if _pipeline_cancel_requested():
            stop_pipeline = True

        scenario_filter_on = bool(SCENARIO_FILTER_ENABLED)
        scenario_stats: dict[str, Any] | None = None
        export_rows_for_skus: list[dict[str, str]] = list(export_rows_full)
        if scenario_filter_on:
            fr, scenario_stats = filter_rows_by_scenario(export_rows_full)
            export_rows_for_skus = fr
            print(
                f"[流水线] 应用场景筛选：全量 {scenario_stats['input_rows']} 行 → "
                f"保留 {scenario_stats['kept_rows']} 行（剔除 {scenario_stats['dropped_rows']}），"
                f"标签分布 {scenario_stats.get('tag_counts')!r}",
                file=sys.stderr,
            )
            if not export_rows_for_skus and SCENARIO_FILTER_FALLBACK_TO_UNFILTERED:
                print(
                    "[流水线] 筛选后无命中行，已按 SCENARIO_FILTER_FALLBACK_TO_UNFILTERED "
                    "回退为未筛选列表",
                    file=sys.stderr,
                )
                export_rows_for_skus = list(export_rows_full)
                scenario_stats = {
                    **(scenario_stats or {}),
                    "fallback_unfiltered": True,
                }

        csv_mode = (SCENARIO_FILTER_PC_SEARCH_CSV or "full").strip().lower()
        if csv_mode == "filtered":
            rows_for_search_csv = (
                export_rows_for_skus
                if scenario_filter_on
                else list(export_rows_full)
            )
        else:
            rows_for_search_csv = list(export_rows_full)

        skus_ordered: list[str] = []
        seen: set[str] = set()
        for row in export_rows_for_skus:
            sid = str(row.get(_SKU_CSV_HEADER) or "").strip()
            if not sid or sid in seen:
                continue
            seen.add(sid)
            skus_ordered.append(sid)
            if len(skus_ordered) >= max(1, int(MAX_SKUS)):
                break

        print(
            f"[流水线] 搜索导出 {len(export_rows_full)} 行（写入 CSV {len(rows_for_search_csv)} 行），"
            f"取前 {len(skus_ordered)} 个 SKU 拉详情+评论",
            file=sys.stderr,
        )
        if _pipeline_cancel_requested():
            stop_pipeline = True

        search_csv_path = run_dir / FILE_PC_SEARCH_CSV
        sbuf = StringIO()
        sw = csv.DictWriter(
            sbuf, fieldnames=list(CSV_FIELDS), extrasaction="ignore"
        )
        sw.writeheader()
        sw.writerows(rows_for_search_csv)
        search_csv_path.write_text("\ufeff" + sbuf.getvalue(), encoding="utf-8")
        print(
            f"[流水线] 已写 PC 搜索导出 {search_csv_path}",
            file=sys.stderr,
        )

        detail_dir = run_dir / "detail"
        detail_dir.mkdir(parents=True, exist_ok=True)

        detail_ctx = browser.new_context(
            user_agent=_JD_DETAIL_UA,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            extra_http_headers=dict(_JD_DETAIL_CONTEXT_EXTRA_HEADERS),
        )
        page = detail_ctx.new_page()

        for idx, sku in enumerate(skus_ordered):
            if stop_pipeline or _pipeline_cancel_requested():
                stop_pipeline = True
                break
            if idx > 0:
                if stop_pipeline or _pipeline_cancel_requested():
                    stop_pipeline = True
                    break
                _sleep_range(SKU_STEP_DELAY, "SKU 间隔")

            search_row = next(
                (
                    r
                    for r in export_rows_full
                    if str(r.get(_SKU_CSV_HEADER) or "").strip() == sku
                ),
                {},
            )
            merged: dict[str, str] = {k: str(search_row.get(k) or "") for k in CSV_FIELDS}
            merged["pipeline_keyword"] = kw

            if stop_pipeline or _pipeline_cancel_requested():
                stop_pipeline = True
                break

            d_code, d_text, d_meta = fetch_ware_business(
                detail_ctx,
                page,
                sku,
                cookie_file=cookie_path,
                timeout_ms=45_000,
                cookie_override=_cookie_override,
                max_attempts=int(DETAIL_FETCH_MAX_ATTEMPTS),
                retry_delay_sec=float(DETAIL_FETCH_RETRY_DELAY_SEC),
                cancel_check=_pipeline_cancel_requested,
            )
            body_for_parse = d_text if d_code == 200 else ""
            ware_flat, _wok = parse_ware_business_response_text(body_for_parse)
            merged.update(ware_flat)
            raw_body_urls = str(d_meta.get("detail_body_image_urls") or "").strip()
            _skip_vision_and_later_network = (
                stop_pipeline or _pipeline_cancel_requested()
            )
            if _skip_vision_and_later_network:
                stop_pipeline = True
            if (
                EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES
                and _ingredient_vision_ok
                and not _skip_vision_and_later_network
            ):
                try:
                    _ex_src = getattr(
                        _ac_mod,
                        "extract_ingredients_from_body_image_urls_reversed_with_source",
                        None,
                    )
                    if callable(_ex_src):
                        di, src_u = _ex_src(raw_body_urls)
                        merged["detail_body_ingredients"] = di
                        merged["detail_body_ingredients_source_url"] = (
                            str(src_u).strip() if src_u else ""
                        )
                    else:
                        merged["detail_body_ingredients"] = (
                            _ac_mod.extract_ingredients_from_body_image_urls_reversed(
                                raw_body_urls
                            )
                        )
                        merged["detail_body_ingredients_source_url"] = ""
                    di = merged["detail_body_ingredients"]
                    if str(di).startswith("【未识别"):
                        print(f"[流水线] sku={sku} {di}", file=sys.stderr)
                    else:
                        su = str(
                            merged.get("detail_body_ingredients_source_url") or ""
                        ).strip()
                        if su:
                            print(
                                f"[流水线] sku={sku} 已从详情长图解析配料表，图源: {su}",
                                file=sys.stderr,
                            )
                        else:
                            print(
                                f"[流水线] sku={sku} 已从详情长图（自后向前，首次命中）解析配料表",
                                file=sys.stderr,
                            )
                except Exception as e:
                    print(
                        f"[流水线] sku={sku} 配料视觉提取异常: {e}",
                        file=sys.stderr,
                    )
                    merged["detail_body_ingredients"] = (
                        f"【未识别到配料】识别过程异常：{e}"[:800]
                    )
                    merged["detail_body_ingredients_source_url"] = ""
            elif EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES and not _ingredient_vision_ok:
                merged["detail_body_ingredients"] = (
                    "【未识别到配料】未配置或无效的多模态 API，已跳过识别。"
                )
                merged["detail_body_ingredients_source_url"] = ""
            else:
                merged["detail_body_ingredients"] = raw_body_urls
                merged["detail_body_ingredients_source_url"] = ""

            response_body = format_ware_response_for_save(
                d_text or "",
                normalize=True,
                sort_keys=True,
                indent=2,
            )
            (detail_dir / f"ware_{sku}_response.json").write_text(
                response_body, encoding="utf-8"
            )
            _d_ing = str(merged.get("detail_body_ingredients") or "").strip()
            _d_src = str(
                merged.get("detail_body_ingredients_source_url") or ""
            ).strip()
            if (DETAIL_WARE_CSV_MODE or "lean").strip().lower() == "full":
                detail_csv_rows.append(
                    ware_parsed_row(
                        sku,
                        d_code,
                        d_text or "",
                        detail_body_ingredients=_d_ing,
                        detail_body_ingredients_source_url=_d_src,
                    )
                )
            else:
                detail_csv_rows.append(
                    detail_ware_lean_csv_row(
                        sku,
                        d_code,
                        d_text or "",
                        detail_body_ingredients=_d_ing,
                        detail_body_ingredients_source_url=_d_src,
                    )
                )

            if stop_pipeline or _pipeline_cancel_requested():
                stop_pipeline = True
                merged["comment_count"] = "0"
                merged["comment_preview"] = ""
                merged_rows.append(merged)
                break

            _sleep_range(SKU_STEP_DELAY, "详情→评论")

            if stop_pipeline or _pipeline_cancel_requested():
                stop_pipeline = True
                merged["comment_count"] = "0"
                merged["comment_preview"] = ""
                merged_rows.append(merged)
                break

            try:
                pack = export_item_comment_request_json(
                    sku,
                    comment_num=int(COMMENT_NUM),
                    shop_type=str(SHOP_TYPE),
                    cookie_file=cookie_path,
                )
            except SystemExit:
                merged["comment_count"] = "0"
                merged["comment_preview"] = ""
                merged_rows.append(merged)
                continue

            if stop_pipeline or _pipeline_cancel_requested():
                stop_pipeline = True
                merged["comment_count"] = "0"
                merged["comment_preview"] = ""
                merged_rows.append(merged)
                break

            try:
                url = pack["url"]
                hdrs = {str(k): str(v) for k, v in pack["headers"].items()}
                resp = ctx.request.get(url, headers=hdrs, timeout=45_000)
                c_st = resp.status
                c_text = resp.text()
                parsed = _loads_json(c_text)
                comment_rows: list[dict[str, Any]] = []
                if isinstance(parsed, dict):
                    comment_rows.extend(
                        extract_comment_rows_from_parsed(sku, parsed)
                    )
                list_delay = (
                    (COMMENT_LIST_DELAY or "").strip() or SKU_STEP_DELAY
                )
                if (
                    WITH_COMMENT_LIST
                    and isinstance(parsed, dict)
                    and 200 <= c_st < 300
                ):
                    cat, fguid = category_and_first_guid_from_lego(parsed)
                    if (LIST_CATEGORY or "").strip():
                        cat = LIST_CATEGORY.strip()
                    if (LIST_FIRST_GUID or "").strip():
                        fguid = LIST_FIRST_GUID.strip()
                    if cat and fguid:
                        pages = parse_list_pages_spec(LIST_PAGES or "1")
                        lfid = (LIST_FUNCTION_ID or "getCommentListPage").strip()
                        lstyle = (LIST_STYLE or "1").strip()
                        for pi, pnum in enumerate(pages):
                            if stop_pipeline or _pipeline_cancel_requested():
                                stop_pipeline = True
                                break
                            _sleep_range(list_delay, "评论列表分页")
                            if stop_pipeline or _pipeline_cancel_requested():
                                stop_pipeline = True
                                break
                            try:
                                pack_p = export_item_comment_page_request_json(
                                    sku,
                                    category=cat,
                                    first_guid=fguid,
                                    page_num=pnum,
                                    is_first=(pi == 0),
                                    function_id=lfid,
                                    shop_type=str(SHOP_TYPE),
                                    cookie_file=cookie_path,
                                    style=lstyle,
                                )
                            except SystemExit:
                                print(
                                    "[流水线] 评论分页 Node 失败，已停止该 SKU 后续分页",
                                    file=sys.stderr,
                                )
                                break
                            if stop_pipeline or _pipeline_cancel_requested():
                                stop_pipeline = True
                                break
                            url_p = pack_p["url"]
                            hdrs_p = {
                                str(k): str(v)
                                for k, v in pack_p["headers"].items()
                            }
                            form_p = {
                                str(k): str(v)
                                for k, v in (pack_p.get("form") or {}).items()
                            }
                            try:
                                resp_p = ctx.request.post(
                                    url_p,
                                    headers=hdrs_p,
                                    form=form_p,
                                    timeout=45_000,
                                )
                                st_p = resp_p.status
                                text_p = resp_p.text()
                            except Exception as e:
                                print(
                                    f"[流水线] 评论分页 POST 异常: {e}",
                                    file=sys.stderr,
                                )
                                break
                            parsed_p = _loads_json(text_p)
                            if isinstance(parsed_p, dict):
                                comment_rows.extend(
                                    extract_comment_rows_from_parsed(
                                        sku, parsed_p
                                    )
                                )
                    else:
                        print(
                            "[流水线] WITH_COMMENT_LIST 已开但缺少 category 或 "
                            "firstCommentGuid，仅保留首屏评价",
                            file=sys.stderr,
                        )
                comment_rows = _dedupe_comment_rows(comment_rows)
                merged.update(_comment_fields_from_rows(comment_rows))
                all_comment_rows.extend(comment_rows)
            except Exception as e:
                print(
                    f"[流水线] sku={sku} 评论请求异常: {e}",
                    file=sys.stderr,
                )
                merged["comment_count"] = "0"
                merged["comment_preview"] = ""

            merged_rows.append(merged)
            print(f"[流水线] [{idx + 1}/{len(skus_ordered)}] sku={sku} OK", file=sys.stderr)
            if stop_pipeline:
                break

        try:
            page.close()
        except Exception:
            pass
        try:
            detail_ctx.close()
        except Exception:
            pass
        browser.close()

    out_path = run_dir / FILE_MERGED_CSV
    fieldnames = _merged_csv_fieldnames()
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    w.writerows(merged_rows)
    out_path.write_text("\ufeff" + buf.getvalue(), encoding="utf-8")
    print(
        f"[流水线] 已写合并表 {out_path} 共 {len(merged_rows)} 行 "
        f"（MERGED_CSV_MODE={MERGED_CSV_MODE!r}，{len(fieldnames)} 列）",
        file=sys.stderr,
    )

    detail_csv_path = run_dir / FILE_DETAIL_WARE_CSV
    detail_csv_path.parent.mkdir(parents=True, exist_ok=True)
    detail_fn = _detail_ware_csv_fieldnames()
    with detail_csv_path.open("w", encoding="utf-8-sig", newline="") as dcf:
        dw = csv.DictWriter(
            dcf,
            fieldnames=detail_fn,
            extrasaction="ignore",
        )
        dw.writeheader()
        dw.writerows(detail_csv_rows)
    print(
        f"[流水线] 已写详情扁平表 {detail_csv_path} 共 {len(detail_csv_rows)} 行 "
        f"（DETAIL_WARE_CSV_MODE={DETAIL_WARE_CSV_MODE!r}，{len(detail_fn)} 列）",
        file=sys.stderr,
    )

    comments_path = run_dir / FILE_COMMENTS_FLAT_CSV
    write_comments_flat_csv(comments_path, all_comment_rows)
    print(
        f"[流水线] 已写评价扁平表 {comments_path} 共 {len(all_comment_rows)} 条",
        file=sys.stderr,
    )

    meta = {
        "keyword": kw,
        "page_start": page_start,
        "page_to": page_to,
        "max_skus_config": int(MAX_SKUS),
        "extract_ingredients_from_detail_body_images": bool(
            EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES
        ),
        "ingredient_vision_api_ready": bool(_ingredient_vision_ok),
        "scenario_filter_enabled": bool(SCENARIO_FILTER_ENABLED),
        "scenario_filter_pc_search_csv": (SCENARIO_FILTER_PC_SEARCH_CSV or "full")
        .strip()
        .lower(),
        "scenario_filter_stats": scenario_stats,
        "pc_search_export_rows": len(rows_for_search_csv),
        "pc_search_export_rows_full": len(export_rows_full),
        "merged_rows": len(merged_rows),
        "merged_csv_mode": (MERGED_CSV_MODE or "lean").strip().lower(),
        "merged_csv_column_count": len(fieldnames),
        "detail_ware_csv_mode": (DETAIL_WARE_CSV_MODE or "lean").strip().lower(),
        "detail_ware_csv_column_count": len(detail_fn),
        "comment_flat_rows": len(all_comment_rows),
        "detail_ware_csv_rows": len(detail_csv_rows),
        "with_comment_list": bool(WITH_COMMENT_LIST),
        "list_pages": (LIST_PAGES or "").strip(),
    }
    (run_dir / FILE_RUN_META_JSON).write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if stop_pipeline:
        print("[流水线] 已按请求终止（已写出当前进度）", file=sys.stderr)
        raise PipelineCancelled(run_dir)
    return run_dir


if __name__ == "__main__":
    main()
