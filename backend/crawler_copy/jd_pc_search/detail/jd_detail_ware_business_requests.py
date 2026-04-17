# -*- coding: utf-8 -*-
"""
京东 PC 详情 ``pc_detailpage_wareBusiness``（appid=pc-item-soa）。

**唯一链路**：打开 ``item.jd.com/{sku}.html``，可选点击底部锚点栏「商品详情」
（``#SPXQ-tab-column``，SPXQ=商品详情），再拦截页面发起的 ``api.m.jd.com`` 请求
（不经 Node 拼 ``h5st``，与真实浏览器一致）。开关见 ``CLICK_PRODUCT_DETAIL_TAB``。

**定位 tab 相关 JSON**：切到「商品详情」后，左侧 ``left-tabs-content`` 里 ``#SPXQ-title`` 下方常见 **整块 HTML 注入**（参数表 ``_scoped_*``、SSD 图文 ``ssd-module-wrap``、``#detail-main`` / ``#related-layout-head`` 等），
随后 Network 里会多出大量 **图片/CSS**（如 ``img30.360buyimg.com``、``.avif`` / ``background-image``），这些才是图文详情本体，**不是**一条可替代的「详情 JSON」。
页内脚本可能对 ``api.m.jd.com/client.action`` 发 ``mGetsByColor``（搭配 ``h5st``）等，用于模块内 **凑单/关联 SKU 价签**，与主图长页是两类数据。
结构化字段（品牌、编号、类目等）仍以本脚本拦截的 ``pc_detailpage_wareBusiness`` 为主；若要对齐 DevTools，设 ``LOG_API_M_JD_TRACE=True`` 看各阶段 ``functionId`` 与 URL。

未命中接口、HTTP 非 200、正文空、无法解析 JSON 或解析后业务字段全空时，按 ``FETCH_MAX_ATTEMPTS`` /
``FETCH_RETRY_DELAY_SEC`` 自动重试（``fetch_ware_business`` 的 ``max_attempts`` / ``retry_delay_sec``）。

落盘 / 单 SKU 标准输出时，可对 JSON **缩进 + 递归键排序**（``NORMALIZE_WARE_JSON`` / ``SORT_JSON_KEYS``），便于阅读与 diff。

**输出路径（均在文件顶部用变量配置）**：
- 设 ``OUTPUT_SKU_AND_BODY_IMAGES_ONLY=True``（默认）时，**仅搜集** ``skuId``、``detail_body_ingredients``、``detail_body_ingredients_source_url``（视觉命中时的图源）：不写 ware 原始 JSON；``OUT_PARSED`` / ``OUT_PARSED_DIR`` / ``OUT_PARSED_CSV`` 含上述列；批量时可不配 ``OUT_DIR``（配 ``OUT_PARSED_CSV`` 或 ``OUT_PARSED_DIR`` 即可）。
- 若 ``OUTPUT_SKU_AND_BODY_IMAGES_ONLY=False``：**原始接口 JSON** 单 SKU ``OUT``、批量 ``OUT_DIR`` / ``ware_{sku}.json``；解析扁平含全部 ``detail_*``；汇总表 ``OUT_PARSED_CSV``。

**解析 API**：``flatten_ware_business`` / ``parse_ware_business_response_text`` / ``ware_parsed_row``，
列 ``detail_body_ingredients`` 为配料表文本（由 ``#detail-main`` 长图经 ``AI_crawler`` 自后向前多模态识别）；列 ``detail_body_ingredients_source_url`` 为**实际用于识别**的那张长图 URL（命中即停）。未配置 API 或识别失败时配料列为原因说明、图源列为空。内部 ``meta["detail_body_image_urls"]`` 仍为全部长图 URL 串，仅供解析用。

Cookie：``../common/jd_cookie.txt``（或配置项 ``COOKIE_FILE`` / ``COOKIE_OVERRIDE``），经 ``add_cookies`` 注入。

依赖: pip install playwright && playwright install chromium（``USE_CHROME=True`` 时用本机 Chrome）

用法: 改下方「运行配置」后执行 ``python jd_detail_ware_business_requests.py``（无命令行参数）。

**模块划分**：响应 JSON 的扁平化与落盘格式化在 ``jd_detail_ware_parse.py``；
Playwright 打开商品页、拦截接口、``#detail-main`` 抽图在 ``jd_detail_ware_fetch.py``；
本文件保留运行配置、CLI ``main``、配料视觉桥接与解析结果写盘辅助，并对外 re-export 解析符号以兼容旧导入路径。
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Callable

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# 运行配置（按需改这里）
# ---------------------------------------------------------------------------
# 路径：副本通过 LOW_GI_PROJECT_ROOT 指向「Low GI」根目录（与搜索 / 流水线一致）
_JD_PC_SEARCH = Path(__file__).resolve().parents[1]
if str(_JD_PC_SEARCH) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH))
from _low_gi_root import low_gi_project_root  # noqa: E402

_PROJECT_ROOT = low_gi_project_root()
_PROJECT_DATA = _PROJECT_ROOT / "data" / "JD"

# SKU：单个商品 ID（item.jd.com/{SKU}.html）；与 SKU_FILE 二选一，不可都填或都空
SKU = "100307995056"
# SKU_FILE：每行一个 SKU，# 开头为注释；启用时须清空 SKU，并设置 OUT_DIR
SKU_FILE = ""
# OUT：单 SKU 时 **原始** 接口 JSON；OUTPUT_SKU_AND_BODY_IMAGES_ONLY=True 时不写入。空则 stdout 见下方
OUT = ""
# OUT_DIR：批量写 ware_{sku}.json；极简模式下可不配（配合 OUT_PARSED_CSV / OUT_PARSED_DIR）
OUT_DIR = ""
# OUT_PARSED：单 SKU 时 **解析扁平** 结果；写入 JSON（含 skuId、http_status 与 WARE_BUSINESS_MERGE_FIELDNAMES）；
# 空字符串表示不写解析文件（仅写 OUT 或仅打印）
OUT_PARSED = str(_PROJECT_DATA / "ware_out_parsed.json")
# OUT_PARSED_DIR：批量时目录，每个 SKU 写 ``ware_{sku}_parsed.json``；空表示批量也不落盘解析结果
# 常用：str(_PROJECT_DATA / "ware_parsed")
OUT_PARSED_DIR = ""
# OUT_PARSED_CSV：解析结果汇总表（UTF-8 BOM）；每次运行结束时写入，行数=本次请求的 SKU 数
# （单 SKU、批量均适用）；空表示不写。常用：str(_PROJECT_DATA / "ware_detail_summary.csv")
OUT_PARSED_CSV = str(_PROJECT_DATA / "ware_detail_summary.csv")
# COOKIE_FILE：Cookie 文本路径；空则使用 jd_pc_search/common/jd_cookie.txt
COOKIE_FILE = ""
# COOKIE_OVERRIDE：非空时覆盖文件中的整段 Cookie 请求头
COOKIE_OVERRIDE = ""
# TIMEOUT_SEC：打开商品页与等待拦截的总超时（秒）
TIMEOUT_SEC = 45.0
# GOTO_WAIT_UNTIL：``page.goto`` 的 wait_until。京东详情页常因长连接/埋点迟迟不触发 ``load``，默认用 ``domcontentloaded``；若需严格等全部资源可改 ``load``。
GOTO_WAIT_UNTIL = "domcontentloaded"
# FETCH_MAX_ATTEMPTS：未捕获接口、非 200、正文空或解析后无有效字段时最多重试次数
FETCH_MAX_ATTEMPTS = 3
# FETCH_RETRY_DELAY_SEC：相邻两次尝试之间的休眠（秒）
FETCH_RETRY_DELAY_SEC = 2.0
# CLICK_PRODUCT_DETAIL_TAB：True 时在进入商品页后点击「商品详情」tab（#SPXQ-tab-column），便于滚到详情区并触发与 tab 相关的请求
CLICK_PRODUCT_DETAIL_TAB = True
# LOG_API_M_JD_TRACE：True 时在 stderr 列出本次打开商品页过程中每条 api.m.jd.com 响应（阶段+functionId+URL），用于对照 DevTools 找 tab 对应接口
LOG_API_M_JD_TRACE = False
# COLLECT_DETAIL_MAIN_IMAGE_URLS：True 时在点击商品详情 tab 并等待后，从 #detail-main 抽取图文 URL（style 中 background-image、img src、zbViewWeChatMiniImages），
# 补全为 https 后写入 meta（见 DETAIL_BODY_IMAGE_URL_SEPARATOR）；再经多模态写入列 detail_body_ingredients（配料文本）与 detail_body_ingredients_source_url（命中图源）
COLLECT_DETAIL_MAIN_IMAGE_URLS = True
# EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES：True 时 detail_body_ingredients 为配料表文本、detail_body_ingredients_source_url 为命中图源；False 时配料列恒为空、图源列恒为空。需 .env 中 OPENAI_* / LLM_*（见上级目录 AI_crawler.py）
EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES = True
# DETAIL_BODY_IMAGE_URL_SEPARATOR：写入 CSV/JSON 单单元格时的分隔符
DETAIL_BODY_IMAGE_URL_SEPARATOR = "; "
# DETAIL_BODY_IMAGE_URLS_MAX_CHARS：单单元格最大字符数（兼顾 Excel 单元格上限）
DETAIL_BODY_IMAGE_URLS_MAX_CHARS = 31000
# OUTPUT_SKU_AND_BODY_IMAGES_ONLY：True 时落盘/stdout 为 skuId + detail_body_ingredients + detail_body_ingredients_source_url（不写 ware 原始 JSON；CSV 三列）；仍为抓 wareBusiness 打开页面，若已抽到图文 URL 则不再因接口扁平为空而重试
OUTPUT_SKU_AND_BODY_IMAGES_ONLY = True
# HEADED：True 显示浏览器窗口（调页面/登录态）
HEADED = False
# USE_CHROME：True 使用本机已安装的 Google Chrome（channel=chrome），否则用内置 Chromium
USE_CHROME = True
# DEBUG_PAUSE：True 时请求结束后终端按回车再关浏览器
DEBUG_PAUSE = False
# PRETTY_STDOUT：单 SKU 且无 OUT、且 NORMALIZE_WARE_JSON=False 时，是否在终端缩进打印（不排序键）
PRETTY_STDOUT = True
# VERBOSE_HTTP：True 在 stderr 打印本次拦截的 URL/状态/响应体摘要（Cookie 会截断）
VERBOSE_HTTP = False
# HTTP_LOG：非空路径则写入完整 request/response JSON（多 SKU 时为数组）；对照 Network 用
HTTP_LOG = ""  # 例：str(_PROJECT_DATA / "ware_http_log.json")
# VERBOSE_HTTP_BODY_LIMIT：与 VERBOSE_HTTP 合用时，stderr 中响应体最多字符数
VERBOSE_HTTP_BODY_LIMIT = 8000
# NORMALIZE_WARE_JSON：True 时对成功解析的 JSON 规整后写出（缩进 + 可选键排序），失败则仍写原文
NORMALIZE_WARE_JSON = True
# SORT_JSON_KEYS：True 时递归按字典键名排序，便于 diff 与浏览；False 保留接口原始字段顺序
SORT_JSON_KEYS = True
# JSON_INDENT：规整时的缩进空格数；0 表示紧凑单行（仍可能已排序）
JSON_INDENT = 2
# ---------------------------------------------------------------------------

_JD_DIR = Path(__file__).resolve().parent
_JD_PC_SEARCH_DIR = _JD_DIR.parent
if str(_JD_PC_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_JD_PC_SEARCH_DIR))
_DEFAULT_COOKIE_PATH = (_JD_DIR.parent / "common" / "jd_cookie.txt").resolve()

_JD_DETAIL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
_JD_DETAIL_CONTEXT_EXTRA_HEADERS: dict[str, str] = {
    "sec-ch-ua": (
        '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
    ),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ---------------------------------------------------------------------------
# 解析（JSON 扁平）与采集（Playwright 拦截）分模块，见 jd_detail_ware_parse / jd_detail_ware_fetch
# ---------------------------------------------------------------------------
from jd_detail_ware_fetch import (  # noqa: E402
    WareFetchRuntime,
    _print_http_verbose,
    fetch_ware_business as _fetch_ware_business_impl,
)
from jd_detail_ware_parse import (  # noqa: E402
    DETAIL_WARE_LEAN_CSV_FIELDNAMES,
    SKU_BODY_IMAGES_ONLY_FIELDNAMES,
    WARE_BUSINESS_MERGE_FIELDNAMES,
    WARE_PARSED_CSV_FIELDNAMES,
    detail_ware_lean_csv_row,
    flatten_ware_business,
    format_ware_response_for_save,
    format_ware_response_text,
    minimal_sku_body_images_row,
    parse_ware_business_response_text,
    ware_parsed_row,
)

_WARE_FETCH_RUNTIME = WareFetchRuntime(
    log_api_m_jd_trace=bool(LOG_API_M_JD_TRACE),
    goto_wait_until=str(GOTO_WAIT_UNTIL or "domcontentloaded").strip(),
    click_product_detail_tab=bool(CLICK_PRODUCT_DETAIL_TAB),
    collect_detail_main_image_urls=bool(COLLECT_DETAIL_MAIN_IMAGE_URLS),
    detail_body_image_url_separator=str(DETAIL_BODY_IMAGE_URL_SEPARATOR or "; "),
    detail_body_image_urls_max_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
)


def fetch_ware_business(
    context: Any,
    page: Any,
    sku_id: str,
    *,
    cookie_file: str | None = None,
    timeout_ms: int = 30_000,
    cookie_override: str = "",
    max_attempts: int = 1,
    retry_delay_sec: float = 2.0,
    cancel_check: Callable[[], bool] | None = None,
) -> tuple[int, str, dict[str, Any]]:
    '''打开商品页并拦截 pc_detailpage_wareBusiness（与流水线兼容的薄封装）。'''
    return _fetch_ware_business_impl(
        context,
        page,
        sku_id,
        cookie_file=cookie_file,
        timeout_ms=timeout_ms,
        cookie_override=cookie_override,
        max_attempts=max_attempts,
        retry_delay_sec=retry_delay_sec,
        cancel_check=cancel_check,
        output_sku_and_body_images_only=bool(OUTPUT_SKU_AND_BODY_IMAGES_ONLY),
        runtime=_WARE_FETCH_RUNTIME,
    )


def _detail_body_ingredients_column_value(
    urls_joined: str,
    *,
    vision_mod: Any,
    vision_ok: bool,
) -> tuple[str, str]:
    """
    返回 ``(detail_body_ingredients, detail_body_ingredients_source_url)``。
    配料列为文本；失败时为 ``【未识别到配料】…``。图源列仅在视觉成功命中配料时非空。
    """
    if not EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES:
        return "", ""
    if not vision_ok or vision_mod is None:
        return (
            "【未识别到配料】未配置或无效的多模态 API（见 market_assistant/.env 中 OPENAI_* / LLM_*）。",
            "",
        )
    raw = (urls_joined or "").strip()
    try:
        fn = getattr(
            vision_mod,
            "extract_ingredients_from_body_image_urls_reversed_with_source",
            None,
        )
        if callable(fn):
            text, src_url = fn(raw)
            return str(text or "").strip(), (str(src_url).strip() if src_url else "")
        text = str(
            vision_mod.extract_ingredients_from_body_image_urls_reversed(raw)
        ).strip()
        return text, ""
    except Exception as e:
        print(f"[京东] 配料视觉提取异常: {e}", file=sys.stderr)
        return f"【未识别到配料】识别异常：{e}"[:800], ""


def _write_minimal_body_images_json(
    path: Path,
    sku: str,
    detail_body_ingredients: str,
    *,
    detail_body_ingredients_source_url: str = "",
) -> None:
    row = minimal_sku_body_images_row(
        sku,
        detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
        max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[京东] 已写图文 URL JSON：{path}", file=sys.stderr)


def _write_ware_parsed_json(
    path: Path,
    sku: str,
    http_status: int,
    response_text: str,
    *,
    detail_body_ingredients: str = "",
    detail_body_ingredients_source_url: str = "",
) -> None:
    row = ware_parsed_row(
        sku,
        http_status,
        response_text,
        detail_body_ingredients=detail_body_ingredients,
        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
        max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[京东] 已写解析 JSON：{path}", file=sys.stderr)


def main() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    sku = (SKU or "").strip()
    sku_file = (SKU_FILE or "").strip()
    out_path = (OUT or "").strip()
    out_dir = (OUT_DIR or "").strip()
    if bool(sku) == bool(sku_file):
        print("请在文件顶部配置 SKU 或 SKU_FILE（二选一）", file=sys.stderr)
        sys.exit(2)
    if sku_file and out_path:
        print("使用 SKU_FILE 时不要配置 OUT，请用 OUT_DIR", file=sys.stderr)
        sys.exit(2)

    out_parsed = (OUT_PARSED or "").strip()
    out_parsed_dir = (OUT_PARSED_DIR or "").strip()
    out_parsed_csv = (OUT_PARSED_CSV or "").strip()
    output_minimal = bool(OUTPUT_SKU_AND_BODY_IMAGES_ONLY)
    if sku_file and not out_dir:
        if not (
            output_minimal
            and (out_parsed_csv or out_parsed_dir)
        ):
            print("使用 SKU_FILE 时请配置 OUT_DIR", file=sys.stderr)
            sys.exit(2)
    if sku_file and out_parsed and not out_parsed_dir:
        print(
            "[京东] 批量模式请配置 OUT_PARSED_DIR（每 SKU 写 ware_{sku}_parsed.json）；"
            "已忽略 OUT_PARSED",
            file=sys.stderr,
        )
        out_parsed = ""

    cookie_file_cli = (COOKIE_FILE or "").strip()
    if cookie_file_cli:
        cookie_file_cli = str(Path(cookie_file_cli).resolve())
    cf = cookie_file_cli or None
    cookie_override = (COOKIE_OVERRIDE or "").strip()
    timeout_ms = max(1000, int(float(TIMEOUT_SEC) * 1000))

    skus = [sku] if sku else []
    if sku_file:
        raw = Path(sku_file).read_text(encoding="utf-8")
        for line in raw.splitlines():
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            skus.append(t)
    if not skus:
        print("无有效 SKU", file=sys.stderr)
        sys.exit(2)

    http_log_path = (HTTP_LOG or "").strip()
    http_records: list[dict[str, Any]] = []
    pretty = bool(PRETTY_STDOUT)
    verbose_http = bool(VERBOSE_HTTP)
    verbose_body_lim = int(VERBOSE_HTTP_BODY_LIMIT)
    normalize_json = bool(NORMALIZE_WARE_JSON)
    sort_keys = bool(SORT_JSON_KEYS)
    json_indent = max(0, int(JSON_INDENT))
    parsed_csv_rows: list[dict[str, str]] = []

    vision_mod: Any = None
    vision_ok = False
    if EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES:
        try:
            import AI_crawler as vision_mod  # noqa: WPS433

            vision_mod._resolve_credentials(None, None, None)
            vision_ok = True
        except Exception as e:
            print(
                f"[京东] 已开启配料视觉提取但未就绪（{e}），"
                f"列 detail_body_ingredients / detail_body_ingredients_source_url 将按未就绪处理",
                file=sys.stderr,
            )

    def emit(
        code: int,
        text: str,
        s: str,
        detail_body_ingredients: str = "",
        *,
        detail_body_ingredients_source_url: str = "",
    ) -> None:
        if code == 403:
            print(
                f"[京东] SKU {s}：接口 403，请更新 Cookie 或设 USE_CHROME=True。",
                file=sys.stderr,
            )
            frag = (text or "").strip().replace("\n", " ")[:400]
            if frag:
                print(f"[京东] 响应片段：{frag}", file=sys.stderr)
        if code != 200:
            print(f"[京东] SKU {s} HTTP {code}", file=sys.stderr)

        if output_minimal:
            row_m = minimal_sku_body_images_row(
                s,
                detail_body_ingredients,
                detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
            )
            if out_dir:
                if out_parsed_dir:
                    pd = Path(out_parsed_dir).resolve() / f"ware_{s}_parsed.json"
                    _write_minimal_body_images_json(
                        pd,
                        s,
                        detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if out_path:
                if out_parsed:
                    _write_minimal_body_images_json(
                        Path(out_parsed).resolve(),
                        s,
                        detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    )
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if len(skus) != 1:
                if out_parsed_csv:
                    parsed_csv_rows.append(dict(row_m))
                return
            if out_parsed:
                _write_minimal_body_images_json(
                    Path(out_parsed).resolve(),
                    s,
                    detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            sys.stdout.write(
                json.dumps(row_m, ensure_ascii=False, indent=2) + "\n"
            )
            if out_parsed_csv:
                parsed_csv_rows.append(dict(row_m))
            return

        if out_dir:
            out_p = Path(out_dir).resolve() / f"ware_{s}.json"
            out_p.parent.mkdir(parents=True, exist_ok=True)
            body, _ok = format_ware_response_text(
                text,
                normalize=normalize_json,
                sort_keys=sort_keys,
                indent=json_indent,
            )
            out_p.write_text(body, encoding="utf-8")
            print(f"[京东] 已写 {out_p}", file=sys.stderr)
            if out_parsed_dir:
                pd = Path(out_parsed_dir).resolve() / f"ware_{s}_parsed.json"
                _write_ware_parsed_json(
                    pd,
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                        max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
                    )
                )
            return
        if out_path:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            if normalize_json:
                body, _ok = format_ware_response_text(
                    text,
                    normalize=True,
                    sort_keys=sort_keys,
                    indent=json_indent,
                )
                Path(out_path).write_text(body, encoding="utf-8")
            elif pretty:
                try:
                    obj = json.loads(text)
                    Path(out_path).write_text(
                        json.dumps(obj, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                except json.JSONDecodeError:
                    Path(out_path).write_text(text, encoding="utf-8")
            else:
                Path(out_path).write_text(text, encoding="utf-8")
            print(f"[京东] 已写 {out_path}", file=sys.stderr)
            if out_parsed:
                _write_ware_parsed_json(
                    Path(out_parsed).resolve(),
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                )
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                        max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
                    )
                )
            return
        if len(skus) != 1:
            sys.stdout.write(text + ("\n" if text and not text.endswith("\n") else ""))
            if out_parsed_csv:
                parsed_csv_rows.append(
                    ware_parsed_row(
                        s,
                        code,
                        text,
                        detail_body_ingredients=detail_body_ingredients,
                        detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                        max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
                    )
                )
            return
        if out_parsed:
            _write_ware_parsed_json(
                Path(out_parsed).resolve(),
                s,
                code,
                text,
                detail_body_ingredients=detail_body_ingredients,
                detail_body_ingredients_source_url=detail_body_ingredients_source_url,
            )
        if normalize_json:
            body, ok = format_ware_response_text(
                text,
                normalize=True,
                sort_keys=sort_keys,
                indent=json_indent,
            )
            sys.stdout.write(
                body
                if ok
                else text + ("\n" if text and not text.endswith("\n") else "")
            )
        elif pretty:
            try:
                obj = json.loads(text)
                sys.stdout.write(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
            except json.JSONDecodeError:
                sys.stdout.write(text + ("\n" if not text.endswith("\n") else ""))
        else:
            sys.stdout.write(text + ("\n" if text and not text.endswith("\n") else ""))
        if out_parsed_csv:
            parsed_csv_rows.append(
                ware_parsed_row(
                    s,
                    code,
                    text,
                    detail_body_ingredients=detail_body_ingredients,
                    detail_body_ingredients_source_url=detail_body_ingredients_source_url,
                    max_cell_chars=int(DETAIL_BODY_IMAGE_URLS_MAX_CHARS),
                )
            )

    max_att = max(1, int(FETCH_MAX_ATTEMPTS))
    retry_sec = max(0.0, float(FETCH_RETRY_DELAY_SEC))

    def run_one(page: Any, s: str) -> None:
        code, text, meta = fetch_ware_business(
            page.context,
            page,
            s,
            cookie_file=cf,
            timeout_ms=timeout_ms,
            cookie_override=cookie_override,
            max_attempts=max_att,
            retry_delay_sec=retry_sec,
        )
        if verbose_http:
            _print_http_verbose(meta, body_max=max(500, verbose_body_lim))
        if http_log_path:
            http_records.append(meta)
        print(f"[京东] sku={s} HTTP {code}", file=sys.stderr)
        if code == 0 and meta.get("request", {}).get("note"):
            print(f"[京东] {meta['request']['note']}", file=sys.stderr)
        body_urls_meta = str(meta.get("detail_body_image_urls") or "").strip()
        body_col, body_src = _detail_body_ingredients_column_value(
            body_urls_meta,
            vision_mod=vision_mod,
            vision_ok=vision_ok,
        )
        if EXTRACT_INGREDIENTS_FROM_DETAIL_BODY_IMAGES and body_col:
            if str(body_col).startswith("【未识别"):
                print(f"[京东] sku={s} {body_col}", file=sys.stderr)
            elif body_src:
                print(
                    f"[京东] sku={s} 已自详情长图解析配料表（首次命中即停）图源: {body_src}",
                    file=sys.stderr,
                )
            else:
                print(f"[京东] sku={s} 已自详情长图解析配料表（首次命中即停）", file=sys.stderr)
        emit(
            code,
            text,
            s,
            body_col,
            detail_body_ingredients_source_url=body_src,
        )

    headed = bool(HEADED or DEBUG_PAUSE)
    use_chrome = bool(USE_CHROME or DEBUG_PAUSE)
    launch_kw: dict[str, Any] = {"headless": not headed}
    if use_chrome:
        launch_kw["channel"] = "chrome"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(**launch_kw)
        context = browser.new_context(
            user_agent=_JD_DETAIL_UA,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            extra_http_headers=dict(_JD_DETAIL_CONTEXT_EXTRA_HEADERS),
        )
        page = context.new_page()
        if output_minimal:
            print(
                "[京东] 极简输出：skuId + detail_body_ingredients"
                "（仍打开页抓接口以加载 DOM）",
                file=sys.stderr,
            )
        else:
            print(
                "[京东] 加载商品页并拦截 pc_detailpage_wareBusiness",
                file=sys.stderr,
            )
        try:
            for s in skus:
                run_one(page, s)
        finally:
            if DEBUG_PAUSE:
                print(
                    "[京东] 调试：浏览器仍将保持打开；在此终端按回车后再关闭…",
                    file=sys.stderr,
                )
                try:
                    input()
                except EOFError:
                    pass
            try:
                page.close()
            except Exception:
                pass
            browser.close()

    if parsed_csv_rows and out_parsed_csv:
        csv_fields = list(
            SKU_BODY_IMAGES_ONLY_FIELDNAMES
            if output_minimal
            else WARE_PARSED_CSV_FIELDNAMES
        )
        cpp = Path(out_parsed_csv).resolve()
        cpp.parent.mkdir(parents=True, exist_ok=True)
        with cpp.open("w", encoding="utf-8-sig", newline="") as cf:
            w = csv.DictWriter(cf, fieldnames=csv_fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(parsed_csv_rows)
        print(
            f"[京东] 已写解析 CSV：{cpp}（{len(parsed_csv_rows)} 行）",
            file=sys.stderr,
        )

    if http_log_path:
        log_p = Path(http_log_path).resolve()
        log_p.parent.mkdir(parents=True, exist_ok=True)
        payload: Any = http_records[0] if len(http_records) == 1 else http_records
        log_p.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"[京东] 已写 HTTP 往返记录：{log_p}", file=sys.stderr)


if __name__ == "__main__":
    main()


