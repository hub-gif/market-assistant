"""
本品 / 产品手册：策略生成时并入 ``rules_draft_markdown`` 的 **§1.3「业务侧本品依据」** 块（随任务关键词从 PDF 摘录或由前端粘贴）。

- 优先使用接口 POST 的 ``our_product_profile``（前端粘贴或摘要）。
- 若为空且设置 ``MA_STRATEGY_PRODUCT_MANUAL_PDF``，则按 ``strategy_keyword``（与任务关键词一致）
  从 PDF 正文摘录相关段落；摘录总长默认不限制（可用 ``MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS`` 限制）。
  PDF 抽取总长默认一百万字符量级（见 ``MA_STRATEGY_PRODUCT_MANUAL_PDF_MAX_RAW``；``0`` / ``none`` / ``full`` 表示不截断，注意内存）。
  若出现「品名主标题单独一行 + 功效/成分多行」的版式，优先从该标题起摘录到下一「短标题级」SKU（例如另一行的「透亮水光精华液」）为止，以覆盖单页完整产品块。
  路径为相对 ``LOW_GI_PROJECT_ROOT`` 或绝对路径；需安装 ``pypdf``。
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from django.conf import settings

logger = logging.getLogger(__name__)


def _env_positive_or_none(key: str, *, default: int | None) -> int | None:
    """读环境变量：空串用 default；0 / none / full / unlimited 表示不限制；否则为正整数。"""
    raw = (os.environ.get(key) or "").strip()
    if not raw:
        return default
    low = raw.lower()
    if low in ("0", "none", "full", "unlimited"):
        return None
    try:
        n = int(raw, 10)
    except ValueError:
        return default
    return None if n <= 0 else n


def pdf_extract_max_raw_chars() -> int | None:
    """从 PDF 合并出的最大字符数；``None`` 表示不截断（注意内存）。"""
    return _env_positive_or_none(
        "MA_STRATEGY_PRODUCT_MANUAL_PDF_MAX_RAW",
        default=10_000_000,
    )


def excerpt_cap_chars() -> int | None:
    """按关键词并入策略前，摘录结果最大字符数；``None`` 表示不截断。"""
    return _env_positive_or_none(
        "MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS",
        default=None,
    )


def _project_root() -> Path:
    raw = (getattr(settings, "LOW_GI_PROJECT_ROOT", None) or "").strip()
    return Path(raw).expanduser().resolve() if raw else Path(settings.BASE_DIR).resolve().parent


def _collapse_ws(s: str) -> str:
    return "".join(str(s).split())


def _paragraphs_for_matching(body: str) -> list[str]:
    """按空行分段；单段过长（常见 PDF 抽成整页一坨）时再按单行切分，便于只命中含关键词的几行。"""
    chunks = [p.strip() for p in re.split(r"\n\s*\n+", body) if p.strip()]
    paras: list[str] = []
    for c in chunks:
        if len(c) <= 5000:
            paras.append(c)
        else:
            paras.extend(line.strip() for line in c.split("\n") if line.strip())
    return paras


# 手册中「另一 SKU」的常见短标题行（勿用泛泛的 xxx抗衰精华液，避免误截断副标题）
_OTHER_PRODUCT_SHORT_TITLE = re.compile(
    r"^[\s\u200b]*[\u4e00-\u9fff·]{1,14}水光精华液\s*$|"
    r"^[\s\u200b]*[\u4e00-\u9fff·]{1,16}(?:精华乳|面膜|洁面慕斯|洁颜油|洁面|凝萃水)\s*$"
)


def _line_ok_as_product_block_anchor(line: str, keyword: str) -> bool:
    """含关键词的一行是否像「单品主标题」而非目录长行，亦非「品名：说明」混排。"""
    s = (line or "").strip()
    if keyword not in s and _collapse_ws(keyword) not in _collapse_ws(s):
        return False
    if len(s) > 72:
        return False
    if "：" in s or ":" in s:
        return False
    if len(s) <= len(keyword) + 1:
        return True
    return bool(
        len(s) <= 44
        and (
            s.rstrip("。").endswith("水光精华液")
            or s.rstrip("。").endswith("精华乳")
            or s.rstrip("。").endswith("面膜")
        )
    )


def _line_starts_other_product_block(line: str, keyword: str) -> bool:
    if not (line or "").strip():
        return False
    s = line.strip()
    if keyword in s or _collapse_ws(keyword) in _collapse_ws(s):
        return False
    return bool(_OTHER_PRODUCT_SHORT_TITLE.match(line))


def _excerpt_product_block_by_lines(body: str, keyword: str) -> str:
    """
    从「主标题行含关键词」起逐行向下合并，直到遇到另一条「短标题级」SKU 行（同手册版式）。
    用于 PDF 抽成「品名单独一行、下文多行」时的完整产品块（如 132–148 行）。
    """
    kw = (keyword or "").strip()
    if not kw or not (body or "").strip():
        return ""
    kw_flat = _collapse_ws(kw)
    loose = re.compile(r"\s*".join(re.escape(c) for c in kw))
    lines = body.splitlines()
    start: int | None = None
    for i, line in enumerate(lines):
        if kw in line or kw_flat in _collapse_ws(line):
            if _line_ok_as_product_block_anchor(line, kw):
                start = i
                break
            continue
        if loose.search(line) and _line_ok_as_product_block_anchor(line, kw):
            start = i
            break
    if start is None:
        return ""
    out_lines: list[str] = []
    max_out = 120
    max_chars_soft = 50_000
    buf_len = 0
    for j in range(start, len(lines)):
        if j > start and _line_starts_other_product_block(lines[j], kw):
            break
        out_lines.append(lines[j])
        buf_len += len(lines[j]) + 1
        if len(out_lines) >= max_out or buf_len >= max_chars_soft:
            break
    return "\n".join(out_lines).strip()


def excerpt_text_by_keyword(
    text: str,
    keyword: str,
    *,
    max_chars: int | None = None,
) -> str:
    """
    从手册长文中取出与 ``keyword`` 相关的片段：优先收集含关键词的段落；否则在全文上取关键词窗口。
    ``max_chars`` 为 ``None`` 时不做长度截断；窗口模式在不限长时延伸至文末（多产品场景可能含后续品名，可改用可搜索 PDF 或更精确的段落）。
    若出现「品名单独成行 + 下文多行」的版式，则优先整段摘录到下一短标题 SKU 行为止。
    """
    body = (text or "").strip()
    kw = (keyword or "").strip()
    if not body or not kw:
        return ""
    kw_flat = _collapse_ws(kw)
    if not kw_flat:
        return ""

    block = _excerpt_product_block_by_lines(body, kw)
    if block:
        if max_chars is None or len(block) <= max_chars:
            return block
        cut = max_chars - 100
        return (
            block[:cut].rstrip()
            + "\n\n…（与关键词匹配的段落较多，此处已截断；可调大 MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS 或设为 0 不限制。）\n"
        )

    paras = _paragraphs_for_matching(body)
    hits: list[str] = []
    for p in paras:
        if kw in p or kw_flat in _collapse_ws(p):
            hits.append(p)
    if hits:
        out = "\n\n".join(hits)
        if max_chars is None or len(out) <= max_chars:
            return out
        cut = max_chars - 100
        return (
            out[:cut].rstrip()
            + "\n\n…（与关键词匹配的段落较多，此处已截断；可调大 MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS 或设为 0 不限制。）\n"
        )
    idx = body.find(kw)
    if idx < 0:
        # 正文可能含空格 / 换行插字，尝试宽松匹配后取物理跨度
        pat = r"\s*".join(re.escape(c) for c in kw)
        m = re.search(pat, body)
        if m:
            idx = m.start()
            endm = m.end()
            radius_before = 1_500
            end = len(body) if max_chars is None else min(len(body), endm + 12_000)
            start = max(0, idx - radius_before)
            chunk = body[start:end].strip()
            if max_chars is not None and len(chunk) > max_chars:
                cut = max_chars - 80
                chunk = chunk[:cut].rstrip() + "\n\n…（已从关键词位置截断。）\n"
            return chunk
        logger.info("产品手册中未命中关键词，未自动摘录：%s", kw[:80])
        return ""
    # PDF 常缺少空行分段，退化为关键词邻居窗口
    radius_before = 1_500
    end = len(body) if max_chars is None else min(len(body), idx + len(kw) + 12_000)
    start = max(0, idx - radius_before)
    chunk = body[start:end].strip()
    if max_chars is not None and len(chunk) > max_chars:
        cut = max_chars - 80
        chunk = chunk[:cut].rstrip() + "\n\n…（已从关键词位置截断。）\n"
    return chunk


def extract_pdf_plain_text(path: Path, *, max_chars: int | None = 48_000) -> str:
    """从 PDF 抽取合并纯文本；``max_chars`` 为 ``None`` 时不截断（注意内存）。"""
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("已配置 MA_STRATEGY_PRODUCT_MANUAL_PDF 但未安装 pypdf，跳过 PDF 抽取")
        return ""
    try:
        reader = PdfReader(str(path))
        parts: list[str] = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
        text = "\n".join(parts).strip()
        if not text:
            return ""
        if max_chars is not None and len(text) > max_chars:
            cut = max_chars - 120
            text = text[:cut].rstrip() + "\n\n…（PDF 全文较长，已截断；可调大 MA_STRATEGY_PRODUCT_MANUAL_PDF_MAX_RAW 或设为 0。）\n"
        return text
    except Exception as e:
        logger.warning("读取产品手册 PDF 失败 %s: %s", path, e)
        return ""


_USE_ENV_EXCERPT_CAP = object()


def load_product_manual_text_from_env(
    *,
    strategy_keyword: str,
    excerpt_max_chars: int | None | object = _USE_ENV_EXCERPT_CAP,
) -> str:
    """
    读取 ``MA_STRATEGY_PRODUCT_MANUAL_PDF``，按 ``strategy_keyword`` 摘录后记入策略；未配置 / 失败 / 无关键词 / 未命中 返回空。
    摘录长度默认读 ``MA_STRATEGY_PRODUCT_MANUAL_EXCERPT_MAX_CHARS``（不设则不加总长限制）；传入 ``excerpt_max_chars`` 可覆盖（单测或脚本）。
    """
    kw = (strategy_keyword or "").strip()
    if not kw:
        logger.info("未提供策略关键词，跳过从 PDF 自动摘录（避免注入多产品全文）")
        return ""
    raw = (os.environ.get("MA_STRATEGY_PRODUCT_MANUAL_PDF") or "").strip()
    if not raw:
        return ""
    p = Path(raw)
    if not p.is_absolute():
        p = _project_root() / raw
    p = p.expanduser().resolve()
    if not p.is_file():
        logger.warning("MA_STRATEGY_PRODUCT_MANUAL_PDF 路径不存在: %s", p)
        return ""
    if p.suffix.lower() != ".pdf":
        logger.warning("MA_STRATEGY_PRODUCT_MANUAL_PDF 暂仅支持 .pdf: %s", p)
        return ""
    cap_raw = pdf_extract_max_raw_chars()
    full = extract_pdf_plain_text(p, max_chars=cap_raw)
    if not full.strip():
        return ""
    cap_ex = excerpt_cap_chars() if excerpt_max_chars is _USE_ENV_EXCERPT_CAP else excerpt_max_chars
    return excerpt_text_by_keyword(full, kw, max_chars=cap_ex)


def merged_our_product_profile_for_strategy(*, user_text: str, strategy_keyword: str = "") -> str:
    """
    返回最终并入策略底稿 §1.3「业务侧本品依据」的正文。
    ``user_text`` 非空时仅用用户内容；否则按 ``strategy_keyword`` 从环境变量 PDF 摘录。
    """
    u = (user_text or "").strip()
    if u:
        return u
    return load_product_manual_text_from_env(strategy_keyword=strategy_keyword)


__all__ = [
    "excerpt_cap_chars",
    "excerpt_text_by_keyword",
    "extract_pdf_plain_text",
    "load_product_manual_text_from_env",
    "merged_our_product_profile_for_strategy",
    "pdf_extract_max_raw_chars",
]
