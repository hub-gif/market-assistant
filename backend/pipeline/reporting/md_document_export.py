"""Markdown → Word（.docx）/ 简易 PDF；供任务报告与策略稿导出。"""
from __future__ import annotations

import os
import re
from io import BytesIO
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape


def _strip_inline_md(s: str) -> str:
    s = re.sub(r"\*\*(.+?)\*\*", r"\1", s)
    s = re.sub(r"`([^`]+)`", r"\1", s)
    return s


def _is_table_sep(line: str) -> bool:
    t = line.strip()
    if not t.startswith("|"):
        return False
    inner = t.strip("|").replace(" ", "")
    return bool(inner) and all(p in ("", "---", ":---", "---:", ":---:") for p in t.split("|"))


_RE_HEADING = re.compile(r"^(#{1,6})\s+(.+)$")
_RE_UL = re.compile(r"^\s*[-*+]\s+(.+)$")
_RE_OL = re.compile(r"^\s*(\d+)\.\s+(.+)$")
_RE_BLOCKQUOTE = re.compile(r"^\s*>\s?(.*)$")
_RE_HR = re.compile(r"^\s*(?:[-*_]\s*){3,}\s*$")

_img_line = re.compile(r"^!\[([^\]]*)\]\(([^)]+)\)\s*$")


def _match_heading(line: str) -> tuple[int, str] | None:
    """返回 (docx level 0–8, 标题文本) 或 None。"""
    m = _RE_HEADING.match(line.strip())
    if not m:
        return None
    depth = len(m.group(1))
    title = _strip_inline_md(m.group(2).strip())
    level = min(max(depth - 1, 0), 8)
    return (level, title)


def markdown_to_docx_bytes(md: str, *, asset_root: Path | None = None) -> bytes:
    from docx import Document
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    from docx.shared import Inches, Pt

    doc = Document()
    try:
        style = doc.styles["Normal"]
        style.font.name = "Microsoft YaHei"
        style.font.size = Pt(10.5)
    except Exception:
        pass

    def _add_list_bullet(text: str) -> None:
        t = _strip_inline_md(text)
        try:
            doc.add_paragraph(t, style="List Bullet")
        except KeyError:
            doc.add_paragraph("• " + t)

    def _add_list_number(text: str) -> None:
        t = _strip_inline_md(text)
        try:
            doc.add_paragraph(t, style="List Number")
        except KeyError:
            doc.add_paragraph(t)

    lines = (md or "").replace("\r\n", "\n").split("\n")
    i = 0
    in_fence = False
    while i < len(lines):
        raw = lines[i]
        if raw.strip().startswith("```"):
            in_fence = not in_fence
            i += 1
            continue
        if in_fence:
            p = doc.add_paragraph(xml_escape(raw) or " ")
            p.style = doc.styles["Normal"]
            for run in p.runs:
                run.font.name = "Consolas"
                run.font.size = Pt(9)
            i += 1
            continue

        line = raw.rstrip()
        if not line.strip():
            doc.add_paragraph("")
            i += 1
            continue

        if _RE_HR.match(line):
            doc.add_paragraph("")
            i += 1
            continue

        hm = _match_heading(line)
        if hm is not None:
            doc.add_heading(hm[1], level=hm[0])
            i += 1
            continue

        mimg = _img_line.match(line.strip())
        if mimg and asset_root is not None:
            rel = mimg.group(2).strip()
            if not (rel.startswith("http://") or rel.startswith("https://")):
                img_path = (asset_root / rel).resolve()
                try:
                    img_path.relative_to(asset_root.resolve())
                except ValueError:
                    i += 1
                    continue
                if img_path.is_file():
                    doc.add_picture(str(img_path), width=Inches(5.9))
            i += 1
            continue

        if line.strip().startswith("|"):
            rows: list[list[str]] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                row_line = lines[i].strip()
                if _is_table_sep(row_line):
                    i += 1
                    continue
                cells = [c.strip() for c in row_line.strip("|").split("|")]
                rows.append([_strip_inline_md(c) for c in cells])
                i += 1
            if rows:
                max_cols = max(len(r) for r in rows)
                pad_rows = [r + [""] * (max_cols - len(r)) for r in rows]
                tbl = doc.add_table(rows=len(pad_rows), cols=max_cols)
                tbl.style = "Table Grid"
                for ri, row in enumerate(pad_rows):
                    for ci, cell in enumerate(row):
                        tbl.rows[ri].cells[ci].text = cell
            continue

        mu = _RE_UL.match(line)
        if mu:
            _add_list_bullet(mu.group(1))
            i += 1
            continue

        mo = _RE_OL.match(line)
        if mo:
            _add_list_number(mo.group(2))
            i += 1
            continue

        mq = _RE_BLOCKQUOTE.match(line)
        if mq:
            inner = mq.group(1).strip()
            if inner:
                p = doc.add_paragraph()
                p.paragraph_format.left_indent = Inches(0.25)
                p.add_run(_strip_inline_md(inner))
            i += 1
            continue

        p = doc.add_paragraph()
        p.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
        text = _strip_inline_md(line)
        p.add_run(text)
        i += 1

    bio = BytesIO()
    doc.save(bio)
    return bio.getvalue()


def _pdf_font_candidates() -> list[Path]:
    raw = (os.environ.get("MA_PDF_FONT") or "").strip()
    out: list[Path] = []
    if raw:
        out.append(Path(raw))
    windir = os.environ.get("WINDIR", r"C:\Windows")
    out.extend(
        [
            Path(windir) / "Fonts" / "simhei.ttf",
            Path(windir) / "Fonts" / "simsun.ttc",
            Path(windir) / "Fonts" / "msyh.ttf",
        ]
    )
    # Linux / 容器常见中文字体（路径不存在则跳过）
    out.extend(
        [
            Path("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"),
            Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
            Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
            Path("/usr/share/fonts/truetype/noto/NotoSansCJKsc-Regular.otf"),
            Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        ]
    )
    return out


def _pdf_flowable_image(img_path: Path, *, max_w: float, max_h: float) -> Any:
    """将插图缩放到不超过 max_w×max_h（ReportLab 单位，与 cm 一致），保持宽高比，避免矩阵长图撑爆版面。"""
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import Image as RLImage

    p = str(img_path)
    try:
        ir = ImageReader(p)
        iw, ih = ir.getSize()
    except Exception:
        return RLImage(p, width=max_w * 0.9, height=max_h * 0.9)
    if iw <= 0 or ih <= 0:
        return RLImage(p, width=max_w * 0.9, height=max_h * 0.9)
    w = float(max_w)
    h = w * (float(ih) / float(iw))
    if h > float(max_h):
        h = float(max_h)
        w = h * (float(iw) / float(ih))
    return RLImage(p, width=w, height=h)


def markdown_to_pdf_bytes(md: str, *, asset_root: Path | None = None) -> bytes:
    """简易 PDF；需本机 .ttf 中文字体或环境变量 MA_PDF_FONT。"""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    font_name = "MaExportCJK"
    registered = False
    for p in _pdf_font_candidates():
        if not p.is_file():
            continue
        try:
            if p.suffix.lower() == ".ttc":
                try:
                    pdfmetrics.registerFont(
                        TTFont(font_name, str(p), subfontIndex=0)
                    )
                except TypeError:
                    pdfmetrics.registerFont(TTFont(font_name, str(p)))
            else:
                pdfmetrics.registerFont(TTFont(font_name, str(p)))
            registered = True
            break
        except Exception:
            continue
    if not registered:
        raise ValueError(
            "未找到可用的中文字体文件。请在 Windows 上安装黑体/宋体，"
            "或设置环境变量 MA_PDF_FONT 指向 .ttf 文件路径。"
        )

    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        name="BodyCJK",
        parent=styles["Normal"],
        fontName=font_name,
        fontSize=10,
        leading=14,
    )
    h1s = ParagraphStyle(
        name="H1CJK",
        parent=body,
        fontSize=16,
        leading=20,
        spaceAfter=8,
    )
    h2s = ParagraphStyle(
        name="H2CJK",
        parent=body,
        fontSize=13,
        leading=17,
        spaceAfter=6,
    )
    h3s = ParagraphStyle(
        name="H3CJK",
        parent=body,
        fontSize=12,
        leading=16,
        spaceAfter=5,
    )
    h4s = ParagraphStyle(
        name="H4CJK",
        parent=body,
        fontSize=11,
        leading=15,
        spaceAfter=4,
    )
    h56s = ParagraphStyle(
        name="H56CJK",
        parent=body,
        fontSize=10.5,
        leading=14,
        spaceAfter=3,
    )
    quote_style = ParagraphStyle(
        name="QuoteCJK",
        parent=body,
        leftIndent=14,
        fontSize=9.5,
        textColor=colors.HexColor("#444444"),
    )
    bullet_body = ParagraphStyle(
        name="BulletBodyCJK",
        parent=body,
        leftIndent=18,
        bulletIndent=8,
        firstLineIndent=0,
    )

    story: list[Any] = []
    lines = (md or "").replace("\r\n", "\n").split("\n")
    i = 0
    in_fence = False

    def _para_cell(s: str, style: Any) -> Paragraph:
        return Paragraph(xml_escape(_strip_inline_md(s)), style)

    while i < len(lines):
        raw = lines[i]
        if raw.strip().startswith("```"):
            in_fence = not in_fence
            i += 1
            continue
        s = raw.rstrip()
        if in_fence:
            story.append(Paragraph(xml_escape(s or " "), body))
            story.append(Spacer(1, 0.1 * cm))
            i += 1
            continue
        if not s.strip():
            story.append(Spacer(1, 0.15 * cm))
            i += 1
            continue

        if _RE_HR.match(s):
            story.append(Spacer(1, 0.2 * cm))
            i += 1
            continue

        hm = _match_heading(s)
        if hm is not None:
            level, title = hm
            title_esc = xml_escape(title)
            if level == 0:
                story.append(Paragraph(title_esc, h1s))
            elif level == 1:
                story.append(Paragraph(title_esc, h2s))
            elif level == 2:
                story.append(Paragraph(title_esc, h3s))
            elif level == 3:
                story.append(Paragraph(title_esc, h4s))
            else:
                story.append(Paragraph(title_esc, h56s))
            i += 1
            continue

        mimg = _img_line.match(s.strip())
        if mimg and asset_root is not None:
            rel = mimg.group(2).strip()
            if not (rel.startswith("http://") or rel.startswith("https://")):
                img_path = (asset_root / rel).resolve()
                try:
                    img_path.relative_to(asset_root.resolve())
                except ValueError:
                    i += 1
                    continue
                if img_path.is_file():
                    story.append(
                        _pdf_flowable_image(
                            img_path, max_w=13 * cm, max_h=24 * cm
                        )
                    )
                    story.append(Spacer(1, 0.2 * cm))
            i += 1
            continue

        if s.strip().startswith("|"):
            rows: list[list[str]] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                row_line = lines[i].strip()
                if _is_table_sep(row_line):
                    i += 1
                    continue
                cells = [c.strip() for c in row_line.strip("|").split("|")]
                rows.append([_strip_inline_md(c) for c in cells])
                i += 1
            if rows:
                max_cols = max(len(r) for r in rows)
                pad_rows = [r + [""] * (max_cols - len(r)) for r in rows]
                usable_w = 13 * cm
                col_w = usable_w / float(max_cols)
                data: list[list[Any]] = []
                for row in pad_rows:
                    data.append(
                        [_para_cell(c, body) for c in row]
                    )
                t = Table(data, colWidths=[col_w] * max_cols)
                t.setStyle(
                    TableStyle(
                        [
                            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 4),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                            ("TOPPADDING", (0, 0), (-1, -1), 3),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                        ]
                    )
                )
                story.append(t)
                story.append(Spacer(1, 0.15 * cm))
            continue

        mu = _RE_UL.match(s)
        if mu:
            txt = xml_escape(_strip_inline_md(mu.group(1)))
            story.append(Paragraph(f"• {txt}", bullet_body))
            i += 1
            continue

        mo = _RE_OL.match(s)
        if mo:
            n, rest = mo.group(1), mo.group(2)
            txt = xml_escape(_strip_inline_md(rest))
            story.append(Paragraph(f"{n}. {txt}", bullet_body))
            i += 1
            continue

        mq = _RE_BLOCKQUOTE.match(s)
        if mq:
            inner = mq.group(1).strip()
            if inner:
                story.append(
                    Paragraph(xml_escape(_strip_inline_md(inner)), quote_style)
                )
            i += 1
            continue

        plain = _strip_inline_md(s)
        story.append(Paragraph(xml_escape(plain), body))
        i += 1

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )
    doc.build(story)
    return buf.getvalue()
