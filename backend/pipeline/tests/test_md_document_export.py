"""Markdown → docx/pdf 导出（防回归：docx 主循环须递增行指针）。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.md_document_export import markdown_to_docx_bytes, markdown_to_pdf_bytes


class MdDocumentExportTests(SimpleTestCase):
    def test_docx_plain_lines_terminate(self) -> None:
        md = "第一行\n\n第二行\n仍是一段"
        data = markdown_to_docx_bytes(md)
        self.assertGreater(len(data), 2000)
        self.assertTrue(data.startswith(b"PK"))

    def test_pdf_plain_lines_terminate(self) -> None:
        md = "标题\n\n正文一行"
        data = markdown_to_pdf_bytes(md)
        self.assertGreater(len(data), 100)
        self.assertTrue(data.startswith(b"%PDF"))
