"""简报包 ZIP 与要点摘录 Markdown。"""
from __future__ import annotations

import json
import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase

from pipeline.reporting.brief_pack import (
    build_brief_pack_zip_bytes,
    markdown_summary_from_brief,
)


class BriefPackTests(SimpleTestCase):
    def test_markdown_summary_contains_keyword_and_hints(self) -> None:
        md = markdown_summary_from_brief(
            {
                "keyword": "测试词",
                "batch_label": "batch1",
                "scope": {"merged_sku_count": 3, "comment_flat_rows": 10},
                "strategy_hints": ["提示一行"],
            }
        )
        self.assertIn("测试词", md)
        self.assertIn("提示一行", md)
        self.assertIn("深入 SKU 数", md)

    def test_zip_contains_expected_entries(self) -> None:
        brief = {"keyword": "k", "schema_version": 1}
        with TemporaryDirectory() as td:
            p = Path(td) / "competitor_analysis.md"
            p.write_text("# 报告\n", encoding="utf-8")
            raw = build_brief_pack_zip_bytes(Path(td), brief)
        buf = BytesIO(raw)
        with zipfile.ZipFile(buf, "r") as zf:
            names = set(zf.namelist())
            data = json.loads(zf.read("02_结构化摘要.json").decode())
        self.assertIn("01_竞品分析报告.md", names)
        self.assertIn("02_结构化摘要.json", names)
        self.assertIn("03_要点摘录.md", names)
        self.assertIn("00_说明.txt", names)
        self.assertEqual(data["keyword"], "k")

    def test_zip_raises_without_report_file(self) -> None:
        with TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):
                build_brief_pack_zip_bytes(Path(td), {})
