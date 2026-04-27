"""竞品报告 Markdown：第八章情感小节嵌入（烟测）。"""
from __future__ import annotations

import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from pipeline.competitor_report import jd_report as jcr


class JdReportSentimentSectionTests(SimpleTestCase):
    def test_chapter_83_embeds_when_sentiment_md_provided(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "report_assets").mkdir(parents=True)
            md = jcr.build_competitor_markdown(
                run_dir=run_dir,
                keyword="测试词",
                merged_rows=[],
                search_export_rows=[],
                comment_rows=[],
                meta=None,
                llm_sentiment_section_md="#### 饼干\n\n- 正向\n",
            )
        self.assertIn("### 8.3 评价正/负向主题（按细类 · 大模型）", md)
        self.assertIn("不替代**探针的开放词表", md)
        self.assertIn("#### 饼干", md)
