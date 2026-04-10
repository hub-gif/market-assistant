"""结构化竞品摘要：空样本烟测（不依赖真实 run_dir CSV）。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class BuildCompetitorBriefTests(SimpleTestCase):
    def test_empty_merged_json_safe(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "pc_search_raw").mkdir(parents=True)

            out = jcr.build_competitor_brief(
                run_dir=run_dir,
                keyword="测试",
                merged_rows=[],
                search_export_rows=[],
                comment_rows=[],
                meta=None,
            )

        self.assertEqual(out["schema_version"], 1)
        self.assertEqual(out["scope"]["merged_sku_count"], 0)
        self.assertIsInstance(out["strategy_hints"], list)
        self.assertEqual(out["matrix_by_group"], [])
        self.assertIn("comment_sentiment_lexicon", out)
        self.assertEqual(out["comment_sentiment_lexicon"].get("text_units"), 0)
        import json

        json.dumps(out)

    def test_custom_focus_words_in_report_config(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "pc_search_raw").mkdir(parents=True)

            out = jcr.build_competitor_brief(
                run_dir=run_dir,
                keyword="测试",
                merged_rows=[],
                search_export_rows=[],
                comment_rows=[
                    {
                        "tagCommentContent": "自定义词阿尔法出现两次 自定义词阿尔法",
                    }
                ],
                meta=None,
                report_config={"comment_focus_words": ["自定义词阿尔法"]},
            )

        words = {x["word"] for x in out["comment_focus_keywords"]}
        self.assertIn("自定义词阿尔法", words)
