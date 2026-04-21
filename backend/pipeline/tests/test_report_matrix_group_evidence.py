"""从宿主报告 MD 按细类抽取大模型小节。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.llm.generate_sections import demote_sentiment_inner_h4_to_h5_for_matrix_group
from pipeline.reporting.report_matrix_group_evidence import (
    extract_level4_sections_by_group_title,
)


class ReportMatrixGroupEvidenceTests(SimpleTestCase):
    def test_extract_multiple_blocks_in_order(self) -> None:
        md = """#### 细类要点归纳（大模型）

> 说明

#### 饼干
A 段矩阵归纳。

#### 饮料
别的细类。

---

#### 细类评价与关注词要点归纳（大模型）

#### 饼干
B 段评论归纳。
"""
        parts = extract_level4_sections_by_group_title(md, "饼干")
        self.assertEqual(len(parts), 2)
        self.assertIn("A 段矩阵归纳", parts[0])
        self.assertIn("B 段评论归纳", parts[1])

    def test_no_match(self) -> None:
        self.assertEqual(
            extract_level4_sections_by_group_title("## 二\n", "饼干"),
            [],
        )

    def test_extract_8_3_sentiment_when_inner_subsections_are_h5(self) -> None:
        """8.3 在 #### 细类 下须用 ##### 子标题，否则抽取在首个 #### 子节处断开。"""
        inner = demote_sentiment_inner_h4_to_h5_for_matrix_group(
            "#### 正向体验主题\n\n正文A\n\n#### 负向评价主题归因\n\n正文B\n"
        )
        md = (
            "### 8.3 评价正/负向主题\n\n"
            "#### 饼干\n\n"
            f"{inner}\n"
        )
        parts = extract_level4_sections_by_group_title(md, "饼干")
        self.assertEqual(len(parts), 1)
        self.assertIn("正文A", parts[0])
        self.assertIn("正文B", parts[0])

    def test_demote_known_h4_titles_only(self) -> None:
        raw = "#### 正向体验主题\nx\n#### 饼干\ny\n"
        out = demote_sentiment_inner_h4_to_h5_for_matrix_group(raw)
        self.assertIn("##### 正向体验主题", out)
        self.assertIn("#### 饼干", out)
