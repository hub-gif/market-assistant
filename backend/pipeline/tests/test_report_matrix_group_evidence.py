"""从宿主报告 MD 按细类抽取大模型小节。"""
from __future__ import annotations

from django.test import SimpleTestCase

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
