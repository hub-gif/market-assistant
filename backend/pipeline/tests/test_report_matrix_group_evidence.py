"""从宿主报告 MD 按细类抽取大模型小节。"""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase

from pipeline.reporting.report_matrix_group_evidence import (
    extract_level4_sections_by_group_title,
    extract_sentiment_83_level4_body,
    load_report_matrix_group_evidence_markdown,
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

    def test_sentiment_83_nested_level4(self) -> None:
        md = """## 八、消费者反馈

### 8.3 评价正/负向主题（按细类 · 大模型）

> 说明

#### 饼干

#### 正向体验主题
酥脆好评。

#### 负向评价主题归因
略贵。

#### 西式糕点

#### 正向体验主题
别的细类。

## 九、策略
"""
        body = extract_sentiment_83_level4_body(md, "饼干")
        self.assertIn("酥脆好评", body)
        self.assertIn("正向体验主题", body)
        self.assertIn("略贵", body)
        self.assertNotIn("别的细类", body)

    def test_load_includes_83_after_splice(self) -> None:
        md = """#### 饼干
矩阵段。

### 8.3 评价正/负向主题（按细类 · 大模型）

#### 饼干

#### 正向体验主题
情感段。

## 九、策略
"""
        with TemporaryDirectory() as td:
            p = Path(td) / "competitor_analysis.md"
            p.write_text(md, encoding="utf-8")
            out, src = load_report_matrix_group_evidence_markdown(td, "饼干")
        self.assertEqual(src, "competitor_analysis_md")
        self.assertIn("矩阵段", out)
        self.assertIn("情感段", out)
