"""product_manual：按关键词从长文本摘录（与多产品手册配套）。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.reporting.product_manual import excerpt_text_by_keyword


class ProductManualExcerptTests(SimpleTestCase):
    def test_paragraph_hits_concatenated(self) -> None:
        text = "其它产品线。\n\n清肌水光精华液：修护保湿。\n\n另一款面霜。\n\n清肌水光精华液 使用说明。\n"
        out = excerpt_text_by_keyword(text, "清肌水光精华液", max_chars=10_000)
        self.assertIn("修护保湿", out)
        self.assertIn("使用说明", out)
        self.assertNotIn("另一款面霜", out)

    def test_whitespace_tolerant_in_paragraph(self) -> None:
        text = "标题\n\n清肌 水光 精华液 系列卖点 XYZ。\n"
        out = excerpt_text_by_keyword(text, "清肌水光精华液", max_chars=10_000)
        self.assertIn("卖点", out)

    def test_fallback_window_when_no_double_newline(self) -> None:
        text = "前言" + "x" * 500 + "清肌水光精华液" + "y" * 500
        out = excerpt_text_by_keyword(text, "清肌水光精华液", max_chars=10_000)
        self.assertIn("清肌水光精华液", out)

    def test_loose_match_inserts_spaces_in_body(self) -> None:
        text = "前缀" + "z" * 200 + "清肌 水光 精华液卖点" + "t" * 200
        out = excerpt_text_by_keyword(text, "清肌水光精华液", max_chars=10_000)
        self.assertIn("卖点", out)

    def test_long_chunk_splits_by_single_newlines(self) -> None:
        lines = ["其它产品A", "清肌水光精华液 专段一行", "其它产品B", "清肌水光精华液 又一节"]
        blob = "\n\n".join(lines)
        filler = "x" * 1200
        big = (filler + "\n") * 5
        text = big + "\n\n" + "短段\n\n" + blob
        out = excerpt_text_by_keyword(text, "清肌水光精华液", max_chars=20_000)
        self.assertIn("专段一行", out)
        self.assertIn("又一节", out)
        self.assertNotIn("其它产品A", out)
        self.assertNotIn("其它产品B", out)

    def test_max_chars_none_returns_all_hits(self) -> None:
        paras = "\n\n".join([f"清肌水光精华液 段{i}" for i in range(40)])
        out = excerpt_text_by_keyword(paras, "清肌水光精华液", max_chars=None)
        self.assertIn("段0", out)
        self.assertIn("段39", out)

    def test_product_block_collects_lines_until_next_short_title(self) -> None:
        text = (
            "前文\n\n"
            "蓝铜肽水光精华液\n"
            "GHK-Cu \n"
            "Hydrating Serum\n"
            "修护型抗老副标题·高能修护\n"
            "核心成分\n"
            "蓝铜胜肽\n"
            "成分说明一句。\n"
            "透亮水光精华液\n"
            "下一段不应出现。\n"
        )
        out = excerpt_text_by_keyword(text, "蓝铜肽水光精华液", max_chars=10_000)
        self.assertIn("蓝铜胜肽", out)
        self.assertIn("核心成分", out)
        self.assertIn("Hydrating Serum", out)
        self.assertNotIn("下一段不应出现", out)
        self.assertNotIn("透亮水光精华液", out)

    def test_long_catalog_line_does_not_use_line_block(self) -> None:
        row = "x" * 80 + "蓝铜肽水光精华液" + "y" * 80
        text = row + "\n\n单标题\n蓝铜肽水光精华液\n成分A\n"
        out = excerpt_text_by_keyword(text, "蓝铜肽水光精华液", max_chars=10_000)
        self.assertIn("成分A", out)
        self.assertNotIn(row.strip()[:20], out[:200])

    def test_max_chars_limits_long_join(self) -> None:
        paras = "\n\n".join([f"清肌水光精华液 段{i} " + "x" * 80 for i in range(80)])
        out = excerpt_text_by_keyword(paras, "清肌水光精华液", max_chars=400)
        self.assertLess(len(out), len(paras))
        self.assertIn("截断", out)
