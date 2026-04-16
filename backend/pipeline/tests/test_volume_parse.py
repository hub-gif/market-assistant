"""销量/评价量文案解析（与 reporting.charts._cn_volume_int 口径一致）。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.volume_parse import (
    cn_volume_int,
    comment_count_sort_value_from_cell,
    sales_sort_value_from_search_cells,
)


class VolumeParseTests(SimpleTestCase):
    def test_cn_volume_wan(self) -> None:
        self.assertEqual(cn_volume_int("已售50万+"), 500_000)

    def test_cn_volume_yi(self) -> None:
        self.assertEqual(cn_volume_int("1.2亿件"), 120_000_000)

    def test_sales_sort_prefers_total_sales(self) -> None:
        v = sales_sort_value_from_search_cells("已售1万+", "已售5000+")
        self.assertEqual(v, 10_000)

    def test_sales_sort_fallback_floor(self) -> None:
        v = sales_sort_value_from_search_cells("", "已售3万+")
        self.assertEqual(v, 30_000)

    def test_comment_count_cell(self) -> None:
        v = comment_count_sort_value_from_cell("5000+条评价")
        self.assertEqual(v, 5000)
