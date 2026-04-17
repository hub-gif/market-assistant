"""buyer_offer_export_csv：榜单列与促销文案列（分隔符拼接）。"""
from __future__ import annotations

import sys
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase

from pipeline.csv.schema import strip_buyer_ranking_line_prefix


class BuyerOfferExportCsvTests(SimpleTestCase):
    def test_strip_buyer_ranking_prefix(self) -> None:
        self.assertEqual(
            strip_buyer_ranking_line_prefix("榜单/曝光：老金磨方药食同源热卖榜·第1名。"),
            "老金磨方药食同源热卖榜·第1名。",
        )
        self.assertEqual(
            strip_buyer_ranking_line_prefix("榜单/曝光粗粮饼干热卖榜·第5名"),
            "粗粮饼干热卖榜·第5名",
        )

    def test_ranking_and_promo_from_profile(self) -> None:
        dr = Path(settings.CRAWLER_JD_ROOT).resolve() / "detail"
        if str(dr) not in sys.path:
            sys.path.insert(0, str(dr))
        import jd_detail_buyer_extraction as be  # noqa: WPS433

        prof = {
            "visibility": {"rankings": ["20-40元酥性饼干热卖榜·第8名"]},
            "buyer_summary_lines": [
                "当前展示「到手价」约 27.97 元。",
                "详情页优惠拆解：购买立减。",
                "榜单/曝光：应被排除。",
                "送达：应被排除。",
                "企业采购提示：应被排除。",
            ],
        }
        self.assertEqual(
            be.buyer_ranking_line_from_profile(prof),
            "20-40元酥性饼干热卖榜·第8名。",
        )
        t = be.buyer_promo_text_from_profile(prof)
        self.assertNotIn("榜单", t)
        self.assertNotIn("送达", t)
        self.assertNotIn("企业采购", t)
        self.assertIn(" | ", t)
        self.assertIn("到手价", t)
