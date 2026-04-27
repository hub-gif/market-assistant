"""price_promotion_signals 策略摘要句与统计一致。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.competitor_report.price_promo import (
    _analyze_price_promotions,
    price_promotion_signals_strategy_brief_cn,
)


class PricePromoStrategyBriefCnTests(SimpleTestCase):
    def test_brief_cn_mentions_alignment_when_both_prices(self) -> None:
        rows = [
            {"标价": "100", "券后到手价": "80"},
            {"标价": "50", "券后到手价": "50"},
        ]
        p = _analyze_price_promotions(rows)
        t = price_promotion_signals_strategy_brief_cn(p)
        self.assertIn("同时有标价与券后", t)
        self.assertRegex(t, r"可对齐\*\* 的行 \*\*2\*\*")
        self.assertRegex(t, r"严格低于\*\*标价的行 \*\*1\*\*")
        self.assertIn("§8.3 请写清", t)

    def test_empty_dict_message(self) -> None:
        t = price_promotion_signals_strategy_brief_cn({})
        self.assertIn("未携带", t)
