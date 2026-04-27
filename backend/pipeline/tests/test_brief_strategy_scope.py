"""brief 按矩阵分组收窄。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.reporting.brief_strategy_scope import (
    filter_brief_for_strategy_matrix_group,
    list_matrix_groups_for_api,
    resolve_strategy_matrix_group_index,
)


class BriefStrategyScopeTests(SimpleTestCase):
    def _sample_brief(self) -> dict:
        return {
            "schema_version": 1,
            "keyword": "低GI",
            "matrix_by_group": [
                {
                    "group": "饮料",
                    "sku_count": 1,
                    "skus": [
                        {
                            "brand": "A",
                            "shop": "S1",
                            "category": "水饮 > 茶",
                            "list_price_show": "10",
                        }
                    ],
                },
                {
                    "group": "饼干",
                    "sku_count": 2,
                    "skus": [
                        {
                            "brand": "B",
                            "shop": "S2",
                            "category": "休闲食品 > 饼干 > 粗粮饼干",
                            "标价": "100",
                            "券后到手价": "80",
                        },
                        {
                            "brand": "B",
                            "shop": "S2",
                            "category": "休闲食品 > 饼干 > 苏打饼干",
                            "标价": "50",
                            "券后到手价": "50",
                        },
                    ],
                },
            ],
            "consumer_feedback_by_matrix_group": [
                {
                    "group": "饮料",
                    "comment_rows": 5,
                    "effective_comment_text_units": 5,
                },
                {
                    "group": "饼干",
                    "comment_rows": 8,
                    "effective_comment_text_units": 8,
                },
            ],
            "usage_scenarios_by_matrix_group": [
                {"group": "饮料", "scenarios": []},
                {"group": "饼干", "scenarios": [{"scenario": "早餐", "count": 4}]},
            ],
            "notes": ["原注"],
        }

    def test_list_matrix_groups(self) -> None:
        b = self._sample_brief()
        m = list_matrix_groups_for_api(b)
        self.assertEqual(len(m), 2)
        self.assertEqual(m[1]["group"], "饼干")
        self.assertEqual(m[1]["index"], 1)

    def test_resolve_by_label(self) -> None:
        b = self._sample_brief()
        idx, err = resolve_strategy_matrix_group_index(
            b, matrix_group_label="饼干"
        )
        self.assertIsNone(err)
        self.assertEqual(idx, 1)

    def test_resolve_by_index(self) -> None:
        b = self._sample_brief()
        idx, err = resolve_strategy_matrix_group_index(
            b, matrix_group_index=1
        )
        self.assertIsNone(err)
        self.assertEqual(idx, 1)

    def test_resolve_mismatch(self) -> None:
        b = self._sample_brief()
        idx, err = resolve_strategy_matrix_group_index(
            b, matrix_group_index=0, matrix_group_label="饼干"
        )
        self.assertIsNotNone(err)
        self.assertIsNone(idx)

    def test_filter_keeps_only_group(self) -> None:
        b = self._sample_brief()
        out = filter_brief_for_strategy_matrix_group(b, matrix_group_index=1)
        self.assertEqual(len(out["matrix_by_group"]), 1)
        self.assertEqual(out["matrix_by_group"][0]["group"], "饼干")
        self.assertEqual(len(out["matrix_by_group"][0]["skus"]), 2)
        self.assertEqual(len(out["consumer_feedback_by_matrix_group"]), 1)
        self.assertEqual(out["comment_focus_keywords"], [])
        self.assertEqual(out["price_stats_source"], "strategy_scope_matrix_group_skus")
        self.assertIn("strategy_scope_applied", out)
        self.assertEqual(out["strategy_scope_applied"]["group"], "饼干")

    def test_filter_recomputes_price_promotion_signals(self) -> None:
        b = self._sample_brief()
        out = filter_brief_for_strategy_matrix_group(b, matrix_group_index=1)
        pps = out.get("price_promotion_signals")
        self.assertIsInstance(pps, dict)
        assert isinstance(pps, dict)
        self.assertEqual(pps.get("row_count"), 2)
        self.assertEqual(pps.get("rows_with_both_list_and_coupon"), 2)
        self.assertEqual(pps.get("rows_coupon_below_list_price"), 1)
