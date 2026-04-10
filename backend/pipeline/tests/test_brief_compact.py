"""brief_compact：矩阵裁剪后仍保留 matrix_overview_for_llm。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.brief_compact import compact_brief_for_llm, matrix_overview_for_llm


class BriefCompactTests(SimpleTestCase):
    def test_matrix_overview_always_in_compact_output(self) -> None:
        brief = {
            "keyword": "测",
            "matrix_by_group": [
                {
                    "group": "饼干",
                    "sku_count": 2,
                    "skus": [
                        {"brand": "A牌", "sku_id": "1"},
                        {"brand": "B牌", "sku_id": "2"},
                    ],
                }
            ],
        }
        out = compact_brief_for_llm(brief, max_chars=120_000)
        self.assertEqual(len(out["matrix_overview_for_llm"]), 1)
        self.assertEqual(out["matrix_overview_for_llm"][0]["group"], "饼干")
        self.assertIn("A牌", out["matrix_overview_for_llm"][0]["distinct_brands_sample"])

    def test_overview_preserved_when_matrix_omitted(self) -> None:
        brief = {
            "matrix_by_group": [
                {
                    "group": f"G{i}",
                    "sku_count": 2,
                    "skus": [
                        {"brand": "B", "sku_id": str(j)}
                        for j in range(2)
                    ],
                }
                for i in range(40)
            ],
            "consumer_feedback_by_matrix_group": [],
        }
        out = compact_brief_for_llm(brief, max_chars=200)
        self.assertTrue(out.get("matrix_by_group_omitted"))
        self.assertEqual(len(out["matrix_overview_for_llm"]), 40)

    def test_matrix_overview_for_llm_empty_without_matrix(self) -> None:
        self.assertEqual(matrix_overview_for_llm({}), [])
