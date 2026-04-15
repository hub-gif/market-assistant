"""结构化竞品摘要：空样本烟测（不依赖真实 run_dir CSV）。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase

from pipeline.csv_schema import infer_total_sales_from_sales_floor
from pipeline.reporting.charts import _cn_volume_int


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

    def test_comment_sentiment_llm_payload_has_semantic_pool(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        texts = ["口感软硬适中很好吃", "太差了不建议"]
        attr = [f"【细类：A｜SKU：1｜品名：x｜店铺：y】{t}" for t in texts]
        pl = jcr.build_comment_sentiment_llm_payload(
            texts,
            attributed_texts=attr,
            shuffle_seed="unit-test-seed",
            semantic_pool_max=10,
        )
        self.assertIn("sample_reviews_semantic_pool", pl)
        self.assertEqual(pl.get("sentiment_bucket_method"), "keyword_substring_heuristic")
        self.assertGreaterEqual(len(pl["sample_reviews_semantic_pool"]), 1)

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

    def test_matrix_groups_require_detail_category_path(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        sku_h = "SKU(skuId)"
        merged = [
            {
                sku_h: "111",
                "detail_category_path": "食品饮料 > 休闲食品 > 饼干 > 粗粮饼干",
                "标题(wareName)": "A",
            },
            {sku_h: "222", "标题(wareName)": "B"},
        ]
        groups = jcr._merged_rows_grouped_for_matrix(merged)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0][1]), 1)
        self.assertEqual(groups[0][1][0][sku_h], "111")
        smap = jcr._sku_to_matrix_group_map(merged, sku_h)
        self.assertEqual(smap.get("111"), "饼干")
        self.assertNotIn("222", smap)
        fb = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=[
                {"sku": "222", "tagCommentContent": "缺路径仍不应进细类桶"},
                {"sku": "111", "tagCommentContent": "有路径进细类"},
            ],
            sku_header=sku_h,
        )
        counts = {g: len(cr) for g, cr, _ in fb}
        self.assertEqual(counts.get("饼干"), 1)

    def test_comment_lines_with_product_context_prefix(self) -> None:
        """评价抽样须带细类/SKU/品名前缀，便于归因。"""
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        sku_h = "SKU(skuId)"
        title_h = "标题(wareName)"
        merged = [
            {
                sku_h: "100",
                title_h: "低GI全麦饼干1kg",
                "detail_brand": "B",
                "detail_price_final": "29",
                "detail_shop_name": "店",
                "detail_category_path": "休闲食品 > 饼干 > 粗粮饼干",
                "detail_product_attributes": "x",
            },
        ]
        comments = [{"sku": "100", "tagCommentContent": "整体口感还差点意思"}]
        lines = jcr._comment_lines_with_product_context(
            comments, merged, sku_header=sku_h, title_h=title_h
        )
        self.assertEqual(len(lines), 1)
        self.assertIn("【细类：", lines[0])
        self.assertIn("SKU：100", lines[0])
        self.assertIn("品名：", lines[0])
        self.assertIn("店铺：", lines[0])
        self.assertIn("整体口感还差点意思", lines[0])

    def test_scenario_groups_llm_payload_matches_section_8_4_counts(self) -> None:
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        sku_h = "SKU(skuId)"
        merged = [
            {
                sku_h: "111",
                "detail_category_path": "食品饮料 > 休闲食品 > 饼干 > 粗粮饼干",
                "标题(wareName)": "A饼",
                "detail_shop_name": "店甲",
            },
        ]
        scen = (("早餐/代餐", ("早餐",)),)
        fb = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=[
                {"sku": "111", "tagCommentContent": "早上当早餐吃还不错"},
            ],
            sku_header=sku_h,
        )
        pl = jcr.build_scenario_groups_llm_payload(
            feedback_groups=fb,
            scenario_groups=scen,
            merged_rows=merged,
            sku_header=sku_h,
            title_h="标题(wareName)",
        )
        self.assertIn("groups", pl)
        self.assertIn("scenario_lexicon", pl)
        g0 = pl["groups"][0]
        self.assertEqual(g0["group"], "饼干")
        self.assertEqual(g0["effective_text_count"], 1)
        self.assertEqual(g0["scenario_distribution"][0]["mention_rows"], 1)
        self.assertEqual(
            g0["scenario_distribution"][0]["scenario"], "早餐/代餐"
        )

    def test_cn_volume_int_parses_total_sales_trailer(self) -> None:
        self.assertEqual(
            _cn_volume_int("已售50万+ | good:99%好评"), 500_000
        )
        self.assertEqual(_cn_volume_int("2.5亿件"), 250_000_000)

    def test_mix_top_remainder_sums_to_all_rows(self) -> None:
        """mix_top 各 count 之和须等于 strip 后可统计行数（与扇图同源）。"""
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        names = [f"店{i}" for i in range(30)]
        mix = jcr._counter_mix_top_rows_with_remainder(
            names, top_n=24, remainder_label="（其余店铺）"
        )
        self.assertEqual(sum(v for _, v in mix), 30)
        self.assertEqual(mix[-1][0], "（其余店铺）")
        self.assertEqual(mix[-1][1], 6)
        self.assertEqual(len(jcr._structure_names_for_pie_counter(names)), 30)

    def test_infer_total_sales_from_sales_floor(self) -> None:
        self.assertEqual(
            infer_total_sales_from_sales_floor("good:99%好评 | 已售50万+"),
            "已售50万+",
        )
        self.assertEqual(infer_total_sales_from_sales_floor(""), "")
