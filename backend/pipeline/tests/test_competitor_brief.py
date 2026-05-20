"""结构化竞品摘要：空样本烟测（不依赖真实 run_dir CSV）。"""
from __future__ import annotations

import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from pipeline.competitor_report.matrix_group import UNCATEGORIZED_MATRIX_GROUP_LABEL
from pipeline.competitor_report import jd_report as jcr
from pipeline.competitor_report.comment_sentiment import (
    _comment_sentiment_lexicon,
    build_comment_sentiment_llm_payload,
)
from pipeline.csv.schema import infer_total_sales_from_sales_floor
from pipeline.reporting.charts import _cn_volume_int
from pipeline.competitor_report.csv_io import _price_context_spec_merged


class BuildCompetitorBriefTests(SimpleTestCase):
    def test_empty_merged_json_safe(self) -> None:
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
        self.assertNotIn("comment_sentiment_lexicon", out)
        import json

        json.dumps(out)

    def test_comment_sentiment_llm_payload_has_semantic_pool(self) -> None:
        texts = ["口感软硬适中很好吃", "太差了不建议"]
        attr = [f"【细类：A｜SKU：1｜品名：x｜店铺：y】{t}" for t in texts]
        pl = build_comment_sentiment_llm_payload(
            texts,
            attributed_texts=attr,
            shuffle_seed="unit-test-seed",
            semantic_pool_max=10,
        )
        self.assertIn("sample_reviews_semantic_pool", pl)
        self.assertNotIn("comment_sentiment_lexicon", pl)
        self.assertNotIn("negative_lexeme_hits_top", pl)
        self.assertGreaterEqual(len(pl["sample_reviews_semantic_pool"]), 1)

    def test_comment_sentiment_score_then_lexeme(self) -> None:
        texts = ["很好吃", "太差了", "一般般"]
        scores = [5, 1, 3]
        lex = _comment_sentiment_lexicon(texts, scores)
        self.assertEqual(lex.get("method"), "score_then_lexeme")
        self.assertEqual(lex.get("positive_only"), 1)
        self.assertEqual(lex.get("negative_only"), 1)
        self.assertEqual(lex.get("neutral_or_empty"), 1)
        pl = build_comment_sentiment_llm_payload(texts, scores=scores)
        dist = pl.get("star_rating_distribution") or {}
        self.assertEqual(dist.get("score_1_2"), 1)
        self.assertEqual(dist.get("score_3"), 1)
        self.assertEqual(dist.get("score_4_5"), 1)
        self.assertNotIn("comment_sentiment_lexicon", pl)

    def test_comment_sentiment_all_scores_missing_falls_back_keyword(self) -> None:
        texts = ["好吃推荐", "差评"]
        scores = [None, None]
        lex = _comment_sentiment_lexicon(texts, scores)
        self.assertEqual(lex.get("method"), "keyword_lexicon")

    def test_brief_omits_preset_comment_focus_keywords(self) -> None:
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

        self.assertEqual(out["comment_focus_keywords"], [])

    def test_uncategorized_bucket_when_detail_category_missing(self) -> None:
        """无类目路径但有 SKU：归入「未分类」，评价与矩阵载荷仍对齐。"""
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
        self.assertEqual(len(groups), 2)
        by_name = {name: rows for name, rows in groups}
        self.assertEqual(len(by_name["饼干"]), 1)
        self.assertEqual(len(by_name[UNCATEGORIZED_MATRIX_GROUP_LABEL]), 1)
        self.assertEqual(by_name["饼干"][0][sku_h], "111")
        self.assertEqual(by_name[UNCATEGORIZED_MATRIX_GROUP_LABEL][0][sku_h], "222")
        smap = jcr._sku_to_matrix_group_map(merged, sku_h)
        self.assertEqual(smap.get("111"), "饼干")
        self.assertEqual(smap.get("222"), UNCATEGORIZED_MATRIX_GROUP_LABEL)
        fb = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=[
                {"sku": "222", "tagCommentContent": "详情失败仍应进入未分类归纳"},
                {"sku": "111", "tagCommentContent": "有路径进细类"},
            ],
            sku_header=sku_h,
        )
        counts = {g: len(cr) for g, cr, _ in fb}
        self.assertEqual(counts.get("饼干"), 1)
        self.assertEqual(counts.get(UNCATEGORIZED_MATRIX_GROUP_LABEL), 1)

    def test_comment_lines_with_product_context_prefix(self) -> None:
        """评价抽样须带细类/SKU/品名前缀，便于归因。"""
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

    def test_cn_volume_int_parses_total_sales_trailer(self) -> None:
        self.assertEqual(
            _cn_volume_int("已售50万+ | good:99%好评"), 500_000
        )
        self.assertEqual(_cn_volume_int("2.5亿件"), 250_000_000)

    def test_mix_top_remainder_sums_to_all_rows(self) -> None:
        """mix_top 各 count 之和须等于 strip 后可统计行数（与扇图同源）。"""
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

    def test_price_context_spec_merged_from_title_when_attr_empty(self) -> None:
        """规格属性列为空时，应从标题抽取 ml 等片段，供价盘可比性对照。"""
        row = {"标题": "某品牌精华液修护紧致20ml小样旅行装", "规格属性": ""}
        self.assertIn("20ml", _price_context_spec_merged(row))

    def test_price_context_spec_merged_subsumes_title_hints_in_attr(self) -> None:
        """标题与规格列重复时，不重复堆叠「；标题片段」。"""
        row = {"标题": "精华液20ml", "规格属性": "容量：20ml｜功效：保湿"}
        m = _price_context_spec_merged(row)
        self.assertNotIn("；20ml", m)
        self.assertIn("容量", m)

    def test_price_context_spec_merged_reads_legacy_title_header(self) -> None:
        row = {"标题(wareName)": "紧致精华30ml", "规格属性": ""}
        self.assertIn("30ml", _price_context_spec_merged(row))

    def test_price_context_spec_merged_title_volume_and_bottle_count(self) -> None:
        row = {"标题": "某精华修护30ml 三瓶 小样", "规格属性": ""}
        m = _price_context_spec_merged(row)
        self.assertIn("30ml", m)
        self.assertIn("三瓶", m)

    def test_price_context_spec_merged_title_arabic_bottles(self) -> None:
        row = {"标题": "补水精华3瓶30ml组合", "规格属性": ""}
        m = _price_context_spec_merged(row)
        self.assertIn("3瓶", m)
        self.assertIn("30ml", m)

    def test_price_context_spec_merged_prefers_ml_times_count(self) -> None:
        row = {"标题": "同款30ml×3礼盒", "规格属性": ""}
        m = _price_context_spec_merged(row)
        self.assertRegex(m, r"30\s*ml\s*[×xXＸ]\s*3")
