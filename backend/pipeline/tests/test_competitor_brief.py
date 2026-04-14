"""结构化竞品摘要：空样本烟测（不依赖真实 run_dir CSV）。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


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
        self.assertTrue(out.get("matrix_compact_section"))
        self.assertIn("comment_sentiment_lexicon", out)
        self.assertEqual(out["comment_sentiment_lexicon"].get("text_units"), 0)
        import json

        json.dumps(out)

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

    def test_matrix_group_key_product_like_title_not_used_as_group(self) -> None:
        """类目列误入商品标题时，勿当作「饼干」式细类名。"""
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        cat = "类目(leafCategory,cid3Name,catid)"
        prop = "规格属性(propertyList,color,catid,shortName)"

        title_row = {
            cat: "南纳香低gi大米10斤 GI值≤55（1 款）",
            prop: "",
        }
        self.assertEqual(
            jcr._competitor_matrix_group_key(title_row, catid_short={}),
            jcr._MATRIX_GROUP_LIST_PRODUCTLIKE_FALLBACK,
        )

        title_with_sn = {
            cat: "南纳香低gi大米10斤 GI值≤55",
            prop: "简称: 大米",
        }
        self.assertEqual(
            jcr._competitor_matrix_group_key(title_with_sn, catid_short={}),
            "大米",
        )

        low_gi_noodle = {cat: "低GI面条", prop: ""}
        self.assertEqual(
            jcr._competitor_matrix_group_key(low_gi_noodle, catid_short={}),
            "低GI面条",
        )

    def test_detail_empty_rows_excluded_from_matrix_groups(self) -> None:
        """商详五项全空时不进入矩阵分组（避免仅凭列表类目硬分）。"""
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        cat = "类目(leafCategory,cid3Name,catid)"
        prop = "规格属性(propertyList,color,catid,shortName)"
        sku = "SKU(skuId)"
        title = "标题(wareName)"

        fail_row = {
            sku: "111",
            title: "仅列表有标题",
            cat: "99999",
            prop: "",
            "detail_brand": "",
            "detail_price_final": "",
            "detail_shop_name": "",
            "detail_category_path": "",
            "detail_product_attributes": "",
        }
        ok_row = {
            sku: "222",
            title: "有商详",
            cat: "饼干",
            prop: "",
            "detail_brand": "某品牌",
            "detail_price_final": "19.9",
            "detail_shop_name": "某店",
            "detail_category_path": "",
            "detail_product_attributes": "配料:小麦粉",
        }
        self.assertFalse(jcr._merged_row_has_detail_for_matrix(fail_row))
        self.assertTrue(jcr._merged_row_has_detail_for_matrix(ok_row))

        grouped = jcr._merged_rows_grouped_for_matrix([fail_row, ok_row])
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0][1][0][sku], "222")

        m = jcr._sku_to_matrix_group_map([fail_row, ok_row], sku)
        self.assertEqual(m.get("111"), jcr._MATRIX_SKU_DETAIL_FAILED_BUCKET)
        self.assertNotEqual(m.get("222"), jcr._MATRIX_SKU_DETAIL_FAILED_BUCKET)

    def test_list_shop_mix_top_counts_sum_to_shop_rows(self) -> None:
        """Top-N 截断时须带尾桶，否则饼图分母小于含店铺名行数、与 §4.2 表格不一致。"""
        root = Path(settings.CRAWLER_JD_ROOT).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import jd_competitor_report as jcr  # noqa: WPS433

        shop_k = "店铺名(shopName)"
        rows: list[dict[str, str]] = []
        rows.append({shop_k: "头部店", "SKU(skuId)": "1"})
        for i in range(50):
            rows.append({shop_k: f"小店{i}", "SKU(skuId)": str(i + 2)})
        mix = jcr._label_count_dicts_top_n_plus_other(
            jcr._structure_shops(rows, list_export=True),
            top_n=24,
            other_label="其他（Top24 以外店铺行数合计）",
        )
        self.assertEqual(sum(int(x["count"]) for x in mix), 51)
        self.assertTrue(
            any(
                (x.get("label") or "").startswith("其他（Top24")
                for x in mix
            ),
            "长尾店铺应合并到「其他」尾桶",
        )

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
                "detail_category_path": "a>饼干",
                "detail_product_attributes": "x",
            }
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

        fb = jcr._consumer_feedback_by_matrix_group(
            merged_rows=merged,
            comment_rows=comments,
            sku_header=sku_h,
        )
        pl = jcr.build_comment_groups_llm_payload(
            feedback_groups=fb,
            focus_words=("口感",),
            merged_rows=merged,
            sku_header=sku_h,
            title_h=title_h,
        )
        self.assertTrue(pl)
        snip = (pl[0].get("sample_text_snippets") or [""])[0]
        self.assertIn("SKU：100", snip)
        self.assertIn("店铺：", snip)
        self.assertIn("整体口感还差点意思", snip)
