"""市场策略草稿 Markdown（规则，无 LLM）。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.llm.generate_strategy import _omit_ch8_probe_wordchart_fields
from pipeline.llm.generate_strategy import strategy_decisions_substantive
from pipeline.reporting.strategy_draft import build_strategy_draft_markdown


class StrategyDraftTests(SimpleTestCase):
    def test_strategy_decisions_substantive(self) -> None:
        self.assertFalse(strategy_decisions_substantive(None))
        self.assertFalse(strategy_decisions_substantive({}))
        self.assertFalse(
            strategy_decisions_substantive(
                {"ack_risk_keywords": True, "product_role": "  "}
            )
        )
        self.assertTrue(
            strategy_decisions_substantive({"product_role": "新品"})
        )

    def test_for_llm_input_omits_dev_traces(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "K",
            "batch_label": "b1",
            "scope": {"merged_sku_count": 2},
            "strategy_hints": ["线索1"],
            "meta": {"page_start": 1, "page_to": 3, "max_skus_config": 100},
        }
        md = build_strategy_draft_markdown(
            job_id=7,
            keyword="K",
            brief=brief,
            generated_at_iso="2026-01-01",
            for_llm_input=True,
        )
        self.assertNotIn("任务 ID", md)
        self.assertNotIn("generate_strategy.py", md)
        self.assertNotIn("strategy_hints", md)
        self.assertIn("监测摘要自动线索", md)
        self.assertIn("列表页约第 1～3 页", md)
        self.assertNotIn("埋伏笔", md)
        self.assertNotIn("成稿须与 §2", md)
        self.assertNotIn("回扣 §2", md)

    def test_build_contains_sections_and_notes(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "测试K",
            "batch_label": "b1",
            "scope": {"merged_sku_count": 2, "comment_flat_rows": 5},
            "strategy_hints": ["假设A"],
        }
        md = build_strategy_draft_markdown(
            job_id=99,
            keyword="测试K",
            brief=brief,
            business_notes="重点：华东",
            generated_at_iso="2026-04-09T12:00:00",
        )
        self.assertIn("测试K", md)
        self.assertIn("任务 ID**：99", md)
        self.assertIn("假设A", md)
        self.assertIn("重点：华东", md)
        self.assertIn("## 摘要", md)
        self.assertIn("策略范围与前提", md)
        self.assertIn("针对痛点要怎么做", md)
        self.assertIn("类目/细类", md)
        self.assertIn("全局禁止编造", md)
        self.assertNotIn("### 3.2 转化障碍与应对", md)
        self.assertIn("## 一、顾客是谁", md)
        self.assertIn("## 七、品牌四线", md)
        self.assertIn("市场策略制定草稿", md)

    def test_strategy_decisions_merge(self) -> None:
        brief = {"schema_version": 1, "keyword": "K", "batch_label": "b"}
        decisions = {
            "product_role": "追赶型",
            "positioning_choice": "mid",
            "competitive_stance": "flank",
            "marketing_strategy": "内容种草+搜索承接",
            "general_strategy": "先腰后顶",
            "pillar_product": "做低糖配方",
            "ack_risk_keywords": True,
            "ack_risk_price": False,
            "ack_risk_concentration": True,
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="K",
            brief=brief,
            strategy_decisions=decisions,
            report_config={"chapter8_text_mining_probe": False},
        )
        self.assertIn("**本品角色**：追赶型", md)
        self.assertIn("**营销策略**：内容种草+搜索承接", md)
        self.assertIn("**总体策略**：先腰后顶", md)
        self.assertIn("- [x] **卡腰**", md)
        self.assertIn("- [ ] **贴顶**", md)
        i4 = md.find("## 四、为什么要选")
        i5 = md.find("## 五、与其它品牌")
        self.assertGreater(i5, i4)
        self.assertNotIn("贴顶", md[i4:i5])
        i82 = md.find("### 8.2 定价策略")
        self.assertGreater(i82, 0)
        self.assertGreater(md.find("- [x] **卡腰**"), i82)
        self.assertIn("侧翼切入", md)
        self.assertIn("做低糖配方", md)
        self.assertIn("### 7.1 品牌建设", md)
        self.assertIn("- [x] 评论侧归纳是否以偏概全", md)
        self.assertIn("- [ ] 价格带是否含大促", md)
        self.assertIn("- [x] 列表集中度与深入样本品牌是否不一致", md)

    def test_chapter8_probe_omits_focus_scenario_count_bullets(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "低GI",
            "comment_focus_keywords": [{"word": "口感", "count": 501}],
            "usage_scenarios": [
                {
                    "scenario": "控糖/血糖相关",
                    "count": 305,
                    "share_of_text_units": 0.272,
                }
            ],
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="低GI",
            brief=brief,
            report_config={"chapter8_text_mining_probe": True},
        )
        self.assertIn("文本挖掘", md)
        self.assertNotIn("子串统计命中约 **501**", md)
        self.assertNotIn("场景「控糖", md)

    def test_non_probe_path_no_preset_focus_enumeration(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "低GI",
            "comment_focus_keywords": [{"word": "口感", "count": 501}],
            "usage_scenarios": [
                {
                    "scenario": "控糖/血糖相关",
                    "count": 305,
                    "share_of_text_units": 0.272,
                }
            ],
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="低GI",
            brief=brief,
            report_config={"chapter8_text_mining_probe": False},
        )
        self.assertIn("不再", md)
        self.assertIn("预设关注词", md)
        self.assertNotIn("子串统计命中约 **501**", md)
        self.assertNotIn("场景「控糖", md)

    def test_matrix_scope_concentration_not_list_rows_wording(self) -> None:
        """收窄矩阵时 concentration 来自分组内 SKU，§5.1 勿写「列表行」。"""
        brief = {
            "schema_version": 1,
            "keyword": "低GI",
            "strategy_scope_applied": {"group": "饼干"},
            "concentration": {
                "shops_from_list": {
                    "first_share": 0.238,
                    "top_three_combined_share": 0.571,
                    "top_label": "碧翠园京东自营旗舰店",
                },
                "detail_brand_among_merged": {
                    "first_share": 0.238,
                    "top_three_combined_share": 0.667,
                    "top_label": "碧翠园",
                },
            },
        }
        md = build_strategy_draft_markdown(job_id=1, keyword="低GI", brief=brief)
        self.assertIn("与全关键词 **PC 搜索列表行** 集中度**不是同一口径**", md)
        self.assertIn("店铺分布（「饼干」内样本 SKU）", md)
        self.assertIn("该分组样本 SKU 的", md)
        self.assertNotIn("列表侧店铺集中度", md)
        self.assertNotIn("第一大店铺约占列表行的", md)

    def test_shops_unique_sku_basis_rendered(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "测试",
            "concentration": {
                "shops_from_list": {
                    "first_share": 0.6,
                    "top_three_combined_share": 0.85,
                    "top_label": "京东自营",
                    "unique_sku_basis": {
                        "first_share": 0.35,
                        "top_three_combined_share": 0.7,
                        "top_label": "京东自营",
                        "n_unique_skus": 120,
                    },
                },
                "detail_brand_among_merged": {},
            },
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="测试",
            brief=brief,
        )
        self.assertIn("按去重 SKU", md)
        self.assertIn("120", md)
        self.assertIn("35.0%", md)

    def test_chapter8_probe_filters_strategy_hints_focus_scenario_lines(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "K",
            "strategy_hints": [
                "评价文本中「口感、甜」等主题出现较多，可作为假设输入（非严格主题模型）。",
                "用途/场景中「控糖/血糖相关」在约 72% 的有效评价自述中出现，可作为优先假设（词组规则）。",
                "样本内品牌较分散，存在定位空间（需验证）。",
            ],
            "price_promotion_signals": {"rows_with_both_list_and_coupon": 10},
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="K",
            brief=brief,
            report_config={"chapter8_text_mining_probe": True},
        )
        self.assertNotIn("评价文本中「口感", md)
        self.assertNotIn("用途/场景中「控糖", md)
        self.assertIn("样本内品牌较分散", md)
        self.assertIn("price_promotion_signals", md)
        self.assertIn("price_promotion_signals", md)

    def test_ch8_probe_omit_wordchart_nested_in_consumer_feedback(self) -> None:
        compact = {
            "comment_focus_keywords": [{"word": "x", "count": 1}],
            "usage_scenarios": [],
            "comment_sentiment_lexicon": {"pos": ["好"], "neg": ["差"]},
            "strategy_hints": ["条形图同源句子"],
            "consumer_feedback_by_matrix_group": [
                {
                    "group": "饼干",
                    "comment_rows": 10,
                    "focus_keyword_hits": [{"word": "口感", "count": 5}],
                    "scenarios_top": [{"scenario": "早餐", "count": 2}],
                }
            ],
        }
        _omit_ch8_probe_wordchart_fields(compact)
        self.assertNotIn("comment_focus_keywords", compact)
        self.assertNotIn("comment_sentiment_lexicon", compact)
        self.assertNotIn("strategy_hints", compact)
        self.assertNotIn("focus_keyword_hits", compact["consumer_feedback_by_matrix_group"][0])
        self.assertNotIn("scenarios_top", compact["consumer_feedback_by_matrix_group"][0])
        self.assertEqual(
            compact["consumer_feedback_by_matrix_group"][0].get("comment_rows"), 10
        )
