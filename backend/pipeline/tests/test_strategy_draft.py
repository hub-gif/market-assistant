"""市场策略草稿 Markdown（规则，无 LLM）。"""
from __future__ import annotations

import json
from pathlib import Path

from django.test import SimpleTestCase

from pipeline.llm.generate_strategy import _omit_ch8_probe_wordchart_fields
from pipeline.llm.generate_strategy import sanitize_strategy_s21_pain_column_md
from pipeline.llm.generate_strategy import strategy_decisions_substantive
from pipeline.reporting.strategy_draft import build_strategy_draft_markdown


class StrategyDraftTests(SimpleTestCase):
    def test_sanitize_s21_pain_column_strips_partial_competitor(self) -> None:
        md = """## 二、产品价值与用户痛点

### 2.1 针对痛点要怎么做

| 类目/细类（本决策适用） | 用户痛点（简述） | 策略动作 | 具体怎么做 | 如何验证 |
|------------------------|----------------|----------|------------|----------|
| 粗粮饼干 | 用户希望在控糖的同时获得良好的饱腹感与口感，但部分竞品口感偏硬或缺乏层次感 | A | B | C |
| 酥性饼干 | 部分竞品包装差 | 推小包装 | D | E |

## 三、为什么要买
"""
        out = sanitize_strategy_s21_pain_column_md(md)
        self.assertNotIn("部分竞品", out)
        self.assertIn("控糖", out)
        self.assertIn("良好的饱腹感", out)
        # 起首以「部分竞品…」时整格收敛为典型场景假设占位
        self.assertIn("典型场景假设", out)

    def test_sanitize_s21_passthrough_without_section2(self) -> None:
        self.assertEqual("## 一\nx", sanitize_strategy_s21_pain_column_md("## 一\nx"))

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
        self.assertTrue(
            strategy_decisions_substantive({"stage_goal_type": "本阶段做销量与转化"})
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
            brief={
                **brief,
                "category_mix_top": [
                    {"label": "粗粮饼干", "count": 11},
                    {"label": "酥性饼干", "count": 10},
                ],
                "pc_search_raw": {"result_count_consensus": 333619},
                "price_stats": {
                    "n": 21,
                    "min": 14.38,
                    "max": 64.97,
                    "median": 27.97,
                },
            },
            generated_at_iso="2026-01-01",
            for_llm_input=True,
            strategy_decisions={"positioning_choice": "mid"},
        )
        self.assertNotIn("任务 ID", md)
        self.assertNotIn("generate_strategy.py", md)
        self.assertNotIn("strategy_hints", md)
        self.assertNotIn("333619", md)
        self.assertNotIn("27.97", md)
        self.assertNotIn("价位阵地取向（表单勾选", md)
        self.assertNotIn("- [ ] **贴顶**", md)
        self.assertNotIn("- [ ] 锁定主推款", md)
        self.assertNotIn("类目结构（摘录）", md)
        self.assertNotIn("粗粮饼干", md)
        self.assertIn("**评论与归纳口径**", md)
        self.assertIn("当前表单取向为卡腰", md)
        self.assertIn("监测摘要自动线索", md)
        self.assertIn("列表页约第 1～3 页", md)
        self.assertNotIn("埋伏笔", md)
        self.assertNotIn("成稿须与 §2", md)
        self.assertNotIn("回扣 §2", md)
        self.assertNotIn("（占位）", md)
        self.assertIn("### 1.3 本品聚焦\n", md)
        self.assertIn("### 5.1 差异化方向\n", md)

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

    def test_stage_goal_type_in_scope_table(self) -> None:
        brief = {
            "schema_version": 1,
            "keyword": "K",
            "batch_label": "b1",
            "scope": {"merged_sku_count": 2},
        }
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="K",
            brief=brief,
            strategy_decisions={"stage_goal_type": "本阶段以拉新尝试为主"},
            generated_at_iso="2026-04-09T12:00:00",
            for_llm_input=True,
        )
        self.assertIn("| **本阶段策略目标类型** | 本阶段以拉新尝试为主 |", md)
        self.assertIn("**本阶段策略目标类型**：本阶段以拉新尝试为主", md)

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
        self.assertIn("**卡腰**", md)
        i4 = md.find("## 四、为什么要选")
        i5 = md.find("## 五、与其它品牌")
        self.assertGreater(i5, i4)
        self.assertNotIn("贴顶", md[i4:i5])
        i82 = md.find("### 8.2 定价策略")
        self.assertGreater(i82, 0)
        self.assertGreater(md.find("卡腰", i82), i82)
        self.assertNotIn("- [ ] **贴顶**", md)
        self.assertIn("侧翼切入", md)
        self.assertIn("做低糖配方", md)
        self.assertIn("### 7.1 品牌建设", md)
        self.assertIn("**评论与归纳口径**", md)
        self.assertIn("**价格带与清洗规则**", md)
        self.assertIn("**列表曝光与深入样本**", md)
        self.assertIn("业务已在表单中勾选", md)
        self.assertNotIn("- [x] 评论侧", md)
        self.assertNotIn("- [ ] 锁定主推款", md)

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

    def test_download_draft_omits_category_mix_excerpt(self) -> None:
        md = build_strategy_draft_markdown(
            job_id=1,
            keyword="K",
            brief={
                "schema_version": 1,
                "batch_label": "b",
                "category_mix_top": [{"label": "粗粮饼干", "count": 11}],
            },
            for_llm_input=False,
        )
        self.assertNotIn("类目结构（摘录）", md)
        self.assertNotIn("粗粮饼干", md)

    def test_section_five_omits_concentration_excerpt_block(self) -> None:
        """规则骨架不再含「对比对象（摘录）」及店铺/品牌集中度铺陈。"""
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
        self.assertNotIn("对比对象（摘录）", md)
        self.assertNotIn("店铺分布（「饼干」内样本 SKU）", md)
        self.assertNotIn("碧翠园京东自营旗舰店", md)
        self.assertNotIn("23.8%", md)
        self.assertIn("### 5.1 差异化方向", md)
        self.assertIn("### 5.2 竞争应对", md)

    def test_concentration_not_rendered_even_with_unique_sku_basis(self) -> None:
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
        self.assertNotIn("按去重 SKU", md)
        self.assertNotIn("120", md)

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
        self.assertIn("促销线索须与摘要中的价格/活动信号一致", md)

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

    def test_strategy_decisions_full_lowgi_biscuit_fixture(self) -> None:
        from pipeline.demos.run_strategy_decisions_full_fixture_demo import load_full_fixture
        from pipeline.llm.generate_strategy import strategy_decisions_substantive
        from pipeline.reporting.strategy_draft import build_strategy_draft_markdown

        sd = load_full_fixture()
        self.assertTrue(strategy_decisions_substantive(sd))
        self.assertIn("tactic_promotion", sd)
        self.assertIn("满 99", (sd.get("tactic_promotion") or ""))
        brief = {
            "schema_version": 1,
            "keyword": "低GI饼干",
            "batch_label": "demo_fixture",
            "scope": {"merged_sku_count": 2},
            "strategy_hints": ["fixture 联调"],
            "meta": {"page_start": 1, "page_to": 3, "max_skus_config": 100},
            "category_mix_top": [
                {"label": "粗粮饼干", "count": 11},
            ],
            "pc_search_raw": {"result_count_consensus": 100000},
            "price_stats": {"n": 10, "min": 1, "max": 99, "median": 30},
        }
        md = build_strategy_draft_markdown(
            job_id=0,
            keyword="低GI饼干",
            brief=brief,
            business_notes="",
            generated_at_iso="2026-01-01T00:00:00+00:00",
            strategy_decisions=sd,
            for_llm_input=False,
        )
        self.assertIn("表单促销策略", md)
        self.assertIn("满 99", md)
        self.assertIn("卡位监测中位", md)

    def test_strategy_decision_field_names_match_strategy_draft_serializer(self) -> None:
        from pipeline.serializers import StrategyDraftRequestSerializer
        from pipeline.strategy_decision_keys import (
            STRATEGY_DECISION_FIELD_NAMES,
            STRATEGY_DRAFT_POST_NON_DECISION_FIELD_NAMES,
        )

        ser = StrategyDraftRequestSerializer()
        names = set(ser.fields.keys()) - STRATEGY_DRAFT_POST_NON_DECISION_FIELD_NAMES
        self.assertEqual(
            names,
            set(STRATEGY_DECISION_FIELD_NAMES),
            "序列化器与 strategy_decision_keys 不一致：增删字段时须同时改 strategy_decision_keys 与 demo fixture",
        )

    def test_empty_strategy_decisions_has_all_21_keys(self) -> None:
        from pipeline.strategy_decision_keys import (
            STRATEGY_DECISION_FIELD_NAMES,
            build_strategy_decisions_dict,
        )

        d = build_strategy_decisions_dict({})
        self.assertEqual(len(STRATEGY_DECISION_FIELD_NAMES), 21)
        self.assertEqual(set(d.keys()), set(STRATEGY_DECISION_FIELD_NAMES))
        self.assertEqual(d["tactic_promotion"], "")
        self.assertFalse(d["ack_risk_keywords"])

    def test_strategy_draft_request_full_fixture_matches_serializer(self) -> None:
        """``strategy_draft_request_full_*.json`` 含 HTTP POST 全字段（含 generator / 矩阵作用域等）。"""
        from pipeline.serializers import StrategyDraftRequestSerializer
        from pipeline.strategy_decision_keys import build_strategy_decisions_dict

        root = Path(__file__).resolve().parent.parent
        p = root / "demos" / "fixtures" / "strategy_draft_request_full_lowgi_biscuit.json"
        self.assertTrue(p.is_file(), f"缺少固定样例: {p}")
        data = json.loads(p.read_text(encoding="utf-8"))
        ser = StrategyDraftRequestSerializer(data=data)
        self.assertTrue(ser.is_valid(), ser.errors)
        sd = build_strategy_decisions_dict(ser.validated_data)
        only = {k: v for k, v in data.items() if k in sd}
        self.assertEqual(sd, only)
