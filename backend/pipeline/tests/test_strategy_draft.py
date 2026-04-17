"""市场策略草稿 Markdown（规则，无 LLM）。"""
from __future__ import annotations

from django.test import SimpleTestCase

from pipeline.reporting.strategy_draft import build_strategy_draft_markdown


class StrategyDraftTests(SimpleTestCase):
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
        self.assertIn("目标与边界", md)
        self.assertIn("营销策略与总体方向", md)
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
        self.assertIn("侧翼切入", md)
        self.assertIn("| 产品 | 做低糖配方 |", md)
        self.assertIn("- [x] 关注词/场景统计是否以偏概全", md)
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

    def test_legacy_report_shows_focus_scenario_bullets(self) -> None:
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
        self.assertIn("「口感」：子串统计命中约 **501** 次", md)
        self.assertIn("场景「控糖/血糖相关」：约 **305** 条", md)
