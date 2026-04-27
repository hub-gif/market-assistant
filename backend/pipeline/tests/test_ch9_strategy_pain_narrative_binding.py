"""
第九章「策略与机会」与痛点叙事的单测对齐（**不修改** runner / jd_report 等生产链路）。

背景：当前流水线里 ``llm_sentiment_md`` 未传入 ``generate_strategy_opportunities_llm``。
若产品上要「策略与痛点叙事强绑定」，需要在编排层把 8.3 等节选并入 ``chapter_llm_narratives``；
本文件仅在**单测**中演示：直接向 ``generate_strategy_opportunities_llm`` 传入含痛点锚点的节选，
并断言 **发给大模型的 user JSON** 中原样携带该锚点（与 ``STRATEGY_OPPORTUNITIES_SYSTEM`` 中
「转化与体验须呼应 sec8_3_*」的约定一致）。

真机产出是否复述痛点，属模型行为；此处只测**输入契约**强绑定。
"""
from __future__ import annotations

import json
from unittest.mock import patch

from django.test import SimpleTestCase

from pipeline.llm.generate_strategy import generate_strategy_opportunities_llm


def _parse_strategy_user_json(user_prompt: str) -> dict[str, object]:
    """``STRATEGY_OPPORTUNITIES_USER_PREFIX`` 后为单行或多行 JSON。"""
    i = user_prompt.find("{")
    assert i >= 0, "user_prompt 中应有 JSON 对象"
    return json.loads(user_prompt[i:])


def _minimal_brief() -> dict:
    """供 ``compact_brief_for_llm`` 的最小合法 competitor_brief。"""
    return {
        "schema_version": 1,
        "keyword": "单测词",
        "batch_label": "test-batch",
        "scope": {
            "merged_sku_count": 1,
            "comment_flat_rows": 3,
            "structure_source_rows": 5,
            "uses_pc_search_list_export": False,
            "category_mix_source": "keyword_pipeline_merged",
            "category_mix_valid_matrix_sku_count": 1,
        },
        "matrix_by_group": [],
        "consumer_feedback_by_matrix_group": [],
        "notes": [],
    }


class Ch9StrategyPainNarrativeBindingTests(SimpleTestCase):
    """痛点叙事通过 ``prior_chapter_llm_narratives`` 进入第九章请求体。"""

    _ANCHOR = "PAIN_ANCHOR_CH9_BINDING_TEST_7f3a"

    def _fake_llm(self, captured: dict[str, str]):
        def _fn(system_prompt: str, user_prompt: str, **kwargs) -> str:
            captured["user"] = user_prompt
            return (
                "#### 定价与价带\n假设：待验证。\n\n"
                "#### 差异化与应对齐的优势\n假设：待验证。\n\n"
                "#### 风险与避免项\n假设：待验证。\n\n"
                "#### 促销与活动机制\n输入未体现。\n\n"
                "#### 转化与体验\n假设：待验证。\n"
            )

        return _fn

    def test_sec8_3_text_mining_probe_narrative_carries_pain_anchor_in_user_json(
        self,
    ) -> None:
        """系统提示要求转化与体验呼应 ``sec8_3_text_mining_probe``；节选须进入请求 JSON。"""
        captured: dict[str, str] = {}
        narratives = {
            "sec8_3_text_mining_probe": (
                "#### 饼干\n"
                f"负向体验归纳（单测锚点）：用户集中抱怨「口感发干、保质期偏短」。锚点标记 {self._ANCHOR}。"
            ),
        }
        with patch(
            "pipeline.llm.generate_strategy.call_llm",
            side_effect=self._fake_llm(captured),
        ):
            out = generate_strategy_opportunities_llm(
                _minimal_brief(),
                keyword="单测词",
                chapter_llm_narratives=narratives,
            )
        self.assertIn("转化与体验", out)
        user = captured.get("user", "")
        self.assertIn(self._ANCHOR, user)
        obj = _parse_strategy_user_json(user)
        narr = obj.get("prior_chapter_llm_narratives") or {}
        self.assertIn(self._ANCHOR, narr.get("sec8_3_text_mining_probe", ""))

    def test_sec8_3_comment_focus_summaries_carries_pain_anchor_in_user_json(
        self,
    ) -> None:
        """与探针二选一时的第八章节选键；同样须进入请求 JSON。"""
        captured: dict[str, str] = {}
        narratives = {
            "sec8_3_comment_focus_summaries": (
                f"细类评论要点：复购障碍与「漏发」相关讨论较多。锚点 {self._ANCHOR}。"
            ),
        }
        with patch(
            "pipeline.llm.generate_strategy.call_llm",
            side_effect=self._fake_llm(captured),
        ):
            generate_strategy_opportunities_llm(
                _minimal_brief(),
                keyword="单测词",
                chapter_llm_narratives=narratives,
            )
        user = captured.get("user", "")
        self.assertIn(self._ANCHOR, user)
        obj = _parse_strategy_user_json(user)
        narr = obj.get("prior_chapter_llm_narratives") or {}
        self.assertIn(self._ANCHOR, narr.get("sec8_3_comment_focus_summaries", ""))

    def test_extra_narrative_key_sec8_sentiment_passed_through_for_alignment(
        self,
    ) -> None:
        """
        ``generate_strategy_opportunities_llm`` 会把 ``chapter_llm_narratives`` 中
        所有非空字符串键并入 ``prior_chapter_llm_narratives``（无白名单过滤）。
        单测层可用额外键（如模拟 8.3 全文节选）与系统提示「与各键定性主题方向一致」形成契约；
        生产是否增加该键仅影响编排，不需改本函数签名。
        """
        captured: dict[str, str] = {}
        narratives = {
            "sec8_3_text_mining_probe": "探针摘要略。",
            "sec8_3_comment_sentiment_themes": (
                f"#### 饼干\n负向主题：配送挤压导致碎裂。锚点 {self._ANCHOR}。"
            ),
        }
        with patch(
            "pipeline.llm.generate_strategy.call_llm",
            side_effect=self._fake_llm(captured),
        ):
            generate_strategy_opportunities_llm(
                _minimal_brief(),
                keyword="单测词",
                chapter_llm_narratives=narratives,
            )
        obj = _parse_strategy_user_json(captured["user"])
        narr = obj.get("prior_chapter_llm_narratives") or {}
        self.assertIn("sec8_3_comment_sentiment_themes", narr)
        self.assertIn(self._ANCHOR, narr["sec8_3_comment_sentiment_themes"])
        # 截断后锚点仍在（锚点放在短文首段即可）
        self.assertLess(len(narr["sec8_3_comment_sentiment_themes"]), 5000)
