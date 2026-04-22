"""商详包：两步 JSON LLM 与解析。"""
from __future__ import annotations

import json
from unittest.mock import patch

from django.test import SimpleTestCase

from pipeline.llm.generate_marketing_detail import (
    _parse_llm_json,
    generate_marketing_detail_pack,
)


class MarketingDetailPackTests(SimpleTestCase):
    def test_parse_llm_json_strips_wrapped(self) -> None:
        raw = '前缀 {"a": 1, "b": "x"} 后缀'
        d = _parse_llm_json(raw)
        self.assertEqual(d, {"a": 1, "b": "x"})

    def test_parse_llm_json_rejects_non_object(self) -> None:
        with self.assertRaises(ValueError):
            _parse_llm_json("[1,2]")

    def test_generate_marketing_detail_pack_two_calls(self) -> None:
        core = {
            "one_liner_value": "低负担早餐选择",
            "buyer_job_to_be_done": "控糖加餐",
            "key_pain_or_desire": "怕升糖",
            "why_this_product": "配方可核对",
            "proof_or_trust_angle": "输入未体现",
            "differentiation_vs_alternatives": "同价带更少添加糖",
            "price_value_framing": "中端",
            "compliance_taboos": "不涉疗效",
            "open_points_for_business": "",
        }
        pack = {
            "listing_titles": ["标题A", "标题B"],
            "listing_subtitle": "副文案",
            "detail_headline": "首屏一句",
            "selling_bullets": ["卖点1"],
            "spec_sidebar_lines": [],
            "faq": [{"question": "是否低GI？", "answer": "以包装与检测为准。"}],
        }
        with patch(
            "pipeline.llm.generate_marketing_detail.call_llm",
            side_effect=[json.dumps(core, ensure_ascii=False), json.dumps(pack, ensure_ascii=False)],
        ):
            out = generate_marketing_detail_pack(
                keyword="低GI",
                strategy_markdown="# 策略\n\n- 要点",
                strategy_decisions={"product_role": "新品"},
                business_notes="",
            )
        self.assertEqual(out["core_info_card"]["one_liner_value"], "低负担早餐选择")
        self.assertEqual(out["detail_page_pack"]["listing_titles"][0], "标题A")
