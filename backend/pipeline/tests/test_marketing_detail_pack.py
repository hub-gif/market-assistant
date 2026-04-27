"""营销内容包：两步 JSON LLM 与解析。"""
from __future__ import annotations

import json
import tempfile
from unittest.mock import patch

from django.test import SimpleTestCase

from pipeline.llm.generate_marketing_detail import (
    _parse_llm_json,
    generate_marketing_detail_pack,
    normalize_detail_page_pack,
)
from pipeline.reporting.marketing_pack_persist import persist_marketing_detail_pack_v1


class MarketingDetailPackTests(SimpleTestCase):
    def test_normalize_fills_missing_keys_including_aigc(self) -> None:
        """旧结果或模型漏键时补齐文生图/文生视频等字段。"""
        slim = {
            "traceability_note": "x",
            "main_image_three_points": ["a", "b", "c"],
            "live_or_short_hook": "h",
            "customer_service_opening": "c",
        }
        n = normalize_detail_page_pack(slim)
        self.assertEqual(n["text_to_image_prompt_main"], "")
        self.assertEqual(n["text_to_image_prompt_scene"], "")
        self.assertEqual(n["text_to_video_prompt"], "")
        self.assertEqual(n["listing_titles"], [])
        self.assertEqual(n["detail_mid_story_paragraphs"], [])
        self.assertEqual(n["live_script_bullets"], [])
        self.assertEqual(n["traceability_note"], "x")

    def test_parse_llm_json_strips_wrapped(self) -> None:
        raw = '前缀 {"a": 1, "b": "x"} 后缀'
        d = _parse_llm_json(raw)
        self.assertEqual(d, {"a": 1, "b": "x"})

    def test_parse_llm_json_rejects_non_object(self) -> None:
        with self.assertRaises(ValueError):
            _parse_llm_json("[1,2]")

    def test_generate_marketing_detail_pack_two_calls(self) -> None:
        core = {
            "what_we_sell": "低GI方向早餐饼干（监测关键词语境，待业务定主推款）",
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
            "traceability_note": "与策略方向一致；具体宣称以包装为准。",
            "main_image_three_points": ["低GI早餐", "独立小包", "配料可核对"],
            "live_or_short_hook": "控糖早餐怎么选？先看配料表。",
            "customer_service_opening": "您好，这款饼干适合关注血糖管理的早餐场景，需要我帮您对比规格吗？",
            "text_to_image_prompt_main": "电商主图，白底，居中摆放一盘低GI早餐饼干，柔和棚拍光，写实，无品牌logo，健康清爽风格",
            "text_to_image_prompt_scene": "早餐餐桌场景，牛奶与饼干搭配，自然窗光，生活感，无文字贴片",
            "text_to_video_prompt": "竖屏9:16，5秒：从俯拍早餐桌缓慢推近至饼干包装，无对白，干净色调，无疗效字幕",
            "detail_mid_story_paragraphs": ["段落一适合控糖早餐。", "段落二配料可核对。"],
            "usage_and_pairing_tips": ["配无糖酸奶", "开封后密封防潮"],
            "short_graphic_post_variants": ["低GI早餐饼干，小包装方便。"],
            "live_script_bullets": ["大家好看配料表", "独立小包控量", "适合加餐"],
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
        self.assertIn("竖屏", out["detail_page_pack"]["text_to_video_prompt"])

    def test_persist_marketing_detail_pack_v1_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload = {"schema_version": 1, "job_id": 1, "x": "y"}
            path = persist_marketing_detail_pack_v1(td, payload)
            self.assertIsNotNone(path)
            assert path is not None
            self.assertTrue(path.is_file())
            self.assertEqual(path.parent.name, "marketing")
            self.assertIn("marketing_detail_pack_v1.json", path.name)
            loaded = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(loaded["job_id"], 1)
