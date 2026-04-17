"""§8 末「细类评论与关注词要点归纳」大模型：单细类烟测（mock 网关，不调真实 API）。"""
from __future__ import annotations

import json
from unittest.mock import patch

from django.test import SimpleTestCase

from pipeline.llm.generate import (
    generate_comment_group_summaries_llm,
    generate_comment_group_summaries_llm_chunked,
)


def _load_jcr():
    from pipeline.competitor_report import jd_report as jcr  # noqa: WPS433

    return jcr


def _payload_single_category_biscuit() -> tuple[list[dict], str, str]:
    """合并表 + 评价仅对应一个矩阵细类「饼干」。"""
    jcr = _load_jcr()
    sku_h = "SKU(skuId)"
    title_h = "标题(wareName)"
    merged = [
        {
            sku_h: "111",
            "detail_category_path": "食品饮料 > 休闲食品 > 饼干 > 粗粮饼干",
            title_h: "测试饼干",
            "detail_shop_name": "测试店",
        },
    ]
    comments = [
        {
            "sku": "111",
            "tagCommentContent": "口感不错，低GI很适合控糖",
        },
    ]
    fb = jcr._consumer_feedback_by_matrix_group(
        merged_rows=merged,
        comment_rows=comments,
        sku_header=sku_h,
    )
    pl = jcr.build_comment_groups_llm_payload(
        feedback_groups=fb,
        focus_words=jcr.COMMENT_FOCUS_WORDS,
        merged_rows=merged,
        sku_header=sku_h,
        title_h=title_h,
    )
    return pl, sku_h, title_h


class CommentGroupSummariesLlmTests(SimpleTestCase):
    def test_single_matrix_group_payload_and_llm_smoke(self) -> None:
        pl, _, _ = _payload_single_category_biscuit()
        self.assertEqual(len(pl), 1)
        self.assertEqual(pl[0].get("group"), "饼干")
        self.assertIn("sample_text_snippets", pl[0])
        self.assertTrue(any("饼干" in s for s in pl[0]["sample_text_snippets"]))

        with patch(
            "pipeline.llm.generate_group_summaries.call_llm",
            return_value="#### 饼干\n单测归纳段落。",
        ) as mock_llm:
            out = generate_comment_group_summaries_llm(pl, keyword="低GI测试")

        mock_llm.assert_called_once()
        _sys, user = mock_llm.call_args[0]
        self.assertIn("细类评论与关注词要点归纳", user)
        self.assertIn("低GI测试", user)
        raw = user.split("正文（Markdown）。\n\n", 1)[-1]
        data = json.loads(raw.strip())
        self.assertEqual(data["keyword"], "低GI测试")
        self.assertEqual(len(data["groups"]), 1)
        self.assertEqual(data["groups"][0]["group"], "饼干")
        self.assertIn("饼干", out)

    def test_chunked_single_category_one_gateway_call(self) -> None:
        """与生产「按细类拆分」一致：仅一个细类时只请求一次。"""
        pl, _, _ = _payload_single_category_biscuit()
        with patch(
            "pipeline.llm.generate_group_summaries.call_llm",
            return_value="#### 饼干\nchunked。",
        ) as mock_llm:
            out = generate_comment_group_summaries_llm_chunked(pl, keyword="低GI测试")

        self.assertEqual(mock_llm.call_count, 1)
        self.assertIn("饼干", out)
