"""按矩阵细类拆分 group 归纳 LLM 请求：拼接与开关行为。"""
from __future__ import annotations

import os
from unittest.mock import patch

from django.test import SimpleTestCase

from pipeline.jd.runner import use_chunked_group_summaries_llm
from pipeline.llm.generate import (
    _join_chunked_group_markdown,
    generate_matrix_group_summaries_llm_chunked,
)


class ChunkedGroupSummariesTests(SimpleTestCase):
    def test_join_strips_and_skips_empty(self) -> None:
        self.assertEqual(
            _join_chunked_group_markdown([" x ", "", "y"]),
            "x\n\ny",
        )

    @patch("pipeline.llm.generate.generate_matrix_group_summaries_llm")
    def test_matrix_chunked_one_call_per_group(self, mock_mx) -> None:
        mock_mx.side_effect = (
            lambda groups, keyword: f"#### {groups[0]['group']}\ntext"
        )
        out = generate_matrix_group_summaries_llm_chunked(
            [{"group": "饼干"}, {"group": "挂面"}],
            keyword="低GI",
        )
        self.assertEqual(mock_mx.call_count, 2)
        self.assertIn("饼干", out)
        self.assertIn("挂面", out)

    def test_use_chunked_respects_bulk_env(self) -> None:
        with patch.dict(os.environ, {"MA_LLM_GROUP_SUMMARIES_BULK": "1"}):
            self.assertFalse(
                use_chunked_group_summaries_llm(
                    {"llm_group_summaries_chunk_by_matrix": True}
                )
            )

    @patch.dict(os.environ, {"MA_LLM_GROUP_SUMMARIES_BULK": ""}, clear=False)
    def test_use_chunked_false_from_config(self) -> None:
        self.assertFalse(
            use_chunked_group_summaries_llm(
                {"llm_group_summaries_chunk_by_matrix": False}
            )
        )

    @patch.dict(os.environ, {"MA_LLM_GROUP_SUMMARIES_BULK": ""}, clear=False)
    def test_use_chunked_default_true(self) -> None:
        self.assertTrue(use_chunked_group_summaries_llm({}))
