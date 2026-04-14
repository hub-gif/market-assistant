"""llm_keyword_suggest：分块/解析烟测；有 API 配置时直连大模型做联调。"""
from __future__ import annotations

import os
import unittest
from pathlib import Path

try:
    from dotenv import load_dotenv

    _ma_env = Path(__file__).resolve().parents[3] / ".env"
    if _ma_env.is_file():
        load_dotenv(_ma_env)
except ImportError:
    pass

from django.test import SimpleTestCase

from pipeline.llm_keyword_suggest import (
    MAX_CHUNK_CHARS,
    MAX_CHUNKS,
    _chunk_comment_texts,
    _parse_phrases_object,
    _parse_scenarios_object,
    suggest_focus_keywords_from_all_comments,
    suggest_scenario_groups_llm,
)


def _llm_configured() -> bool:
    key = (os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY") or "").strip()
    base = (os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL") or "").strip()
    return bool(key and base)


class ChunkCommentTextsTests(SimpleTestCase):
    def test_empty(self) -> None:
        self.assertEqual(_chunk_comment_texts([]), [])

    def test_respects_max_chunk_chars(self) -> None:
        a = "x" * (MAX_CHUNK_CHARS // 2)
        b = "y" * (MAX_CHUNK_CHARS // 2)
        c = "z" * (MAX_CHUNK_CHARS // 2)
        parts = _chunk_comment_texts([a, b, c])
        self.assertGreaterEqual(len(parts), 2)
        for p in parts:
            self.assertLessEqual(len(p) + p.count("\n"), MAX_CHUNK_CHARS + 50)

    def test_max_chunks_trims(self) -> None:
        texts = [f"段落{i} " + "字" * 800 for i in range(80)]
        parts = _chunk_comment_texts(texts)
        self.assertLessEqual(len(parts), MAX_CHUNKS)


class ParsePhrasesObjectTests(SimpleTestCase):
    def test_plain_json(self) -> None:
        raw = '{"phrases": ["低糖", "口感好"]}'
        self.assertEqual(_parse_phrases_object(raw), ["低糖", "口感好"])

    def test_fenced_json(self) -> None:
        raw = '```json\n{"phrases": ["A", "B"]}\n```'
        self.assertEqual(_parse_phrases_object(raw), ["A", "B"])

    def test_embedded_object(self) -> None:
        raw = '前缀 {"phrases": ["x"]} 后缀'
        self.assertEqual(_parse_phrases_object(raw), ["x"])

    def test_invalid_returns_empty(self) -> None:
        self.assertEqual(_parse_phrases_object("not json"), [])


class ParseScenariosObjectTests(SimpleTestCase):
    def test_plain_json(self) -> None:
        raw = '{"scenarios": [{"label": "下午茶", "triggers": ["下午茶", "配咖啡"]}]}'
        out = _parse_scenarios_object(raw)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["label"], "下午茶")
        self.assertEqual(out[0]["triggers"], ["下午茶", "配咖啡"])

    def test_fenced(self) -> None:
        raw = '```json\n{"scenarios": [{"label": "A", "triggers": ["触发甲", "触发乙"]}]}\n```'
        out = _parse_scenarios_object(raw)
        self.assertEqual(out[0]["label"], "A")
        self.assertEqual(out[0]["triggers"], ["触发甲", "触发乙"])


class SuggestFocusKeywordsTests(SimpleTestCase):
    def test_no_comments_returns_empty(self) -> None:
        out = suggest_focus_keywords_from_all_comments(
            keyword="低GI",
            brief_slice={"comment_focus_keywords": []},
            all_comment_texts=[],
        )
        self.assertEqual(out["suggested_focus_keywords"], [])
        self.assertEqual(out["chunks_processed"], 0)
        self.assertIn("无评价", out["rationale"])


@unittest.skipUnless(
    _llm_configured(),
    "需要环境变量 OPENAI_API_KEY+OPENAI_BASE_URL（或 LLM_API_KEY+LLM_BASE_URL），"
    "与 AI_crawler 相同；可在 market_assistant/.env 配置后重跑。",
)
class SuggestFocusKeywordsLiveLLMTests(SimpleTestCase):
    """直连网关调用 ``chat_completion_text``，会消耗少量 token。"""

    def test_live_extracts_phrases_from_comments(self) -> None:
        comments = [
            "低GI饼干口感偏硬，甜度刚好，饱腹感不错。",
            "物流有点慢，包装压扁了一角，但味道还行。",
            "希望出小包装，一次吃不完容易受潮。",
        ]
        out = suggest_focus_keywords_from_all_comments(
            keyword="低GI饼干",
            brief_slice={"comment_focus_keywords": [{"word": "甜度"}]},
            all_comment_texts=comments,
        )
        self.assertGreaterEqual(out["chunks_processed"], 1)
        self.assertEqual(out["total_comment_texts"], 3)
        kws = out["suggested_focus_keywords"]
        self.assertIsInstance(kws, list)
        self.assertGreater(len(kws), 0, "模型应返回至少 1 条短语")
        for p in kws:
            self.assertIsInstance(p, str)
            self.assertGreaterEqual(len(p), 2)
            self.assertLessEqual(len(p), 24)
        self.assertNotIn("甜度", kws)


@unittest.skipUnless(
    _llm_configured(),
    "需要 OPENAI_* 或 LLM_* 密钥与网关地址。",
)
class SuggestScenarioGroupsLiveLLMTests(SimpleTestCase):
    def test_live_suggests_new_scenario_groups(self) -> None:
        existing = [
            {"label": "早餐/代餐", "triggers": ["早餐", "代餐"]},
        ]
        comments = [
            "下午配咖啡当下午茶还不错，办公室同事分着吃。",
            "周末露营带了一盒，孩子当零食。",
        ]
        out = suggest_scenario_groups_llm(
            keyword="饼干",
            existing_groups=existing,
            all_comment_texts=comments,
        )
        groups = out.get("suggested_scenario_groups") or []
        self.assertIsInstance(groups, list)
        self.assertGreater(len(groups), 0, "应至少返回 1 组新场景")
        labels = {str(g.get("label", "")).strip().lower() for g in groups if isinstance(g, dict)}
        self.assertNotIn("早餐/代餐", labels)
        for g in groups:
            tr = g.get("triggers") or []
            self.assertGreaterEqual(len(tr), 1)
