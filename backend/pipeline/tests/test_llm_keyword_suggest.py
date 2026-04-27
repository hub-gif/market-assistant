"""llm_keyword_suggest 解析与数据结构（不调用真实 LLM）。"""
from __future__ import annotations

import unittest

from pipeline.llm.keyword_suggest import _parse_phrases_object


class ParsePhrasesTests(unittest.TestCase):
    def test_json_object(self) -> None:
        raw = '{"phrases": ["口感", " 回购  "]}'
        self.assertEqual(_parse_phrases_object(raw), ["口感", "回购"])

    def test_fenced_json(self) -> None:
        raw = '```json\n{"phrases": ["低糖"]}\n```'
        self.assertEqual(_parse_phrases_object(raw), ["低糖"])


if __name__ == "__main__":
    unittest.main()
