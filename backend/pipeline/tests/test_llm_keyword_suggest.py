"""llm_keyword_suggest 解析与数据结构（不调用真实 LLM）。"""
from __future__ import annotations

import unittest

from pipeline.llm_keyword_suggest import _parse_phrases_object, _parse_scenarios_object


class ParsePhrasesTests(unittest.TestCase):
    def test_json_object(self) -> None:
        raw = '{"phrases": ["口感", " 回购  "]}'
        self.assertEqual(_parse_phrases_object(raw), ["口感", "回购"])

    def test_fenced_json(self) -> None:
        raw = '```json\n{"phrases": ["低糖"]}\n```'
        self.assertEqual(_parse_phrases_object(raw), ["低糖"])


class ParseScenariosTests(unittest.TestCase):
    def test_min_triggers_in_parser(self) -> None:
        raw = '{"scenarios": [{"label": "早餐", "triggers": ["早上"]}]}'
        out = _parse_scenarios_object(raw)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["label"], "早餐")
        self.assertEqual(out[0]["triggers"], ["早上"])

    def test_fenced(self) -> None:
        raw = '```\n{"scenarios": [{"label": "露营", "triggers": ["户外", "野餐"]}]}\n```'
        out = _parse_scenarios_object(raw)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["label"], "露营")


if __name__ == "__main__":
    unittest.main()
