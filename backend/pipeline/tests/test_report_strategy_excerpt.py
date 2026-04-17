"""``report_strategy_excerpt``：第九章正文加载与截取。"""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.reporting.report_strategy_excerpt import (
    CHAPTER_NINE_HEADING,
    extract_chapter_nine_strategy_markdown,
    load_report_strategy_excerpt,
)


def test_extract_chapter_nine_stops_before_appendix() -> None:
    md = f"""# x

{CHAPTER_NINE_HEADING}（假设清单，待验证）

正文一段。

---

## 附录 A：数据留存说明

尾部。
"""
    out = extract_chapter_nine_strategy_markdown(md)
    assert CHAPTER_NINE_HEADING in out
    assert "正文一段" in out
    assert "附录" not in out
    assert "尾部" not in out


def test_load_prefers_json_markdown(tmp_path: Path) -> None:
    (tmp_path / "strategy_opportunities_llm.json").write_text(
        json.dumps(
            {"schema_version": 1, "ok": True, "markdown": "JSON 内第九章正文"},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (tmp_path / "competitor_analysis.md").write_text(
        f"{CHAPTER_NINE_HEADING}\n\n从 MD 来\n\n## 附录 A\n",
        encoding="utf-8",
    )
    text, src = load_report_strategy_excerpt(tmp_path)
    assert src == "json_markdown"
    assert "JSON 内第九章" in text


def test_load_falls_back_to_competitor_md(tmp_path: Path) -> None:
    (tmp_path / "strategy_opportunities_llm.json").write_text(
        json.dumps({"schema_version": 1, "ok": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tmp_path / "competitor_analysis.md").write_text(
        f"{CHAPTER_NINE_HEADING}（假设清单，待验证）\n\n从 MD 截取。\n\n## 附录 A\n",
        encoding="utf-8",
    )
    text, src = load_report_strategy_excerpt(tmp_path)
    assert src == "competitor_analysis_md"
    assert "从 MD 截取" in text


def test_load_none_when_missing(tmp_path: Path) -> None:
    text, src = load_report_strategy_excerpt(tmp_path)
    assert src == "none"
    assert text == ""
