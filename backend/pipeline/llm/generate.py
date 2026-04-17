"""
竞品报告 / 策略稿的**大模型生成**：通过 ``crawler_copy/jd_pc_search/AI_crawler`` 的
``chat_completion_text`` 调用，与配料识别共用网关与密钥。

实现已拆分为子模块（``llm_client``、``generate_*``），本模块保留对外符号以兼容
``from pipeline.llm.generate import …`` 与测试中的 patch 路径。
"""
from __future__ import annotations

from .generate_competitor_full import (
    REPORT_SYSTEM,
    REPORT_USER_PREFIX,
    generate_competitor_report_markdown_llm,
)
from .generate_group_summaries import (
    COMMENT_GROUPS_SYSTEM,
    COMMENT_GROUPS_USER_PREFIX,
    MATRIX_GROUPS_SYSTEM,
    MATRIX_GROUPS_USER_PREFIX,
    PRICE_GROUPS_SYSTEM,
    PRICE_GROUPS_USER_PREFIX,
    PROMO_GROUPS_SYSTEM,
    PROMO_GROUPS_USER_PREFIX,
    SCENARIO_GROUPS_SYSTEM,
    SCENARIO_GROUPS_USER_PREFIX,
    _join_chunked_group_markdown,
    generate_comment_group_summaries_llm,
    generate_comment_group_summaries_llm_chunked,
    generate_matrix_group_summaries_llm,
    generate_matrix_group_summaries_llm_chunked,
    generate_price_group_summaries_llm,
    generate_price_group_summaries_llm_chunked,
    generate_promo_group_summaries_llm,
    generate_promo_group_summaries_llm_chunked,
    generate_scenario_group_summaries_llm,
    generate_scenario_group_summaries_llm_chunked,
)
from .generate_sections import (
    BRIDGE_SECTIONS_SYSTEM,
    SENTIMENT_LLM_SYSTEM,
    generate_comment_sentiment_analysis_llm,
    generate_section_bridges_llm,
    split_competitor_report_for_bridges,
)
from .generate_strategy import (
    STRATEGY_OPPORTUNITIES_SYSTEM,
    STRATEGY_OPPORTUNITIES_USER_PREFIX,
    STRATEGY_SYSTEM,
    STRATEGY_USER_PREFIX,
    generate_strategy_draft_markdown_llm,
    generate_strategy_opportunities_llm,
)
from .llm_client import call_llm as _call_llm

__all__ = [
    "BRIDGE_SECTIONS_SYSTEM",
    "COMMENT_GROUPS_SYSTEM",
    "COMMENT_GROUPS_USER_PREFIX",
    "MATRIX_GROUPS_SYSTEM",
    "MATRIX_GROUPS_USER_PREFIX",
    "PRICE_GROUPS_SYSTEM",
    "PRICE_GROUPS_USER_PREFIX",
    "PROMO_GROUPS_SYSTEM",
    "PROMO_GROUPS_USER_PREFIX",
    "REPORT_SYSTEM",
    "REPORT_USER_PREFIX",
    "SCENARIO_GROUPS_SYSTEM",
    "SCENARIO_GROUPS_USER_PREFIX",
    "SENTIMENT_LLM_SYSTEM",
    "STRATEGY_OPPORTUNITIES_SYSTEM",
    "STRATEGY_OPPORTUNITIES_USER_PREFIX",
    "STRATEGY_SYSTEM",
    "STRATEGY_USER_PREFIX",
    "_call_llm",
    "_join_chunked_group_markdown",
    "generate_comment_group_summaries_llm",
    "generate_comment_group_summaries_llm_chunked",
    "generate_comment_sentiment_analysis_llm",
    "generate_competitor_report_markdown_llm",
    "generate_matrix_group_summaries_llm",
    "generate_matrix_group_summaries_llm_chunked",
    "generate_price_group_summaries_llm",
    "generate_price_group_summaries_llm_chunked",
    "generate_promo_group_summaries_llm",
    "generate_promo_group_summaries_llm_chunked",
    "generate_scenario_group_summaries_llm",
    "generate_scenario_group_summaries_llm_chunked",
    "generate_section_bridges_llm",
    "generate_strategy_draft_markdown_llm",
    "generate_strategy_opportunities_llm",
    "split_competitor_report_for_bridges",
]
