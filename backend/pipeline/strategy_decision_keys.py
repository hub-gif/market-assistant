"""
策略制定表单：写入 ``strategy_decisions`` 的字段名（与 ``StrategyDraftRequestSerializer`` 对应项一致）。

不含 ``business_notes``、``generator``、``strategy_matrix_group*``（由接口另字段承载）。
"""
from __future__ import annotations

from typing import Any

# 与 ``JobStrategyDraftView`` 中 ``strategy_decisions`` 字符串/选项列一致
STRATEGY_DECISION_TEXT_FIELD_NAMES: tuple[str, ...] = (
    "product_role",
    "stage_goal_type",
    "time_horizon",
    "success_criteria",
    "non_goals",
    "battlefield_one_line",
    "positioning_choice",
    "competitive_stance",
    "pillar_product",
    "pillar_price",
    "pillar_channel",
    "pillar_comm",
    "tactic_promotion",
    "audience_segment",
    "competitor_reference",
    "resource_notes",
    "marketing_strategy",
    "general_strategy",
)

STRATEGY_DECISION_BOOL_FIELD_NAMES: tuple[str, ...] = (
    "ack_risk_keywords",
    "ack_risk_price",
    "ack_risk_concentration",
)

STRATEGY_DECISION_FIELD_NAMES: tuple[str, ...] = (
    *STRATEGY_DECISION_TEXT_FIELD_NAMES,
    *STRATEGY_DECISION_BOOL_FIELD_NAMES,
)

# POST 中不并入 ``strategy_decisions``、但与策略制定请求一并提交的字段
STRATEGY_DRAFT_POST_NON_DECISION_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "business_notes",
        "generator",
        "strategy_matrix_group",
        "strategy_matrix_group_index",
    }
)


def build_strategy_decisions_dict(validated: dict[str, Any]) -> dict[str, Any]:
    """由 ``StrategyDraftRequestSerializer`` 的 ``validated_data`` 组装与线上一致的 ``strategy_decisions``。"""
    out: dict[str, Any] = {}
    for k in STRATEGY_DECISION_TEXT_FIELD_NAMES:
        out[k] = validated.get(k) or ""
    for k in STRATEGY_DECISION_BOOL_FIELD_NAMES:
        out[k] = bool(validated.get(k))
    return out


def empty_strategy_decisions() -> dict[str, Any]:
    return build_strategy_decisions_dict({})
