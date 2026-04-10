# -*- coding: utf-8 -*-
"""
根据 ``brief_content.txt`` 4.1 / 4.2 应用场景，对京东搜索导出行做关键词保留筛选。

- **保留**：标题/卖点/类目/规格中至少命中一类场景词（中式面点主食或烘焙）。
- **剔除**：未命中任一类（如饮料、保健品、与面点/烘焙无关的零食等）。

说明：基于展示文案的规则匹配，边界案例需人工复核；可随时扩充 ``_KW_41`` / ``_KW_42``。
"""

from __future__ import annotations

from typing import Any

# 与 CSV 导出列名一致（jd_h5_search_requests.CSV_FIELDS 子集）
_SCENARIO_TEXT_FIELDS: tuple[str, ...] = (
    "标题(wareName)",
    "卖点(sellingPoint)",
    "类目(leafCategory,cid3Name,catid)",
    "规格属性(propertyList,color,catid,shortName)",
)

# 4.1 中式（米）面点及主食（含常见同义/细分）
_KW_41: tuple[str, ...] = (
    "包子",
    "馒头",
    "花卷",
    "饺子",
    "饺皮",
    "饺子皮",
    "水饺",
    "蒸饺",
    "锅贴",
    "馄饨",
    "云吞",
    "抄手",
    "面条",
    "挂面",
    "拉面",
    "刀削面",
    "凉面",
    "冷面",
    "热干面",
    "意面",
    "方便面",
    "泡面",
    "速食面",
    "米粉",
    "米线",
    "河粉",
    "粉丝",
    "螺蛳粉",
    "米糕",
    "年糕",
    "糍粑",
    "发糕",
    "重组米",
    "重组大米",
    "大米",
    "米饭",
    "杂粮饭",
    "白米饭",
    "自热米饭",
    "煲仔饭",
    "烧麦",
    "烧卖",
    "面皮",
    "春卷",
    "手抓饼",
    "葱油饼",
    "馅饼",
    "烧饼",
    "油条",
    "窝头",
    "窝窝头",
    "荞麦面",
    "青稞",
    "全麦面条",
)

# 4.2 烘焙（含与 brief 一致的低温慢烤饼干等）
_KW_42: tuple[str, ...] = (
    "面包",
    "吐司",
    "列巴",
    "欧包",
    "贝果",
    "可颂",
    "牛角",
    "蛋糕",
    "糕点",
    "饼干",
    "曲奇",
    "烘焙",
    "酥饼",
    "桃酥",
    "威化",
    "华夫",
    "司康",
    "蛋挞",
    "月饼",
    "酥性",
    "苏打饼干",
    "全麦面包",
    "手撕面包",
)


def scenario_row_text(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in _SCENARIO_TEXT_FIELDS:
        parts.append(str(row.get(k) or ""))
    return "\n".join(parts)


def row_scenario_match(row: dict[str, Any]) -> tuple[bool, str]:
    """
    是否命中应用场景；第二个返回值为标签 ``4.1`` / ``4.2`` / ``4.1+4.2`` / ````。
    """
    text = scenario_row_text(row)
    hit41 = any(kw in text for kw in _KW_41)
    hit42 = any(kw in text for kw in _KW_42)
    if hit41 and hit42:
        return True, "4.1+4.2"
    if hit41:
        return True, "4.1"
    if hit42:
        return True, "4.2"
    return False, ""


def filter_rows_by_scenario(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    保持原顺序，仅保留命中 4.1 或 4.2 的行。
    返回 (filtered_rows, stats)。
    """
    kept: list[dict[str, Any]] = []
    tag_counts: dict[str, int] = {}
    for r in rows:
        ok, tag = row_scenario_match(r)
        if ok:
            kept.append(r)
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    stats: dict[str, Any] = {
        "input_rows": len(rows),
        "kept_rows": len(kept),
        "dropped_rows": len(rows) - len(kept),
        "tag_counts": tag_counts,
    }
    return kept, stats
