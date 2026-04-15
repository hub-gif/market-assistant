"""压缩 competitor-brief 供大模型输入，控制 token 体积（无 Django 依赖）。"""
from __future__ import annotations

import copy
import json
from typing import Any


def matrix_overview_for_llm(brief: dict[str, Any]) -> list[dict[str, Any]]:
    """
    从**完整** brief 提取矩阵分组摘要，供大模型在 matrix 正文被裁剪后仍能写「按细分类目分组」章节。
    """
    mg = brief.get("matrix_by_group")
    if not isinstance(mg, list) or not mg:
        return []
    out: list[dict[str, Any]] = []
    for g in mg:
        if not isinstance(g, dict):
            continue
        name = g.get("group") or "—"
        skus = g.get("skus") if isinstance(g.get("skus"), list) else []
        brands: list[str] = []
        for s in skus[:100]:
            if isinstance(s, dict):
                bb = (s.get("brand") or "").strip()
                if bb and bb not in brands:
                    brands.append(bb)
        out.append(
            {
                "group": name,
                "sku_count": int(g.get("sku_count") or len(skus)),
                "distinct_brands_sample": brands[:15],
            }
        )
    return out


def _trim_matrix(b: dict[str, Any], *, per_group: int, max_groups: int) -> None:
    mg = b.get("matrix_by_group")
    if not isinstance(mg, list):
        return
    trimmed: list[dict[str, Any]] = []
    for g in mg[:max_groups]:
        if not isinstance(g, dict):
            continue
        g2 = dict(g)
        skus = g2.get("skus")
        if isinstance(skus, list):
            if per_group <= 0:
                g2["skus"] = []
            else:
                g2["skus"] = skus[:per_group]
        trimmed.append(g2)
    b["matrix_by_group"] = trimmed


def _trim_feedback(b: dict[str, Any], max_groups: int) -> None:
    cf = b.get("consumer_feedback_by_matrix_group")
    if isinstance(cf, list):
        b["consumer_feedback_by_matrix_group"] = cf[:max_groups]


def _json_len(b: dict[str, Any]) -> int:
    return len(json.dumps(b, ensure_ascii=False))


def compact_brief_for_llm(
    brief: dict[str, Any],
    *,
    max_chars: int = 350_000,
) -> dict[str, Any]:
    """
    深拷贝后裁剪矩阵 SKU 列表、反馈组数；仍超长则逐步收紧直至省略大块。
    始终附带 ``matrix_overview_for_llm``（来自裁剪前完整 brief），避免大模型漏写竞品矩阵章节。
    """
    matrix_ov = matrix_overview_for_llm(brief)
    b = copy.deepcopy(brief)
    if isinstance(b.get("run_dir"), str):
        b["run_dir"] = "(已省略)"

    def _finalize() -> dict[str, Any]:
        b["matrix_overview_for_llm"] = matrix_ov
        return b

    caps = [(120, 24), (80, 20), (40, 16), (30, 12), (18, 10), (10, 8), (5, 6), (0, 6)]
    for per_g, max_gr in caps:
        _trim_matrix(b, per_group=per_g, max_groups=max_gr)
        _trim_feedback(b, max_gr)
        if _json_len(b) <= max_chars:
            return _finalize()

    b.pop("matrix_by_group", None)
    b["matrix_by_group_omitted"] = True
    _trim_feedback(b, 6)
    if _json_len(b) <= max_chars:
        return _finalize()

    b.pop("consumer_feedback_by_matrix_group", None)
    b["consumer_feedback_by_matrix_group_omitted"] = True
    lv = b.get("list_visibility_proxy")
    if isinstance(lv, dict) and _json_len(b) > max_chars:
        b["list_visibility_proxy"] = {"_omitted": True, "keys": list(lv.keys())[:20]}
    return _finalize()
