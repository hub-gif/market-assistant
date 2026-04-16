"""一键简报包：ZIP 内含完整 Markdown 报告、结构化 JSON、要点摘录。"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

from .brief_concentration import (
    concentration_first_share,
    concentration_top_three_share,
)


def _pct(x: Any) -> str:
    if x is None:
        return "—"
    try:
        return f"{100 * float(x):.1f}%"
    except (TypeError, ValueError):
        return str(x)


def _num(x: Any) -> str:
    if x is None:
        return "—"
    if isinstance(x, (int, float)):
        if isinstance(x, float) and x != int(x):
            return f"{x:.2f}"
        return str(int(x)) if isinstance(x, float) and x == int(x) else str(x)
    return str(x)


def markdown_summary_from_brief(brief: dict[str, Any]) -> str:
    """由 ``competitor-brief`` JSON 生成便于扫读的 Markdown（非 LLM）。"""
    lines: list[str] = [
        "# 竞品要点摘录（机器整理）",
        "",
        "> 与同批 **完整报告**、**数据汇总**同源；规则汇总，定稿前请人工核对。",
        "",
    ]
    kw = brief.get("keyword") or "—"
    batch = brief.get("batch_label") or "—"
    lines.extend(
        [
            "## 基本信息",
            "",
            f"- **监测词**：{kw}",
            f"- **批次**：{batch}",
            "",
        ]
    )

    scope = brief.get("scope") or {}
    if scope:
        lines.extend(
            [
                "## 样本范围",
                "",
                f"- **深入 SKU 数**：{_num(scope.get('merged_sku_count'))}",
                f"- **评价条数（扁平）**：{_num(scope.get('comment_flat_rows'))}",
                f"- **结构分析用列表行数**：{_num(scope.get('structure_source_rows'))}",
                f"- **是否含 PC 搜索全量导出**：{'是' if scope.get('uses_pc_search_list_export') else '否'}",
                "",
            ]
        )

    raw = brief.get("pc_search_raw") or {}
    if raw.get("result_count_consensus") is not None:
        lines.extend(
            [
                "## 列表侧检索规模（平台展示）",
                "",
                f"- **检索结果条数（多份响应取一致值）**：{_num(raw.get('result_count_consensus'))}",
                "",
            ]
        )

    conc = brief.get("concentration") or {}
    shops = conc.get("shops_from_list") or {}
    if concentration_first_share(shops) is not None or shops.get("top_label"):
        lines.extend(
            [
                "## 店铺集中度（列表）",
                "",
                f"- **第一大店铺份额**：{_pct(concentration_first_share(shops))}（第一店铺：{shops.get('top_label') or '—'}）",
                f"- **前三店铺合计份额**：{_pct(concentration_top_three_share(shops))}",
                "",
            ]
        )
    dbrand = conc.get("detail_brand_among_merged") or {}
    if concentration_first_share(dbrand) is not None or dbrand.get("top_label"):
        lines.extend(
            [
                "## 品牌（深入样本）",
                "",
                f"- **第一大品牌份额（深入样本）**：{_pct(concentration_first_share(dbrand))}（头部：{dbrand.get('top_label') or '—'}）",
                f"- **前三品牌合计份额**：{_pct(concentration_top_three_share(dbrand))}",
                "",
            ]
        )

    pst = brief.get("price_stats") or {}
    if pst.get("n"):
        src = brief.get("price_stats_source") or "—"
        lines.extend(
            [
                "## 价格（展示价统计）",
                "",
                f"- **样本量（条）**：{_num(pst.get('n'))}；**价格来源**：{src}",
                f"- **区间**：{_num(pst.get('min'))} ～ {_num(pst.get('max'))}；**中位数**：{_num(pst.get('median'))}",
                "",
            ]
        )

    mix = brief.get("category_mix_top") or []
    if mix:
        lines.extend(["## 类目结构（Top）", ""])
        for item in mix[:8]:
            if isinstance(item, dict):
                lines.append(
                    f"- {item.get('label') or '—'}：{_num(item.get('count'))}"
                )
        lines.append("")

    ckw = brief.get("comment_focus_keywords") or []
    if ckw:
        lines.extend(["## 评价关注词（Top）", ""])
        for item in ckw[:10]:
            if isinstance(item, dict):
                lines.append(
                    f"- **{item.get('word') or '—'}**：{_num(item.get('count'))} 次"
                )
        lines.append("")

    usc = brief.get("usage_scenarios") or []
    if usc:
        lines.extend(["## 用途/场景（预设词组，Top）", ""])
        for item in usc[:8]:
            if isinstance(item, dict):
                lines.append(
                    f"- **{item.get('scenario') or '—'}**：{_num(item.get('count'))} 条（约 {_pct(item.get('share_of_text_units'))} 文本单元）"
                )
        lines.append("")

    hints = brief.get("strategy_hints") or []
    if hints:
        lines.extend(["## 策略提示（规则）", ""])
        for h in hints:
            lines.append(f"- {h}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "*更细的矩阵与消费者反馈见简报包内完整分析报告；结构化字段见同包内摘要数据文件。*",
            "",
        ]
    )
    return "\n".join(lines)


README_TXT = """竞品「一键简报包」说明（Market-Assistant）
============================================

本 ZIP 由「报告查看」页一键导出，内含：

  说明文件              — 本文件
  完整分析报告          — 与任务批次中的主报告文稿一致
  统计图 PNG            — report_assets 目录（与报告内插图同源）
  结构化摘要数据        — 与「结构化摘要」接口同源，可供其它工具读取
  要点摘录              — 由摘要自动整理的速读稿，便于邮件/转发前浏览

使用建议：对外发送前请核对要点摘录与完整报告中的结论；数据边界见报告第一章。
"""


def build_brief_pack_zip_bytes(run_dir: Path, brief: dict[str, Any]) -> bytes:
    """
    生成 ZIP 字节流。``run_dir`` 下须存在 ``competitor_analysis.md``。
    """
    run_dir = Path(run_dir).resolve()
    report_path = run_dir / "competitor_analysis.md"
    if not report_path.is_file():
        raise FileNotFoundError("缺少已生成的分析报告文件，请先在「报告生成」中生成报告")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("01_竞品分析报告.md", report_path.read_text(encoding="utf-8"))
        zf.writestr(
            "02_结构化摘要.json",
            json.dumps(brief, ensure_ascii=False, indent=2),
        )
        zf.writestr("03_要点摘录.md", markdown_summary_from_brief(brief))
        zf.writestr("00_说明.txt", README_TXT)
        assets = run_dir / "report_assets"
        if assets.is_dir():
            for fp in sorted(assets.iterdir()):
                if fp.is_file():
                    zf.write(fp, f"report_assets/{fp.name}")
    return buf.getvalue()
