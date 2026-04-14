"""根据结构化 brief 生成报告用 PNG 统计图（matplotlib），写入 ``run_dir/report_assets/``。"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


def _setup_matplotlib_cjk() -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    windir = os.environ.get("WINDIR", r"C:\Windows")
    for name in ("simhei.ttf", "msyh.ttc", "simsun.ttc"):
        fp = Path(windir) / "Fonts" / name
        if fp.is_file():
            try:
                font_manager.fontManager.addfont(str(fp))
                fam = font_manager.FontProperties(fname=str(fp)).get_name()
                plt.rcParams["font.family"] = [fam]
                break
            except Exception:
                continue
    plt.rcParams["axes.unicode_minus"] = False


def _label_count_pairs(
    items: Any,
    *,
    key_label: str = "label",
    key_count: str = "count",
    cap: int = 40,
) -> tuple[list[str], list[float]]:
    labs: list[str] = []
    vals: list[float] = []
    if not isinstance(items, list):
        return labs, vals
    for item in items[:cap]:
        if not isinstance(item, dict):
            continue
        lbl = str(item.get(key_label) or "").strip()[:48]
        cnt = item.get(key_count)
        if lbl and isinstance(cnt, (int, float)) and cnt > 0:
            labs.append(lbl)
            vals.append(float(cnt))
    return labs, vals


def _merge_labeled_counts_tail(
    pairs: list[tuple[str, float]], *, max_items: int
) -> list[tuple[str, float]]:
    if len(pairs) <= max_items:
        return pairs
    head = pairs[: max_items - 1]
    rest = sum(c for _, c in pairs[max_items - 1 :])
    if rest > 0:
        head.append(("其他", rest))
    return head


def _matrix_block_chart_slug(group: str, index: int) -> str:
    """与 ``jd_competitor_report._scenario_group_asset_slug`` 规则一致（文件名对齐）。"""
    raw = (group or "").strip()
    core = re.sub(r"[^\w\u4e00-\u9fff-]", "", raw)[:20]
    if not core:
        core = "group"
    return f"i{index:02d}_{core}"


def _parse_price_from_text(s: str) -> float | None:
    t = (s or "").strip().replace(",", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        v = float(m.group(1))
        return v if 0 < v < 1_000_000 else None
    except ValueError:
        return None


def _parse_comment_fuzzy_sortable(s: str) -> float | None:
    """评价量/声量展示文案粗转可排序正数（启发式，非精确条数）。"""
    t = (s or "").strip().replace("＋", "+").replace(" ", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*万\+?", t)
    if m:
        return float(m.group(1)) * 10_000
    m = re.search(r"(\d+(?:\.\d+)?)\s*亿\+?", t)
    if m:
        return float(m.group(1)) * 100_000_000
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m:
        return float(m.group(1)) * 10_000
    m = re.search(r"(\d{2,})\+", t)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+)", t)
    if m:
        return float(m.group(1))
    return None


def _merge_tail_as_other(
    labels: list[str], values: list[float], *, max_slices: int
) -> tuple[list[str], list[float]]:
    pairs = [(l, v) for l, v in zip(labels, values) if v > 0]
    if not pairs:
        return [], []
    if len(pairs) <= max_slices:
        return [p[0] for p in pairs], [p[1] for p in pairs]
    head = pairs[: max_slices - 1]
    rest = sum(v for _, v in pairs[max_slices - 1 :])
    labs = [p[0] for p in head]
    vals = [p[1] for p in head]
    if rest > 0:
        labs.append("其他")
        vals.append(rest)
    return labs, vals


# 已不再写入报告正文的旧版图，避免 run_dir 里残留误导性 PNG
_OBSOLETE_REPORT_ASSETS: frozenset[str] = frozenset(
    {
        "chart_focus_keywords_bar.png",
        "chart_usage_scenarios.png",
        "chart_usage_scenarios_pie.png",
        "chart_focus_keywords_pie.png",
        "chart_comment_focus_global_bar.png",
        "chart_usage_scenarios_global_bar.png",
    }
)


def _cleanup_obsolete_report_assets(out_dir: Path) -> None:
    """删除历史版本生成的、当前报告不再引用的插图文件。"""
    if not out_dir.is_dir():
        return
    for name in _OBSOLETE_REPORT_ASSETS:
        fp = out_dir / name
        if fp.is_file():
            try:
                fp.unlink()
            except OSError:
                pass
    for fp in out_dir.glob("chart_usage_scenarios_pie__*.png"):
        try:
            fp.unlink()
        except OSError:
            pass


def generate_report_charts(run_dir: Path, brief: dict[str, Any]) -> list[str]:
    """生成扇形/条形 PNG。返回已写入的文件名列表（不含路径）。"""
    _setup_matplotlib_cjk()
    import matplotlib.pyplot as plt

    out_dir = Path(run_dir).resolve() / "report_assets"
    out_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_obsolete_report_assets(out_dir)
    created: list[str] = []

    def save_bar_h(
        labels: list[str],
        values: list[float],
        title: str,
        fname: str,
        xlabel: str = "",
    ) -> None:
        if not labels or not values or max(values) <= 0:
            return
        n = len(labels)
        fig_h = max(3.2, min(14.0, 0.38 * n + 1.5))
        fig, ax = plt.subplots(figsize=(8.2, fig_h))
        y_pos = range(n)
        ax.barh(list(y_pos), values, color="#2563eb", height=0.65)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=12, pad=10)
        if xlabel:
            ax.set_xlabel(xlabel, fontsize=9)
        fig.tight_layout()
        path = out_dir / fname
        fig.savefig(path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        created.append(fname)

    def save_bar_h_share_of_text(
        labels: list[str],
        counts: list[float],
        n_texts: int,
        title: str,
        fname: str,
    ) -> None:
        """
        横轴 = count / n_texts * 100，与报告表格「占有效文本比例」一致（多标签下各柱比例可相加 >100%）。
        """
        if not labels or not counts or n_texts <= 0 or max(counts) <= 0:
            return
        pcts = [100.0 * c / n_texts for c in counts]
        n_b = len(labels)
        fig_h = max(3.2, min(14.0, 0.38 * n_b + 1.8))
        fig, ax = plt.subplots(figsize=(8.8, fig_h))
        y_pos = range(n_b)
        bars = ax.barh(list(y_pos), pcts, color="#2563eb", height=0.65)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=12, pad=10)
        ax.set_xlabel("占有效评价文本比例（%）", fontsize=9)
        xmax = max(pcts) * 1.12 + 4.0
        ax.set_xlim(0, max(xmax, max(pcts) + 10.0, 24.0))
        for bar, c, p in zip(bars, counts, pcts):
            ax.text(
                min(bar.get_width() + 0.6, ax.get_xlim()[1] * 0.97),
                bar.get_y() + bar.get_height() / 2,
                f"{int(c)}条 · {p:.1f}%",
                va="center",
                fontsize=8,
            )
        fig.tight_layout()
        path = out_dir / fname
        fig.savefig(path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        created.append(fname)

    def save_pie(
        labels: list[str],
        values: list[float],
        title: str,
        fname: str,
        *,
        max_slices: int = 8,
    ) -> None:
        labs, vals = _merge_tail_as_other(labels, values, max_slices=max_slices)
        if not labs or not vals or sum(vals) <= 0:
            return
        fig, ax = plt.subplots(figsize=(7.2, 5.4))
        colors = plt.cm.Set3(range(len(labs)))
        wedges, _t, autotexts = ax.pie(
            vals,
            labels=None,
            autopct=lambda p: f"{p:.1f}%" if p >= 3.5 else "",
            pctdistance=0.72,
            colors=colors,
            startangle=90,
        )
        for t in autotexts:
            t.set_fontsize(8)
        ax.legend(
            wedges,
            labs,
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            fontsize=8,
            frameon=False,
        )
        ax.set_title(title, fontsize=12, pad=12)
        fig.tight_layout()
        path = out_dir / fname
        fig.savefig(path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        created.append(fname)

    mix = brief.get("category_mix_top") or []
    labs_m, vals_m = _label_count_pairs(mix)
    save_pie(
        labs_m,
        vals_m,
        "类目/可读名称分布（列表行占比）",
        "chart_category_mix_pie.png",
    )
    save_bar_h(
        labs_m[:15],
        vals_m[:15],
        "类目分布（行数，Top）",
        "chart_category_mix.png",
        "行数",
    )

    brand_mix = brief.get("list_brand_mix_top") or []
    lb, vb = _label_count_pairs(brand_mix, key_label="label")
    save_pie(
        lb,
        vb,
        "品牌列表曝光占比",
        "chart_brand_rows_pie.png",
    )

    shop_mix = brief.get("list_shop_mix_top") or []
    ls, vs = _label_count_pairs(shop_mix, key_label="label")
    save_pie(
        ls,
        vs,
        "店铺列表曝光占比",
        "chart_shop_rows_pie.png",
    )

    sf = brief.get("sales_floor_analysis") or {}
    sf_chart = sf.get("bucket_chart") if isinstance(sf, dict) else None
    labs_sf, vals_sf = _label_count_pairs(sf_chart or [])
    save_bar_h(
        labs_sf,
        vals_sf,
        "销量楼层档位分布（结构行数）",
        "chart_sales_floor_buckets_bar.png",
        "行数",
    )

    def scenario_group_asset_slug(group: str, index: int) -> str:
        """与 ``jd_competitor_report._scenario_group_asset_slug`` 保持一致。"""
        raw = (group or "").strip()
        core = re.sub(r"[^\w\u4e00-\u9fff-]", "", raw)[:20]
        if not core:
            core = "group"
        return f"i{index:02d}_{core}"

    by_grp = brief.get("usage_scenarios_by_matrix_group") or []
    if isinstance(by_grp, list):
        for item in by_grp:
            if not isinstance(item, dict):
                continue
            slug = (item.get("chart_slug") or "").strip()
            gname = str(item.get("group") or "").strip()[:24]
            idx = item.get("matrix_group_index")
            if not slug and gname != "" and isinstance(idx, int):
                slug = scenario_group_asset_slug(gname, idx)
            if not slug:
                continue
            scen_rows = item.get("scenarios") or []
            n_unit = int(item.get("effective_text_units") or 0)
            gpairs: list[tuple[str, float]] = []
            if isinstance(scen_rows, list):
                for r in scen_rows:
                    if not isinstance(r, dict):
                        continue
                    lb = str(r.get("scenario") or "").strip()[:48]
                    c = r.get("count")
                    if lb and isinstance(c, (int, float)) and c > 0:
                        gpairs.append((lb, float(c)))
            gpairs = _merge_labeled_counts_tail(gpairs, max_items=14)
            if gpairs and n_unit > 0:
                gl = [p[0] for p in gpairs]
                gv = [p[1] for p in gpairs]
                title_base = f"「{gname}」· 场景/用途" if gname else "细类 · 场景/用途"
                save_bar_h_share_of_text(
                    gl,
                    gv,
                    n_unit,
                    f"{title_base}（占有效评价文本比例）",
                    f"chart_usage_scenarios_bar__{slug}.png",
                )

    fb = brief.get("consumer_feedback_by_matrix_group") or []
    if isinstance(fb, list):
        for item in fb:
            if not isinstance(item, dict):
                continue
            slug = (item.get("chart_slug") or "").strip()
            gname = str(item.get("group") or "").strip()[:24]
            idx = item.get("matrix_group_index")
            if not slug and gname != "" and isinstance(idx, int):
                slug = scenario_group_asset_slug(gname, idx)
            if not slug:
                continue
            hk = item.get("focus_keyword_hits") or []
            wl: list[str] = []
            vl: list[float] = []
            if isinstance(hk, list):
                for row in hk[:20]:
                    if not isinstance(row, dict):
                        continue
                    w = str(row.get("word") or "").strip()[:32]
                    c = row.get("count")
                    if w and isinstance(c, (int, float)) and c > 0:
                        wl.append(w)
                        vl.append(float(c))
            wl = wl[:18]
            vl = vl[:18]
            if not wl:
                continue
            tkw = f"「{gname}」· 关注词命中次数" if gname else "细类 · 关注词命中次数"
            save_bar_h(wl, vl, tkw, f"chart_focus_keywords_bar__{slug}.png", "命中次数")

    matrix_compact = bool(brief.get("matrix_compact_section", True))
    mg = brief.get("matrix_by_group") or []
    if matrix_compact and isinstance(mg, list):
        for gi, block in enumerate(mg):
            if not isinstance(block, dict):
                continue
            gname = str(block.get("group") or "").strip()
            skus = block.get("skus")
            if not isinstance(skus, list) or not skus:
                continue
            slug = _matrix_block_chart_slug(gname, gi)
            triples: list[tuple[str, float, float]] = []
            for s in skus:
                if not isinstance(s, dict):
                    continue
                title = (s.get("title") or "").strip()[:30] or str(
                    s.get("sku_id") or ""
                ).strip()[:14] or "—"
                p = _parse_price_from_text(str(s.get("detail_price_final") or ""))
                if p is None:
                    p = _parse_price_from_text(
                        str(s.get("coupon_or_detail_price") or "")
                    )
                if p is None:
                    p = _parse_price_from_text(str(s.get("list_price_show") or ""))
                v = _parse_comment_fuzzy_sortable(str(s.get("comment_fuzzy") or ""))
                triples.append((title, p or 0.0, v or 0.0))
            if not triples:
                continue
            triples.sort(key=lambda x: x[1], reverse=True)
            triples = triples[:36]
            labs = [t[0] for t in triples]
            prs = [t[1] for t in triples]
            cms = [t[2] for t in triples]
            tbase = f"「{gname}」" if gname else "细类"
            if max(prs) > 0:
                save_bar_h(
                    labs,
                    prs,
                    f"{tbase}· 详情/券后价（元）",
                    f"chart_matrix_price__{slug}.png",
                    "元",
                )
            if max(cms) > 0:
                save_bar_h(
                    labs,
                    cms,
                    f"{tbase}· 评价量展示（粗算排序，非精确条数）",
                    f"chart_matrix_comments__{slug}.png",
                    "粗算值",
                )

    sent = brief.get("comment_sentiment_lexicon") or {}
    if isinstance(sent, dict):
        pie_labs = ["偏正向", "偏负向", "正负混合", "中性/空"]
        pie_vals = [
            float(sent.get("positive_only") or 0),
            float(sent.get("negative_only") or 0),
            float(sent.get("mixed_positive_and_negative") or 0),
            float(sent.get("neutral_or_empty") or 0),
        ]
        pl = [a for a, b in zip(pie_labs, pie_vals) if b > 0]
        pv = [b for b in pie_vals if b > 0]
        save_pie(pl, pv, "评价语气四象限占比", "chart_sentiment_overview_pie.png")
        save_bar_h(
            pl,
            pv,
            "评价正负面粗判（条数）",
            "chart_sentiment.png",
            "条数",
        )

        pos_h = sent.get("positive_tone_lexeme_hits") or []
        neg_h = sent.get("negative_tone_lexeme_hits") or []
        plx, pvx = _label_count_pairs(
            pos_h, key_label="word", key_count="texts_matched", cap=16
        )
        save_bar_h(
            plx,
            pvx,
            "正向/混合语境 · 正向口语短语命中条数",
            "chart_positive_lexemes_bar.png",
            "条数",
        )
        nlx, nvx = _label_count_pairs(
            neg_h, key_label="word", key_count="texts_matched", cap=16
        )
        save_bar_h(
            nlx,
            nvx,
            "负向/混合语境 · 负向口语短语命中条数",
            "chart_negative_lexemes_bar.png",
            "条数",
        )

    return created
