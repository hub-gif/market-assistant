"""根据结构化 brief 生成报告用 PNG 统计图（matplotlib），写入 ``run_dir/report_assets/``。"""

from __future__ import annotations

import math
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


def _float_price_from_cell(s: str) -> float | None:
    t = (s or "").strip().replace(",", "").replace("，", "")
    if not t:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    if 0 < v < 1_000_000:
        return v
    return None


def _reviews_volume_int(s: str) -> int:
    """与矩阵「评价量」列同源：从搜索侧模糊文案中抽取整数（含「万」）。"""
    t = (s or "").strip().replace(",", "").replace("，", "")
    if not t:
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m:
        return int(round(float(m.group(1)) * 10_000))
    m2 = re.search(r"(\d+)", t)
    if m2:
        return int(m2.group(1))
    return 0


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

    matrix_groups = brief.get("matrix_by_group") or []
    if isinstance(matrix_groups, list):
        for gi, block in enumerate(matrix_groups):
            if not isinstance(block, dict):
                continue
            gname = str(block.get("group") or "").strip()
            skus = block.get("skus") or []
            if not isinstance(skus, list) or not skus:
                continue
            slug = scenario_group_asset_slug(gname, gi)
            rows_data: list[tuple[str, float | None, int]] = []
            for s in skus:
                if not isinstance(s, dict):
                    continue
                title = str(s.get("title") or "").strip()
                sku = str(s.get("sku_id") or "").strip()
                # 与 §5 矩阵「产品」列一致：纵轴优先品名，无标题时再退化为 SKU
                if title:
                    label = title if len(title) <= 48 else title[:46] + "…"
                elif sku:
                    label = sku if len(sku) <= 22 else sku[:20] + "…"
                else:
                    label = "?"
                p: float | None = None
                for k in (
                    "detail_price_final",
                    "list_price_show",
                    "coupon_or_detail_price",
                ):
                    p = _float_price_from_cell(str(s.get(k) or ""))
                    if p is not None:
                        break
                rev = _reviews_volume_int(str(s.get("comment_fuzzy") or ""))
                rows_data.append((label, p, rev))
            rows_data.sort(key=lambda x: x[0])
            if not rows_data:
                continue
            if not any(
                (pr is not None and pr > 0) or rv > 0
                for _, pr, rv in rows_data
            ):
                continue
            n = len(rows_data)
            labels_mx = [x[0] for x in rows_data]
            prices_mx = [x[1] for x in rows_data]
            reviews_mx = [x[2] for x in rows_data]
            y_pos = list(range(n))
            fig_h = max(3.4, min(14.0, 0.38 * n + 2.4))
            fig, (ax_l, ax_r) = plt.subplots(
                1, 2, figsize=(10.6, fig_h), sharey=True
            )
            for yi, pr in enumerate(prices_mx):
                if pr is not None and pr > 0 and math.isfinite(pr):
                    ax_l.barh(yi, pr, height=0.62, color="#2563eb")
            ax_l.set_yticks(y_pos)
            ax_l.set_yticklabels(labels_mx, fontsize=8)
            ax_l.invert_yaxis()
            ax_l.set_xlabel("展示价（元）", fontsize=9)
            ax_l.set_title("展示价", fontsize=10, pad=8)
            ax_r.barh(y_pos, reviews_mx, height=0.62, color="#059669")
            ax_r.set_xlabel("评价量（搜索侧）", fontsize=9)
            ax_r.set_title("评价量 / 声量", fontsize=10, pad=8)
            ax_r.tick_params(axis="y", left=False, labelleft=False)
            ttl = gname[:22] if gname else "细类"
            fig.suptitle(
                f"「{ttl}」· 竞品矩阵：价格与评价量（与 §5 表同源）",
                fontsize=11,
                y=1.01,
            )
            fig.tight_layout()
            out_mx = out_dir / f"chart_matrix_prices_reviews__{slug}.png"
            fig.savefig(out_mx, dpi=130, bbox_inches="tight")
            plt.close(fig)
            created.append(out_mx.name)

    return created
