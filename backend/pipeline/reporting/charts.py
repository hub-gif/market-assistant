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


def _cn_volume_int(s: str) -> int:
    """
    从搜索侧文案抽取非负整数（评价量/销量等）：支持「亿」「万」及纯数字；
    如 ``已售50万+ | good:99%好评`` → 500000。
    """
    t = (s or "").strip().replace(",", "").replace("，", "")
    if not t:
        return 0
    m = re.search(r"(\d+(?:\.\d+)?)\s*亿", t)
    if m:
        return int(round(float(m.group(1)) * 100_000_000))
    m = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m:
        return int(round(float(m.group(1)) * 10_000))
    m2 = re.search(r"(\d+)", t)
    if m2:
        return int(m2.group(1))
    return 0


def _format_xaxis_int_cn(x: float, _pos: int | None) -> str:
    """
    横轴大整数刻度：用「万」「亿」表述，避免 matplotlib 默认 ``1e6`` 科学计数法。
    用于销量、评价量、条数等非负计数。
    """
    if not math.isfinite(x):
        return ""
    if abs(x) < 1e-9:
        return "0"
    ax = abs(x)
    sign = "-" if x < 0 else ""
    if ax < 10_000:
        return sign + str(int(round(ax)))
    if ax < 100_000_000:
        wan = ax / 10_000.0
        if wan >= 1000:
            return sign + f"{wan:.0f}万"
        if wan >= 100:
            return sign + f"{wan:.0f}万"
        if abs(wan - round(wan)) < 1e-6:
            return sign + f"{int(round(wan))}万"
        s = f"{wan:.1f}".rstrip("0").rstrip(".")
        return sign + s + "万"
    yi = ax / 100_000_000.0
    if abs(yi - round(yi)) < 1e-6:
        return sign + f"{int(round(yi))}亿"
    s = f"{yi:.2f}".rstrip("0").rstrip(".")
    return sign + s + "亿"


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
# 横向条形图：统一柱厚、柱端数值字号（全文件条形图共用）
_BARH_HEIGHT = 0.6
_BAR_VALUE_FONTSIZE = 8


def _thin_barh_height(n: int) -> float:
    """
    横向条形图：类目条数 n 较少时降低 barh 的 height（与 y 轴跨度同量纲）。
    n=1 时若仍用 0.6 且 ylim 跨度仅 1，单条会占满大半幅、视觉上极粗。
    """
    if n <= 0:
        return _BARH_HEIGHT
    if n == 1:
        return 0.30
    if n == 2:
        return 0.44
    if n <= 5:
        return 0.50
    if n <= 10:
        return 0.54
    return _BARH_HEIGHT


def _set_barh_category_ylim(ax: Any, n: int) -> None:
    """n 条类目横条时设置纵轴范围；n=1 时略放宽，使柱相对更细。"""
    if n <= 0:
        return
    if n == 1:
        ax.set_ylim(-1.0, 1.0)
    else:
        ax.set_ylim(-0.5, float(n) - 0.5)


def _fmt_bar_value(v: float, *, as_int: bool = False) -> str:
    if as_int or (math.isfinite(v) and abs(v - round(v)) < 1e-6):
        return str(int(round(v)))
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _annotate_barh_numeric(
    ax: Any,
    bars: Any,
    values: list[float],
    *,
    as_int: bool = False,
    x_pad_ratio: float = 0.02,
) -> None:
    """在横向柱末端标注数值；调用前请已设置合适的 xlim。"""
    if not bars or not values:
        return
    x1 = ax.get_xlim()[1]
    if x1 <= 0:
        return
    pad = max(x1 * x_pad_ratio, 0.02 * max(values) if values else 0.1)
    for bar, v in zip(bars, values):
        if v is None or not math.isfinite(float(v)) or float(v) <= 0:
            continue
        w = bar.get_width()
        ax.text(
            w + pad,
            bar.get_y() + bar.get_height() / 2,
            _fmt_bar_value(float(v), as_int=as_int),
            va="center",
            fontsize=_BAR_VALUE_FONTSIZE,
        )


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
    for pat in (
        "chart_focus_keywords_bar__*.png",
        "chart_usage_scenarios_bar__*.png",
    ):
        for fp in out_dir.glob(pat):
            try:
                fp.unlink()
            except OSError:
                pass


def generate_report_charts(run_dir: Path, brief: dict[str, Any]) -> list[str]:
    """生成扇形/条形 PNG。返回已写入的文件名列表（不含路径）。"""
    _setup_matplotlib_cjk()
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

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
        bh = _thin_barh_height(n)
        bars = ax.barh(
            list(y_pos), values, color="#2563eb", height=bh
        )
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        _set_barh_category_ylim(ax, n)
        ax.set_title(title, fontsize=12, pad=10)
        if xlabel:
            ax.set_xlabel(xlabel, fontsize=9)
        ax.tick_params(axis="y", left=True, right=False, labelleft=True, labelright=False)
        vmax = max(values)
        ax.set_xlim(0, vmax * 1.14 + max(0.08 * vmax, 0.5))
        _annotate_barh_numeric(ax, bars, list(values), as_int=True)
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
        bh = _thin_barh_height(n_b)
        bars = ax.barh(list(y_pos), pcts, color="#2563eb", height=bh)
        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        _set_barh_category_ylim(ax, n_b)
        ax.set_title(title, fontsize=12, pad=10)
        ax.set_xlabel("占有效评价文本比例（%）", fontsize=9)
        ax.tick_params(axis="y", left=True, right=False, labelleft=True, labelright=False)
        xmax = max(pcts) * 1.12 + 4.0
        ax.set_xlim(0, max(xmax, max(pcts) + 10.0, 24.0))
        x1 = ax.get_xlim()[1]
        pad = max(x1 * 0.015, 0.35)
        for bar, c, p in zip(bars, counts, pcts):
            ax.text(
                min(bar.get_width() + pad, x1 * 0.985),
                bar.get_y() + bar.get_height() / 2,
                f"{int(c)}条 · {p:.1f}%",
                va="center",
                fontsize=_BAR_VALUE_FONTSIZE,
            )
        fig.tight_layout()
        path = out_dir / fname
        fig.savefig(path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        created.append(fname)

    def save_combo_focus_scenario_bar(
        *,
        gname: str,
        slug: str,
        wl: list[str],
        vl: list[float],
        gl: list[str],
        gv: list[float],
        n_texts: int,
    ) -> None:
        """左：关注词命中次数；右：场景占有效文本 %。两侧 **各自独立 Y 轴**（类目互不混用）。"""
        has_l = bool(wl and vl and max(vl) > 0)
        has_r = bool(gl and gv and n_texts > 0 and max(gv) > 0)
        if not has_l and not has_r:
            return
        n_l = len(wl) if has_l else 0
        n_r = len(gl) if has_r else 0
        n_ref = max(n_l, n_r, 1)
        fig_h = max(3.4, min(14.0, 0.38 * n_ref + 2.8))
        fig = plt.figure(figsize=(10.8, fig_h))
        ttl = (gname or "").strip()[:22] or "细类"
        fig.suptitle(
            f"「{ttl}」· 关注词与使用场景",
            fontsize=11,
            y=0.98,
        )
        # 底对齐、高度按各自类目数比例分配，避免共用一个「拉伸后的」纵轴比例尺
        base_bottom = 0.10
        ax_w = 0.36
        x_gap = 0.06
        x_l = 0.07
        x_r = x_l + ax_w + x_gap
        max_h = 0.72
        n_den = max(n_ref, 1)
        h_l = max(0.26, max_h * (max(n_l, 1) / n_den)) if has_l else max(0.26, max_h * 0.35)
        h_r = max(0.26, max_h * (max(n_r, 1) / n_den)) if has_r else max(0.26, max_h * 0.35)
        ax_l = fig.add_axes([x_l, base_bottom, ax_w, h_l])
        ax_r = fig.add_axes([x_r, base_bottom, ax_w, h_r])
        if has_l:
            y_pos = list(range(n_l))
            bh_l = _thin_barh_height(n_l)
            bars_l = ax_l.barh(
                y_pos, vl[:n_l], color="#2563eb", height=bh_l
            )
            ax_l.set_yticks(y_pos)
            ax_l.set_yticklabels(wl[:n_l], fontsize=8)
            ax_l.invert_yaxis()
            _set_barh_category_ylim(ax_l, n_l)
            ax_l.set_xlabel("关注词子串命中次数", fontsize=9)
            ax_l.set_title("关注词", fontsize=10, pad=6)
            ax_l.tick_params(axis="y", left=True, right=False, labelleft=True, labelright=False)
            vmax_l = max(vl[:n_l])
            ax_l.set_xlim(0, vmax_l * 1.14 + max(0.5, 0.08 * vmax_l))
            _annotate_barh_numeric(
                ax_l, bars_l, list(vl[:n_l]), as_int=True
            )
        else:
            ax_l.text(
                0.5,
                0.5,
                "本细类无关注词命中\n或无数文本",
                ha="center",
                va="center",
                transform=ax_l.transAxes,
                fontsize=10,
                color="#64748b",
            )
            ax_l.set_axis_off()
        if has_r:
            pcts = [100.0 * c / n_texts for c in gv[: len(gl)]]
            n_b = len(gl)
            y_pos = list(range(n_b))
            bh_r = _thin_barh_height(n_b)
            bars = ax_r.barh(y_pos, pcts, color="#059669", height=bh_r)
            ax_r.set_yticks(y_pos)
            ax_r.set_yticklabels(gl[:n_b], fontsize=8)
            ax_r.invert_yaxis()
            _set_barh_category_ylim(ax_r, n_b)
            ax_r.set_xlabel("占有效评价文本比例（%）", fontsize=9)
            if pcts:
                xmax = max(pcts) * 1.12 + 4.0
                ax_r.set_xlim(0, max(xmax, max(pcts) + 10.0, 24.0))
            else:
                ax_r.set_xlim(0, 24.0)
            x1r = ax_r.get_xlim()[1]
            pad_r = max(x1r * 0.015, 0.35)
            for bar, c, p in zip(bars, gv[:n_b], pcts):
                ax_r.text(
                    min(bar.get_width() + pad_r, x1r * 0.985),
                    bar.get_y() + bar.get_height() / 2,
                    f"{int(c)}条 · {p:.1f}%",
                    va="center",
                    fontsize=_BAR_VALUE_FONTSIZE,
                )
            ax_r.set_title("使用场景", fontsize=10, pad=6)
            ax_r.yaxis.tick_left()
            ax_r.yaxis.set_label_position("left")
            ax_r.tick_params(axis="y", left=True, right=False, labelleft=True, labelright=False)
        else:
            ax_r.text(
                0.5,
                0.5,
                "本细类无场景词命中\n或无数文本",
                ha="center",
                va="center",
                transform=ax_r.transAxes,
                fontsize=10,
                color="#64748b",
            )
            ax_r.set_axis_off()
        path = out_dir / f"chart_focus_and_scenarios_bar__{slug}.png"
        fig.savefig(path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        created.append(path.name)

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
        "细类分布（合并表 SKU）",
        "chart_category_mix_pie.png",
    )
    save_bar_h(
        labs_m[:15],
        vals_m[:15],
        "细类分布（合并表 SKU 数，Top）",
        "chart_category_mix.png",
        "SKU 数",
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

    scen_by_slug: dict[str, tuple[list[str], list[float], int]] = {}
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
                scen_by_slug[slug] = (
                    [p[0] for p in gpairs],
                    [p[1] for p in gpairs],
                    n_unit,
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
            gl, gv, n_scen = scen_by_slug.get(slug, ([], [], 0))
            n_unit_fb = int(item.get("effective_comment_text_units") or 0)
            n_texts = n_scen if n_scen > 0 else n_unit_fb
            save_combo_focus_scenario_bar(
                gname=gname,
                slug=slug,
                wl=wl,
                vl=vl,
                gl=gl,
                gv=gv,
                n_texts=n_texts,
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
                sales = _cn_volume_int(str(s.get("total_sales") or ""))
                rows_data.append((label, p, sales))
            rows_data.sort(key=lambda x: x[0])
            if not rows_data:
                continue
            if not any(
                (pr is not None and pr > 0) or sv > 0
                for _, pr, sv in rows_data
            ):
                continue
            n = len(rows_data)
            labels_mx = [x[0] for x in rows_data]
            prices_mx = [x[1] for x in rows_data]
            sales_mx = [x[2] for x in rows_data]
            y_pos = list(range(n))
            fig_h = max(3.4, min(14.0, 0.38 * n + 2.4))
            fig, (ax_l, ax_r) = plt.subplots(
                1, 2, figsize=(10.6, fig_h), sharey=True
            )
            bh_mx = _thin_barh_height(n)
            price_w = [
                float(pr)
                if pr is not None and pr > 0 and math.isfinite(pr)
                else 0.0
                for pr in prices_mx
            ]
            bars_pl = ax_l.barh(
                y_pos, price_w, height=bh_mx, color="#2563eb"
            )
            ax_l.set_yticks(y_pos)
            ax_l.set_yticklabels(labels_mx, fontsize=8)
            ax_l.invert_yaxis()
            _set_barh_category_ylim(ax_l, n)
            ax_l.set_xlabel("展示价（元）", fontsize=9)
            ax_l.set_title("展示价", fontsize=10, pad=8)
            ax_l.tick_params(axis="y", left=True, right=False, labelleft=True, labelright=False)
            pmax = max(price_w) if price_w else 0.0
            if pmax > 0:
                ax_l.set_xlim(0, pmax * 1.12 + max(0.08 * pmax, 0.5))
            else:
                ax_l.set_xlim(0, 1)
            pad_p = max(ax_l.get_xlim()[1] * 0.012, 0.08)
            for bar, pr in zip(bars_pl, prices_mx):
                if pr is not None and pr > 0 and math.isfinite(pr):
                    ax_l.text(
                        bar.get_width() + pad_p,
                        bar.get_y() + bar.get_height() / 2,
                        _fmt_bar_value(float(pr), as_int=False),
                        va="center",
                        fontsize=_BAR_VALUE_FONTSIZE,
                    )
            sales_f = [float(s) for s in sales_mx]
            bars_sr = ax_r.barh(
                y_pos, sales_f, height=bh_mx, color="#059669"
            )
            ax_r.set_xlabel("销量", fontsize=9)
            ax_r.set_title("销量", fontsize=10, pad=8)
            ax_r.xaxis.set_major_formatter(
                FuncFormatter(_format_xaxis_int_cn)
            )
            ax_r.tick_params(axis="y", left=False, labelleft=False)
            smax = max(sales_f) if sales_f else 0.0
            if smax > 0:
                ax_r.set_xlim(0, smax * 1.1 + max(0.04 * smax, smax * 0.02))
            else:
                ax_r.set_xlim(0, 1)
            pad_s = max(ax_r.get_xlim()[1] * 0.008, smax * 0.01 if smax else 0.1)
            for bar, sv in zip(bars_sr, sales_mx):
                if sv > 0:
                    ax_r.text(
                        bar.get_width() + pad_s,
                        bar.get_y() + bar.get_height() / 2,
                        _format_xaxis_int_cn(float(sv), None),
                        va="center",
                        fontsize=_BAR_VALUE_FONTSIZE,
                    )
            ttl = gname[:22] if gname else "细类"
            fig.suptitle(
                f"「{ttl}」· 竞品矩阵：价格与销量",
                fontsize=11,
                y=1.01,
            )
            fig.tight_layout()
            out_mx = out_dir / f"chart_matrix_prices_sales__{slug}.png"
            fig.savefig(out_mx, dpi=130, bbox_inches="tight")
            plt.close(fig)
            created.append(out_mx.name)

    return created
