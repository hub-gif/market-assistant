"""
第八章「文本挖掘探针」独立脚本（**不修改**主报告代码）。

流程（按细类分组）：清洗（jieba 分词 + 停用词）→ **词云图（可选）** → 词频 / TF-IDF → 共现对 → LDA 主题
→ 规则化叙事小结 → 文末可选 **探针专用 LLM**（结构化 JSON + ``PROBE_TEXT_MINING_SYSTEM`` + ``_call_llm``；**非** ``COMMENT_GROUPS_SYSTEM``、**非** §8.2 情感）。

依赖（请自行安装）::

    pip install jieba scikit-learn numpy wordcloud matplotlib

用法（在 ``backend`` 目录下）::

    python -m pipeline.demos.chapter8_text_mining_probe --run-dir \"../data/JD/pipeline_runs/20260413_104252_低GI\"
    python -m pipeline.demos.chapter8_text_mining_probe --run-dir \"...\" --out chapter8_probe.md
    python -m pipeline.demos.chapter8_text_mining_probe --run-dir \"...\" --live-llm
    python -m pipeline.demos.chapter8_text_mining_probe --run-dir \"...\" --live-llm --llm-chunked

输出：默认写入 ``<run_dir>/chapter8_text_mining_probe.md``。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

JCR_ROOT = BACKEND_ROOT / "crawler_copy" / "jd_pc_search"
if str(JCR_ROOT) not in sys.path:
    sys.path.insert(0, str(JCR_ROOT))

import jd_competitor_report as jcr  # noqa: E402
import jd_keyword_pipeline as kpl  # noqa: E402

from pipeline.csv_schema import MERGED_FIELD_TO_CSV_HEADER  # noqa: E402
from pipeline.llm.generate import _call_llm  # noqa: E402 探针专用，不新增 generate 导出

try:
    import jieba  # noqa: WPS433
    from sklearn.decomposition import LatentDirichletAllocation  # noqa: WPS433
    from sklearn.feature_extraction.text import (  # noqa: WPS433
        CountVectorizer,
        TfidfVectorizer,
    )
except ImportError as e:
    print(
        "缺少依赖，请先安装：pip install jieba scikit-learn numpy\n"
        f"原始错误: {e}",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: WPS433
    from wordcloud import WordCloud  # noqa: WPS433

    _WORDCLOUD_AVAILABLE = True
except ImportError:
    plt = None  # type: ignore[assignment]
    WordCloud = None  # type: ignore[assignment]
    _WORDCLOUD_AVAILABLE = False


# 精简中文停用词（可换外部文件）；与业务无关，仅用于探针
_STOP_BASIC: frozenset[str] = frozenset(
    """
    的 了 和 是 在 也 有 就 都 很 啊 还 吗 吧 呢 呀 哦 噢 哈 呵 与 及 或 等 为 被 让 从 到 把 而 又 对 中 这 那 其 一个 一些 没有 不是 可以 这样 我们 你们 他们 它们 它 会 要 能 去 来 做 用 给 自己 这个 那个 什么 怎么 如果 因为 所以 但是 而且 然后 还是 或者 还有 就是 只是 只是 已经 还是
    非常 真的 比较 特别 感觉 觉得 认为 看到 收到 东西 商品 产品 卖家 买家 店铺 京东 物流 快递 包装 评价 评论 购买 买 卖 收到 天 次 个 款 种 条 块
    """.split()
)

# --- 文本挖掘探针 · 专用 LLM（与正式报告 ``COMMENT_GROUPS_SYSTEM`` / §8.2 均不同）---
PROBE_TEXT_MINING_SYSTEM = """你是用户研究与文本挖掘方向的助手。

输入 JSON 为「第八章文本挖掘探针」的**专用**结果（``schema_version``=1），**不是**正式竞品报告里的关注词规则统计、也不是 §8.2 情感 Lexicon。其中的数字与词表来自 **jieba 分词 + sklearn（TF-IDF / 共现 / LDA）**，与业务侧子串计数**口径不同**。

每个 ``groups`` 元素含：
- ``probe_status``：``ok`` 表示该细类已完成分词与统计；``skipped`` 表示样本过少等未下钻。
- ``word_freq_top`` / ``tfidf_top`` / ``cooccurrence_top``：统计型特征（开放词表）。
- ``lda``：无监督主题词；**仅为探索**，同一词可出现在多主题，**禁止**当作严格品类或固定标签。
- ``focus_hit_lines`` / ``sample_text_snippets``：与正式管线摘取方式**类似**，仅用于**对照语境**；若与 TF-IDF 焦点冲突，以**整句原文**为准，并在段末或「使用注意」中可点明「统计与语义可能不一致」。

**任务**：对 ``groups`` 中**每一项**输出对应 Markdown（**顺序与输入一致**）：
- 对 ``probe_status == "ok"``：以 ``#### `` + 与该条 ``group`` 字段**完全一致**的细类名作为小节标题（勿用 ``##`` 一级标题）；每段约 **100～260 字**。
- 内容须包含：①用 **1～2 句**概括该细类评论**主要讨论焦点**（综合词频与 TF-IDF，**不要罗列具体数字**）；② **1～2 句**说明共现词对**暗示**哪些维度常一起出现（**非因果**）；③若 ``lda.topics`` 非空，**1～2 句**说明主题粗分侧重点，并**明确** LDA 无监督、**不**与矩阵细类一一对应；④至少 **一句**与 ``sample_text_snippets`` 或 ``focus_hit_lines`` 中的**原文信息**呼应（可保留 ``【细类…店铺…】`` 前缀）；若无可用摘录则写明。
- 对 ``probe_status == "skipped"``：该小节仅 **一句**说明原因。

**禁止**：编造数据中未出现的品牌、价格、医学功效或疗效承诺；不要把 ``keyword`` 监测词写进「用户原话」；不要输出 Markdown 表格；不要声称本段与「正式报告 §8 末」完全同源——本任务为**探针解读**。

全文末可另起一段 **「使用注意」**（简短）：点明开放词表统计与人工阅读差异、LDA 局限、小样本细类不可靠。

总字数约 **800～4500 字**（细类多则偏长）。仅输出正文 Markdown，不要用代码围栏包裹全文。"""

PROBE_TEXT_MINING_USER_PREFIX = (
    "请根据以下 JSON 撰写「文本挖掘探针」解读正文（Markdown）。\n\n"
)


def _is_noise_token(w: str) -> bool:
    if len(w) < 2:
        return True
    if w in ("ldquo", "rdquo", "nbsp", "mdash"):
        return True
    if re.match(r"^[a-z]{1,8}$", w) and w not in ("gi",):
        return True
    return False


def _word_freq_from_cut_docs(cut_docs: list[str]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for line in cut_docs:
        for w in line.split():
            if _is_noise_token(w):
                continue
            c[w] += 1
    return dict(c)


def _font_path_chinese() -> str | None:
    windir = os.environ.get("WINDIR", r"C:\Windows")
    candidates = [
        Path(windir) / "Fonts" / "msyh.ttc",
        Path(windir) / "Fonts" / "msyhbd.ttc",
        Path(windir) / "Fonts" / "simhei.ttf",
        Path(windir) / "Fonts" / "simsun.ttc",
        Path("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"),
        Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
    ]
    for p in candidates:
        try:
            if p.is_file():
                return str(p)
        except OSError:
            continue
    return None


def _slug_safe(gname: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "_", (gname or "").strip())
    return s[:80] if len(s) > 80 else s


def _save_wordcloud_png(
    freq: dict[str, int],
    out_path: Path,
    *,
    font_path: str | None,
) -> str:
    """写入 PNG；成功返回空串，失败返回错误说明。"""
    if not _WORDCLOUD_AVAILABLE or WordCloud is None or plt is None:
        return "wordcloud/matplotlib 未安装"
    if not freq:
        return "词频为空"
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wc = WordCloud(
            font_path=font_path,
            width=960,
            height=540,
            background_color="white",
            max_words=220,
            relative_scaling=0.35,
            colormap="viridis",
            prefer_horizontal=0.88,
            min_font_size=10,
        ).generate_from_frequencies(freq)
        fig, ax = plt.subplots(figsize=(10.5, 6), dpi=120)
        ax.imshow(wc, interpolation="bilinear")
        ax.axis("off")
        fig.tight_layout(pad=0)
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
        plt.close(fig)
    except Exception as e:
        return str(e)
    return ""


def _load_run(
    run_dir: Path,
) -> tuple[str, list[dict[str, str]], list[dict[str, str]]]:
    run_dir = run_dir.resolve()
    merged_path = run_dir / kpl.FILE_MERGED_CSV
    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表: {merged_path}")
    _, merged_rows = jcr._read_csv_rows(merged_path)
    _, comment_rows = jcr._read_csv_rows(run_dir / kpl.FILE_COMMENTS_FLAT_CSV)
    meta_path = run_dir / kpl.FILE_RUN_META_JSON
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = None
    kw = ""
    if meta and str(meta.get("keyword") or "").strip():
        kw = str(meta.get("keyword")).strip()
    return kw, merged_rows, comment_rows


def _cut_one(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []
    raw = jieba.lcut(s)
    out: list[str] = []
    for w in raw:
        w = w.strip()
        if len(w) < 2:
            continue
        if w in _STOP_BASIC:
            continue
        if re.match(r"^[0-9\s\W_]+$", w):
            continue
        out.append(w)
    return out


def _docs_cut(texts: list[str]) -> list[str]:
    """每条评论 → 空格连接的分词串，供 sklearn。"""
    rows: list[str] = []
    for t in texts:
        toks = _cut_one(t)
        if toks:
            rows.append(" ".join(toks))
    return rows


def _term_freq_top(cut_docs: list[str], top_k: int) -> list[tuple[str, int]]:
    c: Counter[str] = Counter()
    for line in cut_docs:
        for w in line.split():
            c[w] += 1
    return c.most_common(top_k)


def _tfidf_top(cut_docs: list[str], top_k: int) -> list[tuple[str, float]]:
    if not cut_docs:
        return []
    vec = TfidfVectorizer(max_features=min(2000, max(50, len(cut_docs) * 3)))
    try:
        X = vec.fit_transform(cut_docs)
    except ValueError:
        return []
    feats = np.array(vec.get_feature_names_out())
    scores = np.asarray(X.mean(axis=0)).ravel()
    idx = np.argsort(-scores)[:top_k]
    return [(str(feats[i]), float(scores[i])) for i in idx]


def _cooc_top_pairs(
    cut_docs: list[str],
    vocab_cap: int,
    pair_top: int,
) -> list[tuple[str, str, int]]:
    """同一条评论内无序词对共现（过滤低频词）。"""
    term_freq = Counter()
    for line in cut_docs:
        for w in set(line.split()):
            term_freq[w] += 1
    top_terms = {w for w, _ in term_freq.most_common(vocab_cap)}
    pair_c: Counter[tuple[str, str]] = Counter()
    for line in cut_docs:
        toks = sorted({w for w in line.split() if w in top_terms})
        for i in range(len(toks)):
            for j in range(i + 1, len(toks)):
                a, b = toks[i], toks[j]
                if a > b:
                    a, b = b, a
                pair_c[(a, b)] += 1
    return [(a, b, n) for (a, b), n in pair_c.most_common(pair_top)]


def _lda_topics(
    cut_docs: list[str],
    n_topics: int,
    n_top_words: int,
) -> tuple[list[list[str]], str]:
    if len(cut_docs) < 4:
        return [], "文本条数过少，跳过 LDA。"
    n_topics = max(2, min(n_topics, len(cut_docs) // 2))
    try:
        vec = CountVectorizer(max_df=0.95, min_df=2, max_features=800)
        X = vec.fit_transform(cut_docs)
    except ValueError as e:
        return [], f"LDA 向量化失败：{e}"
    if X.shape[0] < 3 or X.shape[1] < 3:
        return [], "矩阵过稀疏，跳过 LDA。"
    lda = LatentDirichletAllocation(
        n_components=n_topics,
        max_iter=30,
        learning_method="batch",
        random_state=42,
        n_jobs=1,
    )
    try:
        lda.fit(X)
    except Exception as e:
        return [], f"LDA 拟合失败：{e}"
    names = vec.get_feature_names_out()
    topics: list[list[str]] = []
    for topic_idx, topic in enumerate(lda.components_):
        top_ix = np.argsort(-topic)[:n_top_words]
        topics.append([str(names[i]) for i in top_ix])
    return topics, ""


def _md_escape(s: str) -> str:
    return (s or "").replace("|", "\\|").replace("\n", " ")


def _narrative_stub(
    n_raw: int,
    n_cut: int,
    tf_top: list[tuple[str, int]],
    tfidf_top: list[tuple[str, float]],
    cooc: list[tuple[str, str, int]],
    lda_topics: list[list[str]],
    lda_note: str,
) -> str:
    lines: list[str] = [
        f"本细类有效评论约 **{n_cut}** 条（原始非空 **{n_raw}** 条，经分词去停用后用于建模）。",
        "",
        "**词频 Top**："
        + (
            "、".join(f"「{w}」({n})" for w, n in tf_top[:12])
            if tf_top
            else "（无）"
        )
        + "。",
        "",
        "**TF-IDF 加权 Top**（相对区分度）："
        + (
            "、".join(f"「{w}」({s:.3f})" for w, s in tfidf_top[:12])
            if tfidf_top
            else "（无）"
        )
        + "。",
        "",
        "**共现较强的词对**（同条评论内，供联想维度）："
        + (
            "；".join(f"「{a}」-「{b}」({c})" for a, b, c in cooc[:10])
            if cooc
            else "（无）"
        )
        + "。",
        "",
    ]
    if lda_note:
        lines.append(f"*LDA：{lda_note}*")
        lines.append("")
    elif lda_topics:
        lines.append("**LDA 主题（无监督，仅作探索）**：")
        for i, words in enumerate(lda_topics):
            lines.append(f"- 主题 {i + 1}：{'、'.join(words)}")
        lines.append("")
    lines.append(
        "> 以上为主题探索与统计摘要，**不等同**于业务结论；若与星级、规则词表冲突，以人工抽样为准。"
    )
    return "\n".join(lines)


def _effective_focus_words(run_dir: Path) -> tuple[str, ...]:
    """与 ``runner.write_competitor_analysis_for_run_dir`` 一致：优先 ``effective_report_config.json``。"""
    p = run_dir / "effective_report_config.json"
    if p.is_file():
        try:
            eff = json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            eff = None
        if isinstance(eff, dict):
            fw_src = eff.get("comment_focus_words") or list(jcr.COMMENT_FOCUS_WORDS)
            fw_tuple = tuple(
                str(x).strip() for x in fw_src if str(x).strip()
            ) or jcr.COMMENT_FOCUS_WORDS
            return fw_tuple
    return jcr.COMMENT_FOCUS_WORDS


def _truncate_probe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """压缩摘录长度，避免单次 JSON 顶满上下文。"""
    out: dict[str, Any] = dict(payload)
    groups: list[Any] = []
    for g in (out.get("groups") or []):
        if not isinstance(g, dict):
            groups.append(g)
            continue
        g2 = dict(g)
        sn = g2.get("sample_text_snippets")
        if isinstance(sn, list):
            g2["sample_text_snippets"] = [str(x)[:260] for x in sn[:12]]
        groups.append(g2)
    out["groups"] = groups
    return out


def _merge_snippets_from_comment_groups(
    probe_rows: list[dict[str, Any]],
    *,
    merged_rows: list[dict[str, str]],
    comment_rows: list[dict[str, str]],
    run_dir: Path,
) -> None:
    """把正式 ``build_comment_groups_llm_payload`` 中的摘录并入探针行（原地修改）。"""
    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    title_h = MERGED_FIELD_TO_CSV_HEADER["title"]
    fb = jcr._consumer_feedback_by_matrix_group(
        merged_rows=merged_rows,
        comment_rows=comment_rows,
        sku_header=sku_h,
    )
    fw = _effective_focus_words(run_dir)
    pl = jcr.build_comment_groups_llm_payload(
        feedback_groups=fb,
        focus_words=fw,
        merged_rows=merged_rows,
        sku_header=sku_h,
        title_h=title_h,
    )
    by_g = {str(x.get("group")): x for x in pl if isinstance(x, dict)}
    for row in probe_rows:
        if row.get("probe_status") != "ok":
            continue
        gname = str(row.get("group") or "")
        src = by_g.get(gname)
        if not src:
            continue
        row["comment_flat_rows"] = src.get("comment_flat_rows")
        fh = src.get("focus_hit_lines")
        if isinstance(fh, list):
            row["focus_hit_lines"] = [str(x) for x in fh[:14]]
        sn = src.get("sample_text_snippets")
        if isinstance(sn, list):
            row["sample_text_snippets"] = [str(x)[:300] for x in sn[:14]]


def _run_probe_text_mining_llm(
    payload: dict[str, Any],
    *,
    chunked: bool,
) -> str:
    """探针专用：``PROBE_TEXT_MINING_SYSTEM`` + 结构化 JSON；可选按细类拆分调用。"""
    if not payload.get("groups"):
        return "> **探针 LLM 解读**：无分组数据，跳过。"
    try:
        if not chunked:
            p = _truncate_probe_payload(payload)
            raw = json.dumps(p, ensure_ascii=False)
            if len(raw) > 88_000:
                raw = (
                    raw[:82_000]
                    + "\n\n…（JSON 过长已截断，仅依据可见字段撰写。）\n"
                )
            return _call_llm(
                PROBE_TEXT_MINING_SYSTEM,
                PROBE_TEXT_MINING_USER_PREFIX + raw,
            ).strip()
        kw = str(payload.get("keyword") or "")
        note = str(payload.get("probe_note") or "")
        parts: list[str] = []
        for g in payload.get("groups") or []:
            if not isinstance(g, dict):
                continue
            gname = str(g.get("group") or "?")
            if g.get("probe_status") != "ok":
                parts.append(
                    f"#### {gname}\n\n"
                    f"*（未做探针：{g.get('reason', '')}）*"
                )
                continue
            mini = {
                "schema_version": payload.get("schema_version", 1),
                "keyword": kw,
                "probe_note": note,
                "groups": [g],
            }
            raw = json.dumps(mini, ensure_ascii=False)
            if len(raw) > 48_000:
                raw = raw[:44_000] + "\n…\n"
            parts.append(
                _call_llm(
                    PROBE_TEXT_MINING_SYSTEM,
                    PROBE_TEXT_MINING_USER_PREFIX + raw,
                ).strip()
            )
        return "\n\n---\n\n".join(parts)
    except Exception as e:
        return f"> **探针 LLM 解读**调用失败：{e}"


def build_markdown(
    run_dir: Path,
    *,
    min_texts: int,
    lda_topics_n: int,
    top_k_words: int,
    cooc_vocab: int,
    cooc_pairs: int,
    live_llm: bool,
    llm_chunked: bool,
    wordcloud_enabled: bool,
    wordcloud_max: int,
) -> str:
    kw, merged, comments = _load_run(run_dir)
    sku_h = MERGED_FIELD_TO_CSV_HEADER["sku_id"]
    groups = jcr._consumer_feedback_by_matrix_group(
        merged_rows=merged,
        comment_rows=comments,
        sku_header=sku_h,
    )

    lines: list[str] = [
        "# 八、消费者反馈与用户画像（文本挖掘探针 · 实验稿）",
        "",
        f"- **运行目录**：`{run_dir}`",
        f"- **监测词（run_meta）**：{kw or '—'}",
        f"- **生成脚本**：`pipeline.demos.chapter8_text_mining_probe`",
        "",
        "## 8.0 说明",
        "",
        "本稿为**独立探针**，流程参考「清洗 → 词云（可选）→ 词频/TF-IDF → 共现 → LDA → 叙事小结」；"
        "文末可选 **探针专用 LLM 解读**（独立 JSON + ``PROBE_TEXT_MINING_SYSTEM``，**非**正式 ``COMMENT_GROUPS_SYSTEM`` / §8.2）。"
        "与线上一致的部分：**细类划分与 SKU 归因**复用 ``jd_competitor_report._consumer_feedback_by_matrix_group``；"
        "其余为 **jieba + sklearn** 的开放词表分析，**不替代**正式报告中的规则统计。",
        "",
        "---",
        "",
    ]
    if wordcloud_enabled and not _WORDCLOUD_AVAILABLE:
        lines.extend(
            [
                "> **词云**：当前环境未安装 ``wordcloud`` / ``matplotlib``，已跳过出图。"
                "请执行：``pip install wordcloud matplotlib``。",
                "",
            ]
        )
    elif wordcloud_enabled and not _font_path_chinese():
        lines.extend(
            [
                "> **词云字体**：未检测到常见中文字体路径，词云可能出现方框；"
                "Windows 可确认 ``C:\\Windows\\Fonts\\msyh.ttc`` 是否存在。",
                "",
            ]
        )

    wc_n = 0
    probe_rows: list[dict[str, Any]] = []
    for gname, _cr_rows, texts in groups:
        n_raw = len([t for t in texts if (t or "").strip()])
        if n_raw < min_texts:
            probe_rows.append(
                {
                    "group": gname,
                    "probe_status": "skipped",
                    "reason": f"有效文本 {n_raw} 条，低于 min_texts={min_texts}",
                }
            )
            lines.extend(
                [
                    f"## {gname}",
                    "",
                    f"*本细类有效文本 {n_raw} 条，低于 ``--min-texts``={min_texts}，跳过。*",
                    "",
                    "---",
                    "",
                ]
            )
            continue

        cut_docs = _docs_cut(texts)
        if len(cut_docs) < 2:
            probe_rows.append(
                {
                    "group": gname,
                    "probe_status": "skipped",
                    "reason": "分词后不足 2 条",
                }
            )
            lines.extend(
                [
                    f"## {gname}",
                    "",
                    "*分词后不足 2 条，跳过。*",
                    "",
                    "---",
                    "",
                ]
            )
            continue

        tf_top = _term_freq_top(cut_docs, top_k_words)
        tfidf_top = _tfidf_top(cut_docs, top_k_words)
        cooc = _cooc_top_pairs(cut_docs, cooc_vocab, cooc_pairs)
        lda_t, lda_err = _lda_topics(cut_docs, lda_topics_n, 12)
        if (lda_err or "").strip():
            lda_obj: dict[str, Any] = {
                "status": "skipped",
                "reason": lda_err.strip(),
            }
        else:
            lda_obj = {"status": "ok", "topics": lda_t}
        probe_rows.append(
            {
                "group": gname,
                "probe_status": "ok",
                "comment_text_units": n_raw,
                "jieba_cut_document_count": len(cut_docs),
                "word_freq_top": [
                    {"term": w, "count": int(n)} for w, n in tf_top[:20]
                ],
                "tfidf_top": [
                    {"term": w, "score": round(float(s), 4)}
                    for w, s in tfidf_top[:20]
                ],
                "cooccurrence_top": [
                    {"term_a": a, "term_b": b, "joint_count": int(c)}
                    for a, b, c in cooc[:15]
                ],
                "lda": lda_obj,
            }
        )

        lines.extend([f"## {gname}", ""])
        if (
            wordcloud_enabled
            and _WORDCLOUD_AVAILABLE
            and wc_n < wordcloud_max
        ):
            freq_wc = _word_freq_from_cut_docs(cut_docs)
            fn = f"wordcloud_probe__{wc_n:02d}_{_slug_safe(gname)}.png"
            img_path = run_dir.resolve() / "report_assets" / fn
            err_wc = _save_wordcloud_png(
                freq_wc,
                img_path,
                font_path=_font_path_chinese(),
            )
            if not err_wc:
                lines.append(
                    f"![词云（本分词词频权重；探针）](report_assets/{fn})"
                )
                lines.append("")
                wc_n += 1
            else:
                lines.append(f"> 词云未生成：{err_wc}")
                lines.append("")
        lines.append(_narrative_stub(
            n_raw,
            len(cut_docs),
            tf_top,
            tfidf_top,
            cooc,
            lda_t,
            lda_err,
        ))
        lines.extend(["", "---", ""])

    _merge_snippets_from_comment_groups(
        probe_rows,
        merged_rows=merged,
        comment_rows=comments,
        run_dir=run_dir,
    )
    llm_payload: dict[str, Any] = {
        "schema_version": 1,
        "keyword": kw,
        "probe_note": (
            "jieba 分词 + 停用词；TF-IDF/共现/LDA 为 sklearn；"
            "与正式报告的关注词规则统计、§8.2 情感口径均不同；"
            "评价摘录字段合并自 build_comment_groups_llm_payload，供语境对照。"
        ),
        "groups": probe_rows,
    }

    lines.extend(
        [
            "",
            "---",
            "",
            "## 探针归纳（大模型 · 文本挖掘专用）",
            "",
            "> 输入为探针 JSON（``schema_version``=1）：每细类含 **word_freq / tfidf / cooccurrence / lda** 及合并后的 **focus_hit_lines、sample_text_snippets**；"
            "系统提示为脚本内 ``PROBE_TEXT_MINING_SYSTEM``，**不是** ``COMMENT_GROUPS_SYSTEM``。",
            "",
        ]
    )
    if live_llm:
        lines.append(_run_probe_text_mining_llm(llm_payload, chunked=llm_chunked))
    else:
        lines.append(
            "> **探针 LLM 解读**：未启用。请使用 ``--live-llm``（需 ``AI_crawler`` 等可用）；"
            "细类很多、单次 JSON 易超长时可加 ``--llm-chunked``（按细类多次调用后拼接）。"
        )

    lines.append("")
    lines.append("*（完）*")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="第八章文本挖掘探针（独立脚本）")
    ap.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="pipeline_runs 下某批次目录（含 keyword_pipeline_merged.csv、comments_flat.csv）",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="输出 Markdown 路径（默认 <run_dir>/chapter8_text_mining_probe.md）",
    )
    ap.add_argument("--min-texts", type=int, default=8, help="细类最少评论条数才分析")
    ap.add_argument("--lda-topics", type=int, default=4, help="LDA 主题数上限（会按样本量裁剪）")
    ap.add_argument("--top-k-words", type=int, default=30, help="词频/TF-IDF 展示长度")
    ap.add_argument("--cooc-vocab", type=int, default=80, help="共现矩阵保留的高频词数")
    ap.add_argument("--cooc-pairs", type=int, default=25, help="输出词对数量")
    ap.add_argument(
        "--live-llm",
        action="store_true",
        help="文末调用探针专用 LLM（PROBE_TEXT_MINING_SYSTEM + 结构化 JSON；需 AI_crawler 等）",
    )
    ap.add_argument(
        "--llm-chunked",
        action="store_true",
        help="按细类拆分多次调用同一探针提示词，防单次 JSON 过长",
    )
    ap.add_argument(
        "--no-wordcloud",
        action="store_true",
        help="不生成词云 PNG（默认生成，需 wordcloud+matplotlib）",
    )
    ap.add_argument(
        "--wordcloud-max",
        type=int,
        default=40,
        help="最多为多少个细类各出一张词云（防文件过多）",
    )
    args = ap.parse_args()

    md = build_markdown(
        args.run_dir,
        min_texts=args.min_texts,
        lda_topics_n=args.lda_topics,
        top_k_words=args.top_k_words,
        cooc_vocab=args.cooc_vocab,
        cooc_pairs=args.cooc_pairs,
        live_llm=args.live_llm,
        llm_chunked=args.llm_chunked,
        wordcloud_enabled=not args.no_wordcloud,
        wordcloud_max=max(0, args.wordcloud_max),
    )
    out = args.out or (args.run_dir.resolve() / "chapter8_text_mining_probe.md")
    out.write_text(md, encoding="utf-8")
    print(f"已写入: {out}", flush=True)


if __name__ == "__main__":
    main()
