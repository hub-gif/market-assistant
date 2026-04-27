"""评价文本单元迭代、星级解析、大模型情感载荷（语义池）；保留旧版 lexicon 辅助函数供测试，不再进入报告主链路。"""
from __future__ import annotations

import hashlib
import random
import re
from collections import Counter
from typing import Any

from pipeline.csv.schema import MERGED_FIELD_TO_CSV_HEADER

from .constants import (
    _COMMENT_CSV_BODY,
    _COMMENT_CSV_SCORE,
    _COMMENT_SCORE_NEG_MAX,
    _COMMENT_SCORE_POS_MIN,
)
from .csv_io import _cell


def _comment_keyword_hits(
    rows: list[dict[str, str]],
    focus_words: tuple[str, ...],
) -> Counter[str]:
    c: Counter[str] = Counter()
    texts: list[str] = []
    for row in rows:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if t:
            texts.append(t)
    blob = "\n".join(texts)
    for w in focus_words:
        if len(w) == 1:
            continue
        n = blob.count(w)
        if n:
            c[w] += n
    return c


def _merge_comment_previews(merged_rows: list[dict[str, str]]) -> str:
    parts: list[str] = []
    for row in merged_rows:
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            parts.append(p)
    return "\n".join(parts)


def _parse_comment_score(val: Any) -> int | None:
    """解析 ``commentScore`` /「评分」列；期望京东 1～5 星，非法或空返回 None。"""
    s = str(val or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        x = float(m.group(1))
    except ValueError:
        return None
    if x < 1 or x > 5:
        return None
    return int(round(x))


def _iter_comment_text_units_and_scores(
    comment_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
) -> tuple[list[str], list[int | None]]:
    """逐条评价正文与同序评分（无则为 None）；无 flat 评论时用合并表 comment_preview 按行兜底（评分均为 None）。"""
    texts: list[str] = []
    scores: list[int | None] = []
    for row in comment_rows:
        t = _cell(row, _COMMENT_CSV_BODY, "tagCommentContent")
        if not t:
            continue
        texts.append(t)
        scores.append(_parse_comment_score(_cell(row, _COMMENT_CSV_SCORE, "commentScore")))
    if texts:
        return texts, scores
    for row in merged_rows:
        p = _cell(
            row,
            MERGED_FIELD_TO_CSV_HEADER["comment_preview"],
            "comment_preview",
        )
        if p:
            texts.append(p)
            scores.append(None)
    return texts, scores


def _iter_comment_text_units(
    comment_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
) -> list[str]:
    """逐条评价正文；无 flat 评论时用合并表 comment_preview 按行兜底。"""
    texts, _ = _iter_comment_text_units_and_scores(comment_rows, merged_rows)
    return texts


_POS_LEX = (
    "好",
    "赞",
    "满意",
    "回购",
    "推荐",
    "不错",
    "喜欢",
    "香",
    "实惠",
    "值得",
    "棒",
    "鲜嫩",
    "好吃",
    "划算",
    "正品",
    "好评",
)
_NEG_LEX = (
    "差",
    "烂",
    "难吃",
    "失望",
    "假",
    "骗",
    "退货",
    "不建议",
    "糟糕",
    "难用",
    "臭",
    "差评",
    "不好",
    "难喝",
    "发霉",
)
# 条形图/摘要用：多字短语优先，避免只显示「硬、差」等单字
_POS_LEXEME_DETAIL = (
    "已经回购很多次",
    "还会再买",
    "值得回购",
    "推荐购买",
    "性价比很高",
    "性价比不错",
    "物美价廉",
    "物流很快",
    "包装很用心",
    "包装完好",
    # 包装/物流等维度：若未命中下列句式，条形图仍可能偏低；关注词「包装」等反映提及频率
    "包装不错",
    "包装很好",
    "独立包装",
    "小包装很方便",
    "包装精美",
    "密封性好",
    "包装严实",
    "快递很快",
    "口感很好",
    "味道不错",
    "很好吃",
    "香而不腻",
    "饱腹感不错",
    "控糖很友好",
    "低糖很适合",
    "代餐很方便",
    "品质很稳定",
    "值得信赖",
    "软硬适中",
)
_NEG_LEXEME_DETAIL = (
    # 质地（保留；与分量类并列，避免统计只突出硬而不计少量）
    "口感偏硬",
    "口感很硬",
    "咬不动",
    "发硬",
    "硬邦邦",
    "口感发粘",
    # 分量/规格（常见生活化抱怨，与条形图、§8.2 归纳对齐）
    "分量少",
    "量太少",
    "太少了",
    "不够吃",
    "一袋很少",
    "比想象少",
    "克重不足",
    "太甜了",
    "甜得发腻",
    "甜度过高",
    "不太好吃",
    "很难吃",
    "味道很奇怪",
    "有股怪味",
    "一股异味",
    "包装破损",
    "漏气受潮",
    "日期不新鲜",
    "临期产品",
    "质量很差",
    "不值这个价",
    "与描述不符",
    "疑似假货",
    "发货特别慢",
    "物流太慢了",
    "售后很差",
    "退款很麻烦",
    "不建议购买",
    "不会再买",
)


def _lex_tuple_classify(*parts: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for tup in parts:
        for w in tup:
            w = (w or "").strip()
            if w and w not in seen:
                seen.add(w)
                out.append(w)
    out.sort(key=len, reverse=True)
    return tuple(out)


_POS_CLASS = _lex_tuple_classify(_POS_LEX, _POS_LEXEME_DETAIL)
_NEG_CLASS = _lex_tuple_classify(_NEG_LEX, _NEG_LEXEME_DETAIL)
_POS_LEX_HITS = tuple(sorted(_POS_LEXEME_DETAIL, key=len, reverse=True))
_NEG_LEX_HITS = tuple(sorted(_NEG_LEXEME_DETAIL, key=len, reverse=True))


def _lexeme_hits_in_texts(
    texts: list[str], lexemes: tuple[str, ...]
) -> list[dict[str, Any]]:
    """每条文本内同一短语只计 1 次；``lexemes`` 宜按长度降序以优先匹配更长表述。"""
    c: Counter[str] = Counter()
    for raw in texts:
        s = (raw or "").strip()
        if not s:
            continue
        seen_line: set[str] = set()
        for k in lexemes:
            if not k or k not in s:
                continue
            if k in seen_line:
                continue
            seen_line.add(k)
            c[k] += 1
    return [{"word": w, "texts_matched": n} for w, n in c.most_common(18)]


def _keyword_sentiment_quadrant(stripped: str) -> str:
    """``pos_only`` | ``neg_only`` | ``mixed`` | ``neutral``（空文本为 neutral）。"""
    if not stripped:
        return "neutral"
    hp = any(k in stripped for k in _POS_CLASS)
    hn = any(k in stripped for k in _NEG_CLASS)
    if hp and hn:
        return "mixed"
    if hp:
        return "pos_only"
    if hn:
        return "neg_only"
    return "neutral"


def _sentiment_quadrant_for_row(
    stripped: str,
    score: int | None,
    *,
    use_score_column: bool,
) -> str:
    if not stripped:
        return "neutral"
    if use_score_column and score is not None:
        if score <= _COMMENT_SCORE_NEG_MAX:
            return "neg_only"
        if score >= _COMMENT_SCORE_POS_MIN:
            return "pos_only"
        return "neutral"
    return _keyword_sentiment_quadrant(stripped)


def _include_in_positive_lexeme_corpus(
    stripped: str,
    score: int | None,
    *,
    use_score_column: bool,
) -> bool:
    """口语短语正向统计语境：评分模式下为 4～5 星；否则为命中正向词（含原「混合」条）。"""
    if not stripped:
        return False
    if use_score_column and score is not None:
        return score >= _COMMENT_SCORE_POS_MIN
    hp = any(k in stripped for k in _POS_CLASS)
    return hp


def _include_in_negative_lexeme_corpus(
    stripped: str,
    score: int | None,
    *,
    use_score_column: bool,
) -> bool:
    """口语短语负向统计语境：评分模式下为 1～2 星；否则为命中负向词（含原「混合」条）。"""
    if not stripped:
        return False
    if use_score_column and score is not None:
        return score <= _COMMENT_SCORE_NEG_MAX
    hn = any(k in stripped for k in _NEG_CLASS)
    return hn


def _comment_sentiment_lexicon(
    texts: list[str],
    scores: list[int | None] | None = None,
) -> dict[str, Any]:
    """
    正/负向粗判（非深度学习）：

    - 若 ``scores`` 与 ``texts`` 等长且**至少有一条非空评分**，则**先按 1～5 星分桶**，再在对应子集内统计
      正向/负向口语短语（条形图）；无评分或非法评分的行仍按**关键词子串**粗判。
    - 否则：与旧版一致，**仅关键词**划分四象限与短语语境。
    """
    use_score_column = bool(
        scores is not None
        and len(scores) == len(texts)
        and any(s is not None for s in scores)
    )
    pos_only = neg_only = mixed = neutral = 0
    corpus_pos_mixed: list[str] = []
    corpus_neg_mixed: list[str] = []
    for i, t in enumerate(texts):
        s = (t or "").strip()
        sc: int | None = None
        if use_score_column:
            sc = scores[i] if scores is not None and i < len(scores) else None
        if not s:
            neutral += 1
            continue
        quad = _sentiment_quadrant_for_row(s, sc, use_score_column=use_score_column)
        if quad == "pos_only":
            pos_only += 1
        elif quad == "neg_only":
            neg_only += 1
        elif quad == "mixed":
            mixed += 1
        else:
            neutral += 1
        if _include_in_positive_lexeme_corpus(s, sc, use_score_column=use_score_column):
            corpus_pos_mixed.append(s)
        if _include_in_negative_lexeme_corpus(s, sc, use_score_column=use_score_column):
            corpus_neg_mixed.append(s)
    total = len(texts)
    pos_lex = _lexeme_hits_in_texts(corpus_pos_mixed, _POS_LEX_HITS)
    neg_lex = _lexeme_hits_in_texts(corpus_neg_mixed, _NEG_LEX_HITS)
    method = "score_then_lexeme" if use_score_column else "keyword_lexicon"
    base_note = (
        "「正向短语」与「负向短语」条形图统计的是预设口语片段在**对应语境**下的命中条数，"
        "每条每短语最多计 1 次；非分词模型。"
    )
    if use_score_column:
        scope_extra = (
            "当前批次启用了**评分列**：四象限以星级为主（1～2 星偏负、4～5 星偏正、3 星为中评、空文本为中性）；"
            "「正向口语短语」仅在 **4～5 星** 评价条内统计；「负向口语短语」仅在 **1～2 星** 评价条内统计；"
            "无评分行仍按关键词子串归入四象限并参与短语语境。"
            "条形图不是全文情感或某维度的完整满意度；未收录说法仍可能出现在关注词与语义池。"
        )
    else:
        scope_extra = (
            "「正向短语」仅在命中正向词表的评价条内统计（含关键词混合条）；"
            "「负向短语」仅在命中负向词表的评价条内统计（含关键词混合条）。"
            "条形图表示的是「预设短语命中条数」，不是全文情感或某维度（如包装、物流）的完整满意度；"
            "若用户用「盒子不错」「没压坏」等未收录说法，仍可能落在关注词「包装」子串与语义池原文中。"
            "预设表无法覆盖全部说法（如「一袋就一点点」），须结合语义池原文。"
        )
    return {
        "method": method,
        "text_units": total,
        "positive_only": pos_only,
        "negative_only": neg_only,
        "mixed_positive_and_negative": mixed,
        "neutral_or_empty": neutral,
        "positive_lexicon_sample": list(_POS_LEX[:10]) + list(_POS_LEXEME_DETAIL[:5]),
        "negative_lexicon_sample": list(_NEG_LEX[:10]) + list(_NEG_LEXEME_DETAIL[:5]),
        "positive_tone_lexeme_hits": pos_lex,
        "negative_tone_lexeme_hits": neg_lex,
        "lexeme_scope_note": base_note + scope_extra,
    }


def build_comment_sentiment_llm_payload(
    texts: list[str],
    *,
    scores: list[int | None] | None = None,
    attributed_texts: list[str] | None = None,
    max_samples_positive: int = 16,
    max_samples_negative: int = 30,
    max_samples_mixed: int = 10,
    max_chars_per_review: int = 300,
    semantic_pool_max: int = 40,
    shuffle_seed: str = "",
) -> dict[str, Any]:
    """
    供大模型做正/负向**语义**归纳：**仅**提供去重后的 ``sample_reviews_semantic_pool``（洗牌抽样原文），
    以及可选 ``star_rating_distribution``（有有效评分列时 1～2 / 3 / 4～5 星条数，**非**预设子串词表）。

    已移除：``comment_sentiment_lexicon``、预设口语短语命中、按关键词/词表机械分桶的样本列表（与报告已废弃口径一致）。
    参数 ``max_samples_*`` 保留签名以兼容旧调用方，**不再使用**。
    """
    _ = (max_samples_positive, max_samples_negative, max_samples_mixed)
    use_score_column = bool(
        scores is not None
        and len(scores) == len(texts)
        and any(s is not None for s in scores)
    )
    use_attr = (
        attributed_texts is not None
        and len(attributed_texts) == len(texts)
    )
    all_unique_disp: list[str] = []
    seen_unique: set[str] = set()
    text_unit_count = 0
    star_dist: dict[str, int] | None = None
    if use_score_column:
        star_dist = {"score_1_2": 0, "score_3": 0, "score_4_5": 0, "no_score": 0}

    for i, t in enumerate(texts):
        s = (t or "").strip()
        if not s:
            continue
        text_unit_count += 1
        disp = (
            (attributed_texts[i] or s).strip()
            if use_attr
            else s
        )
        if disp and disp not in seen_unique:
            seen_unique.add(disp)
            all_unique_disp.append(disp)
        if star_dist is not None and scores is not None:
            sc = scores[i] if i < len(scores) else None
            if sc is None:
                star_dist["no_score"] += 1
            elif sc <= 2:
                star_dist["score_1_2"] += 1
            elif sc == 3:
                star_dist["score_3"] += 1
            else:
                star_dist["score_4_5"] += 1

    def _semantic_pool(seq: list[str], cap: int) -> list[str]:
        """去重列表的洗牌子样本；shuffle_seed 非空时按种子固定顺序以便同任务可复现。"""
        if not seq or cap <= 0:
            return []
        work = list(seq)
        if (shuffle_seed or "").strip():
            h = hashlib.sha256(shuffle_seed.encode("utf-8")).digest()
            rnd = random.Random(int.from_bytes(h[:8], "big"))
            rnd.shuffle(work)
        out: list[str] = []
        for raw in work:
            if len(raw) > max_chars_per_review:
                out.append(raw[:max_chars_per_review] + "…")
            else:
                out.append(raw)
            if len(out) >= cap:
                break
        return out

    semantic_pool = _semantic_pool(all_unique_disp, semantic_pool_max)

    out: dict[str, Any] = {
        "text_unit_count": text_unit_count,
        "unique_attributed_snippets_count": len(all_unique_disp),
        "sample_reviews_semantic_pool": semantic_pool,
        "semantic_pool_note": (
            "为去重后的评价原文抽样（可含细类/SKU/店铺前缀）。请据**整句语义**归纳正向体验与负向抱怨；"
            "勿引用已废弃的预设子串词表、勿把星级分布等同于具体抱怨主题。"
        ),
    }
    if star_dist is not None:
        out["star_rating_distribution"] = star_dist
    return out


__all__ = [
    "build_comment_sentiment_llm_payload",
    "_comment_keyword_hits",
    "_comment_sentiment_lexicon",
    "_iter_comment_text_units",
    "_iter_comment_text_units_and_scores",
    "_merge_comment_previews",
    "_parse_comment_score",
]
