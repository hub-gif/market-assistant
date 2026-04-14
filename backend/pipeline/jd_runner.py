"""
使用 ``crawler_copy/jd_pc_search`` 中的副本脚本执行流水线并生成竞品 Markdown。
依赖环境变量 ``LOW_GI_PROJECT_ROOT``（由 Django settings 从 ``market_assistant/.env`` 注入）。
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from django.conf import settings

from .models import PipelineJob


def merge_llm_supplement_with_rules_report(llm_md: str, rules_md: str) -> str:
    """
    **以规则引擎全文为正文**（含 §5 完整竞品矩阵、各章内嵌统计图与表格）。

    大模型稿作为 **§8.5** 嵌入在 **第八章末、第九章策略** 之前，与 §8.2～8.4 等具体分析同卷连贯，
    **不再**插在篇首「## 一、」之前。
    """
    body = (rules_md or "").strip()
    sup = (llm_md or "").strip()
    if not sup:
        return body
    if not body:
        return sup
    marker = "\n---\n\n## 九、策略与机会提示（假设清单，待验证）"
    insert = (
        "\n\n---\n\n"
        "### 8.5 大模型深度补充（与 §2～§8.4 定量内容互补）\n\n"
        "> **说明**：本段位于**第八章末**；**竞品矩阵、价盘表、统计图与 §8.2～8.4 词频等以正文各节为准**，"
        "此处为跨小节语义整合，便于衔接第九章。\n\n"
        f"{sup.strip()}\n"
        "\n---\n\n## 九、策略与机会提示（假设清单，待验证）"
    )
    if marker in body:
        return body.replace(marker, insert, 1)
    # 旧版报告标题或语言差异时的回退
    for alt in (
        "\n## 九、策略与机会提示（假设清单，待验证）",
        "\n## 九、策略与机会提示",
    ):
        if alt in body and marker not in body:
            return body.replace(
                alt,
                "\n\n---\n\n### 8.5 大模型深度补充（与 §2～§8.4 定量内容互补）\n\n"
                "> **说明**：位于第八章末；**矩阵与图表以正文为准**。\n\n"
                f"{sup.strip()}\n"
                + alt,
                1,
            )
    app = "\n## 附录 A：数据留存说明"
    if app in body:
        tail = (
            "\n\n---\n\n### 8.5 大模型深度补充（与 §2～§8.4 定量内容互补）\n\n"
            f"{sup.strip()}\n"
        )
        return body.replace(app, tail + app, 1)
    return body + "\n\n---\n\n### 8.5 大模型深度补充\n\n" + sup.strip() + "\n"


def merge_llm_report_with_rules_charts(llm_md: str, rules_md: str) -> str:
    """兼容旧名：等价于 ``merge_llm_supplement_with_rules_report``。"""
    return merge_llm_supplement_with_rules_report(llm_md, rules_md)


def inject_section_bridges_into_markdown(md: str, bridges: dict[str, str]) -> str:
    """
    在「## 一、」…「## 九、」各章标题行之后插入 ``#### 衔接分析（大模型）`` 段落。
    自第九章向前替换，避免多次插入导致偏移错位。
    """
    out = md
    for key in "九八七六五四三二一":
        content = (bridges.get(key) or "").strip()
        if not content:
            continue
        pat = re.compile(rf"^(## {key}、[^\n]*)\n", re.MULTILINE)

        def _repl(m: re.Match[str], _c: str = content) -> str:
            return m.group(0) + "\n#### 衔接分析（大模型）\n\n" + _c + "\n\n"

        out = pat.sub(_repl, out, count=1)
    return out


def _flat_comment_texts(comment_rows: list[dict[str, str]]) -> list[str]:
    """全部非空评价正文（与报告统计同源）。"""
    out: list[str] = []
    for row in comment_rows:
        t = (row.get("tagCommentContent") or "").strip()
        if t:
            out.append(t)
    return out


def _safe_dir_segment_for_job(s: str, max_len: int = 48) -> str:
    """与 ``jd_keyword_pipeline._safe_dir_segment`` 一致，避免多线程下改模块全局。"""
    bad = '<>:"/\\|?*\n\r\t'
    t = "".join("_" if c in bad else c for c in (s or "").strip())[:max_len]
    t = t.strip(" .") or "run"
    return t


def resolve_pipeline_run_directory_for_job(job: PipelineJob) -> Path:
    """
    在拉起子进程前固定本次 ``run_dir``（与 ``jd_keyword_pipeline._resolve_pipeline_run_dir`` 同口径）。
    调用方负责 ``mkdir``。
    """
    root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    project_data = Path(root).resolve() / "data" / "JD"
    prd = (job.pipeline_run_dir or "").strip()
    kw = (job.keyword or "").strip()
    if prd:
        p = Path(prd).expanduser()
        if not p.is_absolute():
            p = project_data / p
        return p.resolve()
    import time

    stamp = time.strftime("%Y%m%d_%H%M%S")
    seg = _safe_dir_segment_for_job(kw)
    return (project_data / "pipeline_runs" / f"{stamp}_{seg}").resolve()


def try_write_competitor_report_if_merged_exists(
    run_dir: Path,
    keyword: str,
    *,
    report_config: dict[str, Any] | None = None,
) -> None:
    """若已有合并表则补写竞品 Markdown（用于子进程被 terminate 后的部分产物）。"""
    _, kpl = _jd_crawler_modules()
    base = Path(run_dir).resolve()
    merged = base / kpl.FILE_MERGED_CSV
    if not merged.is_file():
        return
    try:
        write_competitor_analysis_for_run_dir(
            base, keyword, report_config=report_config
        )
    except Exception:
        pass


def _jd_crawler_modules():
    root = Path(settings.CRAWLER_JD_ROOT)
    if not root.is_dir():
        raise FileNotFoundError(f"爬虫副本目录不存在: {root}")
    root_s = str(root.resolve())
    if root_s not in sys.path:
        sys.path.insert(0, root_s)
    import jd_competitor_report as jcr  # noqa: WPS433
    import jd_keyword_pipeline as kpl  # noqa: WPS433

    return jcr, kpl


def get_default_report_config() -> dict[str, Any]:
    """与 ``jd_competitor_report`` 模块常量一致的默认报告调参（供前端回填）。"""
    jcr, _ = _jd_crawler_modules()
    return {
        "llm_comment_sentiment": False,
        "llm_section_bridges": False,
        "comment_focus_words": list(jcr.COMMENT_FOCUS_WORDS),
        "comment_scenario_groups": [
            {"label": lbl, "triggers": list(trs)}
            for lbl, trs in jcr.COMMENT_SCENARIO_GROUPS
        ],
        "external_market_table_rows": [
            {"indicator": a, "value_and_scope": b, "source": c, "year": d}
            for a, b, c, d in jcr.EXTERNAL_MARKET_TABLE_ROWS
        ],
    }


def write_competitor_analysis_for_run_dir(
    run_dir: Path,
    keyword: str,
    *,
    report_config: dict[str, Any] | None = None,
) -> Path:
    """
    在已有流水线目录上读取 CSV / meta，写入 ``competitor_analysis.md``（不重新爬取）。
    """
    jcr, kpl = _jd_crawler_modules()
    kw = (keyword or "").strip()
    if not kw:
        raise ValueError("keyword 不能为空")

    run_dir = Path(run_dir).resolve()
    merged_path = run_dir / kpl.FILE_MERGED_CSV
    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表，无法生成报告: {merged_path.name}")

    _, merged_rows = jcr._read_csv_rows(merged_path)
    _, search_export_rows = jcr._read_csv_rows(run_dir / kpl.FILE_PC_SEARCH_CSV)
    _, comment_rows = jcr._read_csv_rows(run_dir / kpl.FILE_COMMENTS_FLAT_CSV)

    meta_path = run_dir / kpl.FILE_RUN_META_JSON
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = None

    eff_rc: dict[str, Any] = (
        dict(report_config) if isinstance(report_config, dict) else {}
    )
    all_tx = _flat_comment_texts(comment_rows)
    suggest_path = run_dir / "keyword_suggest_llm.json"
    suggest_record: dict[str, Any] = {
        "schema_version": 3,
        "total_comment_texts": len(all_tx),
    }
    skip_kw = os.environ.get("MA_SKIP_LLM_KEYWORD_SUGGEST", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if not skip_kw:
        try:
            from .llm_keyword_suggest import suggest_focus_keywords_from_all_comments

            brief_pre = jcr.build_competitor_brief(
                run_dir=run_dir,
                keyword=kw,
                merged_rows=merged_rows,
                search_export_rows=search_export_rows,
                comment_rows=comment_rows,
                meta=meta,
                report_config=eff_rc,
            )
            brief_slice = {
                "keyword": brief_pre.get("keyword"),
                "comment_focus_keywords": (
                    brief_pre.get("comment_focus_keywords") or []
                )[:20],
                "usage_scenarios": (brief_pre.get("usage_scenarios") or [])[:8],
                "category_mix_top": (brief_pre.get("category_mix_top") or [])[:6],
                "scope": brief_pre.get("scope"),
            }
            sug = suggest_focus_keywords_from_all_comments(
                keyword=kw,
                brief_slice=brief_slice,
                all_comment_texts=all_tx,
            )
            suggest_record.update(sug)
            base_words = list(eff_rc.get("comment_focus_words") or [])
            for w in sug.get("suggested_focus_keywords") or []:
                if isinstance(w, str):
                    t = w.strip()
                    if t and t not in base_words:
                        base_words.append(t)
            eff_rc["comment_focus_words"] = base_words[:80]
        except Exception as e:
            suggest_record["error"] = str(e)
            suggest_record["suggested_focus_keywords"] = []
    else:
        suggest_record["skipped"] = True
        suggest_record["suggested_focus_keywords"] = []

    skip_scen = os.environ.get("MA_SKIP_LLM_SCENARIO_SUGGEST", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if not skip_scen:
        try:
            from .llm_keyword_suggest import suggest_scenario_groups_llm

            raw_sg = eff_rc.get("comment_scenario_groups")
            if isinstance(raw_sg, list) and raw_sg:
                scen_base = [x for x in raw_sg if isinstance(x, dict)]
            else:
                scen_base = [
                    {"label": lbl, "triggers": list(trs)}
                    for lbl, trs in jcr.COMMENT_SCENARIO_GROUPS
                ]
            scen_out = suggest_scenario_groups_llm(
                keyword=kw,
                existing_groups=scen_base,
                all_comment_texts=all_tx,
            )
            suggest_record["suggested_scenario_groups"] = scen_out.get(
                "suggested_scenario_groups"
            ) or []
            suggest_record["scenario_rationale"] = scen_out.get("scenario_rationale") or ""
            exist_labels = {
                str(x.get("label") or "").strip().lower()
                for x in scen_base
                if str(x.get("label") or "").strip()
            }
            merged_scen = list(scen_base)
            for g in suggest_record["suggested_scenario_groups"]:
                if not isinstance(g, dict):
                    continue
                lab = str(g.get("label") or "").strip()
                tr_in = g.get("triggers")
                triggers: list[str] = []
                if isinstance(tr_in, list):
                    for t in tr_in[:48]:
                        s = str(t).strip()
                        if 2 <= len(s) <= 48:
                            triggers.append(s)
                if not lab or lab.lower() in exist_labels or len(triggers) < 2:
                    continue
                merged_scen.append({"label": lab[:80], "triggers": triggers[:48]})
                exist_labels.add(lab.lower())
            eff_rc["comment_scenario_groups"] = merged_scen[:40]
        except Exception as e:
            suggest_record["scenario_error"] = str(e)
            suggest_record["suggested_scenario_groups"] = []
    else:
        suggest_record["scenario_skipped"] = True
        suggest_record["suggested_scenario_groups"] = []

    suggest_path.write_text(
        json.dumps(suggest_record, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "effective_report_config.json").write_text(
        json.dumps(eff_rc, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    brief_final = jcr.build_competitor_brief(
        run_dir=run_dir,
        keyword=kw,
        merged_rows=merged_rows,
        search_export_rows=search_export_rows,
        comment_rows=comment_rows,
        meta=meta,
        report_config=eff_rc,
    )
    from .report_charts import generate_report_charts

    generate_report_charts(run_dir, brief_final)

    llm_sentiment_md = ""
    sentiment_llm_record: dict[str, Any] = {
        "schema_version": 1,
        "attempted": False,
    }
    skip_sent = os.environ.get(
        "MA_SKIP_LLM_COMMENT_SENTIMENT", ""
    ).strip().lower() in ("1", "true", "yes")
    env_on = os.environ.get("MA_ENABLE_LLM_COMMENT_SENTIMENT", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    want_sent = bool(eff_rc.get("llm_comment_sentiment")) or env_on
    if want_sent and not skip_sent:
        comment_units = jcr._iter_comment_text_units(comment_rows, merged_rows)
        if len(comment_units) >= 2:
            sentiment_llm_record["attempted"] = True
            try:
                from .llm_generate import generate_comment_sentiment_analysis_llm

                pl = jcr.build_comment_sentiment_llm_payload(
                    comment_units,
                    max_samples_positive=16,
                    max_samples_negative=30,
                    max_samples_mixed=10,
                    max_chars_per_review=300,
                )
                pl["keyword"] = kw
                llm_sentiment_md = generate_comment_sentiment_analysis_llm(pl)
                sentiment_llm_record["ok"] = True
                sentiment_llm_record["chars"] = len(llm_sentiment_md)
            except Exception as e:
                sentiment_llm_record["ok"] = False
                sentiment_llm_record["error"] = str(e)
        else:
            sentiment_llm_record["skipped"] = "insufficient_comment_texts"
    elif skip_sent:
        sentiment_llm_record["skipped"] = "MA_SKIP_LLM_COMMENT_SENTIMENT"
    elif not want_sent:
        sentiment_llm_record["skipped"] = "not_enabled"

    (run_dir / "comment_sentiment_llm.json").write_text(
        json.dumps(sentiment_llm_record, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    md = jcr.build_competitor_markdown(
        run_dir=run_dir,
        keyword=kw,
        merged_rows=merged_rows,
        search_export_rows=search_export_rows,
        comment_rows=comment_rows,
        meta=meta,
        report_config=eff_rc,
        llm_sentiment_section_md=llm_sentiment_md or None,
    )

    bridge_record: dict[str, Any] = {
        "schema_version": 1,
        "attempted": False,
    }
    skip_bridge = os.environ.get(
        "MA_SKIP_LLM_SECTION_BRIDGES", ""
    ).strip().lower() in ("1", "true", "yes")
    env_bridge = os.environ.get(
        "MA_ENABLE_LLM_SECTION_BRIDGES", ""
    ).strip().lower() in ("1", "true", "yes")
    want_bridge = bool(eff_rc.get("llm_section_bridges")) or env_bridge
    if want_bridge and not skip_bridge:
        from .llm_generate import (
            generate_section_bridges_llm,
            split_competitor_report_for_bridges,
        )

        parts = split_competitor_report_for_bridges(md)
        if parts:
            bridge_record["attempted"] = True
            try:
                bridges = generate_section_bridges_llm(
                    keyword=kw, brief=brief_final, sections=parts
                )
                bridge_record["keys_received"] = sorted(bridges.keys())
                md = inject_section_bridges_into_markdown(md, bridges)
                bridge_record["ok"] = True
            except Exception as e:
                bridge_record["ok"] = False
                bridge_record["error"] = str(e)
        else:
            bridge_record["skipped"] = "no_h2_sections_matched"
    elif skip_bridge:
        bridge_record["skipped"] = "MA_SKIP_LLM_SECTION_BRIDGES"
    elif not want_bridge:
        bridge_record["skipped"] = "not_enabled"

    (run_dir / "section_bridge_llm.json").write_text(
        json.dumps(bridge_record, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    out_md = run_dir / "competitor_analysis.md"
    out_md.write_text(md, encoding="utf-8")
    return run_dir


def regenerate_competitor_report(
    run_dir_str: str,
    keyword: str,
    *,
    report_config: dict[str, Any] | None = None,
) -> Path:
    """校验 ``run_dir`` 位于 ``LOW_GI_PROJECT_ROOT/data/JD`` 下后，重写竞品 Markdown。"""
    low_root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not low_root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    base = Path(run_dir_str).expanduser().resolve()
    jd_root = (Path(low_root) / "data" / "JD").resolve()
    try:
        base.relative_to(jd_root)
    except ValueError as e:
        raise ValueError("run_dir 不在京东数据目录下") from e
    return write_competitor_analysis_for_run_dir(
        base, keyword, report_config=report_config
    )


def write_competitor_analysis_markdown(run_dir_str: str, markdown: str) -> Path:
    """将已生成的 Markdown 正文写入 ``run_dir/competitor_analysis.md``（与规则重生成同路径）。"""
    low_root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not low_root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    base = Path(run_dir_str).expanduser().resolve()
    jd_root = (Path(low_root) / "data" / "JD").resolve()
    try:
        base.relative_to(jd_root)
    except ValueError as e:
        raise ValueError("run_dir 不在京东数据目录下") from e
    out = base / "competitor_analysis.md"
    out.write_text(markdown or "", encoding="utf-8")
    return out


def build_competitor_brief_for_job(
    run_dir_str: str,
    keyword: str,
    *,
    report_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    读取 ``run_dir`` 下合并表 / 搜索导出 / 评价 / meta，返回与 Markdown 报告同口径的 **JSON 结构化摘要**（规则驱动）。
    ``run_dir`` 须位于 ``LOW_GI_PROJECT_ROOT/data/JD`` 下。
    """
    low_root = (settings.LOW_GI_PROJECT_ROOT or "").strip()
    if not low_root:
        raise RuntimeError("LOW_GI_PROJECT_ROOT 未配置")
    base = Path(run_dir_str).expanduser().resolve()
    jd_root = (Path(low_root) / "data" / "JD").resolve()
    try:
        base.relative_to(jd_root)
    except ValueError as e:
        raise ValueError("run_dir 不在京东数据目录下") from e

    jcr, kpl = _jd_crawler_modules()
    kw = (keyword or "").strip()
    if not kw:
        raise ValueError("keyword 不能为空")

    merged_path = base / kpl.FILE_MERGED_CSV
    if not merged_path.is_file():
        raise FileNotFoundError(f"缺少合并表，无法生成摘要: {merged_path.name}")

    _, merged_rows = jcr._read_csv_rows(merged_path)
    _, search_export_rows = jcr._read_csv_rows(base / kpl.FILE_PC_SEARCH_CSV)
    _, comment_rows = jcr._read_csv_rows(base / kpl.FILE_COMMENTS_FLAT_CSV)

    meta_path = base / kpl.FILE_RUN_META_JSON
    meta: dict[str, Any] | None = None
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = None

    eff: dict[str, Any] | None = None
    if isinstance(report_config, dict):
        eff = dict(report_config)
    eff_path = base / "effective_report_config.json"
    if eff_path.is_file():
        try:
            loaded = json.loads(eff_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict) and loaded:
                eff = loaded
        except json.JSONDecodeError:
            pass

    return jcr.build_competitor_brief(
        run_dir=base,
        keyword=kw,
        merged_rows=merged_rows,
        search_export_rows=search_export_rows,
        comment_rows=comment_rows,
        meta=meta,
        report_config=eff,
    )


def run_jd_keyword_and_report(
    keyword: str,
    *,
    max_skus: int | None = None,
    page_start: int | None = None,
    page_to: int | None = None,
    pipeline_run_dir: str | None = None,
    cookie_file_path: str | None = None,
    pvid: str | None = None,
    request_delay: str | None = None,
    list_pages: str | None = None,
    scenario_filter_enabled: bool | None = None,
    report_config: dict[str, Any] | None = None,
    cancel_check: Any | None = None,
) -> Path:
    _, kpl = _jd_crawler_modules()

    kw = (keyword or "").strip()
    if not kw:
        raise ValueError("keyword 不能为空")

    backup: dict[str, Any] = {}
    if cancel_check is not None:
        backup["PIPELINE_CANCEL_CHECK"] = getattr(kpl, "PIPELINE_CANCEL_CHECK", None)
        kpl.PIPELINE_CANCEL_CHECK = cancel_check
    try:
        if max_skus is not None:
            backup["MAX_SKUS"] = kpl.MAX_SKUS
            kpl.MAX_SKUS = max(1, int(max_skus))
        if page_start is not None:
            backup["PAGE_START"] = kpl.PAGE_START
            kpl.PAGE_START = max(1, int(page_start))
        if page_to is not None:
            backup["PAGE_TO"] = kpl.PAGE_TO
            kpl.PAGE_TO = max(1, int(page_to))

        prd = (pipeline_run_dir or "").strip()
        if prd:
            backup["PIPELINE_RUN_DIR"] = kpl.PIPELINE_RUN_DIR
            kpl.PIPELINE_RUN_DIR = prd

        cf = (cookie_file_path or "").strip()
        if cf:
            backup["PIPELINE_COOKIE_FILE"] = kpl.PIPELINE_COOKIE_FILE
            kpl.PIPELINE_COOKIE_FILE = cf

        pv = (pvid or "").strip()
        if pv:
            backup["PVID"] = kpl.PVID
            kpl.PVID = pv

        rd = (request_delay or "").strip()
        if rd:
            backup["REQUEST_DELAY"] = kpl.REQUEST_DELAY
            kpl.REQUEST_DELAY = rd

        lp = (list_pages or "").strip()
        if lp:
            backup["LIST_PAGES"] = kpl.LIST_PAGES
            kpl.LIST_PAGES = lp

        if scenario_filter_enabled is not None:
            backup["SCENARIO_FILTER_ENABLED"] = kpl.SCENARIO_FILTER_ENABLED
            kpl.SCENARIO_FILTER_ENABLED = bool(scenario_filter_enabled)

        run_dir = kpl.main(keyword=kw)
    except kpl.PipelineCancelled as e:
        run_dir_path = Path(e.run_dir).resolve()
        merged = run_dir_path / kpl.FILE_MERGED_CSV
        if merged.is_file():
            try:
                write_competitor_analysis_for_run_dir(
                    run_dir_path, kw, report_config=report_config
                )
            except Exception:
                pass
        raise
    finally:
        for name, val in backup.items():
            setattr(kpl, name, val)

    return write_competitor_analysis_for_run_dir(
        Path(run_dir).resolve(), kw, report_config=report_config
    )
