# -*- coding: utf-8 -*-
"""
从本地图片路径或图片 URL 调用 OpenAI 兼容多模态接口，提取配料表等；并提供**纯文本** ``chat/completions`` 供报告/策略等场景复用。

**密钥与网关仅通过环境变量配置**（勿写入代码）：

- ``OPENAI_API_KEY``：API Key（必填）
- ``OPENAI_BASE_URL``：网关根地址，如 ``https://llm.example.com/v1``（必填，勿尾斜杠多余路径）
- ``OPENAI_VISION_MODEL``：视觉模型名（可选，默认 ``Qwen/Qwen3-Omni-30B-A3B``）

**纯文本调用**（``chat_completion_text``）优先使用：

- ``OPENAI_TEXT_MODEL`` 或 ``LLM_TEXT_MODEL``；未设置时依次回退到 ``OPENAI_VISION_MODEL``、``LLM_MODEL``、上述默认。

兼容别名（二选一即可）：``LLM_API_KEY``、``LLM_BASE_URL``、``LLM_MODEL``。

上述变量与 Django 共用 **一份** ``market_assistant/.env``（与本脚本所在 ``backend`` 的上三级目录下的 ``.env``；需 ``pip install python-dotenv``）。

**运行方式**：在下方「运行配置」里改好 ``IMAGE_SOURCE`` 等变量后，直接执行 ``python AI_crawler.py``，无需命令行参数。
"""

from __future__ import annotations

import base64
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
# backend/crawler_copy/jd_pc_search -> parents[3] == market_assistant
_MA_ROOT = Path(__file__).resolve().parents[3]


def _load_market_assistant_dotenv() -> None:
    """先于 LOW_GI_PROJECT_ROOT 解析加载 ``market_assistant/.env``（唯一配置源）。"""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    p = _MA_ROOT / ".env"
    if p.is_file():
        load_dotenv(p)


_load_market_assistant_dotenv()

from _low_gi_root import low_gi_project_root  # noqa: E402

_PROJECT_ROOT = low_gi_project_root()

# ---------------------------------------------------------------------------
# 运行配置（按需修改；启动时不要求命令行参数）
# ---------------------------------------------------------------------------
# 必填：本地图片路径，或 http(s) 图片链接（如京东主图 / 详情图）
IMAGE_SOURCE = "https://img30.360buyimg.com/sku/jfs/t1/390444/8/13018/103574/6982e951Fc44d9d7b/00d62ee56189d75d.jpg.avif"
# IMAGE_SOURCE = "https://img30.360buyimg.com/sku/jfs/t1/382894/31/7432/241977/694cf41aFa27be91e/00d63164ffeb8b46.jpg.avif"

# 提示词：留空则使用 ``PROMPT_DEFAULT``
USER_PROMPT = ""
PROMPT_DEFAULT = (
    "请识别图片中的配料表，只输出配料列表本身，不要将菜品做法、步骤、用料等认为是配料表，不要误识别为食谱；用中文逗号或顿号分隔，"
    "输出为连续一段文字，不要使用多行换行（避免与食谱、做法步骤混淆）。"
    "每种原料名称只出现一次，禁止重复罗列同一添加剂（如磷酸三钾、磷酸三钠等勿循环抄写多遍）；"
    "若图为表格中多行同类添加剂，可概括为「食品添加剂（按国家标准使用）」或合并为一句，勿展开成数百字重复。"
    "【禁止猜测】必须严格依据图中清晰可见的印刷文字归纳；不得根据商品品类、常识或模糊字迹推测、补全、编造任何原料。"
    "若本图无配料表、仅有产品信息/广告、文字被裁切、过小、模糊到无法逐字确认，或你只能「猜」出部分内容，则禁止输出配料列表："
    "请只输出且仅输出一句「无法识别图片中的配料表」（不要解释、不要道歉长文、不要列出疑似项）。"
)

# 拉取远程图时的 Referer（京东图床一般需类似商城域名）
IMAGE_REFERER = "https://www.jd.com/"

TEMPERATURE = 0.0
MAX_TOKENS = 2048
# 部分 Qwen 网关需要关闭 thinking
QWEN_OMNI_TEMPLATE = False
# ---------------------------------------------------------------------------

DEFAULT_MODEL = "Qwen/Qwen3-Omni-30B-A3B"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _normalize_chat_content(content: Any) -> str:
    """
    兼容 OpenAI 兼容网关：``message.content`` 可能是 str，也可能是
    ``[{type:text, text:...}, ...]``；避免对 list 误用 ``.strip()`` 或得到怪异字符串。
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(str(item.get("text") or ""))
                elif "text" in item:
                    parts.append(str(item.get("text") or ""))
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts).strip()
    return str(content).strip()


def normalize_ingredients_text_for_csv(text: str) -> str:
    """
    将配料 OCR 结果压成**单行**，便于 ``detail_ware_export.csv`` / 合并表展示。
    模型常按「一行一项」输出食谱或列表，会产生多换行；合并为非换行文本，行间用中文分号分隔。
    """
    t = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]
    if len(lines) <= 1:
        return lines[0] if lines else ""
    return "；".join(lines)


def _split_ingredient_segments(text: str) -> list[str]:
    """按常见分隔符拆成原料小段（用于检测尾部循环复读）。"""
    t = (text or "").strip()
    if not t:
        return []
    return [p.strip() for p in re.split(r"[；、,，]+", t) if p.strip()]


def sanitize_vision_ingredients_output(text: str) -> str:
    """
    清洗多模态配料识别结果：去掉尾部引号、切除「仅两三种词循环数百次」的模型复读尾巴、超长截断。

    典型故障：真实配料后无限重复「磷酸三钾、磷酸三钠…」，仍因前半段通过业务校验。
    """
    t = (text or "").strip()
    _trail_q = frozenset({'"', "'", "\u201c", "\u201d", "\u2018", "\u2019", "\uff02"})
    while t and t[-1] in _trail_q:
        t = t[:-1].strip()

    segs = _split_ingredient_segments(t)
    if not segs:
        return ""

    min_spam_run = 28
    cut_i = len(segs)
    for i in range(0, max(0, len(segs) - min_spam_run + 1)):
        suf = segs[i:]
        if len(suf) >= min_spam_run and len(set(suf)) <= 3:
            cut_i = i
            break
    segs = segs[:cut_i]
    if not segs:
        return ""

    t = "、".join(segs)

    # 字符级兜底：同一短词组高频重复（未按顿号切分时）
    t = re.sub(
        r"(磷酸三[钾钠][、,，]?\s*){35,}",
        "磷酸三钾、磷酸三钠等（按国家标准使用）",
        t,
    )

    max_chars = 3200
    if len(t) > max_chars:
        cut = t[:max_chars]
        last = max(cut.rfind("、"), cut.rfind("，"), cut.rfind(","), cut.rfind("；"))
        if last > max_chars // 2:
            t = cut[: last + 1] + "…（已截断）"
        else:
            t = cut + "…（已截断）"
    return t.strip()


def _resolve_credentials(
    api_key: str | None,
    base_url: str | None,
    model: str | None,
) -> tuple[str, str, str]:
    """凭证只从环境变量（及可选函数参数）读取，不在代码中写死。"""
    key = (
        (api_key or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY") or "").strip()
    )
    base = (
        (base_url or "").strip().rstrip("/")
        or (
            os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL") or ""
        ).strip().rstrip("/")
    )
    m = (
        (model or "").strip()
        or (
            os.environ.get("OPENAI_VISION_MODEL")
            or os.environ.get("LLM_MODEL")
            or DEFAULT_MODEL
        ).strip()
    )
    if not key:
        raise ValueError("请设置环境变量 OPENAI_API_KEY（或 LLM_API_KEY）")
    if not base:
        raise ValueError(
            "请设置环境变量 OPENAI_BASE_URL（或 LLM_BASE_URL），例如 https://your-gateway.com/v1"
        )
    return key, base, m


def resolve_text_model_name(model: str | None = None) -> str:
    """
    文本补全所用模型：显式 ``model`` 优先，否则读环境变量（见模块文档）。
    """
    m = (model or "").strip()
    if m:
        return m
    for env in (
        "OPENAI_TEXT_MODEL",
        "LLM_TEXT_MODEL",
        "OPENAI_VISION_MODEL",
        "LLM_MODEL",
    ):
        v = (os.environ.get(env) or "").strip()
        if v:
            return v
    return DEFAULT_MODEL


def strip_outer_markdown_fence(text: str) -> str:
    """若模型用 ``` / ```markdown 包裹全文，去掉最外层围栏。"""
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    while lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def chat_completion_text(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
    temperature: float = 0.2,
    max_tokens: int = 8192,
    timeout: int = 300,
    extra_json: dict[str, Any] | None = None,
) -> str:
    """
    OpenAI 兼容网关的**纯文本**多轮占位为 system + user 各一条，与 ``extract_ingredients_from_image`` 共用凭证与端点。
    返回助手消息正文（已 ``strip`` / 兼容 list 型 content）。
    """
    k, b, _ = _resolve_credentials(api_key, base_url, None)
    m = resolve_text_model_name(model)
    body: dict[str, Any] = {
        "model": m,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if extra_json:
        body.update(extra_json)
    r = requests.post(
        f"{b}/chat/completions",
        headers={
            "Authorization": f"Bearer {k}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=timeout,
    )
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = ""
        if e.response is not None:
            snippet = (e.response.text or "")[:1200].replace("\r\n", "\n").replace("\n", " ")
        if snippet:
            raise requests.HTTPError(
                f"{e!s} | body: {snippet}",
                response=e.response,
                request=e.request,
            ) from e
        raise
    data = r.json()
    msg = (data.get("choices") or [{}])[0].get("message") or {}
    return _normalize_chat_content(msg.get("content"))


def _mime_for_path(path: str) -> str:
    ext = path.lower().rsplit(".", 1)[-1]
    return {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "gif": "image/gif",
        "avif": "image/avif",
    }.get(ext, "image/jpeg")


def _mime_from_response(url: str, content_type: str | None) -> str:
    if content_type and content_type.lower().startswith("image/"):
        return content_type.split(";")[0].strip().lower()
    u = url.lower().split("?")[0]
    for suf, mime in (
        (".png", "image/png"),
        (".webp", "image/webp"),
        (".avif", "image/avif"),
        (".gif", "image/gif"),
        (".jpg", "image/jpeg"),
        (".jpeg", "image/jpeg"),
    ):
        if u.endswith(suf):
            return mime
    return "image/jpeg"


def image_to_data_url(
    source: str,
    *,
    referer: str = "https://www.jd.com/",
    timeout: int = 60,
) -> tuple[str, str]:
    """
    ``source`` 为本地路径或以 http(s) 开头的 URL。
    返回 (data_url, 来源说明)。
    """
    s = source.strip()
    if s.lower().startswith(("http://", "https://")):
        headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
            "Referer": referer,
        }
        r = requests.get(s, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        mime = _mime_from_response(s, r.headers.get("Content-Type"))
        b64 = base64.standard_b64encode(r.content).decode("ascii")
        return f"data:{mime};base64,{b64}", f"url:{s[:80]}"

    with open(s, "rb") as f:
        raw = f.read()
    mime = _mime_for_path(s)
    b64 = base64.standard_b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{b64}", f"file:{s}"


def extract_ingredients_from_image(
    image_path_or_url: str,
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    model: str | None = None,
    user_prompt: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 2048,
    referer: str = "https://www.jd.com/",
    extra_json: dict[str, Any] | None = None,
    prompt_default: str | None = None,
) -> str:
    """
    从本地图片路径或图片 URL 识别配料表（可改 ``user_prompt`` 扩展为营养成分表等）。
    未传入 ``api_key`` / ``base_url`` / ``model`` 时从环境变量读取。
    返回值为经 ``normalize_ingredients_text_for_csv`` 处理后的**单行**文本，便于写入 CSV。
    """
    k, b, m = _resolve_credentials(api_key, base_url, model)

    data_url, _src = image_to_data_url(image_path_or_url, referer=referer)

    _fallback = (
        prompt_default
        or "请识别图片中的配料表，只输出配料列表，不要误识别为做法用料；用逗号或顿号分隔为一段，不要换行分段。"
    )
    prompt = user_prompt if user_prompt is not None and str(user_prompt).strip() else _fallback

    body: dict[str, Any] = {
        "model": m,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_url}},
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if extra_json:
        body.update(extra_json)

    r = requests.post(
        f"{b}/chat/completions",
        headers={
            "Authorization": f"Bearer {k}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    msg = (data.get("choices") or [{}])[0].get("message") or {}
    raw = normalize_ingredients_text_for_csv(_normalize_chat_content(msg.get("content")))
    return sanitize_vision_ingredients_output(raw)


def parse_joined_image_urls(joined: str) -> list[str]:
    """
    解析详情 DOM 拼出的 URL 串（与列 ``detail_body_ingredients`` 在「仅 URL」阶段同形：分号、换行分隔的 http(s) 链接）。
    保持从前到后的顺序；去重不在这里做（上游已去重）。
    """
    t = (joined or "").strip()
    if not t:
        return []
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    parts = re.split(r"\s*;\s*|\s*\n\s*", t)
    out: list[str] = []
    for p in parts:
        u = p.strip()
        if u.startswith(("http://", "https://")):
            out.append(u)
    return out


def _looks_like_recipe_or_dish_prep(text: str) -> bool:
    """
    判断模型输出是否更像**菜谱/做法备料**（详情图里常见），而非包装「配料表」。
    命中则不应写入 ``detail_body_ingredients``，继续尝试其它长图。
    """
    t = (text or "").strip()
    if not t:
        return False

    recipe_kw = (
        "做法",
        "制作步骤",
        "烹饪步骤",
        "第一步",
        "第二步",
        "第三步",
        "教程",
        "准备食材",
        "食材准备",
        "下锅",
        "翻炒",
        "煮熟",
        "大火烧开",
        "转小火",
        "装盘",
        "小贴士",
        "腌制",
        "爆香",
        "焯水",
        "切丝",
        "切丁",
        "切片",
        "打匀",
        "搅拌均匀",
        "油热",
        "调味",
    )
    if any(k in t for k in recipe_kw):
        return True

    # 「葱花蒜末 各1勺」类菜谱用量
    if re.search(r"各[一二两三四五六七八九十\d零]+勺", t):
        return True

    # 多条「短名称 + 数量 + 料理常用单位」并列（典型备料清单）
    dish_qty = re.findall(
        r"[^\n；。,，、]{1,14}\s+\d+(?:\.\d+)?\s*[个只根块片勺条袋包杯碗适量克gG毫升mlML]{1,4}",
        t,
    )
    if len(dish_qty) >= 2:
        return True

    # 半块/半根等家常分量词 + 生鲜食材名（包装配料表极少这样写）
    if "半块" in t and re.search(r"鸡胸|鸡腿|牛肉|猪肉|黄瓜|番茄|土豆|豆腐", t):
        return True
    if "半根" in t and re.search(r"黄瓜|胡萝卜|玉米|香肠|葱", t):
        return True

    # 规范化后的「A；B；C；…」若多段都很短且多段含数字，多为做法用料枚举
    parts = [p.strip() for p in t.split("；") if p.strip()]
    if len(parts) >= 4:
        short_with_digit = [p for p in parts if len(p) <= 24 and re.search(r"\d", p)]
        if len(short_with_digit) >= 4:
            return True

    # 多行/多段里至少 3 条「短句 + 数字 + 个根块勺克」
    lines = [ln.strip() for ln in re.split(r"[\n；]", t) if ln.strip()]
    if len(lines) >= 3:
        n_short_qty = sum(
            1
            for ln in lines
            if len(ln) <= 22
            and re.search(r"\d", ln)
            and re.search(r"[个只根块片勺克gG]", ln)
        )
        if n_short_qty >= 3:
            return True

    return False


def _looks_like_packaged_ingredient_enumeration(text: str) -> bool:
    """
    视觉模型常把包装图上的「配料表」整段压成**逗号/顿号分隔的原料枚举**，丢掉标题与含量行。
    此类文本与菜谱备料（鸡胸、黄瓜、葱花等）可区分时，视为有效配料信号。
    """
    t = (text or "").strip()
    if not t:
        return False

    parts = [p.strip() for p in re.split(r"[,，、;；]", t) if p.strip()]
    if len(parts) < 3:
        return False

    # 多段像「家常备料」则不走此路（避免鸡胸、鸡蛋、黄瓜…误过）
    recipe_seg = re.compile(
        r"鸡胸|鸡腿|牛腩|牛肉|五花肉|里脊|鸡蛋|鸭蛋|皮蛋|黄瓜|番茄|西红柿|土豆|马铃薯|"
        r"葱花|蒜末|姜丝|小米椒|青椒|洋葱|胡萝卜|生菜|菠菜|白菜|芹菜|香菜|小葱|"
        r"面条$|挂面|粉条|粉丝"
    )
    n_recipe_like = sum(1 for p in parts if recipe_seg.search(p))
    if n_recipe_like >= 2:
        return False

    # 工业化配料常见子串（粉体、纤维、添加剂类别、粮谷原料等）
    industrial = re.compile(
        r"食用|食品添加|麦麸|纤维|淀粉|魔芋|提取物|谷朊|谷胱|麸皮|糖浆|山梨|麦芽|柠檬酸|碳酸|"
        r"酵母|乳粉|全脂|脱脂|果胶|黄原|卡拉胶|海藻酸|小麦|面粉|荞麦|燕麦|藜麦|青稞|糙米|黑米|"
        r"棕榈|植物油|精炼油|氢化|起酥|可可脂"
    )
    n_industrial = sum(1 for p in parts if industrial.search(p))
    if len(parts) >= 4 and n_industrial >= 2:
        return True
    if len(parts) >= 3 and n_industrial >= 3:
        return True
    return False


def _has_packaged_ingredient_table_signals(text: str) -> bool:
    """
    正向判断：是否像**包装配料表**——标题+含量、行内含量、或（多段工业化原料枚举）。

    仅 OCR 出一段家常食材名、无上述结构时，返回 False。
    """
    t = (text or "").strip()
    if not t:
        return False

    # 行内「××（含量≥50%）」等，常见于包装，不强制出现「配料表」标题
    if re.search(
        r"[\u4e00-\u9fff\w·．\d]{1,18}[（(]\s*含量\s*[≥>=＝]?\s*[\d.]+\s*%?\s*[)）]",
        t,
    ):
        return True

    label = bool(
        re.search(r"配料表", t)
        or re.search(r"配\s*料\s*[:：]", t)
        or re.search(r"原\s*料\s*[:：]", t)
        or re.search(r"食品添加剂", t)
        or re.search(r"产品\s*配\s*料", t)
    )

    # 「含量」相关信息：百分比、不等式、法规用语、添加量表述等（不含单独「50克」类菜谱用量）
    content = bool(
        re.search(r"含量", t)
        or re.search(r"添加量", t)
        or re.search(r"\d+(?:\.\d+)?\s*[％%]", t)
        or re.search(r"[≥＞>]\s*[\d.]+", t)
        or re.search(r"按\s*添\s*加\s*量\s*递\s*减", t)
    )

    if label and content:
        return True

    # 模型只输出「原料1,原料2,…」时仍可能是正规配料表
    if _looks_like_packaged_ingredient_enumeration(t):
        return True

    return False


def _ingredient_extraction_acceptable(text: str) -> bool:
    """粗判模型输出是否像有效配料信息（过滤拒识句、伪列表、过短碎片、菜谱备料）。

    通过条件之一：配料表标题+含量类信号；行内「××（含量≥x%）」；或多段工业化原料枚举（见
    ``_looks_like_packaged_ingredient_enumeration``，用于模型只输出逗号分隔原料、丢掉标题时）。
    """
    t = (text or "").strip()
    if len(t) < 6:
        return False
    # 模型偶发输出类似 Python 列表的字符串，或 JSON 数组形态
    if re.match(r"^\s*\[.*\]\s*$", t):
        return False
    refuse = (
        "无法识别",
        "没有配料",
        "看不清",
        "不存在配料",
        "未在图中",
        "未在图片",
        "抱歉，我",
        "抱歉，无法",
        "不能识别",
        "没有识别到",
        "图中没有",
        "图片中没有",
        "无配料",
        "未见配料",
    )
    if any(x in t for x in refuse):
        return False
    # 真配料表通常含分隔符或足够长；避免「无」「暂无」等被当成命中
    if t in ("无", "暂无", "没有", "无。", "无，"):
        return False
    if _looks_like_recipe_or_dish_prep(t):
        return False
    if not _has_packaged_ingredient_table_signals(t):
        return False
    tail = _split_ingredient_segments(t)
    if len(tail) >= 32:
        if len(set(tail[-32:])) <= 3:
            return False
    sep_chars = "，、,;；"
    if len(t) < 18 and not any(c in t for c in sep_chars):
        return False
    return True


REASON_NO_BODY_URLS = "【未识别到配料】未解析到任何详情长图 URL。"
REASON_NO_VISION_API = (
    "【未识别到配料】未配置多模态 API（需环境变量 OPENAI_API_KEY + OPENAI_BASE_URL，"
    "或 LLM_API_KEY + LLM_BASE_URL）。"
)


def extract_ingredients_from_body_image_urls_reversed_with_source(
    urls_joined: str,
    *,
    referer: str | None = None,
    user_prompt: str | None = None,
    prompt_default: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    extra_json: dict[str, Any] | None = None,
) -> tuple[str, str | None]:
    """
    与 ``extract_ingredients_from_body_image_urls_reversed`` 相同逻辑；额外返回命中配料时所用的**图片 URL**
    （自后向前首次通过校验的那张）。未命中或失败时第二项为 ``None``。
    """
    urls = parse_joined_image_urls(urls_joined)
    if not urls:
        return REASON_NO_BODY_URLS, None
    try:
        _resolve_credentials(None, None, None)
    except ValueError:
        return REASON_NO_VISION_API, None

    ref = (referer if referer is not None else IMAGE_REFERER) or "https://www.jd.com/"
    temp = float(temperature) if temperature is not None else float(TEMPERATURE)
    mt = int(max_tokens) if max_tokens is not None else int(MAX_TOKENS)
    extra = extra_json
    if extra is None and QWEN_OMNI_TEMPLATE:
        extra = {"chat_template_kwargs": {"enable_thinking": False}}

    pu = user_prompt if user_prompt is not None else ((USER_PROMPT or "").strip() or None)
    pd = prompt_default if prompt_default is not None else PROMPT_DEFAULT

    n = len(urls)
    n_err = 0
    n_rejected = 0
    for url in reversed(urls):
        try:
            text = extract_ingredients_from_image(
                url,
                user_prompt=pu,
                referer=ref.strip(),
                temperature=temp,
                max_tokens=mt,
                extra_json=extra,
                prompt_default=pd,
            )
        except Exception:
            n_err += 1
            continue
        t = (text or "").strip()
        if _ingredient_extraction_acceptable(t):
            return t, url
        if t:
            n_rejected += 1

    parts = [
        f"【未识别到配料】已对 {n} 张详情长图自后向前依次尝试（命中即停），未得到有效配料表。"
    ]
    if n_err:
        parts.append(f" 请求异常 {n_err} 次。")
    if n_rejected:
        parts.append(f" 有 {n_rejected} 次返回未通过配料校验。")
    if not n_err and not n_rejected:
        parts.append(" 模型返回均为空或过短。")
    return "".join(parts), None


def extract_ingredients_from_body_image_urls_reversed(
    urls_joined: str,
    *,
    referer: str | None = None,
    user_prompt: str | None = None,
    prompt_default: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    extra_json: dict[str, Any] | None = None,
) -> str:
    """
    对 URL 串拆出的链接 **从后往前**依次调用视觉模型：**首次**通过校验的配料文本立即返回（省时间）。

    若始终无命中：返回以 ``【未识别到配料】`` 开头的原因说明（**不再返回空串**）。
    未配置 API 时返回 ``REASON_NO_VISION_API``。

    命中条件（见 ``_ingredient_extraction_acceptable``）：须像**包装配料表**——「配料/含量」标题结构、
    ``××（含量≥x%）``、或多段工业化原料逗号/顿号枚举（模型常省略标题）；纯家常备料（鸡胸、黄瓜、葱花等）
    仍丢弃并试下一张图。

    若需同时得到所用图片 URL，请用 ``extract_ingredients_from_body_image_urls_reversed_with_source``。
    """
    text, _ = extract_ingredients_from_body_image_urls_reversed_with_source(
        urls_joined,
        referer=referer,
        user_prompt=user_prompt,
        prompt_default=prompt_default,
        temperature=temperature,
        max_tokens=max_tokens,
        extra_json=extra_json,
    )
    return text


def main() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    src = (IMAGE_SOURCE or "").strip()
    if not src:
        print(
            "[AI_crawler] 请在文件顶部设置 IMAGE_SOURCE（图片路径或 URL）后重试。",
            file=sys.stderr,
        )
        sys.exit(2)

    prompt_use = (USER_PROMPT or "").strip() or None
    extra = None
    if QWEN_OMNI_TEMPLATE:
        extra = {"chat_template_kwargs": {"enable_thinking": False}}

    try:
        text = extract_ingredients_from_image(
            src,
            user_prompt=prompt_use,
            referer=(IMAGE_REFERER or "https://www.jd.com/").strip(),
            temperature=float(TEMPERATURE),
            max_tokens=int(MAX_TOKENS),
            extra_json=extra,
            prompt_default=PROMPT_DEFAULT,
        )
    except ValueError as e:
        print(f"[AI_crawler] {e}", file=sys.stderr)
        sys.exit(2)
    except requests.HTTPError as e:
        err_body = ""
        if e.response is not None and e.response.text:
            err_body = e.response.text[:1500]
        print(f"[AI_crawler] HTTP 错误: {e}\n{err_body}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[AI_crawler] 失败: {e}", file=sys.stderr)
        sys.exit(1)

    t = (text or "").strip()
    if _ingredient_extraction_acceptable(t):
        print(t)
    else:
        print(
            "【未通过配料表校验】输出须同时包含包装配料表常见结构（如「配料/配料表/原料/食品添加剂」）"
            "与含量或百分比等信息，或为「××（含量≥x%）」形态；纯食材/菜谱备料枚举不会采纳。"
            "与 extract_ingredients_from_body_image_urls_reversed 流水线规则一致。"
        )
        if t:
            print(f"[AI_crawler] 模型原始输出（未采纳）: {t}", file=sys.stderr)


if __name__ == "__main__":
    main()
