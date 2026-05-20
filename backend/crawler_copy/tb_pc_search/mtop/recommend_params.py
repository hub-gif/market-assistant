# -*- coding: utf-8 -*-
"""
``mtop.relationrecommend.wirelessrecommend.recommend`` 的 URL 与内层 params 构造、默认 H5 请求头。

与 :mod:`mtop.h5_sign`、:mod:`mtop.recommend_client`、:mod:`mtop.transport` 配合使用。
"""
from __future__ import annotations

import argparse
import time
from typing import Any
from urllib.parse import quote, urlencode

from .constants import USER_AGENT
from .transport import mtop_jsonp_script_headers

# 与推荐列表接口路径一致；GET 时在其后拼查询串
MTOP_RECOMMEND_BASE_URL = (
    "https://h5api.m.taobao.com/h5/mtop.relationrecommend.wirelessrecommend.recommend/2.0/"
)
# 兼容旧名
MTOP_PATH = MTOP_RECOMMEND_BASE_URL

# 与 PC 搜索 SRP + Chrome 下发 mtop 时常见默认内层字段（可由 CLI overrides 覆盖）。
# 注：实测 PC SRP Referer + Windows UA 时，内层仍常为 HUAWEI/Android device 与较小 viewResolution，
# 与同条请求里的 ``screenResolution`` ``userAgent`` 并存——与后端模板一致即可，不等同物理机型。
DEFAULT_INNER_PAGE_SOURCE = "a21bo.jianhua/a.search_manual.0"
DEFAULT_VIEW_RESOLUTION = "548x1345"


def build_pc_search_referer(
    q: str,
    page: int = 1,
    *,
    client_preload_id: str | None = None,
    initiative_id: str = "tbindexz_20170306",
    spm: str = "a21bo.jianhua/a.search_manual.0",
    source_id: str = "tb.index",
    commend: str = "all",
    search_type: str = "item",
    tab: str = "all",
    ssid: str = "s5-e",
    pre_load_origin: str = "https://www.taobao.com",
) -> str:
    """
    构造与浏览器 ``s.taobao.com/search?...`` 常见形态一致的 Referer（用于 mtop JSONP 请求）。

    ``client_preload_id`` 省略时按 ``preload_<毫秒时间戳>`` 生成，贴近真实页面。
    """
    preload = client_preload_id if client_preload_id is not None else f"preload_{int(time.time() * 1000)}"
    qry: dict[str, str] = {
        "clientPreloadId": preload,
        "commend": commend,
        "ie": "utf8",
        "initiative_id": initiative_id,
        "page": str(int(page)),
        "preLoadOrigin": pre_load_origin,
        "q": q,
        "search_type": search_type,
        "sourceId": source_id,
        "spm": spm,
        "ssid": ssid,
        "tab": tab,
    }
    return "https://s.taobao.com/search?" + urlencode(qry, encoding="utf-8")


def default_headers(cookie: str, *, referer: str | None = None) -> dict[str, str]:
    """
    兼容旧入口：未传 ``referer`` 时沿用仅域名的短 Referer。

    新逻辑请用 :func:`build_default_mtop_headers`（与 SRP 抓包一致）。
    """
    ref = referer if referer is not None else "https://s.taobao.com/"
    return mtop_jsonp_script_headers(cookie, referer=ref)


def _srp_session_preload_id(args: argparse.Namespace) -> str:
    """未手写 ``preload_id`` 时，单次 ``args`` 生命周期内复用同一 ``preload_<ms>``，避免 SRP 导航与 Referer 头不一致。"""
    raw = getattr(args, "preload_id", None)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    cached = getattr(args, "_tb_srp_session_preload", None)
    if isinstance(cached, str) and cached.strip():
        return cached.strip()
    val = f"preload_{int(time.time() * 1000)}"
    setattr(args, "_tb_srp_session_preload", val)
    return val


def resolve_mtop_referer_url(
    args: argparse.Namespace,
    *,
    srp_page: int = 1,
) -> str:
    """
    与 :func:`build_default_mtop_headers` 所用 Referer URL 一致（SRP 前置导航与 mtop 请求同源）。
    """
    explicit = (getattr(args, "referer", None) or "").strip()
    if explicit:
        return explicit

    if getattr(args, "short_referer", False):
        return "https://s.taobao.com/"

    spm_kw = getattr(args, "spm", None)
    spm = (spm_kw.strip() if isinstance(spm_kw, str) and spm_kw.strip() else DEFAULT_INNER_PAGE_SOURCE)

    return build_pc_search_referer(
        getattr(args, "q", "") or "",
        page=srp_page,
        client_preload_id=_srp_session_preload_id(args),
        spm=spm,
    )


def build_default_mtop_headers(
    cookie: str,
    args: argparse.Namespace,
    *,
    srp_page: int = 1,
) -> dict[str, str]:
    """
    根据 CLI 参数构造推荐请求头：``--referer`` 优先，其次 ``--short-referer``，否则按 ``q`` 拼完整 SRP URL。
    """
    ref = resolve_mtop_referer_url(args, srp_page=srp_page)
    return mtop_jsonp_script_headers(cookie, referer=ref)


def build_inner_params(
    q: str,
    page: int = 1,
    page_size: int = 48,
    *,
    page_source: str | None = None,
    view_resolution: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    与浏览器抓包结构一致的 params 对象（字符串字段多为 "true"/"false"）。
    q 会经 URL 编码后写入 params.q。
    ``page_source`` / ``view_resolution`` 省略时使用与本仓库对照过的 PC SRP 抓包默认值（见你本地 Network 导出）。
    """
    q_encoded = quote(q, safe="")
    ps = page_source if page_source is not None else DEFAULT_INNER_PAGE_SOURCE
    vr = view_resolution if view_resolution is not None else DEFAULT_VIEW_RESOLUTION
    d: dict[str, Any] = {
        "device": "HMA-AL00",
        "isBeta": "false",
        "grayHair": "false",
        "from": "nt_history",
        "brand": "HUAWEI",
        "info": "wifi",
        "index": "4",
        "rainbow": "",
        "schemaType": "auction",
        "elderHome": "false",
        "isEnterSrpSearch": "true",
        "newSearch": "false",
        "network": "wifi",
        "subtype": "",
        "hasPreposeFilter": "false",
        "prepositionVersion": "v2",
        "client_os": "Android",
        "gpsEnabled": "false",
        "searchDoorFrom": "srp",
        "debug_rerankNewOpenCard": "false",
        "homePageVersion": "v7",
        "searchElderHomeOpen": "false",
        "search_action": "initiative",
        "sugg": "_4_1",
        "sversion": "13.6",
        "style": "list",
        "ttid": "600000@taobao_pc_10.7.0",
        "needTabs": "true",
        "areaCode": "CN",
        "vm": "nw",
        "countryNum": "156",
        "m": "pc",
        "page": page,
        "n": page_size,
        "q": q_encoded,
        "qSource": "url",
        "pageSource": ps,
        "channelSrp": "",
        "tab": "all",
        "pageSize": page_size,
        "totalPage": 100,
        "totalResults": 4800,
        "sourceS": "0",
        "sort": "_coefp",
        "bcoffset": "",
        "ntoffset": "",
        "filterTag": "",
        "service": "",
        "prop": "",
        "loc": "",
        "start_price": None,
        "end_price": None,
        "startPrice": None,
        "endPrice": None,
        "itemIds": None,
        "p4pIds": None,
        "p4pS": None,
        "categoryp": "",
        "ha3Kvpairs": None,
        "myCNA": "",
        "screenResolution": "1707x1067",
        "viewResolution": vr,
        "userAgent": USER_AGENT,
        "couponUnikey": "",
        "subTabId": "",
        "np": "",
        "clientType": "h5",
        "isNewDomainAb": "false",
        "forceOldDomain": "false",
    }
    if overrides:
        d.update(overrides)
    return d


def build_inner_params_from_args(args: argparse.Namespace, *, page: int) -> dict[str, Any]:
    """从 ``Namespace``（如本仓库 ``namespace_from_run_defaults``）读取可选覆盖项后生成内层 params。"""
    ps_raw = getattr(args, "page_source", None)
    ps = (ps_raw.strip() if isinstance(ps_raw, str) and ps_raw.strip() else None)
    vr_raw = getattr(args, "view_resolution", None)
    vr = (vr_raw.strip() if isinstance(vr_raw, str) and vr_raw.strip() else None)
    return build_inner_params(
        getattr(args, "q", "") or "",
        page=page,
        page_size=int(getattr(args, "page_size", 48)),
        page_source=ps,
        view_resolution=vr,
    )
