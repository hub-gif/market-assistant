# -*- coding: utf-8 -*-
"""先拼给 mtop 的 data，再 MD5(sign)，再拼完整 GET Query；结构按你自己的顺序来。"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
import requests

_HERE = Path(__file__).resolve().parent

COOKIE_INLINE = r"""mtop_partitioned_detect=1; _m_h5_tk=75521186089c4c67974ea590d98d2926_1777365266723; _m_h5_tk_enc=d7b11dddab2722e80c9dac19138ea09e; xlly_s=1; t=2c77c6ac0d39d52390ba3eeace1e3544; _tb_token_=f868efeaeb71e; thw=cn; sca=48631404; _samesite_flag_=true; 3PcFlag=1777356987927; cookie2=128e0fe777109b8e2853fd31ceec80bb; sgcookie=E100iNxLnv2IbDaJOXKNfipJItT9L58q6IzOTT%2BY1Wn7xdqBSA7GTKm2bcMzd9TDWEdEYNwksSPA0kQVM99gXDB2kduX4m%2FYT%2Ba5%2F1VSzr3nNho%3D; wk_cookie2=1b8cf87725c5816365a4738da4649365; wk_unb=UUpgRK0Eax1PlNAt8w%3D%3D; unb=2212041554345; uc1=cookie16=VFC%2FuZ9az08KUQ56dCrZDlbNdA%3D%3D&existShop=false&cookie15=VFC%2FuZ9ayeYq2g%3D%3D&cookie14=UoYZbYnMaaKITw%3D%3D&cookie21=U%2BGCWk%2F7pY%2FF&pas=0; uc3=vt3=F8dD29oQyTt7ojsKjBs%3D&lg2=U%2BGCWk%2F75gdr5Q%3D%3D&nk2=F5RDLeQJoma3ibWK&id2=UUpgRK0Eax1PlNAt8w%3D%3D; csg=7518e7b2; lgc=tb6111547670; cancelledSubSites=empty; cookie17=UUpgRK0Eax1PlNAt8w%3D%3D; dnk=tb6111547670; skt=3eecc50e94235967; existShop=MTc3NzM1NzAwMg%3D%3D; uc4=id4=0%40U2gqy1wZ%2F6T7h7GBZP%2Bq%2FDF8m0DHJN8t&nk4=0%40FY4I7jaC3n2Igw5uAAAUzNoDnu658q4%3D; tracknick=tb6111547670; _cc_=WqG3DMC9EA%3D%3D; _l_g_=Ug%3D%3D; sg=059; _nk_=tb6111547670; cookie1=WvKXALYTox8aTWacGEW0ZHunCp4rKr5l%2FKFF8zaslU0%3D; fastSlient=1777357069127; aui=2212041554345; tfstk=g6PqI0fQisC45US38ycNU2LoGwcxZfSCg5isSP4ilmmmcP0i_uq1kmaGDlPZqPefkAsv7EFzYCOfDtUM_fGGAM1COr3j6fjCeACvwm3TrcbSnnzmMEcGAM1BFUDAcf4jAxIiEL0KqqAmsKbrq20ZsVD0j0Do-2tmsfqGzY0jWdAijf4krqiojfcgjzbr500msfqgrauta5ZYXHgnnZSfIgH9E4kqxrmy_rFroxJYo0RMs7yr3DAs4CAgaq4MKmlWTOi3pmMIvufWgfzo7ukQ3MR4bP4LIYPwmZZ3SoFrMXW1kvyuFJM4UiAZVSgZKfDySC4zo0ZUiXfH44eulRVY0FRikSNIQDHPSCHsZWM3Ko8OJzcmSlHL1MdxmP4L9JGFgH34EylP4gKtrv9g6--MQx0-zD_Pz_4iAwhf_Tp9BdHoK4oCkAv9Bx0-zD_PzdptHB0rAZHG.; isg=BJGRwoDunFBOZvAGwCY-JRKCoJ0r_gVwhO2XDHMmjdh2GrFsu04VQD94vO78Ep2o""".strip()

APP_KEY = "12574478"
APP_ID = "34385"

DATA_JSON_INLINE = ""
T_MS = ""
sign_real = "7e5ba72cf0c770ffcf9b4c28eedf4f1c"

_COOKIE_FILE = _HERE / "taobao_cookie.txt"

BASE_URL = (
    "https://h5api.m.taobao.com/h5/mtop.relationrecommend.wirelessrecommend.recommend/2.0/"
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

_REFERER = (
    "https://s.taobao.com/search?clientPreloadId=preload_1777357090203"
    "&commend=all&ie=utf8&initiative_id=tbindexz_20170306&page=1"
    "&preLoadOrigin=https%3A%2F%2Fwww.taobao.com"
    "&q=%E9%9D%A2%E5%8C%85&search_type=item&sourceId=tb.index"
    "&spm=a21bo.jianhua%2Fa.search_manual.0&ssid=s5-e&tab=all"
)

headers = {
    "Referer": _REFERER,
    "User-Agent": _UA,
    "Cookie": COOKIE_INLINE,
}

# 你给的内层字段
params = {
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
    "page": 1,
    "n": 48,
    "q": "%E9%9D%A2%E5%8C%85",
    "qSource": "url",
    "pageSource": "a21bo.jianhua/a.search_manual.0",
    "channelSrp": "",
    "tab": "all",
    "pageSize": 48,
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
    "viewResolution": "548x1345",
    "userAgent": _UA,
    "couponUnikey": "",
    "subTabId": "",
    "np": "",
    "clientType": "h5",
    "isNewDomainAb": "false",
    "forceOldDomain": "false",
}


def _m_h5_token(cookie: str) -> str | None:
    m = re.search(r"(?:^|;\s*)_m_h5_tk=([^;]+)", cookie.strip(), re.I)
    if not m:
        return None
    raw = m.group(1).strip()
    if "_" not in raw:
        return None
    return raw.split("_", 1)[0]


def main() -> None:
    cookie = (COOKIE_INLINE or "").strip()
    if not cookie and _COOKIE_FILE.is_file():
        cookie = _COOKIE_FILE.read_text(encoding="utf-8").strip()
    if not cookie:
        print("需要 COOKIE_INLINE 或 taobao_cookie.txt", file=sys.stderr)
        sys.exit(2)

    headers["Cookie"] = cookie

    # 1) 参与签名的 data 整串：要么整段覆盖，要么从 params 包一层再 dumps
    if DATA_JSON_INLINE.strip():
        data_str = DATA_JSON_INLINE.strip()
    else:
        data = {
            "appId": APP_ID,
            "params": json.dumps(params, ensure_ascii=False, separators=(",", ":")),
        }
        data_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    # 2) t 与 sign
    t = (T_MS or "").strip() or str(int(time.time() * 1000))
    tok = _m_h5_token(cookie)
    if not tok:
        print("Cookie 无 _m_h5_tk", file=sys.stderr)
        sys.exit(2)
    sign = hashlib.md5(f"{tok}&{t}&{APP_KEY}&{data_str}".encode("utf-8")).hexdigest()

    # 3) 完整 GET 查询键值（与 Network Query String 一致）
    query = {
        "jsv": "2.7.4",
        "appKey": APP_KEY,
        "t": t,
        "sign": sign,
        "api": "mtop.relationrecommend.wirelessrecommend.recommend",
        "v": "2.0",
        "timeout": "10000",
        "type": "jsonp",
        "dataType": "jsonp",
        "callback": "mtopjsonp6",
        "data": data_str,
        "bx-ua": "fast-load",
    }

    print(sign)

    url = BASE_URL.rstrip("/") + "/?" + urlencode(query)

    print(json.dumps(query, ensure_ascii=False, indent=2))
    print(url)

    sr = (sign_real or "").strip()
    if sr:
        print(sign == sr.lower())

    response = requests.get(url, headers=headers, timeout=30)
    print(response.text[:4000])

if __name__ == "__main__":
    main()
