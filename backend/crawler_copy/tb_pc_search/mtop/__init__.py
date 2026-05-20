# -*- coding: utf-8 -*-
"""
淘宝 H5 mtop 协议与推荐列表相关实现（子模块分文件）。

- ``constants``：``USER_AGENT`` 等
- ``h5_sign``：``_m_h5_tk``、``t``/``sign``、``data`` 外层 JSON
- ``jsonp``：JSONP 去壳、风控/错误说明
- ``transport``：JSONP GET 请求头、Playwright ``fetch_mtop_jsonp``
- ``recommend_params``：本推荐接口的 URL、内层 params、Referer 与默认请求头
- ``recommend_client``：等待、落盘、单页 GET
- ``item_extract``：响应中商品行解析
"""
