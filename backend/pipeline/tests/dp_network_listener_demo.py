"""
SeleniumBase UC + ``driver.add_cdp_listener``：只观察 **JSON 类** 的 ``Network.responseReceived``。

- 回调收到的是 performance 里解出来的整条 CDP ``message``（含 ``method`` / ``params``）。
- 按 ``mimeType`` 过滤（含 ``application/json``、``+json`` 等）；不含正文，避免在 Reactor 线程里调 ``getResponseBody``。
  若要正文：在主线程里对 ``requestId`` 执行 ``Network.getResponseBody``（宜在 ``loadingFinished`` 之后再试）。
- **范围**：Reactor 从 ``driver.get_log("performance")`` 取事件，与当前 WebDriver 会话/target 强相关；**实际效果大多等价于「当前（或该会话关注的）标签」**，
  其它标签、尤其是 **纯手动 Ctrl+T** 新开的页，**常常进不来**。跨标签需对每个 target 单独挂监听，或用仓库里 ``sb_browser.cdp_json_listen`` 等多标签方案。
"""

from __future__ import annotations

from rich.pretty import pprint
from seleniumbase import BaseCase

BaseCase.main(__name__, __file__, "--uc", "--uc-cdp")


def _is_json_like_mime(mime: str) -> bool:
    m = (mime or "").lower().strip()
    if not m or "json" not in m:
        return False
    # 排除 jsonp / 误伤极少见类型
    if "javascript" in m or "ecmascript" in m:
        return False
    return True


class CDPTests(BaseCase):
    def add_cdp_listener_json_only(self) -> None:
        def on_response_received(message: dict) -> None:
            params = message.get("params") or {}
            response = params.get("response") or {}
            mime = response.get("mimeType") or ""
            if not _is_json_like_mime(mime):
                return
            pprint(
                {
                    "url": response.get("url"),
                    "status": response.get("status"),
                    "mimeType": response.get("mimeType"),
                    "requestId": params.get("requestId"),
                }
            )

        self.driver.add_cdp_listener(
            "Network.responseReceived",
            on_response_received,
        )

    def test_display_cdp_events(self):
        if not (self.undetectable and self.uc_cdp_events):
            self.get_new_driver(undetectable=True, uc_cdp_events=True)
        url = "https://www.jd.com/"
        self.add_cdp_listener_json_only()
        self.sleep(3)
        self.uc_open_with_reconnect(url, 2)
        self.sleep(20)
        self.refresh()
        self.sleep(1.2)
