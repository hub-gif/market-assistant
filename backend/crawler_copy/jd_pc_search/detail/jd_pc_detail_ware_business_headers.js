/**
 * 与 Chrome 146 访问 item.jd.com → api.m.jd.com pc_detailpage_wareBusiness 的请求头对齐
 *（含 Accept-Encoding: zstd，与 DevTools 抓包一致）。
 *
 * Playwright 的 APIRequestContext 对部分 sec-ch-* 会忽略 per-request headers；
 * Python 侧在 ``browser.new_context({ userAgent, extraHTTPHeaders })`` 中重复注入同组
 * Client Hints（见 jd_detail_ware_business_requests.py），与真实请求一致。
 */
function buildJdPcDetailWareBusinessHeaders(opts) {
  const sku = opts.skuId != null ? String(opts.skuId).trim() : "";
  const itemPage = sku
    ? `https://item.jd.com/${encodeURIComponent(sku)}.html`
    : "https://item.jd.com/";
  const h = {
    Accept: "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Priority: "u=1, i",
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    Referer: "https://item.jd.com/",
    Origin: "https://item.jd.com",
    "sec-ch-ua":
      '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "x-referer-page": itemPage,
    "x-rp-client": "h5_1.0.0",
  };
  if (opts.cookie) h.Cookie = opts.cookie;
  return h;
}

module.exports = { buildJdPcDetailWareBusinessHeaders };
