/**
 * 与 Chrome 访问 item.jd.com → api.m.jd.com getLegoWareDetailComment 的请求头对齐。
 */
function buildJdPcItemCommentHeaders(opts) {
  const sku = opts.sku != null ? String(opts.sku).trim() : "";
  const referer = sku
    ? `https://item.jd.com/${encodeURIComponent(sku)}.html`
    : "https://item.jd.com/";
  const h = {
    Accept: "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Priority: "u=1, i",
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    Referer: referer,
    Origin: "https://item.jd.com",
    "sec-ch-ua":
      '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "x-referer-page": referer,
    "x-rp-client": "h5_1.0.0",
  };
  if (opts.cookie) h.Cookie = opts.cookie;
  return h;
}

module.exports = { buildJdPcItemCommentHeaders };
