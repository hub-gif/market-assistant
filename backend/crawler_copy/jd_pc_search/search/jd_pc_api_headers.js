/**
 * 与 Chrome 访问 search.jd.com → api.m.jd.com 的 pc_search_searchWare 请求头对齐
 *（含首包、懒加载第二包：body 里 page/s 会变，头字段形态与 DevTools 一致）。
 * Accept-Encoding 与常见 Chrome 一致；../common/jd_https_fetch.js 会解压 gzip/deflate/br。
 *
 * spmTag 可用环境变量 JD_SPM_TAG 覆盖。
 */
function buildJdPcSearchWareHeaders(opts) {
  const keyword = opts.keyword || "低GI";
  const pvid = opts.pvid || "90ac040818aa42a389a880e3b119e375";
  const spmTag =
    opts.spmTag ||
    process.env.JD_SPM_TAG ||
    "YTAyMTkuYjAwMjM1Ni5jMDAwMDQ2ODkuc2VhcmNoX2NvbmZpcm1";
  const encKw = encodeURIComponent(keyword);
  const encPvid = encodeURIComponent(pvid);
  const encSpm = encodeURIComponent(spmTag);
  const referer = `https://search.jd.com/Search?keyword=${encKw}&enc=utf-8&wq=${encKw}&pvid=${encPvid}&spmTag=${encSpm}`;

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
    Origin: "https://search.jd.com",
    "sec-ch-ua":
      '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "x-referer-page": "https://search.jd.com/Search",
    "x-rp-client": "h5_1.0.0",
  };
  if (opts.cookie) h.Cookie = opts.cookie;
  return h;
}

module.exports = { buildJdPcSearchWareHeaders };
