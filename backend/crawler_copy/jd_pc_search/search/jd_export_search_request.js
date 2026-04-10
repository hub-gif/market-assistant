/**
 * stdout 输出一行 JSON：{ url, headers }，供 jd_search_playwright.py 使用。h5st 来自 get_h5st。
 *
 *   cd crawler/jd_pc_search/search && node jd_export_search_request.js --q 低GI --page 1
 *   # --page / --s 为 **API 请求体** body.page / body.s（非 Python 里的「逻辑页」L；逻辑页 L 的首包为 page=2L-1）
 *   node jd_export_search_request.js --q 低GI --page 3 --s 45
 *   set JD_PVID=搜索页 URL 里的 pvid   # 与浏览器 body.pvid、Referer 一致（可选）
 */
const { parseSearchCliArgs, loadJdSearchAuth } = require("../common/jd_search_common.js");
const { get_h5st, build_pc_search_api_url } = require("./jd_h5st.js");
const { buildJdPcSearchWareHeaders } = require("./jd_pc_api_headers.js");

try {
  const { q, page, s, pvid: pvidOpt, cookiePath } = parseSearchCliArgs();
  const { cookie, uuid, xApiEidToken } = loadJdSearchAuth(cookiePath);
  if (!cookie) throw new Error("Cookie 为空或文件不存在（--cookie-file 或 common/jd_cookie.txt）");
  if (!uuid || !xApiEidToken) throw new Error("缺少 uuid 或 x-api-eid-token（Cookie）");

  const pvid = pvidOpt && String(pvidOpt).trim();
  const pack = get_h5st(pvid ? { page, s, pvid } : { page, s });
  const url = build_pc_search_api_url(pack, {
    keyword: q,
    uuid,
    xApiEidToken,
    bodyMode: "json",
  });
  const headers = buildJdPcSearchWareHeaders({
    cookie,
    keyword: q,
    pvid: pack.searchParams.pvid,
  });
  process.stdout.write(JSON.stringify({ url, headers }));
} catch (e) {
  console.error(e.message || String(e));
  process.exit(1);
}
