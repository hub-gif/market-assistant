/**
 * stdout 一行 JSON：{ url, method, form, headers } — POST client.action（与 item.jd.com 抓包一致）。
 *
 *   node jd_export_item_comment_page_request.js --sku 10145684793035 --category 36574;44419;44439 \\
 *     --first-guid T6NdP8N2gZgtdRyCXKCcwHaB --page-num 1 --is-first true
 */
const path = require("path");
const { loadJdSearchAuth } = require("../common/jd_search_common.js");
const {
  get_h5st_item_comment_page,
  build_item_comment_page_client_action_form,
} = require("./jd_h5st_item_comment_page.js");
const { buildJdPcItemCommentHeaders } = require("./jd_pc_item_comment_headers.js");

const DEFAULT_COOKIE = path.join(__dirname, "..", "common", "jd_cookie.txt");

function parseCli(argv = process.argv.slice(2)) {
  const out = {
    sku: null,
    category: null,
    firstGuid: null,
    pageNum: "1",
    isFirst: true,
    shopType: "0",
    spuId: null,
    style: "1",
    functionId:
      process.env.JD_COMMENT_LIST_FUNCTION_ID || "getCommentListPage",
    cookiePath: DEFAULT_COOKIE,
  };
  const a = argv;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    const take = (key) => {
      if (a[i + 1]) out[key] = a[++i];
    };
    if (x === "--sku") take("sku");
    else if (x.startsWith("--sku=")) out.sku = x.slice(6);
    else if (x === "--category") take("category");
    else if (x.startsWith("--category=")) out.category = x.slice(11);
    else if (x === "--first-guid") take("firstGuid");
    else if (x.startsWith("--first-guid=")) out.firstGuid = x.slice(13);
    else if (x === "--page-num") take("pageNum");
    else if (x.startsWith("--page-num=")) out.pageNum = x.slice(11);
    else if (x === "--is-first") take("isFirst");
    else if (x.startsWith("--is-first=")) {
      const v = x.slice(11).toLowerCase();
      out.isFirst = v === "true" || v === "1" || v === "yes";
    } else if (x === "--shop-type") take("shopType");
    else if (x.startsWith("--shop-type=")) out.shopType = x.slice(12);
    else if (x === "--spu-id") take("spuId");
    else if (x.startsWith("--spu-id=")) out.spuId = x.slice(9);
    else if (x === "--style") take("style");
    else if (x.startsWith("--style=")) out.style = x.slice(8);
    else if (x === "--function-id") take("functionId");
    else if (x.startsWith("--function-id=")) out.functionId = x.slice(14);
    else if (x === "--cookie-file") take("cookiePath");
    else if (x.startsWith("--cookie-file=")) out.cookiePath = x.slice(14);
  }
  if (typeof out.isFirst === "string") {
    const v = String(out.isFirst).toLowerCase();
    out.isFirst = v === "true" || v === "1" || v === "yes";
  }
  return out;
}

try {
  const cli = parseCli();
  if (!cli.sku) throw new Error("需要 --sku");
  if (!cli.category) throw new Error("需要 --category（如 36574;44419;44439）");
  if (!cli.firstGuid) throw new Error("需要 --first-guid（首条评价 guid）");
  if (!cli.functionId) throw new Error("需要 --function-id 或环境变量 JD_COMMENT_LIST_FUNCTION_ID");

  const { cookie, uuid } = loadJdSearchAuth(cli.cookiePath);
  if (!cookie) throw new Error("Cookie 为空或不存在");
  if (!uuid) throw new Error("缺少 uuid（Cookie 中 __jdu / mba_muid）");

  const pack = get_h5st_item_comment_page({
    sku: cli.sku,
    category: cli.category,
    firstCommentGuid: cli.firstGuid,
    pageNum: cli.pageNum,
    isFirstRequest: cli.isFirst,
    shopType: cli.shopType,
    spuId: cli.spuId || undefined,
    style: cli.style,
    functionId: cli.functionId,
  });
  const { url, form } = build_item_comment_page_client_action_form(pack, {
    uuid,
  });
  const headers = buildJdPcItemCommentHeaders({ cookie, sku: cli.sku });
  process.stdout.write(
    JSON.stringify({ url, method: "POST", form, headers })
  );
} catch (e) {
  console.error(e.message || String(e));
  process.exit(1);
}
