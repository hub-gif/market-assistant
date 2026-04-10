/**
 * stdout 输出一行 JSON：{ url, headers }，供 jd_h5_item_comment_requests.py 使用。
 *
 *   cd crawler/jd_pc_search/comment && node jd_export_item_comment_request.js --sku 10145684793035
 *   node jd_export_item_comment_request.js --sku 10145684793035 --comment-num 5 --shop-type 0
 */
const path = require("path");
const { loadJdSearchAuth } = require("../common/jd_search_common.js");
const {
  get_h5st_item_lego_detail_comment,
  build_item_lego_comment_api_url,
} = require("./jd_h5st_item_comment.js");
const { buildJdPcItemCommentHeaders } = require("./jd_pc_item_comment_headers.js");

const DEFAULT_COOKIE = path.join(__dirname, "..", "common", "jd_cookie.txt");

function parseItemCommentCliArgs(argv = process.argv.slice(2)) {
  const out = {
    sku: null,
    commentNum: 5,
    shopType: "0",
    source: "pc",
    cookiePath: DEFAULT_COOKIE,
  };
  const a = argv;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    if (x === "--sku" && a[i + 1]) {
      out.sku = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--sku=")) {
      out.sku = x.slice(6).trim();
      continue;
    }
    if (x === "--comment-num" && a[i + 1]) {
      out.commentNum = Math.max(1, parseInt(a[++i], 10) || 5);
      continue;
    }
    if (x.startsWith("--comment-num=")) {
      out.commentNum = Math.max(1, parseInt(x.slice(14), 10) || 5);
      continue;
    }
    if (x === "--shop-type" && a[i + 1]) {
      out.shopType = String(a[++i]);
      continue;
    }
    if (x.startsWith("--shop-type=")) {
      out.shopType = x.slice(12);
      continue;
    }
    if (x === "--source" && a[i + 1]) {
      out.source = String(a[++i]);
      continue;
    }
    if (x.startsWith("--source=")) {
      out.source = x.slice(9);
      continue;
    }
    if (x === "--cookie-file" && a[i + 1]) {
      out.cookiePath = String(a[++i]);
      continue;
    }
    if (x.startsWith("--cookie-file=")) {
      out.cookiePath = x.slice(14);
      continue;
    }
  }
  return out;
}

try {
  const cli = parseItemCommentCliArgs();
  if (!cli.sku) throw new Error("需要 --sku（商品 SKU，与 item.jd.com/{sku}.html 一致）");

  const { cookie, uuid, xApiEidToken } = loadJdSearchAuth(cli.cookiePath);
  if (!cookie) throw new Error("Cookie 为空或不存在（jd_cookie.txt 或 --cookie-file）");
  if (!uuid || !xApiEidToken)
    throw new Error("缺少 uuid 或 x-api-eid-token（Cookie 中 __jdu/mba_muid 与 3AB9D23F7A4B3CSS）");

  const pack = get_h5st_item_lego_detail_comment({
    sku: cli.sku,
    commentNum: cli.commentNum,
    shopType: cli.shopType,
    source: cli.source,
  });
  const url = build_item_lego_comment_api_url(pack, {
    uuid,
    xApiEidToken,
    bodyMode: "json",
  });
  const headers = buildJdPcItemCommentHeaders({
    cookie,
    sku: cli.sku,
  });
  process.stdout.write(JSON.stringify({ url, headers }));
} catch (e) {
  console.error(e.message || String(e));
  process.exit(1);
}
