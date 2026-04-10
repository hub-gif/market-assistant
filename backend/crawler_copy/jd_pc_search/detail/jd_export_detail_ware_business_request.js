/**
 * stdout 一行 JSON：{ url, headers }，供 jd_detail_ware_business_requests.py 使用。
 *
 *   cd crawler/jd_pc_search/detail && node jd_export_detail_ware_business_request.js --sku 10166848058665
 *   node jd_export_detail_ware_business_request.js --sku 10166848058665 --area 19_1601_50258_129167
 *
 * 与浏览器 URL 对齐：可用 --uuid、--x-api-eid-token 覆盖（同次抓包里 Query 可能与 Cookie 中 3CSS 不一致）；
 * 亦可设环境变量 JD_UUID、JD_X_API_EID_TOKEN（见 common/jd_search_common.js）。
 */
const path = require("path");
const { loadJdSearchAuth } = require("../common/jd_search_common.js");
const {
  get_h5st_detail_ware_business,
  build_detail_ware_business_api_url,
} = require("./jd_h5st_detail_ware_business.js");
const {
  buildJdPcDetailWareBusinessHeaders,
} = require("./jd_pc_detail_ware_business_headers.js");

const DEFAULT_COOKIE = path.join(__dirname, "..", "common", "jd_cookie.txt");

function parseCliArgs(argv = process.argv.slice(2)) {
  const out = {
    skuId: null,
    area: null,
    num: "1",
    sfTime: "1,0,0",
    cookiePath: DEFAULT_COOKIE,
    uuid: null,
    xApiEidToken: null,
  };
  const a = argv;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    if ((x === "--sku" || x === "--sku-id") && a[i + 1]) {
      out.skuId = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--sku=")) {
      out.skuId = x.slice(6).trim();
      continue;
    }
    if (x.startsWith("--sku-id=")) {
      out.skuId = x.slice(9).trim();
      continue;
    }
    if (x === "--area" && a[i + 1]) {
      out.area = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--area=")) {
      out.area = x.slice(7).trim();
      continue;
    }
    if (x === "--num" && a[i + 1]) {
      out.num = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--num=")) {
      out.num = x.slice(6).trim();
      continue;
    }
    if (x === "--sf-time" && a[i + 1]) {
      out.sfTime = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--sf-time=")) {
      out.sfTime = x.slice(10).trim();
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
    if (x === "--uuid" && a[i + 1]) {
      out.uuid = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--uuid=")) {
      out.uuid = x.slice(7).trim();
      continue;
    }
    if (x === "--x-api-eid-token" && a[i + 1]) {
      out.xApiEidToken = String(a[++i]).trim();
      continue;
    }
    if (x.startsWith("--x-api-eid-token=")) {
      out.xApiEidToken = x.slice(18).trim();
      continue;
    }
  }
  return out;
}

try {
  const cli = parseCliArgs();
  if (!cli.skuId) {
    throw new Error("需要 --sku 或 --sku-id（商品 SKU）");
  }

  if (cli.uuid) process.env.JD_UUID = cli.uuid;
  if (cli.xApiEidToken) process.env.JD_X_API_EID_TOKEN = cli.xApiEidToken;

  const { cookie, uuid, xApiEidToken } = loadJdSearchAuth(cli.cookiePath);
  if (!cookie) throw new Error("Cookie 为空或不存在（jd_cookie.txt 或 --cookie-file）");
  if (!uuid || !xApiEidToken) {
    throw new Error(
      "缺少 uuid 或 x-api-eid-token（Cookie 中 __jdu/mba_muid 与 3AB9D23F7A4B3CSS）"
    );
  }

  const pack = get_h5st_detail_ware_business({
    skuId: cli.skuId,
    area: cli.area || undefined,
    num: cli.num,
    sfTime: cli.sfTime,
  });
  const url = build_detail_ware_business_api_url(pack, {
    uuid,
    xApiEidToken,
    bodyMode: "json",
  });
  const headers = buildJdPcDetailWareBusinessHeaders({
    cookie,
    skuId: cli.skuId,
  });
  process.stdout.write(JSON.stringify({ url, headers }));
} catch (e) {
  console.error(e.message || String(e));
  process.exit(1);
}
