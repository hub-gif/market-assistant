/**
 * 商品详情 pc_detailpage_wareBusiness（appid=pc-item-soa）的 h5st。
 * 与 jd_h5st.js（搜索）、jd_h5st_item_comment.js（评论）分离。
 * ParamsSign 使用抓包第二段 appId：fb5df（与 getLegoWareDetailComment 一致）。
 */
require("../common/jd_browser_env.js");
require("../common/code.js");
const CryptoJS = require("crypto-js");

const DETAIL_WARE_BUSINESS_SIGN_APP_ID = "fb5df";
const DEFAULT_AREA = "19_1601_50258_129167";

let _psign = null;
function _ensurePsign() {
  if (!_psign) {
    _psign = new window.ParamsSign({
      appId: DETAIL_WARE_BUSINESS_SIGN_APP_ID,
      preRequest: false,
      onSign: () => {},
      onRequestTokenRemotely: () => {},
    });
  }
  return _psign;
}

/**
 * @param {object} opt
 * @param {string|number} opt.skuId 商品 SKU（与 item.jd.com/{sku}.html 一致，body 里为字符串）
 * @param {string} [opt.area]
 * @param {string} [opt.num='1']
 * @param {string} [opt.sfTime='1,0,0'] 与 PC 详情抓包一致
 * @param {number} [opt.t]
 */
function get_h5st_detail_ware_business(opt) {
  const o = opt || {};
  const skuId = o.skuId != null ? String(o.skuId).trim() : "";
  if (!skuId || !/^\d+$/.test(skuId)) {
    throw new Error("get_h5st_detail_ware_business: 需要有效 opt.skuId（数字 SKU）");
  }
  const area = o.area != null ? String(o.area) : DEFAULT_AREA;
  const num = o.num != null ? String(o.num) : "1";
  const sfTime = o.sfTime != null ? String(o.sfTime) : "1,0,0";
  const time = o.t != null ? Number(o.t) : Date.now();

  const bodyObj = {
    skuId,
    area,
    num,
    sfTime,
  };
  const bodyJson = JSON.stringify(bodyObj);
  const bodySha = CryptoJS.SHA256(bodyJson).toString();
  const functionId = "pc_detailpage_wareBusiness";
  const appid = "pc-item-soa";
  const paramsH5sign = {
    appid,
    functionId,
    client: "pc",
    clientVersion: "1.0.0",
    t: time,
    body: bodySha,
  };
  const signed = _ensurePsign()._$sdnmd({ ...paramsH5sign });

  return {
    h5st: signed.h5st,
    signed,
    bodyJson,
    bodySha256: signed.body,
    bodyObj,
    tQuerySecond: String(signed.t),
  };
}

/**
 * 拼 https://api.m.jd.com/?functionId=...（与 DevTools 路径一致，无 /api 前缀）。
 * Query 键顺序与 Chrome 一致：functionId, body, h5st, uuid, loginType, appid,
 * clientVersion, client, t, x-api-eid-token。
 * body 为 JSON 字符串（非 SHA256），键顺序 skuId → area → num → sfTime，值均为字符串。
 */
function build_detail_ware_business_api_url(pack, opts) {
  const uuid = opts.uuid != null ? String(opts.uuid) : "";
  const xApiEidToken = opts.xApiEidToken != null ? String(opts.xApiEidToken) : "";
  const bodyMode = opts.bodyMode === "sha256" ? "sha256" : "json";
  const signed = pack.signed;
  const bodyValue = bodyMode === "sha256" ? pack.bodySha256 : pack.bodyJson;
  const qParts = [
    ["functionId", signed.functionId],
    ["body", bodyValue],
    ["h5st", signed.h5st],
    ["uuid", uuid],
    ["loginType", "3"],
    ["appid", signed.appid],
    ["clientVersion", signed.clientVersion],
    ["client", signed.client],
    ["t", pack.tQuerySecond],
    ["x-api-eid-token", xApiEidToken],
  ];
  const qs = qParts
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");
  return `https://api.m.jd.com/?${qs}`;
}

module.exports = {
  get_h5st_detail_ware_business,
  build_detail_ware_business_api_url,
  DEFAULT_AREA,
};
