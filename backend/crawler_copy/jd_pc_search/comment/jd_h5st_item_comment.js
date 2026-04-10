/**
 * 商品详情 getLegoWareDetailComment 的 h5st（ParamsSign appId fb5df）。
 * 与 jd_h5st.js 分离，避免改动搜索列表 pc_search 链路。
 */
require("../common/jd_browser_env.js");
require("../common/code.js");
const CryptoJS = require("crypto-js");

const ITEM_COMMENT_PARAMS_SIGN_APP_ID = "fb5df";

let _psign = null;
function _ensurePsignItemComment() {
  if (!_psign) {
    _psign = new window.ParamsSign({
      appId: ITEM_COMMENT_PARAMS_SIGN_APP_ID,
      preRequest: false,
      onSign: () => {},
      onRequestTokenRemotely: () => {},
    });
  }
  return _psign;
}

/**
 * @param {object} opt
 * @param {number|string} opt.sku
 * @param {number} [opt.commentNum=5]
 * @param {string} [opt.shopType='0']
 * @param {string} [opt.source='pc']
 * @param {number} [opt.t]
 */
function get_h5st_item_lego_detail_comment(opt) {
  const o = opt || {};
  const sku = o.sku != null ? Number(o.sku) : NaN;
  if (!Number.isFinite(sku) || sku <= 0) {
    throw new Error("get_h5st_item_lego_detail_comment: 需要有效 opt.sku");
  }
  const commentNum = Math.max(
    1,
    parseInt(String(o.commentNum != null ? o.commentNum : 5), 10) || 5
  );
  const shopType = o.shopType != null ? String(o.shopType) : "0";
  const source = o.source != null ? String(o.source) : "pc";
  const time = o.t != null ? Number(o.t) : Date.now();

  const bodyObj = {
    shopType,
    sku,
    commentNum,
    source,
  };
  const bodyJson = JSON.stringify(bodyObj);
  const bodySha = CryptoJS.SHA256(bodyJson).toString();
  const functionId = "getLegoWareDetailComment";
  const paramsH5sign = {
    appid: "item-v3",
    functionId,
    client: "pc",
    clientVersion: "1.0.0",
    t: time,
    body: bodySha,
  };
  const signed = _ensurePsignItemComment()._$sdnmd({
    ...paramsH5sign,
  });

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
 * @param {object} pack get_h5st_item_lego_detail_comment 返回值
 * @param {object} opts
 * @param {string} opts.uuid
 * @param {string} opts.xApiEidToken
 * @param {'json'|'sha256'} [opts.bodyMode='json']
 */
function build_item_lego_comment_api_url(pack, opts) {
  const uuid = opts.uuid != null ? String(opts.uuid) : "";
  const xApiEidToken =
    opts.xApiEidToken != null ? String(opts.xApiEidToken) : "";
  const bodyMode = opts.bodyMode === "sha256" ? "sha256" : "json";
  const signed = pack.signed;
  const bodyValue = bodyMode === "sha256" ? pack.bodySha256 : pack.bodyJson;
  const build = opts.build != null ? String(opts.build) : "100000";
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
    ["build", build],
  ];
  const qs = qParts
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");
  return `https://api.m.jd.com/?${qs}`;
}

module.exports = {
  get_h5st_item_lego_detail_comment,
  build_item_lego_comment_api_url,
};
