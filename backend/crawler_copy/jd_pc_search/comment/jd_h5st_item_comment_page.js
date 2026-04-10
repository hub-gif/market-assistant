/**
 * 商品页「评价列表」分页：POST https://api.m.jd.com/client.action
 *（application/x-www-form-urlencoded，与 item.jd.com 抓包一致）。
 *
 * - 参与签名的 appid：**pc-rate-qa**（非 item-v3）
 * - ParamsSign 构造器 appId：**01a47**（h5st 第三段，非 fb5df）
 * - 表单字段：appid、body、client、clientVersion、functionId、h5st、loginType、t、uuid
 *
 * 首包 isFirstRequest:true 无 style；后续包 isFirstRequest:false 且含 style:"1"。
 */
require("../common/jd_browser_env.js");
require("../common/code.js");
const CryptoJS = require("crypto-js");

const COMMENT_LIST_CLIENT_ACTION_APPID = "pc-rate-qa";
/** 与浏览器 h5st 中第三段一致 */
const COMMENT_LIST_PARAMS_SIGN_APP_ID = "01a47";

let _psign = null;
function _ensurePsign() {
  if (!_psign) {
    _psign = new window.ParamsSign({
      appId: COMMENT_LIST_PARAMS_SIGN_APP_ID,
      preRequest: false,
      onSign: () => {},
      onRequestTokenRemotely: () => {},
    });
  }
  return _psign;
}

function _extInfoBlock(spuId) {
  const s = String(spuId != null ? spuId : "");
  return {
    isQzc: "0",
    spuId: s,
    commentRate: "1",
    needTopAlbum: "1",
    bbtf: "",
    userGroupComment: "1",
  };
}

/**
 * @param {object} opt
 * @param {string} opt.sku
 * @param {string} opt.category 如 36574;44419;44439（来自首屏 maidianInfo 等）
 * @param {string} opt.firstCommentGuid 首条评价 guid
 * @param {string|number} opt.pageNum
 * @param {boolean} opt.isFirstRequest
 * @param {string} [opt.shopType='0']
 * @param {string} [opt.spuId] 默认与 sku 字符串相同
 * @param {string} [opt.style='1'] 仅 isFirstRequest 为 false 时写入 body
 * @param {string} [opt.num='10']
 * @param {string} [opt.pageSize='10']
 * @param {string} [opt.sortType='5']
 * @param {number} [opt.t]
 */
function build_item_comment_page_body(opt) {
  const o = opt || {};
  const skuStr = String(o.sku != null ? o.sku : "").trim();
  if (!skuStr) throw new Error("build_item_comment_page_body: 需要 opt.sku");
  const categoryStr = String(o.category != null ? o.category : "").trim();
  if (!categoryStr) throw new Error("build_item_comment_page_body: 需要 opt.category");
  const guid = String(o.firstCommentGuid != null ? o.firstCommentGuid : "").trim();
  if (!guid) throw new Error("build_item_comment_page_body: 需要 opt.firstCommentGuid");
  const shopTypeStr = o.shopType != null ? String(o.shopType) : "0";
  const spuId = o.spuId != null ? String(o.spuId) : skuStr;
  const pageNum = String(o.pageNum != null ? o.pageNum : "1");
  const isFirst = Boolean(o.isFirstRequest);
  const num = o.num != null ? String(o.num) : "10";
  const pageSize = o.pageSize != null ? String(o.pageSize) : "10";
  const sortType = o.sortType != null ? String(o.sortType) : "5";
  const extInfo = _extInfoBlock(spuId);

  /** @type {Record<string, unknown>} */
  const base = {
    requestSource: "pc",
    shopComment: 0,
    sameComment: 0,
    channel: null,
    extInfo,
    num,
    pictureCommentType: "A",
    scval: null,
    shadowMainSku: "0",
    shopType: shopTypeStr,
    firstCommentGuid: guid,
    sku: skuStr,
    category: categoryStr,
    shieldCurrentComment: "1",
    pageSize,
    isFirstRequest: isFirst,
  };
  if (!isFirst) {
    base.style = o.style != null ? String(o.style) : "1";
  }
  base.isCurrentSku = false;
  base.sortType = sortType;
  base.tagId = "";
  base.tagType = "";
  base.type = "0";
  base.pageNum = pageNum;
  return base;
}

/**
 * @param {object} opt 同 build_item_comment_page_body，另需 functionId
 * @param {string} opt.functionId
 */
function get_h5st_item_comment_page(opt) {
  const o = opt || {};
  const functionId = o.functionId != null ? String(o.functionId).trim() : "";
  if (!functionId) throw new Error("get_h5st_item_comment_page: 需要 opt.functionId");
  const time = o.t != null ? Number(o.t) : Date.now();
  const bodyObj = build_item_comment_page_body(o);
  const bodyJson = JSON.stringify(bodyObj);
  const bodySha = CryptoJS.SHA256(bodyJson).toString();
  const paramsH5sign = {
    appid: COMMENT_LIST_CLIENT_ACTION_APPID,
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

const CLIENT_ACTION_URL = "https://api.m.jd.com/client.action";

/**
 * POST client.action 的 x-www-form-urlencoded 字段（顺序与常见抓包一致）。
 * @param {object} pack get_h5st_item_comment_page 返回值
 * @param {object} opts
 * @param {string} opts.uuid
 */
function build_item_comment_page_client_action_form(pack, opts) {
  const uuid = opts.uuid != null ? String(opts.uuid) : "";
  if (!uuid) throw new Error("build_item_comment_page_client_action_form: 需要 opts.uuid");
  const signed = pack.signed;
  return {
    url: CLIENT_ACTION_URL,
    form: {
      appid: COMMENT_LIST_CLIENT_ACTION_APPID,
      body: pack.bodyJson,
      client: signed.client,
      clientVersion: signed.clientVersion,
      functionId: signed.functionId,
      h5st: signed.h5st,
      loginType: "3",
      t: pack.tQuerySecond,
      uuid,
    },
  };
}

module.exports = {
  build_item_comment_page_body,
  get_h5st_item_comment_page,
  build_item_comment_page_client_action_form,
  COMMENT_LIST_CLIENT_ACTION_APPID,
  COMMENT_LIST_PARAMS_SIGN_APP_ID,
};
