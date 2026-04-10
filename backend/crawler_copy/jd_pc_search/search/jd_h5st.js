/**
 * 统一封装：h5st 只通过 get_h5st() 获取（内部 ParamsSign / code.js）。
 * 流程：get_h5st(opt) → build_pc_search_api_url(pack, { keyword, uuid, xApiEidToken }) → 直连 GET。
 *
 * 商品评论签名见 ../comment/jd_h5st_item_comment.js（不修改本文件搜索列表链路）。
 */
require("../common/jd_browser_env.js");
require("../common/code.js");
const CryptoJS = require("crypto-js");

const DEFAULT_PVID = "90ac040818aa42a389a880e3b119e375";
const DEFAULT_AREA = "19_1601_50258_129167";

let _psign = null;
function _ensurePsign() {
  if (!_psign) {
    _psign = new window.ParamsSign({
      appId: "f06cc",
      preRequest: false,
      onSign: () => {},
      onRequestTokenRemotely: () => {},
    });
  }
  return _psign;
}

/**
 * 生成 h5st 及签名侧字段（与传入签名的 body 为 SHA256(hex) 一致）。
 *
 * @param {object} [opt]
 * @param {number} [opt.page=1] 请求 body.page（懒加载时每次续拉后浏览器会 page+1，与 body.s 一起变）
 * @param {string} [opt.pvid]
 * @param {string} [opt.area]
 * @param {number} [opt.s=1] 请求 body.s（续拉：上一包 s + max(1, 本包自然位−1)；例 s=1 且自然位 22 → 次包 s=22）
 * @param {string|number} [opt.psort='3'] 排序：与 PC 搜索一致，'3' 为按销量（与京东前台「销量」选项对应）
 * @param {number} [opt.t] 签名字段 t，默认 Date.now()
 * @param {string} [opt.functionId] 默认 pc_search_searchWare
 * @returns {{
 *   h5st: string,
 *   signed: object,
 *   bodyJson: string,
 *   bodySha256: string,
 *   searchParams: object,
 *   tQuerySecond: string
 * }}
 */
function get_h5st(opt) {
  const o = opt || {};
  const page = Math.max(1, parseInt(String(o.page != null ? o.page : 1), 10) || 1);
  const pvid = o.pvid != null ? String(o.pvid) : DEFAULT_PVID;
  const area = o.area != null ? String(o.area) : DEFAULT_AREA;
  const time = o.t != null ? Number(o.t) : Date.now();
  const functionId = o.functionId || "pc_search_searchWare";

  const psort = o.psort != null ? String(o.psort) : "3";

  const searchParams = {
    area,
    concise: false,
    enc: "utf-8",
    hoverPictures: false,
    mode: null,
    newAdvRepeat: false,
    new_interval: true,
    page,
    pvid,
    s: o.s != null ? o.s : 1,
    psort,
  };
  const bodyJson = JSON.stringify(searchParams);
  const bodySha = CryptoJS.SHA256(bodyJson).toString();
  const paramsH5sign = {
    appid: "search-pc-java",
    functionId,
    client: "pc",
    clientVersion: "1.0.0",
    t: time,
    body: bodySha,
  };
  const signed = _ensurePsign()._$sdnmd({ ...paramsH5sign });
  const tQuerySecond = String(Date.now());

  return {
    h5st: signed.h5st,
    signed,
    bodyJson,
    bodySha256: signed.body,
    searchParams,
    tQuerySecond,
  };
}

/**
 * 拼 https://api.m.jd.com/api?...（query 里两个 t，body 为 JSON 或 SHA256 与签名一致）
 *
 * @param {object} pack get_h5st() 返回值
 * @param {object} opts
 * @param {string} opts.keyword
 * @param {string} opts.uuid
 * @param {string} opts.xApiEidToken
 * @param {'json'|'sha256'} [opts.bodyMode='json']
 */
function build_pc_search_api_url(pack, opts) {
  const keyword = opts.keyword != null ? String(opts.keyword) : "";
  const uuid = opts.uuid != null ? String(opts.uuid) : "";
  const xApiEidToken = opts.xApiEidToken != null ? String(opts.xApiEidToken) : "";
  const bodyMode = opts.bodyMode === "sha256" ? "sha256" : "json";
  const signed = pack.signed;
  const bodyValue = bodyMode === "sha256" ? pack.bodySha256 : pack.bodyJson;
  const qParts = [
    ["appid", signed.appid],
    ["t", String(signed.t)],
    ["client", signed.client],
    ["clientVersion", signed.clientVersion],
    ["cthr", "1"],
    ["uuid", uuid],
    ["loginType", "3"],
    ["keyword", keyword],
    ["functionId", signed.functionId],
    ["body", bodyValue],
    ["x-api-eid-token", xApiEidToken],
    ["h5st", signed.h5st],
    ["t", pack.tQuerySecond],
  ];
  const qs = qParts
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");
  return `https://api.m.jd.com/api?${qs}`;
}

module.exports = {
  get_h5st,
  getH5st: get_h5st,
  build_pc_search_api_url,
};

