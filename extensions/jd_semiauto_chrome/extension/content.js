/**
 * MAIN world：hook fetch / XHR，拦截 api.m.jd.com JSON（含 client.action 评价分页）。
 */
(function () {
  if (window.__jd_semiauto_hook_installed) return;
  window.__jd_semiauto_hook_installed = true;

  const FN_KIND = {
    pc_detailpage_wareBusiness: "detail",
    getLegoWareDetailComment: "comment",
    getCommentListPage: "comment",
    pc_item_getWareGraphic: "graphic",
  };

  function functionIdFromUrl(url) {
    try {
      const u = new URL(url, location.href);
      return u.searchParams.get("functionId") || u.searchParams.get("functionid") || "";
    } catch {
      return "";
    }
  }

  function functionIdFromFormBody(formBody) {
    if (!formBody || typeof formBody !== "string") return "";
    try {
      const params = new URLSearchParams(formBody);
      return params.get("functionId") || params.get("functionid") || "";
    } catch {
      return "";
    }
  }

  function functionIdFromRequest(url, formBody) {
    return functionIdFromUrl(url) || functionIdFromFormBody(formBody);
  }

  function kindFromFunctionId(fid) {
    if (!fid) return null;
    if (FN_KIND[fid]) return FN_KIND[fid];
    const fl = fid.toLowerCase();
    if (fl.includes("getcommentlistpage") || fl.includes("commentlist")) return "comment";
    if (fl.includes("getlegowaredetailcomment")) return "comment";
    if (fl.includes("pc_detailpage_warebusiness")) return "detail";
    if (fl.includes("getwaregraphic")) return "graphic";
    return null;
  }

  function looksLikeComment(parsed) {
    if (!parsed || typeof parsed !== "object") return false;
    if ("commentFloorShowNum" in parsed && "commentIconInfo" in parsed) return true;
    if (Array.isArray(parsed.commentInfoList) && parsed.commentInfoList.length > 0) {
      return true;
    }
    if (Array.isArray(parsed.lastCommentInfoList) && parsed.lastCommentInfoList.length > 0) {
      return true;
    }
    if (Array.isArray(parsed.floors) && parsed.floors.length > 0) return true;
    const data = parsed.data;
    if (data && typeof data === "object") {
      if (Array.isArray(data.commentInfoList) && data.commentInfoList.length > 0) {
        return true;
      }
      if (Array.isArray(data.floors) && data.floors.length > 0) return true;
    }
    const result = parsed.result;
    if (result && typeof result === "object") {
      const fl = result.floors;
      if (
        Array.isArray(fl) &&
        fl.some((f) => String((f && f.mId) || "").toLowerCase().includes("comment"))
      ) {
        return true;
      }
    }
    return false;
  }

  function searchDataRoot(parsed) {
    if (!parsed || typeof parsed !== "object") return null;
    let data = parsed.data;
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch {
        return null;
      }
    }
    if (data && typeof data === "object" && !Array.isArray(data)) return data;
    return null;
  }

  /** 真·列表：data 为对象且 wareList / wareListPro 非空（不靠 functionId 猜 list） */
  function looksLikeList(parsed) {
    const data = searchDataRoot(parsed);
    if (!data) return false;
    for (const key of ["wareList", "wareListPro"]) {
      const wl = data[key];
      if (Array.isArray(wl) && wl.length > 0) return true;
    }
    return false;
  }

  function classify(url, parsed, formBody) {
    if (!parsed || typeof parsed !== "object") return null;
    if (looksLikeList(parsed)) return "list";
    if ("productAttributeVO" in parsed) return "detail";
    if (looksLikeComment(parsed)) return "comment";
    const data = searchDataRoot(parsed) || parsed.data;
    if (data && typeof data === "object" && typeof data.graphicContent === "string") {
      const gc = data.graphicContent;
      if (gc.includes("data-lazyload=") || gc.includes("background-image:url")) {
        return "graphic";
      }
    }
    const fid = functionIdFromRequest(url, formBody);
    return kindFromFunctionId(fid);
  }

  function bodyDictFromUrl(url) {
    try {
      const u = new URL(url, location.href);
      const raw = u.searchParams.get("body");
      if (!raw) return {};
      try {
        const obj = JSON.parse(raw);
        return obj && typeof obj === "object" ? obj : {};
      } catch {
        const obj = JSON.parse(decodeURIComponent(raw));
        return obj && typeof obj === "object" ? obj : {};
      }
    } catch {
      return {};
    }
  }

  function bodyDictFromForm(formBody) {
    if (!formBody) return {};
    try {
      const params = new URLSearchParams(formBody);
      const raw = params.get("body");
      if (!raw) return {};
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }

  function bodyDictFromRequest(url, formBody) {
    const fromUrl = bodyDictFromUrl(url);
    if (fromUrl && Object.keys(fromUrl).length) return fromUrl;
    const fromForm = bodyDictFromForm(formBody);
    return fromForm && typeof fromForm === "object" ? fromForm : {};
  }

  function firstSkuishInObject(obj, budget) {
    if (!obj || budget <= 0) return "";
    if (typeof obj === "object" && !Array.isArray(obj)) {
      for (const k of ["skuId", "wareId", "sku"]) {
        const s = String(obj[k] || "").trim();
        if (/^\d{5,}$/.test(s)) return s;
      }
      for (const v of Object.values(obj)) {
        const found = firstSkuishInObject(v, budget - 1);
        if (found) return found;
      }
    } else if (Array.isArray(obj)) {
      for (let i = 0; i < Math.min(obj.length, 80); i++) {
        const found = firstSkuishInObject(obj[i], budget - 1);
        if (found) return found;
      }
    }
    return "";
  }

  function resolveSku(url, parsed, kind, formBody) {
    if (kind === "list") return "";
    const bd = bodyDictFromRequest(url, formBody);
    for (const k of ["skuId", "wareId", "sku"]) {
      const s = String(bd[k] || "").trim();
      if (/^\d{5,}$/.test(s)) return s;
    }
    const fromParsed = firstSkuishInObject(parsed, 400);
    if (fromParsed) return fromParsed;
    return "";
  }

  function shouldCapture(url) {
    try {
      const u = new URL(url, location.href);
      const h = u.hostname.toLowerCase();
      const path = u.pathname.toLowerCase();
      if (h.includes("h5speed") || path.includes("/event/log")) return false;
      if (h === "api.m.jd.com" || h === "api.jd.com") return true;
      if (h.includes("jd.com") && (u.searchParams.has("functionId") || /client\.action/i.test(path))) {
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }

  function formBodyToString(body) {
    if (!body) return "";
    if (typeof body === "string") return body;
    try {
      if (typeof URLSearchParams !== "undefined" && body instanceof URLSearchParams) {
        return body.toString();
      }
      if (typeof FormData !== "undefined" && body instanceof FormData) {
        const parts = [];
        body.forEach((v, k) => {
          parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
        });
        return parts.join("&");
      }
    } catch {
      /* ignore */
    }
    return "";
  }

  function emitCapture(url, status, method, parsed, formBody) {
    const kind = classify(url, parsed, formBody);
    if (!kind) return;
    const resolved_sku = resolveSku(url, parsed, kind, formBody);
    const function_id = functionIdFromRequest(url, formBody);
    let list_keyword = "";
    const sdata = searchDataRoot(parsed);
    if (sdata && sdata.listKeyWord) {
      list_keyword = String(sdata.listKeyWord);
    }
    window.postMessage(
      {
        source: "jd_semiauto_ext",
        type: "jd_capture",
        payload: {
          capture_kind: kind,
          function_id,
          url,
          status: status || 200,
          method: method || "GET",
          parsed,
          resolved_sku,
          list_keyword,
        },
      },
      "*"
    );
  }

  function tryParseJson(text) {
    if (!text || typeof text !== "string") return null;
    let t = text.trim();
    if (!t) return null;
    if (t[0] !== "{" && t[0] !== "[") {
      const m = t.match(/^[a-zA-Z0-9_$]+\((.*)\)\s*;?\s*$/s);
      if (m) t = m[1].trim();
    }
    if (t[0] !== "{" && t[0] !== "[") return null;
    try {
      return JSON.parse(t);
    } catch {
      return null;
    }
  }

  const origFetch = window.fetch;
  if (typeof origFetch === "function") {
    window.fetch = async function (...args) {
      let reqUrl = "";
      let method = "GET";
      let formBody = "";
      try {
        const req = args[0];
        const init = args[1] || {};
        if (typeof req === "string") {
          reqUrl = req;
          method = (init.method || "GET").toUpperCase();
          formBody = formBodyToString(init.body);
        } else if (req && req.url) {
          reqUrl = req.url;
          method = (req.method || init.method || "GET").toUpperCase();
          try {
            formBody = formBodyToString(await req.clone().text());
          } catch {
            formBody = formBodyToString(init.body);
          }
        }
      } catch {
        /* ignore */
      }

      const res = await origFetch.apply(this, args);
      try {
        if (shouldCapture(reqUrl)) {
          res
            .clone()
            .text()
            .then((text) => {
              const parsed = tryParseJson(text);
              if (parsed) emitCapture(reqUrl, res.status, method, parsed, formBody);
            })
            .catch(() => {});
        }
      } catch {
        /* ignore */
      }
      return res;
    };
  }

  const XHR = window.XMLHttpRequest;
  if (XHR && XHR.prototype) {
    const open = XHR.prototype.open;
    const send = XHR.prototype.send;
    XHR.prototype.open = function (method, url, ...rest) {
      this.__jd_semiauto_method = method;
      this.__jd_semiauto_url = url;
      return open.call(this, method, url, ...rest);
    };
    XHR.prototype.send = function (body, ...rest) {
      this.__jd_semiauto_form_body = formBodyToString(body);
      this.addEventListener("load", function () {
        try {
          const url = this.__jd_semiauto_url || "";
          if (!shouldCapture(url)) return;
          const parsed = tryParseJson(this.responseText);
          if (parsed) {
            emitCapture(
              url,
              this.status,
              this.__jd_semiauto_method || "GET",
              parsed,
              this.__jd_semiauto_form_body || ""
            );
          }
        } catch {
          /* ignore */
        }
      });
      return send.call(this, body, ...rest);
    };
  }
})();
