/**
 * 页面上下文 hook（由 content_bridge 注入）。拦截京东聚合 API JSON。
 */
(function () {
  if (window.__jd_semiauto_hook_installed) return;
  window.__jd_semiauto_hook_installed = true;

  const FN_KIND = {
    pc_search_searchWare: "list",
    pc_detailpage_wareBusiness: "detail",
    getLegoWareDetailComment: "comment",
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

  function kindFromParsed(parsed) {
    if (!parsed || typeof parsed !== "object") return null;
    if ("productAttributeVO" in parsed) return "detail";
    if ("commentFloorShowNum" in parsed && "commentIconInfo" in parsed) return "comment";
    const data = parsed.data;
    if (data && typeof data === "object") {
      if ("listKeyWord" in data && "resultCount" in data) return "list";
      const gc = data.graphicContent;
      if (typeof gc === "string" && gc.includes("data-lazyload=")) return "graphic";
      if (data.wareList) return "list";
    }
    return null;
  }

  function classify(url, parsed) {
    const fid = functionIdFromUrl(url);
    if (fid && FN_KIND[fid]) return FN_KIND[fid];
    const fromP = kindFromParsed(parsed);
    if (fromP) return fromP;
    return null;
  }

  function bodyDictFromUrl(url) {
    try {
      const u = new URL(url, location.href);
      const raw = u.searchParams.get("body");
      if (!raw) return {};
      return JSON.parse(decodeURIComponent(raw));
    } catch {
      return {};
    }
  }

  function resolveSku(url, parsed, kind) {
    if (kind === "list") return "";
    const bd = bodyDictFromUrl(url);
    for (const k of ["skuId", "wareId"]) {
      const s = String(bd[k] || "").trim();
      if (/^\d{5,}$/.test(s)) return s;
    }
    const data = parsed && parsed.data;
    if (data && typeof data === "object") {
      for (const k of ["skuId", "wareId"]) {
        const s = String(data[k] || "").trim();
        if (/^\d{5,}$/.test(s)) return s;
      }
    }
    return "";
  }

  function shouldCapture(url) {
    try {
      const u = new URL(url, location.href);
      const h = u.hostname.toLowerCase();
      if (h === "api.m.jd.com" || h.endsWith(".m.jd.com")) return true;
      if (h.includes("jd.com") && u.searchParams.has("functionId")) return true;
      return false;
    } catch {
      return false;
    }
  }

  function emitCapture(url, status, method, parsed) {
    const kind = classify(url, parsed);
    if (!kind) return;
    window.postMessage(
      {
        source: "jd_semiauto_ext",
        type: "jd_capture",
        payload: {
          capture_kind: kind,
          function_id: functionIdFromUrl(url),
          url,
          status: status || 200,
          method: method || "GET",
          parsed,
          resolved_sku: resolveSku(url, parsed, kind),
          list_keyword:
            parsed && parsed.data && parsed.data.listKeyWord
              ? String(parsed.data.listKeyWord)
              : "",
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
      const res = await origFetch.apply(this, args);
      try {
        const req = args[0];
        const url =
          typeof req === "string" ? req : req && req.url ? req.url : String(req);
        if (shouldCapture(url)) {
          res
            .clone()
            .text()
            .then((text) => {
              const parsed = tryParseJson(text);
              if (parsed) emitCapture(url, res.status, "GET", parsed);
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
    XHR.prototype.send = function (...args) {
      this.addEventListener("load", function () {
        try {
          const url = this.__jd_semiauto_url || "";
          if (!shouldCapture(url)) return;
          const parsed = tryParseJson(this.responseText);
          if (parsed) {
            emitCapture(url, this.status, this.__jd_semiauto_method || "GET", parsed);
          }
        } catch {
          /* ignore */
        }
      });
      return send.apply(this, args);
    };
  }
})();
