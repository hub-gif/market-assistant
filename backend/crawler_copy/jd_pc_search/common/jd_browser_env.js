/**
 * 在 Node 下为 crawler/code.js（ParamsSign / h5st 相关）补齐最小「浏览器」全局环境。
 *
 * 用法（必须在 require("./code.js") 之前）：
 *   require("./jd_browser_env.js");
 *
 * 说明：仅满足当前 bundle 在加载期访问到的 DOM/BOM；若京东更新脚本仍可能缺字段，再按报错补桩。
 */
(function applyJDBrowserEnv() {
  const g = globalThis;
  if (g.__JD_BROWSER_ENV_APPLIED__) {
    return;
  }
  g.__JD_BROWSER_ENV_APPLIED__ = true;

  g.window = g;

  function Element() {}
  Element.prototype.scrollIntoViewIfNeeded = function () {};
  g.Element = Element;

  const memStore = Object.create(null);
  const storage = {
    getItem(k) {
      return Object.prototype.hasOwnProperty.call(memStore, k)
        ? memStore[k]
        : null;
    },
    setItem(k, v) {
      memStore[String(k)] = String(v);
    },
    removeItem(k) {
      delete memStore[String(k)];
    },
    clear() {
      for (const k of Object.keys(memStore)) {
        delete memStore[k];
      }
    },
  };

  g.document = {
    all: null,
    cookie: "",
    domain: "jd.com",
    referrer: "https://search.jd.com/",
    createElement() {
      return Object.assign(new Element(), {
        style: {},
        appendChild() {},
        setAttribute() {},
        remove() {},
      });
    },
    getElementsByTagName() {
      return [{ appendChild() {} }];
    },
    querySelector() {
      return null;
    },
  };

  g.navigator = {
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    language: "zh-CN",
    languages: ["zh-CN", "zh", "en"],
    mimeTypes: { length: 0 },
    plugins: { length: 0 },
    appVersion: "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    platform: "Win32",
    webdriver: false,
    hardwareConcurrency: 8,
  };

  g.location = {
    href: "https://search.jd.com/Search?keyword=&enc=utf-8",
    origin: "https://search.jd.com",
    protocol: "https:",
    host: "search.jd.com",
    pathname: "/Search",
    search: "",
  };

  g.history = {
    replaceState() {},
    pushState() {},
    back() {},
  };

  g.screen = {
    width: 1920,
    height: 1080,
    availWidth: 1920,
    availHeight: 1040,
  };
  g.outerWidth = 1920;
  g.outerHeight = 1080;
  g.innerWidth = 1920;
  g.innerHeight = 969;
  g.devicePixelRatio = 1;

  g.chrome = {};

  g.localStorage = { ...storage };
  g.sessionStorage = { ...storage };

  function XMLHttpRequest() {
    this.readyState = 0;
    this.status = 0;
    this.responseText = "";
  }
  XMLHttpRequest.prototype.open = function () {};
  XMLHttpRequest.prototype.setRequestHeader = function () {};
  XMLHttpRequest.prototype.send = function () {};
  XMLHttpRequest.prototype.abort = function () {};
  g.XMLHttpRequest = XMLHttpRequest;

  g.getComputedStyle = function () {
    return {};
  };

  g.MutationObserver = function () {
    this.observe = function () {};
    this.disconnect = function () {};
  };

  g.WebKitMutationObserver = g.MutationObserver;
})();

module.exports = {};
