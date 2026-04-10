const fs = require("fs");
const path = require("path");

const DEFAULT_COOKIE = path.join(__dirname, "jd_cookie.txt");

function readCookieFile(p = DEFAULT_COOKIE) {
  if (!fs.existsSync(p)) return "";
  const chunks = [];
  for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
    const s = line.trim();
    if (!s || s.startsWith("#")) continue;
    chunks.push(s);
  }
  return chunks.join("; ").trim();
}

function cookieGet(cookie, name) {
  const m = new RegExp(`(?:^|;\\s*)${name}=([^;]*)`).exec(cookie);
  return m ? decodeURIComponent(m[1].trim()) : "";
}

function parseSearchCliArgs(argv = process.argv.slice(2)) {
  const out = {
    q: process.env.JD_KEYWORD || "低GI",
    page: Math.max(1, parseInt(process.env.JD_PAGE || "1", 10) || 1),
    s: Math.max(1, parseInt(process.env.JD_S || "1", 10) || 1),
    pvid: (process.env.JD_PVID || "").trim() || null,
    cookiePath: DEFAULT_COOKIE,
  };
  const a = argv;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    if (x === "--q" && a[i + 1]) {
      out.q = a[++i];
      continue;
    }
    if (x.startsWith("--q=")) {
      out.q = x.slice(4);
      continue;
    }
    // --page：写入 pc_search **body.page**（与 Python 脚本「逻辑页 L」不同；L 的首包为 2L-1）
    if (x === "--page" && a[i + 1]) {
      out.page = Math.max(1, parseInt(a[++i], 10) || 1);
      continue;
    }
    if (x.startsWith("--page=")) {
      out.page = Math.max(1, parseInt(x.slice(7), 10) || 1);
      continue;
    }
    if (x === "--s" && a[i + 1]) {
      out.s = Math.max(1, parseInt(a[++i], 10) || 1);
      continue;
    }
    if (x.startsWith("--s=")) {
      out.s = Math.max(1, parseInt(x.slice(4), 10) || 1);
      continue;
    }
    if (x === "--pvid" && a[i + 1]) {
      out.pvid = String(a[++i]).trim() || null;
      continue;
    }
    if (x.startsWith("--pvid=")) {
      const v = x.slice(7).trim();
      out.pvid = v || null;
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
  // 兼容：node script.js 低GI 2（首参词、次参为 **body.page**）。不得在有 --q 时把 a[1]（往往是关键词）当成 page。
  if (a[0] && !a[0].startsWith("-")) {
    out.q = a[0];
  }
  if (a[1] && !a[1].startsWith("-")) {
    const pi = parseInt(a[1], 10);
    if (!Number.isNaN(pi)) {
      out.page = Math.max(1, pi);
    }
  }
  return out;
}

function loadJdSearchAuth(cookiePath = DEFAULT_COOKIE) {
  const cookie = readCookieFile(cookiePath);
  const uuid =
    process.env.JD_UUID ||
    cookieGet(cookie, "__jdu") ||
    cookieGet(cookie, "mba_muid") ||
    "";
  const xApiEidToken =
    process.env.JD_X_API_EID_TOKEN ||
    cookieGet(cookie, "3AB9D23F7A4B3CSS") ||
    cookieGet(cookie, "cd_eid") ||
    "";
  return { cookie, uuid, xApiEidToken };
}

module.exports = {
  DEFAULT_COOKIE,
  readCookieFile,
  cookieGet,
  parseSearchCliArgs,
  loadJdSearchAuth,
};
