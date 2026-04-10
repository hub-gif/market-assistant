/**
 * 直连 HTTPS GET（不走代理）。自动按 Content-Encoding 解压 gzip/deflate/br。
 */
const https = require("https");
const zlib = require("zlib");
const { URL } = require("url");

function decodeBody(buf, headers) {
  if (!buf || !buf.length) return "";
  const enc = String(headers["content-encoding"] || "").toLowerCase();
  try {
    if (enc.includes("br")) return zlib.brotliDecompressSync(buf).toString("utf8");
    if (enc.includes("gzip")) return zlib.gunzipSync(buf).toString("utf8");
    if (enc.includes("deflate")) return zlib.inflateSync(buf).toString("utf8");
  } catch {
    /* 非压缩或损坏时按原文 UTF-8 */
  }
  return buf.toString("utf8");
}

function httpsGet(urlString, headers) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlString);
    const opt = {
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      method: "GET",
      headers: { ...headers, Host: u.hostname },
    };
    const req = https.request(opt, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const buf = Buffer.concat(chunks);
        const body = decodeBody(buf, res.headers);
        resolve({
          status: res.statusCode || 0,
          statusMessage: res.statusMessage || "",
          headers: res.headers,
          body,
        });
      });
    });
    req.on("error", (err) => {
      if (err && err.name === "AggregateError" && err.errors && err.errors[0]) {
        reject(err.errors[0]);
      } else {
        reject(err);
      }
    });
    req.setTimeout(45000, () => {
      req.destroy();
      reject(new Error("timeout"));
    });
    req.end();
  });
}

module.exports = { httpsGet };
