/** ISOLATED world：把 MAIN world 的 postMessage 转给 background */
window.addEventListener("message", (ev) => {
  if (!ev.data || ev.data.source !== "jd_semiauto_ext") return;
  if (ev.data.type !== "jd_capture") return;
  chrome.runtime.sendMessage({
    type: "capture",
    item: ev.data.payload,
  });
});
