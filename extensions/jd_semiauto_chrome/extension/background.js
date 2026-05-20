importScripts("stats_merge.js");

const DEFAULT_SIDECAR = "http://127.0.0.1:8765";
const BATCH_SIZE = 8;
const FLUSH_MS = 1500;
const PHASE_LISTENING = "listening";
/** 仅 sidecar 重启为 idle 时保留本地采集中 */
function resolvePhaseFromSidecar(localPhase, sidecarPhase) {
  const local = localPhase || "idle";
  const remote = sidecarPhase || "idle";
  if (remote === "idle" && (local === "waiting_login" || local === "listening")) {
    return local;
  }
  if (remote && remote !== "idle") return remote;
  return local;
}

let queue = [];
let flushTimer = null;
let captureEnabled = false;

/** 避免 sendResponse 时通道已关闭（MV3 SW 休眠常见） */
function safeRespond(sendResponse, payload) {
  try {
    sendResponse(payload);
  } catch {
    /* message port closed */
  }
}

async function getSidecarBase() {
  const { sidecarUrl } = await chrome.storage.local.get(["sidecarUrl"]);
  return (sidecarUrl || DEFAULT_SIDECAR).replace(/\/$/, "");
}

async function persistSession(patch) {
  const prev = await chrome.storage.local.get([
    "sessionPhase",
    "captureEnabled",
    "sessionKeyword",
    "runDir",
    "jobId",
    "sidecarUrl",
    "stats",
  ]);
  const next = { ...prev, ...patch };
  if (patch.stats) next.stats = { ...defaultStats(), ...prev.stats, ...patch.stats };
  await chrome.storage.local.set(next);
  if (patch.sessionPhase !== undefined) {
    captureEnabled = patch.sessionPhase === PHASE_LISTENING;
  } else if (typeof patch.captureEnabled === "boolean") {
    captureEnabled = patch.captureEnabled;
  }
}

async function restoreSessionFromStorage() {
  const s = await chrome.storage.local.get([
    "sessionPhase",
    "captureEnabled",
    "sessionKeyword",
    "runDir",
    "jobId",
    "stats",
    "sidecarUrl",
  ]);
  captureEnabled = (s.sessionPhase || "idle") === PHASE_LISTENING;
  return s;
}

async function sidecarFetch(path, body) {
  const base = await getSidecarBase();
  const res = await fetch(`${base}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

async function syncFromSidecar() {
  try {
    const base = await getSidecarBase();
    const data = await (await fetch(`${base}/job/status`)).json();
    if (!data.ok && data.error) return null;

    const prev = await chrome.storage.local.get([
      "stats",
      "runDir",
      "jobId",
      "sessionPhase",
      "statsSessionKey",
    ]);
    const phase = resolvePhaseFromSidecar(prev.sessionPhase, data.phase);
    const runDir = data.run_dir || prev.runDir;
    const jobId = data.job_id ?? prev.jobId;
    const sessionKey = sessionKeyForStats(runDir, jobId);
    const stats = resolveStats(
      prev.stats,
      data.counts,
      sessionKey,
      prev.statsSessionKey
    );

    await persistSession({
      sessionPhase: phase,
      captureEnabled: phase === PHASE_LISTENING,
      runDir,
      jobId,
      statsSessionKey: sessionKey || prev.statsSessionKey || "",
      stats,
      lastError: data.error_message || "",
    });
    return data;
  } catch {
    return null;
  }
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushQueue();
  }, FLUSH_MS);
}

async function flushQueue() {
  if (!captureEnabled || queue.length === 0) return;
  const batch = queue.splice(0, BATCH_SIZE);
  const { sessionKeyword, runDir } = await chrome.storage.local.get([
    "sessionKeyword",
    "runDir",
  ]);
  const kw = (sessionKeyword || "").trim();
  const items = kw
    ? batch.map((item) => ({ ...item, keyword: kw }))
    : batch;
  try {
    const result = await sidecarFetch("/capture/batch", {
      items,
      run_dir: runDir || undefined,
    });
    if (result.counts) {
      const s = statsFromSidecar(result.counts);
      await chrome.storage.local.set({ stats: s });
    } else {
      const { stats = defaultStats() } = await chrome.storage.local.get(["stats"]);
      for (const it of batch) {
        if (stats[it.capture_kind] !== undefined) stats[it.capture_kind] += 1;
      }
      stats.posted = (stats.posted || 0) + (result.written || 0);
      await chrome.storage.local.set({ stats });
    }
  } catch (e) {
    console.warn("[jd_semiauto] batch failed", e);
    queue = batch.concat(queue);
    scheduleFlush();
  }
  if (queue.length > 0) scheduleFlush();
}

/** popup 写入 storage 后通知 SW 重载 captureEnabled（同步、毫秒级） */
function notifyApplyStorage(sendResponse) {
  restoreSessionFromStorage()
    .then((s) => safeRespond(sendResponse, { ok: true, captureEnabled, ...s }))
    .catch((e) => safeRespond(sendResponse, { ok: false, error: String(e) }));
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === "capture") {
    (async () => {
      await restoreSessionFromStorage();
      if (!captureEnabled) {
        safeRespond(sendResponse, { ok: false, reason: "not_listening" });
        return;
      }
      queue.push(msg.item);
      if (queue.length >= BATCH_SIZE) await flushQueue();
      else scheduleFlush();
      safeRespond(sendResponse, { ok: true });
    })();
    return true;
  }

  if (msg.type === "apply_storage") {
    notifyApplyStorage(sendResponse);
    return true;
  }

  if (msg.type === "flush_now") {
    flushQueue()
      .then(() => safeRespond(sendResponse, { ok: true }))
      .catch((e) => safeRespond(sendResponse, { ok: false, error: String(e) }));
    return true;
  }

  return false;
});

chrome.alarms.create("jd_semiauto_sync", { periodInMinutes: 0.5 });
chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name !== "jd_semiauto_sync") return;
  const s = await chrome.storage.local.get(["sessionPhase"]);
  if (["waiting_login", "listening", "stopping"].includes(s.sessionPhase)) {
    await syncFromSidecar();
    await restoreSessionFromStorage();
  }
  if (captureEnabled && queue.length > 0) await flushQueue();
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") return;
  if (changes.sessionPhase || changes.captureEnabled) {
    void restoreSessionFromStorage();
  }
});

chrome.runtime.onStartup.addListener(() => restoreSessionFromStorage());
chrome.runtime.onInstalled.addListener(() => restoreSessionFromStorage());
restoreSessionFromStorage();
