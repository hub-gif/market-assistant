const PHASE_LABELS = {
  idle: "",
  waiting_login: "请在本浏览器打开 jd.com 并完成登录",
  listening: "监听中（可翻页、进商详；关掉 popup 仍继续）",
  stopping: "正在解析 CSV 并入库…",
  done: "已完成",
  failed: "失败",
};

const STEPS = ["启动", "登录", "监听", "入库", "完成"];
let softSyncInFlight = false;

/** 仅 sidecar 重启为 idle 时保留本地采集中；stopping→done 须跟 sidecar 走 */
function resolvePhaseFromSidecar(localPhase, sidecarPhase) {
  const local = localPhase || "idle";
  const remote = sidecarPhase || "idle";
  if (remote === "idle" && (local === "waiting_login" || local === "listening")) {
    return local;
  }
  if (remote && remote !== "idle") return remote;
  return local;
}

function $(id) {
  return document.getElementById(id);
}

function showMsg(text, kind = "") {
  const el = $("msg");
  el.textContent = text || "";
  el.className = kind;
}

function sidecarBase() {
  return ($("sidecarUrl").value || "http://127.0.0.1:8765").replace(/\/$/, "");
}

async function sidecarGet(path) {
  const r = await fetch(`${sidecarBase()}${path}`);
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
}

async function sidecarPost(path, body) {
  const r = await fetch(`${sidecarBase()}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
}

function notifyBackgroundStorage() {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ type: "apply_storage" }, (res) => {
      if (chrome.runtime.lastError) {
        resolve({ ok: false, error: chrome.runtime.lastError.message });
        return;
      }
      resolve(res || { ok: true });
    });
  });
}

/** 轮询只同步阶段/计数/run_dir，不覆盖 sessionKeyword（避免被 sidecar 默认 manual 刷回） */
function mergeSidecarIntoStorage(data) {
  return chrome.storage.local.get(null).then((prev) => {
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
    const patch = {
      sessionPhase: phase,
      captureEnabled: phase === "listening",
      runDir,
      jobId,
      statsSessionKey: sessionKey || prev.statsSessionKey || "",
      sessionKeyword: prev.sessionKeyword || "",
      draftKeyword: prev.draftKeyword ?? prev.sessionKeyword ?? "",
      stats,
      lastError: data.error_message || prev.lastError || "",
      sidecarUrl: prev.sidecarUrl,
      createDjangoJob: prev.createDjangoJob,
    };
    return chrome.storage.local.set(patch).then(() => patch);
  });
}

function renderStats(stats) {
  const s = { ...defaultStats(), ...(stats || {}) };
  $("stats").textContent =
    `list ${s.list} · detail ${s.detail} · comment ${s.comment} · graphic ${s.graphic}\n已落盘 ${s.posted || 0} 个文件`;
}

function renderSteps(phase) {
  const order = ["waiting_login", "listening", "stopping", "done"];
  const idx =
    phase === "failed" ? 3 : phase === "idle" ? -1 : order.indexOf(phase);
  $("steps").innerHTML = STEPS.map((label, i) => {
    let cls = "step";
    const si = i === 0 ? -1 : i - 1;
    if (si < idx) cls += " done";
    if (si === idx) cls += " active";
    if (phase === "done" && i === 4) cls += " done active";
    return `<span class="${cls}">${label}</span>`;
  }).join(" › ");
}

function applyStoredState(s, opts = {}) {
  const phase = s.sessionPhase || "idle";
  const active = ["waiting_login", "listening", "stopping"].includes(phase);

  $("panelIdle").classList.toggle("hidden", active);
  $("panelActive").classList.toggle("hidden", !active);

  if (!opts.skipKeywordInput) {
    if (active) {
      $("sessionKeyword").value = s.sessionKeyword || "";
    } else {
      const draft = s.draftKeyword ?? s.sessionKeyword ?? "";
      if (draft) $("keyword").value = draft;
    }
  }

  if (s.sidecarUrl) $("sidecarUrl").value = s.sidecarUrl;
  if (s.runDir) $("runDir").value = s.runDir;
  if (typeof s.createDjangoJob === "boolean") $("createJob").checked = s.createDjangoJob;

  renderSteps(phase);
  $("phaseLabel").textContent = PHASE_LABELS[phase] || phase;
  renderStats(s.stats);

  const meta = [];
  if (s.jobId) meta.push(`任务 #${s.jobId}`);
  if (s.sessionKeyword) meta.push(`搜索词：${s.sessionKeyword}`);
  $("meta").textContent = meta.join(" · ");

  $("btnConfirm").classList.toggle("hidden", phase !== "waiting_login");
  $("btnStop").classList.toggle(
    "hidden",
    !["waiting_login", "listening"].includes(phase)
  );
  $("btnReset").classList.toggle("hidden", !["done", "failed"].includes(phase));

  if (s.lastError && phase === "failed") {
    showMsg(s.lastError, "err");
  }
}

async function softSync() {
  if (softSyncInFlight) return null;
  softSyncInFlight = true;
  try {
    const prev = await chrome.storage.local.get(["sessionPhase"]);
    const data = await sidecarGet("/job/status");
    const patch = await mergeSidecarIntoStorage(data);
    applyStoredState(patch, { skipKeywordInput: true });
    if (patch.sessionPhase === "done") {
      showMsg("盘后已完成", "ok");
    } else if (patch.sessionPhase === "failed" && patch.lastError) {
      showMsg(patch.lastError, "err");
    } else if (
      (prev.sessionPhase === "waiting_login" || prev.sessionPhase === "listening") &&
      data.phase === "idle"
    ) {
      showMsg(
        "sidecar 已重启但扩展仍在监听；落盘时会自动恢复。若仍无数据请重启 sidecar 后重新「确认登录」。",
        "ok"
      );
    }
    return data;
  } catch {
    return null;
  } finally {
    softSyncInFlight = false;
  }
}

$("keyword").addEventListener("input", () => {
  const v = $("keyword").value.trim();
  if (v) chrome.storage.local.set({ draftKeyword: v });
});

$("btnStart").addEventListener("click", async () => {
  showMsg("");
  const keyword = $("keyword").value.trim();
  if (!keyword) {
    showMsg("请输入搜索词", "err");
    return;
  }
  const sidecarUrl = $("sidecarUrl").value.trim();
  const createDjangoJob = $("createJob").checked;
  await chrome.storage.local.set({
    sidecarUrl,
    createDjangoJob,
    draftKeyword: keyword,
    sessionKeyword: keyword,
    stats: defaultStats(),
    statsSessionKey: "",
  });
  $("btnStart").disabled = true;
  showMsg("正在启动（建目录/任务可能需几秒）…", "ok");
  try {
    const data = await sidecarPost("/job/start", {
      keyword,
      create_django_job: createDjangoJob,
    });
    const sessionKey = sessionKeyForStats(data.run_dir, data.job_id);
    await chrome.storage.local.set({
      sessionKeyword: keyword,
      draftKeyword: keyword,
      runDir: data.run_dir,
      jobId: data.job_id,
      stats: defaultStats(),
      statsSessionKey: sessionKey,
    });
    const patch = await mergeSidecarIntoStorage(data);
    patch.sessionKeyword = keyword;
    await chrome.storage.local.set({ sessionKeyword: keyword });
    await notifyBackgroundStorage();
    applyStoredState({ ...patch, sessionKeyword: keyword });
    showMsg("已创建目录，请登录后点「确认登录」", "ok");
  } catch (e) {
    showMsg(String(e.message || e), "err");
  } finally {
    $("btnStart").disabled = false;
  }
});

$("btnConfirm").addEventListener("click", async () => {
  showMsg("");
  $("btnConfirm").disabled = true;
  try {
    const data = await sidecarPost("/job/confirm-login", {});
    await chrome.storage.local.set({
      sessionPhase: "listening",
      captureEnabled: true,
    });
    const patch = await mergeSidecarIntoStorage(data);
    patch.sessionPhase = "listening";
    patch.captureEnabled = true;
    await notifyBackgroundStorage();
    applyStoredState(patch);
    showMsg("监听已开始，可翻页浏览", "ok");
  } catch (e) {
    showMsg(String(e.message || e), "err");
  } finally {
    $("btnConfirm").disabled = false;
  }
});

$("btnStop").addEventListener("click", async () => {
  showMsg("");
  $("btnStop").disabled = true;
  try {
    chrome.runtime.sendMessage({ type: "flush_now" }, () => void chrome.runtime.lastError);
    const data = await sidecarPost("/job/stop", {
      run_postprocess: $("createJob").checked,
    });
    const patch = await mergeSidecarIntoStorage(data);
    await notifyBackgroundStorage();
    applyStoredState(patch);
    showMsg("已结束采集，盘后处理中…", "ok");
    softSync();
  } catch (e) {
    showMsg(String(e.message || e), "err");
  } finally {
    $("btnStop").disabled = false;
  }
});

$("btnReset").addEventListener("click", async () => {
  const draft = $("keyword").value.trim() || $("sessionKeyword").value.trim();
  await chrome.storage.local.set({
    sessionPhase: "idle",
    captureEnabled: false,
    lastError: "",
    draftKeyword: draft,
    stats: defaultStats(),
    statsSessionKey: "",
  });
  await notifyBackgroundStorage();
  applyStoredState({
    sessionPhase: "idle",
    stats: defaultStats(),
    statsSessionKey: "",
    draftKeyword: draft,
  });
  showMsg("");
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") return;
  const onlyStats =
    changes.stats &&
    !changes.sessionPhase &&
    !changes.sessionKeyword &&
    !changes.draftKeyword;
  if (changes.stats || changes.sessionPhase) {
    chrome.storage.local.get(null, (stored) => {
      applyStoredState(stored, { skipKeywordInput: onlyStats });
    });
  }
});

/** 清理旧版 storage.keyword（常被 sync 写成 manual，导致输入框被刷回） */
async function migrateLegacyStorage() {
  const stored = await chrome.storage.local.get(null);
  const patch = {};
  if (stored.keyword && !stored.sessionKeyword && stored.keyword !== "manual") {
    patch.sessionKeyword = stored.keyword;
  }
  if (stored.keyword !== undefined) {
    await chrome.storage.local.remove(["keyword"]);
  }
  if (Object.keys(patch).length) await chrome.storage.local.set(patch);
  return { ...stored, ...patch, keyword: undefined };
}

(async function init() {
  const stored = await migrateLegacyStorage();
  if (stored.draftKeyword && !stored.sessionPhase) {
    $("keyword").value = stored.draftKeyword;
  } else if (!stored.sessionPhase && stored.sessionKeyword) {
    $("keyword").value = stored.sessionKeyword;
  }
  applyStoredState(stored);
  await softSync();
  setInterval(() => softSync(), 3000);
})();
