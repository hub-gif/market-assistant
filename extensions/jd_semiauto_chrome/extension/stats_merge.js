/** popup + service worker 共用：按 run_dir 会话合并计数，新任务清零 */
function defaultStats() {
  return { list: 0, detail: 0, comment: 0, graphic: 0, posted: 0 };
}

function statsFromSidecar(sidecarCounts) {
  const stats = defaultStats();
  if (!sidecarCounts) return stats;
  for (const k of ["list", "detail", "comment", "graphic"]) {
    const n = sidecarCounts[k];
    if (typeof n === "number" && n >= 0) stats[k] = n;
  }
  if (typeof sidecarCounts.written === "number" && sidecarCounts.written >= 0) {
    stats.posted = sidecarCounts.written;
  } else {
    stats.posted = stats.list + stats.detail + stats.comment + stats.graphic;
  }
  return stats;
}

function sessionKeyForStats(runDir, jobId) {
  const rd = (runDir || "").trim();
  if (rd) return rd;
  if (jobId != null && jobId !== "") return `job:${jobId}`;
  return "";
}

/** 换 run_dir / 任务 → 以 sidecar 为准（常为 0）；同任务且 sidecar 有数 → 覆盖本地；否则保留本地 */
function resolveStats(prevStats, sidecarCounts, sessionKey, prevSessionKey) {
  if (sessionKey && sessionKey !== (prevSessionKey || "")) {
    return statsFromSidecar(sidecarCounts);
  }
  if (sidecarCounts && typeof sidecarCounts === "object") {
    const sum =
      (Number(sidecarCounts.list) || 0) +
      (Number(sidecarCounts.detail) || 0) +
      (Number(sidecarCounts.comment) || 0) +
      (Number(sidecarCounts.graphic) || 0) +
      (Number(sidecarCounts.written) || 0);
    if (sum > 0) return statsFromSidecar(sidecarCounts);
  }
  return { ...defaultStats(), ...(prevStats || {}) };
}
