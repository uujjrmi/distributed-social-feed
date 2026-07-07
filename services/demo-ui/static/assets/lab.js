const state = {
  overview: null,
  busy: false,
};

const els = {
  status: document.querySelector("#labStatus"),
  metrics: document.querySelector("#labMetrics"),
  services: document.querySelector("#labServices"),
  incidents: document.querySelector("#labIncidents"),
  log: document.querySelector("#labLog"),
  seedButton: document.querySelector("#seedButton"),
  postButton: document.querySelector("#postButton"),
  trafficButton: document.querySelector("#trafficButton"),
  redisButton: document.querySelector("#redisButton"),
  feedCrashButton: document.querySelector("#feedCrashButton"),
  notificationCrashButton: document.querySelector("#notificationCrashButton"),
  recoverButton: document.querySelector("#recoverButton"),
  resetButton: document.querySelector("#resetButton"),
};

const buttons = [
  els.seedButton,
  els.postButton,
  els.trafficButton,
  els.redisButton,
  els.feedCrashButton,
  els.notificationCrashButton,
  els.recoverButton,
  els.resetButton,
];

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "content-type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = payload.detail ? JSON.stringify(payload.detail) : response.statusText;
    throw new Error(detail);
  }
  return payload;
}

function setBusy(isBusy) {
  state.busy = isBusy;
  buttons.forEach((button) => {
    button.disabled = isBusy;
  });
}

function log(message) {
  const stamp = new Date().toLocaleTimeString();
  els.log.textContent = `[${stamp}] ${message}\n${els.log.textContent}`.slice(0, 2200);
}

async function refresh() {
  try {
    const overview = await api("/api/overview");
    state.overview = overview;
    renderOverview(overview);
  } catch (error) {
    renderStatus("down", "Disconnected");
    log(`refresh failed: ${error.message}`);
  }
}

function renderOverview(overview) {
  const status = overview.summary.healthy_services === overview.summary.total_services ? "ready" : "degraded";
  renderStatus(status, `${overview.summary.healthy_services}/${overview.summary.total_services} ready`);
  renderMetrics(overview);
  renderServices(overview.services);
  renderIncidents(overview.incidents);
}

function renderStatus(status, text) {
  els.status.innerHTML = `<span class="status-dot ${status}"></span><span>${escapeHtml(text)}</span>`;
}

function renderMetrics(overview) {
  const redis = overview.services.find((service) => service.id === "feed")?.detail?.redis || "unknown";
  els.metrics.innerHTML = [
    ["Availability", `${overview.summary.availability_score.toFixed(1)}%`, "ready"],
    ["Users", number(overview.stats.users), "ready"],
    ["Posts", number(overview.stats.posts), "ready"],
    ["Redis", redis, redis === "ready" ? "ready" : "degraded"],
  ]
    .map(
      ([label, value, status]) => `
        <article class="mini-metric">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
          <i class="${escapeHtml(status)}"></i>
        </article>
      `,
    )
    .join("");
}

function renderServices(services) {
  els.services.innerHTML = services
    .map((service) => {
      const status = normalizeStatus(service.status);
      return `
        <article class="service-mini ${status}">
          <span class="status-dot ${status}"></span>
          <strong>${escapeHtml(service.name)}</strong>
          <small>${escapeHtml(service.status)} · ${service.latency_ms ?? "no"} ms</small>
        </article>
      `;
    })
    .join("");
}

function renderIncidents(incidents) {
  if (!incidents.length) {
    els.incidents.innerHTML = `<div class="soft-empty">No incidents recorded.</div>`;
    return;
  }
  els.incidents.innerHTML = incidents
    .map(
      (incident) => `
        <article class="timeline-item ${incident.status === "resolved" ? "resilient" : "danger"}">
          <span></span>
          <p>${escapeHtml(incident.service)} recorded ${escapeHtml(incident.type)} as ${escapeHtml(incident.status)}.</p>
        </article>
      `,
    )
    .join("");
}

async function runAction(label, path, options = {}) {
  setBusy(true);
  renderStatus("pending", label);
  log(`${label} started`);
  try {
    const payload = await api(path, { method: "POST", ...options });
    log(`${label} complete: ${compact(payload)}`);
    await refresh();
  } catch (error) {
    log(`${label} failed: ${error.message}`);
  } finally {
    setBusy(false);
  }
}

function createPost() {
  const user = state.overview?.users?.[0];
  if (!user) {
    log("create post skipped: seed data first");
    return;
  }
  return runAction("Create post", "/api/actions/post", {
    body: JSON.stringify({
      author_id: user.id,
      content: "A live scenario-lab post entered the history feed through Kafka.",
    }),
  });
}

function normalizeStatus(status) {
  if (status === "ready" || status === "ok") return "ready";
  if (status === "down" || status === "failed") return "down";
  return "degraded";
}

function compact(payload) {
  return JSON.stringify(payload).replace(/\s+/g, " ").slice(0, 220);
}

function number(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

els.seedButton.addEventListener("click", () => runAction("Seed social graph", "/api/actions/seed"));
els.postButton.addEventListener("click", createPost);
els.trafficButton.addEventListener("click", () => runAction("Generate feed traffic", "/api/actions/traffic"));
els.redisButton.addEventListener("click", () => runAction("Redis outage", "/api/actions/redis-outage"));
els.feedCrashButton.addEventListener("click", () => runAction("Feed service crash", "/api/actions/feed-crash"));
els.notificationCrashButton.addEventListener("click", () =>
  runAction("Notification service crash", "/api/actions/notification-crash"),
);
els.recoverButton.addEventListener("click", () => runAction("Recover services", "/api/actions/recover"));
els.resetButton.addEventListener("click", () => runAction("Reset demo", "/api/actions/reset-demo"));

log("scenario lab online");
refresh();
setInterval(refresh, 5000);
