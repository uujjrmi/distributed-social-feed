const state = {
  overview: null,
  selectedUserId: null,
  busy: false,
};

const els = {
  globalStatus: document.querySelector("#globalStatus"),
  metricStrip: document.querySelector("#metricStrip"),
  meshTag: document.querySelector("#meshTag"),
  topology: document.querySelector("#serviceTopology"),
  commandLog: document.querySelector("#commandLog"),
  feedList: document.querySelector("#feedList"),
  incidentList: document.querySelector("#incidentList"),
  incidentTag: document.querySelector("#incidentTag"),
  userSelect: document.querySelector("#userSelect"),
  refreshButton: document.querySelector("#refreshButton"),
  seedButton: document.querySelector("#seedButton"),
  postButton: document.querySelector("#postButton"),
  redisButton: document.querySelector("#redisButton"),
  trafficButton: document.querySelector("#trafficButton"),
  feedCrashButton: document.querySelector("#feedCrashButton"),
  recoverButton: document.querySelector("#recoverButton"),
  resetButton: document.querySelector("#resetButton"),
};

const actionButtons = [
  els.seedButton,
  els.postButton,
  els.redisButton,
  els.trafficButton,
  els.feedCrashButton,
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
  actionButtons.forEach((button) => {
    button.disabled = isBusy;
  });
  els.refreshButton.disabled = isBusy;
}

function log(message) {
  const stamp = new Date().toLocaleTimeString();
  els.commandLog.textContent = `[${stamp}] ${message}\n${els.commandLog.textContent}`.slice(0, 2400);
}

async function refresh() {
  try {
    const overview = await api("/api/overview");
    state.overview = overview;
    renderOverview(overview);
    await refreshFeed();
  } catch (error) {
    renderGlobalStatus("down", "Disconnected");
    log(`refresh failed: ${error.message}`);
  }
}

function renderOverview(overview) {
  const { summary, stats, services, incidents, users } = overview;
  renderGlobalStatus(
    summary.healthy_services === summary.total_services ? "ready" : "degraded",
    `${summary.healthy_services}/${summary.total_services} ready`,
  );
  renderMetrics(summary, stats);
  renderTopology(services);
  renderUsers(users);
  renderIncidents(incidents);
}

function renderGlobalStatus(status, text) {
  els.globalStatus.innerHTML = `<span class="status-dot ${status}"></span><span>${escapeHtml(text)}</span>`;
}

function renderMetrics(summary, stats) {
  const metrics = [
    {
      label: "Availability",
      value: `${summary.availability_score.toFixed(1)}%`,
      foot: `${summary.healthy_services} services ready`,
    },
    {
      label: "Social Graph",
      value: number(stats.users),
      foot: `${number(stats.follows)} follows`,
    },
    {
      label: "Event Fanout",
      value: number(stats.posts),
      foot: `${number(stats.notifications)} notifications`,
    },
  ];
  els.metricStrip.innerHTML = metrics
    .map(
      (item) => `
      <article class="metric">
        <div class="metric-label">${escapeHtml(item.label)}</div>
        <div class="metric-value">${escapeHtml(item.value)}</div>
        <div class="metric-foot">${escapeHtml(item.foot)}</div>
      </article>
    `,
    )
    .join("");
}

function renderTopology(services) {
  const ready = services.filter((service) => service.status === "ready").length;
  els.meshTag.textContent = `${ready} / ${services.length} ready`;
  els.topology.innerHTML = services
    .map((service) => {
      const status = normalizeStatus(service.status);
      const latency = service.latency_ms === null ? "no response" : `${service.latency_ms} ms`;
      return `
        <article class="service-node ${status}">
          <div class="node-head">
            <span class="status-dot ${status}"></span>
            <span class="panel-tag">${escapeHtml(service.status)}</span>
          </div>
          <div class="node-name">${escapeHtml(service.name)}</div>
          <div class="node-kind">${escapeHtml(service.kind)}</div>
          <div class="node-latency">${escapeHtml(latency)}</div>
        </article>
      `;
    })
    .join("");
}

function renderUsers(users) {
  const previous = state.selectedUserId || els.userSelect.value;
  els.userSelect.innerHTML = users.length
    ? users
        .map(
          (user) =>
            `<option value="${escapeHtml(user.id)}">${escapeHtml(user.display_name)} · ${escapeHtml(user.username)}</option>`,
        )
        .join("")
    : `<option value="">Seed data first</option>`;
  const next = users.find((user) => user.id === previous)?.id || users[0]?.id || "";
  state.selectedUserId = next;
  els.userSelect.value = next;
}

async function refreshFeed() {
  if (!state.selectedUserId) {
    els.feedList.innerHTML = `<div class="empty-state">No feed user available.</div>`;
    return;
  }
  try {
    const feed = await api(`/api/feed/${state.selectedUserId}?limit=8`);
    renderFeed(feed);
  } catch (error) {
    els.feedList.innerHTML = `<div class="empty-state">Feed unavailable: ${escapeHtml(error.message)}</div>`;
  }
}

function renderFeed(feed) {
  if (!feed.items || feed.items.length === 0) {
    els.feedList.innerHTML = `<div class="empty-state">This feed has no materialized posts yet.</div>`;
    return;
  }
  els.feedList.innerHTML = feed.items
    .map(
      (item) => `
        <article class="feed-item">
          <div class="feed-meta">
            <span>${escapeHtml(feed.source || "feed")}</span>
            <span>${formatTime(item.created_at)}</span>
          </div>
          <p class="feed-content">${escapeHtml(item.content)}</p>
        </article>
      `,
    )
    .join("");
}

function renderIncidents(incidents) {
  els.incidentTag.textContent = `${incidents.length} incidents`;
  if (!incidents.length) {
    els.incidentList.innerHTML = `<div class="empty-state">No incidents recorded.</div>`;
    return;
  }
  els.incidentList.innerHTML = incidents
    .map((incident) => {
      const signals = parseSignals(incident.signals).join(" · ");
      return `
        <article class="incident-item">
          <div class="incident-meta">
            <span>${escapeHtml(incident.severity)} · ${escapeHtml(incident.status)}</span>
            <span>${formatTime(incident.detected_at)}</span>
          </div>
          <p class="incident-title">${escapeHtml(incident.service)} / ${escapeHtml(incident.type)}</p>
          <p class="incident-signals">${escapeHtml(signals || "Policy action recorded")}</p>
        </article>
      `;
    })
    .join("");
}

async function runAction(label, path, options = {}) {
  setBusy(true);
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

function normalizeStatus(status) {
  if (status === "ready" || status === "ok") return "ready";
  if (status === "down" || status === "failed") return "down";
  return "degraded";
}

function parseSignals(signals) {
  if (Array.isArray(signals)) return signals;
  if (typeof signals !== "string") return [];
  try {
    const parsed = JSON.parse(signals);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [signals];
  }
}

function formatTime(value) {
  if (!value) return "now";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function number(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function compact(payload) {
  return JSON.stringify(payload).replace(/\s+/g, " ").slice(0, 220);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

els.refreshButton.addEventListener("click", refresh);
els.userSelect.addEventListener("change", () => {
  state.selectedUserId = els.userSelect.value;
  refreshFeed();
});
els.seedButton.addEventListener("click", () => runAction("seed data", "/api/actions/seed"));
els.postButton.addEventListener("click", () =>
  runAction("create post", "/api/actions/post", {
    body: JSON.stringify({
      author_id: state.selectedUserId,
      content: `Live demo post at ${new Date().toLocaleTimeString()}`,
    }),
  }),
);
els.redisButton.addEventListener("click", () => runAction("redis outage", "/api/actions/redis-outage"));
els.trafficButton.addEventListener("click", () => runAction("feed traffic", "/api/actions/traffic"));
els.feedCrashButton.addEventListener("click", () => runAction("feed crash", "/api/actions/feed-crash"));
els.recoverButton.addEventListener("click", () => runAction("recover", "/api/actions/recover"));
els.resetButton.addEventListener("click", () => runAction("reset demo", "/api/actions/reset-demo"));

log("cockpit online");
refresh();
setInterval(refresh, 8000);
