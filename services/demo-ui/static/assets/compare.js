const state = {
  selectedUserId: null,
  busy: false,
};

const els = {
  status: document.querySelector("#compareStatus"),
  userSelect: document.querySelector("#userSelect"),
  baselineTag: document.querySelector("#baselineTag"),
  resilientTag: document.querySelector("#resilientTag"),
  baselineExperience: document.querySelector("#baselineExperience"),
  resilientExperience: document.querySelector("#resilientExperience"),
  compareMetrics: document.querySelector("#compareMetrics"),
  compareTimeline: document.querySelector("#compareTimeline"),
  refreshButton: document.querySelector("#refreshButton"),
};

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

async function refresh() {
  const query = state.selectedUserId ? `?user_id=${encodeURIComponent(state.selectedUserId)}` : "";
  try {
    const payload = await api(`/api/social/experience${query}`);
    if (!state.selectedUserId && payload.selected_user) {
      state.selectedUserId = payload.selected_user.id;
    }
    render(payload);
  } catch (error) {
    renderStatus("down", "Disconnected");
    els.baselineExperience.innerHTML = emptyState(error.message);
    els.resilientExperience.innerHTML = emptyState(error.message);
  }
}

function render(payload) {
  renderStatus(statusForSystem(payload.system), statusCopy(payload.system));
  renderUserSelect(payload.users);
  renderExperience(els.baselineExperience, els.baselineTag, payload.without_healing, "baseline");
  renderExperience(els.resilientExperience, els.resilientTag, payload.with_healing, "resilient");
  renderMetrics(payload);
  renderTimeline(payload.timeline);
}

function renderStatus(status, text) {
  els.status.innerHTML = `<span class="status-dot ${status}"></span><span>${escapeHtml(text)}</span>`;
}

function renderUserSelect(users) {
  const previous = state.selectedUserId || els.userSelect.value;
  els.userSelect.innerHTML = users.length
    ? users
        .map(
          (user) =>
            `<option value="${escapeHtml(user.id)}">${escapeHtml(user.display_name)} · ${escapeHtml(shortUsername(user.username))}</option>`,
        )
        .join("")
    : `<option value="">Seed first</option>`;
  const next = users.find((user) => user.id === previous)?.id || users[0]?.id || "";
  state.selectedUserId = next;
  els.userSelect.value = next;
}

function renderExperience(container, tag, experience, variant) {
  const status = normalizeExperienceStatus(experience.status);
  tag.textContent = experience.status === "failed" ? "user leaves" : experience.source;
  tag.className = `panel-tag tag-${status}`;
  if (experience.status === "failed") {
    container.innerHTML = `
      <div class="phone-header">
        <span class="phone-brand">TimeScroll</span>
        <span class="phone-status failed">stalled</span>
      </div>
      <div class="failed-feed">
        <div class="loader-ring"></div>
        <h3>Feed is still loading</h3>
        <p>${escapeHtml(experience.message)}</p>
        <strong>${escapeHtml(formatMs(experience.load_ms))}</strong>
      </div>
    `;
    return;
  }
  container.innerHTML = `
    <div class="phone-header">
      <span class="phone-brand">TimeScroll</span>
      <span class="phone-status ${status}">${escapeHtml(experience.status)}</span>
    </div>
    <div class="phone-banner ${status}">
      <strong>${escapeHtml(experience.message)}</strong>
      <span>${escapeHtml(formatMs(experience.load_ms))} · ${escapeHtml(experience.source)}</span>
    </div>
    <div class="phone-feed ${variant}">
      ${renderPosts(experience.items)}
    </div>
  `;
}

function renderPosts(items) {
  if (!items.length) return emptyState("Seed demo data to populate this feed.");
  return items
    .slice(0, 5)
    .map(
      (item, index) => `
        <article class="mini-post">
          <div class="history-thumb thumb-${item.media_index ?? index % 6}"></div>
          <div>
            <strong>${escapeHtml(item.author?.display_name || "History Creator")}</strong>
            <p>${escapeHtml(item.content)}</p>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderMetrics(payload) {
  const system = payload.system;
  els.compareMetrics.innerHTML = [
    ["Without", formatMs(payload.without_healing.load_ms), payload.without_healing.status === "failed" ? "down" : "ready"],
    ["With", formatMs(payload.with_healing.load_ms), payload.with_healing.status === "recovering" ? "degraded" : "ready"],
    ["Redis", system.redis_status, system.redis_status === "ready" ? "ready" : "degraded"],
    ["Services", `${system.services_ready}/${system.services_total}`, system.feed_status === "down" ? "down" : "ready"],
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

function renderTimeline(timeline) {
  els.compareTimeline.innerHTML = timeline
    .map(
      (item) => `
        <article class="timeline-item ${escapeHtml(item.state)}">
          <span></span>
          <p>${escapeHtml(item.label)}</p>
        </article>
      `,
    )
    .join("");
}

function statusForSystem(system) {
  if (system.feed_status === "down") return "down";
  if (system.redis_status !== "ready" || system.feed_source === "postgres_degraded") return "degraded";
  return "ready";
}

function statusCopy(system) {
  if (system.feed_status === "down") return "Feed down";
  if (system.redis_status !== "ready") return "Outage active";
  return `${system.services_ready}/${system.services_total} ready`;
}

function normalizeExperienceStatus(status) {
  if (status === "ready") return "ready";
  if (status === "failed") return "down";
  return "degraded";
}

function formatMs(value) {
  if (!value) return "0 ms";
  return value >= 1000 ? `${(value / 1000).toFixed(1)} s` : `${value} ms`;
}

function emptyState(text) {
  return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

function shortUsername(username) {
  return String(username || "history").replace(/^demo_\d+_/, "");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

els.userSelect.addEventListener("change", () => {
  state.selectedUserId = els.userSelect.value;
  refresh();
});
els.refreshButton.addEventListener("click", refresh);

refresh();
setInterval(refresh, 5000);
