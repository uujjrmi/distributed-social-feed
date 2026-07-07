const state = {
  selectedUserId: null,
  busy: false,
};

const els = {
  status: document.querySelector("#socialStatus"),
  creatorList: document.querySelector("#creatorList"),
  composerName: document.querySelector("#composerName"),
  feedModeBanner: document.querySelector("#feedModeBanner"),
  historyFeed: document.querySelector("#historyFeed"),
  miniMetrics: document.querySelector("#miniMetrics"),
  agentTimeline: document.querySelector("#agentTimeline"),
  notificationCount: document.querySelector("#notificationCount"),
  notificationList: document.querySelector("#notificationList"),
  postButton: document.querySelector("#postButton"),
};

const actionButtons = [els.postButton];

const postPrompts = [
  "The weirdest part of Roman concrete is not that it lasted. It is that some formulas healed cracks better with age.",
  "A medieval map can be geographically wrong and politically accurate at the same time.",
  "The archive photo looks quiet until you notice the power lines, uniforms, and who is missing from the frame.",
  "A museum label is the shortest possible argument about why an object matters.",
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
    els.historyFeed.innerHTML = emptyState(`Feed unavailable: ${error.message}`);
  }
}

function render(payload) {
  renderStatus(statusForSystem(payload.system), statusCopy(payload.system));
  renderCreators(payload.users);
  renderFeedBanner(payload.with_healing);
  renderFeed(payload.with_healing.items);
  renderMetrics(payload);
  renderTimeline(payload.timeline);
  renderNotifications(payload.notifications);
  els.composerName.textContent = payload.selected_user
    ? payload.selected_user.display_name
    : "Seed the network";
}

function renderStatus(status, text) {
  els.status.innerHTML = `<span class="status-dot ${status}"></span><span>${escapeHtml(text)}</span>`;
}

function renderCreators(users) {
  if (!users.length) {
    els.creatorList.innerHTML = emptyState("No creators yet.");
    return;
  }
  els.creatorList.innerHTML = users
    .slice(0, 12)
    .map((user, index) => {
      const active = user.id === state.selectedUserId ? "active" : "";
      return `
        <button class="creator-button ${active}" data-user-id="${escapeHtml(user.id)}" type="button">
          <span class="avatar theme-${index % 6}">${initials(user.display_name)}</span>
          <span>
            <strong>${escapeHtml(user.display_name)}</strong>
            <small>@${escapeHtml(shortUsername(user.username))}</small>
          </span>
        </button>
      `;
    })
    .join("");
  els.creatorList.querySelectorAll("[data-user-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedUserId = button.dataset.userId;
      refresh();
    });
  });
}

function renderFeedBanner(experience) {
  const status = normalizeExperienceStatus(experience.status);
  els.feedModeBanner.className = `feed-mode-banner ${status}`;
  els.feedModeBanner.innerHTML = `
    <span class="status-dot ${status}"></span>
    <div>
      <strong>${escapeHtml(modeTitle(experience))}</strong>
      <span>${escapeHtml(experience.message)} Load path: ${escapeHtml(experience.source)}.</span>
    </div>
  `;
}

function renderFeed(items) {
  if (!items.length) {
    els.historyFeed.innerHTML = emptyState("Seed demo data to populate the feed.");
    return;
  }
  els.historyFeed.innerHTML = items
    .map(
      (item, index) => `
        <article class="history-card">
          <div class="history-thumb thumb-${item.media_index ?? index % 6}"></div>
          <div class="history-card-body">
            <div class="post-meta">
              <span class="avatar theme-${index % 6}">${initials(item.author?.display_name || "History")}</span>
              <span>
                <strong>${escapeHtml(item.author?.display_name || "History Creator")}</strong>
                <small>${formatTime(item.created_at)}</small>
              </span>
            </div>
            <p>${escapeHtml(item.content)}</p>
            <div class="post-actions">
              <span>Save</span>
              <span>Discuss</span>
              <span>Share</span>
            </div>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderMetrics(payload) {
  const system = payload.system;
  const exp = payload.with_healing;
  els.miniMetrics.innerHTML = [
    ["Services", `${system.services_ready}/${system.services_total}`, "ready"],
    ["Redis", system.redis_status, system.redis_status === "ready" ? "ready" : "degraded"],
    ["Feed", exp.source, exp.status === "ready" ? "ready" : "degraded"],
    ["Load", `${exp.load_ms} ms`, exp.status === "recovering" ? "degraded" : "ready"],
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
  els.agentTimeline.innerHTML = timeline
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

function renderNotifications(notifications) {
  els.notificationCount.textContent = number(notifications.count);
  if (!notifications.items.length) {
    els.notificationList.innerHTML = `<div class="soft-empty">No recent notifications.</div>`;
    return;
  }
  els.notificationList.innerHTML = notifications.items
    .map(
      (item) => `
        <article class="notification-item">
          <strong>${escapeHtml(item.actor_name)}</strong>
          <span>posted a new history note</span>
        </article>
      `,
    )
    .join("");
}

async function runAction(label, path, options = {}) {
  setBusy(true);
  renderStatus("pending", `${label}...`);
  try {
    await api(path, { method: "POST", ...options });
    await refresh();
  } finally {
    setBusy(false);
  }
}

function createPost() {
  const content = postPrompts[Math.floor(Math.random() * postPrompts.length)];
  return runAction("posting", "/api/actions/post", {
    body: JSON.stringify({ author_id: state.selectedUserId, content }),
  });
}

function statusForSystem(system) {
  if (system.feed_status === "down") return "down";
  if (system.redis_status !== "ready" || system.feed_source === "postgres_degraded") return "degraded";
  return "ready";
}

function statusCopy(system) {
  if (system.feed_status === "down") return "Feed down";
  if (system.redis_status !== "ready") return "Resilient mode";
  return `${system.services_ready}/${system.services_total} ready`;
}

function normalizeExperienceStatus(status) {
  if (status === "ready") return "ready";
  if (status === "failed") return "down";
  return "degraded";
}

function modeTitle(experience) {
  if (experience.status === "resilient") return "Resilient mode active";
  if (experience.status === "recovering") return "Recovering feed service";
  return "Fast feed path";
}

function emptyState(text) {
  return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

function initials(name) {
  return String(name || "U")
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase();
}

function shortUsername(username) {
  return String(username || "history").replace(/^demo_\d+_/, "");
}

function formatTime(value) {
  if (!value) return "now";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

els.postButton.addEventListener("click", createPost);

refresh();
setInterval(refresh, 5000);
