// Forge web UI — single-file ES module, no build step, no deps.
//
// Sections:
//   1. State + persistence
//   2. API client (auth-aware fetch wrapper)
//   3. Markdown renderer (dependency-free, XSS-safe)
//   4. Auth (login/register/logout)
//   5. Sessions (list/select/new)
//   6. Chat (history, send, live SSE stream, tool cards)
//   7. Voice — STT (Parakeet) + TTS (Kokoro)
//   8. UI chrome (drawer, dialogs, toast, textarea autosize)
//   9. Boot

// ============================================================
// 1. State + persistence
// ============================================================

const API_BASE = ""; // same-origin; the forge API serves the UI
const LS = {
  key: "forge.apiKey",
  email: "forge.email",
  voice: "forge.voice",
  autoSpeak: "forge.autoSpeak",
  dictateTap: "forge.dictateTap",
  showTools: "forge.showTools",
};

const state = {
  apiKey: localStorage.getItem(LS.key) || "",
  user: null,
  profiles: [], // id -> profile (joined into sessions)
  sessions: [],
  currentSessionId: null,
  lastSeq: 0, // max message sequence rendered for the current session
  renderedSeqs: new Set(), // dedupe SSE deliveries
  lastAssistantBubble: null, // for merging consecutive assistant text chunks
  toolCards: new Map(), // tool_call_id -> card element (for pairing result into call)
  sseController: null, // AbortController for the active stream
  status: "idle", // idle | thinking | tool | error | done
  voice: { stt: false, tts: false, defaultVoice: "af_heart", voices: [] },
  settings: {
    voice: localStorage.getItem(LS.voice) || "",
    autoSpeak: localStorage.getItem(LS.autoSpeak) === "1",
    dictateTap: localStorage.getItem(LS.dictateTap) !== "0", // default on
    showTools: localStorage.getItem(LS.showTools) !== "0",
  },
  ttsAudio: null, // currently playing <audio> (for stop/restart)
};

// ============================================================
// 2. API client
// ============================================================

async function api(path, { method = "GET", body, headers = {}, raw = false, signal } = {}) {
  const opts = {
    method,
    headers: {
      ...(state.apiKey ? { "X-API-Key": state.apiKey } : {}),
      ...headers,
    },
    signal,
  };
  if (body !== undefined) {
    if (body instanceof FormData) {
      opts.body = body; // let fetch set the multipart boundary
    } else {
      opts.headers["Content-Type"] = "application/json";
      opts.body = typeof body === "string" ? body : JSON.stringify(body);
    }
  }
  const resp = await fetch(API_BASE + path, opts);
  if (resp.status === 401) {
    // Key invalid/expired — force re-login.
    state.apiKey = "";
    localStorage.removeItem(LS.key);
    showLogin();
    throw new Error("Unauthorized");
  }
  if (!resp.ok) {
    let detail = "";
    try { detail = (await resp.json()).error || ""; } catch {}
    throw new Error(detail || `${resp.status} ${resp.statusText}`);
  }
  return raw ? resp : resp.json();
}

// ============================================================
// 3. Markdown renderer (dependency-free, XSS-safe)
//
// Escape first, then transform the escaped text. Code fences are
// extracted before inline processing so their content is never
// mangled by inline rules. Supports: headings, bold, italic,
// inline code, fenced code, links, lists (ul/ol), blockquotes,
// hr, tables, paragraphs. Good enough for agent output.
// ============================================================

function escapeHtml(s) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderMarkdown(text) {
  if (!text) return "";
  let src = escapeHtml(text);
  const codeBlocks = [];

  // Extract fenced code blocks (```lang\n...```). Preserve the
  // raw (escaped) content; render with a lang label + copy btn.
  src = src.replace(/```([a-zA-Z0-9_+-]*)\n?([\s\S]*?)```/g, (_, lang, code) => {
    const id = codeBlocks.length;
    codeBlocks.push({ lang: lang || "text", code: code.replace(/\n$/, "") });
    return `\u0000CODE${id}\u0000`;
  });

  // Inline code (after fences, so fence content is already gone).
  src = src.replace(/`([^`\n]+)`/g, (_, c) => `<code>${c}</code>`);

  // Process block-level line by line.
  const lines = src.split("\n");
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];

    // Code-block placeholder on its own line -> render the block.
    const cb = line.match(/^\u0000CODE(\d+)\u0000$/);
    if (cb) {
      out.push(renderCodeBlock(codeBlocks[+cb[1]]));
      i++;
      continue;
    }

    // Blank line.
    if (/^\s*$/.test(line)) { i++; continue; }

    // Headings.
    const h = line.match(/^(#{1,6})\s+(.*)$/);
    if (h) { out.push(`<h${h[1].length}>${inline(h[2])}</h${h[1].length}>`); i++; continue; }

    // Horizontal rule.
    if (/^\s*([-*_])\1{2,}\s*$/.test(line)) { out.push("<hr>"); i++; continue; }

    // Blockquote (consecutive > lines).
    if (/^\s*&gt;\s?/.test(line)) {
      const quote = [];
      while (i < lines.length && /^\s*&gt;\s?/.test(lines[i])) {
        quote.push(lines[i].replace(/^\s*&gt;\s?/, ""));
        i++;
      }
      out.push(`<blockquote>${inline(quote.join(" "))}</blockquote>`);
      continue;
    }

    // Table (| a | b | + | --- | --- |).
    if (/^\s*\|.*\|\s*$/.test(line) && i + 1 < lines.length && /^\s*\|[\s:|-]+\|\s*$/.test(lines[i + 1])) {
      const header = splitRow(line);
      i += 2;
      const rows = [];
      while (i < lines.length && /^\s*\|.*\|\s*$/.test(lines[i])) {
        rows.push(splitRow(lines[i]));
        i++;
      }
      const thead = header.map((c) => `<th>${inline(c)}</th>`).join("");
      const tbody = rows.map((r) => `<tr>${r.map((c) => `<td>${inline(c)}</td>`).join("")}</tr>`).join("");
      out.push(`<table><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>`);
      continue;
    }

    // Unordered list.
    if (/^\s*([-*+])\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*([-*+])\s+/.test(lines[i])) {
        items.push(`<li>${inline(lines[i].replace(/^\s*([-*+])\s+/, ""))}</li>`);
        i++;
      }
      out.push(`<ul>${items.join("")}</ul>`);
      continue;
    }

    // Ordered list.
    if (/^\s*\d+\.\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
        items.push(`<li>${inline(lines[i].replace(/^\s*\d+\.\s+/, ""))}</li>`);
        i++;
      }
      out.push(`<ol>${items.join("")}</ol>`);
      continue;
    }

    // Paragraph: gather consecutive non-blank, non-block lines.
    const para = [];
    while (i < lines.length && !/^\s*$/.test(lines[i]) &&
      !/^(#{1,6})\s+/.test(lines[i]) &&
      !/^\u0000CODE\d+\u0000$/.test(lines[i]) &&
      !/^\s*&gt;\s?/.test(lines[i]) &&
      !/^\s*([-*+])\s+/.test(lines[i]) &&
      !/^\s*\d+\.\s+/.test(lines[i]) &&
      !/^\s*\|.*\|\s*$/.test(lines[i])) {
      para.push(lines[i]);
      i++;
    }
    if (para.length) out.push(`<p>${inline(para.join(" "))}</p>`);
  }

  // Restore code-block placeholders that ended up inside a
  // paragraph (rare; the line-based walker usually isolates them).
  let html = out.join("\n");
  html = html.replace(/\u0000CODE(\d+)\u0000/g, (_, id) => renderCodeBlock(codeBlocks[+id]));
  return `<div class="md">${html}</div>`;
}

function splitRow(line) {
  return line.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());
}

// Inline transformations: bold, italic, links, leftover code.
// Runs on already-escaped text.
function inline(s) {
  return s
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/__([^_]+)__/g, "<strong>$1</strong>")
    .replace(/(^|[^*])\*([^*]+)\*/g, "$1<em>$2</em>")
    .replace(/(^|[^_])_([^_]+)_/g, "$1<em>$2</em>")
    .replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
}

function renderCodeBlock({ lang, code }) {
  const id = "cb" + Math.random().toString(36).slice(2, 9);
  return `<div class="codeblk"><div class="codeblk-bar"><span class="codeblk-lang">${lang}</span><button class="codeblk-copy" data-copy="${id}">copy</button></div><pre id="${id}">${code}</pre></div>`;
}

// ============================================================
// 4. Auth
// ============================================================

const $ = (sel) => document.querySelector(sel);

async function bootstrapAuth() {
  if (!state.apiKey) { showLogin(); return; }
  try {
    // Validate the key by listing profiles (cheap, always works).
    const { profiles } = await api("/profiles");
    state.profiles = profiles || [];
    await loadCurrentUser();
    await loadSessions();
    hideLogin();
  } catch {
    showLogin();
  }
}

async function loadCurrentUser() {
  // The API has /users; list returns the current user (or all).
  // We just need a name for the chip; fall back to stored email.
  const email = localStorage.getItem(LS.email) || "";
  state.user = { email };
  renderUserChip();
}

function renderUserChip() {
  const chip = $("#user-chip");
  const logout = $("#logout-btn");
  if (state.user?.email) {
    chip.textContent = state.user.email;
    chip.hidden = false;
    logout.hidden = false;
  } else {
    chip.hidden = true;
    logout.hidden = true;
  }
}

function showLogin() {
  $("#login-dialog").showModal();
  $("#login-email").focus();
}
function hideLogin() { $("#login-dialog").close(); }

async function doLogin(email, password) {
  const err = $("#login-error");
  err.hidden = true;
  try {
    const { api_key } = await api("/auth/login", {
      method: "POST",
      body: { email, password },
    });
    state.apiKey = api_key;
    state.user = { email };
    localStorage.setItem(LS.key, api_key);
    localStorage.setItem(LS.email, email);
    hideLogin();
    renderUserChip();
    await loadSessions();
    toast("Signed in");
  } catch (e) {
    err.textContent = e.message || "Login failed";
    err.hidden = false;
  }
}

async function doRegister(email, password, name) {
  const err = $("#login-error");
  err.hidden = true;
  try {
    await api("/auth/register", {
      method: "POST",
      body: { email, password, name: name || email.split("@")[0] },
    });
    // Auto-login after register.
    await doLogin(email, password);
  } catch (e) {
    err.textContent = e.message || "Registration failed";
    err.hidden = false;
  }
}

async function doLogout() {
  try { await api("/auth/logout", { method: "POST" }); } catch {}
  state.apiKey = "";
  state.user = null;
  localStorage.removeItem(LS.key);
  state.sessions = [];
  state.currentSessionId = null;
  renderSessions();
  renderWelcome();
  renderUserChip();
  showLogin();
}

// ============================================================
// 5. Sessions
// ============================================================

async function loadSessions() {
  try {
    const [{ sessions }, { profiles }] = await Promise.all([
      api("/sessions"),
      state.profiles.length ? Promise.resolve({ profiles: state.profiles }) : api("/profiles"),
    ]);
    state.sessions = sessions || [];
    state.profiles = profiles || [];
    renderSessions();
  } catch (e) {
    toast("Failed to load sessions: " + e.message);
  }
}

function profileFor(id) {
  return state.profiles.find((p) => p.id === id);
}

function renderSessions() {
  const list = $("#sessions-list");
  list.innerHTML = "";
  if (!state.sessions.length) {
    list.innerHTML = '<div class="empty-hint">No sessions yet</div>';
    return;
  }
  for (const s of state.sessions) {
    const p = profileFor(s.profile_id);
    const el = document.createElement("div");
    el.className = "session-item" + (s.id === state.currentSessionId ? " active" : "");
    el.role = "listitem";
    el.dataset.id = s.id;
    const when = new Date(s.last_active || s.created_at).toLocaleString(undefined, {
      month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
    });
    el.innerHTML = `
      <div class="si-title">${escapeHtml(s.title || "Untitled")}</div>
      <div class="si-meta">
        ${p ? `<span class="si-badge">${escapeHtml(p.name)}</span>` : ""}
        <span>${when}</span>
      </div>`;
    el.addEventListener("click", () => selectSession(s.id));
    list.appendChild(el);
  }
}

async function selectSession(id) {
  if (state.currentSessionId === id) { closeDrawer(); return; }
  state.currentSessionId = id;
  state.lastSeq = 0;
  state.renderedSeqs = new Set();
  state.lastAssistantBubble = null;
  state.toolCards = new Map();
  closeSse();
  renderSessions();
  const s = state.sessions.find((x) => x.id === id);
  const p = s ? profileFor(s.profile_id) : null;
  $("#chat-title").textContent = s?.title || "Chat";
  setStatus("idle", p ? `${p.model}` : "");
  renderModelSwitcher();
  await loadHistory(id);
  openSse(id);
  closeDrawer();
  $("#input").focus();
}

async function loadHistory(sessionId) {
  const container = $("#messages");
  container.innerHTML = "";
  try {
    const { messages } = await api(`/messages?session_id=${sessionId}`);
    for (const m of messages || []) renderMessage(m);
    scrollToBottom();
  } catch (e) {
    container.innerHTML = `<div class="welcome"><p>Failed to load history: ${escapeHtml(e.message)}</p></div>`;
  }
}

async function newChat() {
  // Populate profile select.
  const sel = $("#profile-select");
  sel.innerHTML = "";
  if (!state.profiles.length) {
    try {
      const { profiles } = await api("/profiles");
      state.profiles = profiles || [];
    } catch {}
  }
  if (!state.profiles.length) {
    toast("Create a profile first (via the API or CLI)");
    return;
  }
  for (const p of state.profiles) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = `${p.name} · ${p.model}`;
    sel.appendChild(opt);
  }
  $("#new-chat-title").value = "";
  $("#new-chat-dialog").showModal();
}

async function createChat(profileId, title) {
  try {
    const { session } = await api("/sessions", {
      method: "POST",
      body: { profile_id: profileId, title: title || undefined },
    });
    state.sessions.unshift(session);
    renderSessions();
    selectSession(session.id);
  } catch (e) {
    toast("Failed to create session: " + e.message);
  }
}

// ============================================================
// 6. Chat — rendering, sending, live SSE
// ============================================================

function renderMessage(m) {
  if (state.renderedSeqs.has(m.sequence)) return null;
  state.renderedSeqs.add(m.sequence);
  state.lastSeq = Math.max(state.lastSeq, m.sequence);

  const container = $("#messages");
  const welcome = container.querySelector(".welcome");
  if (welcome) container.innerHTML = "";

  // Tool call (assistant row with tool_name + [tool_call:...] content).
  if (m.role === "assistant" && m.tool_name) {
    state.lastAssistantBubble = null;
    if (!state.settings.showTools) return container;
    const card = renderToolCard(m);
    container.appendChild(card);
    return card;
  }

  // Tool result.
  if (m.role === "tool") {
    state.lastAssistantBubble = null;
    if (!state.settings.showTools) return container;
    const card = pairToolResult(m) || renderToolResultCard(m);
    return card;
  }

  // System.
  if (m.role === "system") {
    const wrap = document.createElement("div");
    wrap.className = "msg system";
    wrap.innerHTML = `<div class="bubble">${escapeHtml(m.content || "")}</div>`;
    container.appendChild(wrap);
    state.lastAssistantBubble = null;
    return wrap;
  }

  // User.
  if (m.role === "user") {
    const wrap = document.createElement("div");
    wrap.className = "msg user";
    wrap.innerHTML = `<div class="bubble">${escapeHtml(m.content || "")}</div>`;
    container.appendChild(wrap);
    state.lastAssistantBubble = null;
    return wrap;
  }

  // Assistant text — merge with the previous assistant text bubble
  // if it's the most recent message (pi emits text in chunks; each
  // chunk is its own row but one logical message).
  if (m.role === "assistant") {
    const content = m.content || "";
    if (state.lastAssistantBubble && container.lastElementChild === state.lastAssistantBubble.wrap) {
      appendAssistantText(state.lastAssistantBubble, content);
    } else {
      const built = buildAssistantBubble(content);
      container.appendChild(built.wrap);
      state.lastAssistantBubble = built;
    }
    return state.lastAssistantBubble.wrap;
  }

  return null;
}

function buildAssistantBubble(content) {
  const wrap = document.createElement("div");
  wrap.className = "msg assistant";
  const bubble = document.createElement("div");
  bubble.className = "bubble md";
  bubble.innerHTML = renderMarkdown(content);
  wrap.appendChild(bubble);
  // per-bubble speak button (appears on hover/focus; always
  // tappable on touch via the composer speak btn for last reply)
  const actions = document.createElement("div");
  actions.className = "msg-meta";
  actions.innerHTML = `<button class="btn btn-ghost btn-sm speak-this" title="Speak" aria-label="Speak">🔊</button>`;
  actions.querySelector(".speak-this").addEventListener("click", (e) => {
    e.stopPropagation();
    speakText(content);
  });
  wrap.appendChild(actions);
  return { wrap, bubble, text: content };
}

function appendAssistantText(handle, content) {
  handle.text += content;
  handle.bubble.innerHTML = renderMarkdown(handle.text);
  handle.bubble.classList.add("caret");
}

// ---- Tool cards ----

function renderToolCard(m) {
  const card = document.createElement("details");
  card.className = "tool-card";
  const input = m.tool_input;
  const detail = toolInputSummary(m.tool_name, input);
  card.innerHTML = `
    <summary>
      <svg class="tc-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M14 7l3 3M5 19h4l11-11a2.83 2.83 0 0 0-4-4L5 15v4Z" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span class="tc-name">Tool</span>
      <span class="tc-detail">${escapeHtml(m.tool_name)} ${escapeHtml(detail)}</span>
      <span class="tc-dur" data-dur></span>
      <svg class="tc-chev" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path d="M9 6l6 6-6 6" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
    </summary>
    <div class="tc-body">
      <div class="tc-section"><div class="tc-label">Input</div><pre>${escapeHtml(JSON.stringify(input, null, 2))}</pre></div>
      <div class="tc-section tc-result" data-result></div>
    </div>`;
  if (m.tool_call_id) state.toolCards.set(m.tool_call_id, card);
  $("#messages").appendChild(card);
  return card;
}

function renderToolResultCard(m) {
  const card = document.createElement("details");
  card.className = "tool-card";
  const out = m.tool_output;
  const ok = out && out.success !== false;
  const dur = m.duration_ms != null ? `${Math.round(m.duration_ms)}ms` : "";
  card.innerHTML = `
    <summary>
      <svg class="tc-icon" viewBox="0 0 24 24" style="color:${ok ? "var(--ok)" : "var(--danger)"}" aria-hidden="true"><path d="M9 12l2 2 4-4M12 21a9 9 0 1 1 0-18 9 9 0 0 1 0 18Z" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <span class="tc-name">Result</span>
      <span class="tc-detail">${escapeHtml(m.tool_name)}${ok ? "" : " (failed)"}</span>
      <span class="tc-dur">${dur}</span>
      <svg class="tc-chev" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path d="M9 6l6 6-6 6" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
    </summary>
    <div class="tc-body">${renderToolOutput(m.tool_name, out)}</div>`;
  $("#messages").appendChild(card);
  return card;
}

// Pair a tool result into its call card (by tool_call_id) so the
// UI shows one collapsible "Tool ... -> Result" instead of two.
function pairToolResult(m) {
  if (!m.tool_call_id) return null;
  const card = state.toolCards.get(m.tool_call_id);
  if (!card) return null;
  const out = m.tool_output;
  const ok = out && out.success !== false;
  const dur = m.duration_ms != null ? `${Math.round(m.duration_ms)}ms` : "";
  const slot = card.querySelector("[data-dur]");
  if (slot) slot.textContent = dur;
  const nameEl = card.querySelector(".tc-name");
  if (nameEl) nameEl.textContent = ok ? "Result" : "Failed";
  const res = card.querySelector("[data-result]");
  if (res) res.innerHTML = `<div class="tc-label">Result</div>${renderToolOutput(m.tool_name, out)}`;
  return card;
}

function toolInputSummary(name, input) {
  if (!input) return "";
  if (name === "bash") return input.command ? `: ${String(input.command).slice(0, 60)}` : "";
  if (name === "read") return input.path ? `: ${input.path}` : "";
  if (name === "write" || name === "edit") return input.path ? `: ${input.path}` : "";
  return "";
}

function renderToolOutput(name, out) {
  if (!out) return "<pre>(no output)</pre>";
  if (name === "bash") {
    const stdout = out.stdout != null ? String(out.stdout) : (out.streamed ? "(streamed; not captured)" : "");
    const stderr = out.stderr != null ? String(out.stderr) : "";
    const ec = out.exit_code != null ? `exit ${out.exit_code}` : "";
    return `<div class="tc-section"><div class="tc-label">stdout</div><pre>${escapeHtml(stdout)}</pre></div>
      ${stderr ? `<div class="tc-section"><div class="tc-label">stderr</div><pre>${escapeHtml(stderr)}</pre></div>` : ""}
      ${ec ? `<div class="tc-section"><div class="tc-label">${out.timed_out ? "timed out" : "exit"}</div><pre>${escapeHtml(String(out.exit_code))}</pre></div>` : ""}`;
  }
  // read/write/edit -> {output, error, success}
  const body = out.output != null ? out.output : (out.error || JSON.stringify(out, null, 2));
  return `<pre>${escapeHtml(typeof body === "string" ? body : JSON.stringify(body, null, 2))}</pre>`;
}

// ---- Send + SSE ----

async function sendMessage(text) {
  if (!state.currentSessionId || !text.trim()) return;
  const input = $("#input");
  input.value = "";
  autosize(input);
  setSendDisabled(true);
  setStatus("thinking", "thinking…");
  try {
    const { message } = await api("/messages", {
      method: "POST",
      body: { session_id: state.currentSessionId, content: text },
    });
    // The user row is also published to the bus; dedupe by seq.
    renderMessage(message);
    scrollToBottom();
    // Ensure the SSE stream is open (it may have closed).
    if (!state.sseController) openSse(state.currentSessionId);
  } catch (e) {
    setStatus("error", e.message);
    toast("Send failed: " + e.message);
    input.value = text; // restore unsent text
    autosize(input);
  }
  setSendDisabled(false);
}

// Fetch-based SSE reader (EventSource can't set auth headers).
// Reconnects with since=lastSeq on close while the session is
// still active. Parses text/event-stream manually.
function openSse(sessionId) {
  closeSse();
  if (sessionId !== state.currentSessionId) return;
  const controller = new AbortController();
  state.sseController = controller;
  (async () => {
    let backoff = 500;
    while (controller === state.sseController && sessionId === state.currentSessionId) {
      try {
        const url = `/sessions/${sessionId}/events?since=${state.lastSeq}`;
        const resp = await fetch(API_BASE + url, {
          headers: state.apiKey ? { "X-API-Key": state.apiKey } : {},
          signal: controller.signal,
        });
        if (!resp.ok || !resp.body) throw new Error(`SSE ${resp.status}`);
        backoff = 500;
        const reader = resp.body.getReader();
        const dec = new TextDecoder();
        let buf = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let idx;
          while ((idx = buf.indexOf("\n\n")) >= 0) {
            const block = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            handleSseBlock(block);
          }
        }
        // Stream ended (server closed). Reconnect after backoff if
        // still on this session and no fatal close requested.
        if (controller !== state.sseController) return;
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 8000);
      } catch (e) {
        if (e.name === "AbortError") return;
        if (controller !== state.sseController) return;
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 8000);
      }
    }
  })();
}

function closeSse() {
  if (state.sseController) {
    state.sseController.abort();
    state.sseController = null;
  }
}

function handleSseBlock(block) {
  let eventName = "message";
  const dataLines = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) eventName = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
  }
  if (!dataLines.length) {
    // heartbeat / keepalive comment
    return;
  }
  const data = dataLines.join("\n");
  try {
    if (eventName === "message") {
      const { message } = JSON.parse(data);
      if (message) {
        renderMessage(message);
        scrollToBottom();
        if (message.role === "assistant" && !message.tool_name) {
          setStatus("thinking", "typing…");
          if (state.settings.autoSpeak) maybeAutoSpeak(message);
        } else if (message.role === "assistant" && message.tool_name) {
          setStatus("tool", `running ${message.tool_name}…`);
        } else if (message.role === "tool") {
          setStatus("tool", "tool…");
        }
      }
    } else if (eventName === "turn_ended") {
      // Finalize the streaming caret on the last assistant bubble.
      if (state.lastAssistantBubble) state.lastAssistantBubble.bubble.classList.remove("caret");
      setStatus("done", "done");
    } else if (eventName === "lagged") {
      // We fell behind; the server told us how many we missed.
      // The next message events will fill the gap (server
      // re-queries); just reload history to be safe.
      const missed = JSON.parse(data);
      console.warn("SSE lagged, missed", missed);
    }
  } catch (e) {
    console.error("SSE parse error", e, data);
  }
}

let autoSpeakPending = null;
function maybeAutoSpeak(message) {
  // Coalesce: speak the last bubble once the turn ends, not every
  // chunk. We debounce — the last chunk before turn_ended wins.
  if (autoSpeakPending) clearTimeout(autoSpeakPending);
  autoSpeakPending = setTimeout(() => {
    if (state.lastAssistantBubble?.text) speakText(state.lastAssistantBubble.text);
    autoSpeakPending = null;
  }, 350);
}

// ============================================================
// 5b. Profiles (create)
// ============================================================

async function newProfile() {
  // Reset form.
  $("#profile-name").value = "";
  $("#profile-model").value = "";
  $("#profile-base-url").value = "";
  $("#profile-api-key").value = "";
  $("#profile-working-dir").value = "";
  $("#profile-system-prompt").value = "";
  $("#profile-error").hidden = true;
  $("#new-profile-dialog").showModal();
  $("#profile-name").focus();
}

async function createProfile() {
  const err = $("#profile-error");
  err.hidden = true;
  const name = $("#profile-name").value.trim();
  const provider = $("#profile-provider").value;
  const model = $("#profile-model").value.trim();
  if (!name || !model) {
    err.textContent = "Name and model are required";
    err.hidden = false;
    return;
  }
  const body = {
    name,
    provider,
    model,
    working_dir: $("#profile-working-dir").value.trim() || `/tmp/${name}`,
  };
  const baseUrl = $("#profile-base-url").value.trim();
  if (baseUrl) body.base_url = baseUrl;
  const apiKey = $("#profile-api-key").value.trim();
  if (apiKey) body.api_key = apiKey;
  const sp = $("#profile-system-prompt").value.trim();
  if (sp) body.system_prompt = sp;
  try {
    const { profile } = await api("/profiles", { method: "POST", body });
    state.profiles.push(profile);
    toast(`Profile "${name}" created`);
    // Refresh the model switcher + new-chat dropdown.
    renderModelSwitcher();
  } catch (e) {
    err.textContent = e.message || "Failed to create profile";
    err.hidden = false;
  }
}

// ============================================================
// 6b. Model switcher (change a session's profile mid-conversation)
// ============================================================

function renderModelSwitcher() {
  const sel = $("#model-switcher");
  if (!state.currentSessionId || !state.profiles.length) {
    sel.hidden = true;
    return;
  }
  const s = state.sessions.find((x) => x.id === state.currentSessionId);
  if (!s) { sel.hidden = true; return; }
  sel.hidden = false;
  sel.innerHTML = "";
  for (const p of state.profiles) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = `${p.name} · ${p.model}`;
    if (p.id === s.profile_id) opt.selected = true;
    sel.appendChild(opt);
  }
}

async function switchModel(profileId) {
  if (!state.currentSessionId || !profileId) return;
  const s = state.sessions.find((x) => x.id === state.currentSessionId);
  if (s && s.profile_id === profileId) return; // no-op
  try {
    const { session, profile } = await api(`/sessions/${state.currentSessionId}`, {
      method: "PATCH",
      body: { profile_id: profileId },
    });
    // Update the local session record.
    if (s) s.profile_id = session.profile_id;
    // Close the SSE stream — the server tore down the old agent;
    // the next message re-opens it with the new model.
    closeSse();
    // Update the header.
    setStatus("idle", profile ? profile.model : "");
    renderSessions();
    renderModelSwitcher();
    toast(`Switched to ${profile ? profile.name + " · " + profile.model : "new model"}`);
  } catch (e) {
    toast("Switch failed: " + e.message);
    renderModelSwitcher(); // revert the dropdown
  }
}

// ============================================================
// 7. Voice — STT (Parakeet) + TTS (Kokoro)
// ============================================================

async function loadVoiceAvailability() {
  try {
    const v = await api("/v1/audio/voices");
    state.voice = v;
  } catch {
    state.voice = { stt: false, tts: false, defaultVoice: "af_heart", voices: [] };
  }
  renderVoiceUi();
}

function renderVoiceUi() {
  const mic = $("#mic-btn");
  const speak = $("#speak-btn");
  mic.hidden = !state.voice.stt;
  speak.hidden = !state.voice.tts;

  // Settings dialog voice bits.
  const status = $("#voice-status");
  const sel = $("#voice-select");
  status.innerHTML = `
    <span class="vs-pill ${state.voice.stt ? "on" : "off"}">STT</span>
    <span class="vs-pill ${state.voice.tts ? "on" : "off"}">TTS</span>`;
  sel.innerHTML = "";
  const voices = state.voice.voices?.length ? state.voice.voices : [state.voice.defaultVoice];
  for (const v of voices) {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    sel.appendChild(opt);
  }
  sel.value = state.settings.voice || state.voice.defaultVoice || "af_heart";
  sel.disabled = !state.voice.tts;
  $("#auto-speak").checked = state.settings.autoSpeak;
  $("#dictate-tap").checked = state.settings.dictateTap;
  $("#show-tools").checked = state.settings.showTools;
}

// ---- STT ----

let mediaRecorder = null;
let recChunks = [];
let recStream = null;

async function startRecording() {
  if (!state.voice.stt) return;
  try {
    recStream = await navigator.mediaDevices.getUserMedia({ audio: true });
  } catch (e) {
    toast("Microphone unavailable: " + e.message);
    return;
  }
  recChunks = [];
  // Prefer opus in webm (small, Parakeet's ffmpeg decodes it);
  // fall back to whatever the browser offers.
  const mime = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4", "audio/ogg"]
    .find((m) => MediaRecorder.isTypeSupported(m));
  mediaRecorder = new MediaRecorder(recStream, mime ? { mimeType: mime } : undefined);
  mediaRecorder.ondataavailable = (e) => { if (e.data.size) recChunks.push(e.data); };
  mediaRecorder.onstop = () => {
    recStream.getTracks().forEach((t) => t.stop());
    const blob = new Blob(recChunks, { type: mime || "audio/webm" });
    transcribe(blob);
  };
  mediaRecorder.start();
  $("#mic-btn").classList.add("recording");
}

function stopRecording() {
  if (mediaRecorder && mediaRecorder.state !== "inactive") mediaRecorder.stop();
  $("#mic-btn").classList.remove("recording");
}

async function transcribe(blob) {
  toast("Transcribing…");
  try {
    const form = new FormData();
    form.append("file", blob, "dictation.webm");
    form.append("model", "parakeet");
    form.append("response_format", "json");
    const { text } = await api("/v1/audio/transcriptions", { method: "POST", body: form });
    if (text) {
      const input = $("#input");
      insertAtCursor(input, text + " ");
      autosize(input);
      input.focus();
    }
    toast(text ? "Inserted" : "No speech detected");
  } catch (e) {
    toast("Transcription failed: " + e.message);
  }
}

function insertAtCursor(field, text) {
  const start = field.selectionStart ?? field.value.length;
  const end = field.selectionEnd ?? field.value.length;
  field.value = field.value.slice(0, start) + text + field.value.slice(end);
  const pos = start + text.length;
  field.selectionStart = field.selectionEnd = pos;
}

// ---- TTS ----

async function speakText(text) {
  if (!state.voice.tts || !text.trim()) return;
  // Stop anything currently playing.
  if (state.ttsAudio) { state.ttsAudio.pause(); state.ttsAudio = null; }
  try {
    const resp = await api("/v1/audio/speech", {
      method: "POST",
      body: {
        model: "kokoro",
        input: text.slice(0, 4000), // Kokoro chunks internally; cap for safety
        voice: state.settings.voice || state.voice.defaultVoice || "af_heart",
        response_format: "ogg",
        speed: 1.0,
      },
      raw: true,
    });
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const audio = new Audio(url);
    audio.onended = () => URL.revokeObjectURL(url);
    state.ttsAudio = audio;
    await audio.play();
  } catch (e) {
    toast("TTS failed: " + e.message);
  }
}

// ============================================================
// 8. UI chrome
// ============================================================

function setStatus(kind, label) {
  state.status = kind;
  const el = $("#chat-status");
  el.className = "chat-status" + (kind === "idle" ? "" : ` ${kind}`);
  el.innerHTML = `<span class="dot"></span>${label ? `<span>${escapeHtml(label)}</span>` : ""}`;
}

function scrollToBottom() {
  const m = $("#messages");
  // Only auto-scroll if the user is near the bottom (so we don't
  // yank them up while they're reading history).
  const nearBottom = m.scrollHeight - m.scrollTop - m.clientHeight < 120;
  if (nearBottom) m.scrollTop = m.scrollHeight;
}

function setSendDisabled(disabled) { $("#send-btn").disabled = disabled; }

let toastTimer = null;
function toast(msg) {
  let el = document.querySelector(".toast");
  if (el) el.remove();
  el = document.createElement("div");
  el.className = "toast";
  el.textContent = msg;
  document.body.appendChild(el);
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.remove(), 2600);
}

function renderWelcome() {
  $("#model-switcher").hidden = true;
  $("#messages").innerHTML = `
    <div class="welcome">
      <img src="icon.svg" alt="" width="64" height="64" />
      <h2>Forge</h2>
      <p>Durable AI coding agents. Pick a session or start a new chat.</p>
    </div>`;
  $("#chat-title").textContent = "Forge";
  setStatus("idle", "");
}

function openDrawer() {
  $("#sessions-pane").classList.add("is-open");
  $("#scrim").hidden = false;
}
function closeDrawer() {
  $("#sessions-pane").classList.remove("is-open");
  $("#scrim").hidden = true;
}

function autosize(field) {
  field.style.height = "auto";
  field.style.height = Math.min(field.scrollHeight, 160) + "px";
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================================
// 9. Boot — wire up DOM events + start
// ============================================================

function wireEvents() {
  // Composer.
  const input = $("#input");
  input.addEventListener("input", () => autosize(input));
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input.value);
    }
  });
  $("#composer").addEventListener("submit", (e) => {
    e.preventDefault();
    sendMessage(input.value);
  });

  // New chat.
  $("#new-chat-btn").addEventListener("click", newChat);

  // New profile.
  $("#new-profile-btn").addEventListener("click", newProfile);
  $("#new-profile-dialog").addEventListener("close", (e) => {
    if (e.target.returnValue === "create") createProfile();
  });
  $("#new-profile-dialog").querySelector("[data-close]").addEventListener("click", (e) => {
    e.target.closest("dialog").close("cancel");
  });

  // Model switcher (chat header dropdown).
  $("#model-switcher").addEventListener("change", (e) => {
    switchModel(e.target.value);
  });

  $("#new-chat-dialog").addEventListener("close", (e) => {
    if (e.target.returnValue === "create") {
      const pid = $("#profile-select").value;
      const title = $("#new-chat-title").value.trim();
      if (pid) createChat(pid, title);
    }
  });
  $("#new-chat-dialog").querySelector("[data-close]").addEventListener("click", (e) => {
    e.target.closest("dialog").close("cancel");
  });

  // Settings.
  $("#settings-btn").addEventListener("click", () => {
    loadVoiceAvailability();
    $("#settings-dialog").showModal();
  });
  $("#settings-dialog").addEventListener("close", (e) => {
    if (e.target.returnValue === "save") {
      state.settings.voice = $("#voice-select").value;
      state.settings.autoSpeak = $("#auto-speak").checked;
      state.settings.dictateTap = $("#dictate-tap").checked;
      state.settings.showTools = $("#show-tools").checked;
      localStorage.setItem(LS.voice, state.settings.voice);
      localStorage.setItem(LS.autoSpeak, state.settings.autoSpeak ? "1" : "0");
      localStorage.setItem(LS.dictateTap, state.settings.dictateTap ? "1" : "0");
      localStorage.setItem(LS.showTools, state.settings.showTools ? "1" : "0");
      // Re-render current view so tool-card visibility applies.
      if (state.currentSessionId) selectSession(state.currentSessionId);
    }
  });

  // Logout.
  $("#logout-btn").addEventListener("click", doLogout);

  // Mobile drawer.
  $("#back-btn").addEventListener("click", openDrawer);
  $("#scrim").addEventListener("click", closeDrawer);

  // Mic — tap-to-toggle (dictate-tap) or hold-to-record.
  const mic = $("#mic-btn");
  let pressTimer = null;
  mic.addEventListener("pointerdown", (e) => {
    if (!state.settings.dictateTap) {
      pressTimer = setTimeout(() => startRecording(), 120);
    }
  });
  mic.addEventListener("pointerup", () => {
    if (!state.settings.dictateTap) {
      clearTimeout(pressTimer);
      if (mediaRecorder && mediaRecorder.state === "recording") stopRecording();
    }
  });
  mic.addEventListener("pointerleave", () => {
    if (!state.settings.dictateTap) {
      clearTimeout(pressTimer);
      if (mediaRecorder && mediaRecorder.state === "recording") stopRecording();
    }
  });
  mic.addEventListener("click", () => {
    if (state.settings.dictateTap) {
      if (mediaRecorder && mediaRecorder.state === "recording") stopRecording();
      else startRecording();
    }
  });

  // Speak last reply (composer button).
  $("#speak-btn").addEventListener("click", () => {
    if (state.ttsAudio) { state.ttsAudio.pause(); state.ttsAudio = null; toast("Stopped"); return; }
    if (state.lastAssistantBubble?.text) speakText(state.lastAssistantBubble.text);
    else toast("No reply to speak yet");
  });

  // Login form.
  const loginForm = $("#login-form");
  let registering = false;
  $("#login-submit").textContent = "Log in";
  $("#register-toggle").addEventListener("click", () => {
    registering = !registering;
    $("#login-submit").textContent = registering ? "Register" : "Log in";
    $("#register-toggle").textContent = registering ? "Back to login" : "Create account";
    $("#login-error").hidden = true;
  });
  loginForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const email = $("#login-email").value.trim();
    const pw = $("#login-password").value;
    if (registering) doRegister(email, pw);
    else doLogin(email, pw);
  });

  // Copy code blocks (event delegation — content is re-rendered).
  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".codeblk-copy");
    if (!btn) return;
    const pre = document.getElementById(btn.dataset.copy);
    if (pre) {
      navigator.clipboard.writeText(pre.innerText).then(() => {
        btn.textContent = "copied";
        setTimeout(() => (btn.textContent = "copy"), 1200);
      });
    }
  });

  // Visibility: pause reconnect churn when hidden.
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && state.currentSessionId && !state.sseController) {
      openSse(state.currentSessionId);
    }
  });
}

function boot() {
  wireEvents();
  renderWelcome();
  loadVoiceAvailability();
  // Register the service worker for installability/offline shell.
  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.register("sw.js").catch(() => {});
  }
  bootstrapAuth();
}

boot();
