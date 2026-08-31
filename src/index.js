import { DurableObject } from "cloudflare:workers";

const GRACE_MS = 15000;
const HEARTBEAT_MS = 5000;

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), ms)),
  ]);
}

function parseCookies(request) {
  const header = request.headers.get("Cookie") || "";
  const out = {};
  header.split(";").forEach((pair) => {
    const idx = pair.indexOf("=");
    if (idx === -1) return;
    out[pair.slice(0, idx).trim()] = pair.slice(idx + 1).trim();
  });
  return out;
}

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sessions = [];
    this.hostToken = null;
    this.lastHostToken = null;
    this.hostGraceStartedAt = null;
    this.clientTokens = new Set();
  }

  async broadcast(obj, targets = this.sessions) {
    const encoder = new TextEncoder();
    const payload = encoder.encode(`data: ${JSON.stringify(obj)}\n\n`);
    await Promise.allSettled(targets.map((s) => withTimeout(s.writer.write(payload), 3000)));
  }

  async ensureHeartbeat() {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null) await this.ctx.storage.setAlarm(Date.now() + HEARTBEAT_MS);
  }

  async startHostGraceIfNeeded() {
    if (this.hostToken && this.hostGraceStartedAt === null) {
      this.lastHostToken = this.hostToken;
      this.hostGraceStartedAt = Date.now();
      await this.broadcast({ type: "status", text: `host disconnected, ${GRACE_MS / 1000}s grace period started` });
      await this.ctx.storage.setAlarm(Date.now() + GRACE_MS);
    }
  }

  roleOf(token) {
    if (token && token === this.hostToken) return "host";
    if (token && this.hostGraceStartedAt !== null && token === this.lastHostToken) return "host-pending-reconnect";
    if (token && this.clientTokens.has(token)) return "client";
    return null;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).pop() || "root";
    const cookies = parseCookies(request);
    const cookieToken = cookies["token"];

    if (action === "join") {
      const wantRole = url.searchParams.get("role") === "host" ? "host" : "client";
      let token;
      if (wantRole === "host") {
        if (this.hostToken) return new Response(JSON.stringify({ error: "host taken" }), { status: 409 });
        token = crypto.randomUUID();
        this.hostToken = token;
      } else {
        token = crypto.randomUUID();
        this.clientTokens.add(token);
      }
      return new Response(JSON.stringify({ token, role: wantRole }), { headers: { "Content-Type": "application/json" } });
    }

    if (action === "connect") {
      const kind = this.roleOf(cookieToken);
      if (!kind) return new Response("not joined", { status: 401 });

      let role = "client";
      let isReconnect = false;
      if (kind === "host") {
        role = "host";
      } else if (kind === "host-pending-reconnect") {
        role = "host";
        this.hostToken = cookieToken;
        this.hostGraceStartedAt = null;
        isReconnect = true;
      }

      const connId = crypto.randomUUID();
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sessions.push({ writer, connId, token: cookieToken, role });

      const encoder = new TextEncoder();
      writer.write(encoder.encode(`data: ${JSON.stringify({ type: "init", role, connId })}\n\n`)).catch(() => {});

      await this.ensureHeartbeat();
      if (isReconnect) await this.broadcast({ type: "status", text: "host reconnected" });

      return new Response(readable, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
      });
    }

    if (action === "command") {
      if (!cookieToken || cookieToken !== this.hostToken) return new Response("forbidden", { status: 403 });
      const text = await request.text();
      await this.broadcast({ type: "msg", text });
      return new Response(`sent to ${this.sessions.length} clients`);
    }

    if (action === "leave") {
      const connId = url.searchParams.get("conn");
      const target = this.sessions.find((s) => s.connId === connId);
      this.sessions = this.sessions.filter((s) => s.connId !== connId);
      if (target && target.role === "host" && target.token === this.hostToken) {
        const stillHasHostConn = this.sessions.some((s) => s.role === "host" && s.token === this.hostToken);
        if (!stillHasHostConn) await this.startHostGraceIfNeeded();
      }
      return new Response("ok");
    }

    if (action === "page") {
      const kind = this.roleOf(cookieToken);
      const html = `<!DOCTYPE html><html><body>
<h3>${kind === "host" || kind === "host-pending-reconnect" ? "Host" : "Client"} page (URL has no suffix)</h3>
<div id="status">connecting...</div>
<div id="controls"></div>
<div id="log"></div>
<script>
const log = document.getElementById('log');
const status = document.getElementById('status');
const controls = document.getElementById('controls');
let myConnId = null;
const es = new EventSource('/connect');
es.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.type === 'init') {
    myConnId = data.connId;
    status.textContent = 'role: ' + data.role + ' | connId: ' + myConnId.slice(0, 8);
    if (data.role === 'host') {
      controls.innerHTML = '<input id="txt" placeholder="message"><button id="send">send</button>';
      document.getElementById('send').onclick = () => {
        fetch('/command', { method: 'POST', body: document.getElementById('txt').value });
      };
    }
  } else {
    const p = document.createElement('div');
    p.textContent = new Date().toLocaleTimeString() + ' - [' + data.type + '] ' + data.text;
    log.prepend(p);
  }
};
es.onerror = () => { status.textContent = '[disconnected]'; };
window.addEventListener('pagehide', () => {
  if (myConnId) navigator.sendBeacon('/leave?conn=' + myConnId);
});
</script></body></html>`;
      return new Response(html, { headers: { "Content-Type": "text/html;charset=UTF-8" } });
    }

    return new Response("not found", { status: 404 });
  }

  async alarm() {
    const alive = [];
    for (const s of this.sessions) {
      try {
        await withTimeout(s.writer.write(new TextEncoder().encode(`:hb\n\n`)), 3000);
        alive.push(s);
      } catch (e) {}
    }
    this.sessions = alive;
    const hostPresent = alive.some((s) => s.role === "host" && s.token === this.hostToken);

    if (this.hostToken && !hostPresent) {
      if (this.hostGraceStartedAt === null) {
        await this.startHostGraceIfNeeded();
      } else if (Date.now() - this.hostGraceStartedAt >= GRACE_MS) {
        this.hostToken = null;
        this.lastHostToken = null;
        this.hostGraceStartedAt = null;
        await this.broadcast({ type: "status", text: "grace period expired, host slot released" });
      }
    }

    if (this.sessions.length > 0) {
      const pending = await this.ctx.storage.getAlarm();
      if (pending === null || pending > Date.now() + HEARTBEAT_MS) {
        await this.ctx.storage.setAlarm(Date.now() + HEARTBEAT_MS);
      }
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cookies = parseCookies(request);
    const roomCookie = cookies["room"];

    // 根路径且没有房间 cookie -> 自动建房间，设为 host
    if (url.pathname === "/" && !roomCookie) {
      const roomId = crypto.randomUUID();
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);
      const joinResp = await stub.fetch(new Request("https://internal/join?role=host"));
      const { token } = await joinResp.json();

      const headers = new Headers();
      headers.set("Location", "/");
      headers.append("Set-Cookie", `room=${roomId}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=3600`);
      headers.append("Set-Cookie", `token=${token}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=3600`);
      return new Response(null, { status: 302, headers });
    }

    if (!roomCookie) return new Response("no room", { status: 401 });

    const id = env.ROOM.idFromName(roomCookie);
    const stub = env.ROOM.get(id);

    if (url.pathname === "/") {
      return stub.fetch(new Request("https://internal/page", { headers: request.headers }));
    }

    return stub.fetch(request); // /connect /command /leave 直接转发
  },
};
