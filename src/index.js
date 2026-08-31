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
    this.sessions = []; // {writer, connId, token, role}
    this.hostToken = null;       // 当前占位的 host token
    this.lastHostToken = null;   // 上一个 host 的 token，grace 期间用来允许其"拿回"位置
    this.hostGraceStartedAt = null;
    this.clientTokens = new Set(); // 记录哪些 token 是通过 /join 合法签发的 client
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

  // 服务端唯一真相：这个 token 现在到底是什么角色，不看客户端的 cookie 里怎么写
  roleOf(token) {
    if (token && token === this.hostToken) return "host";
    if (token && this.hostGraceStartedAt !== null && token === this.lastHostToken) return "host-pending-reconnect";
    if (token && this.clientTokens.has(token)) return "client";
    return null; // 未知/无效 token
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).pop();
    const roomId = url.pathname.match(/^\/room\/([^/]+)\//)[1];
    const cookies = parseCookies(request);
    const cookieToken = cookies[`room_${roomId}_token`];

    if (action === "join") {
      const wantRole = url.searchParams.get("role") === "host" ? "host" : "client";
      let token;

      if (wantRole === "host") {
        if (this.hostToken) return new Response("host already connected", { status: 409 });
        token = crypto.randomUUID();
        this.hostToken = token;
      } else {
        token = crypto.randomUUID();
        this.clientTokens.add(token);
      }

      const headers = new Headers();
      headers.set("Location", `/room/${roomId}/test`);
      headers.append("Set-Cookie", `room_${roomId}_token=${token}; Path=/room/${roomId}; HttpOnly; Secure; SameSite=Lax; Max-Age=3600`);
      return new Response(null, { status: 302, headers });
    }

    if (action === "connect") {
      const kind = this.roleOf(cookieToken);
      if (!kind) return new Response("not joined: visit /join first", { status: 401 });

      let role = "client";
      let isReconnect = false;

      if (kind === "host") {
        role = "host";
      } else if (kind === "host-pending-reconnect") {
        role = "host";
        this.hostToken = cookieToken; // 拿回位置
        this.hostGraceStartedAt = null;
        isReconnect = true;
      } else {
        role = "client";
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
      if (!cookieToken || cookieToken !== this.hostToken) return new Response("forbidden: host only", { status: 403 });
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

    if (action === "test") {
      const html = `<!DOCTYPE html><html><body>
<h3>Server-authoritative role test</h3>
<div id="status">connecting...</div>
<div id="controls"></div>
<div id="log"></div>
<script>
const log = document.getElementById('log');
const status = document.getElementById('status');
const controls = document.getElementById('controls');
const basePath = window.location.pathname.replace(/\\/test$/, '');
let myConnId = null;
const es = new EventSource(basePath + '/connect');

es.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.type === 'init') {
    myConnId = data.connId;
    status.textContent = 'role: ' + data.role + ' | connId: ' + myConnId.slice(0, 8);
    if (data.role === 'host') {
      controls.innerHTML = '<input id="txt" placeholder="message"><button id="send">send</button>';
      document.getElementById('send').onclick = () => {
        fetch(basePath + '/command', { method: 'POST', body: document.getElementById('txt').value });
      };
    }
  } else {
    const p = document.createElement('div');
    p.textContent = new Date().toLocaleTimeString() + ' - [' + data.type + '] ' + data.text;
    log.prepend(p);
  }
};
es.onerror = () => { status.textContent = '[not connected / disconnected — try /join first]'; };

window.addEventListener('pagehide', () => {
  if (myConnId) navigator.sendBeacon(basePath + '/leave?conn=' + myConnId);
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
    const match = url.pathname.match(/^\/room\/([^/]+)\//);
    if (!match) return new Response("usage: /room/:roomId/join|connect|command|leave|test", { status: 400 });
    const id = env.ROOM.idFromName(match[1]);
    return env.ROOM.get(id).fetch(request);
  },
};
