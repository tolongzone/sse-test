import { DurableObject } from "cloudflare:workers";

const GRACE_MS = 15000;
const HEARTBEAT_MS = 5000;

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), ms)),
  ]);
}

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sessions = []; // {writer, token, role}
    this.hostToken = null;
    this.hostGraceStartedAt = null;
  }

  async broadcast(obj, targets = this.sessions) {
    const encoder = new TextEncoder();
    const payload = encoder.encode(`data: ${JSON.stringify(obj)}\n\n`);
    await Promise.allSettled(targets.map((s) => withTimeout(s.writer.write(payload), 3000)));
  }

  async ensureHeartbeat() {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null) {
      await this.ctx.storage.setAlarm(Date.now() + HEARTBEAT_MS);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).pop();

    if (action === "connect") {
      const wantRole = url.searchParams.get("role") === "host" ? "host" : "client";
      const reconnectToken = url.searchParams.get("token");

      let token, role;
      let isReconnect = false;

      if (wantRole === "host") {
        if (this.hostToken && reconnectToken === this.hostToken) {
          token = this.hostToken;
          role = "host";
          isReconnect = true;
          this.hostGraceStartedAt = null;
        } else if (this.hostToken) {
          return new Response("host already connected", { status: 409 });
        } else {
          token = crypto.randomUUID();
          role = "host";
          this.hostToken = token;
        }
      } else {
        token = crypto.randomUUID();
        role = "client";
      }

      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sessions.push({ writer, token, role });

      const encoder = new TextEncoder();
      writer.write(encoder.encode(`data: ${JSON.stringify({ type: "init", role, token })}\n\n`)).catch(() => {});

      await this.ensureHeartbeat();
      if (isReconnect) await this.broadcast({ type: "status", text: "host reconnected" });

      return new Response(readable, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
      });
    }

    if (action === "command") {
      const token = request.headers.get("X-Token");
      if (!token || token !== this.hostToken) {
        return new Response("forbidden: host only", { status: 403 });
      }
      const text = await request.text();
      await this.broadcast({ type: "msg", text });
      return new Response(`sent to ${this.sessions.length} clients`);
    }

    if (action === "test") {
      const html = `<!DOCTYPE html><html><body>
<h3>Heartbeat + grace period test</h3>
<div id="status">connecting...</div>
<div id="controls"></div>
<div id="log"></div>
<script>
const log = document.getElementById('log');
const status = document.getElementById('status');
const controls = document.getElementById('controls');
let token = null;
const params = new URLSearchParams(window.location.search);
const es = new EventSource(window.location.pathname.replace(/\\/test$/, '/connect') + '?' + params.toString());
es.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.type === 'init') {
    token = data.token;
    status.textContent = 'role: ' + data.role + (data.role === 'host' ? ' | token: ' + token : '');
    if (data.role === 'host') {
      controls.innerHTML = '<input id="txt" placeholder="message"><button id="send">send</button>';
      document.getElementById('send').onclick = () => {
        fetch(window.location.pathname.replace(/\\/test$/, '/command'), {
          method: 'POST', headers: { 'X-Token': token }, body: document.getElementById('txt').value,
        });
      };
    }
  } else {
    const p = document.createElement('div');
    p.textContent = new Date().toLocaleTimeString() + ' - [' + data.type + '] ' + data.text;
    log.prepend(p);
  }
};
es.onerror = () => { status.textContent += ' [disconnected]'; };
</script></body></html>`;
      return new Response(html, { headers: { "Content-Type": "text/html;charset=UTF-8" } });
    }

    return new Response("not found", { status: 404 });
  }

  async alarm() {
    const alive = [];
    const dead = [];
    for (const s of this.sessions) {
      try {
        await withTimeout(s.writer.write(new TextEncoder().encode(`:hb\n\n`)), 3000);
        alive.push(s);
      } catch (e) {
        dead.push(s);
      }
    }
    this.sessions = alive;

    const hostPresent = alive.some((s) => s.role === "host");

    if (this.hostToken) {
      if (!hostPresent && this.hostGraceStartedAt === null) {
        this.hostGraceStartedAt = Date.now();
        await this.broadcast({ type: "status", text: `host disconnected, ${GRACE_MS / 1000}s grace period started` });
      } else if (!hostPresent && Date.now() - this.hostGraceStartedAt >= GRACE_MS) {
        this.hostToken = null;
        this.hostGraceStartedAt = null;
        await this.broadcast({ type: "status", text: "grace period expired, host slot released" });
      }
    }

    if (this.sessions.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + HEARTBEAT_MS);
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([^/]+)\//);
    if (!match) return new Response("usage: /room/:roomId/connect|command|test", { status: 400 });
    const id = env.ROOM.idFromName(match[1]);
    return env.ROOM.get(id).fetch(request);
  },
};
