import { DurableObject } from "cloudflare:workers";

const GRACE_MS = 15000;

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
  }

  async broadcast(obj) {
    const encoder = new TextEncoder();
    const payload = encoder.encode(`data: ${JSON.stringify(obj)}\n\n`);
    const results = await Promise.allSettled(
      this.sessions.map((s) => withTimeout(s.writer.write(payload), 3000))
    );
    this.sessions = this.sessions.filter((_, i) => results[i].status === "fulfilled");
  }

  async fetch(request) {
    const url = new URL(request.url);
    const action = url.pathname.split("/").filter(Boolean).pop();

    if (action === "connect") {
      const wantRole = url.searchParams.get("role") === "host" ? "host" : "client";
      const reconnectToken = url.searchParams.get("token");

      let token, role;

      if (wantRole === "host") {
        if (this.hostToken && reconnectToken === this.hostToken) {
          // 宽限期内重连
          token = this.hostToken;
          role = "host";
          await this.ctx.storage.deleteAlarm();
          await this.broadcast({ type: "status", text: "host reconnected" });
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
      const initMsg = JSON.stringify({ type: "init", role, token });
      writer.write(encoder.encode(`data: ${initMsg}\n\n`)).catch(() => {});

      request.signal.addEventListener("abort", () => {
        this.sessions = this.sessions.filter((s) => s.writer !== writer);
        if (role === "host" && this.hostToken === token) {
          this.ctx.storage.setAlarm(Date.now() + GRACE_MS);
          this.broadcast({ type: "status", text: `host disconnected, ${GRACE_MS / 1000}s grace period started` });
        }
      });

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
<h3>Grace period test</h3>
<div id="status">connecting...</div>
<div id="controls"></div>
<div id="log"></div>
<script>
const log = document.getElementById('log');
const status = document.getElementById('status');
const controls = document.getElementById('controls');
let token = null;

const params = new URLSearchParams(window.location.search);
let connectUrl = window.location.pathname.replace(/\\/test$/, '/connect') + '?' + params.toString();
const es = new EventSource(connectUrl);

es.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.type === 'init') {
    token = data.token;
    status.textContent = 'role: ' + data.role + (data.role === 'host' ? ' | token: ' + token : '');
    if (data.role === 'host') {
      controls.innerHTML = '<input id="txt" placeholder="message"><button id="send">send</button>';
      document.getElementById('send').onclick = () => {
        const text = document.getElementById('txt').value;
        fetch(window.location.pathname.replace(/\\/test$/, '/command'), {
          method: 'POST', headers: { 'X-Token': token }, body: text,
        });
      };
    }
  } else {
    const p = document.createElement('div');
    p.textContent = new Date().toLocaleTimeString() + ' - [' + data.type + '] ' + data.text;
    log.prepend(p);
  }
};
es.onerror = () => {
  status.textContent += ' [disconnected]';
};
</script></body></html>`;
      return new Response(html, { headers: { "Content-Type": "text/html;charset=UTF-8" } });
    }

    return new Response("not found", { status: 404 });
  }

  async alarm() {
    this.hostToken = null;
    await this.broadcast({ type: "status", text: "grace period expired, host slot released" });
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
