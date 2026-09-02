import { DurableObject } from "cloudflare:workers";
import qrcode from "qrcode-generator";

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
    const action = url.pathname.split("/").filter(Boolean).pop();

    let token;
    if (request.method === "POST") {
      const body = await request.clone().json().catch(() => ({}));
      token = body.token;
    } else {
      token = url.searchParams.get("token");
    }

    if (action === "createhost") {
      if (this.hostToken) return new Response(JSON.stringify({ error: "already has host" }), { status: 409 });
      const newToken = crypto.randomUUID();
      this.hostToken = newToken;
      return new Response(JSON.stringify({ token: newToken }), { headers: { "Content-Type": "application/json" } });
    }

    if (action === "join") {
      const newToken = crypto.randomUUID();
      this.clientTokens.add(newToken);
      return new Response(JSON.stringify({ token: newToken, role: "client" }), { headers: { "Content-Type": "application/json" } });
    }

    if (action === "connect") {
      const kind = this.roleOf(token);
      if (!kind) return new Response("unauthorized", { status: 401 });

      let role = "client";
      let isReconnect = false;
      if (kind === "host") {
        role = "host";
      } else if (kind === "host-pending-reconnect") {
        role = "host";
        this.hostToken = token;
        this.hostGraceStartedAt = null;
        isReconnect = true;
      }

      const connId = crypto.randomUUID();
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sessions.push({ writer, connId, token, role });

      const encoder = new TextEncoder();
      writer.write(encoder.encode(`data: ${JSON.stringify({ type: "init", role, connId })}\n\n`)).catch(() => {});

      await this.ensureHeartbeat();
      if (isReconnect) await this.broadcast({ type: "status", text: "host reconnected" });

      return new Response(readable, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
      });
    }

    if (action === "command") {
      // 房间里任何合法成员（host 或 client）都能发指令；是否真正生效
      // 由 host 页面自己的业务逻辑把关，不在这里卡权限
      if (!this.roleOf(token)) return new Response("forbidden", { status: 403 });
      const body = await request.clone().json().catch(() => ({}));
      const { token: _drop, ...payload } = body;
      await this.broadcast({ type: "msg", ...payload });
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

// 把 room_id/token 直接注入静态页面里，页面自己的 JS 通过
// window.__ROOM_ID__ / window.__ROOM_TOKEN__ 读取，不依赖 Cookie
async function serveWithInjectedVars(env, request, assetPath, roomId, token) {
  const assetUrl = new URL(request.url);
  assetUrl.pathname = assetPath;
  const assetResp = await env.ASSETS.fetch(new Request(assetUrl, request));
  if (!assetResp.ok) return assetResp;

  let html = await assetResp.text();
  const inject =
    "<script>" +
    "window.__ROOM_ID__=" + JSON.stringify(roomId) + ";" +
    "window.__ROOM_TOKEN__=" + JSON.stringify(token) + ";" +
    "</script>";
  html = html.replace("</head>", inject + "</head>");

  return new Response(html, {
    headers: { "Content-Type": "text/html;charset=UTF-8", "Cache-Control": "no-store" },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const match = url.pathname.match(/^\/_room\/([^/]+)\/(.+)$/);
    if (match) {
      const [, roomId, action] = match;
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);

      if (action === "qrcode") {
        const enterUrl = `${url.origin}/_room/${roomId}/enter`;
        const qr = qrcode(0, "M");
        qr.addData(enterUrl);
        qr.make();
        let svg = qr.createSvgTag({ cellSize: 4, margin: 4 });
        if (!svg.includes("xmlns=")) {
          svg = svg.replace("<svg ", '<svg xmlns="http://www.w3.org/2000/svg" ');
        }
        return new Response(svg, { headers: { "Content-Type": "image/svg+xml" } });
      }

      if (action === "enter") {
        const joinResp = await stub.fetch(new Request("https://internal/join"));
        const { token } = await joinResp.json();
        return serveWithInjectedVars(env, request, "/remote.html", roomId, token);
      }

      // connect / command / leave 直接转发给 DO，不用先经过静态资源层
      const innerUrl = new URL(request.url);
      innerUrl.pathname = "/" + action;
      return stub.fetch(new Request(innerUrl, request));
    }

    if (url.pathname === "/") {
      const roomId = crypto.randomUUID();
      const id = env.ROOM.idFromName(roomId);
      const stub = env.ROOM.get(id);
      const resp = await stub.fetch(new Request("https://internal/createhost", { method: "POST" }));
      const { token } = await resp.json();

      return serveWithInjectedVars(env, request, "/index.html", roomId, token);
    }

    // 其余路径（如果有）交给静态资源层兜底（比如以后加图标之类）
    return env.ASSETS.fetch(request);
  },
};
