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
  // 房间里任何合法成员（host 或 client）都能发指令；
  // 是否真正生效由 host 页面自己的 handleRemoteCommand 逻辑把关，不在这里卡权限
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

function renderPage(roomId, token, role) {
  const isHost = role === "host";
  return `<!DOCTYPE html><html><body>
<h3>${isHost ? "Host" : "Client"} page — startGame round-trip test</h3>
<div id="status">connecting...</div>
${isHost
  ? `<div><button id="score-plus">东 加10分</button> <button id="push">推送状态</button></div>`
  : `<div><button id="start">开始游戏</button></div>`}
<div id="screen-view">(等待状态...)</div>
<pre id="state-view"></pre>
<script>
window.__ROOM_ID__ = ${JSON.stringify(roomId)};
window.__TOKEN__ = ${JSON.stringify(token)};
const status = document.getElementById('status');
const screenView = document.getElementById('screen-view');
const stateView = document.getElementById('state-view');
let myConnId = null;
let myRole = null;

let gameState = {
  isRunning: false,
  screen: 'idle',
  difen: 20, beilv: 1, danbu: 10,
  initialDealer: '东', currentDealer: '东', missingDir: null,
  roundsCount: 1, gamesCount: 0, records: [],
  players: { 东:{name:'',current:0,benzhuang:0,buci:0}, 南:{name:'',current:0,benzhuang:0,buci:0}, 西:{name:'',current:0,benzhuang:0,buci:0}, 北:{name:'',current:0,benzhuang:0,buci:0} }
};

function pushState() {
  const state = {
    action: 'state',
    players: {},
    isRunning: gameState.isRunning,
    screen: gameState.screen,
    qrcodeOpen: false,
    difen: gameState.difen, beilv: gameState.beilv, danbu: gameState.danbu,
    initialDealer: gameState.initialDealer, currentDealer: gameState.currentDealer, missingDir: gameState.missingDir,
    gamesCount: gameState.gamesCount, roundsCount: gameState.roundsCount, records: gameState.records
  };
  ['东','南','西','北'].forEach(dir => {
    const p = gameState.players[dir];
    const base = gameState.difen > p.current ? gameState.difen + gameState.difen - p.current : gameState.difen;
    state.players[dir] = { name: p.name, current: p.current, benzhuang: p.benzhuang, buci: p.buci,
      jiesuan: base + p.buci * gameState.danbu, isDealer: dir === gameState.currentDealer, isMissing: dir === gameState.missingDir };
  });
  sendCommand(state);
}

function sendCommand(payload) {
  fetch('/_room/' + window.__ROOM_ID__ + '/command', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(Object.assign({}, payload, { token: window.__TOKEN__ })),
  });
}

function handleRemoteCommand(data) {
  if (data.action === 'startGame') {
    if (!gameState.isRunning) {
      gameState.isRunning = true;
      gameState.screen = 'game';
      pushState();
    }
    return;
  }
}

function renderScreen(state) {
  if (state.screen === 'idle') {
    screenView.textContent = '等待房主开局...';
  } else {
    screenView.textContent = '游戏进行中';
  }
  stateView.textContent = JSON.stringify(state.players, null, 2);
}

const es = new EventSource('/_room/' + window.__ROOM_ID__ + '/connect?token=' + encodeURIComponent(window.__TOKEN__));
es.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.type === 'init') {
    myConnId = data.connId;
    myRole = data.role;
    status.textContent = 'role: ' + data.role + ' | room: ' + window.__ROOM_ID__.slice(0,8);
    ${isHost ? `pushState();` : ``}
  } else if (data.type === 'msg') {
    if (data.action === 'state') {
      renderScreen(data);
    } else if (myRole === 'host') {
      handleRemoteCommand(data);
    }
  }
};
es.onerror = () => { status.textContent = '[disconnected]'; };

${isHost ? `
document.getElementById('score-plus').onclick = () => {
  gameState.players['东'].current += 10;
  pushState();
};
document.getElementById('push').onclick = pushState;
` : `
document.getElementById('start').onclick = () => {
  sendCommand({ action: 'startGame' });
};
`}

window.addEventListener('pagehide', () => {
  if (myConnId) navigator.sendBeacon('/_room/' + window.__ROOM_ID__ + '/leave?conn=' + myConnId);
});
</script>
${isHost ? `<hr><p>扫码加入(client):</p><img src="/_room/${roomId}/qrcode" width="200" height="200">` : ""}
</body></html>`;
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
        return new Response(renderPage(roomId, token, "client"), {
          headers: { "Content-Type": "text/html;charset=UTF-8", "Cache-Control": "no-store" },
        });
      }

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

      return new Response(renderPage(roomId, token, "host"), {
        headers: { "Content-Type": "text/html;charset=UTF-8", "Cache-Control": "no-store" },
      });
    }

    return new Response("not found", { status: 404 });
  },
};
