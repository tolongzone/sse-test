import { DurableObject } from "cloudflare:workers";

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
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/connect") {
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sessions.push(writer);
      const encoder = new TextEncoder();
      writer.write(encoder.encode(`data: connected, total=${this.sessions.length}\n\n`)).catch(() => {});

      request.signal.addEventListener("abort", () => {
        this.sessions = this.sessions.filter((s) => s !== writer);
      });

      return new Response(readable, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
      });
    }

    if (url.pathname === "/broadcast") {
      const encoder = new TextEncoder();
      const msg = encoder.encode(`data: broadcast at ${new Date().toISOString()}\n\n`);
      const results = await Promise.allSettled(
        this.sessions.map((w) => withTimeout(w.write(msg), 3000))
      );
      this.sessions = this.sessions.filter((_, i) => results[i].status === "fulfilled");
      return new Response(`sent to ${this.sessions.length} clients`);
    }

    if (url.pathname === "/test") {
      const html = `<!DOCTYPE html><html><body>
<h3>SSE test</h3>
<div id="log"></div>
<script>
const log = document.getElementById('log');
const es = new EventSource('/connect');
es.onmessage = (e) => {
  const p = document.createElement('div');
  p.textContent = new Date().toLocaleTimeString() + ' - ' + e.data;
  log.prepend(p);
};
es.onerror = () => {
  const p = document.createElement('div');
  p.textContent = new Date().toLocaleTimeString() + ' - [disconnected]';
  p.style.color = 'red';
  log.prepend(p);
};
</script></body></html>`;
      return new Response(html, { headers: { "Content-Type": "text/html;charset=UTF-8" } });
    }

    return new Response("not found", { status: 404 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([^/]+)(\/.*)?$/);

    if (!match) {
      return new Response("usage: /room/:roomId/connect|broadcast|test", { status: 400 });
    }

    const roomId = match[1];
    const subPath = match[2] || "/";

    const id = env.ROOM.idFromName(roomId);
    const stub = env.ROOM.get(id);

    // 把去掉房间前缀的子路径转发给 DO
    const innerUrl = new URL(request.url);
    innerUrl.pathname = subPath;
    return stub.fetch(new Request(innerUrl, request));
  },
};
