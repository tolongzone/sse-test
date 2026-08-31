import { DurableObject } from "cloudflare:workers";

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
  // 不 await,不阻塞 return
  writer.write(encoder.encode(`data: connected, total=${this.sessions.length}\n\n`)).catch(() => {});
  return new Response(readable, {
    headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
  });
}

    if (url.pathname === "/broadcast") {
      const encoder = new TextEncoder();
      const msg = encoder.encode(`data: broadcast at ${new Date().toISOString()}\n\n`);
      const alive = [];
      for (const w of this.sessions) {
        try { await w.write(msg); alive.push(w); } catch (e) {}
      }
      this.sessions = alive;
      return new Response(`sent to ${this.sessions.length} clients`);
    }

    return new Response("not found", { status: 404 });
  }
}

export default {
  async fetch(request, env) {
    const id = env.ROOM.idFromName("test-room");
    const stub = env.ROOM.get(id);
    return stub.fetch(request);
  },
};
