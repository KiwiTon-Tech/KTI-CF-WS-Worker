/**
 * KTI-CF-WS-Worker
 *
 * Cloudflare Worker + Durable Object for real-time price streaming.
 *
 * Routes:
 *   GET  /ws/prices          WebSocket upgrade → PriceHub Durable Object
 *   POST /internal/tick      Authenticated tick ingress from KTI-Price-Publisher
 *   GET  /health             Liveness probe
 *
 * The PriceHub Durable Object (single "global" instance) tracks:
 *   clientSymbols : Map<WebSocket, Set<string>>  — subscriptions per client
 *   symbolClients : Map<string,    Set<WebSocket>> — clients per symbol
 *
 * Tick wire format (matches KTI-Market-Data-Service + frontend realtime.js):
 *   { symbol, price, size?, timestamp?, asset_class? }
 *
 * Control frames from browser client:
 *   { type: "subscribe",   symbols: string[] }
 *   { type: "unsubscribe", symbols: string[] }
 */

export interface Env {
  PRICE_HUB: DurableObjectNamespace;
  INTERNAL_SECRET: string;
  ALPACA_API_KEY: string;
  ALPACA_API_SECRET: string;
  SYMBOLS_STOCKS: string;  // comma-separated, e.g. "SPY,AAPL,TSLA"
  SYMBOLS_CRYPTO: string;  // comma-separated, e.g. "BTC/USD,ETH/USD"
}

interface TickPayload {
  symbol: string;
  price: number;
  size?: number;
  timestamp?: string;
  asset_class?: string;
}

// ---- PriceHub Durable Object ------------------------------------------

export class PriceHub implements DurableObject {
  private readonly clientSymbols = new Map<WebSocket, Set<string>>();
  private readonly symbolClients = new Map<string, Set<WebSocket>>();

  // _state and _env are required by the Durable Object interface but not
  // used in the current implementation (no storage or cross-DO calls yet).
  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(_state: DurableObjectState, _env: Env) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      return this._upgradeWebSocket(request);
    }

    if (url.pathname === "/tick" && request.method === "POST") {
      return this._handleTick(request);
    }

    if (url.pathname === "/stats" && request.method === "GET") {
      return new Response(
        JSON.stringify({
          clients: this.clientSymbols.size,
          symbols: this.symbolClients.size,
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    return new Response("Not found", { status: 404 });
  }

  private _upgradeWebSocket(request: Request): Response {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader?.toLowerCase() !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair) as [WebSocket, WebSocket];

    server.accept();
    this.clientSymbols.set(server, new Set());

    server.addEventListener("message", (event: MessageEvent) => {
      this._handleClientMessage(server, event.data as string | ArrayBuffer);
    });
    server.addEventListener("close", () => this._removeClient(server));
    server.addEventListener("error", () => this._removeClient(server));

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as ResponseInit);
  }

  private async _handleTick(request: Request): Promise<Response> {
    let tick: TickPayload;
    try {
      tick = (await request.json()) as TickPayload;
    } catch {
      return new Response("Invalid JSON", { status: 400 });
    }
    if (!tick?.symbol) {
      return new Response("Missing symbol", { status: 400 });
    }
    this._broadcast(tick.symbol.toUpperCase(), tick);
    return new Response("ok");
  }

  private _handleClientMessage(ws: WebSocket, raw: string | ArrayBuffer): void {
    const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
    let payload: { type?: unknown; symbols?: unknown };
    try {
      payload = JSON.parse(text) as { type?: unknown; symbols?: unknown };
    } catch {
      return;
    }
    if (!payload || typeof payload !== "object") return;
    if (!Array.isArray(payload.symbols)) return;

    const symbols = (payload.symbols as unknown[])
      .filter((s): s is string => typeof s === "string")
      .map((s) => s.toUpperCase());

    const clientSubs = this.clientSymbols.get(ws);
    if (!clientSubs) return;

    if (payload.type === "subscribe") {
      for (const sym of symbols) {
        clientSubs.add(sym);
        if (!this.symbolClients.has(sym)) {
          this.symbolClients.set(sym, new Set());
        }
        this.symbolClients.get(sym)!.add(ws);
      }
    } else if (payload.type === "unsubscribe") {
      for (const sym of symbols) {
        clientSubs.delete(sym);
        const set = this.symbolClients.get(sym);
        if (set) {
          set.delete(ws);
          if (set.size === 0) this.symbolClients.delete(sym);
        }
      }
    }
  }

  private _broadcast(symbol: string, tick: TickPayload): void {
    const clients = this.symbolClients.get(symbol);
    if (!clients || clients.size === 0) return;
    const msg = JSON.stringify(tick);
    for (const ws of clients) {
      try {
        ws.send(msg);
      } catch {
        this._removeClient(ws);
      }
    }
  }

  private _removeClient(ws: WebSocket): void {
    const subs = this.clientSymbols.get(ws);
    if (subs) {
      for (const sym of subs) {
        const set = this.symbolClients.get(sym);
        if (set) {
          set.delete(ws);
          if (set.size === 0) this.symbolClients.delete(sym);
        }
      }
    }
    this.clientSymbols.delete(ws);
    try {
      ws.close();
    } catch {
      /* already closed */
    }
  }
}

// ---- Alpaca REST polling helpers ------------------------------------

const ALPACA_DATA = "https://data.alpaca.markets";

async function fetchStockTrades(
  symbols: string[],
  key: string,
  secret: string,
): Promise<TickPayload[]> {
  if (symbols.length === 0) return [];
  const url = `${ALPACA_DATA}/v2/stocks/trades/latest?symbols=${symbols.join(",")}&feed=iex`;
  const resp = await fetch(url, {
    headers: { "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret },
  });
  if (!resp.ok) return [];
  const body = (await resp.json()) as { trades: Record<string, { p: number; s: number; t: string }> };
  return Object.entries(body.trades ?? {}).map(([symbol, t]) => ({
    symbol,
    price: t.p,
    size: t.s,
    timestamp: t.t,
    asset_class: "us_equity",
  }));
}

async function fetchCryptoTrades(
  symbols: string[],
  key: string,
  secret: string,
): Promise<TickPayload[]> {
  if (symbols.length === 0) return [];
  const encoded = symbols.map((s) => encodeURIComponent(s)).join(",");
  const url = `${ALPACA_DATA}/v1beta3/crypto/us/latest/trades?symbols=${encoded}`;
  const resp = await fetch(url, {
    headers: { "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret },
  });
  if (!resp.ok) return [];
  const body = (await resp.json()) as { trades: Record<string, { p: number; s: number; t: string }> };
  return Object.entries(body.trades ?? {}).map(([symbol, t]) => ({
    symbol,
    price: t.p,
    size: t.s,
    timestamp: t.t,
    asset_class: "crypto",
  }));
}

// ---- Worker entry point -----------------------------------------------

export default {
  async fetch(
    request: Request,
    env: Env,
    _ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/ws/prices") {
      const id = env.PRICE_HUB.idFromName("global");
      const stub = env.PRICE_HUB.get(id);
      const doUrl = new URL(request.url);
      doUrl.pathname = "/ws";
      return stub.fetch(new Request(doUrl.toString(), request));
    }

    if (url.pathname === "/internal/tick") {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }
      const auth = request.headers.get("X-KTI-Internal");
      if (!env.INTERNAL_SECRET || auth !== env.INTERNAL_SECRET) {
        return new Response("Unauthorized", { status: 401 });
      }
      const id = env.PRICE_HUB.idFromName("global");
      const stub = env.PRICE_HUB.get(id);
      const doUrl = new URL(request.url);
      doUrl.pathname = "/tick";
      return stub.fetch(new Request(doUrl.toString(), request));
    }

    if (url.pathname === "/health") {
      const id = env.PRICE_HUB.idFromName("global");
      const stub = env.PRICE_HUB.get(id);
      const doUrl = new URL(request.url);
      doUrl.pathname = "/stats";
      const stats = await stub.fetch(new Request(doUrl.toString()));
      const data = (await stats.json()) as { clients: number; symbols: number };
      return new Response(
        JSON.stringify({ status: "ok", ...data }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const stockSymbols = (env.SYMBOLS_STOCKS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
    const cryptoSymbols = (env.SYMBOLS_CRYPTO ?? "").split(",").map((s) => s.trim()).filter(Boolean);

    const [stockTicks, cryptoTicks] = await Promise.all([
      fetchStockTrades(stockSymbols, env.ALPACA_API_KEY, env.ALPACA_API_SECRET),
      fetchCryptoTrades(cryptoSymbols, env.ALPACA_API_KEY, env.ALPACA_API_SECRET),
    ]);

    const ticks = [...stockTicks, ...cryptoTicks];
    if (ticks.length === 0) return;

    const id = env.PRICE_HUB.idFromName("global");
    const stub = env.PRICE_HUB.get(id);

    ctx.waitUntil(
      Promise.all(
        ticks.map((tick) =>
          stub.fetch(
            new Request("https://do-internal/tick", {
              method: "POST",
              body: JSON.stringify(tick),
              headers: { "Content-Type": "application/json" },
            }),
          ),
        ),
      ),
    );
  },
};
