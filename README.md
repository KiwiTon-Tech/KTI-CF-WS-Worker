# KTI-CF-WS-Worker

Cloudflare Worker + Durable Object that serves the real-time price WebSocket
endpoint for KiwiTon Investments.

## Architecture

```
Browser  ←→  wss://api.kiwiton-investments.com/ws/prices
                   ↓  (Cloudflare orange-cloud intercepts)
         KTI-CF-WS-Worker (this service)
              PriceHub Durable Object
                   ↑
         HTTP POST /internal/tick  (from KTI-Price-Publisher on Fly.io)
```

The Durable Object (`PriceHub`) is a single global instance that:
- Accepts WebSocket connections from browser clients
- Tracks per-client symbol subscriptions (`subscribe` / `unsubscribe` frames)
- Receives authenticated tick POSTs from the Fly.io publisher
- Broadcasts ticks to all clients subscribed to that symbol

## Prerequisites

- [Cloudflare account](https://dash.cloudflare.com) with **Workers Paid plan** ($5/mo) — required for Durable Objects
- `api.kiwiton-investments.com` must be **orange-cloud (proxied)** in Cloudflare DNS
- [Node.js 18+](https://nodejs.org) and npm
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/)

## Setup

### 1. Install dependencies

```bash
npm install
```

### 2. Authenticate Wrangler

```bash
npx wrangler login
```

### 3. Set the internal secret

```bash
npx wrangler secret put INTERNAL_SECRET
# Enter the same secret you set in KTI-Price-Publisher's INTERNAL_SECRET env var
```

### 4. Orange-cloud `api.kiwiton-investments.com` in Cloudflare DNS

In the Cloudflare dashboard:
1. Go to your `kiwiton-investments.com` zone → DNS
2. Find the `api` subdomain record
3. Click the grey cloud icon to toggle it to **orange (proxied)**

> REST API requests still reach the origin (cPanel) transparently.  
> The Worker routes intercept only `/ws/*` and `/internal/tick`.

### 5. Deploy

```bash
npm run deploy
```

### 6. Verify

```bash
# Health check
curl https://api.kiwiton-investments.com/health

# WebSocket test (requires wscat: npm i -g wscat)
wscat -c wss://api.kiwiton-investments.com/ws/prices
# Send: {"type":"subscribe","symbols":["AAPL"]}
# You should receive ticks once KTI-Price-Publisher is running
```

## Local Development

```bash
# Create .dev.vars (gitignored)
echo "INTERNAL_SECRET=dev-secret" > .dev.vars

npm run dev
# Worker runs at http://localhost:8787
```

## Wire Formats

### Client → Worker (control frames)
```json
{ "type": "subscribe",   "symbols": ["AAPL", "SPY"] }
{ "type": "unsubscribe", "symbols": ["AAPL"] }
```

### Worker → Client (tick frames)
```json
{ "symbol": "AAPL", "price": 182.45, "size": 100, "timestamp": "2024-01-15T14:30:00Z", "asset_class": "us_equity" }
```

### Publisher → Worker (`POST /internal/tick`)
Same tick format as above. Authenticated with `X-KTI-Internal: <INTERNAL_SECRET>`.

## Endpoints

| Path | Method | Description |
|---|---|---|
| `/ws/prices` | `GET` (WS upgrade) | Browser WebSocket endpoint |
| `/internal/tick` | `POST` | Authenticated tick ingress from publisher |
| `/health` | `GET` | Returns `{ status, clients, symbols }` |
