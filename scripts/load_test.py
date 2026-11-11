"""WebSocket fan-out load test for the PriceHub Durable Object.

Opens N concurrent connections, subscribes each to the same symbols, and
waits long enough for the 1-minute cron to deliver at least one tick round.
Only price ticks ({symbol, price, ...} with no ``type``) count; subscription
acks and rejections are reported separately.

Usage:
    pip install websockets
    python scripts/load_test.py                     # production endpoint
    WS_URL=ws://127.0.0.1:8787/ws/prices python scripts/load_test.py
    NUM_CONNECTIONS=20 WAIT_SECONDS=75 python scripts/load_test.py

Exits 0 when every connection received ticks, 1 otherwise.
"""

import asyncio
import json
import os
import sys
import time
from collections import Counter

import websockets

WS_URL = os.environ.get("WS_URL", "wss://api.kiwiton-investments.com/ws/prices")
SYMBOLS = [
    "SPY",
    "AAPL",
    "TSLA",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "BTC/USD",
    "ETH/USD",
]
NUM_CONNECTIONS = int(os.environ.get("NUM_CONNECTIONS", "100"))
WAIT_SECONDS = float(os.environ.get("WAIT_SECONDS", "90"))  # cron is 60s; 1.5x headroom


async def connect(conn_id: int) -> dict:
    ticks: list[str] = []
    acked = False
    rejected: list = []
    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "subscribe", "symbols": SYMBOLS}))
            deadline = time.monotonic() + WAIT_SECONDS
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    frame = json.loads(
                        await asyncio.wait_for(ws.recv(), timeout=remaining)
                    )
                except TimeoutError:
                    break
                if frame.get("type") == "subscribed":
                    acked = True
                    rejected = frame.get("rejected") or []
                elif "type" not in frame and "price" in frame:
                    ticks.append(frame.get("symbol"))
    except Exception as exc:  # noqa: BLE001 - report any connection failure
        return {"id": conn_id, "error": f"{type(exc).__name__}: {exc}", "ticks": ticks}
    return {"id": conn_id, "acked": acked, "rejected": rejected, "ticks": ticks}


async def main() -> int:
    print(f"Opening {NUM_CONNECTIONS} connections to {WS_URL}")
    print(
        f"Subscribing each to {len(SYMBOLS)} symbols, waiting {WAIT_SECONDS:.0f}s for ticks\n"
    )

    t0 = time.monotonic()
    results = await asyncio.gather(*(connect(i) for i in range(NUM_CONNECTIONS)))
    elapsed = time.monotonic() - t0

    errors = [r for r in results if "error" in r]
    connected = [r for r in results if "error" not in r]
    acked = sum(1 for r in connected if r["acked"])
    with_ticks = sum(1 for r in connected if r["ticks"])
    per_symbol = Counter(sym for r in results for sym in r["ticks"])
    rejected = {rej.get("symbol") for r in connected for rej in r.get("rejected", [])}

    print(f"Results ({elapsed:.1f}s)")
    print(f"  connected:        {len(connected)}/{NUM_CONNECTIONS}")
    print(f"  subscribe acked:  {acked}/{len(connected)}")
    print(f"  received ticks:   {with_ticks}/{len(connected)}")
    print(f"  total ticks:      {sum(per_symbol.values())}")
    if rejected:
        print(f"  rejected symbols: {', '.join(sorted(map(str, rejected)))}")
    for sym in SYMBOLS:
        print(f"    {sym:<10} {per_symbol.get(sym, 0):>5}")
    for r in errors[:5]:
        print(f"  conn {r['id']}: {r['error']}")

    ok = not errors and connected and with_ticks == len(connected)
    print(
        "\nPASS"
        if ok
        else f"\nFAIL: {with_ticks}/{NUM_CONNECTIONS} connections received ticks"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
