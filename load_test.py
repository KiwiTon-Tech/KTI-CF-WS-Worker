"""
WS load test: 100 concurrent connections × 10 symbols.
Waits up to 90s for the CF Worker cron to fire and deliver ticks.

Usage:
    pip install websockets
    python load_test.py
"""

import asyncio
import json
import time
from collections import defaultdict

import websockets

WS_URL = "wss://api.kiwiton-investments.com/ws/prices"
SYMBOLS = ["SPY", "AAPL", "TSLA", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BTC/USD", "ETH/USD"]
NUM_CONNECTIONS = 100
WAIT_SECONDS = 90  # CF cron fires every 60s; give 1.5x headroom


async def connect(conn_id: int, results: dict) -> None:
    received = []
    try:
        async with websockets.connect(WS_URL, open_timeout=10) as ws:
            await ws.send(json.dumps({"type": "subscribe", "symbols": SYMBOLS}))
            deadline = time.monotonic() + WAIT_SECONDS
            while time.monotonic() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
                    tick = json.loads(msg)
                    received.append(tick.get("symbol"))
                except asyncio.TimeoutError:
                    break
                except Exception:
                    break
    except Exception as exc:
        results[conn_id] = {"error": str(exc), "ticks": []}
        return

    results[conn_id] = {"ticks": received}


async def main() -> None:
    print(f"Opening {NUM_CONNECTIONS} connections to {WS_URL}")
    print(f"Subscribing each to {len(SYMBOLS)} symbols, waiting up to {WAIT_SECONDS}s for ticks...\n")

    results: dict = {}
    t0 = time.monotonic()

    tasks = [connect(i, results) for i in range(NUM_CONNECTIONS)]
    await asyncio.gather(*tasks)

    elapsed = time.monotonic() - t0

    errors = [i for i, r in results.items() if "error" in r]
    connected = NUM_CONNECTIONS - len(errors)
    tick_counts = [len(r.get("ticks", [])) for r in results.values()]
    total_ticks = sum(tick_counts)
    conns_with_ticks = sum(1 for c in tick_counts if c > 0)

    symbol_counts: dict = defaultdict(int)
    for r in results.values():
        for sym in r.get("ticks", []):
            symbol_counts[sym] += 1

    print(f"{'='*50}")
    print(f"Results ({elapsed:.1f}s elapsed)")
    print(f"{'='*50}")
    print(f"Connections:        {connected}/{NUM_CONNECTIONS} established")
    print(f"Errors:             {len(errors)}")
    print(f"Conns with ticks:   {conns_with_ticks}/{connected}")
    print(f"Total ticks:        {total_ticks}")
    print(f"Ticks per symbol:")
    for sym in SYMBOLS:
        count = symbol_counts.get(sym, 0)
        bar = "█" * min(count // 2, 40)
        print(f"  {sym:<10} {count:>4}  {bar}")

    if errors:
        print(f"\nFirst 5 errors:")
        for i in errors[:5]:
            print(f"  conn {i}: {results[i]['error']}")

    print(f"\n{'='*50}")
    if conns_with_ticks == connected and not errors:
        print("✅ PASS — all connections received ticks")
    elif conns_with_ticks > connected * 0.95:
        print(f"⚠️  PARTIAL — {conns_with_ticks}/{connected} received ticks (>{95}% threshold)")
    else:
        print(f"❌ FAIL — only {conns_with_ticks}/{connected} received ticks")


if __name__ == "__main__":
    asyncio.run(main())
