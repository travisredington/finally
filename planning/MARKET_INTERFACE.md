# Market Data Interface Design

Unified Python interface for market data in FinAlly. Two implementations — the built-in **simulator** and the **Massive** REST poller — sit behind one abstract interface. All downstream code (SSE streaming, portfolio valuation, trade execution) is source-agnostic.

Related docs: [MASSIVE_API.md](MASSIVE_API.md) for the Massive REST reference, [MARKET_SIMULATOR.md](MARKET_SIMULATOR.md) for the GBM simulator design.

## Design Goals

1. **Single interface, two implementations** — swapping data sources is a one-line change based on an env var.
2. **Push, not pull** — the source runs a background task and writes to a shared cache. Downstream consumers read from the cache; they never poll the source directly.
3. **Fully async-friendly** — start/stop/mutation methods are coroutines so they compose cleanly with FastAPI's lifespan.
4. **Dynamic tickers** — watchlist can grow or shrink at runtime; both sources handle add/remove without restart.
5. **Thread-safe cache** — the SSE endpoint (async) and background poll loops (may run in threads) both touch the cache.

## Selection Logic

```
if MASSIVE_API_KEY is set and non-empty  →  MassiveDataSource
else                                     →  SimulatorDataSource
```

This is intentionally the only knob. No mode flags, no partial mocking. If the key is present, we hit the real API.

## Core Data Model

```python
from dataclasses import dataclass
from typing import Literal

Direction = Literal["up", "down", "flat"]

@dataclass(frozen=True)
class PriceUpdate:
    """A single price update for one ticker. Immutable."""
    ticker: str
    price: float
    previous_price: float
    timestamp: float          # Unix seconds
    change: float             # price - previous_price
    direction: Direction
```

`PriceUpdate` is the *only* data structure that leaves the market data layer. Downstream code — SSE serializer, portfolio math, LLM context builder — works with `PriceUpdate` and nothing else. Making it `frozen` prevents downstream code from mutating shared cache entries by accident.

## Abstract Interface

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """Abstract interface for market data providers.

    Implementations run a background task that periodically pushes fresh
    prices into a shared PriceCache. Consumers read from the cache; they
    do not interact with the source directly.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Called once at app startup. Idempotent — calling twice is a no-op
        (or raises, at the implementer's discretion).
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop producing price updates and release resources.

        Called at app shutdown. Cancels the background task and awaits
        its termination. Safe to call even if start() was never called.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. Takes effect on the next tick."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set and evict it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return a snapshot of the current active ticker list."""
```

Notes:
- The interface does **not** return prices directly. Consumers get prices from the shared `PriceCache`.
- `add_ticker` / `remove_ticker` are coroutines because implementations may need to acquire locks or await mid-poll transitions.
- `get_tickers` is synchronous because it just returns a copy of the list — no I/O.

## Shared Price Cache

```python
import time
from threading import Lock

class PriceCache:
    """Thread-safe cache of the latest price per ticker.

    Producers (data sources) call update(). Consumers (SSE endpoint,
    trade executor) call get() / get_all(). A monotonically increasing
    version counter lets the SSE endpoint detect changes without diffing.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate."""
        with self._lock:
            ts = timestamp if timestamp is not None else time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            if price > previous_price:
                direction: Direction = "up"
            elif price < previous_price:
                direction = "down"
            else:
                direction = "flat"

            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=ts,
                change=price - previous_price,
                direction=direction,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        with self._lock:
            entry = self._prices.get(ticker)
            return entry.price if entry else None

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    def version(self) -> int:
        with self._lock:
            return self._version
```

The **version counter** matters: the SSE endpoint can cheaply check `cache.version()` on a short interval and only serialize+send when it changes. Without it, the SSE loop would either re-broadcast identical payloads (waste) or diff the whole dict each tick (CPU burn on large watchlists).

## Factory

```python
import os
from .cache import PriceCache
from .interface import MarketDataSource

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the market data source based on the environment.

    - MASSIVE_API_KEY set and non-empty  → MassiveDataSource
    - otherwise                          → SimulatorDataSource
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        # Import inside the branch so tests can run without the massive
        # package installed — but at production build time it's a hard dep.
        from .massive_client import MassiveDataSource
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)

    from .simulator import SimulatorDataSource
    return SimulatorDataSource(price_cache=price_cache)
```

## Massive Implementation Sketch

Full API details in [MASSIVE_API.md](MASSIVE_API.md).

```python
import asyncio
import logging
from massive import RESTClient
from massive.rest.models import TickerSnapshot

log = logging.getLogger(__name__)

class MassiveDataSource(MarketDataSource):
    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,   # free tier: 5 req/min
    ) -> None:
        self._client = RESTClient(api_key=api_key)
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def start(self, tickers: list[str]) -> None:
        async with self._lock:
            self._tickers = list(dict.fromkeys(tickers))  # de-dupe, preserve order
        if self._task is None:
            self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def add_ticker(self, ticker: str) -> None:
        async with self._lock:
            if ticker not in self._tickers:
                self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        async with self._lock:
            self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Massive poll failed; keeping last known prices")
            await asyncio.sleep(self._interval)

    async def _poll_once(self) -> None:
        async with self._lock:
            tickers = list(self._tickers)
        if not tickers:
            return

        # The massive client is synchronous — run it in a worker thread.
        snapshots = await asyncio.to_thread(
            self._client.get_snapshot_all, "stocks", tickers,
        )
        for snap in snapshots:
            if isinstance(snap, TickerSnapshot) and snap.last_trade is not None:
                self._cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.timestamp / 1000.0,  # ms → s
                )
```

Key details:
- **`asyncio.to_thread`** wraps the sync client so a slow HTTP call never blocks the FastAPI event loop.
- **Broad `except Exception`** in the loop — one bad poll doesn't kill live streaming. The cache keeps serving stale-but-valid prices while the poller recovers.
- **Lock around the ticker list** — `add_ticker` mid-poll shouldn't produce a partially-mutated request.

## Simulator Implementation Sketch

Full math and correlation design in [MARKET_SIMULATOR.md](MARKET_SIMULATOR.md).

```python
import asyncio
from .simulator_core import GBMSimulator

class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,   # 500ms → feels "live"
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=list(dict.fromkeys(tickers)))
        if self._task is None:
            self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim is not None:
            self._sim.add_ticker(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim is not None:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        assert self._sim is not None
        while True:
            prices = self._sim.step()   # {ticker: new_price}
            for ticker, price in prices.items():
                self._cache.update(ticker=ticker, price=price)
            await asyncio.sleep(self._interval)
```

Note the interval difference: 500 ms for the simulator (in-process, cheap) vs 15 s for Massive (rate limits). Both write the *same* `PriceUpdate` shape to the cache — the UI has no idea which one is running.

## SSE Integration

The SSE endpoint reads from the shared cache and pushes on version changes:

```python
import asyncio
import json
from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

def create_stream_router(cache: PriceCache) -> APIRouter:
    router = APIRouter()

    @router.get("/api/stream/prices")
    async def stream_prices():
        async def gen():
            last_version = -1
            while True:
                v = cache.version()
                if v != last_version:
                    last_version = v
                    prices = cache.get_all()
                    payload = {
                        t: {
                            "ticker": p.ticker,
                            "price": p.price,
                            "previous_price": p.previous_price,
                            "change": p.change,
                            "direction": p.direction,
                            "timestamp": p.timestamp,
                        }
                        for t, p in prices.items()
                    }
                    yield {"event": "prices", "data": json.dumps(payload)}
                await asyncio.sleep(0.25)

        return EventSourceResponse(gen())

    return router
```

The endpoint checks the version every 250 ms and only sends when it changes. With the simulator (500 ms updates), that's ~2 sends/sec. With Massive (15 s polls), that's ~1 send / 15 s. Both feel appropriate to the source rhythm without extra config.

## Wiring at App Startup

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import (
    PriceCache,
    create_market_data_source,
    create_stream_router,
)
from app.db import get_default_watchlist

@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = PriceCache()
    source = create_market_data_source(cache)
    tickers = get_default_watchlist()
    await source.start(tickers)

    app.state.price_cache = cache
    app.state.market_source = source

    try:
        yield
    finally:
        await source.stop()

app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(app.state.price_cache))
```

Trade execution and watchlist mutation handlers pull `app.state.price_cache` / `app.state.market_source` from the request context.

## File Structure

```
backend/
  app/
    market/
      __init__.py            # Re-exports PriceUpdate, MarketDataSource, PriceCache, create_market_data_source
      models.py              # PriceUpdate dataclass, Direction type
      interface.py           # MarketDataSource ABC
      cache.py               # PriceCache (thread-safe, version counter)
      factory.py             # create_market_data_source()
      massive_client.py      # MassiveDataSource
      simulator.py           # SimulatorDataSource wrapper (async loop)
      simulator_core.py      # GBMSimulator (sync math — see MARKET_SIMULATOR.md)
      seed_prices.py         # SEED_PRICES, TICKER_PARAMS constants
      stream.py              # create_stream_router() FastAPI factory
```

## Lifecycle Summary

| Event | Action |
|-------|--------|
| App startup | Create cache → `create_market_data_source(cache)` → `await source.start(initial_tickers)` |
| Watchlist add | `await source.add_ticker(ticker)` |
| Watchlist remove | `await source.remove_ticker(ticker)` (also evicts from cache) |
| SSE client connects | Endpoint enters version-watch loop; sends first payload immediately, then only on changes |
| Trade execution | Reads current price via `cache.get_price(ticker)` |
| Portfolio valuation | Reads full snapshot via `cache.get_all()` |
| App shutdown | `await source.stop()` — cancels the poll task and awaits cleanup |

## Testing Notes

- **Unit-test the ABC contract** with a `FakeDataSource` that manually pushes prices into a cache — use it for portfolio and SSE tests without any network or randomness.
- **Simulator tests** — verify GBM math (`test_simulator.py`), tick determinism under a fixed seed, correlated Cholesky behavior.
- **Massive tests** — mock the `RESTClient` and assert we call `get_snapshot_all("stocks", tickers)` with the current watchlist, and correctly convert ms → s.
- **Factory tests** — set/unset `MASSIVE_API_KEY` and assert the right class is returned.
- **Cache tests** — thread-safety under concurrent writers, version increments, direction detection.
