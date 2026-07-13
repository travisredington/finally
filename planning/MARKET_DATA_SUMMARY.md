# Market Data Backend — Summary

**Status:** Complete, tested, reviewed, all issues resolved.
**Location:** `backend/app/market/` (8 source modules) + `backend/tests/market/` (6 test modules).
**Health:** 73 tests passing, 91% coverage overall, `ruff check` clean.

This is the single source-of-truth doc for the market data subsystem. It consolidates what
previously lived in `MARKET_DATA_DESIGN.md`, `MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`,
`MASSIVE_API.md`, and `MARKET_DATA_REVIEW.md`. The original standalone docs are preserved
in `planning/archive/` for historical reference.

---

## 1. What Was Built

A complete market data subsystem providing live price simulation and (optional) real market
data via a unified interface. Downstream code (SSE streaming, portfolio valuation, trade
execution) is source-agnostic — it only sees `PriceCache` and the `MarketDataSource` ABC.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory, versioned)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation        (not yet built)
        └──→ Trade execution            (not yet built)
```

Both data sources implement the same `MarketDataSource` ABC and **push** prices into a
shared `PriceCache` on their own schedule (500ms for the simulator, 15s for Massive's free
tier). Downstream code reads from the cache and has no idea which source is active.
Selection happens once, at startup, via `create_market_data_source()`, based on whether
`MASSIVE_API_KEY` is set.

### File Structure

```
backend/
  app/
    market/
      __init__.py             # Public re-exports
      models.py               # PriceUpdate dataclass
      interface.py            # MarketDataSource ABC
      cache.py                # PriceCache (thread-safe, versioned)
      seed_prices.py          # SEED_PRICES, TICKER_PARAMS, CORRELATION_GROUPS
      simulator.py            # GBMSimulator + SimulatorDataSource
      massive_client.py       # MassiveDataSource
      factory.py              # create_market_data_source()
      stream.py               # SSE endpoint (FastAPI router factory)
  market_data_demo.py         # Rich terminal demo (`uv run market_data_demo.py`)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

### Module Roles

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, change, direction) |
| `interface.py` | `MarketDataSource` — abstract base class: `start/stop/add_ticker/remove_ticker/get_tickers` |
| `cache.py` | `PriceCache` — thread-safe price store with monotonically-increasing version counter |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (correlated GBM via Cholesky) + `SimulatorDataSource` async wrapper |
| `massive_client.py` | `MassiveDataSource` — REST polling client for Polygon.io/Massive |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on env |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory using version-based change detection |

---

## 2. Key Design Decisions

- **Strategy pattern.** Both data sources implement the same ABC; downstream code is
  source-agnostic.
- **PriceCache as single point of truth.** Producers write, consumers read; no direct
  coupling between data sources and consumers.
- **Push, not pull.** Data sources run background tasks and write to the cache on their own
  schedule (500ms simulator, 15s Massive). SSE reads the cache at its own fixed cadence.
- **Immutable `PriceUpdate`.** `frozen=True, slots=True` dataclass; `change`,
  `change_percent`, `direction` are computed properties so they can never desync from
  `price`/`previous_price`.
- **Thread lock, not asyncio lock.** `PriceCache` uses `threading.Lock` because
  `MassiveDataSource._fetch_snapshots` runs in a real OS thread via `asyncio.to_thread()`.
- **Version counter on cache.** Lets the SSE loop cheaply skip a send when nothing changed
  (matters when Massive is active at 15s cadence but SSE polls every 500ms).
- **SSE over WebSockets.** Simpler, one-way push, universal browser support with automatic
  reconnection built into `EventSource`.

---

## 3. Data Model — `PriceUpdate`

The only data structure that leaves the market data layer. Every downstream consumer works
exclusively with this type.

```python
@dataclass(frozen=True, slots=True)
class PriceUpdate:
    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float: ...
    @property
    def change_percent(self) -> float: ...
    @property
    def direction(self) -> str:  # 'up' | 'down' | 'flat'
        ...

    def to_dict(self) -> dict: ...   # single serialization point for JSON/SSE
```

- `frozen=True` — safe to share across async tasks without copying.
- `slots=True` — memory optimization; many are created per second.
- Computed properties never drift out of sync with the price fields.

---

## 4. Price Cache

Central hub for the subsystem. Data sources write; SSE (and future portfolio/trade code)
reads.

```python
class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0

    def update(self, ticker, price, timestamp=None) -> PriceUpdate: ...
    def get(self, ticker) -> PriceUpdate | None: ...
    def get_price(self, ticker) -> float | None: ...
    def get_all(self) -> dict[str, PriceUpdate]: ...
    def remove(self, ticker) -> None: ...

    @property
    def version(self) -> int: ...
```

**Why the version counter?** The SSE loop polls the cache every ~500ms. Without a version,
it would re-serialize and re-send prices every tick even when Massive (15s cadence) hasn't
updated anything. The counter lets the loop skip cleanly:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

**Why `threading.Lock`, not `asyncio.Lock`?** `MassiveDataSource._fetch_snapshots()` runs
inside `asyncio.to_thread()` — a real OS thread. `asyncio.Lock` wouldn't protect against
that. The critical sections are small dict operations, so contention is a non-issue at this
scale.

---

## 5. Unified Interface

```python
class MarketDataSource(ABC):
    @abstractmethod
    async def start(self, tickers: list[str]) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def add_ticker(self, ticker: str) -> None: ...
    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None: ...
    @abstractmethod
    def get_tickers(self) -> list[str]: ...
```

The interface does **not** return prices directly. Consumers get prices from the shared
`PriceCache`. `start()` must be called exactly once; `stop()` is idempotent.

---

## 6. GBM Simulator

### Math: Geometric Brownian Motion

Stock prices evolve continuously with random noise, can never go negative (multiplicative
update via `exp()`), and produce a lognormal return distribution matching real markets:

```
S(t+dt) = S(t) * exp((mu - sigma²/2) * dt + sigma * sqrt(dt) * Z)
```

- `mu` — annualized drift (expected return), e.g. `0.05`
- `sigma` — annualized volatility, e.g. `0.20`
- `dt` — time step as fraction of a trading year
- `Z` — a correlated standard normal draw

For 500ms updates over 252 days × 6.5 hours:

```
dt = 0.5 / (252 * 6.5 * 3600) ≈ 8.48e-8
```

This tiny `dt` produces sub-cent per-tick moves that accumulate into realistic intraday
ranges.

### Correlation via Cholesky

Real stocks don't move independently. Given a correlation matrix `C`, its Cholesky factor
`L` (`C = L @ L.T`) turns independent standard normals into correlated ones:

```
Z_correlated = L @ Z_independent
```

Correlation structure used:

| Pair | Correlation |
|---|---|
| Same tech sector (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Same finance sector (JPM, V) | 0.5 |
| TSLA with anything | 0.3 (TSLA does its own thing) |
| Cross-sector / unknown tickers | 0.3 |

The matrix is symmetric with values in `[-1, 1]` and diagonal `1.0`, so it's PSD by
construction and `np.linalg.cholesky` won't raise for any subset of the current universe.

### Random Shock Events

Each step, each ticker has ~0.1% chance (`event_probability=0.001`) of a sudden 2–5% move.
With 10 tickers ticking twice per second, expect a shock every ~50 seconds — often enough
to feel dynamic, rare enough not to feel fake.

### Seed Prices & Per-Ticker Parameters

```python
SEED_PRICES = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # high vol, tempered drift
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # high vol, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # low vol (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # low vol (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS = {"sigma": 0.25, "mu": 0.05}
```

Tickers added dynamically that aren't in `SEED_PRICES` get a random seed price in `$50–$300`
and fall back to `DEFAULT_PARAMS`.

### Async Wrapper

`SimulatorDataSource` wraps `GBMSimulator` in an asyncio loop that writes to the cache:

```python
async def _run_loop(self) -> None:
    while True:
        try:
            if self._sim:
                prices = self._sim.step()
                for ticker, price in prices.items():
                    self._cache.update(ticker=ticker, price=price)
        except Exception:
            logger.exception("Simulator step failed")
        await asyncio.sleep(self._interval)
```

**Key behaviors:**
- **Immediate seeding.** `start()` populates the cache with seed prices *before* the loop
  begins, so SSE has data on its very first tick — no blank-screen delay.
- **Graceful cancellation.** `stop()` cancels the task, awaits it, and swallows
  `CancelledError` for clean FastAPI lifespan teardown.
- **Exception resilience.** One bad tick doesn't kill the feed.
- **Positivity guaranteed.** Multiplicative `exp()` update on a positive starting price.

---

## 7. Massive API Client

Polls the Massive (formerly Polygon.io) REST snapshot endpoint on a configurable interval.
`massive` is a **core dependency** — imported at module level, not lazily.

### API Essentials

| | |
|---|---|
| Base URL | `https://api.massive.com` (`api.polygon.io` still works) |
| Python package | `massive` (`uv add massive`) |
| Auth env var | `MASSIVE_API_KEY` |
| Auth header | `Authorization: Bearer <key>` (client sets automatically) |
| Free tier | 5 req/min → poll every 15s |
| Paid tiers | Unlimited → poll every 2–5s |

The **multi-ticker snapshot** endpoint returns *all* requested tickers in one call, so a
single poll covers the whole watchlist — essential for staying inside the free tier:

```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key=...)
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", ...],
)
for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.last_trade.timestamp)
```

### Fields Actually Consumed

| Field | Used for |
|---|---|
| `snap.ticker` | Cache key |
| `snap.last_trade.price` | Current price |
| `snap.last_trade.timestamp` | Event timestamp (ms → converted to seconds) |

Other snapshot fields (`day.open/high/low/close`, `last_quote`, `prev_daily_bar`,
`todays_change_perc`) are available for future features (detail view with OHLC, day-change
display) but not consumed by the live price pipeline today.

### Polling Loop

```python
class MassiveDataSource(MarketDataSource):
    async def start(self, tickers):
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # immediate first poll — no blank screen
        self._task = asyncio.create_task(self._poll_loop())

    async def _poll_once(self):
        try:
            # Sync client → run in a thread to avoid blocking the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    ts = snap.last_trade.timestamp / 1000.0  # ms → s
                    self._cache.update(snap.ticker, price=price, timestamp=ts)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping malformed snapshot: %s", e)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)  # keep looping
```

### Error Handling Philosophy

The poller is intentionally resilient — a live feed that occasionally errors and retries
beats one that crashes:

| Error | Behavior |
|---|---|
| **401 Unauthorized** | Logged; poller keeps running (fix `.env` and restart). |
| **429 Rate limited** | Logged; next poll retries after `poll_interval`. |
| **Network timeout** | Logged; retried on next cycle. |
| **Malformed snapshot** | That ticker is skipped with a warning; others still processed. |
| **All tickers fail** | Cache retains last-known prices — SSE keeps streaming stale-but-present data. |

### Additional Massive Endpoints (Available, Not Used Yet)

For future features when needed:

- **Single ticker snapshot** — `client.get_snapshot_ticker("stocks", "AAPL")` for detail
  view.
- **Previous close** — `client.get_previous_close_agg("AAPL")` for seed prices / day-change
  baselines.
- **Aggregates** — `client.list_aggs("AAPL", 1, "minute", from_, to, ...)` for historical
  OHLCV bars (paginated).
- **Last trade / last quote** — `client.get_last_trade("AAPL")`,
  `client.get_last_quote("AAPL")` — cheaper single-ticker lookups.

Do **not** use the WebSocket API — it requires a Currencies/Stocks Advanced plan and adds a
reconnection layer. REST polling + SSE fan-out is sufficient.

---

## 8. Factory

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

Whitespace-only API keys are correctly treated as unset.

---

## 9. SSE Streaming Endpoint

A FastAPI route that holds a long-lived HTTP connection and pushes price updates as
`text/event-stream`.

```python
def create_stream_router(price_cache: PriceCache) -> APIRouter:
    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering
            },
        )
    return router


async def _generate_events(price_cache, request, interval=0.5) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"          # browser reconnect delay
    last_version = -1
    while True:
        if await request.is_disconnected():
            break
        if price_cache.version != last_version:
            last_version = price_cache.version
            prices = price_cache.get_all()
            if prices:
                data = {t: u.to_dict() for t, u in prices.items()}
                yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(interval)
```

### Wire Format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"}, ...}
```

### Client Side (Frontend)

`EventSource` is native — no library needed. Automatic reconnection built in.

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices is { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
};
```

### Why Poll-and-Push?

The endpoint polls the cache on a fixed interval rather than being event-notified. Simpler,
and produces evenly-spaced updates — which matters because the frontend accumulates these
into sparkline charts. Regular spacing keeps that visualization clean regardless of which
data source is behind it.

---

## 10. FastAPI Lifecycle Integration (Forward-Looking)

The market data modules are complete. `backend/app/main.py` and the rest of the API surface
(portfolio, watchlist, chat, db) do not exist yet. This is the prescribed integration
pattern for whoever builds them:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router
from app.market.interface import MarketDataSource


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    initial_tickers = await load_watchlist_tickers()   # from SQLite
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # App running

    # SHUTDOWN
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Watchlist Coordination

When the watchlist changes — via REST API or LLM chat's `watchlist_changes` — tell the
market source:

```python
@router.post("/watchlist")
async def add_to_watchlist(payload, source: MarketDataSource = Depends(get_market_source)):
    await db.insert_watchlist(payload.ticker)
    await source.add_ticker(payload.ticker)
    # Simulator seeds cache synchronously; Massive shows up on next poll


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker, source: MarketDataSource = Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)
    # Edge case: keep tracking if the user still holds shares (for portfolio valuation)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
```

### Trade Execution

```python
@router.post("/portfolio/trade")
async def execute_trade(trade, price_cache: PriceCache = Depends(get_price_cache)):
    price = price_cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}. Try again shortly.")
    # execute at `price`, persist to SQLite
```

The simulator avoids the "price not yet available" case by seeding the cache synchronously
in `add_ticker()`. Massive may have a brief real gap until the next poll (up to 15s on free
tier).

---

## 11. Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```

---

## 12. Test Suite

**73 tests, all passing.** Overall coverage **91%**.

| Module | Tests | Coverage | Notes |
|---|---|---|---|
| `test_models.py` | 11 | 100% | Computed properties, immutability, serialization |
| `test_cache.py` | 13 | 100% | update/get/remove, direction, version, `__len__`/`__contains__` |
| `test_simulator.py` | 19 | 98% | GBM math, ticker add/remove, correlation, positivity |
| `test_simulator_source.py` | 10 | — | Async lifecycle, dynamic tickers, exception resilience |
| `test_factory.py` | 7 | 100% | Env-var selection, key propagation |
| `test_massive.py` | 13 | 94% | Snapshot polling, malformed data, API errors, ticker normalization |
| `stream.py` (source) | — | 33% | **Coverage gap** — SSE needs a running ASGI app; add test when `main.py` lands |

Run locally:

```bash
cd backend
uv run --extra dev pytest -v              # all tests
uv run --extra dev pytest --cov=app       # with coverage
uv run --extra dev ruff check app/ tests/ # lint
```

---

## 13. Code Review Findings

### Original Review (Resolved)

A comprehensive code review of the initial build identified 7 issues. All resolved:

1. **`pyproject.toml` build config** — added `[tool.hatch.build.targets.wheel] packages = ["app"]`.
2. **Lazy imports removed** — `massive` is a core dependency; imports moved to module level.
3. **SSE return type fixed** — `_generate_events` annotated as `AsyncGenerator[str, None]`.
4. **Public `get_tickers()` on `GBMSimulator`** — replaced private attribute access.
5. **Correlation constants cleaned up** — removed unused `DEFAULT_CORR`, consolidated into
   `CROSS_GROUP_CORR`.
6. **Unused test imports removed** — `pytest`, `math`, `asyncio` cleaned from 4 test files.
7. **Massive test mocks fixed** — `source._client` set in tests; patches target correct
   names.

### Follow-Up Review (Post-Fix)

**Ship it as-is.** Nothing blocks moving on to the rest of the backend. Minor/trivial items
still open:

- **`PriceCache.version` read outside lock** (Low severity). Safe on CPython due to GIL;
  would matter only on PEP 703 free-threaded Python. Not worth fixing today.
- **SSE test coverage at 33%** (Low, expected). Needs a live ASGI app to exercise
  meaningfully. Add an `httpx.AsyncClient` / `ASGITransport` integration test when
  `main.py` is built — this is the single largest remaining test gap.
- **`ruff format --check` cosmetic failures** in 3 test files. `ruff check` (the linter)
  passes; only wrap-style differences on single-line `PriceUpdate(...)` constructor calls.
  Trivial: `uv run --extra dev ruff format tests/`.
- **Ticker normalization asymmetry.** Only `MassiveDataSource` uppercases and strips
  tickers; `SimulatorDataSource` and `GBMSimulator` take them as-is. Whoever builds the
  watchlist API should normalize centrally at that layer rather than relying on
  inconsistent per-source behavior.
- **Massive real-API paths structurally untestable without live creds.** The `while True`
  poll loop and the literal `get_snapshot_all()` call can't be meaningfully unit-tested;
  mocking `_fetch_snapshots`/`_poll_once` is the correct strategy. 94% is the practical
  ceiling for `massive_client.py`.

### Correctness Spot-Checks Verified

- **No negative prices possible** — GBM update is `*=exp(...)` on a positive seed; shock is
  `*=1+r` with `r ∈ [-0.05, 0.05]`.
- **Timestamp unit conversion correct** — Massive returns ms; divided by 1000.0 before
  writing to cache.
- **Whitespace-only `MASSIVE_API_KEY` treated as unset** — factory strips before truthiness
  check.
- **Cholesky guarded for `n <= 1`** — no correlation matrix for a single ticker; won't
  raise.
- **Every method that mutates `_version` does so under the lock** (only the read is
  unlocked).

---

## 14. Configuration Reference

| Parameter | Location | Default | Description |
|---|---|---|---|
| `MASSIVE_API_KEY` | Env var | `""` | If set, use Massive; otherwise simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5`s | Simulator tick interval |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0`s | Massive poll interval (free-tier safe) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Shock probability per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5`s | Cadence for cache-version check |
| SSE retry directive | `_generate_events()` | `1000`ms | Browser `EventSource` reconnection delay |

---

## 15. Demo

A Rich terminal dashboard sanity-checks the whole pipeline end to end without a frontend:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating table of all 10 tickers with sparklines, color-coded direction
arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

---

## 16. Edge Cases & Notes

- **Empty watchlist at startup.** Both sources handle `[]` gracefully — simulator produces
  no prices, Massive skips its API call, SSE sends empty payloads. Adding a ticker later
  starts tracking immediately.
- **Delayed data on free tier.** Massive free-tier snapshots are 15-minute delayed. Paid
  tiers are real-time.
- **After-hours / weekends.** `last_trade.price` reflects the last executed trade; the
  `day` object may be stale until the next session opens.
- **Cholesky rebuild is O(n²)** — fine for `n < 50`. If the watchlist ever grows to
  hundreds of tickers, cache and incrementally update the L matrix.
