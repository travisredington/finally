# Market Data Backend — Comprehensive Review

**Date:** 2026-07-09
**Scope:** `backend/app/market/` (8 source modules) and `backend/tests/market/` (6 test modules), reviewed against `planning/PLAN.md` and `planning/MARKET_DATA_DESIGN.md`.
**Reviewer:** Automated comprehensive review — full test run, coverage analysis, lint/format check, and manual code reading of every module.

---

## 1. Verdict

**The market data backend is solid, correct, and ready to build on.** All 73 tests pass, lint is clean, coverage is 91%, and the implementation faithfully matches both the design document and `PLAN.md` §6 (market data spec). This supersedes the `planning/archive/MARKET_DATA_REVIEW.md` review from the original build — the one blocking issue found there (`pyproject.toml` wheel packaging) and all "should fix" items have since been resolved (confirmed via `git log`: `f89aa14 Fix all issues from market data code review`, `6a2b36e Remove lazy imports for massive package`).

No blocking issues remain. Three minor/trivial items are noted below for optional cleanup.

---

## 2. Test Results

```
cd backend && uv run --extra dev pytest -v
```

**73 passed, 0 failed, in ~2–4s.**

| Module | Tests | What it covers |
|---|---|---|
| `test_cache.py` | 13 | `PriceCache` update/get/remove, direction/version bookkeeping, rounding, `__len__`/`__contains__` |
| `test_factory.py` | 7 | Env-var driven selection between simulator and Massive, key propagation |
| `test_massive.py` | 13 | Snapshot polling, malformed-data resilience, API error handling, ticker normalization, lifecycle |
| `test_models.py` | 11 | `PriceUpdate` computed properties, immutability, serialization |
| `test_simulator.py` | 19 | `GBMSimulator` math, ticker add/remove, correlation logic, positivity, rounding |
| `test_simulator_source.py` | 10 | `SimulatorDataSource` async lifecycle, dynamic tickers, exception resilience |

No skipped, xfail, or flaky tests observed across the run.

### Coverage

```
cd backend && uv run --extra dev pytest --cov=app --cov-report=term-missing
```

**91% overall** (up from 84% at the original build — the test suite has grown since, notably `test_massive.py` and `test_simulator.py` gained more cases).

| Module | Coverage | Missing lines | Assessment |
|---|---|---|---|
| `cache.py` | 100% | — | |
| `factory.py` | 100% | — | |
| `interface.py` | 100% | — | |
| `models.py` | 100% | — | |
| `seed_prices.py` | 100% | — | |
| `__init__.py` | 100% | — | |
| `massive_client.py` | 94% | 85–87 (`_poll_loop`'s infinite `while True`), 125 (`_fetch_snapshots` real API call) | Expected gaps — an infinite polling loop and the literal SDK call can't be meaningfully unit-tested; both are exercised indirectly via `_poll_once`/mocks. |
| `simulator.py` | 98% | 149 (`_add_ticker_internal` duplicate guard), 268–269 (exception log line in `_run_loop`) | Expected — defensive guard and error-log path, both trivial. |
| `stream.py` | 33% | Nearly the whole SSE generator | Expected and previously flagged — needs a running ASGI server (`httpx.AsyncClient`) to test meaningfully; no such test exists yet because the FastAPI app (`main.py`) hasn't been built. Not a regression, but still the biggest coverage gap in the subsystem. |

### Lint & Format

```
cd backend && uv run --extra dev ruff check app/ tests/     # All checks passed!
cd backend && uv run --extra dev ruff format --check app/ tests/
```

- `ruff check`: **clean**, zero warnings (the unused-import warnings noted in the original review have been removed).
- `ruff format --check`: 3 test files (`test_models.py`, `test_simulator.py`, `test_simulator_source.py`) would be reformatted — cosmetic only, single-line `PriceUpdate(...)` constructor calls that exceed the wrapped-line style `ruff format` prefers. Does not affect correctness; `ruff check` doesn't flag it since `E501` is explicitly ignored. **Trivial.**

---

## 3. Architecture Assessment

The subsystem cleanly implements the strategy pattern specified in `PLAN.md` §6:

```
MarketDataSource (ABC)
├── SimulatorDataSource  (GBM simulator)
└── MassiveDataSource    (Polygon.io REST poller)
        │
        ▼
   PriceCache (shared, thread-safe, versioned)
        │
        ▼
   SSE stream → Frontend
```

**Strengths confirmed by this review:**

- **Source-agnostic downstream code.** Nothing outside `simulator.py`/`massive_client.py` knows which provider is active — verified by reading `stream.py`, `factory.py`, and `cache.py`: all interact only through `PriceCache` and the `MarketDataSource` ABC.
- **PriceCache as single point of truth.** Thread-safe via `threading.Lock`, correctly chosen over `asyncio.Lock` since `MassiveDataSource._fetch_snapshots` runs inside `asyncio.to_thread()` — a genuine OS thread, not just another coroutine. Verified this is the only writer path that isn't on the event loop.
- **Immutable `PriceUpdate`.** `frozen=True, slots=True` dataclass with `change`/`change_percent`/`direction` as computed properties — impossible for these to desync from `price`/`previous_price`. Confirmed no code anywhere mutates a `PriceUpdate` after construction (would raise `FrozenInstanceError` if attempted).
- **Correct GBM math.** `S(t+dt) = S(t) * exp((mu - 0.5*sigma²)*dt + sigma*sqrt(dt)*Z)` — verified against the standard formulation, and `test_prices_are_positive` empirically confirms the multiplicative update never produces a negative price across 10,000 steps.
- **Cholesky-correlated moves.** `_rebuild_cholesky()` correctly guards `n <= 1` (no correlation matrix needed/possible for a single ticker), and the correlation matrix (tech 0.6, finance 0.5, cross/TSLA 0.3) is symmetric and positive semi-definite by construction (values in `[-1, 1]`, diagonal 1.0), so `np.linalg.cholesky` won't raise `LinAlgError` for any subset of the current ticker universe. This was previously a "nice to have" test suggestion (full 10-ticker Cholesky test) — now implicitly covered since `test_cholesky_rebuilds_on_add` and the `SimulatorDataSource` integration tests exercise multi-ticker construction.
- **Resilient background loops.** Both `SimulatorDataSource._run_loop` and `MassiveDataSource._poll_once` catch exceptions per-cycle and log rather than crash the task — confirmed by `test_exception_resilience` (simulator) and `test_api_error_does_not_crash` (Massive).
- **Idempotent shutdown.** `stop()` on both sources is safe to call multiple times (`test_stop_is_clean`, `test_stop_is_idempotent`) and correctly awaits the cancelled task, swallowing `CancelledError`.
- **Immediate cache seeding.** Both sources populate the cache synchronously in `start()`/`add_ticker()` before the first background tick, so a freshly-added ticker has a price immediately rather than waiting for the next loop iteration — important for the "no blank screen" UX goal in `PLAN.md` §2.
- **SSE version-based change detection.** `stream.py` only serializes and sends when `price_cache.version` has changed since the last check, avoiding redundant payloads when Massive (15s cadence) is the active source but the SSE loop polls every 500ms.

---

## 4. Issues Found

All issues from the previous review (`planning/archive/MARKET_DATA_REVIEW.md`) have been resolved. This pass found only minor/trivial items, none blocking:

### 4.1 `PriceCache.version` read outside the lock (Severity: Low — unchanged from prior review)

```python
@property
def version(self) -> int:
    return self._version
```

Still not acquired under `self._lock`. On CPython with the GIL this is safe (single `int` read is atomic), and every other method that mutates `_version` does so under the lock, so there's no correctness bug today. Flagging again only because it's a latent inconsistency that would matter on a no-GIL Python build. Not worth fixing unless the project targets PEP 703 free-threaded Python.

### 4.2 SSE streaming has almost no test coverage (Severity: Low, expected)

`stream.py` sits at 33% coverage. This is expected — the SSE generator needs a live ASGI app to exercise meaningfully, and `main.py` doesn't exist yet (per `PLAN.md` §4, the rest of the platform is still to be developed). Once `main.py` is built, an integration test using `httpx.ASGITransport`/`AsyncClient` against `create_stream_router()` should be added — this is the single largest remaining test gap in the subsystem and should be picked up alongside the FastAPI app work, not deferred indefinitely.

### 4.3 Three test files fail `ruff format --check` (Severity: Trivial)

`test_models.py`, `test_simulator.py`, `test_simulator_source.py` have a handful of `PriceUpdate(...)` constructor calls on a single line that `ruff format` would wrap. Purely cosmetic — `ruff check` (the linter) doesn't flag these since `E501` is ignored, and the tests are otherwise correct. Optional: run `uv run --extra dev ruff format tests/` to normalize.

### 4.4 `massive_client.py` real-API paths are structurally untestable without live credentials (Severity: Informational, not a defect)

Lines 85–87 (the poll loop's `while True`) and 125 (the literal `get_snapshot_all()` call) are the only uncovered lines in the module. This is inherent to mocking `_fetch_snapshots`/`_poll_once` rather than the transport layer, which is the correct testing strategy here (avoids depending on Massive's SDK internals or live network in CI) — just noting it as the reason 94% is the practical ceiling for this file, not a gap to chase.

---

## 5. Correctness Spot-Checks Performed

Beyond running the existing suite, this review independently verified several properties by inspection:

- **No negative prices possible.** The only price mutation is `self._prices[ticker] *= math.exp(...)` (GBM step) or `*= (1 + shock)` where shock ∈ [-0.05, 0.05] — both strictly positive multipliers applied to a strictly positive starting price.
- **Ticker uppercasing/whitespace normalization is only applied in `MassiveDataSource.add_ticker`/`remove_ticker`**, not in `SimulatorDataSource` or `GBMSimulator`. This is a minor asymmetry: if a caller passes a lowercase or padded ticker to the simulator path, it will be stored as-is (e.g. `"aapl "` would create a distinct cache entry from `"AAPL"`). In practice this is a non-issue today because the watchlist layer (not yet built) is expected to normalize tickers before calling either data source — but whoever builds that layer should be aware only the Massive path currently defends against it, and should normalize centrally (e.g. in the watchlist API route) rather than relying on inconsistent per-source behavior.
- **Timestamp unit conversion is correct.** Massive returns Unix milliseconds (`snap.last_trade.timestamp`); `massive_client.py` divides by `1000.0` before passing to `PriceCache.update`, matching `PriceUpdate.timestamp`'s documented Unix-seconds contract. Verified against `test_timestamp_conversion`.
- **Factory correctly treats whitespace-only `MASSIVE_API_KEY` as unset** (`.strip()` before the truthiness check), confirmed by `test_creates_simulator_when_api_key_whitespace`.
- **Build configuration is fixed.** `pyproject.toml` has `[tool.hatch.build.targets.wheel] packages = ["app"]`, and `uv sync --extra dev` / `uv run` both succeed cleanly from a fresh environment (verified in this session) — the High-severity blocker from the original review no longer applies.

---

## 6. Alignment with `PLAN.md`

Checked against `PLAN.md` §6 ("Market Data") line by line:

| Spec requirement | Status |
|---|---|
| Abstract interface, both implementations conform | ✅ `MarketDataSource` ABC, both subclasses implement all 5 methods |
| Simulator: GBM with configurable drift/volatility per ticker | ✅ `TICKER_PARAMS` per ticker, `DEFAULT_PARAMS` fallback |
| Simulator: ~500ms update interval | ✅ `SimulatorDataSource(update_interval=0.5)` default |
| Simulator: correlated moves across tickers | ✅ Cholesky decomposition, sector-based grouping |
| Simulator: occasional random events (2-5% moves) | ✅ `event_probability=0.001`, `shock_magnitude ∈ [0.02, 0.05]` |
| Simulator: realistic seed prices | ✅ `SEED_PRICES` matches PLAN.md's example values (AAPL ~$190, GOOGL ~$175, etc.) |
| Simulator: in-process background task, no external deps | ✅ `asyncio.create_task`, only `numpy` required |
| Massive: REST polling, not WebSocket | ✅ `RESTClient.get_snapshot_all()` |
| Massive: union of watched tickers, single call | ✅ one `get_snapshot_all(tickers=self._tickers)` call per poll |
| Massive: free tier 15s poll, paid tiers faster | ✅ `poll_interval=15.0` default, constructor param for override |
| Shared in-memory price cache | ✅ `PriceCache`, single point of truth |
| SSE endpoint `/api/stream/prices`, ~500ms cadence | ✅ implemented in `stream.py` (not yet mounted — `main.py` doesn't exist) |
| Each event has ticker, price, previous_price, timestamp, direction | ✅ `PriceUpdate.to_dict()` |

Every requirement in the market data section of the spec is met. The only outstanding wiring is mounting `create_stream_router()` onto a live FastAPI app, which is explicitly out of scope for this subsystem per the directory-boundaries section of `PLAN.md` §4 (`backend/` internal structure, including `main.py`, is the next phase of work).

---

## 7. Recommendation

**Ship it as-is.** Nothing here blocks moving on to the rest of the backend (database, portfolio, watchlist, chat, and `main.py`). Suggested (non-blocking) follow-ups, roughly in priority order:

1. When `main.py` is built, add an SSE integration test against `create_stream_router()` — closes the one real coverage gap.
2. Decide on a single place to normalize tickers (uppercase + strip) — ideally in the watchlist API layer — rather than only in `MassiveDataSource`, so behavior is consistent regardless of which provider is active.
3. Optional cosmetic: `uv run --extra dev ruff format tests/` to satisfy `ruff format --check`.
