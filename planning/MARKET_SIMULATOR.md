# Market Simulator Design

Approach and code structure for simulating realistic stock prices when no Massive API key is configured (the default for most users of FinAlly).

Related docs: [MARKET_INTERFACE.md](MARKET_INTERFACE.md) for how the simulator plugs into the unified data-source interface.

## Goals

1. **Feel alive** — prices tick continuously and visibly, not in dead 5-second jumps.
2. **Look real** — correlated moves, per-ticker volatility, realistic seed prices, occasional dramatic jumps.
3. **Cost nothing** — pure Python, no external services, no API keys.
4. **Be deterministic when needed** — under a fixed seed, tests get repeatable price paths.
5. **Handle a dynamic watchlist** — add/remove tickers mid-run without restarting.

## Model: Geometric Brownian Motion

Stock prices in the simulator follow **Geometric Brownian Motion (GBM)** — the same process behind Black-Scholes option pricing. Prices evolve continuously with random noise, can never go negative, and follow a lognormal distribution — the same shape observed in real intraday returns.

The discrete GBM step:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt · Z )
```

Where:
- `S(t)` — current price
- `μ` — annualized drift (expected return), e.g. `0.05` for 5%
- `σ` — annualized volatility, e.g. `0.20` for 20%
- `dt` — time step in years
- `Z` — standard normal draw

For 500 ms ticks over ~252 trading days × 6.5 hours × 3600 s:

```
dt = 0.5 / (252 · 6.5 · 3600) ≈ 8.5e-8
```

This tiny `dt` produces sub-cent per-tick moves that accumulate into realistic intraday paths.

## Correlated Moves

Real stocks don't move independently — tech mega-caps rise and fall together, banks move on the same rate news. The simulator uses **Cholesky decomposition** to generate correlated normal draws.

Given a correlation matrix `C`, compute `L = cholesky(C)`. Then for independent standard normals `Z_ind`:

```
Z_corr = L @ Z_ind
```

`Z_corr` has the desired pairwise correlations.

**Default correlation groups**:
- **Tech**: AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX — pairwise ρ ≈ 0.6
- **Finance**: JPM, V — pairwise ρ ≈ 0.5
- **Cross-sector**: ρ ≈ 0.3 baseline
- **TSLA**: ρ ≈ 0.3 with everyone — Tesla marches to its own drummer

The matrix must be **positive semi-definite** for Cholesky to work. The values above form a valid PSD matrix; if a future edit breaks that, `numpy.linalg.cholesky` will raise and tests will catch it.

## Random Shock Events

Every step, each ticker has a small probability (~0.001) of a **shock event** — a sudden ±2%–5% jump. This gives the dashboard visual drama without breaking realism.

```python
if random.random() < EVENT_PROB:
    shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
    price *= (1 + shock)
```

With 10 tickers and 0.001 probability per tick, expect a shock somewhere every ~50 seconds — often enough to feel dynamic, rare enough not to feel fake.

## Seed Prices

Realistic starting prices for the default watchlist:

```python
SEED_PRICES: dict[str, float] = {
    "AAPL":  190.0,
    "GOOGL": 175.0,
    "MSFT":  420.0,
    "AMZN":  185.0,
    "TSLA":  250.0,
    "NVDA":  800.0,
    "META":  500.0,
    "JPM":   195.0,
    "V":     280.0,
    "NFLX":  600.0,
}
```

Tickers added dynamically (not in the seed list) start at a random price in `$50–$300`.

## Per-Ticker Parameters

Each ticker has its own volatility and drift to reflect real-world behavior:

```python
TICKER_PARAMS: dict[str, dict[str, float]] = {
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

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

## Core Implementation

```python
import math
import random
import numpy as np

# Trading-year seconds — used to normalize dt when converted from ms tick rate
TRADING_YEAR_SECONDS = 252 * 6.5 * 3600
DEFAULT_TICK_SECONDS = 0.5
DEFAULT_DT = DEFAULT_TICK_SECONDS / TRADING_YEAR_SECONDS  # ≈ 8.5e-8

EVENT_PROB = 0.001
EVENT_MIN = 0.02
EVENT_MAX = 0.05


class GBMSimulator:
    """Generates correlated GBM price paths for a dynamic set of tickers."""

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = EVENT_PROB,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._rng = rng if rng is not None else np.random.default_rng()

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self.add_ticker(ticker)

    # ---- ticker management ----

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(
            ticker, self._rng.uniform(50.0, 300.0)
        )
        self._params[ticker] = TICKER_PARAMS.get(ticker, DEFAULT_PARAMS)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    # ---- tick ----

    def step(self) -> dict[str, float]:
        """Advance one time step. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        # Correlated standard normals
        z_ind = self._rng.standard_normal(n)
        z = self._cholesky @ z_ind if self._cholesky is not None else z_ind

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            p = self._params[ticker]
            mu, sigma = p["mu"], p["sigma"]

            # GBM increment
            drift = (mu - 0.5 * sigma * sigma) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Rare shock event
            if random.random() < self._event_prob:
                shock = random.uniform(EVENT_MIN, EVENT_MAX) * random.choice([-1, 1])
                self._prices[ticker] *= (1.0 + shock)

            result[ticker] = round(self._prices[ticker], 2)

        return result

    # ---- correlation ----

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_corr(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_corr(t1: str, t2: str) -> float:
        tech = {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"}
        finance = {"JPM", "V"}

        # TSLA does its own thing
        if "TSLA" in (t1, t2):
            return 0.3
        if t1 in tech and t2 in tech:
            return 0.6
        if t1 in finance and t2 in finance:
            return 0.5
        # cross-sector or unknown
        return 0.3
```

## Async Wrapper

The `SimulatorDataSource` in `simulator.py` wraps `GBMSimulator` in an asyncio loop and pushes updates into the shared `PriceCache`. See [MARKET_INTERFACE.md](MARKET_INTERFACE.md) for the wrapper code.

```python
async def _run_loop(self) -> None:
    while True:
        prices = self._sim.step()
        for ticker, price in prices.items():
            self._cache.update(ticker=ticker, price=price)
        await asyncio.sleep(self._interval)   # ~0.5s
```

## Behavior Notes

- **No negative prices** — GBM is multiplicative (`exp(...)` is always positive).
- **Right per-tick scale** — the tiny `dt` produces sub-cent moves per tick, aggregating into realistic intraday ranges. TSLA (`σ=0.5`) shows visibly larger swings than JPM (`σ=0.18`).
- **Cholesky rebuild is O(n²)** — fine for `n < 50`. If the watchlist ever grows to hundreds of tickers, cache the L matrix and update it incrementally.
- **Determinism**: pass a seeded `np.random.default_rng(42)` to reproduce a run. The shock-event `random.*` calls also seed off `random.seed(...)` if the test needs full determinism.
- **Shock frequency**: 0.001/tick × 10 tickers × 2 ticks/sec ≈ 1 event every ~50 s. Tuning knob: `EVENT_PROB`.
- **Adding a ticker mid-session** rebuilds the correlation matrix and the Cholesky factor. Existing tickers keep their prices; the new ticker enters at its seed (or a random value in `$50–$300` if unknown).

## File Structure

```
backend/
  app/
    market/
      simulator_core.py    # GBMSimulator class (sync math)
      simulator.py         # SimulatorDataSource (async wrapper — see MARKET_INTERFACE.md)
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS constants
```

Splitting the math (`simulator_core.py`) from the async wrapper (`simulator.py`) keeps GBM unit-testable without any asyncio scaffolding.

## Testing

| Test | What it verifies |
|------|------------------|
| `test_seed_prices_used` | New simulator initializes tickers at their seed values |
| `test_unknown_ticker_random_price` | Adding an unknown ticker gets a price in `$50–$300` |
| `test_step_returns_all_tickers` | Every step produces one price per active ticker |
| `test_prices_stay_positive` | 10,000 steps never yield a non-positive price |
| `test_deterministic_under_seed` | Two simulators with the same seeded RNG produce identical paths |
| `test_correlation_matrix_psd` | The generated correlation matrix decomposes via Cholesky |
| `test_add_ticker_preserves_others` | Adding mid-run doesn't reset existing prices |
| `test_remove_ticker_evicts` | Remove drops the ticker from `step()` output |
| `test_shock_probability` | Over many steps, shock frequency ≈ `EVENT_PROB × n_tickers` |
| `test_high_vol_ticker_moves_more` | Empirical variance of TSLA returns > JPM returns over many steps |
