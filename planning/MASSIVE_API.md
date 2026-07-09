# Massive API Reference

Reference documentation for the [Massive](https://massive.com/) REST API (formerly Polygon.io) as used in FinAlly for real-time and end-of-day stock prices.

## Background

- **Rebrand**: Polygon.io was rebranded to **Massive** in October 2025. The Python client, docs, and DNS all now live under `massive.com`.
- **Backwards compatibility**: `api.polygon.io` still resolves for legacy integrations; existing API keys continue to work unchanged. New code should target `api.massive.com`.
- **Coverage**: US stocks, options, indices, forex, crypto, futures. FinAlly only uses the **stocks** endpoints.

## Setup

| | |
|---|---|
| Base URL | `https://api.massive.com` |
| Python package | `massive` |
| Install | `uv add massive` (or `pip install -U massive`) |
| Min Python | 3.9+ |
| Auth env var | `MASSIVE_API_KEY` |
| Auth header (raw REST) | `Authorization: Bearer <API_KEY>` (client handles this automatically) |

### Client Initialization

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from environment automatically
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")
```

## Rate Limits

| Tier | Limit |
|------|-------|
| Free | 5 requests/minute |
| Starter and above | Unlimited (stay under ~100 req/s as a courtesy) |

FinAlly polls on a timer. **Free tier**: poll every **15 seconds**. **Paid**: 2–5 seconds. Because the snapshot endpoint returns *all* requested tickers in a single call, one poll per interval covers the whole watchlist.

## Endpoints Used in FinAlly

### 1. Multi-Ticker Snapshot (Primary)

Returns the current minute, day, previous day OHLC, and last trade/quote for a list of tickers — in one call. This is the workhorse for FinAlly's live price polling.

**REST**:
```
GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
```

**Query params**:
| Name | Type | Notes |
|------|------|-------|
| `tickers` | comma-separated string | Case-sensitive. Omit for all tickers (huge response). |
| `include_otc` | boolean | Include OTC securities. Default `false`. |

**Python client**:
```python
from massive import RESTClient
from massive.rest.models import TickerSnapshot, Agg

client = RESTClient()

# Positional args: market ("stocks"), then list of tickers
snapshot = client.get_snapshot_all("stocks", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"])

for item in snapshot:
    if isinstance(item, TickerSnapshot):
        print(f"{item.ticker}: ${item.last_trade.price}")
        print(f"  Day change: {item.todays_change_perc:.2f}%")
        if isinstance(item.prev_day, Agg):
            print(f"  Prev close: {item.prev_day.close}")
```

**REST response shape** (top level):
```json
{
  "status": "OK",
  "count": 5,
  "tickers": [
    {
      "ticker": "AAPL",
      "updated": 1675190399000,
      "todaysChange": -4.54,
      "todaysChangePerc": -3.50,
      "fmv": 125.10,
      "day":      { "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07, "v": 111237700, "vw": 127.35 },
      "min":      { "t": 1675190340000, "o": 125.04, "h": 125.10, "l": 125.03, "c": 125.07, "v": 5000, "vw": 125.06 },
      "prevDay":  { "o": 128.00, "h": 130.00, "l": 127.50, "c": 129.61, "v": 90000000, "vw": 128.90 },
      "lastTrade":{ "t": 1675190399000, "x": 4, "p": 125.07, "s": 100, "c": [12], "i": "abc" },
      "lastQuote":{ "t": 1675190399500, "P": 125.08, "S": 1000, "p": 125.06, "s": 500 }
    }
  ]
}
```

**Field key** (the REST payload uses single-letter keys; the Python client deserializes them to descriptive attributes):

| Raw JSON | Python attribute | Meaning |
|----------|------------------|---------|
| `p` (in lastTrade) | `last_trade.price` | Last trade price |
| `s` (in lastTrade) | `last_trade.size` | Last trade size |
| `t` | `last_trade.timestamp` | Unix ms |
| `o`, `h`, `l`, `c`, `v`, `vw` | `.open`, `.high`, `.low`, `.close`, `.volume`, `.vwap` | Standard OHLCV |
| `todaysChange` | `todays_change` | Absolute change since prev close |
| `todaysChangePerc` | `todays_change_perc` | Percent change since prev close |

**Fields FinAlly extracts** (per ticker):
- `last_trade.price` → current price
- `prev_day.close` → previous close (for day-change calc)
- `todays_change_perc` → daily change % (or compute from the two above)
- `last_trade.timestamp` → when the price was recorded (ms since epoch)

### 2. Single Ticker Snapshot

For detailed data on one ticker (e.g., when the user clicks a ticker for the detail view).

**REST**:
```
GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}
```

**Python client**:
```python
snap = client.get_snapshot_ticker("stocks", "AAPL")

print(f"Price: ${snap.last_trade.price}")
print(f"Bid/Ask: ${snap.last_quote.bid_price} / ${snap.last_quote.ask_price}")
print(f"Day range: ${snap.day.low} - ${snap.day.high}")
```

### 3. Previous Close

Previous trading day's OHLC bar for one ticker. Useful for seed prices and day-change baselines when the snapshot is unavailable.

**REST**:
```
GET /v2/aggs/ticker/{ticker}/prev
```

**Python client**:
```python
aggs = client.get_previous_close_agg("AAPL")

for a in aggs:
    print(f"O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

**Response**:
```json
{
  "ticker": "AAPL",
  "results": [
    {"o": 150.0, "h": 155.0, "l": 149.0, "c": 154.5, "v": 1000000, "t": 1672531200000}
  ]
}
```

### 4. Aggregates (Bars)

Historical OHLCV bars over a date range. Not needed for live polling, but useful for the detail chart if we ever want backfilled history.

**REST**:
```
GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
```

**Python client** — `list_aggs` is a paginated iterator:
```python
aggs = []
for a in client.list_aggs(
    "AAPL",
    1,           # multiplier
    "minute",    # timespan: minute, hour, day, week, month, quarter, year
    "2024-01-01",
    "2024-01-31",
    limit=50000,
):
    aggs.append(a)

for a in aggs:
    print(f"t={a.timestamp} O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

### 5. Last Trade / Last Quote

Individual endpoints for the most recent trade or NBBO quote. Cheaper than a full snapshot if we only care about one ticker's last price.

```python
trade = client.get_last_trade("AAPL")
print(f"Last trade: ${trade.price} x {trade.size} at t={trade.timestamp}")

quote = client.get_last_quote("AAPL")
print(f"Bid: ${quote.bid_price} x {quote.bid_size}")
print(f"Ask: ${quote.ask_price} x {quote.ask_size}")
```

## How FinAlly Uses the API

A single background task polls the multi-ticker snapshot endpoint on a fixed interval:

```python
import asyncio
from massive import RESTClient
from massive.rest.models import TickerSnapshot

async def poll_massive(client: RESTClient, get_tickers, price_cache, interval: float = 15.0):
    """Poll Massive snapshot endpoint and update the shared price cache."""
    while True:
        tickers = get_tickers()
        if tickers:
            # Massive client is synchronous — run in a thread so we don't block the event loop
            snapshots = await asyncio.to_thread(
                client.get_snapshot_all, "stocks", tickers,
            )
            for snap in snapshots:
                if isinstance(snap, TickerSnapshot) and snap.last_trade:
                    price_cache.update(
                        ticker=snap.ticker,
                        price=snap.last_trade.price,
                        timestamp=snap.last_trade.timestamp / 1000.0,  # ms → s
                    )
        await asyncio.sleep(interval)
```

Design points:
- **One call per poll**, regardless of watchlist size — fits the free tier's 5 req/min limit.
- **Sync client in `asyncio.to_thread`** — the `massive` package is synchronous; wrapping in a thread keeps the FastAPI event loop responsive.
- **`price_cache` is the sole downstream contract** — SSE streaming, portfolio valuation, and trade execution all read from the cache; none of them know whether the data came from Massive or the simulator.

## Error Handling

The client raises exceptions for HTTP errors:

| Status | Meaning | Action |
|--------|---------|--------|
| 401 | Invalid API key | Log and fall back to simulator (or halt) |
| 403 | Endpoint not on this plan | Log; the free tier covers the endpoints we use |
| 429 | Rate limit exceeded | Back off (double the poll interval, cap at 60s) |
| 5xx | Massive server error | Retry with backoff — the client has ~3 built-in retries |

The poll loop should catch exceptions from `get_snapshot_all` and keep running rather than let one failure kill the loop. The cache retains the previous prices, so the UI keeps displaying the last known values while the poller recovers.

## Data Freshness & Market Hours

- **Timestamps** are Unix **milliseconds** in the raw REST payload; the Python SDK preserves that. Convert to seconds when storing.
- **After hours / weekends**: `last_trade.price` reflects the last executed trade. The `day` object may be stale until the next session opens.
- **Pre-market**: `day` values may still be from the prior session; `min` reflects current activity.
- **Delayed data**: Free-tier snapshots are 15-minute delayed. Paid tiers are real-time.

## Notes for FinAlly

- The multi-ticker snapshot is the *only* endpoint the live-price poller needs; everything else is optional.
- Do **not** use WebSockets — the WebSocket API requires a Currencies/Stocks Advanced plan and adds a whole reconnection layer. REST polling is enough given SSE fan-out on the frontend.
- Keep the `massive` client as a top-level import (not lazy) — it is a declared dependency and unconditional import makes failures loud and early.

## Sources

- [Massive homepage](https://massive.com/)
- [Massive Stocks REST API overview](https://massive.com/docs/rest/stocks/overview)
- [Full Market Snapshot endpoint](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot)
- [Single Ticker Snapshot endpoint](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot)
- [Massive Python client on GitHub](https://github.com/massive-com/client-python)
- [Massive + Python blog post](https://massive.com/blog/polygon-io-with-python-for-stock-market-data)
