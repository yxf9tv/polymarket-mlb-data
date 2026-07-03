# Poll Speedup Plan

Goal: get `poll_live.py` from ~16 minutes to ~30 seconds so the dashboard can stay consistently fresh.

Measured baseline (poll on 2026-07-01, 134 clean traders):
- Trade fetches: ~13 min — 134 sequential HTTP calls in `fetch_trader_trades()` (~5-6s each)
- Orderbooks: ~148s — 108 sequential token fetches in `main()` step 5
- Market discovery: ~30s — 3 slug searches × up to 5 pages in `fetch_open_mlb_markets()`
- Sentiment computation: seconds (local JSON, not a bottleneck)

## 1. Parallelize API calls (biggest win, do first)

In `scripts/poll_live.py`, wrap the two sequential network loops in a
`concurrent.futures.ThreadPoolExecutor` (start with ~10 workers, tune down if 429s appear):
- `fetch_trader_trades()` — the per-trader loop (one `api_call` per wallet). Each task fetches
  and writes its own `mtrades_<wallet>.json` cache file (distinct files, so no write contention).
- `main()` step 5 — the per-token orderbook loop (`fetch_orderbook` per outcome token).

Notes:
- `api_call()` already handles 429 with exponential backoff, so rate-limit pressure degrades
  gracefully. Keep per-request `timeout=60`.
- Preserve the summary counters (total trades, markets touched) by collecting results from
  futures rather than mutating shared state inside workers.

Expected: trade phase 13 min → ~1 min; orderbooks 148s → ~15s. Total ~2 min.

## 2. Fetch orderbooks only for dashboard-relevant markets

Poll fetched 108 orderbooks but only 9 active markets had trader data. In `main()` step 5,
restrict `active_cids` to markets that are BOTH active AND have sentiment/trader data
(i.e., the ones the depth bars actually render for). ~90% fewer orderbook calls.

## 3. Incremental trade polling

Currently every poll re-fetches 7 days of trades per trader. Instead:
- Store the last successful poll timestamp (e.g., in `data/poll_state.json`, or reuse
  `polled_at` already written into each `mtrades_<wallet>.json`).
- On subsequent polls, request trades with `start_time = last_poll` (minus a small overlap
  buffer, e.g. 10 min) and MERGE new trades into the existing per-wallet cache instead of
  overwriting — dedupe on trade `id` (present in trade records), keep the newest 50
  `mlb_trade_details` and a rolling 7-day window.
- Fall back to the full 7-day fetch when a wallet has no cache or the cache is stale (>7d).

Expected: refresh polls drop to ~20-30s (tiny responses per trader).

Optional experiment before committing: the trades endpoint accepts `proxy_wallet: "ALL"` —
one paginated time-windowed query for ALL trades since last poll, filtered locally to the
134 tracked wallets + MLB slugs, could replace all 134 per-wallet calls. Test how many pages
a 5-minute window of platform-wide volume actually is before adopting.

## 4. Cache market discovery

`fetch_open_mlb_markets()` re-runs 3 paginated slug searches every poll. Cache the result
(e.g., `data/cache/open_markets.json` with a fetched-at timestamp) and reuse it if younger
than ~30-60 min. Open MLB markets don't change minute-to-minute.

## Later (discussed, not in scope yet)

5. Background refresh loop in `server.py` (asyncio task on startup, poll every ~5 min) so the
   dashboard stays fresh without clicking "Poll Now". Do this only after polls are fast.

## Verification

- Time a full cold poll and a warm incremental poll (`time .venv/bin/python3 scripts/poll_live.py`).
- Compare `live_signals.json` from parallel vs sequential runs on the same cache state —
  sentiment numbers should match exactly (order-independent aggregation).
- Watch the log for `[rate-limited...]` lines; if frequent, lower worker count.
- Confirm the dashboard still renders: `/api/live` fresh age, games/consensus tabs populated.

## Context for a fresh session

- This repo is a clone of `~/projects/sentiment2` with bug fixes already applied
  (see git-less diff summary in `~/.claude/plans/hi-please-familiarize-yourself-rippling-token.md`):
  tracker.py ZoneInfo fix, game-level unique_traders rollup fix (compute_sentiment.py +
  poll_live.py), app.js poll-wait fix, server.py poll timeout 300→900s, API docs param
  correction (`proxy_wallet`, not `wallet_proxy`).
- Venv: `.venv` (Python 3.14), deps in `requirements.txt`. API key in `.env`
  (`intelligence_api_key`). Run server: `./run_dashboard.sh` (port 8000).
- A dashboard-triggered poll (`/api/live/poll`) is subprocess-based with a 900s timeout —
  full 16-min polls exceed it; run `scripts/poll_live.py` directly until the speedups land.
