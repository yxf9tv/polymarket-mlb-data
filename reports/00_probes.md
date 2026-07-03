# Probe Report — 00_probes

Run date: 2026-07-02 (UTC). Script: `analysis/ingest/probe.py` (runnable per-probe, e.g. `.venv/bin/python3 analysis/ingest/probe.py P2`). All numbers below are verbatim from live responses captured in this run; nothing is estimated. Supplementary checks that were discovered mid-run (retention floors, param bugs) were executed via the same probe helpers and are baked back into `probe.py` for reproducibility.

Total API calls this run: ~100 (including ~10 wasted retries on an intel-568 400-param bug, since fixed).

Reference markets used throughout:

| Label | Event / market | condition_id | Notes |
|---|---|---|---|
| earliest_2022 | `mlb-who-will-win-yankees-vs-red-sox-scheduled-for-april-10-708-pm-et` (2022-04-10) | `0x1e3d4c84...ed7f8f1` | Gamma volume $17,462.70, AMM era |
| sept_2024 | `mlb-dodgers-vs-padres` (2024-09-26 playoffs) | `0x4c396f3b...c1d585` | Gamma volume $17,989.44 |
| april_2026 | `mlb-lad-wsh-2026-04-03` moneyline | `0x21f4ad71...a3269c4` | Event volume $1,029,872.99 |
| busy_2026 | `will-the-colorado-rockies-win-the-2026-world-series` | `0x190d98a8...496fa0` | Volume $1,735,827.25, still open |

---

## P1 — Earliest MLB season

**Tested:** intel 574 `market_slug=mlb, closed=True` (3 pages) + retention-floor checks with `end_date_max`; Gamma `/events?series_id=3` (the MLB series, id=3) ordered by `id` and by `creationDate`, plus per-season counts via `start_date_min/max`.

**Evidence:**
- `P1.intel | rows=600 | winning_outcome_populated=600/600 | earliest_end_date=2025-04-21T22:40:00Z | latest_end_date=2026-05-26T22:30:00Z`
- `P1.intel.pre2024_check | filter=end_date_max=2024-01-01 | rows=0`
- `P1.intel.pre2025_check | filter=end_date_max=2025-01-01 | rows=2 | earliest_end_date=2024-07-16T12:00:00Z | latest_end_date=2024-09-26T12:00:00Z`
- Gamma MLB series id=3 exists (`/series?slug=mlb`); earliest event by creationDate: `who-will-win-the-2020-mlb-world-series-1` (2020-10-20). Earliest per-game moneyline events: 2021-09-25 (`in-game-trading-will-the-yankees-or-the-red-sox-win-their-september-25-game`), then regular per-game markets from April 2022 (`mlb-who-will-win-yankees-vs-red-sox-...april-10`).
- Gamma closed events per season (`series_id=3`, response capped at 100/page): 2022=**69**, 2023=**30**, 2024=**0** (by start_date filter — but 2024 events exist, e.g. `mlb-dodgers-vs-padres` 2024-09-26 and `world-series-champion-2024` via tag; many 2023/2024 events carry a garbage default `startDate=2021-01-01T17:00:00Z`, so start_date filters undercount), 2025=**100+ (capped)**, 2026=**100+ (capped)**.
- Gamma quirks found: `order=startDate` is unreliable (garbage defaults); `order=id&ascending=true` tracks creation order; page limit is **100 rows max** regardless of `limit` param; `/markets?slug=` exact-match returns `[]` for MLB game markets (works via `/events?slug=<event_slug>` instead); legacy offset endpoints send `deprecation: true` + `sunset: 2026-05-01` headers with `warning: use /events/keyset` — `/events/keyset` and `/markets/keyset` both verified working.

**Verdict: GO (PARTIAL for intelligence API).** Earliest MLB on Polymarket: futures Oct 2020, per-game markets 2021-09-25, regular per-game coverage from April 2022 — Gamma only. The intelligence API 574 slug-search retention floor is **mid-2024** (2 markets in 2024, dense from 2025-04-21). 2023 season is nearly dead on Polymarket (30 events, sampled market volume=0); 2024 is thin (playoffs mostly); real density starts 2025.

**Chosen source:** Gamma (`/events?series_id=3`, keyset pagination) for full-history market discovery incl. resolution prices and clobTokenIds; intel 574 for `winning_outcome` on 2025+ markets. **Fallback:** intel 574 with `end_date_min/max` season windows (2025+ only).

---

## P2 — Multi-year trade retention

**Tested:** intel 556 (`proxy_wallet=ALL`, `condition_id`, window 2020-01-01→now) and Data-API `/trades?market=<conditionId>` for the 2022, 2024, 2026 reference markets and a 2023 daily.

**Evidence:**
- 2022 market (`0x1e3d4c84...`, $17.5k volume): intel `rows=0, has_more=False`; Data-API `rows=0` (HTTP 200). Trades existed (nonzero volume) but are in **neither** source — AMM/FPMM-era trading predates the CLOB trade feed.
- 2023 daily (`mlb-wsh-pit-2023-04-29`, `0x74605387...`): Gamma `volume=0`; intel `rows=0`; Data-API `rows=0`. Not a retention failure — the market never traded.
- 2024 playoffs (`0x4c396f3b...`): intel `rows=160, earliest=2024-09-26T17:15:50Z, latest=2024-09-27T06:36:11Z, has_more=False`; Data-API `rows=75`, same first/last timestamps (2024-09-26T17:15:50Z → 2024-09-27T06:36:11Z). Row-count differs (160 vs 75): the two feeds count fills differently (intel appears to emit both maker+taker legs) — reconcile during ingestion.
- April 2026 (`0x21f4ad71...`): intel `rows=200 (page 1), has_more=True, ts 2026-04-03T21:14:55Z→23:46:25Z`; Data-API `rows=200, ts 1775239853 (2026-04-03T18:10:53Z) → 1775259985 (2026-04-03T23:46:25Z)`.

**Verdict: GO for 2024→present; NO-GO for 2022–2023 trades.** Old trades DO come back from both sources for the CLOB era (verified to Sept 2024). Pre-CLOB (2022/2023) MLB trade data is unrecoverable from either API — and 2023 had almost no MLB volume anyway. Practical trade corpus: **2024 playoffs + 2025 + 2026 seasons.**

**Chosen source:** intel 556 `proxy_wallet=ALL` per condition_id (richer fields: token_id, side, slug). **Fallback:** Data-API `/trades?market=` (no auth, no rate-limit pain, same timestamps; fewer rows/fill-aggregated — fine for census, cross-validate counts on a sample).

---

## P3 — ALL-wallet pulls

**Tested:** intel 556 `proxy_wallet="ALL"` + `condition_id=<busy_2026>` (Rockies WS futures), 2 pages.

**Evidence:**
- `P3.intel.page1 | rows=200 | distinct_wallets_page1=67 | has_more=True`
- `P3.intel.page2 | rows=200 | distinct_wallets_page2=71 | new_wallets_vs_page1=57`

**Verdict: GO.** ALL-wallet per-market pulls work and keep surfacing new wallets page after page (57 new wallets on page 2) — this kills the leaderboard survivorship bias in the current trader list.

**Chosen source:** intel 556 ALL+condition_id. **Fallback:** Data-API `/trades?market=` (includes `proxyWallet` per trade — verified in P2 sample records).

---

## P4 — Candle retention

**Tested:** intel 568 1h candles and CLOB `/prices-history` for outcome tokens of the resolved april_2026, sept_2024, and earliest_2022 markets.

**Evidence:**
- **Bug found:** intel 568 returns HTTP 400 unless BOTH `start_time` and `end_time` are provided (repo docs say optional — they are not). Fixed in probe.py.
- intel 568, april_2026 token, window ±1 day: `rows=44 hourly candles, 2026-04-02T00:00:00Z → 2026-04-03T23:00:00Z` with OHLC + mean + bid/ask high/low + outcome + condition_id fields.
- intel 568, sept_2024 token (2024-09-25→28): `rows=0`.
- intel 568, 2022 token: `rows=0`.
- **CLOB quirk found:** `interval=max` only works for ACTIVE markets (busy_2026 live token: 4,301 points, 2026-06-02T02:30:05Z → 2026-07-02T02:26:03Z); resolved tokens need explicit `startTs/endTs` + `fidelity`.
- CLOB `startTs/endTs&fidelity=60`, april_2026 token: `points=35, 2026-04-01T01:00:52Z → 2026-04-03T21:00:14Z, price 0.62 → 0.995`.
- CLOB same form, sept_2024 token: `points=15`.
- CLOB same form, 2022 token: `points=0`.

**Verdict: GO for 2024→present (CLOB) and 2026/recent (intel); NO-GO for 2022.** Price history exists for the CLOB era. Coverage: CLOB reaches back at least to Sept 2024 at hourly fidelity; intel 568 covers recent (2026) markets with richer OHLC+bid/ask candles but returned nothing for Sept 2024 — its candle retention floor sits somewhere between Oct 2024 and Apr 2026 (bracket it during ingestion; markets probed here are the endpoints).

**Chosen source:** CLOB `/prices-history` (no auth, full CLOB-era reach) for the multi-season backbone; intel 568 1m/1h candles for 2026 signal markets where bid/ask candles matter. **Fallback:** each is the other's fallback.

---

## P5 — Orderbook history

**Tested:** intel 572 for the resolved april_2026 moneyline token, `start_time/end_time` in **milliseconds** spanning game day 2026-04-03 → 04-04 (ms epoch confirmed correct — matches `scripts/poll_live.py::fetch_orderbook`).

**Evidence:**
- `P5.intel.orderbook | rows=200 | sample_bid_depths=[73, 70, 71] | has_more=True` — 200+ historical snapshots exist for a single game day, each with ~70+ bid levels.

**Verdict: GO.** Historical orderbook snapshots for resolved markets exist, are dense (>200/day), and deep (~70 levels). This unlocks the depth-imbalance backtest (H5). No public fallback exists (CLOB book is live-only) — intel 572 is the only source, as the plan anticipated.

**Chosen source:** intel 572 (only option). **Fallback:** none for history; forward-only capture via existing poller if retention proves shallow for older games (retention depth for 2025 games not yet probed — cheap to check when designing the sample study).

---

## P6 — Pagination depth

**Tested:** intel 556 ALL+busy_2026 paginated 10 pages (offset 0→1800); Data-API `/trades` same market, offsets 0→1800.

**Evidence:**
- intel: `pages_fetched=10 | total_rows=2000 | distinct_ids=2000 | duplicate_rows=0 | final_has_more=True` — every page full, zero duplicate trade ids, `has_more` still true past offset 1800.
- Data-API: 10 pages × 200 rows = `total_rows=2000`, all HTTP 200, no cap encountered at offset 1800.

**Verdict: GO.** Both sources paginate cleanly past 1,000 rows with no observed hard cap and no duplication. (Neither was pushed past offset 1800 — if a busy market exceeds ~10k trades, verify deeper offsets once during ingestion; time-slicing by `start_time/end_time` is the ready fallback.)

---

## P7 — MLB schedule

**Tested:** `GET statsapi.mlb.com/api/v1/schedule?sportId=1` for 2026-06-30→2026-07-01 and 2023-06-15.

**Evidence:**
- current window: `HTTP 200, total_games=29, sample_gamePk=824819, sample_gameDate=2026-06-30T22:35:00Z` (UTC first pitch present).
- 2023-06-15: `HTTP 200, total_games=10, sample_gamePk=717753, sample_gameDate=2023-06-15T17:05:00Z`.

**Verdict: GO.** `gamePk` and UTC `gameDate` present for both current and deep-historical dates. Free, no auth, no issues.

**Chosen source:** statsapi.mlb.com. **Fallback:** infer first pitch from candle volume spikes (not needed).

---

## P8 — Settlement cross-check

**Tested:** intel 574 `winning_outcome` vs Gamma `outcomePrices` winner for 3 resolved markets.

**Evidence:**
- earliest_2022: intel=`None` (market absent — pre-retention-floor), Gamma winner=`Red Sox` (outcomePrices `["0.0000007633...","0.9999992366..."]` — AMM-era prices settle near, not exactly, 1; extraction must use argmax>0.99, not `p=="1"`). **agree=n/a (intel has no record)**
- april_2026_moneyline: intel=`Los Angeles Dodgers`, Gamma=`Los Angeles Dodgers` → **agree=True**
- sept_2024_playoffs: intel=`Dodgers`, Gamma=`Dodgers` → **agree=True** (note: outcome LABELS differ in style between eras/sources — "Dodgers" vs "Los Angeles Dodgers" across markets; normalize team names during ingestion).

**Verdict: GO.** Both sources agree wherever both have the market (2/2). Gamma additionally covers the pre-2024 era intel lacks.

**Chosen source:** Gamma `outcomePrices` (argmax > 0.99) as primary settlement ground truth (full history); intel 574 `winning_outcome` as cross-validation for 2025+. Spot-check ~10 settlements vs statsapi final scores before Phase 1 (per plan).

---

## Summary matrix for Phase 0 ingestion design

| Dataset | Primary source | Fallback | Historical floor (verified) |
|---|---|---|---|
| Markets + settlement | Gamma events (`series_id=3`, keyset) | intel 574 (2025+) | Oct 2020 (futures) / Apr 2022 (per-game) |
| Trades (census) | intel 556 `ALL`+cid | Data-API `/trades?market=` | Sept 2024 (CLOB era; 2022–23 unrecoverable, 2023 ~zero volume anyway) |
| Price history | CLOB `prices-history` (startTs/endTs+fidelity!) | intel 568 (start+end REQUIRED; recent-only) | Sept 2024 (CLOB); 2026 confirmed (intel) |
| Orderbook history | intel 572 (ms timestamps) | none (forward capture) | Apr 2026 confirmed; older unprobed |
| Schedule / first pitch | statsapi.mlb.com | candle-spike inference | 2023 confirmed, effectively all years |

**Bottom-line scope revision:** the practical multi-season dataset is **2024 playoffs + 2025 + 2026** (trades/prices). 2022/2023 markets + settlements are ingestable from Gamma for market-level stats only (thin: 69 + 30 events, mostly low/zero volume) — no trade-level or price-level analysis is possible there.
