# Phase 0 Report — Data Lake: Markets & Schedule

Run date: 2026-07-02. Scope: build the Phase 0 ingestion infra (`analysis/ingest/{client,derive,ingest_markets,ingest_schedule,build_lake}.py`) and run the **small** ingestion jobs (Gamma markets sweep + statsapi schedule sweep + parquet build). The big trades/candles/orderbook jobs are **not** run in this task — this report ends with the exact scope handoff for that next job.

## What was ingested

- **Markets/events** — Gamma `/events/keyset`, MLB series (`series_id=3`) **and** the MLB tag (`tag_id=100381`), merged and de-duplicated by event id. Both open and closed markets, no date filter — one full sweep captured 2020 (earliest futures market) through 2027 (long-dated prop end-dates). 97 Gamma API calls total (48 pages for `series_id=3`, 49 for `tag_id=100381`).
- **Schedule** — `statsapi.mlb.com/api/v1/schedule?sportId=1`, one call per season, 2022–2026 (5 calls). Full year range works in a single request (no per-month chunking needed).
- **Settlement cross-check** — 50 intelligence-API (agent 574) calls comparing `winning_outcome` against Gamma's derived winner for a random sample of resolved 2025+ markets.

Total API calls this run: ~152 (well under a "few hundred", as scoped).

### Files written
- `data/lake/markets.parquet` — 42,893 rows (one per Gamma market / `condition_id`)
- `data/lake/events.parquet` — 4,971 rows (one per Gamma event)
- `data/lake/schedule.parquet` — 14,555 rows (one per statsapi `gamePk`)
- `data/lake/raw/markets/events.jsonl` — append-only raw archive (9,600 lines across both sweeps, pre-dedup; 352 MB)
- `data/lake/raw/schedule/<year>.jsonl` — raw statsapi date-buckets, 2022–2026
- `data/lake/state/markets.json`, `data/lake/state/schedule.json` — resumable cursors/completed-years (both sweeps report `sweep_done=true`, safe to rerun as a no-op; will pick up new markets/games on future incremental runs since the cursor state is separate per source and a full re-sweep is one `rm` away)
- `data/lake/state/markets_crosscheck.json` — cross-check result (below)

### Code written (`analysis/ingest/`)
- `client.py` — shared intel-API client (`intel_call`/`intel_paginated`, Bearer auth, 429 backoff copied from `scripts/poll_live.py`), public-API GET helpers (`gamma_get`/`clob_get`/`data_api_get`/`statsapi_get`, 429/retry backoff), `append_jsonl()` raw-page archival, `IngestState` resumable state (completed-id set + arbitrary keyed fields, atomic JSON write).
- `derive.py` — shared settlement-winner / season / market-type derivation, used by both `ingest_markets.py` (cross-check sampling) and `build_lake.py` (parquet construction) so the logic lives in one place.
- `ingest_markets.py` — the two keyset sweeps + cross-check (see Anomalies below for a real bug found in the keyset endpoint).
- `ingest_schedule.py` — per-season statsapi pull, resumable by year.
- `build_lake.py` — raw JSONL → parquet for all three tables; idempotent, safe to rerun anytime after new raw data lands.

## Per-season counts

| Season | Markets | Resolved | % Resolved | Volume ($) | Gamma Events | Schedule Games |
|---|---:|---:|---:|---:|---:|---:|
| 2020 | 1 | 0 | 0.0% | 107,602 | 1 | — |
| 2021 | 17 | 17 | 100.0% | 474,500 | 17 | — |
| 2022 | 134 | 118 | 88.1% | 249,880 | 69 | 2,430 (R) |
| 2023 | 744 | 740 | 99.5% | 158,614 | 56 | 2,430 (R) |
| 2024 | 139 | 139 | 100.0% | 11,113,860 | 63 | 2,430 (R) |
| 2025 | 4,712 | 4,699 | 99.7% | 932,301,100 | 2,488 | 2,430 (R) |
| 2026 | 37,060 | 34,482 | 93.0%¹ | 1,071,356,000 | 2,275 | 2,430 (R, season in progress) |
| 2027² | 86 | 2 | 2.3% | 925,057 | 2 | n/a |
| **Total** | **42,893** | **40,197** | **93.7%** | **≈$2.017B** | **4,971** | **14,555** (all game types, 2022–2026) |

¹ 2026 is the current live season (today is 2026-07-02) — the unresolved 6,578 markets are mostly future-dated games/props not yet played, not a data-quality gap.
² "2027" markets are long-dated prop/futures markets whose Gamma `endDate` falls in early 2027 (e.g. "Will David Ortiz be the next Red Sox manager?", `endDate=2027-02-01`) even though they're 2026-season-tagged MLB content — correctly captured, just season-labeled by resolution date rather than by the MLB season they're about.

Schedule row counts above are regular-season (`gameType='R'`) only, 2,430/year is the standard 30-team 162-game schedule; the schedule table also carries spring training (S), exhibition (E/A), and postseason (D/L/F/W) games (see `game_type` column), 14,555 total games across all types 2022–2026.

Matches the plan's expectation almost exactly: "full history from Oct 2020 (futures) → Apr 2022 (per-game)" for the pre-CLOB era, and confirms probe P1/P2's "practical dataset is 2024 playoffs + 2025 + 2026" for anything trade/price-level, since 2022–2023 are thin (69 + 56 events, mostly low volume) and pre-CLOB.

## Settlement coverage

93.7% of all 42,893 markets have a derived `winning_outcome` (argmax `outcomePrices` > 0.99). The unresolved 6.3% is almost entirely still-open 2026 markets (games/props not yet played) plus a handful of edge cases (e.g. voided/50-50-resolved markets that never cross the 0.99 threshold — expected and correct behavior, not a bug).

## Cross-check: Gamma vs. intelligence-API agent 574

Sampled 50 resolved 2025+ markets (uniform random, seed=42) and compared Gamma's derived winner against intel 574's `winning_outcome`:

```
sample_size=50  agree=50  disagree=0  intel_missing=0
agreement_rate = 100.0%
```

**100% agreement, zero disagreements, zero intel-missing** in this sample. This validates Gamma `outcomePrices` (argmax > 0.99) as the settlement ground truth per the research plan, with intel 574 as a fully consistent cross-check for 2025+.

## Market-type distribution (heuristic classifier, `derive.classify_market_type`)

| Type | Count | Volume ($) |
|---|---:|---:|
| total | 22,540 | 189,495,400 |
| spread | 9,622 | 73,222,160 |
| moneyline | 5,459 | 1,580,600,000 |
| other | 1,741 | 30,680,050 |
| nrfi | 1,637 | 4,533,531 |
| prop | 1,149 | 3,856,179 |
| futures | 745 | 134,299,000 |

This is a slug/question-text heuristic ("guess" per the task spec), not a ground-truth label — most volume concentrates in moneyline (per-game head-to-head, $1.58B) and total (game/inning totals, $189M). The `other` bucket (4.1% of markets, 1.5% of volume) is mostly long-tail props that don't cleanly fit the six named buckets (e.g. player-specific novelty markets like "will X be the next manager") — acceptable residual for a classifier whose job is to bucket, not exhaustively enumerate every prop type.

## Join-rate: markets ↔ schedule (rough, via slug team codes)

92.6% of markets (39,704 / 42,893) use the new-format `mlb-{team1}-{team2}-{YYYY-MM-DD}...` slug (2025+ era; pre-2025 markets use free-text slugs like `mlb-who-will-win-yankees-vs-red-sox-scheduled-for-april-10...` that don't parse this way). Of those slug-parseable markets, joining on `(official_date, both team names present in that date's schedule rows)` via a hand-built team-code→name table:

```
Slug-parseable markets checked: 39,704
Matched to a schedule game:     39,178  (98.7%)
```

The 1.3% unmatched are almost all **±1-day date mismatches** between the market slug's date and statsapi's `officialDate` (spot-checked: `mlb-bos-stl-2025-04-05` slug date vs. the actual Red Sox–Cardinals game on `2025-04-04`/`04-06`) — consistent with late-night games where the slug date and MLB's "official" local game date differ by a day. Not a join failure, a known date-boundary artifact; worth reconciling on `gameDate` (UTC timestamp) rather than `officialDate` in Phase 1 if precise game-level joins are needed.

## Anomalies & quirks found (beyond what probes.md documented)

1. **Gamma `/events/keyset` cursor parameter bug (new finding, not in probes.md).** The response field is `next_cursor`, but the *request* parameter to send it back is `after_cursor` — **not** `next_cursor`, `cursor`, `nextCursor`, or `startCursor`. Sending any of those wrong names is silently ignored: the API returns HTTP 200 with page 1's exact content again, including the *same* `next_cursor` value, forever — no error, looks like a working stuck loop. Confirmed via `cf-cache-status: MISS` + cache-busting params that it's an origin-side bug, not a CDN caching artifact. Found the correct param name (`after_cursor`) by reading `gamma-api.polymarket.com/openapi.json` directly (undocumented anywhere else, including the schema files referenced in API responses). **Fixed in `ingest_markets.py`**; first run before the fix produced 3,300 duplicate rows of the same 100 events (caught immediately via the resumable-state file showing a static cursor, discarded before any parquet build).
2. **Deprecated offset endpoint (`/events?offset=`) still works but hard-caps at offset+limit ≈ 2,000** with `HTTP 422: "offset too large, use /events/keyset for deeper pagination"` — confirms probes.md's "offset endpoints deprecated" but adds the precise cap, and confirms keyset is mandatory (not just recommended) for full-history sweeps of this series (spans thousands of events).
3. **`series_id=3` has a complete gap for 2024.** `mlb-dodgers-vs-padres` (2024-09-26 playoffs, the reference market probes.md used for sept_2024 P2/P4/P8 checks) has `series: None` — never linked to the MLB series object — but does carry `tag_id=100381` ("MLB", created 2024-08-27). Checked adjacent eras: 2022–23 events have the series link but *not* the MLB tag (tag didn't exist yet); 2025–26 events have *both*. This is exactly the "series id=3 misses markets → probe alternatives (tag search)" contingency the research plan anticipated. **Fix:** `ingest_markets.py` runs two independent keyset sweeps (`series_id=3` and `tag_id=100381`), merged and de-duplicated by event id in `build_lake.py`. This recovered all 139 2024 markets (100% resolved) that `series_id=3` alone would have silently missed — validated by checking `mlb-dodgers-vs-padres` is present in the final `markets.parquet` with the correct settlement.
4. **Season-derivation fallback rate is low and expected.** 154/42,893 markets (0.4%) fell back from `market.endDate` to `market.startDate`/`event.endDate` for season assignment (`season_source` column tracks this per-row) — confirms the garbage-startDate caveat from probes.md was correctly avoided by preferring `endDate`.
5. **Token IDs are 100% populated** for every 2024+ (CLOB-era) market (139/139, 4,712/4,712, 37,060/37,060, 86/86) — no gaps for the upcoming price-history job.

## Handoff to the trades job

Per the research plan, the trades census (`ingest_trades.py`, not run in this task) should sweep the CLOB-era resolved markets with actual trading volume:

```
CLOB-era (season >= 2024) resolved markets with volume > 0:  30,289
```

(2024: 138, 2025: 4,366, 2026: 25,783 so far — 2026 will keep growing as the season completes.) This is the market count the next job needs to loop over for per-`condition_id` trade pulls via intel 556 (`proxy_wallet=ALL`) or the Data-API fallback, per probe P2/P3's verdict.
