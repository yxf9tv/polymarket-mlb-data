#!/usr/bin/env python3
"""Ingest per-token price-history candles for the CLOB-era (season>=2024)
resolved, volume>0 market universe (same 30,289-market target list as
ingest_trades.py, via targets.load_target_markets()).

Primary source: CLOB `/prices-history` (no auth, public, fast - probe P4),
one call per clobTokenId with explicit `startTs`/`endTs` + `fidelity=60`
(minute candles). `interval=max` only works for still-ACTIVE markets, not
resolved ones - probe P4 finding.

Window: end_date minus 13 days -> end_date plus 1 day (14 days total).
The task spec said "end_date minus 7d -> end_date", but the smoke test
found Gamma `end_date` semantics vary by era, and the spec window returns
ZERO or near-zero points for most markets:
  * 2025-era per-game markets: end_date = 12:00 noon UTC of game day, while
    the game trades in the evening and settlement trades run into the next
    morning (probe P2 saw trades 18h past end_date) -> a window ENDING at
    end_date misses the game entirely. Hence the +1d pad.
  * 2026-era per-game markets: end_date = scheduled first pitch + 7 days
    exactly (e.g. mlb-nym-lad-2026-04-13 -> end_date 2026-04-21T02:10Z)
    -> "end_date - 7d" lands exactly AT first pitch and misses all
    pre-game trading. Hence 13d back instead of 7d.
The 14-day window covers both semantics: full pre-game week + game +
settlement for 2026, and ~2 pre-game weeks + game for 2025.
More smoke-test findings: (a) CLOB rejects windows longer than 15 days at
fidelity=60 with HTTP 400 "'startTs' and 'endTs' interval is too long"
(boundary probed: 15d OK, 16d fails) - 14d stays under the cap with margin;
(b) futures markets (e.g. eliminated-team World Series longshots) stop
trading weeks/months before end_date, so any end_date-anchored window
returns ~0 points for them by construction - a full-lifetime sliced pull
would be needed if futures candles matter (documented in the report, not
built here).

Secondary source, season-2026 markets ONLY: intelligence API agent 568,
which returns richer OHLC + bid/ask candles but (a) 400s unless BOTH
start_time AND end_time are provided (probe P4 finding - despite docs
calling them optional) and (b) its retention floor sits somewhere between
Oct 2024 and Apr 2026 (probe P4: 0 rows for a Sept-2024 token, 44 rows for
an Apr-2026 token) - so it's only worth calling for 2026 markets, per the
task scope.

Both sources' raw records are archived to the SAME file per market:
data/lake/raw/candles/<season>/<condition_id>.jsonl - each record is
enriched with `token_id`/`condition_id`/`source` (`"clob"` or `"intel"`)
before being written, so build_lake.py can tell them apart and both point
formats are self-describing (CLOB points are {t, p, ...}; intel points
already carry condition_id/token_id but not `source`).

CLOB is public and comparatively fast, so this job defaults to more workers
(10) than the trades job's 6.

Usage:
    .venv/bin/python3 analysis/ingest/ingest_candles.py --seasons 2024,2025 --limit-markets 10
    .venv/bin/python3 analysis/ingest/ingest_candles.py                 # full sweep
    .venv/bin/python3 analysis/ingest/ingest_candles.py --status
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from client import (
    IngestState,
    append_jsonl,
    clob_get,
    intel_call,
    load_api_key,
    AGENT_CANDLES,
)
from targets import load_target_markets, parse_limit_markets_arg, parse_seasons_arg, parse_workers_arg

JOB = "candles"
DEFAULT_WORKERS = 10
FIDELITY_MIN = 60  # minute candles
CANDLE_WINDOW_DAYS = 13  # startTs = end_date - 13d (see module docstring: covers both end_date eras)
END_PAD_DAYS = 1  # endTs = end_date + 1d (total window 14d, under the 15d CLOB fidelity=60 cap)
# intel 568 only worth calling for 2026+ (probe P4 retention floor). Override to
# 9999 via env to run CLOB-only (e.g. while the trades sweep owns the intel rate limit).
INTEL_CANDLE_SEASON_MIN = int(os.environ.get("INTEL_CANDLE_SEASON_MIN", "2026"))
SLEEP_S = 0.05  # CLOB is public+fast


def _parse_iso(date_str) -> datetime | None:
    if not isinstance(date_str, str) or not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_token_ids(token_ids_str) -> list[str]:
    if not isinstance(token_ids_str, str) or not token_ids_str:
        return []
    return [t for t in token_ids_str.split(",") if t]


class CandleFailure(Exception):
    pass


def fetch_clob_candles(token_id: str, cid: str, season: int, start_ts: int, end_ts: int) -> int:
    data = clob_get("/prices-history", {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": FIDELITY_MIN})
    if data is None:
        raise CandleFailure(f"CLOB prices-history request failed for token {token_id[:16]}...")
    history = data.get("history", []) if isinstance(data, dict) else None
    if not history:
        return 0
    enriched = [{**point, "token_id": token_id, "condition_id": cid, "source": "clob"} for point in history]
    append_jsonl(JOB, f"{season}/{cid}", enriched)
    return len(enriched)


def fetch_intel_candles(token_id: str, cid: str, season: int, start_ts: int, end_ts: int, api_key: str) -> int:
    """Paginate intel 568 1h candles for one token (a 14-day window can hold
    up to 336 hourly candles per token, i.e. more than one 200-row page)."""
    offset = 0
    n_rows = 0
    while True:
        resp = intel_call(
            AGENT_CANDLES,
            {"token_id": token_id, "interval": "1h", "start_time": str(start_ts), "end_time": str(end_ts)},
            {"limit": 200, "offset": offset},
            api_key,
        )
        if resp is None:
            raise CandleFailure(f"intel 568 request failed for token {token_id[:16]}...")
        results = resp.get("data", {}).get("results", [])
        if results:
            enriched = [{**r, "source": "intel"} for r in results]
            append_jsonl(JOB, f"{season}/{cid}", enriched)
            n_rows += len(results)
        if not results or not resp.get("pagination", {}).get("has_more", False):
            return n_rows
        offset += 200
        time.sleep(SLEEP_S)


def process_market(row: dict) -> dict:
    """Fetch + archive candles for all of one market's outcome tokens.
    Returns a result dict (never raises)."""
    cid = row["condition_id"]
    season = int(row["season"])
    token_ids = _parse_token_ids(row.get("token_ids"))
    end_dt = _parse_iso(row.get("end_date"))
    t0 = time.time()
    api_key = row["_api_key"]

    if not token_ids or end_dt is None:
        return {
            "condition_id": cid, "season": season, "rows": 0, "n_tokens": 0,
            "ok": False, "error": "missing token_ids or end_date", "elapsed_s": round(time.time() - t0, 2),
        }

    start_dt = end_dt - timedelta(days=CANDLE_WINDOW_DAYS)
    padded_end_dt = end_dt + timedelta(days=END_PAD_DAYS)
    start_ts, end_ts = int(start_dt.timestamp()), int(padded_end_dt.timestamp())

    n_rows = 0
    try:
        for token_id in token_ids:
            n_rows += fetch_clob_candles(token_id, cid, season, start_ts, end_ts)
            time.sleep(SLEEP_S)
            if season >= INTEL_CANDLE_SEASON_MIN:
                n_rows += fetch_intel_candles(token_id, cid, season, start_ts, end_ts, api_key)
                time.sleep(SLEEP_S)
        ok, error = True, None
    except CandleFailure as e:
        ok, error = False, str(e)

    return {
        "condition_id": cid, "season": season, "rows": n_rows, "n_tokens": len(token_ids),
        "ok": ok, "error": error, "elapsed_s": round(time.time() - t0, 2),
    }


def run_sweep(seasons: list[int] | None, limit: int | None, workers: int) -> None:
    state = IngestState(JOB)
    api_key = load_api_key()

    targets_df = load_target_markets(seasons=seasons, limit=limit)
    total_target = len(targets_df)
    todo = targets_df[~targets_df["condition_id"].apply(state.is_done)]
    print(f"Target universe: {total_target} markets ({seasons or 'all seasons>=2024'}, limit={limit}). "
          f"Already completed: {total_target - len(todo)}. Remaining: {len(todo)}. Workers: {workers}.")

    if todo.empty:
        print("Nothing to do - all target markets already completed.")
        return

    jobs = []
    for _, r in todo.iterrows():
        d = r.to_dict()
        d["_api_key"] = api_key
        jobs.append(d)

    done_count = total_target - len(todo)
    total_rows_this_run = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_market, j): j["condition_id"] for j in jobs}
        for fut in as_completed(futures):
            result = fut.result()
            cid = result["condition_id"]
            if result["ok"]:
                state.mark_done(cid)
                state.set(f"rows__{cid}", result["rows"])
                state.set("cum_rows", state.get("cum_rows", 0) + result["rows"])
                state.set("cum_markets", state.get("cum_markets", 0) + 1)
                state.set("cum_elapsed_s", state.get("cum_elapsed_s", 0.0) + result["elapsed_s"])
                state.save()
                done_count += 1
                total_rows_this_run += result["rows"]
                print(f"  [{done_count}/{total_target}] {cid[:14]}... season={result['season']} "
                      f"tokens={result['n_tokens']} rows={result['rows']} elapsed={result['elapsed_s']}s")
            else:
                print(f"  [FAILED] {cid[:14]}... season={result['season']} error={result['error']} "
                      f"(will retry on next run - not marked complete)")

    wall_s = time.time() - t_start
    n_run = len(jobs)
    print(f"\nRun summary: {n_run} markets attempted, {total_rows_this_run} candle rows archived, "
          f"wall_clock={wall_s:.1f}s, avg_per_market={wall_s / max(n_run, 1):.2f}s "
          f"(with {workers} workers)")


def print_status() -> None:
    state = IngestState(JOB)
    targets_df = load_target_markets()
    total = len(targets_df)
    completed = sum(1 for cid in targets_df["condition_id"] if state.is_done(cid))
    cum_rows = state.get("cum_rows", 0)
    cum_markets = state.get("cum_markets", 0)
    cum_elapsed = state.get("cum_elapsed_s", 0.0)
    avg_s = cum_elapsed / cum_markets if cum_markets else None
    remaining = total - completed
    print(f"candles ingest status: {completed}/{total} markets complete ({100.0 * completed / total:.1f}%)")
    print(f"  rows archived so far (this state file's lifetime): {cum_rows}")
    if avg_s is not None:
        est_remaining_wall_s = avg_s * remaining / DEFAULT_WORKERS
        print(f"  avg time/market (serial): {avg_s:.2f}s -> est. remaining wall-clock at {DEFAULT_WORKERS} workers: "
              f"{est_remaining_wall_s / 3600:.1f}h ({remaining} markets left)")
    else:
        print("  no completed markets yet - run the sweep to get timing data")


def main() -> None:
    args = sys.argv[1:]
    if "--status" in args:
        print_status()
        return
    seasons = parse_seasons_arg(args)
    limit = parse_limit_markets_arg(args)
    workers = parse_workers_arg(args, DEFAULT_WORKERS)
    run_sweep(seasons, limit, workers)


if __name__ == "__main__":
    main()
