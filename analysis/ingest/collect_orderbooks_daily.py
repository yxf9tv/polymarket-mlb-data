#!/usr/bin/env python3
"""Forward, day-of orderbook collection for today's active MLB markets with
live trader signals.

WHY THIS EXISTS: intel agent 572 (orderbook history) turned out to have only
a rolling ~3-month retention window (reports/07_edge_rules.md discovered
zero 2025 coverage; floor ~2026-03-26 as of the 2026-07-03 Phase 3 ingest).
Anything not collected while a market is live falls out of the window and is
lost forever -- there is no way to backfill it later. This script is the
forward complement to the one-off Phase 3 analysis/ingest/ingest_orderbooks.py
sweep: run it once a day against TODAY's open MLB markets that already have
a live trader-sentiment signal, so the 2026-forward paper-trading dataset
(scripts/tracker.py's tracked_games.json) doesn't hit the same gap the 2025
backfill did.

Target selection: data/live_signals.json's active_markets with
has_trader_data=True and game_date == today (NY) -- exactly the population
the live dashboard is already generating a signal for. Token ids come from
the same data/cache/mtrades_*.json trade cache scripts/poll_live.py builds
its token_map from (zero extra API calls for market/token discovery).

Collection window: start-of-today (America/New_York) through now, so one run
per day per market captures the whole pre-game trading day regardless of
exact first-pitch time (no schedule lookup needed).

Ingest mechanics (page size, MAX_PAGES cap, archive path) are reused
directly from analysis/ingest/ingest_orderbooks.py's fetch_market_orderbook,
which writes to data/lake/raw/orderbooks/<season>/<condition_id>.jsonl and
shares data/lake/state/orderbooks.json resumable state with the Phase 3
bulk sweep -- a market already fetched by either job is skipped by the
other, so this can run daily without re-pulling markets the bulk sweep
already covered.

Frugal by construction: ~15 games/day x ~1-3 signal markets/game with a
resolved token_id = roughly 15-45 markets/day, each up to MAX_PAGES=3
paginated intel-572 calls -> well within a ~100-300 calls/day budget.

Usage:
    .venv/bin/python3 analysis/ingest/collect_orderbooks_daily.py
    .venv/bin/python3 analysis/ingest/collect_orderbooks_daily.py --status
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from client import DATA_DIR, IngestState, load_api_key
from ingest_orderbooks import JOB, fetch_market_orderbook

NY_TZ = ZoneInfo("America/New_York")
CACHE_DIR = DATA_DIR / "cache"
LIVE_SIGNALS_PATH = DATA_DIR / "live_signals.json"


def today_ny() -> str:
    return datetime.now(NY_TZ).strftime("%Y-%m-%d")


def start_of_day_ms(today_str: str) -> int:
    start = datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=NY_TZ)
    return int(start.timestamp() * 1000)


def build_token_map_from_cache() -> dict[str, dict[str, str]]:
    """condition_id -> {outcome -> token_id}. Mirrors
    scripts/poll_live.py::build_token_map_from_cache (same local trade
    cache, zero API calls)."""
    token_map: dict[str, dict[str, str]] = {}
    if not CACHE_DIR.exists():
        return token_map
    for fname in os.listdir(CACHE_DIR):
        if not fname.startswith("mtrades_"):
            continue
        with open(CACHE_DIR / fname) as f:
            cached = json.load(f)
        for d in cached.get("mlb_trade_details", []):
            cid = d.get("condition_id", "")
            outcome = d.get("outcome", "")
            tid = d.get("token_id", "")
            if cid and outcome and tid:
                token_map.setdefault(cid, {})[outcome] = tid
    return token_map


def load_today_signal_targets(today_str: str) -> list[dict]:
    """Today's active MLB markets that already have a live trader signal --
    same population the dashboard shows, sourced from data/live_signals.json
    (written by scripts/poll_live.py -- run that first)."""
    if not LIVE_SIGNALS_PATH.exists():
        print(f"ERROR: {LIVE_SIGNALS_PATH} not found -- run scripts/poll_live.py first", file=sys.stderr)
        return []
    with open(LIVE_SIGNALS_PATH) as f:
        live = json.load(f)
    token_map = build_token_map_from_cache()
    targets = []
    for m in live.get("active_markets", []):
        if not m.get("has_trader_data") or m.get("game_date") != today_str:
            continue
        cid = m.get("condition_id", "")
        token_id = token_map.get(cid, {}).get(m.get("top_outcome", ""))
        if cid and token_id:
            targets.append({"condition_id": cid, "token_id": token_id})
    return targets


def run() -> None:
    today_str = today_ny()
    season = int(today_str[:4])
    start_ms = start_of_day_ms(today_str)
    end_ms = int(time.time() * 1000)

    state = IngestState(JOB)
    api_key = load_api_key()

    targets = load_today_signal_targets(today_str)
    todo = [t for t in targets if not state.is_done(t["condition_id"])]
    print(f"Daily orderbook collection for {today_str}: {len(targets)} signal markets, "
          f"{len(targets) - len(todo)} already collected, {len(todo)} to fetch.")

    total_rows = 0
    for t in todo:
        cid, token_id = t["condition_id"], t["token_id"]
        try:
            n_rows = fetch_market_orderbook(cid, token_id, season, start_ms, end_ms, api_key)
            state.mark_done(cid)
            state.set(f"rows__{cid}", n_rows)
            state.set("cum_rows", state.get("cum_rows", 0) + n_rows)
            state.save()
            total_rows += n_rows
            print(f"  {cid[:14]}... rows={n_rows}")
        except Exception as e:
            print(f"  [FAILED] {cid[:14]}... error={e} (will retry next run -- not marked complete)")

    print(f"Done: {total_rows} orderbook-snapshot rows archived for {len(todo)} markets.")


def print_status() -> None:
    state = IngestState(JOB)
    today_str = today_ny()
    targets = load_today_signal_targets(today_str)
    done = sum(1 for t in targets if state.is_done(t["condition_id"]))
    print(f"Today ({today_str}): {done}/{len(targets)} signal markets collected.")


def main() -> None:
    if "--status" in sys.argv[1:]:
        print_status()
        return
    run()


if __name__ == "__main__":
    main()
