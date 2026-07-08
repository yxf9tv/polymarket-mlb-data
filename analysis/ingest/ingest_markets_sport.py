#!/usr/bin/env python3
"""Ingest ALL NFL/NBA markets/events from Gamma, 2024 -> present, open + closed,
via keyset pagination. Generalizes ingest_markets.py's MLB sweep to any sport
in SPORT_CONFIG - same Gamma quirks apply identically (verified live for both
tag_id=450/nfl and tag_id=745/nba): `/events/keyset` param `after_cursor` (NOT
`cursor`/`next_cursor`) to continue, response field `next_cursor`, page cap
100/page regardless of `limit`, `order=id&ascending=true` walks in creation
order and is reliable (unlike `startDate`, which has garbage defaults on some
rows).

Two independent keyset sweeps are merged (dedup by event id in build_lake.py):
  1. tag_id=<sport tag> (primary, comprehensive) - e.g. tag 450 for NFL, 745
     for NBA. Confirmed live to cover ALL per-game + season-long futures
     markets 2024->present regardless of series linkage (e.g. the 2024-25
     NBA "nba-cup-winner" event has series=[] but carries tag 745).
  2. series_id=<sport legacy series> (supplementary/defense-in-depth,
     mirrors the MLB script's dual-sweep pattern) - series id=1 (nfl) covers
     the 2024 season's per-game events; series id=2 (nba) covers older
     per-game events. Newer seasons introduced per-season series
     (nfl-2025, nba-2026) which are NOT swept directly here (there's no
     stable single id for "all seasons") - the tag sweep is what catches
     those; this legacy-series sweep is pure defense-in-depth in case the
     tag sweep ever misses an older row.

Raw events (each with its nested `markets` array untouched) are archived
append-only to data/lake/raw/markets_<sport>/events.jsonl. Each sweep's
cursor is persisted to data/lake/state/markets_<sport>.json (separate cursor
per sweep) after every page, so a killed or interrupted run resumes exactly
where it left off with zero duplicate rows.

Crosscheck-vs-intel (as MLB's script does for agent 574) is skipped for a
first pass: the intel API agent ids/params available in client.py
(AGENT_MARKETS=574 etc.) are unconfirmed for NFL/NBA condition_ids, and this
sweep's job is discovery + lake-building, not settlement verification. Flag
is accepted for CLI parity but is a documented no-op.

Usage:
    .venv/bin/python3 analysis/ingest/ingest_markets_sport.py --sport nfl
    .venv/bin/python3 analysis/ingest/ingest_markets_sport.py --sport nba
    .venv/bin/python3 analysis/ingest/ingest_markets_sport.py --sport nfl --sweep-only
"""

from __future__ import annotations

import argparse
import time

from client import IngestState, append_jsonl, gamma_get

PAGE_LIMIT = 100  # Gamma /events/keyset caps at 100/page regardless of `limit` (probe P1, MLB)

SPORT_CONFIG = {
    "nfl": {"tag_id": 450, "legacy_series_id": 1},
    "nba": {"tag_id": 745, "legacy_series_id": 2},
}


def sweep(job: str, state: IngestState, filter_params: dict, sweep_name: str) -> int:
    """Walk /events/keyset with the given filter (tag_id or series_id), id
    ascending, resuming from this sweep's persisted cursor. Returns total
    events fetched this run. `sweep_name` namespaces the cursor/done state
    fields so the two sweeps don't clobber each other. Identical logic to
    ingest_markets.py::sweep, parameterized by job name."""
    cursor_key = f"cursor__{sweep_name}"
    done_key = f"sweep_done__{sweep_name}"
    fetched_key = f"events_fetched__{sweep_name}"

    cursor = state.get(cursor_key)
    done = state.get(done_key, False)
    if done:
        print(f"  [{sweep_name}] Sweep already complete ({state.get(fetched_key, 0)} events archived). "
              f"Delete data/lake/state/{job}.json to force a re-sweep.")
        return 0

    fetched_this_run = 0
    pages_this_run = 0
    total_fetched = state.get(fetched_key, 0)

    print(f"Sweeping Gamma /events/keyset [{sweep_name}] {filter_params} (resuming from cursor={cursor!r}) ...")
    while True:
        params = {
            **filter_params,
            "limit": PAGE_LIMIT,
            "order": "id",
            "ascending": "true",
        }
        if cursor:
            # See ingest_markets.py for the after_cursor/next_cursor quirk explanation.
            params["after_cursor"] = cursor

        data = gamma_get("/events/keyset", params)
        if data is None:
            print("  [gamma sweep] request failed, will resume from last saved cursor on next run")
            break

        events = data.get("events", [])
        if not events:
            state.set(done_key, True)
            state.save()
            print(f"  [{sweep_name}] Sweep complete: no more events.")
            break

        n_written = append_jsonl(job, "events", events)
        fetched_this_run += n_written
        total_fetched += n_written
        pages_this_run += 1

        cursor = data.get("next_cursor")
        state.set(cursor_key, cursor)
        state.set(fetched_key, total_fetched)
        state.save()

        if pages_this_run % 5 == 0 or not cursor:
            last_ev = events[-1]
            print(f"  [{sweep_name}] page {pages_this_run}: +{n_written} events (total={total_fetched}), "
                  f"last_id={last_ev.get('id')} last_slug={last_ev.get('slug')}")

        if not cursor:
            state.set(done_key, True)
            state.save()
            print(f"  [{sweep_name}] Sweep complete: cursor exhausted.")
            break

        time.sleep(0.15)  # gentle on the public API

    print(f"[{sweep_name}] Sweep run summary: {pages_this_run} pages, {fetched_this_run} new events "
          f"(cumulative archived: {total_fetched})")
    return fetched_this_run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", required=True, choices=sorted(SPORT_CONFIG))
    parser.add_argument("--sweep-only", action="store_true")
    parser.add_argument("--crosscheck-only", action="store_true")
    args = parser.parse_args()

    sport = args.sport
    cfg = SPORT_CONFIG[sport]
    job = f"markets_{sport}"
    state = IngestState(job)

    if args.crosscheck_only:
        print("  [crosscheck] not implemented for NFL/NBA in this pass (see module docstring) - no-op")
        return

    sweep(job, state, {"tag_id": cfg["tag_id"]}, "tag")
    sweep(job, state, {"series_id": cfg["legacy_series_id"]}, "series")

    if not args.sweep_only:
        print("  [crosscheck] not implemented for NFL/NBA in this pass (see module docstring) - no-op")


if __name__ == "__main__":
    main()
