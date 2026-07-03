#!/usr/bin/env python3
"""Ingest ALL MLB markets/events from Gamma, 2020 -> present, open + closed,
via keyset pagination (offset endpoints are deprecated - verified in probe
P1, and additionally hard-cap at offset+limit~2000 with a 422 - confirmed
during Phase 0 build).

Two independent keyset sweeps are merged (dedup by event id in build_lake.py):
  1. series_id=3 (the MLB series) - covers 2021-2023 and 2025-2026.
  2. tag_id=100381 (the "MLB" tag) - covers events NOT linked to series_id=3.
Discovered during Phase 0 build: series_id=3 has a complete gap for 2024 -
e.g. `mlb-dodgers-vs-padres` (2024-09-26 playoffs) has `series: None` but
carries tag 100381 ("MLB", created 2024-08-27). Series linkage and the MLB
tag are applied inconsistently across eras (2022-23 events: series link, no
tag; 2024 events: tag, no series link; 2025-26 events: both) - so a single
filter misses markets. This is the "probe alternatives (tag search)" case
anticipated by the research plan when series_id=3 turned out incomplete.

Each sweep is a single monotonic cursor walk ordered by id ascending (id
order tracks creation order, unlike the unreliable startDate field - probe
P1). No `closed` filter is applied so one sweep captures both resolved and
still-open markets across every season.

Raw events (each with its nested `markets` array untouched) are archived
append-only to data/lake/raw/markets/events.jsonl. Each sweep's cursor is
persisted to data/lake/state/markets.json (separate cursor per sweep) after
every page, so a killed or interrupted run resumes exactly where it left off
with zero duplicate rows.

After the sweep, a settlement cross-check samples ~50 resolved 2025+ markets
and compares the Gamma-derived winner (argmax outcomePrices) against
intelligence-API agent 574's `winning_outcome`, reporting agreement.

Usage:
    .venv/bin/python3 analysis/ingest/ingest_markets.py               # full sweep + crosscheck
    .venv/bin/python3 analysis/ingest/ingest_markets.py --sweep-only  # skip crosscheck
    .venv/bin/python3 analysis/ingest/ingest_markets.py --crosscheck-only
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from client import (
    LAKE_DIR,
    RAW_DIR,
    IngestState,
    append_jsonl,
    gamma_get,
    intel_call,
    load_api_key,
    AGENT_MARKETS,
)
from derive import derive_season, derive_winner

JOB = "markets"
SERIES_ID_MLB = 3
TAG_ID_MLB = 100381  # "MLB" tag - fills the series_id=3 gap for 2024 events
PAGE_LIMIT = 100  # Gamma /events/keyset caps at 100/page regardless of `limit` (probe P1)


def sweep(state: IngestState, filter_params: dict, sweep_name: str) -> int:
    """Walk /events/keyset with the given filter (series_id=3 or tag_id=MLB),
    id ascending, resuming from this sweep's persisted cursor. Returns total
    events fetched this run. `sweep_name` namespaces the cursor/done state
    fields so the two sweeps don't clobber each other."""
    cursor_key = f"cursor__{sweep_name}"
    done_key = f"sweep_done__{sweep_name}"
    fetched_key = f"events_fetched__{sweep_name}"

    cursor = state.get(cursor_key)
    done = state.get(done_key, False)
    if done:
        print(f"  [{sweep_name}] Sweep already complete ({state.get(fetched_key, 0)} events archived). "
              f"Delete data/lake/state/{JOB}.json to force a re-sweep.")
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
            # NOTE: the response field is named `next_cursor` but the request
            # param to send it back as is `after_cursor` (confirmed via
            # gamma-api's /openapi.json - not documented anywhere else).
            # Passing it back as `next_cursor` or `cursor` is silently
            # ignored and the API just re-returns page 1 every time (a nasty
            # quirk: no error, HTTP 200, looks like success but is a stuck
            # loop). The deprecated offset endpoint (/events, no /keyset)
            # still works but hard-caps at offset+limit ~2000 with a 422
            # ("offset too large, use /events/keyset for deeper pagination"),
            # so it cannot replace keyset for this series (spans 1000s of
            # events) - keyset with the correct `after_cursor` param is the
            # only way to sweep the full history.
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

        n_written = append_jsonl(JOB, "events", events)
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


def crosscheck_intel(sample_size: int = 50) -> None:
    """Sample ~sample_size resolved 2025+ markets from the raw archive and
    compare Gamma's derived winner against intel 574's winning_outcome."""
    import json
    import random

    events_path = RAW_DIR / JOB / "events.jsonl"
    if not events_path.exists():
        print("  [crosscheck] no archived events found - run the sweep first")
        return

    print("\nLoading archived events for cross-check sampling ...")
    resolved_2025plus = []
    with open(events_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            for market in event.get("markets", []):
                if not market.get("closed"):
                    continue
                winner, prices = derive_winner(market)
                if winner is None:
                    continue
                season, _ = derive_season(event, market)
                if season and season >= 2025:
                    resolved_2025plus.append((event, market, winner))

    print(f"  {len(resolved_2025plus)} resolved 2025+ markets available for sampling")
    if not resolved_2025plus:
        print("  [crosscheck] nothing to sample")
        return

    random.seed(42)
    sample = random.sample(resolved_2025plus, min(sample_size, len(resolved_2025plus)))

    api_key = load_api_key()
    agree = 0
    intel_missing = 0
    disagree = 0
    disagreements = []
    for event, market, gamma_winner in sample:
        cid = market.get("conditionId")
        resp = intel_call(AGENT_MARKETS, {"condition_id": cid, "closed": "True"}, {"limit": 5, "offset": 0}, api_key)
        results = resp.get("data", {}).get("results", []) if resp else []
        intel_winner = results[0].get("winning_outcome") if results else None
        if intel_winner is None:
            intel_missing += 1
        elif intel_winner == gamma_winner:
            agree += 1
        else:
            disagree += 1
            disagreements.append((market.get("slug"), gamma_winner, intel_winner))
        time.sleep(0.25)

    n = len(sample)
    print(f"\nCross-check result (n={n} sampled resolved 2025+ markets):")
    print(f"  agree={agree}  disagree={disagree}  intel_missing={intel_missing}")
    if n - intel_missing > 0:
        pct = 100.0 * agree / (n - intel_missing)
        print(f"  agreement rate (excl. intel-missing) = {pct:.1f}%")
    if disagreements:
        print("  disagreements:")
        for slug, gw, iw in disagreements[:10]:
            print(f"    {slug}: gamma={gw!r} intel={iw!r}")

    # persist result for the report
    result = {
        "sample_size": n,
        "agree": agree,
        "disagree": disagree,
        "intel_missing": intel_missing,
        "agreement_rate_pct": round(100.0 * agree / (n - intel_missing), 1) if (n - intel_missing) > 0 else None,
        "disagreements": [{"slug": s, "gamma_winner": g, "intel_winner": i} for s, g, i in disagreements],
    }
    import json as _json
    out_path = LAKE_DIR / "state" / "markets_crosscheck.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        _json.dump(result, f, indent=2)
    print(f"  Saved cross-check result -> {out_path}")


def main() -> None:
    args = sys.argv[1:]
    sweep_only = "--sweep-only" in args
    crosscheck_only = "--crosscheck-only" in args

    state = IngestState(JOB)

    if not crosscheck_only:
        sweep(state, {"series_id": SERIES_ID_MLB}, "series3")
        sweep(state, {"tag_id": TAG_ID_MLB}, "tag_mlb")

    if not sweep_only:
        crosscheck_intel(sample_size=50)


if __name__ == "__main__":
    main()
