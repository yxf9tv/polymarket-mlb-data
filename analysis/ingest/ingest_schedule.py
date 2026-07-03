#!/usr/bin/env python3
"""Ingest MLB season schedules (statsapi.mlb.com, sportId=1) for 2022-present.

One call per season (statsapi accepts a full-year startDate/endDate range —
verified during Phase 0 build). Archives raw per-season responses to
data/lake/raw/schedule/<year>.jsonl (one line per date-bucket, matching the
statsapi `dates[]` shape) and tracks completed years in
data/lake/state/schedule.json so reruns skip already-fetched seasons.

Usage:
    .venv/bin/python3 analysis/ingest/ingest_schedule.py               # all years
    .venv/bin/python3 analysis/ingest/ingest_schedule.py 2024 2025     # specific years
"""

from __future__ import annotations

import datetime
import sys
import time

from client import IngestState, append_jsonl, statsapi_get

JOB = "schedule"
START_YEAR = 2022
END_YEAR = datetime.datetime.now(datetime.timezone.utc).year  # present (2026)


def ingest_year(year: int, state: IngestState) -> None:
    item_id = str(year)
    if state.is_done(item_id):
        print(f"  [{year}] already done, skipping")
        return

    start = f"{year}-01-01"
    end = f"{year}-12-31"
    print(f"  [{year}] fetching {start}..{end} ...")
    data = statsapi_get({"sportId": 1, "startDate": start, "endDate": end})
    if data is None:
        print(f"  [{year}] FAILED (no response) - will retry on next run")
        return

    dates = data.get("dates", [])
    total_games = data.get("totalGames", 0)
    n_written = append_jsonl(JOB, str(year), dates)

    state.mark_done(item_id)
    state.save()
    print(f"  [{year}] total_games={total_games} date_buckets={n_written}")
    time.sleep(0.2)


def main() -> None:
    args = sys.argv[1:]
    years = [int(y) for y in args] if args else list(range(START_YEAR, END_YEAR + 1))

    state = IngestState(JOB)
    print(f"Ingesting MLB schedule for seasons: {years}")
    for year in years:
        ingest_year(year, state)

    print(f"\nDone. Completed years: {sorted(state.completed_ids)}")


if __name__ == "__main__":
    main()
