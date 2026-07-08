#!/usr/bin/env python3
"""Ingest per-market trade census for the NFL/NBA trades-sweep universe
(see targets_sport.py for the exact per-sport season/market_type scope).

Generalizes ingest_trades.py's battle-tested pull logic to two sports whose
`season` is not always an int (NBA seasons are string labels like "2025-26"),
via a `season_dir` string column carried through instead of `int(season)`.

Primary source: intelligence API agent 556, proxy_wallet="ALL" +
condition_id, offset-paginated (limit=200) until has_more is False - no page
cap (per probe P6: both intel and Data-API paginate cleanly past 1,000 rows
with no observed hard cap). If a single market's trade count blows past
~50k rows with has_more still True (busy futures markets), fall back to
time-slicing the pull by start_time/end_time (daily, then hourly if a day
itself is still too big) - each slice is paginated in full.

Fallback source: Data-API `/trades?market=<condition_id>` (no auth, per
probe P2/P6) if intel repeatedly fails for a market (network/5xx, not just
an empty result). Which source served each market is recorded in the
per-market IngestState metadata and printed in per-market progress lines.

Raw pages are archived append-only, one JSONL line per trade record, to
data/lake/raw/trades_<sport>/<season_dir>/<condition_id>.jsonl - written
incrementally as each page/slice comes back, so a killed run keeps whatever
it fetched (build_lake.py dedupes on trade_id, so a resumed market that
re-fetches from offset 0 just produces harmless duplicate lines). A
market's condition_id is only added to IngestState.completed_ids after its
FULL pagination succeeds, so resuming only re-does incomplete markets,
never already-finished ones.

Markets are parallelized across --workers threads (default 6, the
proven-safe intel concurrency from scripts/poll_live.py); pages within a
single market are fetched serially (pagination is inherently sequential).

Usage:
    .venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nfl --limit-markets 20
    .venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nfl            # full sweep (15,494 markets)
    .venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nba            # full sweep (22,847 markets)
    .venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nfl --status
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from client import (
    IngestState,
    append_jsonl,
    data_api_get,
    intel_call,
    load_api_key,
    AGENT_TRADES,
)
from targets import parse_limit_markets_arg, parse_workers_arg
from targets_sport import load_target_markets_sport

PAGE_LIMIT = 200
DEFAULT_WORKERS = 6
ROW_CAP_FOR_TIME_SLICE = 50_000  # switch to time-slicing past this many rows for one market
MAX_CONSECUTIVE_INTEL_FAILURES = 3  # then fall back to Data-API for this market
SLEEP_S = 0.15


def _parse_sport_arg(args: list[str]) -> str:
    for i, a in enumerate(args):
        if a == "--sport" and i + 1 < len(args):
            return args[i + 1]
        if a.startswith("--sport="):
            return a.split("=", 1)[1]
    raise SystemExit("--sport {nfl,nba} is required")


def _parse_iso(date_str) -> datetime | None:
    # isinstance guard (rather than a truthiness/`is None` check) also
    # safely rejects NaN/pd.NA from parquet-sourced dict rows without
    # raising (bool(pd.NA) itself raises TypeError).
    if not isinstance(date_str, str) or not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _daily_buckets(start: datetime, end: datetime):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=1), end)
        yield cur, nxt
        cur = nxt


def _hourly_buckets(start: datetime, end: datetime):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(hours=1), end)
        yield cur, nxt
        cur = nxt


class IntelFailure(Exception):
    """Raised when an intel page request fails outright (not just empty)."""


def _fetch_intel_window(
    job: str, cid: str, season_dir: str, api_key: str, start_ts: int | None, end_ts: int | None
) -> tuple[int, bool]:
    """Offset-paginate intel 556 for one condition_id, optionally bounded by
    a start/end time window. Archives each page to the raw JSONL as soon as
    it arrives (so a killed run keeps whatever it fetched). Returns
    (row_count, hit_row_cap). Raises IntelFailure on an unrecoverable
    request error (client.intel_call already retries with backoff, so a
    None here means it truly failed)."""
    offset = 0
    n_rows = 0
    while True:
        params: dict = {"proxy_wallet": "ALL", "condition_id": cid}
        if start_ts is not None:
            params["start_time"] = str(start_ts)
        if end_ts is not None:
            params["end_time"] = str(end_ts)
        resp = intel_call(AGENT_TRADES, params, {"limit": PAGE_LIMIT, "offset": offset}, api_key)
        if resp is None:
            raise IntelFailure(f"intel 556 request failed for {cid} offset={offset}")
        results = resp.get("data", {}).get("results", [])
        if results:
            append_jsonl(job, f"{season_dir}/{cid}", results)
            n_rows += len(results)
        has_more = resp.get("pagination", {}).get("has_more", False)
        if not results or not has_more:
            return n_rows, False
        offset += PAGE_LIMIT
        if n_rows >= ROW_CAP_FOR_TIME_SLICE:
            return n_rows, True
        time.sleep(SLEEP_S)


def fetch_market_trades_intel(
    job: str, cid: str, season_dir: str, start_date: str | None, end_date: str | None, api_key: str
) -> int:
    """Full trade pull for one market via intel 556, with time-slice fallback
    if the plain offset pull exceeds ROW_CAP_FOR_TIME_SLICE rows and still
    has more. Raises IntelFailure if any request in the pull fails. Returns
    total row count archived."""
    n_rows, hit_cap = _fetch_intel_window(job, cid, season_dir, api_key, None, None)
    if not hit_cap:
        return n_rows

    # Busy market: re-pull via daily (then hourly, if a day is itself too
    # big) time slices so no single request has to paginate past the cap.
    # NOTE: the plain pull above already wrote ROW_CAP_FOR_TIME_SLICE rows
    # to the archive; build_lake.py dedupes on trade_id so the re-pull below
    # (which covers the market's full date range from scratch) just adds
    # harmless duplicate lines rather than double-counting.
    start = _parse_iso(start_date) or datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = _parse_iso(end_date) or datetime.now(timezone.utc)
    if end <= start:
        end = start + timedelta(days=1)

    sliced_rows = 0
    for day_start, day_end in _daily_buckets(start, end):
        day_n, day_hit_cap = _fetch_intel_window(
            job, cid, season_dir, api_key, int(day_start.timestamp()), int(day_end.timestamp())
        )
        sliced_rows += day_n
        if not day_hit_cap:
            continue
        # Still too big for one day - sub-slice hourly (day total above
        # already includes the capped rows; hourly re-pull covers the same
        # day from scratch, deduped downstream same as above).
        for hr_start, hr_end in _hourly_buckets(day_start, day_end):
            hr_n, hr_hit_cap = _fetch_intel_window(
                job, cid, season_dir, api_key, int(hr_start.timestamp()), int(hr_end.timestamp())
            )
            sliced_rows += hr_n
            if hr_hit_cap:
                print(f"  [WARN] {cid} still >{ROW_CAP_FOR_TIME_SLICE} rows within a single hour "
                      f"({hr_start.isoformat()}) - accepting partial data for that hour")
    return sliced_rows


def fetch_market_trades_data_api(job: str, cid: str, season_dir: str) -> int:
    """Full trade pull for one market via the Data-API fallback, offset-
    paginated (limit=200) until a short page is returned. Archives each page
    as it arrives. Returns total row count archived."""
    offset = 0
    n_rows = 0
    while True:
        page = data_api_get("/trades", {"market": cid, "limit": PAGE_LIMIT, "offset": offset})
        if page is None:
            raise IntelFailure(f"data-api request failed for {cid} offset={offset}")
        if not isinstance(page, list) or not page:
            break
        append_jsonl(job, f"{season_dir}/{cid}", page)
        n_rows += len(page)
        if len(page) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(SLEEP_S)
    return n_rows


def process_market(row: dict) -> dict:
    """Fetch + archive all trades for one market. Returns a result dict
    (never raises - failures are captured in the result so one bad market
    doesn't kill the worker pool)."""
    job = row["_job"]
    cid = row["condition_id"]
    season_dir = row["season_dir"]
    start_date = row.get("start_date")
    end_date = row.get("end_date")
    t0 = time.time()
    api_key = row["_api_key"]

    source = "intel"
    n_rows = 0
    ok = False
    error = None
    for attempt in range(MAX_CONSECUTIVE_INTEL_FAILURES):
        try:
            n_rows = fetch_market_trades_intel(job, cid, season_dir, start_date, end_date, api_key)
            ok = True
            break
        except IntelFailure as e:
            error = str(e)
            time.sleep(1.0)
    if not ok:
        source = "data_api"
        try:
            n_rows = fetch_market_trades_data_api(job, cid, season_dir)
            ok = True
            error = None
        except IntelFailure as e:
            error = str(e)

    return {
        "condition_id": cid,
        "season_dir": season_dir,
        "rows": n_rows,
        "source": source if ok else "failed",
        "ok": ok,
        "error": error,
        "elapsed_s": round(time.time() - t0, 2),
    }


def run_sweep(sport: str, limit: int | None, workers: int) -> None:
    job = f"trades_{sport}"
    state = IngestState(job)
    api_key = load_api_key()

    targets_df = load_target_markets_sport(sport, limit=limit)
    total_target = len(targets_df)
    todo = targets_df[~targets_df["condition_id"].apply(state.is_done)]
    print(f"Target universe: {total_target} {sport} markets (limit={limit}). "
          f"Already completed: {total_target - len(todo)}. Remaining: {len(todo)}. Workers: {workers}.")

    if todo.empty:
        print("Nothing to do - all target markets already completed.")
        return

    jobs = []
    for _, r in todo.iterrows():
        d = r.to_dict()
        d["_api_key"] = api_key
        d["_job"] = job
        jobs.append(d)

    done_count = total_target - len(todo)
    total_rows_this_run = 0
    total_elapsed_this_run = 0.0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_market, j): j["condition_id"] for j in jobs}
        for fut in as_completed(futures):
            result = fut.result()
            cid = result["condition_id"]
            if result["ok"]:
                state.mark_done(cid)
                state.set(f"rows__{cid}", result["rows"])
                state.set(f"source__{cid}", result["source"])
                state.set("cum_rows", state.get("cum_rows", 0) + result["rows"])
                state.set("cum_markets", state.get("cum_markets", 0) + 1)
                state.set("cum_elapsed_s", state.get("cum_elapsed_s", 0.0) + result["elapsed_s"])
                state.save()
                done_count += 1
                total_rows_this_run += result["rows"]
                total_elapsed_this_run += result["elapsed_s"]
                print(f"  [{done_count}/{total_target}] {cid[:14]}... season={result['season_dir']} "
                      f"rows={result['rows']} source={result['source']} elapsed={result['elapsed_s']}s")
            else:
                print(f"  [FAILED] {cid[:14]}... season={result['season_dir']} error={result['error']} "
                      f"(will retry on next run - not marked complete)")

    wall_s = time.time() - t_start
    n_run = len(jobs)
    print(f"\nRun summary: {n_run} markets attempted, {total_rows_this_run} trade rows archived, "
          f"wall_clock={wall_s:.1f}s, avg_per_market={wall_s / max(n_run, 1):.2f}s "
          f"(with {workers} workers)")


def print_status(sport: str) -> None:
    job = f"trades_{sport}"
    state = IngestState(job)
    targets_df = load_target_markets_sport(sport)
    total = len(targets_df)
    completed = sum(1 for cid in targets_df["condition_id"] if state.is_done(cid))
    cum_rows = state.get("cum_rows", 0)
    cum_markets = state.get("cum_markets", 0)
    cum_elapsed = state.get("cum_elapsed_s", 0.0)
    avg_s = cum_elapsed / cum_markets if cum_markets else None
    remaining = total - completed
    print(f"{job} ingest status: {completed}/{total} markets complete ({100.0 * completed / total:.1f}%)")
    print(f"  rows archived so far (this state file's lifetime): {cum_rows}")
    if avg_s is not None:
        est_remaining_wall_s = avg_s * remaining / DEFAULT_WORKERS
        print(f"  avg time/market (serial): {avg_s:.2f}s -> est. remaining wall-clock at {DEFAULT_WORKERS} workers: "
              f"{est_remaining_wall_s / 3600:.1f}h ({remaining} markets left)")
    else:
        print("  no completed markets yet - run the sweep to get timing data")


def main() -> None:
    args = sys.argv[1:]
    sport = _parse_sport_arg(args)
    if sport not in ("nfl", "nba"):
        raise SystemExit(f"--sport must be 'nfl' or 'nba', got {sport!r}")
    if "--status" in args:
        print_status(sport)
        return
    limit = parse_limit_markets_arg(args)
    workers = parse_workers_arg(args, DEFAULT_WORKERS)
    run_sweep(sport, limit, workers)


if __name__ == "__main__":
    main()
