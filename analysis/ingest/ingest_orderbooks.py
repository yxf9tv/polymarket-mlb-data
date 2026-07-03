#!/usr/bin/env python3
"""Phase 3 step 1b: ingest historical orderbook snapshots for the sample
built by analysis/select_orderbook_targets.py (data/lake/orderbook_targets.parquet
-- 3,114 markets: all 2026 crowd-signal totals/moneyline markets [totals
deduped to 1 O/U line/game] + a 500-market 2025 sample).

Source: intelligence API agent 572 only (per probe P5 -- no public fallback,
CLOB book is live-only). One call per market covers the whole
[first_pitch - 6h, first_pitch] window (probe P5: ~70-level snapshots,
dense but a 6h pre-game window is far smaller than the day-window probe
that needed pagination); a second+ page is fetched only if `has_more` is
still true, capped at MAX_PAGES to bound one market's cost.

Raw pages archived append-only to
data/lake/raw/orderbooks/<season>/<condition_id>.jsonl (each record already
carries token_id; condition_id/season are added before writing so
build_orderbook_lake.py doesn't need the targets table again).

DISCOVERED RETENTION LIMIT (not in the original probe): intel 572 orderbook
history is a rolling window, empty for the entire 2025 season -- verified on
17 spot checks incl. the 5 highest-volume 2025 markets (World Series,
Blue Jays/Dodgers, Nov 2025), all returned 0 snapshots. Coverage starts
~2026-03-26 (a 2026-03-03 spring-training market returned 0; 2026-03-26+
markets return data), i.e. roughly a rolling ~3-month window as of the
2026-07-03 ingest date. The 500-market 2025 sample in orderbook_targets.parquet
is therefore SKIPPED by default (season not in SEASONS_WITH_RETENTION) to
avoid burning call budget on markets known to return nothing; pass
--include-no-retention to force-attempt them anyway (e.g. to re-probe if the
window has since grown). This is documented in reports/07_edge_rules.md.

Usage:
    .venv/bin/python3 analysis/ingest/ingest_orderbooks.py --limit-markets 20
    .venv/bin/python3 analysis/ingest/ingest_orderbooks.py                 # full sweep (3,114 markets)
    .venv/bin/python3 analysis/ingest/ingest_orderbooks.py --status
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from client import (
    IngestState,
    LAKE_DIR,
    append_jsonl,
    intel_call,
    load_api_key,
    AGENT_ORDERBOOK,
)
from targets import parse_limit_markets_arg, parse_workers_arg

JOB = "orderbooks"
DEFAULT_WORKERS = 6
PAGE_LIMIT = 200
MAX_PAGES = 3  # cap 600 snapshots/market -- plenty for a 6h pre-game window (probe P5: dense but bursty)
LOOKBACK_HOURS = 6
SLEEP_S = 0.15
SEASONS_WITH_RETENTION = {2026}  # discovered empirically -- see module docstring


class OrderbookFailure(Exception):
    pass


def load_targets(limit: int | None, include_no_retention: bool = False) -> pd.DataFrame:
    df = pd.read_parquet(LAKE_DIR / "orderbook_targets.parquet")
    if not include_no_retention:
        n_before = len(df)
        df = df[df["season"].isin(SEASONS_WITH_RETENTION)]
        print(f"  (skipping {n_before - len(df)} markets outside {sorted(SEASONS_WITH_RETENTION)} -- "
              f"no orderbook retention, see module docstring; pass --include-no-retention to force)")
    df = df.sort_values(["season", "first_pitch"]).reset_index(drop=True)
    if limit:
        df = df.head(limit)
    return df


def fetch_market_orderbook(cid: str, token_id: str, season: int, start_ms: int, end_ms: int, api_key: str) -> int:
    """Offset-paginate intel 572 for one token's pre-game window. Archives
    each page as it arrives. Returns rows archived. Raises OrderbookFailure
    on an unrecoverable request error."""
    offset = 0
    n_rows = 0
    for _page in range(MAX_PAGES):
        params = {"token_id": token_id, "start_time": str(start_ms), "end_time": str(end_ms)}
        resp = intel_call(AGENT_ORDERBOOK, params, {"limit": PAGE_LIMIT, "offset": offset}, api_key)
        if resp is None:
            raise OrderbookFailure(f"intel 572 request failed for {cid} token={token_id[:16]}... offset={offset}")
        results = resp.get("data", {}).get("results", [])
        if results:
            enriched = [{**r, "condition_id": cid, "season": season} for r in results]
            append_jsonl(JOB, f"{season}/{cid}", enriched)
            n_rows += len(results)
        has_more = resp.get("pagination", {}).get("has_more", False)
        if not results or not has_more:
            break
        offset += PAGE_LIMIT
        time.sleep(SLEEP_S)
    return n_rows


def process_market(row: dict) -> dict:
    cid = row["condition_id"]
    season = int(row["season"])
    token_id = row["token_id"]
    api_key = row["_api_key"]
    t0 = time.time()

    first_pitch = row.get("first_pitch")
    if first_pitch is None or pd.isna(first_pitch):
        return {"condition_id": cid, "season": season, "rows": 0, "ok": False,
                "error": "missing first_pitch", "elapsed_s": round(time.time() - t0, 2)}

    end_ms = int(pd.Timestamp(first_pitch).timestamp() * 1000)
    start_ms = end_ms - LOOKBACK_HOURS * 3600 * 1000

    try:
        n_rows = fetch_market_orderbook(cid, token_id, season, start_ms, end_ms, api_key)
        ok, error = True, None
    except OrderbookFailure as e:
        n_rows, ok, error = 0, False, str(e)

    return {"condition_id": cid, "season": season, "rows": n_rows, "ok": ok,
            "error": error, "elapsed_s": round(time.time() - t0, 2)}


def run_sweep(limit: int | None, workers: int, include_no_retention: bool = False) -> None:
    state = IngestState(JOB)
    api_key = load_api_key()

    targets_df = load_targets(limit, include_no_retention)
    total_target = len(targets_df)
    todo = targets_df[~targets_df["condition_id"].apply(state.is_done)]
    print(f"Target universe: {total_target} markets (limit={limit}). "
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
                      f"rows={result['rows']} elapsed={result['elapsed_s']}s")
            else:
                print(f"  [FAILED] {cid[:14]}... season={result['season']} error={result['error']} "
                      f"(will retry on next run - not marked complete)")

    wall_s = time.time() - t_start
    n_run = len(jobs)
    print(f"\nRun summary: {n_run} markets attempted, {total_rows_this_run} orderbook-snapshot rows archived, "
          f"wall_clock={wall_s:.1f}s, avg_per_market={wall_s / max(n_run, 1):.2f}s (with {workers} workers)")


def print_status() -> None:
    state = IngestState(JOB)
    targets_df = load_targets(None)  # status is always w.r.t. the retention-eligible universe
    total = len(targets_df)
    completed = sum(1 for cid in targets_df["condition_id"] if state.is_done(cid))
    cum_rows = state.get("cum_rows", 0)
    cum_markets = state.get("cum_markets", 0)
    cum_elapsed = state.get("cum_elapsed_s", 0.0)
    avg_s = cum_elapsed / cum_markets if cum_markets else None
    remaining = total - completed
    print(f"orderbooks ingest status: {completed}/{total} markets complete ({100.0 * completed / total:.1f}%)")
    print(f"  rows archived so far (this state file's lifetime): {cum_rows}")
    if avg_s is not None:
        est_remaining_wall_s = avg_s * remaining / DEFAULT_WORKERS
        print(f"  avg time/market (serial): {avg_s:.2f}s -> est. remaining wall-clock at {DEFAULT_WORKERS} workers: "
              f"{est_remaining_wall_s / 3600:.2f}h ({remaining} markets left)")
    else:
        print("  no completed markets yet - run the sweep to get timing data")


def main() -> None:
    args = sys.argv[1:]
    if "--status" in args:
        print_status()
        return
    limit = parse_limit_markets_arg(args)
    workers = parse_workers_arg(args, DEFAULT_WORKERS)
    include_no_retention = "--include-no-retention" in args
    run_sweep(limit, workers, include_no_retention)


if __name__ == "__main__":
    main()
