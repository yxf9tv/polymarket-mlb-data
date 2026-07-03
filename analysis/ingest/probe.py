#!/usr/bin/env python3
"""Probe phase for the multi-season MLB data-science ingestion plan (see RESEARCH_PLAN.md).

Runs 8 cheap, read-only probes against two data sources:

  A) Intelligence API (auth, Bearer token from .env `intelligence_api_key`)
     agent 574 = markets, 556 = trades, 568 = candlesticks, 572 = orderbook.
  B) Official Polymarket APIs (no auth): Gamma, CLOB, Data-API.
  C) MLB Stats API (no auth): game schedule / first-pitch times.

Each probe prints structured `PROBE_ID | key=value | key=value ...` evidence
lines to stdout. Nothing here is fabricated: every printed number comes from
a live HTTP response captured in this run.

Usage:
    .venv/bin/python3 analysis/ingest/probe.py            # run all probes
    .venv/bin/python3 analysis/ingest/probe.py P2 P4       # run only these
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent.parent.parent
INTEL_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
MLB_STATSAPI_URL = "https://statsapi.mlb.com/api/v1/schedule"

AGENT_MARKETS = 574
AGENT_TRADES = 556
AGENT_CANDLES = 568
AGENT_ORDERBOOK = 572

# Reference markets pinned down via manual Gamma exploration ahead of writing
# this script (see report). Used as fixed probe targets so results are
# reproducible run-to-run.
EARLY_SEASON_MARKET = {
    "note": "earliest per-game MLB moneyline market found via events?series_id=3&order=id&ascending=true",
    "event_slug": "mlb-who-will-win-yankees-vs-red-sox-scheduled-for-april-10-708-pm-et",
    "condition_id": "0x1e3d4c846dade8b3441d2184e3efee41bf7bbdf715494c416afd19753ed7f8f1",
    "event_date": "2022-04-10",
    "token_ids": [
        "110384394693629001523635729645234363521185082949374959545294022491754609100465",
        "55299711518504564259962238907047217397193245731864478148362465667146011322958",
    ],
}
APRIL_2026_MARKET = {
    "event_slug": "mlb-lad-wsh-2026-04-03",
    "condition_id": "0x21f4ad71aca022013ffbcc71aeb7212bda5a8e28000ea6b178e562814a3269c4",
    "event_date": "2026-04-03",
    "token_ids": [
        "5271566035735665249793786812918631751834398652116798693585486748568903679088",
        "32741682961448483716752830509117507890020319233859345554693589377093827285479",
    ],
}
# High-volume, long-lived futures market -> good stress test for ALL-wallet
# pulls and deep pagination (P3, P6).
BUSY_2026_MARKET = {
    "event_slug": "mlb-world-series-champion-2026",
    "slug": "will-the-colorado-rockies-win-the-2026-world-series",
    "condition_id": "0x190d98a8009045e55fdef0923372d308086469ba1c5286ea5a76a744f6496fa0",
}


def log(probe: str, **kv: Any) -> None:
    parts = " | ".join(f"{k}={v}" for k, v in kv.items())
    print(f"{probe} | {parts}")


def load_api_key() -> str | None:
    load_dotenv(BASE / ".env")
    return os.getenv("intelligence_api_key")


def intel_call(
    agent_id: int,
    params: dict,
    pagination: dict | None,
    api_key: str,
    retries: int = 5,
) -> dict | None:
    """POST to the intelligence API. Copies the retry/backoff pattern proven
    in scripts/poll_live.py::api_call. Returns None (and prints a warning)
    on unrecoverable failure instead of raising, so a single probe failure
    doesn't kill the whole run."""
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body: dict[str, Any] = {
        "agent_id": agent_id,
        "params": params,
        "formatter_config": {"format_type": "raw"},
    }
    if pagination:
        body["pagination"] = pagination
    for attempt in range(retries):
        try:
            resp = requests.post(INTEL_URL, json=body, headers=headers, timeout=60)
            if resp.status_code == 429:
                wait = (2**attempt) * 5
                print(f"  [intel 429, retry in {wait}s]")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (2**attempt) * 3
                print(f"  [intel error: {e}, retry in {wait}s]")
                time.sleep(wait)
            else:
                print(f"  [intel FAILED after {retries} attempts: {e}]")
                return None
    return None


def intel_paginated(
    agent_id: int, params: dict, api_key: str, limit: int = 200, max_pages: int = 5
) -> list[dict]:
    all_results: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        resp = intel_call(agent_id, params, {"limit": limit, "offset": offset}, api_key)
        if resp is None:
            break
        results = resp.get("data", {}).get("results", [])
        if not results:
            break
        all_results.extend(results)
        if not resp.get("pagination", {}).get("has_more", False):
            break
        offset += limit
        time.sleep(0.3)  # dodge 429 storms
    return all_results


def http_get(url: str, params: dict | None = None, retries: int = 3) -> requests.Response | None:
    """GET with 429 backoff, for the unauthenticated public APIs."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = (2**attempt) * 5
                print(f"  [public-api 429, retry in {wait}s]")
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep((2**attempt) * 2)
            else:
                print(f"  [public-api FAILED: {e}]")
                return None
    return None


# ---------------------------------------------------------------------------
# P1 - earliest MLB season
# ---------------------------------------------------------------------------
def probe_p1(api_key: str) -> None:
    print("\n=== P1: earliest MLB season ===")

    # --- Intelligence API: agent 574, closed markets, 3 pages ---
    results = intel_paginated(
        AGENT_MARKETS, {"market_slug": "mlb", "closed": "True"}, api_key, limit=200, max_pages=3
    )
    n = len(results)
    with_winner = [r for r in results if r.get("winning_outcome")]
    end_dates = sorted(r.get("end_date", "") for r in results if r.get("end_date"))
    log(
        "P1.intel",
        rows=n,
        pages_fetched="<=3",
        winning_outcome_populated=f"{len(with_winner)}/{n}",
        earliest_end_date=end_dates[0] if end_dates else "n/a",
        latest_end_date=end_dates[-1] if end_dates else "n/a",
    )

    # --- Intelligence API: any MLB markets ending before 2024? (retention floor) ---
    resp_old = intel_call(
        AGENT_MARKETS,
        {"market_slug": "mlb", "closed": "True", "end_date_max": "1704067200"},  # < 2024-01-01
        {"limit": 200, "offset": 0},
        api_key,
    )
    old_rows = resp_old.get("data", {}).get("results", []) if resp_old else []
    old_dates = sorted(r.get("end_date", "") for r in old_rows if r.get("end_date"))
    log(
        "P1.intel.pre2024_check",
        filter="end_date_max=2024-01-01",
        rows=len(old_rows),
        earliest_end_date=old_dates[0] if old_dates else "n/a",
        latest_end_date=old_dates[-1] if old_dates else "n/a",
    )

    # --- Gamma: MLB series (id=3), ordered by id ascending (id order tracks
    # creation order; startDate is unreliable — many rows carry a garbage
    # default of 2021-01-01T17:00:00Z, see below). ---
    resp = http_get(
        f"{GAMMA_URL}/events",
        {"series_id": 3, "order": "id", "ascending": "true", "limit": 100, "closed": "true"},
    )
    events = resp.json() if resp is not None and resp.status_code == 200 else []
    if events:
        first, last = events[0], events[-1]
        log(
            "P1.gamma.earliest_by_id",
            n_events_page=len(events),
            first_event_id=first.get("id"),
            first_event_creationDate=first.get("creationDate"),
            first_event_slug=first.get("slug"),
            last_event_in_page_id=last.get("id"),
            last_event_in_page_creationDate=last.get("creationDate"),
        )
        # detect the id/date discontinuity noticed during manual exploration
        creation_dates = [e.get("creationDate", "") for e in events]
        years = sorted({d[:4] for d in creation_dates if d})
        log("P1.gamma.years_in_first_100_by_id", years=years)
    else:
        log("P1.gamma.earliest_by_id", error="no events returned / request failed")

    # --- Gamma: per-season existence + capped count via start_date filter ---
    for year in (2022, 2023, 2024, 2025, 2026):
        resp = http_get(
            f"{GAMMA_URL}/events",
            {
                "series_id": 3,
                "closed": "true",
                "start_date_min": f"{year}-01-01T00:00:00Z",
                "start_date_max": f"{year + 1}-01-01T00:00:00Z",
                "limit": 100,
            },
        )
        cnt = len(resp.json()) if resp is not None and resp.status_code == 200 else -1
        log("P1.gamma.season_count_capped100", year=year, events_returned=cnt)
        time.sleep(0.2)

    # --- Gamma: earliest MLB market overall (World Series futures) sanity check ---
    resp = http_get(
        f"{GAMMA_URL}/events", {"series_id": 3, "order": "creationDate", "ascending": "true", "limit": 3}
    )
    if resp is not None and resp.status_code == 200:
        earliest = resp.json()
        for e in earliest:
            log(
                "P1.gamma.earliest_creationDate",
                event_id=e.get("id"),
                creationDate=e.get("creationDate"),
                slug=e.get("slug"),
            )


# ---------------------------------------------------------------------------
# P2 - multi-year trade retention
# ---------------------------------------------------------------------------
def probe_p2(api_key: str) -> None:
    print("\n=== P2: multi-year trade retention ===")
    targets = [
        ("earliest_season_2022-04-10", EARLY_SEASON_MARKET["condition_id"]),
        ("april_2026", APRIL_2026_MARKET["condition_id"]),
    ]
    for label, cid in targets:
        # Intelligence API 556, wide time window (2020-01-01 .. now)
        resp = intel_call(
            AGENT_TRADES,
            {"proxy_wallet": "ALL", "condition_id": cid, "start_time": "1577836800", "end_time": str(int(time.time()))},
            {"limit": 200, "offset": 0},
            api_key,
        )
        results = resp.get("data", {}).get("results", []) if resp else []
        ts = sorted(r.get("timestamp", "") for r in results if r.get("timestamp"))
        log(
            "P2.intel",
            market=label,
            condition_id=cid,
            rows=len(results),
            earliest_ts=ts[0] if ts else "n/a",
            latest_ts=ts[-1] if ts else "n/a",
            has_more=resp.get("pagination", {}).get("has_more") if resp else "n/a",
        )
        time.sleep(0.3)

        # Data-API /trades?market=<conditionId>
        r = http_get(f"{DATA_API_URL}/trades", {"market": cid, "limit": 200})
        rows = r.json() if r is not None and r.status_code == 200 else None
        if isinstance(rows, list):
            ts2 = sorted(t.get("timestamp", 0) for t in rows)
            log(
                "P2.data_api",
                market=label,
                condition_id=cid,
                http_status=r.status_code,
                rows=len(rows),
                earliest_ts=ts2[0] if ts2 else "n/a",
                latest_ts=ts2[-1] if ts2 else "n/a",
            )
        else:
            log(
                "P2.data_api",
                market=label,
                condition_id=cid,
                http_status=r.status_code if r is not None else "no-response",
                rows=0,
            )
        time.sleep(0.2)


# ---------------------------------------------------------------------------
# P3 - ALL-wallet pulls
# ---------------------------------------------------------------------------
def probe_p3(api_key: str) -> None:
    print("\n=== P3: ALL-wallet pulls (busy 2026 market) ===")
    cid = BUSY_2026_MARKET["condition_id"]
    resp = intel_call(
        AGENT_TRADES,
        {"proxy_wallet": "ALL", "condition_id": cid},
        {"limit": 200, "offset": 0},
        api_key,
    )
    results = resp.get("data", {}).get("results", []) if resp else []
    wallets = {r.get("proxy_wallet") for r in results if r.get("proxy_wallet")}
    log(
        "P3.intel.page1",
        market=BUSY_2026_MARKET["slug"],
        condition_id=cid,
        rows=len(results),
        distinct_wallets_page1=len(wallets),
        has_more=resp.get("pagination", {}).get("has_more") if resp else "n/a",
    )

    # one more page to see if new wallets keep showing up (confirms it's not
    # returning the same wallet's trades repeatedly)
    if resp and resp.get("pagination", {}).get("has_more"):
        resp2 = intel_call(
            AGENT_TRADES,
            {"proxy_wallet": "ALL", "condition_id": cid},
            {"limit": 200, "offset": 200},
            api_key,
        )
        results2 = resp2.get("data", {}).get("results", []) if resp2 else []
        wallets2 = {r.get("proxy_wallet") for r in results2 if r.get("proxy_wallet")}
        log(
            "P3.intel.page2",
            rows=len(results2),
            distinct_wallets_page2=len(wallets2),
            new_wallets_vs_page1=len(wallets2 - wallets),
        )


# ---------------------------------------------------------------------------
# P4 - candle retention
# ---------------------------------------------------------------------------
def probe_p4(api_key: str) -> None:
    print("\n=== P4: candle retention ===")
    import datetime

    for label, market in (("april_2026", APRIL_2026_MARKET), ("earliest_season_2022", EARLY_SEASON_MARKET)):
        token_id = market["token_ids"][0]
        # NOTE: agent 568 returns HTTP 400 unless start_time AND end_time are
        # both provided (discovered in this probe run) — always pass them.
        y, m, d = (int(x) for x in market["event_date"].split("-"))
        game_day = datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc)
        start = int((game_day - datetime.timedelta(days=1)).timestamp())
        end = int((game_day + datetime.timedelta(days=1)).timestamp())
        resp = intel_call(
            AGENT_CANDLES,
            {"token_id": token_id, "interval": "1h", "start_time": str(start), "end_time": str(end)},
            {"limit": 200, "offset": 0},
            api_key,
        )
        results = resp.get("data", {}).get("results", []) if resp else []
        ts = sorted(
            (r.get("candle_time") or r.get("timestamp") or r.get("time") or "") for r in results
        )
        log(
            "P4.intel.candles_1h",
            market=label,
            token_id=token_id[:16] + "...",
            rows=len(results),
            earliest=ts[0] if ts else "n/a",
            latest=ts[-1] if ts else "n/a",
        )
        time.sleep(0.3)

        # CLOB prices-history (no auth). NOTE: `interval=max` only returns
        # data for ACTIVE markets; resolved tokens require explicit
        # startTs/endTs + fidelity (discovered in this probe run).
        r = http_get(
            f"{CLOB_URL}/prices-history",
            {"market": token_id, "startTs": start - 86400, "endTs": end + 86400, "fidelity": 60},
        )
        hist = None
        if r is not None and r.status_code == 200:
            try:
                hist = r.json().get("history", [])
            except ValueError:
                hist = None
        log(
            "P4.clob.prices_history_ts",
            market=label,
            http_status=r.status_code if r is not None else "no-response",
            points=len(hist) if hist is not None else "n/a",
            earliest_t=hist[0]["t"] if hist else "n/a",
            latest_t=hist[-1]["t"] if hist else "n/a",
        )


# ---------------------------------------------------------------------------
# P5 - orderbook history
# ---------------------------------------------------------------------------
def probe_p5(api_key: str) -> None:
    print("\n=== P5: orderbook history (resolved April-2026 market) ===")
    token_id = APRIL_2026_MARKET["token_ids"][0]
    # game day window: 2026-04-03 00:00 UTC .. 2026-04-04 00:00 UTC, ms epoch
    import datetime

    start = int(datetime.datetime(2026, 4, 3, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    end = int(datetime.datetime(2026, 4, 4, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    resp = intel_call(
        AGENT_ORDERBOOK,
        {"token_id": token_id, "start_time": str(start), "end_time": str(end)},
        {"limit": 200, "offset": 0},
        api_key,
    )
    results = resp.get("data", {}).get("results", []) if resp else []
    depths = []
    for r in results[:3]:
        bids_raw = r.get("bids", "[]")
        if isinstance(bids_raw, str):
            try:
                bids_raw = json.loads(bids_raw)
            except json.JSONDecodeError:
                bids_raw = []
        depths.append(len(bids_raw))
    log(
        "P5.intel.orderbook",
        token_id=token_id[:16] + "...",
        window="2026-04-03..2026-04-04 (ms epoch)",
        rows=len(results),
        sample_bid_depths=depths,
        has_more=resp.get("pagination", {}).get("has_more") if resp else "n/a",
    )


# ---------------------------------------------------------------------------
# P6 - pagination depth
# ---------------------------------------------------------------------------
def probe_p6(api_key: str) -> None:
    print("\n=== P6: pagination depth ===")
    cid = BUSY_2026_MARKET["condition_id"]
    offset = 0
    limit = 200
    page = 0
    total_rows = 0
    seen_ids: set[str] = set()
    dup_rows = 0
    last_has_more = None
    while page < 10:
        resp = intel_call(
            AGENT_TRADES, {"proxy_wallet": "ALL", "condition_id": cid}, {"limit": limit, "offset": offset}, api_key
        )
        page += 1
        if resp is None:
            log("P6.intel.page", page=page, error="request failed")
            break
        results = resp.get("data", {}).get("results", [])
        has_more = resp.get("pagination", {}).get("has_more")
        for r in results:
            rid = r.get("id")
            if rid in seen_ids:
                dup_rows += 1
            elif rid:
                seen_ids.add(rid)
        total_rows += len(results)
        last_has_more = has_more
        log("P6.intel.page", page=page, offset=offset, rows=len(results), has_more=has_more)
        if not results or not has_more:
            break
        offset += limit
        time.sleep(0.3)
    log(
        "P6.intel.summary",
        pages_fetched=page,
        total_rows=total_rows,
        distinct_ids=len(seen_ids),
        duplicate_rows=dup_rows,
        final_has_more=last_has_more,
    )

    # Data-API offset depth on the same market
    total_da = 0
    for p in range(10):
        r = http_get(f"{DATA_API_URL}/trades", {"market": cid, "limit": 200, "offset": p * 200})
        rows = r.json() if r is not None and r.status_code == 200 else []
        if not isinstance(rows, list):
            rows = []
        log("P6.data_api.page", page=p + 1, offset=p * 200, rows=len(rows), http_status=r.status_code if r else "no-response")
        total_da += len(rows)
        if len(rows) < 200:
            break
        time.sleep(0.1)
    log("P6.data_api.summary", total_rows=total_da)


# ---------------------------------------------------------------------------
# P7 - MLB schedule
# ---------------------------------------------------------------------------
def probe_p7() -> None:
    print("\n=== P7: MLB schedule (statsapi.mlb.com) ===")
    for label, start, end in (
        ("current", "2026-06-30", "2026-07-01"),
        ("2023_historical", "2023-06-15", "2023-06-15"),
    ):
        r = http_get(MLB_STATSAPI_URL, {"sportId": 1, "startDate": start, "endDate": end})
        if r is None or r.status_code != 200:
            log("P7.statsapi", window=label, http_status=r.status_code if r else "no-response", error=True)
            continue
        data = r.json()
        total_games = data.get("totalGames", 0)
        sample_game = None
        for d in data.get("dates", []):
            if d.get("games"):
                sample_game = d["games"][0]
                break
        log(
            "P7.statsapi",
            window=label,
            http_status=r.status_code,
            total_games=total_games,
            sample_gamePk=sample_game.get("gamePk") if sample_game else "n/a",
            sample_gameDate_utc=sample_game.get("gameDate") if sample_game else "n/a",
        )


# ---------------------------------------------------------------------------
# P8 - settlement cross-check
# ---------------------------------------------------------------------------
def probe_p8(api_key: str) -> None:
    print("\n=== P8: settlement cross-check (intel 574 vs Gamma outcomePrices) ===")
    check_markets = [
        ("earliest_season_2022-04-10", EARLY_SEASON_MARKET["condition_id"], EARLY_SEASON_MARKET["event_slug"]),
        ("april_2026_moneyline", APRIL_2026_MARKET["condition_id"], APRIL_2026_MARKET["event_slug"]),
        (
            "sept_2024_playoffs",
            "0x4c396f3b0f4b001b58522883df201336c24b12f703a25044fbdcd02182c1d585",
            "mlb-dodgers-vs-padres",
        ),
    ]
    for label, cid, event_slug in check_markets:
        resp = intel_call(AGENT_MARKETS, {"condition_id": cid, "closed": "True"}, {"limit": 5, "offset": 0}, api_key)
        results = resp.get("data", {}).get("results", []) if resp else []
        intel_winner = results[0].get("winning_outcome") if results else None

        r = http_get(f"{GAMMA_URL}/events", {"slug": event_slug})
        gamma_winner = None
        if r is not None and r.status_code == 200:
            events = r.json()
            if events:
                for m in events[0].get("markets", []):
                    if m.get("conditionId") == cid:
                        try:
                            outcomes = json.loads(m.get("outcomes", "[]"))
                            prices = json.loads(m.get("outcomePrices", "[]"))
                        except json.JSONDecodeError:
                            outcomes, prices = [], []
                        # AMM-era markets settle to prices like 0.9999992, not
                        # exactly "1" — take argmax with a >0.99 threshold.
                        if outcomes and prices:
                            pairs = [(o, float(p)) for o, p in zip(outcomes, prices)]
                            best = max(pairs, key=lambda x: x[1])
                            if best[1] > 0.99:
                                gamma_winner = best[0]
                        break
        agree = (intel_winner is not None and gamma_winner is not None and intel_winner == gamma_winner)
        log(
            "P8",
            market=label,
            condition_id=cid,
            intel_winning_outcome=intel_winner,
            gamma_winning_outcome=gamma_winner,
            agree=agree,
        )
        time.sleep(0.3)


PROBES = {
    "P1": lambda key: probe_p1(key),
    "P2": lambda key: probe_p2(key),
    "P3": lambda key: probe_p3(key),
    "P4": lambda key: probe_p4(key),
    "P5": lambda key: probe_p5(key),
    "P6": lambda key: probe_p6(key),
    "P7": lambda key: probe_p7(),
    "P8": lambda key: probe_p8(key),
}


def main() -> None:
    requested = [a.upper() for a in sys.argv[1:]] or list(PROBES.keys())
    unknown = [p for p in requested if p not in PROBES]
    if unknown:
        print(f"Unknown probe(s): {unknown}. Valid: {list(PROBES.keys())}", file=sys.stderr)
        sys.exit(1)

    api_key = load_api_key()
    if not api_key:
        print("WARNING: intelligence_api_key not found in .env — intelligence-API probes will be skipped.", file=sys.stderr)

    for probe_id in requested:
        needs_key = probe_id != "P7"
        if needs_key and not api_key:
            print(f"\n=== {probe_id}: SKIPPED (no intelligence_api_key) ===")
            continue
        PROBES[probe_id](api_key)


if __name__ == "__main__":
    main()
