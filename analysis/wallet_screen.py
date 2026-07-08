#!/usr/bin/env python3
"""External-reality screen for trader-edge wallet candidates.

Guards against survivorship bias inside a single category screen: a wallet
can look like a specialist inside our MLB (or NFL/NBA) sample while being,
in aggregate across every category they trade on Polymarket, a net loser --
i.e. a busted gambler whose one lucky category streak we'd otherwise mistake
for skill. Case that motivated this module: wallet
0xa5dcb282cab760e31df1f3f5c18350731c95ec43 ("Yikes110") shows +$28,280 on
the MLB persistent-winners screen (reports/08) but -$262,176 LIFETIME
all-category PnL on Polymarket -- a busted gambler, not a specialist.

Data sources (all external to our lake except last-trade timestamp):
  - Lifetime all-category PnL + display name: GET
    lb-api.polymarket.com/profit?window=all&limit=1&address=<wallet>
    -- response is a list of one record; `amount` is lifetime PnL,
    `pseudonym` is the wallet's display name.
  - Current open-position value: GET
    data-api.polymarket.com/value?user=<wallet> -- response is a list of
    one record; `value` is the current mark-to-market value of open
    positions.
  - Last-trade timestamp: max(timestamp) across data/lake/trades*.parquet
    (our own settlement-grade lake -- MLB's trades.parquet plus
    trades_nfl.parquet / trades_nba.parquet, whichever exist) for the
    wallet. Not an API call.

Verdict rules (thresholds configurable via function args):
  FAIL  if lifetime PnL < fail_threshold (default -$10,000): net loser
        across their whole trading history -- our category profit is noise
        inside a losing account, not evidence of skill.
  WARN  if lifetime PnL < warn_frac * category profit (default 25%), and
        lifetime PnL is still positive: a "category hot streak inside a
        modest lifetime record" pattern -- our category profit dominates
        their lifetime total, a red flag for luck/variance even without a
        net-loser verdict (e.g. Radahn-131: +$73.7k MLB profit vs +$18.7k
        lifetime PnL -- MLB profit is ~4x their entire lifetime total).
  OK    otherwise.
  UNKNOWN if the lifetime-PnL API call failed for this wallet (network
        error survived all retries) -- reported honestly, not silently
        defaulted to OK.
Staleness (independent of the FAIL/WARN/OK verdict): if no trade in
`stale_days` (default 30), a note is appended -- the wallet may no longer
be active/copyable even if its historical edge and lifetime PnL look fine.

This module is imported by BOTH analysis/persistent_winners.py (MLB) and
analysis/cross_sport_winners.py (NFL/NBA, as a mandatory final funnel
stage) -- the screen itself, the API calls, and the verdict logic live
here exactly once.

Usage:
    from wallet_screen import screen_wallets
    screens = screen_wallets(wallets, category_profit={"0xabc...": 12345.0})
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"

LB_API_PROFIT_URL = "https://lb-api.polymarket.com/profit"
DATA_API_VALUE_URL = "https://data-api.polymarket.com/value"

DEFAULT_FAIL_THRESHOLD = -10_000.0
DEFAULT_WARN_FRAC = 0.25
DEFAULT_STALE_DAYS = 30
DEFAULT_SLEEP_S = 1.0


# ---------------------------------------------------------------------------
# HTTP (GET, no auth, 429/network retry -- mirrors analysis/ingest/client.py's
# http_get pattern)
# ---------------------------------------------------------------------------
def _http_get_json(url: str, params: dict, retries: int = 5, timeout: int = 20) -> list | dict | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = (2**attempt) * 5
                print(f"    [wallet_screen 429, retry in {wait}s]")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (2**attempt) * 2
                print(f"    [wallet_screen error: {e}, retry in {wait}s]")
                time.sleep(wait)
            else:
                print(f"    [wallet_screen FAILED after {retries} attempts for {url} {params}: {e}]")
                return None
    return None


def _first_row(data: list | dict | None) -> dict | None:
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        return data
    return None


def fetch_lifetime_profit(wallet: str) -> dict:
    """{'pseudonym': str | None, 'lifetime_pnl': float | None}."""
    row = _first_row(_http_get_json(LB_API_PROFIT_URL, {"window": "all", "limit": 1, "address": wallet}))
    if row is None:
        return {"pseudonym": None, "lifetime_pnl": None}
    return {"pseudonym": row.get("pseudonym"), "lifetime_pnl": row.get("amount")}


def fetch_current_value(wallet: str) -> float | None:
    row = _first_row(_http_get_json(DATA_API_VALUE_URL, {"user": wallet}))
    return row.get("value") if row is not None else None


# ---------------------------------------------------------------------------
# Last-trade timestamp from our own lake (not an API call)
# ---------------------------------------------------------------------------
def last_trade_timestamps(wallets: list[str], lake_dir: Path = LAKE_DIR) -> dict[str, pd.Timestamp | None]:
    """max(timestamp) per wallet across every data/lake/trades*.parquet file
    that exists (MLB's trades.parquet plus any trades_<sport>.parquet)."""
    if not wallets:
        return {}
    glob_path = (lake_dir / "trades*.parquet").as_posix()
    con = duckdb.connect()
    wdf = pd.DataFrame({"wallet": wallets})
    con.register("wdf", wdf)
    q = f"""
        SELECT proxy_wallet AS wallet, max(timestamp) AS last_trade
        FROM read_parquet('{glob_path}', union_by_name=True)
        WHERE proxy_wallet IN (SELECT wallet FROM wdf)
        GROUP BY proxy_wallet
    """
    out = con.execute(q).fetchdf()
    con.close()
    idx = out.set_index("wallet")["last_trade"] if not out.empty else pd.Series(dtype="object")
    return {w: (idx.loc[w] if w in idx.index else None) for w in wallets}


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
def _verdict(lifetime_pnl: float | None, category_profit: float | None,
             fail_threshold: float, warn_frac: float) -> tuple[str, list[str]]:
    if lifetime_pnl is None:
        return "UNKNOWN", ["lifetime PnL unavailable (lb-api call failed)"]
    if lifetime_pnl < fail_threshold:
        return "FAIL", [
            f"lifetime PnL ${lifetime_pnl:,.0f} < fail threshold ${fail_threshold:,.0f} -- "
            "net loser across full trading history; category profit is noise inside a "
            "losing account, not evidence of skill"
        ]
    if category_profit is not None and category_profit > 0:
        ratio = lifetime_pnl / category_profit
        if ratio < warn_frac:
            return "WARN", [
                f"lifetime PnL ${lifetime_pnl:,.0f} is {ratio:.1%} of category profit "
                f"${category_profit:,.0f} (< {warn_frac:.0%} threshold) -- "
                "category-hot-streak-inside-loser pattern"
            ]
        if ratio < warn_frac * 1.5:
            # Not under the threshold, but close enough that the reader should see the
            # ratio explicitly rather than a bare OK hiding a near-miss (e.g. Radahn-131:
            # lifetime PnL is 25.3% of category profit, just above the 25% WARN cutoff).
            return "OK", [
                f"lifetime PnL ${lifetime_pnl:,.0f} is {ratio:.1%} of category profit "
                f"${category_profit:,.0f} -- close to the {warn_frac:.0%} WARN threshold, worth watching"
            ]
    return "OK", []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def screen_wallets(
    wallets: list[str],
    category_profit: dict[str, float] | None = None,
    fail_threshold: float = DEFAULT_FAIL_THRESHOLD,
    warn_frac: float = DEFAULT_WARN_FRAC,
    stale_days: int = DEFAULT_STALE_DAYS,
    sleep_s: float = DEFAULT_SLEEP_S,
    lake_dir: Path = LAKE_DIR,
) -> dict[str, dict]:
    """Per-wallet external-reality screen. `category_profit` is an optional
    {wallet: total_profit_full} map from the calling screen (MLB's 7,
    NFL/NBA's funnel survivors, ...) used for the WARN rule; if omitted,
    only the FAIL/staleness checks apply.

    Returns {wallet: {pseudonym, lifetime_pnl, current_value, last_trade
    (pd.Timestamp | None), days_since_last_trade, stale, verdict, notes}}.
    """
    category_profit = category_profit or {}
    print(f"Screening {len(wallets)} wallet(s) against external Polymarket APIs "
          f"(lifetime PnL, current value) + lake (last trade)...")
    last_trade = last_trade_timestamps(wallets, lake_dir=lake_dir)
    now = pd.Timestamp.now("UTC")

    out: dict[str, dict] = {}
    for w in wallets:
        profit_info = fetch_lifetime_profit(w)
        time.sleep(sleep_s)
        value = fetch_current_value(w)
        time.sleep(sleep_s)

        lt = last_trade.get(w)
        lt_ts = pd.Timestamp(lt).tz_convert("UTC") if lt is not None and pd.notna(lt) else None
        days_since = (now - lt_ts).days if lt_ts is not None else None
        stale = days_since is not None and days_since > stale_days

        verdict, notes = _verdict(profit_info["lifetime_pnl"], category_profit.get(w),
                                   fail_threshold, warn_frac)
        if stale:
            notes = notes + [f"no trade in {days_since}d (> {stale_days}d staleness threshold)"]

        print(f"  {w}: pseudonym={profit_info['pseudonym']} lifetime_pnl={profit_info['lifetime_pnl']} "
              f"current_value={value} verdict={verdict}{' STALE' if stale else ''}")

        out[w] = {
            "pseudonym": profit_info["pseudonym"],
            "lifetime_pnl": profit_info["lifetime_pnl"],
            "current_value": value,
            "last_trade": lt_ts,
            "days_since_last_trade": days_since,
            "stale": stale,
            "verdict": verdict,
            "notes": notes,
        }
    return out
