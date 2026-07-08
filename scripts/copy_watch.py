#!/usr/bin/env python3
"""Copy-trading paper console for the persistent-winners program.

Watches the tracked wallet set (data/copy_watch_config.json — the 6
persistent-winner survivors plus one paper-only tier-2 near-miss candidate,
see data/persistent_winners.json) for new pre-game MLB fills and records them
as paper "copy" signals in data/copy_ledger.json. Strategy per
reports/10_mlb7_consensus.md: copy SOLO pre-game entries, stand down (stake=0)
on conflicts between tracked wallets. No real money moves — this is a forward
paper test of whether copying these wallets is viable before risking any.

Modes:
  --once     one poll+settle cycle then exit
  --loop     poll every ~120s forever (settle runs at the end of each cycle)
  --settle   resolve pending signals against real outcomes, then exit
  --status   print ledger summary stats (the numbers to look at before real money)
  --test     push synthetic fills through the real signal path to verify
             ledger/pnl math, then remove them (no real data touched)

Run alongside the dashboard:
  nohup .venv/bin/python3 scripts/copy_watch.py --loop >> data/lake/logs/copy_watch.log 2>&1 &
"""

import argparse
import datetime
import json
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from poll_live import api_call, fetch_orderbook, load_env_key, parse_slug, wallet_short
from tracker import query_resolved_outcome

NY_TZ = ZoneInfo("America/New_York")
BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data"
CACHE_DIR = DATA_DIR / "cache"
CONFIG_PATH = DATA_DIR / "copy_watch_config.json"
LEDGER_PATH = DATA_DIR / "copy_ledger.json"
OUTCOME_CACHE_PATH = CACHE_DIR / "outcome_cache.json"  # shared with backtest.py's resolved-outcome cache
SCHEDULE_PATH = DATA_DIR / "lake" / "schedule.parquet"

AGENT_TRADES = 556

POLL_INTERVAL_S = 120
OVERLAP_S = 60
INITIAL_LOOKBACK_S = 3600  # bootstrap window on first-ever cycle (no last_cycle_at yet)
PAPER_STAKE = 100.0

# code -> full name(s) as they appear in schedule.parquet's team columns.
# Self-contained copy of analysis/trader_metrics.py's TEAM_CODE_NAMES join
# table (read-only reuse of a static fact, not a dependency on the analysis
# pipeline) so first-pitch lookups don't require importing analysis/.
TEAM_CODE_NAMES = {
    "ari": {"Arizona Diamondbacks"},
    "atl": {"Atlanta Braves"},
    "bal": {"Baltimore Orioles"},
    "bos": {"Boston Red Sox"},
    "chc": {"Chicago Cubs"},
    "cin": {"Cincinnati Reds"},
    "cle": {"Cleveland Guardians"},
    "col": {"Colorado Rockies"},
    "cws": {"Chicago White Sox"},
    "det": {"Detroit Tigers"},
    "hou": {"Houston Astros"},
    "kc": {"Kansas City Royals"},
    "laa": {"Los Angeles Angels"},
    "lad": {"Los Angeles Dodgers"},
    "mia": {"Miami Marlins"},
    "mil": {"Milwaukee Brewers"},
    "min": {"Minnesota Twins"},
    "nym": {"New York Mets"},
    "nyy": {"New York Yankees"},
    "oak": {"Oakland Athletics", "Athletics"},
    "phi": {"Philadelphia Phillies"},
    "pit": {"Pittsburgh Pirates"},
    "sd": {"San Diego Padres"},
    "sea": {"Seattle Mariners"},
    "sf": {"San Francisco Giants"},
    "stl": {"St. Louis Cardinals"},
    "tb": {"Tampa Bay Rays"},
    "tex": {"Texas Rangers"},
    "tor": {"Toronto Blue Jays"},
    "wsh": {"Washington Nationals"},
}


# ── Config / ledger / cache I/O ─────────────────────────────────────────

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_ledger():
    if LEDGER_PATH.exists():
        with open(LEDGER_PATH) as f:
            return json.load(f)
    return {"generated_at": "", "last_cycle_at": "", "signals": []}


def save_ledger(ledger):
    ledger["generated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(LEDGER_PATH, "w") as f:
        json.dump(ledger, f, indent=2, default=str)


def load_outcome_cache():
    if OUTCOME_CACHE_PATH.exists():
        with open(OUTCOME_CACHE_PATH) as f:
            return json.load(f)
    return {}


def save_outcome_cache(cache):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTCOME_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


# ── First-pitch lookup (schedule.parquet) ───────────────────────────────

def load_first_pitch_index():
    """(official_date, frozenset({away_code, home_code})) -> earliest first-pitch
    UTC datetime, built from data/lake/schedule.parquet. Mirrors the join logic
    in analysis/trader_metrics.py's build_first_pitch_map, scoped down to just
    the schedule (we match directly off trade slugs, no markets.parquet needed).
    Returns {} on any failure -- callers fall back to a slug-date heuristic.
    """
    if not SCHEDULE_PATH.exists():
        return {}
    try:
        import pandas as pd
    except ImportError:
        print("  [copy_watch: pandas not available, first-pitch lookups will use slug-date fallback only]")
        return {}
    try:
        sched = pd.read_parquet(SCHEDULE_PATH)
        name_to_code = {name: code for code, names in TEAM_CODE_NAMES.items() for name in names}
        sched["away_code"] = sched["away_team_name"].map(name_to_code)
        sched["home_code"] = sched["home_team_name"].map(name_to_code)
        sched = sched.dropna(subset=["away_code", "home_code"])
        sched["game_date_utc"] = pd.to_datetime(sched["game_date_utc"], utc=True, errors="coerce")
        index = {}
        for r in sched.itertuples(index=False):
            if pd.isna(r.game_date_utc):
                continue
            key = (r.official_date, frozenset({r.away_code, r.home_code}))
            if key not in index or r.game_date_utc < index[key]:
                index[key] = r.game_date_utc
        return {k: v.to_pydatetime() for k, v in index.items()}
    except Exception as e:
        print(f"  [copy_watch: schedule lookup build failed: {e}; using slug-date fallback only]")
        return {}


def first_pitch_for_trade(slug, fp_index):
    """First-pitch UTC datetime for a trade's slug, or None if unmatched
    (caller falls back to a slug-date heuristic)."""
    parsed = parse_slug(slug)
    teams, date = parsed.get("teams"), parsed.get("date")
    if not teams or not date:
        return None
    return fp_index.get((date, frozenset(teams)))


# ── Helpers ──────────────────────────────────────────────────────────────

def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def trade_key(tr):
    """Dedupe key for a raw trade record -- same fallback chain poll_live uses."""
    return tr.get("id") or tr.get("transaction_hash") or f"{tr.get('timestamp')}|{tr.get('token_id')}|{tr.get('size')}"


def fetch_wallet_trades(wallet, start_time_s, api_key):
    resp = api_call(
        AGENT_TRADES,
        {"proxy_wallet": wallet, "condition_id": "ALL", "start_time": str(start_time_s)},
        {"limit": 200, "offset": 0},
        api_key,
    )
    return resp.get("data", {}).get("results", []) or []


def find_conflicts(ledger, condition_id, outcome, wallet):
    """Other tracked wallets' BUY signals on this market with a different
    outcome -- the 'net-opposite position' the stand-down rule checks for."""
    return [
        {"wallet": s["wallet"], "pseudonym": s["pseudonym"], "outcome": s["outcome"]}
        for s in ledger["signals"]
        if s["condition_id"] == condition_id
        and s["wallet"] != wallet
        and s["side"] == "BUY"
        and s["outcome"] != outcome
    ]


def compute_pnl(stake, executable_ask, won):
    """paper P&L for a BUY-side copy: stake * (payout - ask) / ask."""
    if stake <= 0 or not executable_ask:
        return 0.0
    payout = 1.0 if won else 0.0
    return round(stake * (payout - executable_ask) / executable_ask, 2)


# ── Signal construction ──────────────────────────────────────────────────

def build_signal(tr, wallet_cfg, ledger, fp_index, api_key, now_dt, today_str, fetch_ob=None):
    """Build a copy signal from one raw trade record, or return None if it's
    not a copyable pre-game MLB game-market fill (futures/props, unparsable,
    or first pitch already passed). Does not mutate the ledger.
    """
    fetch_ob = fetch_ob or (lambda token_id: fetch_orderbook(token_id, api_key))

    slug = tr.get("slug", "")
    if not slug.startswith("mlb-"):
        return None
    parsed = parse_slug(slug)
    if parsed.get("market_type") == "futures" or not parsed.get("teams"):
        return None

    ts_fill = _parse_iso(tr.get("timestamp"))
    if ts_fill is None:
        return None

    game_date = parsed.get("date")
    fp = first_pitch_for_trade(slug, fp_index)
    if fp is not None:
        if fp <= now_dt:
            return None  # first pitch already passed -- not a forward pre-game signal
        minutes_to_first_pitch = round((fp - now_dt).total_seconds() / 60, 1)
    else:
        if not game_date or game_date < today_str:
            return None  # can't confirm first pitch, and slug date is in the past -- skip
        minutes_to_first_pitch = None

    condition_id = tr.get("condition_id", "")
    outcome = tr.get("outcome", "")
    side = tr.get("side", "")
    their_price = float(tr.get("price") or 0)
    their_size = float(tr.get("size") or 0)
    detection_latency_s = round((now_dt - ts_fill).total_seconds(), 1)

    conflict_with = []
    executable_ask = ask_depth = None
    stand_down_reason = None
    paper_stake = 0.0

    if side == "SELL":
        # Copying an exit without their entry is meaningless -- flag, don't copy.
        stand_down_reason = "sell_fill"
    else:
        conflict_with = find_conflicts(ledger, condition_id, outcome, wallet_cfg["wallet"])
        token_id = tr.get("token_id")
        raw_ob = fetch_ob(token_id) if token_id else None
        if raw_ob and raw_ob.get("asks"):
            best_price, best_size = min(raw_ob["asks"], key=lambda lvl: lvl[0])
            executable_ask, ask_depth = best_price, best_size
        if conflict_with:
            stand_down_reason = "conflict"
        elif executable_ask is None:
            stand_down_reason = "no_pricing"
        else:
            paper_stake = PAPER_STAKE

    return {
        "id": trade_key(tr),
        "ts_detected": now_dt.isoformat(),
        "wallet": wallet_cfg["wallet"],
        "pseudonym": wallet_cfg.get("pseudonym", ""),
        "tier": wallet_cfg.get("tier"),
        "condition_id": condition_id,
        "slug": slug,
        "outcome": outcome,
        "side": side,
        "their_price": their_price,
        "their_size": their_size,
        "their_fill_ts": tr.get("timestamp"),
        "detection_latency_s": detection_latency_s,
        "executable_ask": executable_ask,
        "ask_depth": ask_depth,
        "minutes_to_first_pitch": minutes_to_first_pitch,
        "conflict": bool(conflict_with),
        "conflict_with": conflict_with,
        "stand_down_reason": stand_down_reason,
        "paper_stake": paper_stake,
        "status": "skipped_sell" if side == "SELL" else "pending",
        "settled_at": None,
        "actual_outcome": None,
        "payout": None,
        "pnl": None,
    }


# ── Cycle: fetch + detect ───────────────────────────────────────────────

def run_cycle(ledger, config, api_key, fp_index):
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    today_str = now_dt.astimezone(NY_TZ).strftime("%Y-%m-%d")

    last_cycle_iso = ledger.get("last_cycle_at") or ""
    if last_cycle_iso:
        last_dt = _parse_iso(last_cycle_iso)
        start_s = int(last_dt.timestamp()) - OVERLAP_S if last_dt else int(now_dt.timestamp()) - INITIAL_LOOKBACK_S
    else:
        start_s = int(now_dt.timestamp()) - INITIAL_LOOKBACK_S

    existing_ids = {s["id"] for s in ledger["signals"]}
    new_count = 0

    for wc in config["wallets"]:
        if "mlb" not in (wc.get("sports") or ["mlb"]):
            continue
        try:
            trades = fetch_wallet_trades(wc["wallet"], start_s, api_key)
        except Exception as e:
            print(f"  [{wallet_short(wc['wallet'])} fetch error: {e}]")
            continue

        n_new_here = 0
        for tr in trades:
            key = trade_key(tr)
            if key in existing_ids:
                continue
            sig = build_signal(tr, wc, ledger, fp_index, api_key, now_dt, today_str)
            if sig is None:
                continue
            ledger["signals"].append(sig)
            existing_ids.add(key)
            n_new_here += 1
            new_count += 1
        print(f"  {wc.get('pseudonym', '?')} ({wallet_short(wc['wallet'])}) tier{wc.get('tier')}: "
              f"{len(trades)} fetched, {n_new_here} new signal(s)")

    ledger["last_cycle_at"] = now_dt.isoformat()
    print(f"\nCycle done: {new_count} new signal(s) recorded.")
    return new_count


# ── Settlement ───────────────────────────────────────────────────────────

def settle_signals(ledger, api_key, today_str):
    """Resolve pending signals whose game date has passed, via the
    outcome_cache/agent-574 pattern from scripts/backtest.py (shared cache
    file, so an outcome already resolved by another script is reused)."""
    cache = load_outcome_cache()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    newly = 0

    for s in ledger["signals"]:
        if s["status"] != "pending":
            continue
        game_date = parse_slug(s.get("slug", "")).get("date")
        if not game_date or game_date >= today_str:
            continue  # game hasn't happened yet (or unparsable) -- nothing to resolve

        cid = s["condition_id"]
        cached = cache.get(cid)
        winning = cached.get("winning_outcome") if isinstance(cached, dict) else None
        if winning is None:
            winning = query_resolved_outcome(cid, api_key)
            if winning:
                cache[cid] = {"winning_outcome": winning, "resolved_at": now_iso}
            time.sleep(0.3)
        if not winning:
            continue

        won = (s["outcome"] == winning)
        s["actual_outcome"] = winning
        s["status"] = "won" if won else "lost"
        s["settled_at"] = now_iso
        s["payout"] = 1.0 if won else 0.0
        s["pnl"] = compute_pnl(s["paper_stake"], s["executable_ask"], won)
        newly += 1

    if newly:
        save_outcome_cache(cache)
        print(f"Settled {newly} signal(s).")
    return newly


# ── Status ───────────────────────────────────────────────────────────────

def compute_status(ledger):
    signals = ledger.get("signals", [])
    settled = [s for s in signals if s["status"] in ("won", "lost")]
    pending = [s for s in signals if s["status"] == "pending"]
    sell_skipped = [s for s in signals if s["status"] == "skipped_sell"]
    staked = [s for s in settled if s.get("paper_stake", 0) > 0]

    total_pnl = round(sum(s.get("pnl") or 0 for s in staked), 2)
    total_staked = round(sum(s.get("paper_stake", 0) for s in staked), 2)
    roi = round(total_pnl / total_staked, 4) if total_staked else 0.0

    latencies = [s["detection_latency_s"] for s in signals if s.get("detection_latency_s") is not None]
    mean_latency = round(sum(latencies) / len(latencies), 1) if latencies else 0.0

    slippages = [
        (s["executable_ask"] - s["their_price"]) / s["their_price"]
        for s in signals
        if s.get("executable_ask") is not None and s.get("their_price")
    ]
    mean_slippage = round(sum(slippages) / len(slippages), 4) if slippages else 0.0

    return {
        "total": len(signals),
        "settled": len(settled),
        "pending": len(pending),
        "skipped_sell": len(sell_skipped),
        "conflicts": sum(1 for s in signals if s.get("conflict")),
        "staked_signals": len(staked),
        "total_staked": total_staked,
        "paper_pnl": total_pnl,
        "roi": roi,
        "mean_detection_latency_s": mean_latency,
        "mean_ask_slippage": mean_slippage,
    }


def print_status(ledger):
    st = compute_status(ledger)
    print("=" * 64)
    print("COPY-WATCH STATUS")
    print("=" * 64)
    print(f"Signals:      {st['total']} total  |  {st['settled']} settled  |  {st['pending']} pending  |  "
          f"{st['skipped_sell']} sell-fills (flagged, not copied)")
    print(f"Conflicts:    {st['conflicts']} stood down (stake=0, stand-down rule)")
    print(f"Staked:       {st['staked_signals']} signal(s), ${st['total_staked']:,.2f} deployed (settled, solo/agree only)")
    print(f"Paper P&L:    ${st['paper_pnl']:+,.2f}")
    print(f"ROI (at executable prices): {st['roi'] * 100:+.1f}%")
    print(f"Mean detection latency:     {st['mean_detection_latency_s']:.1f}s")
    print(f"Mean ask-vs-their-price slippage: {st['mean_ask_slippage'] * 100:+.2f}%")
    print("=" * 64)


# ── Synthetic test (--test) ─────────────────────────────────────────────

def run_synthetic_test():
    """Push 3 synthetic fills through the real signal-building path -- a solo
    BUY, a conflicting BUY (opposite outcome, different wallet), and a SELL
    fill -- to prove ledger append/conflict/pnl math end-to-end, then remove
    them so the real ledger is left exactly as it was. Uses a fake orderbook
    fetcher (no network) and a synthetic condition_id/slug that can never
    collide with a real market.
    """
    print("Running synthetic signal-path test (no real data touched)...\n")
    config = load_config()
    ledger = load_ledger()
    before_count = len(ledger["signals"])

    fp_index = {}  # empty -> exercises the slug-date fallback branch
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    today_str = now_dt.astimezone(NY_TZ).strftime("%Y-%m-%d")
    future_date = (now_dt + datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    fake_cid = "0xSYNTHETIC_TEST_CONDITION_DO_NOT_USE"
    fake_slug = f"mlb-zzz-yyy-{future_date}"

    tier1_wallet = next(w for w in config["wallets"] if w["tier"] == 1)
    tier2_wallet = next(w for w in config["wallets"] if w["tier"] == 2)

    fill_a = {  # solo BUY -> should stake $100 @ ask 0.45
        "id": "SYNTH-A", "condition_id": fake_cid, "outcome": "Zzz Zeta",
        "price": 0.40, "side": "BUY", "size": 50, "slug": fake_slug,
        "timestamp": (now_dt - datetime.timedelta(seconds=45)).isoformat().replace("+00:00", "Z"),
        "token_id": "SYNTH_TOKEN_A",
    }
    sig_a = build_signal(fill_a, tier1_wallet, ledger, fp_index, None, now_dt, today_str,
                          fetch_ob=lambda t: {"bids": [[0.44, 100]], "asks": [[0.45, 200]], "timestamp": ""})
    assert sig_a is not None, "expected a solo pre-game BUY fill to produce a signal"
    ledger["signals"].append(sig_a)

    fill_b = {  # conflicting BUY: same market, opposite outcome, different tracked wallet
        "id": "SYNTH-B", "condition_id": fake_cid, "outcome": "Yyy Yankee",
        "price": 0.55, "side": "BUY", "size": 30, "slug": fake_slug,
        "timestamp": (now_dt - datetime.timedelta(seconds=20)).isoformat().replace("+00:00", "Z"),
        "token_id": "SYNTH_TOKEN_B",
    }
    sig_b = build_signal(fill_b, tier2_wallet, ledger, fp_index, None, now_dt, today_str,
                          fetch_ob=lambda t: {"bids": [[0.54, 50]], "asks": [[0.56, 80]], "timestamp": ""})
    assert sig_b is not None
    ledger["signals"].append(sig_b)

    fill_c = {  # SELL fill on the same market/outcome as A -- must be flagged, never copied
        "id": "SYNTH-C", "condition_id": fake_cid, "outcome": "Zzz Zeta",
        "price": 0.60, "side": "SELL", "size": 20, "slug": fake_slug,
        "timestamp": (now_dt - datetime.timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
        "token_id": "SYNTH_TOKEN_A",
    }
    sig_c = build_signal(fill_c, tier1_wallet, ledger, fp_index, None, now_dt, today_str,
                          fetch_ob=lambda t: {"bids": [[0.59, 10]], "asks": [[0.61, 10]], "timestamp": ""})
    assert sig_c is not None
    ledger["signals"].append(sig_c)

    print("Injected signals (before settlement):")
    for sig in (sig_a, sig_b, sig_c):
        print(json.dumps(sig, indent=2))

    # Force-settle without a live agent-574 call (the condition_id is
    # synthetic and would never resolve for real): Zzz Zeta wins, so sig_a
    # (solo BUY on Zzz Zeta) wins and sig_b (conflict BUY on Yyy Yankee) loses.
    sig_a["status"], sig_a["actual_outcome"], sig_a["payout"] = "won", sig_a["outcome"], 1.0
    sig_a["pnl"] = compute_pnl(sig_a["paper_stake"], sig_a["executable_ask"], won=True)

    sig_b["status"], sig_b["actual_outcome"], sig_b["payout"] = "lost", sig_a["outcome"], 0.0
    sig_b["pnl"] = compute_pnl(sig_b["paper_stake"], sig_b["executable_ask"], won=False)

    print("\nSettlement math check:")
    expected_a = round(100 * (1.0 - 0.45) / 0.45, 2)
    print(f"  sig_a solo BUY: stake=${sig_a['paper_stake']} ask={sig_a['executable_ask']} WON -> "
          f"pnl=${sig_a['pnl']}  (expected 100*(1-0.45)/0.45={expected_a})  "
          f"{'OK' if sig_a['pnl'] == expected_a else 'MISMATCH'}")
    print(f"  sig_b conflict BUY: conflict={sig_b['conflict']} stake=${sig_b['paper_stake']} -> "
          f"pnl=${sig_b['pnl']}  {'OK (stood down, zero stake -> zero pnl)' if sig_b['paper_stake'] == 0 and sig_b['pnl'] == 0 else 'MISMATCH'}")
    print(f"  sig_c SELL fill: status={sig_c['status']} stand_down_reason={sig_c['stand_down_reason']} "
          f"stake=${sig_c['paper_stake']}  {'OK (flagged, not copied)' if sig_c['status'] == 'skipped_sell' and sig_c['paper_stake'] == 0 else 'MISMATCH'}")

    assert sig_a["conflict"] is False and sig_a["paper_stake"] == PAPER_STAKE
    assert sig_b["conflict"] is True and sig_b["paper_stake"] == 0.0
    assert sig_c["status"] == "skipped_sell" and sig_c["paper_stake"] == 0.0
    print("\nAll assertions passed.")

    ledger["signals"] = [s for s in ledger["signals"] if not s["id"].startswith("SYNTH-")]
    after_count = len(ledger["signals"])
    assert after_count == before_count, "synthetic signals were not fully removed"
    save_ledger(ledger)
    print(f"\nRemoved synthetic signals from the ledger -- count before test: {before_count}, "
          f"after cleanup: {after_count} (restored). Ledger file untouched beyond this round-trip.")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Copy-trading paper console for persistent-winners MLB wallets.")
    ap.add_argument("--once", action="store_true", help="Run one poll+settle cycle then exit.")
    ap.add_argument("--loop", action="store_true", help="Poll every ~120s forever.")
    ap.add_argument("--settle", action="store_true", help="Resolve pending signals against real outcomes, then exit.")
    ap.add_argument("--status", action="store_true", help="Print ledger summary stats.")
    ap.add_argument("--test", action="store_true", help="Synthetic signal-path test (see module docstring).")
    args = ap.parse_args()

    if not any([args.once, args.loop, args.settle, args.status, args.test]):
        ap.print_help()
        return

    if args.status:
        print_status(load_ledger())
        return

    if args.test:
        run_synthetic_test()
        return

    api_key = load_env_key()
    config = load_config()
    fp_index = load_first_pitch_index()

    if args.settle:
        ledger = load_ledger()
        settle_signals(ledger, api_key, datetime.datetime.now(NY_TZ).strftime("%Y-%m-%d"))
        save_ledger(ledger)
        print_status(ledger)
        return

    if args.once:
        ledger = load_ledger()
        run_cycle(ledger, config, api_key, fp_index)
        settle_signals(ledger, api_key, datetime.datetime.now(NY_TZ).strftime("%Y-%m-%d"))
        save_ledger(ledger)
        print_status(ledger)
        return

    if args.loop:
        print(f"Starting copy_watch loop (interval {POLL_INTERVAL_S}s). Ctrl-C to stop.")
        while True:
            ledger = load_ledger()
            try:
                run_cycle(ledger, config, api_key, fp_index)
                settle_signals(ledger, api_key, datetime.datetime.now(NY_TZ).strftime("%Y-%m-%d"))
            except Exception as e:
                print(f"  [cycle error: {e}]")
            save_ledger(ledger)
            time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
