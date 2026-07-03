#!/usr/bin/env python3
"""
Compute MLB market sentiment from top trader activity.

Aggregates trades from top-tier/watchlist MLB traders into per-market
sentiment scores and per-game summaries.

Output -> data/sentiment_scores.json
"""

import json, os, re, datetime
from pathlib import Path
from collections import defaultdict, Counter

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_DIR = DATA_DIR / "cache"
IN_PATH = DATA_DIR / "top_mlb_traders.json"
OUT_PATH = DATA_DIR / "sentiment_scores.json"

# Slug pattern matching
SLUG_RE = re.compile(
    r"^mlb-(?P<team1>[a-z]+)-(?P<team2>[a-z]+)-(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:-(?P<type>total|spread|nrfi))?"
    r"(?:-(?P<side>home|away))?"
    r"(?:-(?P<line>[^-]+))?"
    r"(?:-(?P<line2>[^-]+))?"
    r"$"
)


def parse_slug(slug):
    """Extract structured info from an MLB slug."""
    if slug.startswith("will-") or slug.startswith("1-mlb-") or slug.startswith("2-mlb-") or slug.startswith("3-mlb-"):
        return {"event_slug": "futures-props", "market_type": "futures", "teams": None, "date": None}
    m = SLUG_RE.match(slug)
    if not m:
        return {"event_slug": "other", "market_type": "other", "teams": None, "date": None}
    g = m.groupdict()
    event_slug = f"mlb-{g['team1']}-{g['team2']}-{g['date']}"
    market_type = g.get("type") or "moneyline"
    return {
        "event_slug": event_slug,
        "market_type": market_type,
        "teams": (g["team1"], g["team2"]),
        "date": g["date"],
        "side": g.get("side"),
        "line": g.get("line"),
    }


def is_market_maker(t):
    """Check if a trader shows market-maker or HFT behavior."""
    flags = set(t.get("behavioral_flags") or [])
    return "timing_anomaly" in flags or "sybil_risk" in flags

def load_traders():
    """Load top-tier and watchlist traders with computed weights, excluding MM/HFT."""
    with open(IN_PATH) as f:
        data = json.load(f)
    all_traders = data["top_tier"] + data["watchlist"]

    # Filter out market makers / HFT
    traders = [t for t in all_traders if not is_market_maker(t)]
    excluded = [t for t in all_traders if is_market_maker(t)]
    if excluded:
        print(f"  Excluded {len(excluded)} MM/HFT traders")
        for t in excluded:
            print(f"    {t['wallet'][:6]}...{t['wallet'][-4:]} flags={t.get('behavioral_flags')}")
    print(f"  Using {len(traders)}/{len(all_traders)} traders (filtered MM/HFT)")

    # Compute weight for each trader
    max_pnl = max((t["baseball_pnl_15d"] for t in traders if t["baseball_pnl_15d"] > 0), default=1)
    for t in traders:
        pnl = max(t["baseball_pnl_15d"], 0)
        wr = t["win_rate"] or 0.5
        sharpe = max(t["sharpe_ratio"] or 0, 0)
        human = (t["human_likeness_score"] or 50) / 100.0

        # Composite weight: PnL normalized + WR bonus + Sharpe bonus + human score
        pnl_w = pnl / max_pnl
        wr_w = max(wr - 0.5, 0) * 2
        sharpe_w = min(sharpe / 2.0, 1.0)
        weight = 0.5 * pnl_w + 0.2 * wr_w + 0.15 * sharpe_w + 0.15 * human
        t["_weight"] = round(weight, 4)

    return {t["wallet"]: t for t in traders}


def load_all_trades():
    """Load all cached MLB trades, enriched with slug metadata."""
    trades_by_cid = defaultdict(list)
    total = 0
    for fname in os.listdir(CACHE_DIR):
        if not fname.startswith("mtrades_"):
            continue
        with open(os.path.join(CACHE_DIR, fname)) as f:
            cached = json.load(f)
        wallet = cached["wallet"]
        for d in cached.get("mlb_trade_details", []):
            cid = d["condition_id"]
            slug = d.get("slug", "")
            parsed = parse_slug(slug)
            trades_by_cid[cid].append({
                "wallet": wallet,
                "condition_id": cid,
                "slug": slug,
                "outcome": d.get("outcome", "?"),
                "side": d.get("side"),
                "size": d.get("size", 0) or 0,
                "price": d.get("price", 0) or 0,
                "timestamp": d.get("timestamp", ""),
                "notional": (d.get("size", 0) or 0) * (d.get("price", 0) or 0),
                **parsed,
            })
            total += 1
    return trades_by_cid, total


def compute_market_sentiment(cid, trades, trader_idx):
    """Compute sentiment for a single market (condition_id)."""
    if not trades:
        return None

    # Per-outcome aggregation
    outcomes = defaultdict(lambda: {"weighted_volume": 0.0, "trader_count": 0, "trader_set": set(), "trades": []})

    for tr in trades:
        outcome = tr["outcome"]
        t = trader_idx.get(tr["wallet"])
        if not t:
            continue
        weight = t["_weight"]
        notional = tr["notional"]
        side = tr["side"]

        # BUY = betting on this outcome, SELL = betting against
        signal = notional * weight
        if side == "SELL":
            signal = -signal

        o = outcomes[outcome]
        o["weighted_volume"] += signal
        o["trader_count"] += 1
        o["trader_set"].add(tr["wallet"])
        o["trades"].append(tr)

    if not outcomes:
        return None

    total_weighted = sum(o["weighted_volume"] for o in outcomes.values())
    if total_weighted == 0:
        return None

    # Sort outcomes by weighted volume
    sorted_outcomes = sorted(outcomes.items(), key=lambda x: -abs(x[1]["weighted_volume"]))

    # Sentiment: for the top outcome, what fraction of total volume does it represent?
    top_outcome, top_data = sorted_outcomes[0]
    top_fraction = top_data["weighted_volume"] / total_weighted if total_weighted else 0

    # Conviction: how much does the top outcome dominate?
    if len(sorted_outcomes) >= 2:
        second_fraction = abs(sorted_outcomes[1][1]["weighted_volume"]) / total_weighted
        conviction = abs(top_fraction - second_fraction) / max(top_fraction, second_fraction) if max(top_fraction, second_fraction) > 0 else 0
    else:
        conviction = 1.0

    # Number of unique traders
    all_traders = set()
    for o in outcomes.values():
        all_traders.update(o["trader_set"])

    # Date range
    timestamps = [tr.get("timestamp", "") for tr in trades if tr.get("timestamp")]
    timestamps.sort()
    first_date = timestamps[0][:10] if timestamps else ""
    last_date = timestamps[-1][:10] if timestamps else ""

    return {
        "condition_id": cid,
        "slug": trades[0].get("slug", ""),
        "market_type": trades[0].get("market_type", "other"),
        "event_slug": trades[0].get("event_slug", ""),
        "first_trade_date": first_date,
        "last_trade_date": last_date,
        "outcomes": {
            oc: {
                "weighted_volume": round(od["weighted_volume"], 2),
                "trader_count": len(od["trader_set"]),
                "trade_count": len(od["trades"]),
            }
            for oc, od in sorted_outcomes
        },
        "top_outcome": top_outcome,
        "top_weighted_fraction": round(abs(top_fraction), 4),
        "conviction": round(abs(conviction), 4),
        "total_weighted_volume": round(total_weighted, 2),
        "unique_traders": len(all_traders),
        "total_trade_events": len(trades),
    }


def rollup_to_game(market_sentiments):
    """Roll up per-market sentiments into per-game summaries."""
    games = defaultdict(lambda: {
        "markets": [],
        "total_trade_events": 0,
        # Per-market data only exposes trader counts (not wallets), so the
        # game-level figure is the max market-level count, a lower bound on
        # the true cross-market union.
        "unique_traders": 0,
        "moneyline": None,
        "total": None,
        "spread": None,
    })

    for ms in market_sentiments:
        if not ms:
            continue
        event_slug = ms.get("event_slug") or "unknown"
        g = games[event_slug]
        g["markets"].append(ms)
        g["total_trade_events"] += ms["total_trade_events"]
        g["unique_traders"] = max(g["unique_traders"], ms["unique_traders"])

        # Categorize by market type
        mtype = ms.get("market_type")
        if mtype == "moneyline":
            g["moneyline"] = ms
        elif mtype == "total":
            g["total"] = ms
        elif mtype == "spread":
            g["spread"] = ms

    # Build output
    result = {}
    for event_slug, g in sorted(games.items()):
        top_markets = sorted(g["markets"], key=lambda m: -m["total_trade_events"])[:5]
        result[event_slug] = {
            "event_slug": event_slug,
            "market_count": len(g["markets"]),
            "total_trade_events": g["total_trade_events"],
            "unique_traders": g["unique_traders"],
            "moneyline": g["moneyline"],
            "total": g["total"],
            "spread": g["spread"],
            "top_markets": top_markets,
        }
    return result


def main():
    print("Loading traders...")
    trader_idx = load_traders()
    print(f"  {len(trader_idx)} traders loaded")

    print("Loading trade data...")
    trades_by_cid, total_trades = load_all_trades()
    print(f"  {total_trades} trade events across {len(trades_by_cid)} markets")

    print("Computing per-market sentiment...")
    sentiments = []
    for cid, trades in trades_by_cid.items():
        ms = compute_market_sentiment(cid, trades, trader_idx)
        if ms:
            sentiments.append(ms)

    sentiments.sort(key=lambda m: -m["total_weighted_volume"])

    print("Rolling up to game-level summaries...")
    games = rollup_to_game(sentiments)

    # Top consensus markets: require at least 3 unique traders and 5 trade events
    # Rank by conviction * sqrt(traders) * log(events) to surface real consensus
    consensus = sorted(
        [s for s in sentiments
         if s["conviction"] >= 0.3 and s["unique_traders"] >= 3 and s["total_trade_events"] >= 5],
        key=lambda m: -(
            m["conviction"]
            * (m["unique_traders"] ** 0.5)
            * (m["total_trade_events"] ** 0.3)
        ),
    )

    # Overall date range
    first_dates = [m["first_trade_date"] for m in sentiments if m.get("first_trade_date")]
    last_dates = [m["last_trade_date"] for m in sentiments if m.get("last_trade_date")]
    output = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "summary": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "total_trade_events": total_trades,
            "unique_markets": len(sentiments),
            "unique_games": len(games),
            "unique_traders_in_trades": len(set(
                tr["wallet"]
                for trades in trades_by_cid.values()
                for tr in trades
            )),
            "data_start_date": min(first_dates) if first_dates else "",
            "data_end_date": max(last_dates) if last_dates else "",
        },
        "by_market": sorted(
            sentiments,
            key=lambda m: -(m["total_weighted_volume"]),
        ),
        "by_game": games,
        "top_consensus": consensus[:20],
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nDone! -> {OUT_PATH}")
    print(f"  Markets scored:     {len(sentiments)}")
    print(f"  Games with data:    {len(games)}")
    print(f"  Top consensus:      {len(consensus)} markets")


if __name__ == "__main__":
    main()
