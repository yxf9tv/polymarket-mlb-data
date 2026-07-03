#!/usr/bin/env python3
"""
Poll live MLB markets + top trader activity for current/upcoming games.

Fetch open markets, pull recent trades (7 days) for all clean traders,
compute sentiment on active-only markets, write to data/live_signals.json.

Can run standalone or via server's /api/live/poll.
"""

import os, sys, json, time, datetime, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import requests

NY_TZ = ZoneInfo("America/New_York")

from tracker import update as update_tracker

def today_ny():
    """Get today's date string in America/New_York timezone."""
    return datetime.datetime.now(NY_TZ).strftime("%Y-%m-%d")

API_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = DATA_DIR / "live_signals.json"

AGENT_MARKETS = 574
AGENT_TRADES = 556
AGENT_ORDERBOOK = 572

MLB_SEARCH_SLUGS = ["mlb", "baseball", "world-series"]

# Speedup knobs: parallel API workers (tune down if 429s appear), incremental
# poll overlap buffer, and open-market discovery cache TTL.
MAX_WORKERS = 6
INCREMENTAL_OVERLAP_S = 600
OPEN_MARKETS_CACHE = CACHE_DIR / "open_markets.json"
OPEN_MARKETS_TTL_S = 45 * 60

SLUG_RE = re.compile(
    r"^mlb-(?P<team1>[a-z]+)-(?P<team2>[a-z]+)-(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:-(?P<type>total|spread|nrfi))?"
    r"(?:-(?P<side>home|away))?"
    r"(?:-(?P<line>[^-]+))?"
    r"(?:-(?P<line2>[^-]+))?"
    r"$"
)

MLB_KEYWORDS_RE = re.compile(
    r"\b(?:"
    r"mlb|baseball|world\s*series|"
    r"yankees|red\s*sox|dodgers|astros|braves|"
    r"brewers|cardinals|phillies|padres|giants|"
    r"blue\s*jays|orioles|rays|mariners|twins|"
    r"guardians|tigers|royals|athletics|rangers|"
    r"angels|white\s*sox|cubs|reds|pirates|"
    r"rockies|diamondbacks|marlins|nationals|"
    r"ohtani|judge|acuna"
    r")\b",
    re.IGNORECASE,
)


def load_env_key():
    load_dotenv()
    key = os.getenv("intelligence_api_key")
    if not key:
        print("ERROR: intelligence_api_key not found in .env", file=sys.stderr)
        sys.exit(1)
    return key


def api_call(agent_id, params, pagination=None, api_key=None, retries=3):
    if api_key is None:
        api_key = load_env_key()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "agent_id": agent_id,
        "params": params,
        "formatter_config": {"format_type": "raw"},
    }
    if pagination:
        body["pagination"] = pagination
    for attempt in range(retries):
        try:
            resp = requests.post(API_URL, json=body, headers=headers, timeout=60)
            if resp.status_code == 429:
                wait = (2 ** attempt) * 5
                print(f"  [rate-limited, retry {wait}s]")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (2 ** attempt) * 3
                print(f"  [error: {e}, retry {wait}s]")
                time.sleep(wait)
            else:
                raise


def paginated_call(agent_id, params, api_key, limit=200, max_pages=5):
    all_results = []
    offset = 0
    for page in range(max_pages):
        resp = api_call(agent_id, params, {"limit": limit, "offset": offset}, api_key)
        results = resp.get("data", {}).get("results", [])
        if not results:
            break
        all_results.extend(results)
        if not resp.get("pagination", {}).get("has_more", False):
            break
        offset += limit
    return all_results


def build_token_map_from_cache():
    """Build condition_id -> { outcome -> token_id } from cached trade records."""
    token_map = {}
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
                if cid not in token_map:
                    token_map[cid] = {}
                if outcome not in token_map[cid]:
                    token_map[cid][outcome] = tid
    print(f"\nToken map: {len(token_map)} conditions, {sum(len(v) for v in token_map.values())} outcomes")
    return token_map


def fetch_orderbook(token_id, api_key):
    """Fetch latest orderbook snapshot for a single token_id."""
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 7 * 86400000  # 7-day lookback
    params = {"token_id": token_id, "start_time": str(start_ms), "end_time": str(now_ms)}
    results = paginated_call(AGENT_ORDERBOOK, params, api_key, limit=5, max_pages=1)
    if not results:
        return None
    r = results[0]
    bids_raw = r.get("bids", "[]")
    asks_raw = r.get("asks", "[]")
    if isinstance(bids_raw, str):
        bids_raw = json.loads(bids_raw)
    if isinstance(asks_raw, str):
        asks_raw = json.loads(asks_raw)
    bids = [[float(x["price"]), float(x["size"])] for x in bids_raw]
    asks = [[float(x["price"]), float(x["size"])] for x in asks_raw]
    return {"bids": bids, "asks": asks, "timestamp": r.get("timestamp", "")}


def compute_outcome_depth(raw_ob):
    """Dollar-volume and weighted-average price from raw orderbook levels."""
    if not raw_ob:
        return {"bid_volume": 0, "ask_volume": 0, "wb_avg_bid": 0, "wb_avg_ask": 0, "bid_levels": [], "ask_levels": []}
    bids = raw_ob.get("bids", [])
    asks = raw_ob.get("asks", [])
    bid_vol = 0.0
    bid_wav = 0.0
    for p, s in bids:
        price = float(p)
        size = float(s)
        vol = price * size
        bid_vol += vol
        bid_wav += price * vol
    ask_vol = 0.0
    ask_wav = 0.0
    for p, s in asks:
        price = float(p)
        size = float(s)
        vol = price * size
        ask_vol += vol
        ask_wav += price * vol
    return {
        "bid_volume": round(bid_vol, 2),
        "ask_volume": round(ask_vol, 2),
        "wb_avg_bid": round(bid_wav / bid_vol, 4) if bid_vol > 0 else 0,
        "wb_avg_ask": round(ask_wav / ask_vol, 4) if ask_vol > 0 else 0,
        "bid_levels": [[str(p), str(s)] for p, s in bids[:10]],
        "ask_levels": [[str(p), str(s)] for p, s in asks[:10]],
    }


def compute_depth_imbalance(consensus_outcome, depth_by_outcome):
    """How much resting capital favors the consensus outcome (0..1)."""
    if not depth_by_outcome or len(depth_by_outcome) < 2:
        return 0.5
    outcomes = list(depth_by_outcome.keys())
    if consensus_outcome not in depth_by_outcome:
        return 0.5
    opposing = [o for o in outcomes if o != consensus_outcome][0]
    c = depth_by_outcome[consensus_outcome]
    o = depth_by_outcome[opposing]
    depth_for = c.get("bid_volume", 0) + o.get("ask_volume", 0)
    depth_against = c.get("ask_volume", 0) + o.get("bid_volume", 0)
    total = depth_for + depth_against
    return round(depth_for / total, 4) if total > 0 else 0.5


def wallet_short(w):
    return f"{w[:6]}...{w[-4:]}"


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


def fetch_open_mlb_markets(api_key):
    """Fetch all open MLB markets from the API (cached for OPEN_MARKETS_TTL_S)."""
    if OPEN_MARKETS_CACHE.exists():
        try:
            with open(OPEN_MARKETS_CACHE) as f:
                cached = json.load(f)
            age = time.time() - cached.get("fetched_at", 0)
            if 0 <= age < OPEN_MARKETS_TTL_S and cached.get("markets"):
                print(f"\nUsing cached open MLB markets ({len(cached['markets'])} markets, {age/60:.0f} min old)")
                return cached["markets"]
        except (json.JSONDecodeError, OSError):
            pass
    print("\nFetching open MLB markets...")
    all_markets = {}
    for slug in MLB_SEARCH_SLUGS:
        print(f"  Searching 'market_slug': '{slug}' ...")
        results = paginated_call(
            AGENT_MARKETS,
            {"market_slug": slug, "closed": "False"},
            api_key,
            max_pages=5,
        )
        for m in results:
            cid = m.get("condition_id")
            if cid:
                all_markets[cid] = m
    print(f"  Found {len(all_markets)} unique open markets")
    markets = list(all_markets.values())
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(OPEN_MARKETS_CACHE, "w") as f:
        json.dump({"fetched_at": time.time(), "markets": markets}, f)
    return markets


def is_market_maker(t):
    """Check if a trader shows market-maker or HFT behavior."""
    flags = set(t.get("behavioral_flags") or [])
    return "timing_anomaly" in flags or "sybil_risk" in flags

def load_top_traders(n=200):
    """Load top n traders by baseball PnL from existing data, excluding MM/HFT."""
    path = DATA_DIR / "top_mlb_traders.json"
    if not path.exists():
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        data = json.load(f)
    traders = data.get("top_tier", []) + data.get("watchlist", [])

    # Filter out market makers / HFT
    excluded = [t for t in traders if is_market_maker(t)]
    clean = [t for t in traders if not is_market_maker(t)]
    print(f"\n  Excluded {len(excluded)} MM/HFT traders")
    for t in excluded:
        print(f"    {wallet_short(t['wallet'])} flags={t.get('behavioral_flags')}")

    clean.sort(key=lambda t: -(t.get("baseball_pnl_15d") or 0))
    result = clean[:min(n, len(clean))]
    print(f"\nLoading all {len(result)} clean traders (excluding MM/HFT)")
    for t in result:
        pnl = t.get("baseball_pnl_15d") or 0
        print(f"  {wallet_short(t['wallet'])}  PnL=${pnl:,.0f}")

    # Save human traders (HLS >= 80) to separate file
    human_path = DATA_DIR / "human_traders.json"
    humans = [t for t in result if (t.get("human_likeness_score") or 0) >= 80]
    human_out = []
    for t in humans:
        human_out.append({
            "wallet": t["wallet"],
            "baseball_pnl_15d": t.get("baseball_pnl_15d"),
            "win_rate": t.get("win_rate"),
            "human_likeness_score": t.get("human_likeness_score"),
            "behavioral_flags": t.get("behavioral_flags"),
            "baseball_trades_15d": t.get("baseball_trades_15d"),
            "sports_pnl_15d": t.get("sports_pnl_15d"),
        })
    with open(human_path, "w") as f:
        json.dump(human_out, f, indent=2)
    print(f"  Saved {len(human_out)} human traders -> {human_path}")

    return result


def _parse_trade_ts(ts):
    """Epoch seconds from an ISO trade timestamp, or None if unparsable."""
    try:
        return int(datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return None


def _merge_trade_details(prev, new, cutoff_s):
    """Merge new trades into cached ones: dedupe on trade id, drop trades
    older than cutoff_s, keep the newest 50."""
    by_id = {}
    for d in prev + new:  # new last so fresh records win on id collision
        key = d.get("id") or d.get("transaction_hash") or f"{d.get('timestamp')}|{d.get('token_id')}|{d.get('size')}"
        by_id[key] = d
    kept = []
    for d in by_id.values():
        ts = _parse_trade_ts(d.get("timestamp"))
        if ts is None or ts >= cutoff_s:
            kept.append(d)
    kept.sort(key=lambda d: str(d.get("timestamp", "")), reverse=True)
    return kept[:50]


def _poll_one_trader(t, mlb_cid_set, api_key, seven_days_ago):
    """Fetch trades for one trader and rewrite its cache file.

    Incremental when the wallet has a fresh cache (start_time = last poll
    minus overlap buffer); full 7-day fetch otherwise.
    Returns (wallet, new_mlb_count, total_fetched, merged_cids, mode).
    """
    wallet = t["wallet"]
    cache_path = CACHE_DIR / f"mtrades_{wallet}.json"
    start_time = seven_days_ago
    mode = "full"
    prev_details = []
    prev_total = None
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            polled_s = _parse_trade_ts(cached.get("polled_at"))
            if polled_s and polled_s > seven_days_ago:
                start_time = max(polled_s - INCREMENTAL_OVERLAP_S, seven_days_ago)
                mode = "incr"
                prev_details = cached.get("mlb_trade_details", []) or []
                prev_total = cached.get("total_trades_90d")
        except (json.JSONDecodeError, OSError):
            pass

    resp = api_call(
        AGENT_TRADES,
        {
            "proxy_wallet": wallet,
            "condition_id": "ALL",
            "start_time": str(start_time),
        },
        {"limit": 200, "offset": 0},
        api_key,
    )
    all_trades = resp.get("data", {}).get("results", [])
    mlb_trades = [
        tr for tr in all_trades
        if tr.get("condition_id", "") in mlb_cid_set or MLB_KEYWORDS_RE.search(tr.get("slug", ""))
    ]

    merged = _merge_trade_details(prev_details, mlb_trades, seven_days_ago)
    merged_cids = {d.get("condition_id", "") for d in merged if d.get("condition_id")}
    result = {
        "wallet": wallet,
        "total_trades_90d": len(all_trades) if mode == "full" else (prev_total or 0),
        "mlb_trades_count": len(merged),
        "mlb_condition_ids": sorted(merged_cids),
        "mlb_trade_details": merged,
        "polled_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    with open(cache_path, "w") as f:
        json.dump(result, f, default=str)
    return wallet, len(mlb_trades), len(all_trades), merged_cids, mode


def fetch_trader_trades(traders, mlb_markets, api_key):
    """Fetch recent trades for given traders in parallel. Updates cache files."""
    mlb_cid_set = {m["condition_id"] for m in mlb_markets}
    seven_days_ago = int(time.time()) - (7 * 86400)

    total_mlb_trades = 0
    total_markets_with_trades = set()
    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_poll_one_trader, t, mlb_cid_set, api_key, seven_days_ago): t
            for t in traders
        }
        for fut in as_completed(futures):
            t = futures[fut]
            done += 1
            try:
                wallet, n_mlb, n_all, merged_cids, mode = fut.result()
                total_mlb_trades += n_mlb
                total_markets_with_trades |= merged_cids
                print(f"  [{done}/{len(traders)}] {wallet_short(wallet)} [{mode}] {n_mlb} MLB / {n_all} tot")
            except Exception as e:
                print(f"  [{done}/{len(traders)}] {wallet_short(t['wallet'])} error: {e}")

    print(f"\n  Total: {total_mlb_trades} new MLB trades; cache covers {len(total_markets_with_trades)} markets")


def compute_sentiment(trader_list):
    """Replicate sentiment computation from compute_sentiment.py but only using fresh cache data."""
    # Build trader index with weights
    max_pnl = max((t.get("baseball_pnl_15d") or 0 for t in trader_list), default=1)
    trader_idx = {}
    for t in trader_list:
        pnl = max(t.get("baseball_pnl_15d") or 0, 0)
        wr = t.get("win_rate") or 0.5
        sharpe = max(t.get("sharpe_ratio") or 0, 0)
        human = (t.get("human_likeness_score") or 50) / 100.0
        pnl_w = pnl / max_pnl
        wr_w = max(wr - 0.5, 0) * 2
        sharpe_w = min(sharpe / 2.0, 1.0)
        weight = 0.5 * pnl_w + 0.2 * wr_w + 0.15 * sharpe_w + 0.15 * human
        t["_weight"] = round(weight, 4)
        trader_idx[t["wallet"]] = t

    # Load trades from cache
    trades_by_cid = defaultdict(list)
    total_trades = 0
    for fname in os.listdir(CACHE_DIR):
        if not fname.startswith("mtrades_"):
            continue
        with open(CACHE_DIR / fname) as f:
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
            total_trades += 1

    print(f"\nLoaded {total_trades} trade events across {len(trades_by_cid)} markets from cache")

    # Compute per-market sentiment
    today = today_ny()
    sentiments = []
    for cid, trades in trades_by_cid.items():
        if not trades:
            continue
        outcomes = defaultdict(lambda: {"weighted_volume": 0.0, "trader_count": 0, "trader_set": set(), "trades": []})
        for tr in trades:
            t = trader_idx.get(tr["wallet"])
            if not t:
                continue
            weight = t["_weight"]
            notional = tr["notional"]
            signal = notional * weight
            if tr["side"] == "SELL":
                signal = -signal
            o = outcomes[tr["outcome"]]
            o["weighted_volume"] += signal
            o["trader_count"] += 1
            o["trader_set"].add(tr["wallet"])
            o["trades"].append(tr)

        if not outcomes:
            continue
        total_weighted = sum(o["weighted_volume"] for o in outcomes.values())
        if total_weighted == 0:
            continue

        sorted_outcomes = sorted(outcomes.items(), key=lambda x: -abs(x[1]["weighted_volume"]))
        top_outcome, top_data = sorted_outcomes[0]
        top_fraction = top_data["weighted_volume"] / total_weighted if total_weighted else 0
        if len(sorted_outcomes) >= 2:
            second_fraction = abs(sorted_outcomes[1][1]["weighted_volume"]) / total_weighted
            conviction = abs(top_fraction - second_fraction) / max(top_fraction, second_fraction) if max(top_fraction, second_fraction) > 0 else 0
        else:
            conviction = 1.0

        all_traders = set()
        for o in outcomes.values():
            all_traders.update(o["trader_set"])

        timestamps = [tr.get("timestamp", "") for tr in trades if tr.get("timestamp")]
        timestamps.sort()
        first_date = timestamps[0][:10] if timestamps else ""
        last_date = timestamps[-1][:10] if timestamps else ""

        # Extract game date from slug
        slug = trades[0].get("slug", "")
        event_slug = trades[0].get("event_slug", "")
        market_type = trades[0].get("market_type", "other")
        is_futures = event_slug == "futures-props"
        game_date = ""
        if not is_futures:
            m2 = SLUG_RE.match(slug)
            if m2:
                game_date = m2.group("date")

        sentiments.append({
            "condition_id": cid,
            "slug": slug,
            "market_type": market_type,
            "event_slug": event_slug,
            "game_date": game_date,
            "is_active": is_futures or (game_date and game_date >= today),
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
        })

    print(f"Computed sentiment for {len(sentiments)} markets")

    # Separate active vs expired
    active = [s for s in sentiments if s["is_active"]]
    expired = [s for s in sentiments if not s["is_active"]]
    print(f"  Active (today+future): {len(active)}")
    print(f"  Expired: {len(expired)}")

    return sentiments, active, expired, trader_idx


def rollup_games(sentiments):
    """Roll up per-market sentiments into per-game summaries (same as compute_sentiment.py)."""
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
    for ms in sentiments:
        event_slug = ms.get("event_slug") or "unknown"
        g = games[event_slug]
        g["markets"].append(ms)
        g["total_trade_events"] += ms["total_trade_events"]
        g["unique_traders"] = max(g["unique_traders"], ms["unique_traders"])
        mtype = ms.get("market_type")
        if mtype == "moneyline":
            g["moneyline"] = ms
        elif mtype == "total":
            g["total"] = ms
        elif mtype == "spread":
            g["spread"] = ms

    result = {}
    for event_slug, g in sorted(games.items()):
        top_markets = sorted(g["markets"], key=lambda m: -m["total_trade_events"])[:5]
        game_date = ""
        if event_slug != "futures-props":
            # Derive date from one of the markets in this game
            for m in g["markets"]:
                if m.get("game_date"):
                    game_date = m["game_date"]
                    break
        result[event_slug] = {
            "event_slug": event_slug,
            "game_date": game_date,
            "market_count": len(g["markets"]),
            "total_trade_events": g["total_trade_events"],
            "unique_traders": g["unique_traders"],
            "moneyline": g["moneyline"],
            "total": g["total"],
            "spread": g["spread"],
            "top_markets": top_markets,
        }
    return result


def parse_market_slug(slug):
    """Parse slug to extract game info. Returns dict or None."""
    if slug.startswith("will-") or slug.startswith("1-mlb-") or slug.startswith("2-mlb-") or slug.startswith("3-mlb-"):
        return {"event_slug": "futures-props", "market_type": "futures", "game_date": ""}
    m = SLUG_RE.match(slug)
    if not m:
        return {"event_slug": "other", "market_type": "other", "game_date": ""}
    g = m.groupdict()
    return {
        "event_slug": f"mlb-{g['team1']}-{g['team2']}-{g['date']}",
        "market_type": g.get("type") or "moneyline",
        "game_date": g["date"],
        "teams": (g["team1"], g["team2"]),
    }


def build_live_view(open_markets, sentiments_by_cid, today, orderbook_data=None):
    """Build a complete view of all open markets, enriching with sentiment where available."""
    sentiment_idx = {s["condition_id"]: s for s in sentiments_by_cid}

    game_markets = defaultdict(list)
    for m in open_markets:
        cid = m.get("condition_id")
        slug = m.get("slug", "")
        parsed = parse_market_slug(slug)
        if parsed["event_slug"] == "other":
            continue
        volume = float(m.get("volume_total", 0) or 0)
        if parsed["event_slug"] == "futures-props" and volume < 100:
            continue

        sentiment = sentiment_idx.get(cid)
        entry = {
            "condition_id": cid,
            "slug": slug,
            "market_type": parsed["market_type"],
            "event_slug": parsed["event_slug"],
            "game_date": parsed["game_date"],
            "volume_total": volume,
            "end_date": m.get("end_date", ""),
            "has_trader_data": sentiment is not None,
        }
        if sentiment:
            entry.update({
                "top_outcome": sentiment["top_outcome"],
                "top_weighted_fraction": sentiment["top_weighted_fraction"],
                "conviction": sentiment["conviction"],
                "unique_traders": sentiment["unique_traders"],
                "total_trade_events": sentiment["total_trade_events"],
                "total_weighted_volume": sentiment["total_weighted_volume"],
                "first_trade_date": sentiment["first_trade_date"],
                "last_trade_date": sentiment["last_trade_date"],
                "outcomes": sentiment["outcomes"],
            })
        if sentiment and orderbook_data and cid in orderbook_data:
            entry["orderbook"] = orderbook_data[cid]
        game_markets[parsed["event_slug"]].append(entry)

    # Build games view
    today_str = today
    active_games = {}
    all_games = {}
    for event_slug, markets in game_markets.items():
        game_date = markets[0]["game_date"] if event_slug != "futures-props" else ""
        is_active = event_slug == "futures-props" or (game_date and game_date >= today_str)

        # Count markets with trader data
        with_data = [m for m in markets if m["has_trader_data"]]
        total_events = sum(m.get("total_trade_events", 0) for m in markets)
        # Max market-level count — a lower bound on the cross-market union
        max_traders = max((m.get("unique_traders", 0) for m in markets), default=0)

        game_entry = {
            "event_slug": event_slug,
            "game_date": game_date,
            "market_count": len(markets),
            "markets_with_data": len(with_data),
            "total_trade_events": total_events,
            "unique_traders": max_traders,
            "is_active": is_active,
            "moneyline": next((m for m in markets if m["market_type"] == "moneyline" and m["has_trader_data"]), None),
            "total": next((m for m in markets if m["market_type"] == "total" and m["has_trader_data"]), None),
            "spread": next((m for m in markets if m["market_type"] == "spread" and m["has_trader_data"]), None),
            "top_markets": sorted(with_data, key=lambda m: -(m.get("total_weighted_volume", 0)))[:5],
            "all_markets": markets,
        }
        all_games[event_slug] = game_entry
        if is_active:
            active_games[event_slug] = game_entry

    # All markets with trader data (for consensus)
    all_with_data = [m for markets in game_markets.values() for m in markets if m["has_trader_data"]]
    active_with_data = [m for markets in game_markets.values() for m in markets if m["has_trader_data"] and m.get("game_date", "") >= today_str]

    return all_games, active_games, all_with_data, active_with_data


def main():
    api_key = load_env_key()

    # 1. Load top traders
    traders = load_top_traders()

    # 2. Fetch open markets
    markets = fetch_open_mlb_markets(api_key)

    # 3. Fetch recent trades for traders
    print("\nFetching recent trades (last 7 days)...")
    fetch_trader_trades(traders, markets, api_key)

    # 4. Compute sentiment
    print("\nComputing live sentiment...")
    all_sentiments, active, expired, trader_idx = compute_sentiment(traders)

    today_str = today_ny()

    # 5. Fetch orderbook depth for dashboard-relevant markets only:
    # active sentiment markets that are also in the open-markets view
    # (the ones the depth bars actually render for).
    print("\nFetching orderbook depth...")
    token_map = build_token_map_from_cache()
    open_cids = {m.get("condition_id") for m in markets if m.get("condition_id")}
    active_cids = {s["condition_id"] for s in active} & open_cids
    orderbook_data = {}
    ob_start = time.time()
    ob_tokens_fetched = 0
    ob_tasks = [
        (cid, outcome, tid)
        for cid in active_cids
        for outcome, tid in token_map.get(cid, {}).items()
    ]
    depth_by_cid = defaultdict(dict)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_orderbook, tid, api_key): (cid, outcome)
            for cid, outcome, tid in ob_tasks
        }
        for fut in as_completed(futures):
            cid, outcome = futures[fut]
            try:
                raw = fut.result()
            except Exception as e:
                print(f"  OB {outcome[:20]} error: {e}")
                raw = None
            depth = compute_outcome_depth(raw)
            depth_by_cid[cid][outcome] = depth
            ob_tokens_fetched += 1
            print(f"  OB {outcome[:20]}: bid=${depth['bid_volume']:,.0f} ask=${depth['ask_volume']:,.0f}")
    for cid, depth_by_outcome in depth_by_cid.items():
        if not depth_by_outcome:
            continue
        top = None
        for s in all_sentiments:
            if s["condition_id"] == cid:
                top = s["top_outcome"]
                break
        imb = compute_depth_imbalance(top, depth_by_outcome) if top and len(depth_by_outcome) >= 2 else 0.5
        orderbook_data[cid] = {
            "outcomes": depth_by_outcome,
            "depth_imbalance": imb,
        }
    ob_elapsed = time.time() - ob_start
    print(f"\n  Fetched {ob_tokens_fetched} orderbooks in {ob_elapsed:.0f}s ({len(active_cids)} markets)")

    # 6. Build complete live view from open markets + sentiment + orderbook
    print("\nBuilding live games view...")
    sentiments_by_cid={s["condition_id"]:s for s in all_sentiments}
    all_games,active_games,all_with_data,active_with_data = build_live_view(markets,all_sentiments,today_str,orderbook_data)

    # 7. Track sentiment vs outcome (snapshot today's games, check past resolutions)
    try:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        generated_at = now_utc.isoformat()
        new_tracked, n_resolved, total_resolved = update_tracker(
            active_games, generated_at, today_str, api_key, token_map
        )
    except Exception as e:
        print(f"  [tracker error: {e}]")
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 8. Top consensus from trader data
    live_consensus = sorted(
        [m for m in active_with_data if m.get("unique_traders", 0) >= 2 and m.get("conviction", 0) >= 0.2],
        key=lambda m: -(m["conviction"] * (m["unique_traders"] ** 0.5) * max(m["total_trade_events"], 1) ** 0.3),
    )
    live_consensus_all = sorted(
        [m for m in active_with_data if m.get("unique_traders", 0) >= 1],
        key=lambda m: -(m["conviction"] * (m["unique_traders"] ** 0.5) * max(m["total_trade_events"], 1) ** 0.3),
    )

    # 9. Write output
    output = {
        "generated_at": generated_at,
        "poll_time": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "today": today_str,
        "traders_polled": [t["wallet"] for t in traders],
        "summary": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "total_trade_events": sum(s["total_trade_events"] for s in all_sentiments),
            "unique_markets": len(all_sentiments),
            "active_markets_with_data": len(active_with_data),
            "total_open_markets": sum(len(g["all_markets"]) for g in all_games.values()),
            "active_games": len(active_games),
            "total_open_games": len(all_games),
            "unique_traders_in_trades": len(set(
                tr.get("proxy_wallet", tr.get("wallet", ""))
                for fname in os.listdir(CACHE_DIR)
                if fname.startswith("mtrades_")
                for tr in json.load(open(CACHE_DIR / fname)).get("mlb_trade_details", [])
            )),
            "data_start_date": min((s["first_trade_date"] for s in all_sentiments if s.get("first_trade_date")), default=""),
            "data_end_date": max((s["last_trade_date"] for s in all_sentiments if s.get("last_trade_date")), default=""),
        },
        "by_market": sorted(all_with_data, key=lambda m: -(m.get("total_weighted_volume", 0))),
        "active_markets": sorted(active_with_data, key=lambda m: -(m.get("total_weighted_volume", 0))),
        "by_game": all_games,
        "active_games": active_games,
        "live_consensus": live_consensus[:50],
        "live_consensus_all": live_consensus_all[:100],
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    # 10. Generate betting recommendations
    try:
        from recommend import main as gen_recommendations
        gen_recommendations()
    except Exception as e:
        print(f"  [recommend error: {e}]")

    print(f"\nDone! -> {OUT_PATH}")
    print(f"  Total sentiment markets:  {len(all_sentiments)}")
    print(f"  Open markets in view:     {sum(len(g['all_markets']) for g in all_games.values())}")
    print(f"  Active with trader data:  {len(active_with_data)}")
    print(f"  Open games:               {len(all_games)}")
    print(f"  Active games:             {len(active_games)}")
    print(f"  Live consensus:           {len(live_consensus)}")


if __name__ == "__main__":
    main()
