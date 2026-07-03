#!/usr/bin/env python3
"""
Find top MLB traders on Polymarket.

Pipeline:
  1. Discover MLB markets via keyword search (market_slug="mlb")
  2. Cast wide net for wallets (H-Score × 4 sorts + Leaderboard × 3 periods)
  3. Wallet 360 each candidate → decode Sports / Baseball / MLB PnL from category breakdown
  4. Confirm MLB activity via trade history scan
  5. MLB-scoped PnL for confirmed traders
  6. Ranked JSON output (top-tier >= $500 / watchlist < $500 / dormant)
"""

import os, sys, json, time, datetime, re
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Config ──────────────────────────────────────────────────────────────────
API_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

AGENT_HSCORE = 584
AGENT_LEADERBOARD = 579
AGENT_WALLET_360 = 581
AGENT_TRADES = 556
AGENT_MARKETS = 574
AGENT_PNL = 569

CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

MLB_SEARCH_SLUGS = ["mlb", "baseball", "world-series"]

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

def wallet_short(w):
    return f"{w[:6]}...{w[-4:]}"

# ── API Helpers ─────────────────────────────────────────────────────────────

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

def paginated_call(agent_id, params, api_key, limit=200, max_pages=10):
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

def cache_get(key):
    path = CACHE_DIR / f"{key}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def cache_set(key, data):
    path = CACHE_DIR / f"{key}.json"
    with open(path, "w") as f:
        json.dump(data, f, default=str)

def safe_float(v, default=0.0):
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

# ── Phase 0: MLB Market Discovery ───────────────────────────────────────────

def discover_mlb_markets(api_key):
    print("\n=== Phase 0: Discovering MLB Markets ===")
    cache_key = "mlb_markets"
    cached = cache_get(cache_key)
    if cached:
        print(f"  Loaded {len(cached)} MLB markets from cache")
        return cached

    all_markets = {}
    for slug in MLB_SEARCH_SLUGS:
        print(f"  Searching 'market_slug': '{slug}' ...")
        try:
            results = paginated_call(
                AGENT_MARKETS, {"market_slug": slug, "closed": "False"}, api_key,
                limit=200, max_pages=5,
            )
        except Exception as e:
            print(f"    Error: {e}")
            continue
        for m in results:
            cid = m.get("condition_id")
            if cid and cid not in all_markets:
                all_markets[cid] = {
                    "condition_id": cid,
                    "question": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "event_slug": m.get("event_slug", ""),
                    "volume_total": m.get("volume_total", 0),
                    "end_date": m.get("end_date", ""),
                }
    result = list(all_markets.values())
    print(f"  Found {len(result)} unique MLB markets")
    cache_set(cache_key, result)
    return result

# ── Phase 1: Wallet Discovery ──────────────────────────────────────────────

def fetch_hscore_wallets(api_key):
    print("\n=== Phase 1a: H-Score Leaderboard (4 sort methods) ===")
    cache_key = "hscore_wallets"
    cached = cache_get(cache_key)
    if cached:
        print(f"  Loaded {len(cached)} H-Score entries from cache")
        return cached

    base = {
        "min_win_rate_15d": "0",
        "max_win_rate_15d": "1.0",
        "min_roi_15d": "-100000",
        "min_total_trades_15d": "1",
        "max_total_trades_15d": "1000000",
        "min_pnl_15d": "-10000000",
    }
    sorts = ["hscore", "roi", "pnl", "win_rate"]
    seen = {}

    for s in sorts:
        print(f"  sort_by='{s}' ...")
        params = {**base, "sort_by": s}
        try:
            results = paginated_call(AGENT_HSCORE, params, api_key, limit=200, max_pages=5)
        except Exception as e:
            print(f"    Error: {e}")
            continue
        for r in results:
            w = r.get("wallet", "").lower()
            if not w:
                continue
            if w not in seen:
                seen[w] = {"wallet": w, "hscore_entries": []}
            seen[w]["hscore_entries"].append({
                "sort": s,
                "h_score": r.get("h_score"),
                "roi_pct_15d": r.get("roi_pct_15d"),
                "sharpe_ratio_15d": r.get("sharpe_ratio_15d"),
                "total_pnl_15d": r.get("total_pnl_15d"),
                "total_trades_15d": r.get("total_trades_15d"),
                "win_rate_pct_15d": r.get("win_rate_pct_15d"),
                "total_volume_15d": r.get("total_volume_15d"),
                "tier": r.get("tier"),
                "trajectory": r.get("trajectory"),
                "leaderboard_rank_15d": r.get("leaderboard_rank"),
            })

    for w in seen.values():
        w["hscore_sort_count"] = len(set(e["sort"] for e in w["hscore_entries"]))
    result = list(seen.values())
    # Sort by number of sorts found (more = stronger signal)
    result.sort(key=lambda x: -x["hscore_sort_count"])
    print(f"  Total unique H-Score wallets: {len(result)}")
    cache_set(cache_key, result)
    return result

def fetch_leaderboard_wallets(api_key):
    print("\n=== Phase 1b: Polymarket Leaderboard (3 periods) ===")
    cache_key = "leaderboard_wallets"
    cached = cache_get(cache_key)
    if cached:
        print(f"  Loaded {len(cached)} Leaderboard entries from cache")
        return cached

    periods = ["1d", "7d", "30d"]
    seen = {}
    for p in periods:
        print(f"  period='{p}' ...")
        try:
            results = paginated_call(
                AGENT_LEADERBOARD,
                {"wallet_address": "ALL", "leaderboard_period": p},
                api_key, limit=200, max_pages=3,
            )
        except Exception as e:
            print(f"    Error: {e}")
            continue
        for r in results:
            w = r.get("address", "").lower()
            if not w:
                continue
            if w not in seen:
                seen[w] = {"wallet": w, "lb_entries": [], "lb_periods_set": set()}
            seen[w]["lb_entries"].append({
                "period": p,
                "rank": r.get("rank"),
                "roi": r.get("roi"),
                "sharpe_ratio": r.get("sharpe_ratio"),
                "total_pnl": r.get("total_pnl"),
                "total_trades": r.get("total_trades"),
                "win_rate": r.get("win_rate"),
                "markets_traded": r.get("markets_traded"),
                "total_invested": r.get("total_invested"),
                "avg_trade_size": r.get("avg_trade_size"),
            })
            seen[w]["lb_periods_set"].add(p)

    for w in seen.values():
        w["lb_period_count"] = len(w.pop("lb_periods_set"))
    result = list(seen.values())
    print(f"  Total unique Leaderboard wallets: {len(result)}")
    cache_set(cache_key, result)
    return result

# ── Phase 2: Wallet 360 Profiling ──────────────────────────────────────────

def parse_category_perf(raw):
    """performance_by_category is a JSON string (not base64)."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
    return []

def extract_category_pnl(cats, category_path):
    """Sum PnL for a category path like 'Sports / Baseball / MLB'."""
    total_pnl = 0
    total_trades = 0
    total_invested = 0
    for c in cats:
        cat_name = c.get("category", "")
        if cat_name == category_path:
            total_pnl += safe_float(c.get("total_pnl"))
            total_trades += int(c.get("total_trades", 0))
            total_invested += safe_float(c.get("total_invested"))
    return total_pnl, total_trades, total_invested

def extract_all_baseball_pnl(cats):
    """Get PnL for the most specific baseball category.
    
    'Sports / Baseball / MLB' and 'Sports / Baseball' are hierarchical —
    the child is a filtered view, not additional data. Use the most specific
    category available to avoid double-counting.
    """
    pnl, trades, invested = extract_category_pnl(cats, "Sports / Baseball / MLB")
    if trades > 0 or pnl != 0:
        return (pnl, trades, invested)
    return extract_category_pnl(cats, "Sports / Baseball")

def extract_sports_pnl(cats):
    """Sum PnL across all 'Sports' categories."""
    pnl = 0
    trades = 0
    invested = 0
    for c in cats:
        cat_name = c.get("category", "")
        if cat_name.startswith("Sports"):
            pnl += safe_float(c.get("total_pnl"))
            trades += int(c.get("total_trades", 0))
            invested += safe_float(c.get("total_invested"))
    return pnl, trades, invested

def profile_wallets(wallets, api_key):
    print(f"\n=== Phase 2: Wallet 360 ({len(wallets)} wallets) ===")
    cache_pfx = "w360_"
    results = []
    to_fetch = []

    for wallet in wallets:
        cached = cache_get(f"{cache_pfx}{wallet}")
        if cached:
            # Recompute PnL from cached category_breakdown (fix for previous double-count bug)
            cats = parse_category_perf(cached.get("performance_by_category"))
            if cats:
                cached["category_breakdown"] = cats
                mlb_pnl, mlb_trades, mlb_invested = extract_all_baseball_pnl(cats)
                cached["baseball_pnl"] = round(mlb_pnl, 2)
                cached["baseball_trades"] = mlb_trades
                cached["baseball_invested"] = round(mlb_invested, 2)
                sports_pnl, sports_trades, _ = extract_sports_pnl(cats)
                cached["sports_pnl"] = round(sports_pnl, 2)
                cached["sports_trades"] = sports_trades
            results.append(cached)
        else:
            to_fetch.append(wallet)

    print(f"  Cached: {len(results)}, To fetch: {len(to_fetch)}")

    for i, wallet in enumerate(to_fetch):
        print(f"  [{i+1}/{len(to_fetch)}] {wallet_short(wallet)} ...", end=" ", flush=True)
        try:
            resp = api_call(
                AGENT_WALLET_360,
                {"proxy_wallet": wallet, "window_days": "15"},
                api_key=api_key,
            )
            rows = resp.get("data", {}).get("results", [])
            if not rows:
                print("no data")
                continue
            p = rows[0]
            p["proxy_wallet"] = wallet

            # Parse category breakdown
            cats = parse_category_perf(p.get("performance_by_category"))
            p["category_breakdown"] = cats

            mlb_pnl, mlb_trades, mlb_invested = extract_all_baseball_pnl(cats)
            p["baseball_pnl"] = round(mlb_pnl, 2)
            p["baseball_trades"] = mlb_trades
            p["baseball_invested"] = round(mlb_invested, 2)

            sports_pnl, sports_trades, _ = extract_sports_pnl(cats)
            p["sports_pnl"] = round(sports_pnl, 2)
            p["sports_trades"] = sports_trades

            p["has_mlb_activity"] = mlb_pnl != 0 or mlb_trades > 0
            p["is_sports_trader"] = sports_pnl != 0 or sports_trades > 0

            cache_set(f"{cache_pfx}{wallet}", p)
            results.append(p)
            if p["has_mlb_activity"]:
                print(f"baseball_pnl={mlb_pnl:.0f}t={mlb_trades}" if mlb_pnl else "mlb trades only")
            elif p["is_sports_trader"]:
                print("sports")
            else:
                print("ok")
        except Exception as e:
            print(f"error: {e}")

    return results

# ── Phase 3: Trade Confirmation ────────────────────────────────────────────

def check_mlb_trades(wallet_profiles, mlb_markets, api_key):
    """For wallets with MLB/sports activity, confirm via trade scan."""
    print("\n=== Phase 3: MLB Trade Confirmation ===")

    mlb_cid_set = {m["condition_id"] for m in mlb_markets}
    candidates = [p for p in wallet_profiles
                  if p.get("has_mlb_activity") or p.get("is_sports_trader")]
    print(f"  {len(candidates)} candidates to check")

    cache_pfx = "mtrades_"
    results = []
    to_fetch = []

    for p in candidates:
        wallet = p["proxy_wallet"]
        cached = cache_get(f"{cache_pfx}{wallet}")
        if cached:
            results.append(cached)
        else:
            to_fetch.append(p)

    print(f"  Cached: {len(results)}, To fetch: {len(to_fetch)}")

    now = int(time.time())
    ninety_days_ago = now - (90 * 86400)

    for i, profile in enumerate(to_fetch):
        wallet = profile["proxy_wallet"]
        print(f"  [{i+1}/{len(to_fetch)}] {wallet_short(wallet)} ...", end=" ", flush=True)
        try:
            resp = api_call(
                AGENT_TRADES,
                {
                    "proxy_wallet": wallet,
                    "condition_id": "ALL",
                    "start_time": str(ninety_days_ago),
                },
                {"limit": 200, "offset": 0},
                api_key,
            )
            trades = resp.get("data", {}).get("results", [])
            mlb_trades = []
            mlb_cids_found = set()
            for t in trades:
                slug = t.get("slug", "")
                cid = t.get("condition_id", "")
                is_mlb = False
                if cid in mlb_cid_set:
                    is_mlb = True
                elif MLB_KEYWORDS_RE.search(slug):
                    is_mlb = True
                if is_mlb:
                    mlb_trades.append(t)
                    if cid:
                        mlb_cids_found.add(cid)

            result = {
                "wallet": wallet,
                "total_trades_90d": len(trades),
                "mlb_trades_count": len(mlb_trades),
                "mlb_condition_ids": list(mlb_cids_found),
                "mlb_trade_details": mlb_trades[:50],
            }
            cache_set(f"{cache_pfx}{wallet}", result)
            results.append(result)
            print(f"{len(mlb_trades)} mlb / {len(trades)} tot" if mlb_trades else "no mlb")
        except Exception as e:
            print(f"error: {e}")

    return results

# ── Phase 4: MLB PnL Quantification ────────────────────────────────────────

def quantify_mlb_pnl(mlb_trade_results, api_key):
    """Get realized PnL for confirmed traders over last 90 days."""
    print("\n=== Phase 4: MLB PnL Quantification ===")

    confirmed = [r for r in mlb_trade_results if r.get("mlb_trades_count", 0) > 0]
    print(f"  {len(confirmed)} confirmed MLB traders")

    ninety_days_ago_dt = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

    cache_pfx = "mpnl_"
    results = []
    to_fetch = []

    for r in confirmed:
        wallet = r["wallet"]
        cached = cache_get(f"{cache_pfx}{wallet}")
        if cached:
            results.append(cached)
        else:
            to_fetch.append(r)

    print(f"  Cached: {len(results)}, To fetch: {len(to_fetch)}")

    for i, entry in enumerate(to_fetch):
        wallet = entry["wallet"]
        mlb_cids = entry.get("mlb_condition_ids", [])
        print(f"  [{i+1}/{len(to_fetch)}] {wallet_short(wallet)} ({len(mlb_cids)} MLB mkts) ...", end=" ", flush=True)

        all_pnl_rows = []
        try:
            resp = api_call(
                AGENT_PNL,
                {
                    "granularity": "1d",
                    "wallet": wallet,
                    "start_time": ninety_days_ago_dt,
                    "end_time": today,
                },
                api_key=api_key,
            )
            all_pnl_rows = resp.get("data", {}).get("results", [])
        except Exception as e:
            print(f"pnl err: {e}")

        total_pnl_90d = sum(safe_float(r.get("pnl")) for r in all_pnl_rows)
        total_invested_90d = sum(safe_float(r.get("invested")) for r in all_pnl_rows)

        result = {
            "wallet": wallet,
            "total_realized_pnl_90d": round(total_pnl_90d, 2),
            "total_invested_90d": round(total_invested_90d, 2),
            "realized_pnl_series": all_pnl_rows[:30],
        }
        cache_set(f"{cache_pfx}{wallet}", result)
        results.append(result)
        print(f"pnl={total_pnl_90d:.0f}" if total_pnl_90d else "no pnl")

    return results

# ── Phase 5: Build Leaderboard ─────────────────────────────────────────────

def compute_human_score(p):
    """Heuristic: 0-100, penalizes bot/arb/HFT flags."""
    score = 100
    if p.get("sybil_risk_flag"):
        score -= 30
    if p.get("suspicious_win_rate_flag"):
        score -= 20
    if p.get("timing_anomaly_flag"):
        score -= 20
    if p.get("single_market_dependence_flag"):
        score -= 10
    if p.get("position_size_volatility_flag"):
        score -= 5

    wr = safe_float(p.get("win_rate"))
    tt = int(p.get("total_trades", 0) or 0)
    if wr > 0.9 and tt > 100:
        score -= 15
    pts = safe_float(p.get("perfect_timing_score"))
    if pts > 0.8:
        score -= 10
    mt = int(p.get("markets_traded", 0) or 0)
    if mt < 3:
        score -= 10
    return max(0, score)

def parse_flagged_metrics(raw):
    """Parse '{flag1,flag2}' string into a list."""
    if not raw or raw == "{}" or raw == "None":
        return []
    cleaned = raw.strip("{}")
    return [f.strip() for f in cleaned.split(",") if f.strip()]

def build_leaderboard(wallet_profiles, mlb_trade_data, mlb_pnl_data):
    print("\n=== Phase 5: Building Leaderboard ===")

    profile_idx = {p["proxy_wallet"]: p for p in wallet_profiles}
    trades_idx = {t["wallet"]: t for t in mlb_trade_data}
    pnl_idx = {p["wallet"]: p for p in mlb_pnl_data}

    wallets = set(profile_idx.keys()) | set(trades_idx.keys()) | set(pnl_idx.keys())

    traders = []
    for wallet in wallets:
        p = profile_idx.get(wallet, {})
        t = trades_idx.get(wallet, {})
        pnl = pnl_idx.get(wallet, {})

        if not p:
            continue

        # Parse flags
        flagged = parse_flagged_metrics(p.get("flagged_metrics", ""))

        # Overall realized PnL (all markets, 90d)
        total_pnl = pnl.get("total_realized_pnl_90d", pnl.get("mlb_total_realized_pnl_90d", 0))
        mlb_trades = t.get("mlb_trades_count", 0)
        mlb_cids = t.get("mlb_condition_ids", [])

        total_invested = pnl.get("total_invested_90d", 0)
        # Sports / Baseball from Wallet 360
        bb_pnl = p.get("baseball_pnl", 0)
        bb_trades = p.get("baseball_trades", 0)
        bb_invested = p.get("baseball_invested", 0)

        trader = {
            "wallet": wallet,
            "wallet_short": wallet_short(wallet),

            # ═══ Overall ═══
            "total_realized_pnl_90d": total_pnl,
            "mlb_trades_90d": mlb_trades,
            "mlb_markets_traded": len(mlb_cids),
            "mlb_condition_ids": mlb_cids,

            # Wallet 360 baseball category (broader window)
            "baseball_pnl_15d": bb_pnl,
            "baseball_trades_15d": bb_trades,
            "baseball_roi_15d": round((bb_pnl / bb_invested * 100), 2) if bb_invested else 0,

            # Sports (all sports)
            "sports_pnl_15d": p.get("sports_pnl", 0),
            "sports_trades_15d": p.get("sports_trades", 0),

            # ═══ Overall Performance ═══
            "total_pnl_90d": total_pnl,
            "total_roi_90d": round((total_pnl / total_invested * 100), 2) if total_invested else 0,
            "total_invested_90d": total_invested,
            "total_trades_90d": t.get("total_trades_90d", 0),

            # ═══ Risk / Return ═══
            "sharpe_ratio": p.get("sharpe_ratio"),
            "sortino_ratio": p.get("sortino_ratio"),
            "calmar_ratio": p.get("calmar_ratio"),
            "profit_factor": p.get("profit_factor"),
            "max_drawdown": p.get("max_drawdown"),
            "gain_to_pain_ratio": p.get("gain_to_pain_ratio"),
            "ulcer_index": p.get("ulcer_index"),
            "roi_w360": p.get("roi"),
            "annualized_return": p.get("annualized_return"),
            "win_rate": p.get("win_rate"),
            "win_rate_last_30day": p.get("win_rate_last_30day"),
            "coefficient_of_variation": p.get("coefficient_of_variation"),
            "drawdown_frequency": p.get("drawdown_frequency"),
            "recovery_time_avg": p.get("recovery_time_avg"),
            "curve_volatility": p.get("curve_volatility"),
            "position_size_consistency": p.get("position_size_consistency"),

            # ═══ Behavioral ═══
            "behavioral_flags": flagged,
            "flagged_metrics_raw": p.get("flagged_metrics", ""),
            "sybil_risk_flag": p.get("sybil_risk_flag", False),
            "suspicious_win_rate_flag": p.get("suspicious_win_rate_flag", False),
            "timing_anomaly_flag": p.get("timing_anomaly_flag", False),
            "single_market_dependence_flag": p.get("single_market_dependence_flag", False),
            "position_size_volatility_flag": p.get("position_size_volatility_flag", False),
            "perfect_timing_score": p.get("perfect_timing_score"),
            "perfect_timing_flag": p.get("perfect_timing_flag", False),
            "equity_curve_pattern": p.get("equity_curve_pattern"),
            "performance_trend": p.get("performance_trend"),
            "human_likeness_score": compute_human_score(p),
            "combined_risk_score": p.get("combined_risk_score"),
            "risk_level": p.get("risk_level"),
            "statistical_confidence": p.get("statistical_confidence"),

            # ═══ Activity ═══
            "total_trades_w360": p.get("total_trades"),
            "markets_traded": p.get("markets_traded"),
            "market_concentration_ratio": p.get("market_concentration_ratio"),
            "category_diversity_score": p.get("category_diversity_score"),
            "avg_trade_size": p.get("avg_trade_size"),
            "avg_market_exposure": p.get("avg_market_exposure"),
            "best_market_pnl": p.get("best_market_pnl"),
            "worst_market_pnl": p.get("worst_market_pnl"),
            "days_active": p.get("days_active"),
            "last_active": p.get("last_active"),

            # Category breakdown
            "category_breakdown": p.get("category_breakdown", []),

            # Source
            "hscore_sort_count": p.get("hscore_sort_count", 0),
        }

        traders.append(trader)

    # Split into tiers (baseball PnL only — overall PnL is not MLB-specific)
    top = [t for t in traders if t["baseball_pnl_15d"] >= 500]
    watch = [t for t in traders if 0 < t["baseball_pnl_15d"] < 500]
    extra_watch = [t for t in traders if t not in top and t not in watch
                   and t["mlb_trades_90d"] > 0]
    watch.extend(extra_watch)

    # Dormant: has baseball category but no recent MLB trade activity
    dormant = [t for t in traders if t not in top and t not in watch
               and (t["baseball_pnl_15d"] != 0 or t["baseball_trades_15d"] > 0)
               and t["mlb_trades_90d"] == 0]

    # Others who trade sports generally but no MLB detected
    sports_only = [t for t in traders if t not in top and t not in watch
                   and t not in dormant
                   and (t["sports_pnl_15d"] != 0 or t["sports_trades_15d"] > 0)]

    # Sort each tier
    top.sort(key=lambda t: -(t["baseball_pnl_15d"] or 0))
    watch.sort(key=lambda t: -(t["baseball_pnl_15d"] or 0))
    dormant.sort(key=lambda t: -(t["baseball_pnl_15d"] or 0))
    sports_only.sort(key=lambda t: -(t["sports_pnl_15d"] or 0))

    output = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "total_screened": len(traders),
        "top_tier_count": len(top),
        "watchlist_count": len(watch),
        "dormant_mlb_count": len(dormant),
        "sports_only_count": len(sports_only),
        "tier_definitions": {
            "top_tier": "Baseball PnL >= $500 (Wallet 360 15d, Sports / Baseball / MLB category)",
            "watchlist": "Baseball PnL < $500 but > $0, or MLB trades detected with low activity",
            "dormant_mlb": "Has baseball category on Wallet 360 but no MLB trades in last 90 days",
            "sports_only": "Sports trader with no baseball/MLB category detected",
        },
        "top_tier": top,
        "watchlist": watch,
        "dormant_mlb": dormant,
        "sports_only": sports_only,
    }
    return output

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    api_key = load_env_key()
    print(f"API key loaded (len={len(api_key)})")
    print(f"Data dir: {DATA_DIR}")

    # Phase 0
    mlb_markets = discover_mlb_markets(api_key)

    # Phase 1
    hscore = fetch_hscore_wallets(api_key)
    lb = fetch_leaderboard_wallets(api_key)
    all_wallets = list(
        set(w["wallet"] for w in hscore) | set(w["wallet"] for w in lb)
    )
    print(f"\n  Total unique wallets: {len(all_wallets)}")
    print(f"    H-Score: {len(hscore)}")
    print(f"    Leaderboard: {len(lb)}")

    # Phase 2
    wallet_profiles = profile_wallets(all_wallets, api_key)
    mlb_wallets = [p for p in wallet_profiles if p.get("has_mlb_activity")]
    sports_wallets = [p for p in wallet_profiles if p.get("is_sports_trader")]
    print(f"\n  Sports traders: {len(sports_wallets)}")
    print(f"  MLB/Baseball traders: {len(mlb_wallets)}")

    # Phase 3
    mlb_trade_results = check_mlb_trades(wallet_profiles, mlb_markets, api_key)

    # Phase 4
    mlb_pnl_results = quantify_mlb_pnl(mlb_trade_results, api_key)

    # Phase 5
    leaderboard = build_leaderboard(wallet_profiles, mlb_trade_results, mlb_pnl_results)

    out_path = DATA_DIR / "top_mlb_traders.json"
    with open(out_path, "w") as f:
        json.dump(leaderboard, f, indent=2)

    print(
        f"\n{'='*60}\n"
        f"Done! Output -> {out_path}\n"
        f"Top tier (≥$500):    {leaderboard['top_tier_count']}\n"
        f"Watchlist (<$500):   {leaderboard['watchlist_count']}\n"
        f"Dormant MLB:         {leaderboard['dormant_mlb_count']}\n"
        f"Sports only:         {leaderboard['sports_only_count']}\n"
        f"Total screened:      {leaderboard['total_screened']}\n"
        f"{'='*60}"
    )

if __name__ == "__main__":
    main()
