#!/usr/bin/env python3
"""
FastAPI server — MLB Sentiment Dashboard

Serves live + historical API endpoints and static frontend.
Run: uvicorn server:app --reload
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from math import sqrt, log
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="MLB Sentiment Dashboard")

BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data"
STATIC_DIR = BASE / "scripts" / "static"
SCRIPTS_DIR = BASE / "scripts"

# ── Data state ──────────────────────────────────────────────────────────
historical_data = None
live_data = None
poll_in_progress = False


def load_historical():
    global historical_data
    sp = DATA_DIR / "sentiment_scores.json"
    if sp.exists():
        with open(sp) as f:
            historical_data = json.load(f)


def load_live():
    global live_data
    lp = DATA_DIR / "live_signals.json"
    if lp.exists():
        with open(lp) as f:
            live_data = json.load(f)


load_historical()
load_live()


# ── Helpers ───────────────────────────────────────────────────────────

def slug_to_readable(slug):
    parts = slug.split("-")
    if len(parts) < 4:
        return slug
    team_map = {
        "nyy": "NYY", "bos": "BOS", "lad": "LAD", "sd": "SD",
        "hou": "HOU", "det": "DET", "tex": "TEX", "tor": "TOR",
        "chc": "CHC", "mil": "MIL", "atl": "ATL", "sf": "SF",
        "nym": "NYM", "phi": "PHI", "ari": "ARI", "tb": "TB",
        "oak": "OAK", "laa": "LAA", "sea": "SEA", "col": "COL",
        "min": "MIN", "kc": "KC", "cws": "CWS", "cle": "CLE",
        "cin": "CIN", "pit": "PIT", "stl": "STL", "mia": "MIA",
        "wsh": "WSH", "bal": "BAL",
    }
    if parts[0] == "mlb" and len(parts) >= 5:
        t1, t2, date = parts[1], parts[2], parts[3]
        teams = f"{team_map.get(t1, t1.upper())} @ {team_map.get(t2, t2.upper())}"
        remaining = parts[4:]
        mtype = ""
        if "total" in remaining:
            idx = remaining.index("total")
            line = remaining[idx+1] if idx+1 < len(remaining) else ""
            mtype = f"Total {line}"
        elif "spread" in remaining:
            idx = remaining.index("spread")
            side = remaining[idx+1] if idx+1 < len(remaining) else ""
            line = remaining[idx+2] if idx+2 < len(remaining) else ""
            mtype = f"{side.title()} {line}"
        elif "nrfi" in remaining:
            mtype = "NRFI"
        else:
            mtype = "Moneyline"
        return f"{teams} ({mtype})"
    if slug.startswith("will-"):
        return slug.replace("-", " ").replace("  ", " ").title()
    return slug


def filter_markets(markets, min_conviction=0.0, min_traders=1,
                   market_type=None, search=None, days=None, limit=50):
    cutoff = ""
    if days is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")
    result = []
    for m in markets:
        if m["conviction"] < min_conviction:
            continue
        if m["unique_traders"] < min_traders:
            continue
        if market_type and m.get("market_type") != market_type:
            continue
        if search:
            s = search.lower()
            slug = m.get("slug", "").lower()
            outcomes = " ".join(m.get("outcomes", {}).keys()).lower()
            if s not in slug and s not in outcomes:
                continue
        if cutoff and m.get("last_trade_date", "") < cutoff:
            continue
        result.append(m)
    seen = set()
    deduped = []
    for m in result:
        cid = m.get("condition_id") or m.get("slug")
        if cid not in seen:
            seen.add(cid)
            deduped.append(m)
    deduped.sort(key=lambda m: -(m["conviction"] * sqrt(m["unique_traders"] + 1) * log(m["total_trade_events"] + 2)))
    return deduped[:limit]


# ── Historical Endpoints (kept for reference) ─────────────────────────

@app.get("/api/summary")
def get_summary():
    if not historical_data:
        return {"error": "no data"}
    return historical_data.get("summary", {})


@app.get("/api/consensus")
def get_consensus(
    min_conviction: float = Query(0.0, ge=0, le=1),
    min_traders: int = Query(1, ge=1),
    market_type: str | None = None,
    search: str | None = None,
    days: int | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    if not historical_data:
        return {"error": "no data"}
    markets = historical_data.get("by_market", []) + historical_data.get("top_consensus", [])
    return filter_markets(markets, min_conviction, min_traders, market_type, search, days, limit)


@app.get("/api/games")
def get_games(date: str | None = None, search: str | None = None):
    if not historical_data:
        return {"error": "no data"}
    games = historical_data.get("by_game", {})
    result = []
    for slug, g in games.items():
        if date and date not in slug:
            continue
        if search and search.lower() not in slug:
            continue
        result.append(g)
    result.sort(key=lambda g: -g["total_trade_events"])
    return result


@app.get("/api/game/{event_slug:path}")
def get_game(event_slug: str):
    if not historical_data:
        return {"error": "no data"}
    games = historical_data.get("by_game", {})
    if event_slug in games:
        return games[event_slug]
    for slug, g in games.items():
        if event_slug in slug:
            return g
    return {"error": "game not found"}


@app.get("/api/market-types")
def get_market_types():
    if not historical_data:
        return {"error": "no data"}
    types = set()
    for m in historical_data.get("by_market", []):
        t = m.get("market_type")
        if t:
            types.add(t)
    return sorted(types)


@app.get("/api/traders")
def get_traders(
    tier: str | None = None,
    sort: str = "pnl",
    limit: int = Query(100, ge=1, le=200),
):
    td = DATA_DIR / "top_mlb_traders.json"
    if not td.exists():
        return {"error": "no data"}
    with open(td) as f:
        trader_data = json.load(f)
    tiers_map = {
        "top": trader_data.get("top_tier", []),
        "watch": trader_data.get("watchlist", []),
        "dormant": trader_data.get("dormant_mlb", []),
    }
    if tier and tier in tiers_map:
        result = tiers_map[tier]
    else:
        result = trader_data.get("top_tier", []) + trader_data.get("watchlist", [])
    sort_key = {
        "pnl": lambda t: -(t.get("baseball_pnl_15d") or 0),
        "wr": lambda t: -(t.get("win_rate") or 0),
        "sharpe": lambda t: -(t.get("sharpe_ratio") or 0),
        "human": lambda t: -(t.get("human_likeness_score") or 0),
        "trades": lambda t: -(t.get("baseball_trades_15d") or 0),
    }.get(sort, lambda t: -(t.get("baseball_pnl_15d") or 0))
    result = sorted(result, key=sort_key)
    return result[:limit]


@app.get("/api/trader/{wallet}/trades")
def get_trader_trades(wallet: str):
    cache_dir = DATA_DIR / "cache"
    path = cache_dir / f"mtrades_{wallet}.json"
    if not path.exists():
        return {"error": "trader not found"}
    with open(path) as f:
        data = json.load(f)
    details = data.get("mlb_trade_details", [])
    for d in details:
        d["label"] = slug_to_readable(d.get("slug", ""))
    return details


@app.get("/api/game/{event_slug}/markets")
def get_game_markets(event_slug: str):
    if not historical_data:
        return {"error": "no data"}
    markets = []
    for m in historical_data.get("by_market", []):
        if m.get("event_slug") == event_slug:
            m["label"] = slug_to_readable(m.get("slug", ""))
            markets.append(m)
    return markets


# ── Live Endpoints ────────────────────────────────────────────────────

@app.get("/api/live")
def live_root():
    """Return status and age of live data."""
    if not live_data:
        return {"status": "no_data", "message": "Run poll_live.py first or hit /api/live/poll"}
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(live_data.get("generated_at", "2026-01-01"))).total_seconds()
    sm = live_data.get("summary", {})
    return {
        "status": "ok",
        "age_seconds": int(age),
        "generated_at": live_data.get("generated_at"),
        "today": live_data.get("today"),
        "active_markets_with_data": sm.get("active_markets_with_data", 0),
        "total_open_markets": sm.get("total_open_markets", 0),
        "active_games": sm.get("active_games", 0),
        "total_open_games": sm.get("total_open_games", 0),
    }


@app.get("/api/live/summary")
def live_summary():
    if not live_data:
        return {"error": "no live data"}
    return live_data.get("summary", {})


@app.get("/api/live/consensus")
def live_consensus(
    min_conviction: float = Query(0.0, ge=0, le=1),
    min_traders: int = Query(1, ge=1),
    market_type: str | None = None,
    search: str | None = None,
    limit: int = Query(100, ge=1, le=500),
):
    if not live_data:
        return {"error": "no live data"}
    markets = live_data.get("live_consensus_all", [])
    result = []
    for m in markets:
        conv = m.get("conviction", 0)
        tr = m.get("unique_traders", 0)
        if conv < min_conviction:
            continue
        if tr < min_traders:
            continue
        if market_type and m.get("market_type") != market_type:
            continue
        if search:
            s = search.lower()
            slug = m.get("slug", "").lower()
            outcomes = " ".join(m.get("outcomes", {}).keys()).lower()
            if s not in slug and s not in outcomes:
                continue
        result.append(m)
    return result[:limit]


@app.get("/api/live/games")
def live_games():
    """Return active games (today + future) with current sentiment."""
    if not live_data:
        return {"error": "no live data"}
    games = live_data.get("active_games", {})
    result = list(games.values())
    result.sort(key=lambda g: -(g["total_trade_events"]))
    return result


@app.get("/api/live/game/{event_slug:path}")
def live_game(event_slug: str):
    if not live_data:
        return {"error": "no live data"}
    games = live_data.get("active_games", {})
    if event_slug in games:
        return games[event_slug]
    games2 = live_data.get("by_game", {})
    if event_slug in games2:
        return games2[event_slug]
    return {"error": "game not found"}


@app.get("/api/live/market-types")
def live_market_types():
    if not live_data:
        return {"error": "no live data"}
    types = set()
    for m in live_data.get("live_consensus_all", []):
        t = m.get("market_type")
        if t:
            types.add(t)
    return sorted(types)


@app.get("/api/tracking")
def get_tracking():
    """Return tracked game predictions vs actual outcomes."""
    path = DATA_DIR / "tracked_games.json"
    if not path.exists():
        return {"error": "no tracking data"}
    with open(path) as f:
        return json.load(f)


@app.get("/api/backtest")
def get_backtest():
    """Return backtest results (trader conviction vs actual outcomes)."""
    path = DATA_DIR / "backtest_results.json"
    if not path.exists():
        return {"error": "no backtest data", "hint": "Run scripts/backtest.py first"}
    with open(path) as f:
        return json.load(f)


@app.get("/api/recommendations")
def get_recommendations():
    """Return ranked betting recommendations from live consensus."""
    path = DATA_DIR / "bet_recommendations.json"
    if not path.exists():
        return {"error": "no recommendations", "hint": "Run scripts/recommend.py after poll"}
    with open(path) as f:
        return json.load(f)


@app.get("/api/live/poll")
def trigger_poll():
    """Run the poll script as a subprocess (blocks until the poll finishes)."""
    global poll_in_progress
    if poll_in_progress:
        return {"status": "poll already in progress"}
    poll_in_progress = True
    try:
        poll_script = SCRIPTS_DIR / "poll_live.py"
        venv_python = BASE / ".venv" / "bin" / "python3"
        if not venv_python.exists():
            venv_python = "python3"
        result = subprocess.run(
            [str(venv_python), str(poll_script)],
            capture_output=True, text=True, timeout=900,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        load_live()
        return {
            "status": "ok",
            "stdout": result.stdout[-2000:] if result.stdout else "",
            "stderr": result.stderr[-2000:] if result.stderr else "",
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        poll_in_progress = False


# ── Serve frontend ────────────────────────────────────────────────────

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    idx = STATIC_DIR / "index.html"
    if idx.exists():
        return FileResponse(str(idx))
    return {"error": "frontend not built"}


# ── Main ──────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    load_live()
    load_historical()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
