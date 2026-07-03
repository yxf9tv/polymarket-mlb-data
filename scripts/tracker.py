"""Track sentiment predictions vs actual game outcomes.

Snapshots market sentiment at game-start and compares against resolved
Polymarket outcomes after games end. Snapshots also record executable entry
pricing (best ask + top-of-book depth on the predicted outcome, plus the
last pre-game candle close) so tracked_games.json is an honest forward
test-bed: signal vs. what a bettor could actually have paid vs. settlement
(reports/07_edge_rules.md found the backtested crowd-flow edge does not
survive real execution pricing — this is the live analogue of that check).
"""

import json, time, os, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_DIR = DATA_DIR / "cache"
TRACK_PATH = DATA_DIR / "tracked_games.json"

NY_TZ = ZoneInfo("America/New_York")


def fetch_entry_pricing(token_id, api_key):
    """Executable entry pricing for a token at snapshot time: best ask price
    + top-of-book depth (via poll_live.fetch_orderbook, same live-book call
    the dashboard's depth bars use) plus the last pre-snapshot candle close
    from CLOB's public prices-history endpoint (no auth, cheap). Never
    raises — returns None values on any failure so a pricing hiccup can't
    block a snapshot.
    """
    pricing = {"entry_ask_price": None, "entry_ask_depth": None, "entry_candle_close": None}
    if not token_id:
        return pricing

    try:
        from poll_live import fetch_orderbook  # local import: poll_live imports us at module load
        raw_ob = fetch_orderbook(token_id, api_key)
        if raw_ob and raw_ob.get("asks"):
            best_price, best_size = min(raw_ob["asks"], key=lambda lvl: lvl[0])
            pricing["entry_ask_price"] = best_price
            pricing["entry_ask_depth"] = best_size
    except Exception as e:
        print(f"  [entry pricing: orderbook fetch failed for {token_id[:16]}...: {e}]")

    try:
        import requests
        now_s = int(time.time())
        resp = requests.get(
            "https://clob.polymarket.com/prices-history",
            params={"market": token_id, "startTs": now_s - 3 * 3600, "endTs": now_s, "fidelity": 1},
            timeout=15,
        )
        if resp.status_code == 200:
            history = resp.json().get("history", [])
            if history:
                pricing["entry_candle_close"] = history[-1].get("p")
    except Exception as e:
        print(f"  [entry pricing: candle fetch failed for {token_id[:16]}...: {e}]")

    return pricing


def load_tracked():
    if TRACK_PATH.exists():
        with open(TRACK_PATH) as f:
            return json.load(f)
    return {"generated_at": "", "games": [], "stats": {"total_tracked": 0, "resolved": 0, "correct": 0, "incorrect": 0, "accuracy": 0}}


def save_tracked(data):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(TRACK_PATH, "w") as f:
        json.dump(data, f, indent=2)


def take_snapshots(active_games, generated_at, today_str, api_key=None, token_map=None):
    """Snapshot today's active games' sentiment for outcome tracking.

    Only takes a snapshot once per game (first poll on game day wins).
    When api_key/token_map are supplied, also records executable entry
    pricing for the predicted outcome (see fetch_entry_pricing); otherwise
    those fields are left None so this stays callable without live pricing.
    Returns number of new snapshots taken.
    """
    tracked = load_tracked()
    existing_slugs = {g["event_slug"] for g in tracked["games"]}
    new_count = 0

    for slug, game in active_games.items():
        if slug in existing_slugs or slug == "futures-props":
            continue
        if game.get("game_date", "") != today_str:
            continue

        markets = {}
        for m in game.get("all_markets", []):
            if not m.get("has_trader_data"):
                continue
            cid = m.get("condition_id", "")
            if not cid:
                continue
            market_type = m.get("market_type", "")
            predicted_outcome = m.get("top_outcome", "")
            token_id = (token_map or {}).get(cid, {}).get(predicted_outcome)
            pricing = fetch_entry_pricing(token_id, api_key) if (token_id and api_key) else {
                "entry_ask_price": None, "entry_ask_depth": None, "entry_candle_close": None,
            }
            market_entry = {
                "slug": m.get("slug", ""),
                "condition_id": cid,
                "market_type": market_type,
                "predicted_outcome": predicted_outcome,
                "conviction": m.get("conviction", 0),
                "total_weighted_volume": m.get("total_weighted_volume", 0),
                "unique_traders": m.get("unique_traders", 0),
                "depth_imbalance": m.get("orderbook", {}).get("depth_imbalance", None),
                "entry_ask_price": pricing["entry_ask_price"],
                "entry_ask_depth": pricing["entry_ask_depth"],
                "entry_candle_close": pricing["entry_candle_close"],
                "actual_outcome": None,
                "correct": None,
            }
            key = market_type or f"mkt_{cid[:16]}"
            if key in markets:
                key = f"{key}_{cid[:8]}"
            markets[key] = market_entry

        if not markets:
            continue

        game_entry = {
            "event_slug": slug,
            "game_date": game.get("game_date", today_str),
            "snapshot_at": generated_at,
            "markets": markets,
            "resolved": False,
            "resolved_at": None,
        }
        tracked["games"].append(game_entry)
        new_count += 1

    if new_count:
        tracked["generated_at"] = generated_at
        tracked["stats"]["total_tracked"] = len(tracked["games"])
        save_tracked(tracked)
        print(f"\n  Tracked {new_count} new game(s) for outcome monitoring")

    return new_count


def query_resolved_outcome(condition_id, api_key):
    """Query Agent 574 for a resolved market's winning outcome."""
    import requests
    API_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
    body = {
        "agent_id": 574,
        "params": {"condition_id": condition_id, "closed": "True"},
        "formatter_config": {"format_type": "raw"},
    }
    try:
        resp = requests.post(
            API_URL, json=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        results = (resp.json().get("data") or {}).get("results") or []
        if results:
            return results[0].get("winning_outcome")
    except Exception:
        pass
    return None


def check_resolved(api_key):
    """Check resolved outcomes for tracked games whose game date has passed.

    Returns counts of newly resolved games.
    """
    tracked = load_tracked()
    if not tracked.get("games"):
        return 0, 0

    now = datetime.datetime.now(NY_TZ)
    today_str = now.strftime("%Y-%m-%d")
    newly_resolved = 0

    for game in tracked["games"]:
        if game.get("resolved"):
            continue
        game_date = game.get("game_date", "")
        if game_date >= today_str:
            continue

        any_new = False
        for key, m in game.get("markets", {}).items():
            if m.get("correct") is not None:
                continue
            winning = query_resolved_outcome(m["condition_id"], api_key)
            if winning:
                m["actual_outcome"] = winning
                m["correct"] = (m["predicted_outcome"] == winning)
                any_new = True
                time.sleep(0.5)

        if any_new:
            all_done = all(m.get("correct") is not None for m in game["markets"].values())
            if all_done:
                game["resolved"] = True
                game["resolved_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                newly_resolved += 1

    if newly_resolved:
        tracked["generated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        total = len(tracked["games"])
        resolved = sum(1 for g in tracked["games"] if g["resolved"])
        correct = sum(
            1 for g in tracked["games"]
            if g["resolved"]
            for m in g["markets"].values()
            if m.get("correct") is True
        )
        incorrect = sum(
            1 for g in tracked["games"]
            if g["resolved"]
            for m in g["markets"].values()
            if m.get("correct") is False
        )
        total_judged = correct + incorrect
        tracked["stats"] = {
            "total_tracked": total,
            "resolved": resolved,
            "correct": correct,
            "incorrect": incorrect,
            "accuracy": round(correct / total_judged, 4) if total_judged else 0,
        }
        save_tracked(tracked)
        print(f"  Resolved {newly_resolved} game(s) — "
              f"accuracy: {correct}/{total_judged} ({tracked['stats']['accuracy']:.1%})")

    return newly_resolved, sum(1 for g in tracked["games"] if g["resolved"])


def update(active_games, generated_at, today_str, api_key, token_map=None):
    """Orchestrator: take snapshots + check resolutions. Called from poll_live main()."""
    new = take_snapshots(active_games, generated_at, today_str, api_key, token_map)
    resolved, total_resolved = check_resolved(api_key)
    return new, resolved, total_resolved
