"""Generate betting recommendations from live consensus using validated edge rules.

Rules are loaded dynamically from data/edge_rules.json (written by
analysis/edge_rules.py — never hand-edit). As of the Phase 3 backtest
(reports/07_edge_rules.md, 2026-07-03), zero rules survive execution-cost
pricing (crowd-flow/orderbook edges evaporate once priced through the real
resting book at $200 stakes), so data/edge_rules.json is empty-but-valid and
this produces zero bets/fades until a future analysis/edge_rules.py run
lands a validated rule.

Run after poll_live.py to get fresh picks.
"""

import json, time
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
LIVE_PATH = DATA_DIR / "live_signals.json"
EDGE_RULES_PATH = DATA_DIR / "edge_rules.json"
OUTPUT_PATH = DATA_DIR / "bet_recommendations.json"

NO_EDGE_NOTICE = (
    "No validated betting edge exists right now. Backtested crowd-flow/orderbook "
    "signals did not survive execution-cost pricing (see reports/07_edge_rules.md) — "
    "zero rules cleared the train/validate bar. Recommendations will populate only if "
    "a future analysis/edge_rules.py run lands validated rules in data/edge_rules.json."
)


def load_rules():
    """Load edge rules mined by analysis/edge_rules.py. Only rules that
    survived the held-out validate window (not just the train bar) are
    used — see reports/07_edge_rules.md for why train-bar-only rules
    aren't trustworthy enough to deploy."""
    if not EDGE_RULES_PATH.exists():
        return []
    with open(EDGE_RULES_PATH) as f:
        data = json.load(f)
    return [r for r in data.get("rules", []) if r.get("survives_validate")]


def classify_market(m, rules):
    """Match a live market against a mined rule's conditions.

    Rule conditions are bucket labels (e.g. line_kind/flow_bin/depth_bin/
    price_band) computed by analysis/edge_rules.py from settlement
    backtests. The live poll pipeline does not currently compute those same
    buckets on markets, so a rule only matches if every condition key/value
    is present verbatim on the live market record.
    """
    for rule in rules:
        conditions = rule.get("conditions") or {}
        if conditions and all(str(m.get(k)) == str(v) for k, v in conditions.items()):
            return rule
    return None


def main():
    if not LIVE_PATH.exists():
        print("No live data found")
        return

    with open(LIVE_PATH) as f:
        live = json.load(f)

    rules = load_rules()

    markets = live.get("live_consensus_all", [])
    markets_with_data = [m for m in markets if m.get("has_trader_data")]

    bets = []
    fades = []

    for m in markets_with_data:
        rule = classify_market(m, rules)
        if rule is None:
            continue

        rec = {
            "slug": m.get("slug", ""),
            "event_slug": m.get("event_slug", ""),
            "game_date": m.get("game_date", ""),
            "market_type": m.get("market_type", ""),
            "action": "BET",
            "strategy": rule.get("name", ""),
            "predicted_outcome": m.get("top_outcome", "?"),
            "conviction": m.get("conviction", 0),
            "volume": m.get("total_weighted_volume", 0),
            "traders": m.get("unique_traders", 0),
            "expected_roi": round(rule.get("validate_roi", 0) * 100, 1),
            "confidence": "validated",
        }
        bets.append(rec)

    bets.sort(key=lambda r: -r["expected_roi"])
    fades.sort(key=lambda r: -r["expected_roi"])

    output = {
        "generated_at": live.get("generated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        "today": live.get("today", ""),
        "total_analyzed": len(markets_with_data),
        "bets": bets,
        "fades": fades,
        "notice": None if rules else NO_EDGE_NOTICE,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Bet recommendations: {len(bets)} bets, {len(fades)} fades (from {len(markets_with_data)} markets, {len(rules)} validated rules loaded)")


if __name__ == "__main__":
    main()
