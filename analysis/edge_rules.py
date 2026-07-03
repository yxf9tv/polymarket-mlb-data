#!/usr/bin/env python3
"""Phase 3: is the Phase-2 crowd-flow edge real and executable after
slippage, and can any interpretable rule survive it?

RESEARCH_PLAN.md Phase 3 (redesigned per Phase 2's verdict): Phase 2 found
no trader-selection portfolio deployably beats baselines, BUT the aggregate
crowd (all-wallets-equal pre-game net flow) beats market-favorite by
+2.0-2.2% (CI excludes zero, both protocols), concentrated in totals
markets (+5.1-5.3%), all measured at ZERO-SLIPPAGE entry (last pre-game
candle close). This script:
  H5   -- does resting pre-game orderbook depth imbalance predict settlement,
          and does it ADD to the crowd-flow signal (logistic: outcome ~
          flow_margin + depth_imbalance)?
  Exec -- walk the pre-game book on the crowd-signal side for $50/$200/$1000
          stakes; recompute the crowd portfolio's settlement ROI at
          depth-weighted fill prices instead of the zero-slippage candle
          close. This is the make-or-break number.
  Rules -- mine interpretable buckets (market_type x flow-margin bin x
          liquidity bin x price band) on the after-slippage $200 ROI;
          n>=50, bootstrap CI lower bound > 0 on a train split, re-scored
          (not re-selected) on a validate split -> data/edge_rules.json.
  Copy  -- price decay after topk_clv-cohort (data/trader_portfolio.json)
          pre-game entries: median price move on their side at +1h/+3h/+6h
          -- is there a copy-trading latency window?

DATA CONSTRAINT DISCOVERED DURING INGEST (see ingest_orderbooks.py
docstring): intel 572 orderbook history has ZERO retention for the 2025
season (verified on 17 spot checks incl. the 5 highest-volume 2025
markets) -- coverage starts ~2026-03-26, a rolling ~3-month window as of
the 2026-07-03 ingest date. This forces two deviations from the plan:
  (a) the 2025 orderbook sample is empty by construction -- H5/Exec/Rules
      run on 2026-only data (2,614 crowd-signal totals/moneyline markets,
      1 O/U line/game after dedup -- see select_orderbook_targets.py).
  (b) the "train on 2025+early-2026, validate on latest 2026" rule-mining
      split becomes a WITHIN-2026 time split instead (train = first_pitch <
      the 70th percentile date among priced markets; validate = the rest) --
      still a genuine held-out temporal split, just narrower than planned.
Both are stated plainly in reports/07_edge_rules.md, not glossed over.

Usage:
    .venv/bin/python3 analysis/edge_rules.py
"""

from __future__ import annotations

import json
import sys
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
REPORTS_DIR = BASE / "reports"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402

N_BOOT = 2000
RNG_SEED = 0
STAKE_SIZES = [50, 200, 1000]
RULE_STAKE = 200  # canonical stake size for rule mining
RULE_MIN_N = 50
FULL_FILL_TOL = 0.999  # "fully filled" if walked notional >= stake * this


def _json_default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    return str(o)


def bootstrap_ci_mean(values, n_boot: int = N_BOOT, seed: int = RNG_SEED) -> tuple[float, float]:
    v = pd.Series(values).dropna().to_numpy()
    n = len(v)
    if n < 10:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    means = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        means[i] = v[idx].mean()
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


# =============================================================================
# Load + join
# =============================================================================
def load_data():
    print("Loading lake tables...")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    schedule = pd.read_parquet(LAKE_DIR / "schedule.parquet")
    candles = pd.read_parquet(LAKE_DIR / "candles.parquet")
    fills = pd.read_parquet(LAKE_DIR / "pregame_fills.parquet")
    targets = pd.read_parquet(LAKE_DIR / "orderbook_targets.parquet")
    ob_path = LAKE_DIR / "orderbook.parquet"
    if not ob_path.exists():
        raise SystemExit(f"{ob_path} missing -- run build_orderbook_lake.py first.")
    orderbook = pd.read_parquet(ob_path)
    return markets, schedule, candles, fills, targets, orderbook


def build_signal_book(targets: pd.DataFrame, orderbook: pd.DataFrame) -> pd.DataFrame:
    """One row per target market: last pre-first-pitch orderbook snapshot on
    the crowd-signal token, joined to the target's settlement/signal fields."""
    ob = orderbook.copy()
    ob["ts"] = pd.to_datetime(ob["ts"], utc=True)
    t = targets.copy()
    if "cohort" not in t.columns:
        t["cohort"] = "primary"
    t["first_pitch"] = pd.to_datetime(t["first_pitch"], utc=True)
    ob = ob.merge(t[["condition_id", "first_pitch"]], on="condition_id", how="inner")
    ob = ob[ob["ts"] < ob["first_pitch"]]
    last = ob.sort_values(["condition_id", "ts"]).groupby("condition_id").tail(1)

    sb = t.merge(last, on=["condition_id", "token_id"], how="inner", suffixes=("", "_ob"))
    sb["total_depth_usd"] = sb["bid_depth_usd"].fillna(0) + sb["ask_depth_usd"].fillna(0)
    sb["depth_imbalance"] = np.where(
        sb["total_depth_usd"] > 0, sb["bid_depth_usd"].fillna(0) / sb["total_depth_usd"], np.nan,
    )
    print(f"Signal book: {len(t)} target markets -> {len(sb)} with a pre-first-pitch orderbook snapshot "
          f"({len(sb) / len(t) * 100:.1f}% coverage)")
    return sb


# =============================================================================
# H5: depth imbalance vs settlement, does it ADD to flow_margin?
# =============================================================================
def h5_depth_imbalance(sb: pd.DataFrame) -> dict:
    d = sb.dropna(subset=["depth_imbalance", "signal_strength", "is_winner"]).copy()
    d["y"] = d["is_winner"].astype(int)
    n = len(d)
    print(f"H5: {n} markets with depth_imbalance + flow_margin + settlement")
    if n < 50:
        return {"n": n, "note": "too few markets for a stable logistic fit"}

    X_flow = StandardScaler().fit_transform(d[["signal_strength"]].to_numpy())
    X_both = StandardScaler().fit_transform(d[["signal_strength", "depth_imbalance"]].to_numpy())
    y = d["y"].to_numpy()

    def _fit_auc(X, y):
        m = LogisticRegression(max_iter=1000)
        m.fit(X, y)
        p = m.predict_proba(X)[:, 1]
        try:
            auc = roc_auc_score(y, p)
        except ValueError:
            auc = float("nan")
        return m, auc

    m_flow, auc_flow = _fit_auc(X_flow, y)
    m_both, auc_both = _fit_auc(X_both, y)

    # bootstrap: resample markets, refit both models each time, record AUC delta
    # and the depth_imbalance coefficient (index 1 of the 2-feature model)
    rng = np.random.default_rng(RNG_SEED)
    auc_deltas = np.empty(N_BOOT)
    depth_coefs = np.empty(N_BOOT)
    for i in range(N_BOOT):
        idx = rng.integers(0, n, n)
        yb = y[idx]
        if yb.sum() == 0 or yb.sum() == len(yb):
            auc_deltas[i] = np.nan
            depth_coefs[i] = np.nan
            continue
        Xf, Xb = X_flow[idx], X_both[idx]
        mf = LogisticRegression(max_iter=1000).fit(Xf, yb)
        mb = LogisticRegression(max_iter=1000).fit(Xb, yb)
        try:
            auc_f = roc_auc_score(yb, mf.predict_proba(Xf)[:, 1])
            auc_b = roc_auc_score(yb, mb.predict_proba(Xb)[:, 1])
            auc_deltas[i] = auc_b - auc_f
        except ValueError:
            auc_deltas[i] = np.nan
        depth_coefs[i] = mb.coef_[0][1]

    auc_delta_ci = (float(np.nanpercentile(auc_deltas, 2.5)), float(np.nanpercentile(auc_deltas, 97.5)))
    depth_coef_ci = (float(np.nanpercentile(depth_coefs, 2.5)), float(np.nanpercentile(depth_coefs, 97.5)))

    # simple univariate check too: does depth_imbalance alone separate winners/losers?
    corr = float(pd.Series(d["depth_imbalance"]).corr(pd.Series(d["y"])))

    verdict_adds_signal = depth_coef_ci[0] > 0 or depth_coef_ci[1] < 0
    return {
        "n": n,
        "auc_flow_only": float(auc_flow),
        "auc_flow_plus_depth": float(auc_both),
        "auc_delta": float(auc_both - auc_flow),
        "auc_delta_ci95": auc_delta_ci,
        "depth_coef": float(m_both.coef_[0][1]),
        "depth_coef_ci95": depth_coef_ci,
        "flow_coef_in_both_model": float(m_both.coef_[0][0]),
        "univariate_corr_depth_vs_win": corr,
        "verdict_depth_adds_signal": bool(verdict_adds_signal),
    }


# =============================================================================
# Executability: walk the book, recompute ROI at depth-weighted fill prices
# =============================================================================
def walk_book_fill(levels: list, stake_usd: float) -> tuple[float | None, float]:
    """levels: [[price, size], ...] sorted best-first (ascending price for
    asks -- what we buy against). Returns (vwap_fill_price or None if never
    fully filled, notional_filled)."""
    remaining = stake_usd
    cost = 0.0
    shares = 0.0
    for price, size in levels:
        level_notional = price * size
        if level_notional <= remaining:
            cost += level_notional
            shares += size
            remaining -= level_notional
        else:
            frac_shares = remaining / price
            cost += remaining
            shares += frac_shares
            remaining = 0.0
            break
        if remaining <= 1e-9:
            break
    filled_notional = stake_usd - remaining
    vwap = (cost / shares) if shares > 0 else None
    fully_filled = filled_notional >= stake_usd * FULL_FILL_TOL
    return (vwap if fully_filled else None), filled_notional


def depth_within_cents(levels: list, ref_price: float, cents: float) -> float:
    if ref_price is None or pd.isna(ref_price):
        return 0.0
    cap = ref_price + cents
    return sum(p * s for p, s in levels if p <= cap)


def compute_executability(sb: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for r in sb.itertuples(index=False):
        asks = json.loads(r.asks_json) if isinstance(r.asks_json, str) else []
        row = {
            "condition_id": r.condition_id, "market_type": r.market_type, "season": r.season,
            "first_pitch": r.first_pitch, "is_winner": r.is_winner, "signal_strength": r.signal_strength,
            "entry_price": r.entry_price, "total_depth_usd": r.total_depth_usd,
            "ask_depth_usd": r.ask_depth_usd, "depth_imbalance": r.depth_imbalance,
            "cohort": getattr(r, "cohort", "primary"),
        }
        ref_price = r.entry_price
        for cents, label in [(0.01, "1c"), (0.02, "2c"), (0.05, "5c")]:
            row[f"depth_within_{label}_usd"] = depth_within_cents(asks, ref_price, cents)
        for stake in STAKE_SIZES:
            vwap, filled = walk_book_fill(asks, stake)
            row[f"fill_price_{stake}"] = vwap
            row[f"filled_notional_{stake}"] = filled
        rows.append(row)
    return pd.DataFrame(rows)


def roi_at_price(is_winner, price) -> float:
    return (float(is_winner) - price) / price if price and price > 0 else np.nan


def _exec_summary_block(d: pd.DataFrame) -> dict:
    """zero-slippage + per-stake summary for one slice of the exec table."""
    block: dict = {"n_total_signal_markets": int(len(d))}
    pz = d.dropna(subset=["roi_zero_slippage"])
    block["zero_slippage"] = {
        "n": int(len(pz)), "roi_mean": float(pz["roi_zero_slippage"].mean()) if len(pz) else float("nan"),
        "roi_ci95": bootstrap_ci_mean(pz["roi_zero_slippage"]) if len(pz) else (float("nan"), float("nan")),
    }
    for stake in STAKE_SIZES:
        pr = d.dropna(subset=[f"roi_{stake}"])
        block[f"stake_{stake}"] = {
            "n_fillable": int(len(pr)), "fill_coverage": float(len(pr) / len(d)) if len(d) else float("nan"),
            "roi_mean": float(pr[f"roi_{stake}"].mean()) if len(pr) else float("nan"),
            "roi_ci95": bootstrap_ci_mean(pr[f"roi_{stake}"]) if len(pr) else (float("nan"), float("nan")),
        }
    return block


def executability_table(ex: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    """Per-cohort executability summary. Cohorts are reported separately --
    'primary' (all 2026 crowd-signal moneyline + max-stake totals lines) is a
    census, 'alt_line' is a 1,800-market random sample of the other totals
    lines -- pooling them unweighted would mean nothing."""
    ex = ex.copy()
    ex["roi_zero_slippage"] = ex.apply(lambda r: roi_at_price(r["is_winner"], r["entry_price"]), axis=1)
    for stake in STAKE_SIZES:
        ex[f"roi_{stake}"] = ex.apply(lambda r, s=stake: roi_at_price(r["is_winner"], r[f"fill_price_{s}"]), axis=1)

    out: dict = {"cohorts": {}}
    for cohort, d in ex.groupby("cohort"):
        block = _exec_summary_block(d)
        block["by_market_type"] = {mtype: _exec_summary_block(dm) for mtype, dm in d.groupby("market_type")}
        out["cohorts"][cohort] = block
    return out, ex


# =============================================================================
# Edge rule mining
# =============================================================================
def bucketize(ex: pd.DataFrame) -> pd.DataFrame:
    d = ex.dropna(subset=[f"roi_{RULE_STAKE}", "signal_strength", "total_depth_usd", "entry_price"]).copy()
    # deployable line-kind condition: moneyline / the game's max-crowd-stake
    # O/U line / any other (alternate) O/U line
    d["line_kind"] = np.where(d["market_type"] == "moneyline", "moneyline",
                               np.where(d["cohort"] == "alt_line", "alt_total_line", "primary_total_line"))
    d["flow_bin"] = pd.cut(d["signal_strength"], bins=[-0.001, 0.3, 0.6, 1.001],
                            labels=["flow_low(<=0.3)", "flow_mid(0.3-0.6)", "flow_high(>0.6)"])
    try:
        d["depth_bin"] = pd.qcut(d["total_depth_usd"], q=3, labels=["depth_thin", "depth_mid", "depth_deep"],
                                  duplicates="drop")
    except (ValueError, IndexError):
        # too many duplicate depth values (e.g. many empty books) for 3 clean
        # buckets -- fall back to a single depth bucket rather than erroring
        d["depth_bin"] = "depth_all"
    d["price_band"] = pd.cut(d["entry_price"], bins=[0, 0.4, 0.6, 1.0],
                              labels=["price_dog(<0.4)", "price_close(0.4-0.6)", "price_fav(>0.6)"])
    return d


def mine_rules(d_train: pd.DataFrame, d_val: pd.DataFrame) -> list[dict]:
    rules = []
    dims = ["line_kind", "flow_bin", "depth_bin", "price_band"]
    levels = {dim: sorted(d_train[dim].dropna().unique(), key=str) for dim in dims}

    # 1-way, 2-way, and 3-way combinations (skip full 4-way -- guaranteed n<50 at this sample size)
    from itertools import combinations
    for r in (1, 2, 3):
        for combo_dims in combinations(dims, r):
            for combo_vals in product(*(levels[dim] for dim in combo_dims)):
                mask_train = pd.Series(True, index=d_train.index)
                for dim, val in zip(combo_dims, combo_vals):
                    mask_train &= (d_train[dim] == val)
                sub_train = d_train[mask_train]
                n_train = len(sub_train)
                if n_train < RULE_MIN_N:
                    continue
                roi_vals = sub_train[f"roi_{RULE_STAKE}"]
                roi_mean = float(roi_vals.mean())
                ci = bootstrap_ci_mean(roi_vals)
                if not (ci[0] > 0):
                    continue
                # survives train bar -- score (not re-select) on validate
                mask_val = pd.Series(True, index=d_val.index)
                for dim, val in zip(combo_dims, combo_vals):
                    mask_val &= (d_val[dim] == val)
                sub_val = d_val[mask_val]
                n_val = len(sub_val)
                val_roi = float(sub_val[f"roi_{RULE_STAKE}"].mean()) if n_val else float("nan")
                val_ci = bootstrap_ci_mean(sub_val[f"roi_{RULE_STAKE}"]) if n_val >= 10 else (float("nan"), float("nan"))

                # stake_cap_estimate: largest tested stake still fully fillable
                # in >=80% of this rule's TRAIN markets
                stake_cap = 0
                for stake in STAKE_SIZES:
                    cov = sub_train[f"roi_{stake}"].notna().mean() if f"roi_{stake}" in sub_train else 0.0
                    if cov >= 0.8:
                        stake_cap = stake

                conditions = {dim: str(val) for dim, val in zip(combo_dims, combo_vals)}
                rules.append({
                    "name": "+".join(f"{dim}={val}" for dim, val in zip(combo_dims, combo_vals)),
                    "conditions": conditions,
                    "train": {"n": n_train, "roi_mean": roi_mean, "roi_ci95": ci},
                    "validate": {"n": n_val, "roi_mean": val_roi, "roi_ci95": val_ci},
                    "stake_cap_estimate": stake_cap,
                    "survives_validate": bool(n_val >= 20 and val_ci[0] > 0),
                })
    rules.sort(key=lambda r: r["train"]["roi_mean"], reverse=True)
    return rules


# =============================================================================
# Copy-trade groundwork: price decay after topk_clv-cohort pre-game entries
# =============================================================================
def copy_trade_decay(fills: pd.DataFrame, candles: pd.DataFrame) -> dict:
    portfolio_path = BASE / "data" / "trader_portfolio.json"
    if not portfolio_path.exists():
        return {"note": "data/trader_portfolio.json not found"}
    portfolio = json.load(open(portfolio_path))
    members = [m["wallet"] for m in portfolio.get("closest_contender", {}).get("members_reference_only", [])]
    if not members:
        return {"note": "no members_reference_only in trader_portfolio.json"}

    f = fills[(fills["proxy_wallet"].isin(members)) & (fills["side"] == "BUY")].copy()
    f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
    print(f"Copy-trade decay: {len(f)} pre-game BUY fills from {len(members)} closest_contender members")
    if f.empty:
        return {"note": "no pre-game BUY fills from portfolio members", "n_members": len(members)}

    c = candles[["token_id", "ts", "price"]].rename(columns={"price": "future_price"}).copy()
    c["ts"] = pd.to_datetime(c["ts"], utc=True).astype("datetime64[us, UTC]")
    # restrict candles to tokens the cohort actually traded, then sort for merge_asof(by=)
    c = c[c["token_id"].isin(set(f["token_id"]))].sort_values("ts").reset_index(drop=True)

    horizons = {"1h": pd.Timedelta(hours=1), "3h": pd.Timedelta(hours=3), "6h": pd.Timedelta(hours=6)}
    tol = pd.Timedelta(minutes=45)
    results = {}
    for label, delta in horizons.items():
        f_h = f[["token_id", "timestamp", "price", "market_date"]].copy()
        f_h["target_ts"] = (f_h["timestamp"] + delta).astype("datetime64[us, UTC]")
        # copy-latency question is about PRE-GAME drift: a horizon landing
        # after first pitch would measure in-game repricing (the score), not
        # whether a copier still gets a comparable entry -- exclude those.
        f_h["market_date"] = pd.to_datetime(f_h["market_date"], utc=True)
        f_h = f_h[f_h["target_ts"] <= f_h["market_date"]]
        f_h = f_h.sort_values("target_ts").reset_index(drop=True)
        m = pd.merge_asof(f_h, c, left_on="target_ts", right_on="ts", by="token_id",
                           direction="nearest", tolerance=tol)
        m = m.dropna(subset=["future_price"])
        move = m["future_price"] - m["price"]  # positive = price moved further toward their side (favorable)
        n = len(move)
        results[label] = {
            "n": int(n),
            "median_move": float(move.median()) if n else float("nan"),
            "mean_move": float(move.mean()) if n else float("nan"),
            "frac_still_favorable": float((move > 0).mean()) if n else float("nan"),
            "move_ci95": bootstrap_ci_mean(move) if n >= 10 else (float("nan"), float("nan")),
        }
    return {"n_members": len(members), "n_pregame_buy_fills": int(len(f)), "horizons": results}


# =============================================================================
# Reporting
# =============================================================================
def fmt_pct(x) -> str:
    return f"{x * 100:+.2f}%" if pd.notna(x) else "n/a"


def fmt_ci(ci) -> str:
    a, b = ci
    return f"[{fmt_pct(a)}, {fmt_pct(b)}]" if pd.notna(a) else "[n/a]"


def write_edge_rules_json(rules: list[dict], path: Path, meta: dict) -> None:
    surviving = [r for r in rules if r["survives_validate"]]
    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "meta": meta,
        "n_rules_tested": len(rules),
        "n_rules_survive_train_bar": len(rules),
        "n_rules_survive_validate": len(surviving),
        "rules": [
            {
                "name": r["name"], "conditions": r["conditions"],
                "expected_roi": r["train"]["roi_mean"], "train_ci95": r["train"]["roi_ci95"],
                "n_train": r["train"]["n"], "validate_roi": r["validate"]["roi_mean"],
                "validate_ci95": r["validate"]["roi_ci95"], "n_validate": r["validate"]["n"],
                "stake_cap_estimate": r["stake_cap_estimate"], "survives_validate": r["survives_validate"],
            }
            for r in rules
        ],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=_json_default)
    print(f"Wrote {path}: {len(rules)} rules cleared the train bar, {len(surviving)} also survive validate.")


def crowd_baseline_diagnostic(fills: pd.DataFrame, markets: pd.DataFrame, schedule: pd.DataFrame,
                                candles: pd.DataFrame, targets: pd.DataFrame) -> dict:
    """Reconcile this sample's zero-slippage crowd ROI with Phase 2's
    +2.0-2.2% (totals +5.1-5.3%): same crowd signal, but Phase 2 scored ALL
    totals O/U lines while this sample deduped to 1 line/game (max crowd
    stake) and is restricted to the orderbook-retention window
    (first_pitch >= 2026-03-26). Computes zero-slippage crowd ROI for
    (a) all lines in the retention window, (b) the deduped subset, (c) the
    excluded alternate lines -- so the report can say WHERE any divergence
    comes from rather than hand-waving."""
    import select_orderbook_targets as sot
    first_pitch = tm.build_first_pitch_map(markets, schedule)
    closing_price = sot.build_closing_price_map(candles, markets, first_pitch)

    f = fills[(fills["season"] == 2026) & (fills["market_type"].isin(["total", "moneyline"]))].copy()
    per_tok = f.groupby(["condition_id", "token_id"], as_index=False).agg(
        stake=("stake_signed", "sum"), is_winner=("is_winner", "first"), market_type=("market_type", "first"))
    top = per_tok.sort_values(["condition_id", "stake"], ascending=[True, False]).groupby("condition_id").first().reset_index()
    top["first_pitch"] = top["condition_id"].map(first_pitch)
    top["first_pitch"] = pd.to_datetime(top["first_pitch"], utc=True)
    top = top.dropna(subset=["first_pitch"])
    top = top[top["first_pitch"] >= pd.Timestamp("2026-03-26", tz="UTC")]
    top["entry_price"] = top["token_id"].map(closing_price)
    top = top.dropna(subset=["entry_price"])
    top = top[top["entry_price"] > 0]
    top["roi"] = (top["is_winner"].astype(float) - top["entry_price"]) / top["entry_price"]

    # primary cohort only: the alt_line cohort was sampled FROM the alternate
    # lines, so counting it here would blur the primary-vs-alternate contrast
    if "cohort" in targets.columns:
        dedup_cids = set(targets.loc[targets["cohort"] == "primary", "condition_id"])
    else:
        dedup_cids = set(targets["condition_id"])
    out = {}
    for label, sub in [
        ("all_lines_retention_window", top),
        ("primary_lines(max_stake_dedup)", top[top["condition_id"].isin(dedup_cids)]),
        ("alternate_lines(all)", top[~top["condition_id"].isin(dedup_cids)]),
    ]:
        out[label] = {"n": int(len(sub)), "roi_mean": float(sub["roi"].mean()) if len(sub) else float("nan"),
                       "roi_ci95": bootstrap_ci_mean(sub["roi"]) if len(sub) >= 10 else (float("nan"), float("nan"))}
        by_mt = {}
        for mt, d in sub.groupby("market_type"):
            by_mt[mt] = {"n": int(len(d)), "roi_mean": float(d["roi"].mean()),
                          "roi_ci95": bootstrap_ci_mean(d["roi"]) if len(d) >= 10 else (float("nan"), float("nan"))}
        out[label]["by_market_type"] = by_mt
    return out


def main() -> None:
    markets, schedule, candles, fills, targets, orderbook = load_data()
    sb = build_signal_book(targets, orderbook)

    print("\n=== Crowd-baseline reconciliation diagnostic ===")
    diag = crowd_baseline_diagnostic(fills, markets, schedule, candles, targets)
    print(json.dumps(diag, indent=2, default=_json_default))

    print("\n=== H5: depth imbalance vs settlement ===")
    h5 = h5_depth_imbalance(sb)
    print(json.dumps(h5, indent=2, default=_json_default))

    print("\n=== Executability: walking the book ===")
    ex_raw = compute_executability(sb)
    ex_summary, ex = executability_table(ex_raw)
    print(json.dumps(ex_summary, indent=2, default=_json_default))

    print("\n=== Edge rule mining ===")
    d = bucketize(ex)
    d = d.sort_values("first_pitch")
    if len(d) >= 40:
        cutoff = d["first_pitch"].quantile(0.7)
    else:
        cutoff = d["first_pitch"].median()
    d_train = d[d["first_pitch"] < cutoff]
    d_val = d[d["first_pitch"] >= cutoff]
    print(f"Rule-mining split: cutoff={cutoff}, train n={len(d_train)}, validate n={len(d_val)}")
    rules = mine_rules(d_train, d_val) if len(d_train) >= RULE_MIN_N else []
    meta = {
        "rule_stake_usd": RULE_STAKE, "min_n": RULE_MIN_N, "n_boot": N_BOOT,
        "train_cutoff_first_pitch": cutoff.isoformat() if pd.notna(cutoff) else None,
        "n_train_markets": int(len(d_train)), "n_validate_markets": int(len(d_val)),
        "note": "2025 orderbook data unavailable (retention limit -- see ingest_orderbooks.py); "
                "split is within-2026 time-based (train=earlier 70% by first_pitch, validate=latest 30%), "
                "not the originally planned 2025+early-2026 -> late-2026 split.",
    }
    edge_rules_path = BASE / "data" / "edge_rules.json"
    write_edge_rules_json(rules, edge_rules_path, meta)

    print("\n=== Copy-trade groundwork ===")
    copy_decay = copy_trade_decay(fills, candles)
    print(json.dumps(copy_decay, indent=2, default=_json_default))

    all_results = {
        "crowd_baseline_diagnostic": diag,
        "h5": h5, "executability": ex_summary, "rule_mining_meta": meta,
        "n_rules_tested": len(rules), "n_rules_survive_validate": sum(r["survives_validate"] for r in rules),
        "top_rules_preview": rules[:15],
        "copy_trade_decay": copy_decay,
        "sample_coverage": {
            "n_target_markets": int(len(targets)), "n_with_orderbook_snapshot": int(len(sb)),
            "n_2026": int((targets["season"] == 2026).sum()), "n_2025": int((targets["season"] == 2025).sum()),
        },
    }
    out_path = LAKE_DIR / "edge_rules_raw.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2, default=_json_default)
    print(f"\nWrote {out_path}")

    write_report(h5, ex_summary, rules, copy_decay, sb, targets, diag)


def write_report(h5, ex_summary, rules, copy_decay, sb, targets, diag) -> None:
    surviving = [r for r in rules if r["survives_validate"]]
    lines = []
    lines.append("# Phase 3 Report — Signals & Edge Rules")
    lines.append("")
    lines.append(f"Run date: 2026-07-03. Pipeline: `analysis/edge_rules.py` "
                  f"(target selection: `analysis/select_orderbook_targets.py`, ingest: "
                  f"`analysis/ingest/ingest_orderbooks.py`, lake build: `analysis/build_orderbook_lake.py`).")
    lines.append("")
    lines.append("## Data-retention finding (discovered mid-ingest, not anticipated by the probes)")
    lines.append("")
    lines.append(
        "intel agent 572 (orderbook history) has **zero retention for the entire 2025 season** -- verified on "
        "17 spot checks including the 5 highest-volume 2025 markets (World Series, Blue Jays/Dodgers, Nov 2025), "
        "all returned 0 snapshots. Coverage starts ~2026-03-26 (a 2026-03-03 spring-training market returned 0; "
        "2026-03-26+ markets return data) -- a rolling ~3-month window as of the 2026-07-03 ingest date. "
        "**The planned 500-market 2025 orderbook sample is therefore empty by construction; every result below "
        "is 2026-only.** This also forced the rule-mining train/validate split to be within-2026 (time-based) "
        "instead of the planned 2025+early-2026 -> late-2026 split (see Edge rules section)."
    )
    lines.append("")
    n_2026 = int((targets["season"] == 2026).sum())
    coh = targets.get("cohort", pd.Series("primary", index=targets.index))
    n_primary = int((coh == "primary").sum())
    n_alt = int((coh == "alt_line").sum())
    lines.append(
        f"Sample: {len(targets)} target markets in two cohorts -- `primary` ({n_primary}: all 2026 crowd-signal "
        f"moneyline markets + each game's max-crowd-stake O/U line [avg 5.3 lines/game in the raw lake], plus the "
        f"(empty) 500-market 2025 sample) and `alt_line` ({n_alt}: a random sample of the OTHER 2026 O/U lines, "
        f"added after the reconciliation diagnostic below showed the zero-slippage edge concentrates there). "
        f"{len(sb)}/{n_2026} 2026 markets ({len(sb) / n_2026 * 100:.1f}%) yielded a usable pre-first-pitch "
        f"orderbook snapshot -- the rest either had first_pitch before the 2026-03-26 retention floor or "
        f"the market's book was truly empty at every polled timestamp."
    )
    lines.append("")

    lines.append("## H5 verdict: does depth imbalance predict settlement, and add to the crowd-flow signal?")
    lines.append("")
    if h5.get("n", 0) >= 50:
        verdict = "YES" if h5["verdict_depth_adds_signal"] else "NO"
        lines.append(
            f"**{verdict}.** Logistic outcome ~ flow_margin + depth_imbalance vs. flow_margin alone, n={h5['n']} "
            f"markets with a valid pre-first-pitch depth reading (depth_imbalance = resting bid-side $ / total "
            f"resting $ on the crowd's chosen token -- see caveat below). AUC flow-only={h5['auc_flow_only']:.4f}, "
            f"AUC flow+depth={h5['auc_flow_plus_depth']:.4f}, delta={h5['auc_delta']:+.4f} "
            f"95% CI {fmt_ci_raw(h5['auc_delta_ci95'])}. depth_imbalance coefficient (standardized) = "
            f"{h5['depth_coef']:.3f}, 95% CI {fmt_ci_raw(h5['depth_coef_ci95'])}. Univariate corr(depth_imbalance, "
            f"win) = {h5['univariate_corr_depth_vs_win']:.3f}."
        )
    else:
        lines.append(f"**Inconclusive -- n={h5.get('n', 0)} markets with a valid depth reading, below the "
                      f"n>=50 stability floor for a logistic fit.**")
    lines.append("")
    lines.append(
        "*Caveat on depth_imbalance's definition*: the production dashboard's depth-imbalance metric "
        "(`scripts/poll_live.py::compute_depth_imbalance`) uses BOTH outcome tokens' books (consensus bid + "
        "opposing ask vs. consensus ask + opposing bid). To stay inside the ingest budget, this study fetched "
        "orderbook history for only the crowd's CHOSEN token per market (one call/market instead of two), so "
        "depth_imbalance here is the single-token proxy: resting bid $ / (bid $ + ask $) on that token alone. "
        "This is directionally the same idea (net resting support for the signal side) but not numerically "
        "identical to the dashboard metric; a full two-token re-run would double the orderbook ingest cost. "
        "The fit pools both cohorts (primary + alt_line) -- valid for a conditional model, though the pooled "
        "sample over-represents primary lines relative to the market population."
    )
    lines.append("")

    lines.append("## Crowd-baseline reconciliation: does Phase 2's edge even exist in this window?")
    lines.append("")
    lines.append(
        "Before slippage enters the picture, the zero-slippage crowd ROI must be reconciled with Phase 2's "
        "+2.0-2.2% (totals +5.1-5.3%): this sample differs from Phase 2's crowd baseline in two ways -- (a) it "
        "is restricted to the orderbook-retention window (2026 markets with first_pitch >= 2026-03-26), and "
        "(b) totals were deduped to ONE O/U line per game (max crowd stake) to fit the ingest budget, while "
        "Phase 2 scored every line. Zero-slippage crowd ROI, same signal construction, split three ways:"
    )
    lines.append("")
    lines.append("| slice | n | ROI (mean) | ROI 95% CI | totals-only ROI (n) | moneyline-only ROI (n) |")
    lines.append("|---|---:|---:|---|---|---|")
    for label, d in diag.items():
        mt = d.get("by_market_type", {})
        tot = mt.get("total", {})
        ml = mt.get("moneyline", {})
        lines.append(
            f"| {label} | {d['n']} | {fmt_pct(d['roi_mean'])} | {fmt_ci(d['roi_ci95'])} | "
            f"{fmt_pct(tot.get('roi_mean', float('nan')))} ({tot.get('n', 0)}) | "
            f"{fmt_pct(ml.get('roi_mean', float('nan')))} ({ml.get('n', 0)}) |"
        )
    lines.append("")
    all_roi = diag.get("all_lines_retention_window", {}).get("roi_mean", float("nan"))
    ded_roi = diag.get("primary_lines(max_stake_dedup)", {}).get("roi_mean", float("nan"))
    exc_roi = diag.get("alternate_lines(all)", {}).get("roi_mean", float("nan"))
    lines.append(
        f"Reading: the all-lines slice ({fmt_pct(all_roi)}) is the honest Phase-2-comparable number for this "
        f"window; the primary (max-stake-dedup) lines ({fmt_pct(ded_roi)}) vs ALL alternate lines "
        f"({fmt_pct(exc_roi)}) shows where the zero-slippage edge concentrates -- which is why the alt_line "
        f"cohort was added to the orderbook sample mid-phase."
    )
    lines.append("")

    lines.append("## Executability: the edge-after-slippage table (headline)")
    lines.append("")
    lines.append("Entry side = the crowd's chosen (signal) outcome token, walking the ASK side of its resting "
                  "book at the last snapshot before first pitch, for a stake to be filled fully at that price "
                  "(partial fills are excluded from that stake size's ROI, not padded/assumed).")
    lines.append("")
    lines.append("Cohorts are reported separately -- `primary` (census: all 2026 crowd-signal moneyline markets "
                  "+ each game's max-crowd-stake O/U line) vs `alt_line` (random 1,800-market sample of the other "
                  "O/U lines, where the reconciliation above shows the zero-slippage edge actually lives).")
    lines.append("")
    for cohort, blk in ex_summary["cohorts"].items():
        lines.append(f"**Cohort: {cohort}** ({blk['n_total_signal_markets']} signal markets)")
        lines.append("")
        lines.append("| entry basis | n priced | fill coverage | ROI (mean) | ROI 95% CI |")
        lines.append("|---|---:|---:|---:|---|")
        zs = blk["zero_slippage"]
        lines.append(f"| zero-slippage (candle close) | {zs['n']} | - | {fmt_pct(zs['roi_mean'])} | {fmt_ci(zs['roi_ci95'])} |")
        for stake in STAKE_SIZES:
            s = blk[f"stake_{stake}"]
            lines.append(f"| ${stake} depth-weighted fill | {s['n_fillable']} | {s['fill_coverage'] * 100:.1f}% | "
                          f"{fmt_pct(s['roi_mean'])} | {fmt_ci(s['roi_ci95'])} |")
        lines.append("")
        lines.append("| market_type | n signal markets | zero-slip ROI | $200 fill coverage | $200 ROI | $200 ROI CI |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for mtype, d in blk["by_market_type"].items():
            zs_m = d["zero_slippage"]
            s200 = d["stake_200"]
            lines.append(f"| {mtype} | {d['n_total_signal_markets']} | {fmt_pct(zs_m['roi_mean'])} | "
                          f"{s200['fill_coverage'] * 100:.1f}% | {fmt_pct(s200['roi_mean'])} | {fmt_ci(s200['roi_ci95'])} |")
        lines.append("")

    lines.append("## Edge rules")
    lines.append("")
    n_train = len([r for r in rules])
    lines.append(
        f"{n_train} rule(s) cleared the TRAIN bar (n>=50, bootstrap CI lower bound on ${RULE_STAKE}-stake "
        f"after-slippage ROI > 0); **{len(surviving)} survive re-scoring (not re-selection) on the held-out "
        f"VALIDATE window** (n>=20 and validate CI lower bound > 0)."
    )
    lines.append("")
    if not rules:
        lines.append(
            "**No rule cleared even the train bar.** This is a decisive negative result, stated plainly: once "
            "the crowd-flow signal is priced through the actual resting book at $200 stakes, no "
            "(market_type x flow-margin x liquidity x price-band) bucket shows a bootstrap-CI-positive edge on "
            "this 2026-only sample. Do not deploy a bucket rule off this analysis."
        )
    else:
        lines.append("Top rules by train ROI (validate columns show out-of-sample re-scoring, not re-selection):")
        lines.append("")
        lines.append("| rule | n train | train ROI | train CI | n val | val ROI | val CI | stake cap | survives val |")
        lines.append("|---|---:|---:|---|---:|---:|---|---:|---|")
        for r in rules[:20]:
            lines.append(
                f"| {r['name']} | {r['train']['n']} | {fmt_pct(r['train']['roi_mean'])} | "
                f"{fmt_ci(r['train']['roi_ci95'])} | {r['validate']['n']} | {fmt_pct(r['validate']['roi_mean'])} | "
                f"{fmt_ci(r['validate']['roi_ci95'])} | ${r['stake_cap_estimate']} | "
                f"{'YES' if r['survives_validate'] else 'no'} |"
            )
        if not surviving:
            lines.append("")
            lines.append(
                "**None of the train-bar-clearing rules survive the held-out validate window.** Treat every "
                "row above as train-set overfitting until re-tested on fresh 2026 data; do not deploy."
            )
    lines.append("")

    lines.append("## Copy-trade groundwork: price decay after topk_clv-cohort entries")
    lines.append("")
    if "horizons" in copy_decay:
        lines.append(f"{copy_decay['n_pregame_buy_fills']} pre-game BUY fills from the {copy_decay['n_members']} "
                      f"`data/trader_portfolio.json` closest_contender members, matched to the nearest candle "
                      f"(+/- 45min tolerance) at each horizon after their fill. Horizons landing AFTER first "
                      f"pitch are excluded (they'd measure in-game repricing of the score, not copyable pre-game "
                      f"drift), so n shrinks as the horizon grows. `move` = candle price at +h minus the trader's "
                      f"fill price on the SAME token: positive = the price already ran away from a copier; ~0 = "
                      f"a copier gets essentially the same entry; negative = the copier gets a BETTER entry.")
        lines.append("")
        lines.append("| horizon | n | median price move | mean price move | frac moved against copier | 95% CI (mean) |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for label, h in copy_decay["horizons"].items():
            if h.get("n", 0) == 0:
                lines.append(f"| +{label} | 0 | n/a | n/a | n/a | [n/a] |")
                continue
            lines.append(f"| +{label} | {h['n']} | {h['median_move']:+.4f} | {h['mean_move']:+.4f} | "
                          f"{h['frac_still_favorable'] * 100:.1f}% | {fmt_ci_raw(h['move_ci95'])} |")
    else:
        lines.append(f"Not computed: {copy_decay.get('note', 'unknown reason')}")
    lines.append("")

    lines.append("## Recommendation for Phase 4")
    lines.append("")
    lines.append(_recommendation(h5, ex_summary, rules, surviving))
    lines.append("")

    lines.append("## Files written")
    lines.append("")
    lines.append(f"- `data/lake/orderbook_targets.parquet` -- the {len(targets)}-market sample design "
                  f"(primary + alt_line cohorts)")
    lines.append("- `data/lake/orderbook.parquet` -- parsed pre-first-pitch orderbook snapshots")
    lines.append("- `data/lake/edge_rules_raw.json` -- every number in this report (scalars only)")
    lines.append("- `data/edge_rules.json` -- surviving rule specs (empty-but-valid if none survive)")
    lines.append("- `reports/07_edge_rules.md` -- this report")

    out = REPORTS_DIR / "07_edge_rules.md"
    out.write_text("\n".join(lines))
    print(f"\nWrote {out}")


def fmt_ci_raw(ci) -> str:
    a, b = ci
    if pd.isna(a):
        return "[n/a]"
    return f"[{a:+.4f}, {b:+.4f}]"


def _recommendation(h5, ex_summary, rules, surviving) -> str:
    parts = []
    for cohort, blk in ex_summary["cohorts"].items():
        zs = blk["zero_slippage"]
        s200 = blk["stake_200"]
        if s200["n_fillable"] >= 20 and s200["roi_ci95"][0] > 0:
            parts.append(
                f"[{cohort}] the crowd-flow edge SURVIVES $200-stake slippage: {fmt_pct(s200['roi_mean'])} "
                f"{fmt_ci(s200['roi_ci95'])} (n={s200['n_fillable']}, {s200['fill_coverage']*100:.0f}% fill "
                f"coverage) vs. zero-slippage {fmt_pct(zs['roi_mean'])}."
            )
        else:
            parts.append(
                f"[{cohort}] the crowd-flow edge does NOT clearly survive $200-stake slippage: after-slippage "
                f"ROI {fmt_pct(s200['roi_mean'])} {fmt_ci(s200['roi_ci95'])} (n={s200['n_fillable']}, "
                f"{s200['fill_coverage']*100:.0f}% fill coverage) vs. zero-slippage {fmt_pct(zs['roi_mean'])} "
                f"-- CI does not exclude zero and/or fill coverage too thin."
            )
    if surviving:
        parts.append(f"{len(surviving)} interpretable rule(s) survive train+validate; the strongest is "
                      f"`{surviving[0]['name']}` ({fmt_pct(surviving[0]['validate']['roi_mean'])} on "
                      f"n={surviving[0]['validate']['n']} validate markets, stake cap ${surviving[0]['stake_cap_estimate']}).")
    else:
        parts.append("No interpretable rule survives both train and validate -- do not deploy a bucket rule "
                      "from this analysis.")
    parts.append(
        "Phase 4 should NOT wire a wallet-selection portfolio (confirmed non-beat in Phase 2) nor an "
        "unvalidated edge rule into `scripts/recommend.py`. If a cohort's crowd-flow-at-$200 number above "
        "is positive with CI excluding zero, that cohort's signal is the only candidate worth a small-stake "
        "paper-trading run in Phase 4 -- capped at the stake size actually tested here. Everything else in "
        "this report is a documented null result, not a deployment target."
    )
    return " ".join(parts)


if __name__ == "__main__":
    main()
