#!/usr/bin/env python3
"""MLB-7 consensus analysis: do the 7 persistent-winner wallets

(data/persistent_winners.json survivors) agree with each other on
pre-game markets, and is agreement predictive of settlement ROI?

Reuses analysis/persistent_winners.py's conventions: pre-game fills from
data/lake/pregame_fills.parquet, net pre-game direction per wallet-market
= dominant token by sign of net signed stake (stake_signed, generalizing
portfolio_selection.py's sign_on_a to markets with >2 outcomes), and
settlement ROI via trader_metrics.py's tested reconstruct_positions /
settle_positions cash-flow accounting (invested = buy cost, returned =
sell proceeds + settlement payout). Bootstrap CIs resample at the MARKET
level, 2000 reps, seed=42, matching persistent_winners.py's hygiene.

Fast DuckDB filter (7-wallet fills are ~10k rows) + pandas. No API calls.

Writes: reports/10_mlb7_consensus.md

Usage:
    .venv/bin/python3 analysis/mlb7_consensus.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
DATA_DIR = BASE / "data"
REPORTS_DIR = BASE / "reports"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402

SEASONS = [2025, 2026]
N_BOOT = 2000
RNG_SEED = 42


def load_wallets() -> list[str]:
    with open(DATA_DIR / "persistent_winners.json") as f:
        d = json.load(f)
    return [r["wallet"] for r in d["survivors"]]


def load_fills(wallets: list[str]) -> pd.DataFrame:
    con = duckdb.connect()
    wdf = pd.DataFrame({"wallet": wallets})
    con.register("wdf", wdf)
    q = f"""
        SELECT f.*
        FROM read_parquet('{LAKE_DIR / "pregame_fills.parquet"}') f
        JOIN wdf ON f.proxy_wallet = wdf.wallet
    """
    fills = con.execute(q).fetchdf()
    con.close()
    return fills


def load_markets() -> pd.DataFrame:
    return pd.read_parquet(LAKE_DIR / "markets.parquet",
                            columns=["condition_id", "event_id", "market_type", "season"])


# ---------------------------------------------------------------------------
# Per (wallet, market) side + entry time
# ---------------------------------------------------------------------------
def wallet_market_side(fills: pd.DataFrame) -> pd.DataFrame:
    """Per (wallet, condition_id): dominant token_id (side) by net signed
    stake magnitude, first fill timestamp (entry time), season, market_type."""
    g = fills.groupby(["proxy_wallet", "condition_id", "token_id"], as_index=False).agg(
        net_stake=("stake_signed", "sum"),
        first_ts=("timestamp", "min"),
    )
    g["abs_stake"] = g["net_stake"].abs()
    idx = g.groupby(["proxy_wallet", "condition_id"])["abs_stake"].idxmax()
    side = g.loc[idx, ["proxy_wallet", "condition_id", "token_id"]].rename(columns={"token_id": "side_token"})

    first_ts = fills.groupby(["proxy_wallet", "condition_id"], as_index=False)["timestamp"].min().rename(
        columns={"timestamp": "first_ts"})
    meta = fills.drop_duplicates(["proxy_wallet", "condition_id"])[
        ["proxy_wallet", "condition_id", "season", "market_type"]
    ]
    out = side.merge(first_ts, on=["proxy_wallet", "condition_id"]).merge(
        meta, on=["proxy_wallet", "condition_id"]
    )
    return out.rename(columns={"proxy_wallet": "wallet"})


def wallet_market_pnl(fills: pd.DataFrame) -> pd.DataFrame:
    """Per (wallet, condition_id): invested/returned via tested cash-flow
    reconstruction (same logic as persistent_winners.py's pregame_wm)."""
    cols = ["proxy_wallet", "condition_id", "token_id", "side", "size", "price", "trade_id", "timestamp"]
    out = []
    for season in SEASONS:
        fs = fills[fills["season"] == season]
        if fs.empty:
            continue
        token_winner = fs[["condition_id", "token_id", "is_winner", "market_type", "outcome"]].drop_duplicates()
        pos = tm.reconstruct_positions(fs[cols])
        pos = tm.settle_positions(pos, token_winner)
        pos = pos.rename(columns={"proxy_wallet": "wallet"})
        pos = pos[pos["invested"] > 0]
        g = pos.groupby(["wallet", "condition_id"], as_index=False).agg(
            invested=("invested", "sum"), returned=("returned", "sum"),
        )
        g["season"] = season
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
        columns=["wallet", "condition_id", "invested", "returned", "season"])


# ---------------------------------------------------------------------------
# Bootstrap (resample at market level)
# ---------------------------------------------------------------------------
def bootstrap_roi_ci(inv: np.ndarray, ret: np.ndarray, n_boot=N_BOOT, seed=RNG_SEED):
    n = len(inv)
    if n < 5:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    stats = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        s_inv, s_ret = inv[idx].sum(), ret[idx].sum()
        stats[i] = (s_ret - s_inv) / s_inv if s_inv > 0 else np.nan
    return float(np.nanpercentile(stats, 2.5)), float(np.nanpercentile(stats, 97.5))


def pooled_roi(inv: np.ndarray, ret: np.ndarray) -> float:
    s_inv = inv.sum()
    return float((ret.sum() - s_inv) / s_inv) if s_inv > 0 else float("nan")


# ---------------------------------------------------------------------------
# Q1: overlap frequency (same-market, and looser same-game)
# ---------------------------------------------------------------------------
def overlap_frequency(wm: pd.DataFrame, markets: pd.DataFrame) -> dict:
    wm = wm.merge(markets[["condition_id", "event_id"]], on="condition_id", how="left")
    mkt_counts = wm.groupby("condition_id")["wallet"].nunique().rename("n_of_7").reset_index()
    wm = wm.merge(mkt_counts, on="condition_id", how="left")

    # per-wallet: fraction of own markets with >=1 other of the 7 present
    per_wallet = []
    for w, sub in wm.groupby("wallet"):
        n_total = len(sub)
        n_overlap = (sub["n_of_7"] >= 2).sum()
        per_wallet.append(dict(wallet=w, n_markets=n_total,
                                n_with_overlap=int(n_overlap),
                                frac_overlap=round(n_overlap / n_total, 4) if n_total else float("nan")))

    # distribution of MARKETS by count of the 7 present (dedup per market)
    dist = mkt_counts["n_of_7"].value_counts().sort_index()
    dist_bucketed = {
        "1": int(dist.get(1, 0)),
        "2": int(dist.get(2, 0)),
        "3+": int(dist[dist.index >= 3].sum()),
    }

    # same-GAME overlap (different markets, same event) -- for each wallet's
    # markets, is there >=1 other of the 7 present anywhere in the same event,
    # counting markets where they are NOT already in the same condition_id
    event_wallets = wm.groupby("event_id")["wallet"].apply(lambda s: set(s)).rename("event_wallet_set")
    wm = wm.merge(event_wallets, on="event_id", how="left")

    def _other_in_event(row):
        others = row["event_wallet_set"] - {row["wallet"]}
        return len(others) > 0

    wm["other_in_same_game"] = wm.apply(_other_in_event, axis=1)
    per_wallet_game = wm.groupby("wallet").agg(
        n_markets=("condition_id", "size"),
        n_with_game_overlap=("other_in_same_game", "sum"),
    ).reset_index()
    per_wallet_game["frac_game_overlap"] = (per_wallet_game["n_with_game_overlap"] /
                                             per_wallet_game["n_markets"]).round(4)

    return dict(per_wallet=per_wallet, market_dist=dist_bucketed,
                per_wallet_game=per_wallet_game.to_dict("records"),
                n_markets_total=int(mkt_counts.shape[0]))


# ---------------------------------------------------------------------------
# Q2: agreement rate when >=2 in same market
# ---------------------------------------------------------------------------
def agreement_rate(wm: pd.DataFrame) -> dict:
    mkt_counts = wm.groupby("condition_id")["wallet"].nunique()
    multi = mkt_counts[mkt_counts >= 2].index
    sub = wm[wm["condition_id"].isin(multi)]
    n_sides = sub.groupby("condition_id")["side_token"].nunique()
    n_agree = int((n_sides == 1).sum())
    n_disagree = int((n_sides >= 2).sum())
    n_total = len(n_sides)
    return dict(n_markets_multi=n_total, n_agree=n_agree, n_disagree=n_disagree,
                agree_rate=round(n_agree / n_total, 4) if n_total else float("nan"))


# ---------------------------------------------------------------------------
# Q3: money question -- ROI bucketed by consensus
# ---------------------------------------------------------------------------
def consensus_money(wm: pd.DataFrame, pnl: pd.DataFrame, token_winner: pd.DataFrame) -> dict:
    wm = wm.merge(pnl, on=["wallet", "condition_id", "season"], how="inner")
    mkt_n = wm.groupby("condition_id")["wallet"].nunique().rename("n_of_7")
    n_sides = wm.groupby("condition_id")["side_token"].nunique().rename("n_sides")
    mkt_meta = pd.concat([mkt_n, n_sides], axis=1).reset_index()
    wm = wm.merge(mkt_meta, on="condition_id", how="left")

    def bucket(row):
        if row["n_of_7"] == 1:
            return "solo"
        if row["n_sides"] >= 2:
            return "disagree"
        if row["n_of_7"] == 2:
            return "2_agree"
        return "3+_agree"

    wm["bucket"] = wm.apply(bucket, axis=1)

    out = {}
    for b, sub in wm.groupby("bucket"):
        # aggregate to market level first (sum across wallets within market),
        # then bootstrap resample at the market level
        mkt_agg = sub.groupby("condition_id", as_index=False).agg(invested=("invested", "sum"),
                                                                    returned=("returned", "sum"))
        inv, ret = mkt_agg["invested"].to_numpy(), mkt_agg["returned"].to_numpy()
        roi = pooled_roi(inv, ret)
        ci = bootstrap_roi_ci(inv, ret)
        out[b] = dict(n_markets=len(mkt_agg), n_wallet_market_rows=len(sub),
                      invested=round(float(inv.sum()), 2), returned=round(float(ret.sum()), 2),
                      roi=round(roi, 4), roi_ci95=[round(ci[0], 4), round(ci[1], 4)])

    # disagreement markets: does the bigger-stake side win settlement?
    # ("won" = side_token is the settlement winner, from token_winner --
    # NOT inferred from returned>0, since a wallet may have sold pre-
    # resolution and realized proceeds regardless of eventual settlement)
    dis = wm[wm["bucket"] == "disagree"]
    dis_detail = None
    if not dis.empty:
        side_agg = dis.groupby(["condition_id", "side_token"], as_index=False).agg(
            invested=("invested", "sum"), returned=("returned", "sum"))
        side_agg = side_agg.merge(
            token_winner.rename(columns={"token_id": "side_token"})[["condition_id", "side_token", "is_winner"]],
            on=["condition_id", "side_token"], how="left",
        )
        rows = []
        for cid, sub in side_agg.groupby("condition_id"):
            sub = sub.sort_values("invested", ascending=False).reset_index(drop=True)
            bigger = sub.iloc[0]
            if pd.isna(bigger["is_winner"]):
                continue
            rows.append(dict(condition_id=cid, bigger_invested=bigger["invested"],
                              bigger_won=bool(bigger["is_winner"])))
        dd = pd.DataFrame(rows)
        dis_detail = dict(n_markets=len(dd),
                          bigger_stake_win_rate=round(float(dd["bigger_won"].mean()), 4) if len(dd) else float("nan"))
    return dict(buckets=out, disagreement_detail=dis_detail)


# ---------------------------------------------------------------------------
# Q4: market-type mix of overlap markets + time gap in agreement markets
# ---------------------------------------------------------------------------
def market_type_and_timegap(wm: pd.DataFrame) -> dict:
    mkt_counts = wm.groupby("condition_id")["wallet"].nunique().rename("n_of_7")
    mkt_type = wm.drop_duplicates("condition_id").set_index("condition_id")["market_type"]
    merged = pd.concat([mkt_counts, mkt_type], axis=1).reset_index()
    merged["is_overlap"] = merged["n_of_7"] >= 2

    mix = {}
    for grp, sub in merged.groupby("is_overlap"):
        vc = sub["market_type"].value_counts(normalize=True).round(4)
        mix["overlap" if grp else "solo"] = dict(n=len(sub), pct=vc.to_dict())

    # time gap: for agreement markets (same side, n_of_7>=2), gap between
    # first and second wallet's entry
    n_sides = wm.groupby("condition_id")["side_token"].nunique()
    agree_mkts = n_sides[n_sides == 1].index
    multi = mkt_counts[mkt_counts >= 2].index
    agree_multi = set(agree_mkts) & set(multi)
    gaps = []
    for cid, sub in wm[wm["condition_id"].isin(agree_multi)].groupby("condition_id"):
        ts = sub["first_ts"].sort_values().reset_index(drop=True)
        if len(ts) >= 2:
            gap_min = (ts.iloc[1] - ts.iloc[0]).total_seconds() / 60.0
            gaps.append(gap_min)
    gaps = np.array(gaps)
    gap_stats = dict(n=len(gaps),
                      median_minutes=round(float(np.median(gaps)), 1) if len(gaps) else float("nan"),
                      p25_minutes=round(float(np.percentile(gaps, 25)), 1) if len(gaps) else float("nan"),
                      p75_minutes=round(float(np.percentile(gaps, 75)), 1) if len(gaps) else float("nan"))
    return dict(market_type_mix=mix, time_gap=gap_stats)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_period(fills: pd.DataFrame, markets: pd.DataFrame, label: str) -> dict:
    wm = wallet_market_side(fills)
    pnl = wallet_market_pnl(fills)
    token_winner = fills[["condition_id", "token_id", "is_winner"]].drop_duplicates()
    q1 = overlap_frequency(wm, markets)
    q2 = agreement_rate(wm)
    q3 = consensus_money(wm, pnl, token_winner)
    q4 = market_type_and_timegap(wm)
    return dict(label=label, overlap=q1, agreement=q2, money=q3, market_mix=q4)


def main() -> None:
    wallets = load_wallets()
    print(f"7 wallets loaded: {len(wallets)}")
    fills = load_fills(wallets)
    print(f"Pre-game fills for the 7: {len(fills)} rows, "
          f"{fills['condition_id'].nunique()} distinct markets")
    markets = load_markets()

    results = {}
    results["pooled"] = run_period(fills, markets, "pooled 2025+2026")
    for season in SEASONS:
        results[str(season)] = run_period(fills[fills["season"] == season], markets, str(season))

    write_report(results, wallets)
    # also dump raw json for reproducibility
    with open(DATA_DIR / "mlb7_consensus.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("Wrote data/mlb7_consensus.json")


def write_report(results: dict, wallets: list[str]) -> None:
    lines = []
    lines.append("# Report 10 — MLB-7 Consensus Analysis\n")
    lines.append(f"Do the {len(wallets)} persistent-winner wallets "
                 "(`data/persistent_winners.json`) agree with each other on pre-game "
                 "MLB markets, and is agreement predictive of settlement ROI? "
                 "Pipeline: `analysis/mlb7_consensus.py`. Pre-game fills, cash-flow "
                 "settlement ROI, dominant-token-by-net-signed-stake direction — same "
                 "conventions as `analysis/persistent_winners.py`.\n")
    lines.append("Wallets: " + ", ".join(f"`{w}`" for w in wallets) + "\n")

    for key in ["pooled", "2025", "2026"]:
        r = results[key]
        lines.append(f"## {r['label']}\n")

        # Q1
        lines.append("### Q1 — Overlap frequency\n")
        ov = r["overlap"]
        lines.append(f"Total distinct pre-game markets touched by the 7: {ov['n_markets_total']}\n")
        lines.append("Distribution of markets by count of the 7 present:\n")
        lines.append("| count of 7 present | n markets |\n|---|---:|")
        for k, v in ov["market_dist"].items():
            lines.append(f"| {k} | {v} |")
        lines.append("")
        lines.append("Per-wallet same-MARKET overlap (fraction of own pre-game markets "
                     "with >=1 other of the 7 also present):\n")
        lines.append("| wallet | n markets | n with overlap | frac overlap |\n|---|---:|---:|---:|")
        for row in ov["per_wallet"]:
            lines.append(f"| `{row['wallet']}` | {row['n_markets']} | {row['n_with_overlap']} | "
                         f"{row['frac_overlap']:.1%} |")
        lines.append("")
        lines.append("Per-wallet same-GAME overlap (looser: >=1 other of the 7 anywhere in "
                     "the same event, any market):\n")
        lines.append("| wallet | n markets | n with game overlap | frac |\n|---|---:|---:|---:|")
        for row in ov["per_wallet_game"]:
            lines.append(f"| `{row['wallet']}` | {row['n_markets']} | {row['n_with_game_overlap']} | "
                         f"{row['frac_game_overlap']:.1%} |")
        lines.append("")

        # Q2
        lines.append("### Q2 — Agreement rate (when >=2 of the 7 share a market)\n")
        ag = r["agreement"]
        lines.append(f"n markets with >=2 of the 7 present: **{ag['n_markets_multi']}**. "
                     f"Same side: {ag['n_agree']}. Opposite sides: {ag['n_disagree']}. "
                     f"**Agreement rate: {ag['agree_rate']:.1%}** "
                     f"(n={ag['n_markets_multi']}{' -- UNDERPOWERED, n<30' if ag['n_markets_multi'] < 30 else ''}).\n")

        # Q3
        lines.append("### Q3 — The money question: settlement ROI by consensus bucket\n")
        mb = r["money"]["buckets"]
        lines.append("| bucket | n markets | n wallet-market rows | invested $ | ROI (dollar-wtd) | 95% CI |"
                     "\n|---|---:|---:|---:|---:|---|")
        order = ["solo", "2_agree", "3+_agree", "disagree"]
        for b in order:
            if b not in mb:
                continue
            v = mb[b]
            flag = " **UNDERPOWERED (n<30)**" if v["n_markets"] < 30 else ""
            lines.append(f"| {b} | {v['n_markets']} | {v['n_wallet_market_rows']} | "
                         f"${v['invested']:,.0f} | {v['roi']:+.1%}{flag} | "
                         f"[{v['roi_ci95'][0]:+.1%}, {v['roi_ci95'][1]:+.1%}] |")
        lines.append("")
        dd = r["money"]["disagreement_detail"]
        if dd:
            lines.append(f"Disagreement markets: bigger-stake side wins settlement "
                         f"**{dd['bigger_stake_win_rate']:.1%}** of the time (n={dd['n_markets']}"
                         f"{', UNDERPOWERED' if dd['n_markets'] < 30 else ''}).\n")
        else:
            lines.append("No disagreement markets found in this period.\n")

        # Q4
        lines.append("### Q4 — Market-type mix of overlaps, and time-to-copy\n")
        mm = r["market_mix"]["market_type_mix"]
        for grp in ["overlap", "solo"]:
            if grp in mm:
                v = mm[grp]
                pct_str = ", ".join(f"{k} {p:.0%}" for k, p in sorted(v["pct"].items(), key=lambda kv: -kv[1]))
                lines.append(f"- {grp} markets (n={v['n']}): {pct_str}")
        lines.append("")
        tg = r["market_mix"]["time_gap"]
        lines.append(f"Time gap between first and second wallet's entry in agreement markets: "
                     f"median **{tg['median_minutes']:.0f} min** (IQR {tg['p25_minutes']:.0f}-"
                     f"{tg['p75_minutes']:.0f} min), n={tg['n']}"
                     f"{' -- UNDERPOWERED, n<30' if tg['n'] < 30 else ''}.\n")

    out_path = REPORTS_DIR / "10_mlb7_consensus.md"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
