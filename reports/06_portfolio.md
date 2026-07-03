# Phase 2 Report — Trader Portfolio Selection

Run date: 2026-07-03. Pipeline: `analysis/portfolio_selection.py`. Pre-game fills: 3,602,267 rows across 15,987 markets, 101,415 wallets (timestamp strictly before first pitch, per the new-format-slug schedule join documented in reports/04).

## Method

Every selector below is fit **only** on a fold's TRAIN window (eligibility floor: ≥30 settled pre-game markets in train) and evaluated **only** on that fold's VALIDATION window -- no peeking. Signal per market = the outcome with the highest weighted net pre-game signed stake, summed over portfolio members (weight × (buy dollars − sell dollars) on that outcome's token); signal strength = (top − second) / Σ|stake|. Settlement ROI is scored at the pre-game entry price (last pre-game candle close for the chosen token) — this is the decision-grade number a live copier would have realized, not a flat-odds approximation. Bootstrap 95% CIs (2,000 resamples) resample **markets**, not fills.

Two walk-forward protocols, per RESEARCH_PLAN.md:

1. **Season fold**: train on all 2025 pre-game-joinable fills, validate on all of 2026-to-date (one large fold).
2. **Rolling 2026 folds**: 6-week train / 2-week validate, stepped by 2 weeks so successive validate windows are contiguous and non-overlapping (5 folds produced from the 2026 season-to-date). Rolling-fold results below are **pooled**: validation markets from every rolling fold are concatenated (they never overlap) into one set and scored together, since a per-fold table alone would recombine into a valid pooled estimate identically -- pooling first also gives an honest bootstrap CI on the combined set rather than 6 separate small-n CIs.

Selectors: top-K (K∈{25,50,100,200}) by raw win rate, excess win rate (dollar-weighted mean(1[won] − entry price) — the price-adjusted calibration edge), settlement ROI, and CLV; empirical-Bayes shrunk win rate top-K; greedy marginal-add (cap 100, candidate pool pre-filtered to top 300 by win rate for tractability, added iff it improves inner-val binary-market accuracy); L1-logistic (binary markets only, stake-on-favored-outcome as feature, C chosen by inner train/val accuracy). Each crossed with 3 weighting schemes: equal, metric-proportional, shrunk-metric-proportional. Baselines: the current list (`data/top_mlb_traders.json` top_tier+watchlist = 147 wallets, 134 after the MM/HFT behavioral-flag filter, legacy 0.5/0.2/0.15/0.15 weight formula replicated read-only from `scripts/compute_sentiment.py`), market-favorite (pre-game closing price > 0.5), and all-wallets-equal-weight (every wallet that traded pre-game in that market, weight=1 — the raw crowd).

## Season fold: train 2025 → validate 2026-to-date

**Full comparison, season fold**

| selector | K | weighting | n_members | n_signals | coverage | accuracy | ROI (mean) | ROI 95% CI | n_priced |
|---|---:|---|---:|---:|---:|---:|---:|---|---:|
| topk_clv | 50 | equal | 50 | 560 | 1.6% | +53.21% | +6.27% | [-2.3%, +15.0%] | 558 |
| topk_clv | 50 | metric_proportional | 50 | 560 | 1.6% | +53.21% | +6.26% | [-2.3%, +15.1%] | 558 |
| topk_excess_win_rate | 200 | equal | 200 | 1204 | 3.5% | +52.41% | +5.94% | [+0.1%, +11.8%] | 1202 |
| topk_clv | 50 | shrunk_metric_proportional | 50 | 560 | 1.6% | +52.68% | +5.58% | [-3.1%, +14.6%] | 558 |
| topk_excess_win_rate | 200 | shrunk_metric_proportional | 200 | 1204 | 3.5% | +52.16% | +5.42% | [-0.4%, +11.3%] | 1202 |
| topk_excess_win_rate | 200 | metric_proportional | 200 | 1204 | 3.5% | +52.16% | +5.41% | [-0.5%, +11.4%] | 1202 |
| topk_settlement_roi | 200 | equal | 200 | 976 | 2.9% | +52.66% | +4.70% | [-1.8%, +10.9%] | 974 |
| topk_settlement_roi | 25 | equal | 25 | 78 | 0.2% | +60.26% | +4.49% | [-14.8%, +23.8%] | 78 |
| topk_settlement_roi | 25 | metric_proportional | 25 | 78 | 0.2% | +60.26% | +4.49% | [-14.8%, +23.8%] | 78 |
| topk_settlement_roi | 25 | shrunk_metric_proportional | 25 | 78 | 0.2% | +60.26% | +4.49% | [-14.8%, +23.8%] | 78 |
| topk_settlement_roi | 200 | metric_proportional | 200 | 976 | 2.9% | +52.46% | +4.24% | [-2.1%, +10.4%] | 974 |
| topk_settlement_roi | 200 | shrunk_metric_proportional | 200 | 976 | 2.9% | +52.36% | +4.05% | [-2.4%, +10.2%] | 974 |
| topk_clv | 100 | shrunk_metric_proportional | 100 | 4757 | 13.9% | +54.95% | +3.21% | [+0.1%, +6.5%] | 4752 |
| topk_clv | 100 | equal | 100 | 4757 | 13.9% | +54.91% | +3.15% | [+0.0%, +6.4%] | 4752 |
| topk_clv | 100 | metric_proportional | 100 | 4757 | 13.9% | +54.89% | +3.07% | [-0.0%, +6.3%] | 4752 |
| topk_clv | 200 | equal | 200 | 5072 | 14.9% | +54.16% | +2.63% | [-0.4%, +5.9%] | 5065 |
| topk_clv | 200 | shrunk_metric_proportional | 200 | 5072 | 14.9% | +54.22% | +2.54% | [-0.5%, +5.8%] | 5065 |
| topk_clv | 200 | metric_proportional | 200 | 5072 | 14.9% | +54.18% | +2.51% | [-0.5%, +5.8%] | 5065 |
| baseline_current_list | - | legacy_formula | 134 | 8136 | 23.8% | +55.80% | +2.42% | [+0.1%, +4.6%] | 8116 |
| baseline_all_wallets_equal | - | equal | 66899 | 13770 | 40.3% | +65.40% | +2.04% | [+0.7%, +3.4%] | 13739 |
| topk_settlement_roi | 100 | equal | 100 | 613 | 1.8% | +50.41% | +0.85% | [-7.0%, +9.3%] | 612 |
| topk_settlement_roi | 100 | metric_proportional | 100 | 613 | 1.8% | +50.41% | +0.85% | [-7.0%, +9.3%] | 612 |
| topk_settlement_roi | 100 | shrunk_metric_proportional | 100 | 613 | 1.8% | +50.41% | +0.85% | [-7.0%, +9.3%] | 612 |
| baseline_market_favorite | - | n/a | 0 | 17818 | 52.2% | +66.01% | +0.01% | [-1.1%, +1.1%] | 17818 |
| topk_excess_win_rate | 100 | equal | 100 | 679 | 2.0% | +49.34% | +0.01% | [-8.0%, +7.8%] | 678 |
| topk_excess_win_rate | 100 | metric_proportional | 100 | 679 | 2.0% | +49.34% | +0.01% | [-8.0%, +7.8%] | 678 |
| topk_shrunk_win_rate | 100 | equal | 100 | 1988 | 5.8% | +51.16% | -0.51% | [-4.8%, +3.8%] | 1984 |
| topk_shrunk_win_rate | 100 | metric_proportional | 100 | 1988 | 5.8% | +51.16% | -0.51% | [-4.8%, +3.8%] | 1984 |
| topk_shrunk_win_rate | 100 | shrunk_metric_proportional | 100 | 1988 | 5.8% | +51.16% | -0.51% | [-4.8%, +3.8%] | 1984 |
| topk_excess_win_rate | 100 | shrunk_metric_proportional | 100 | 679 | 2.0% | +49.04% | -0.62% | [-8.5%, +7.1%] | 678 |
| topk_raw_win_rate | 200 | metric_proportional | 200 | 1487 | 4.4% | +48.89% | -0.66% | [-6.0%, +4.9%] | 1484 |
| topk_raw_win_rate | 200 | shrunk_metric_proportional | 200 | 1487 | 4.4% | +48.89% | -0.66% | [-6.0%, +4.9%] | 1484 |
| topk_shrunk_win_rate | 200 | equal | 200 | 2532 | 7.4% | +49.96% | -0.80% | [-4.8%, +3.3%] | 2522 |
| topk_shrunk_win_rate | 200 | metric_proportional | 200 | 2532 | 7.4% | +49.96% | -0.80% | [-4.8%, +3.3%] | 2522 |
| topk_shrunk_win_rate | 200 | shrunk_metric_proportional | 200 | 2532 | 7.4% | +49.96% | -0.80% | [-4.8%, +3.3%] | 2522 |
| topk_raw_win_rate | 200 | equal | 200 | 1487 | 4.4% | +48.82% | -0.84% | [-6.2%, +4.7%] | 1484 |
| topk_excess_win_rate | 50 | equal | 50 | 477 | 1.4% | +48.85% | -0.97% | [-9.8%, +8.1%] | 476 |
| topk_excess_win_rate | 50 | metric_proportional | 50 | 477 | 1.4% | +48.85% | -0.97% | [-9.8%, +8.1%] | 476 |
| topk_excess_win_rate | 50 | shrunk_metric_proportional | 50 | 477 | 1.4% | +48.85% | -0.97% | [-9.8%, +8.1%] | 476 |
| greedy_marginal_add | 100 | equal | 32 | 693 | 2.0% | +46.32% | -3.30% | [-11.2%, +4.9%] | 690 |
| greedy_marginal_add | 100 | metric_proportional | 32 | 693 | 2.0% | +46.32% | -3.30% | [-11.2%, +4.9%] | 690 |
| greedy_marginal_add | 100 | shrunk_metric_proportional | 32 | 693 | 2.0% | +46.32% | -3.30% | [-11.2%, +4.9%] | 690 |
| topk_raw_win_rate | 100 | metric_proportional | 100 | 1164 | 3.4% | +47.51% | -3.57% | [-9.9%, +2.6%] | 1161 |
| topk_raw_win_rate | 100 | shrunk_metric_proportional | 100 | 1164 | 3.4% | +47.51% | -3.57% | [-9.9%, +2.6%] | 1161 |
| topk_clv | 25 | equal | 25 | 235 | 0.7% | +44.26% | -3.78% | [-17.6%, +12.1%] | 234 |
| topk_raw_win_rate | 100 | equal | 100 | 1164 | 3.4% | +47.42% | -3.80% | [-10.0%, +2.4%] | 1161 |
| topk_shrunk_win_rate | 25 | equal | 25 | 787 | 2.3% | +48.16% | -3.95% | [-11.3%, +3.5%] | 785 |
| topk_shrunk_win_rate | 25 | metric_proportional | 25 | 787 | 2.3% | +48.16% | -3.95% | [-11.3%, +3.5%] | 785 |
| topk_shrunk_win_rate | 25 | shrunk_metric_proportional | 25 | 787 | 2.3% | +48.16% | -3.95% | [-11.3%, +3.5%] | 785 |
| topk_raw_win_rate | 25 | equal | 25 | 770 | 2.3% | +47.40% | -4.13% | [-11.7%, +3.5%] | 769 |
| topk_raw_win_rate | 25 | metric_proportional | 25 | 770 | 2.3% | +47.40% | -4.13% | [-11.7%, +3.5%] | 769 |
| topk_raw_win_rate | 25 | shrunk_metric_proportional | 25 | 770 | 2.3% | +47.40% | -4.13% | [-11.7%, +3.5%] | 769 |
| topk_raw_win_rate | 50 | equal | 50 | 779 | 2.3% | +47.50% | -4.35% | [-11.9%, +3.1%] | 778 |
| topk_raw_win_rate | 50 | metric_proportional | 50 | 779 | 2.3% | +47.37% | -4.57% | [-12.0%, +2.8%] | 778 |
| topk_raw_win_rate | 50 | shrunk_metric_proportional | 50 | 779 | 2.3% | +47.37% | -4.57% | [-12.0%, +2.8%] | 778 |
| topk_shrunk_win_rate | 50 | equal | 50 | 1600 | 4.7% | +48.88% | -4.68% | [-9.7%, +0.2%] | 1596 |
| topk_shrunk_win_rate | 50 | metric_proportional | 50 | 1600 | 4.7% | +48.81% | -4.79% | [-9.8%, +0.0%] | 1596 |
| topk_shrunk_win_rate | 50 | shrunk_metric_proportional | 50 | 1600 | 4.7% | +48.81% | -4.79% | [-9.8%, +0.0%] | 1596 |
| topk_clv | 25 | metric_proportional | 25 | 235 | 0.7% | +42.98% | -6.52% | [-20.1%, +8.9%] | 234 |
| topk_clv | 25 | shrunk_metric_proportional | 25 | 235 | 0.7% | +42.98% | -6.52% | [-20.1%, +8.9%] | 234 |
| topk_settlement_roi | 50 | equal | 50 | 179 | 0.5% | +45.25% | -15.45% | [-29.1%, -1.8%] | 178 |
| topk_settlement_roi | 50 | metric_proportional | 50 | 179 | 0.5% | +45.25% | -15.45% | [-29.1%, -1.8%] | 178 |
| topk_settlement_roi | 50 | shrunk_metric_proportional | 50 | 179 | 0.5% | +45.25% | -15.45% | [-29.1%, -1.8%] | 178 |
| topk_excess_win_rate | 25 | equal | 25 | 132 | 0.4% | +40.15% | -18.52% | [-35.7%, -1.0%] | 131 |
| topk_excess_win_rate | 25 | metric_proportional | 25 | 132 | 0.4% | +40.15% | -18.52% | [-35.7%, -1.0%] | 131 |
| topk_excess_win_rate | 25 | shrunk_metric_proportional | 25 | 132 | 0.4% | +40.15% | -18.52% | [-35.7%, -1.0%] | 131 |
| l1_logistic | - | equal | 0 | 0 | 0.0% | n/a | n/a | [n/a] | 0 |
| l1_logistic | - | metric_proportional | 0 | 0 | 0.0% | n/a | n/a | [n/a] | 0 |
| l1_logistic | - | shrunk_metric_proportional | 0 | 0 | 0.0% | n/a | n/a | [n/a] | 0 |

## Rolling 2026 folds (pooled, 6wk train / 2wk validate)

**Full comparison, rolling folds pooled**

| selector | K | weighting | n_members | n_signals | coverage | accuracy | ROI (mean) | ROI 95% CI | n_priced |
|---|---:|---|---:|---:|---:|---:|---:|---|---:|
| topk_clv | 100 | equal | 100 | 3422 | 14.8% | +52.45% | +3.38% | [-0.1%, +6.9%] | 3414 |
| topk_clv | 100 | metric_proportional | 100 | 3422 | 14.8% | +52.31% | +3.09% | [-0.5%, +6.6%] | 3414 |
| topk_clv | 50 | equal | 50 | 2429 | 10.5% | +52.24% | +2.80% | [-1.5%, +7.0%] | 2421 |
| topk_clv | 100 | shrunk_metric_proportional | 100 | 3422 | 14.8% | +52.07% | +2.79% | [-0.8%, +6.4%] | 3414 |
| baseline_current_list | - | legacy_formula | 134 | 6825 | 29.4% | +56.62% | +2.70% | [+0.1%, +5.1%] | 6816 |
| topk_clv | 50 | metric_proportional | 50 | 2429 | 10.5% | +52.08% | +2.59% | [-1.7%, +6.7%] | 2421 |
| topk_clv | 50 | shrunk_metric_proportional | 50 | 2429 | 10.5% | +52.12% | +2.59% | [-1.7%, +6.9%] | 2421 |
| l1_logistic | - | equal | 532 | 7888 | 34.0% | +55.31% | +2.38% | [+0.0%, +4.8%] | 7878 |
| baseline_all_wallets_equal | - | equal | 16118 | 10706 | 46.2% | +66.15% | +2.18% | [+0.7%, +3.6%] | 10699 |
| topk_clv | 25 | equal | 25 | 1748 | 7.5% | +51.83% | +1.66% | [-3.4%, +6.7%] | 1740 |
| topk_clv | 25 | shrunk_metric_proportional | 25 | 1748 | 7.5% | +51.77% | +1.64% | [-3.5%, +6.6%] | 1740 |
| topk_clv | 25 | metric_proportional | 25 | 1748 | 7.5% | +51.72% | +1.52% | [-3.5%, +6.5%] | 1740 |
| l1_logistic | - | shrunk_metric_proportional | 532 | 7888 | 34.0% | +55.31% | +1.43% | [-1.0%, +3.7%] | 7878 |
| topk_excess_win_rate | 50 | equal | 50 | 2112 | 9.1% | +50.76% | +1.40% | [-3.1%, +5.9%] | 2105 |
| l1_logistic | - | metric_proportional | 532 | 7888 | 34.0% | +55.32% | +1.22% | [-1.2%, +3.5%] | 7878 |
| topk_raw_win_rate | 25 | equal | 25 | 2014 | 8.7% | +57.94% | +0.95% | [-3.3%, +5.1%] | 2011 |
| topk_raw_win_rate | 25 | shrunk_metric_proportional | 25 | 2014 | 8.7% | +57.89% | +0.87% | [-3.3%, +5.0%] | 2011 |
| baseline_market_favorite | - | n/a | 0 | 13581 | 58.6% | +66.89% | +0.83% | [-0.4%, +2.1%] | 13581 |
| topk_raw_win_rate | 25 | metric_proportional | 25 | 2014 | 8.7% | +57.85% | +0.75% | [-3.6%, +4.9%] | 2011 |
| greedy_marginal_add | 100 | metric_proportional | 26 | 1794 | 7.7% | +54.01% | +0.73% | [-3.9%, +5.5%] | 1791 |
| greedy_marginal_add | 100 | shrunk_metric_proportional | 26 | 1794 | 7.7% | +54.01% | +0.73% | [-3.9%, +5.5%] | 1791 |
| topk_settlement_roi | 200 | metric_proportional | 200 | 4832 | 20.8% | +50.31% | +0.73% | [-2.5%, +3.9%] | 4823 |
| greedy_marginal_add | 100 | equal | 26 | 1794 | 7.7% | +54.01% | +0.72% | [-3.9%, +5.4%] | 1791 |
| topk_excess_win_rate | 50 | shrunk_metric_proportional | 50 | 2112 | 9.1% | +50.47% | +0.71% | [-3.8%, +5.2%] | 2105 |
| topk_excess_win_rate | 50 | metric_proportional | 50 | 2112 | 9.1% | +50.38% | +0.68% | [-3.8%, +5.3%] | 2105 |
| topk_settlement_roi | 200 | shrunk_metric_proportional | 200 | 4832 | 20.8% | +50.27% | +0.65% | [-2.5%, +3.9%] | 4822 |
| topk_settlement_roi | 100 | shrunk_metric_proportional | 100 | 3724 | 16.1% | +50.27% | +0.62% | [-3.0%, +4.1%] | 3715 |
| topk_settlement_roi | 200 | equal | 200 | 4832 | 20.8% | +50.25% | +0.44% | [-2.8%, +3.7%] | 4823 |
| topk_settlement_roi | 50 | equal | 50 | 2231 | 9.6% | +47.96% | +0.34% | [-4.4%, +5.0%] | 2224 |
| topk_settlement_roi | 100 | metric_proportional | 100 | 3724 | 16.1% | +50.19% | +0.33% | [-3.5%, +4.0%] | 3716 |
| topk_settlement_roi | 50 | metric_proportional | 50 | 2231 | 9.6% | +47.92% | +0.24% | [-4.6%, +5.1%] | 2224 |
| topk_settlement_roi | 25 | equal | 25 | 1401 | 6.0% | +47.32% | +0.16% | [-6.2%, +6.3%] | 1396 |
| topk_excess_win_rate | 100 | shrunk_metric_proportional | 100 | 3510 | 15.1% | +51.34% | +0.13% | [-3.3%, +3.5%] | 3502 |
| topk_excess_win_rate | 200 | shrunk_metric_proportional | 200 | 4560 | 19.7% | +52.37% | +0.12% | [-3.0%, +3.0%] | 4551 |
| topk_excess_win_rate | 200 | metric_proportional | 200 | 4560 | 19.7% | +52.35% | +0.09% | [-2.9%, +3.0%] | 4552 |
| topk_settlement_roi | 100 | equal | 100 | 3724 | 16.1% | +49.97% | +0.01% | [-3.8%, +3.6%] | 3716 |
| topk_shrunk_win_rate | 200 | metric_proportional | 200 | 7753 | 33.4% | +56.52% | -0.01% | [-2.1%, +2.1%] | 7745 |
| topk_shrunk_win_rate | 200 | equal | 200 | 7753 | 33.4% | +56.53% | -0.04% | [-2.2%, +2.0%] | 7745 |
| topk_shrunk_win_rate | 200 | shrunk_metric_proportional | 200 | 7753 | 33.4% | +56.48% | -0.06% | [-2.2%, +2.0%] | 7745 |
| topk_excess_win_rate | 100 | metric_proportional | 100 | 3510 | 15.1% | +51.28% | -0.09% | [-3.4%, +3.7%] | 3503 |
| topk_settlement_roi | 25 | metric_proportional | 25 | 1401 | 6.0% | +47.18% | -0.09% | [-6.3%, +6.0%] | 1396 |
| topk_excess_win_rate | 100 | equal | 100 | 3510 | 15.1% | +51.25% | -0.22% | [-3.7%, +3.5%] | 3503 |
| topk_excess_win_rate | 200 | equal | 200 | 4560 | 19.7% | +52.24% | -0.27% | [-3.3%, +2.8%] | 4552 |
| topk_clv | 200 | shrunk_metric_proportional | 200 | 6588 | 28.4% | +51.05% | -0.49% | [-3.2%, +2.3%] | 6580 |
| topk_shrunk_win_rate | 25 | metric_proportional | 25 | 3874 | 16.7% | +54.93% | -0.53% | [-3.7%, +2.6%] | 3867 |
| topk_settlement_roi | 25 | shrunk_metric_proportional | 25 | 1401 | 6.0% | +46.97% | -0.54% | [-6.8%, +5.6%] | 1396 |
| topk_settlement_roi | 50 | shrunk_metric_proportional | 50 | 2231 | 9.6% | +47.47% | -0.67% | [-5.5%, +4.2%] | 2224 |
| topk_shrunk_win_rate | 25 | shrunk_metric_proportional | 25 | 3874 | 16.7% | +54.80% | -0.73% | [-4.0%, +2.3%] | 3867 |
| topk_raw_win_rate | 50 | equal | 50 | 2859 | 12.3% | +56.87% | -0.77% | [-4.2%, +2.7%] | 2854 |
| topk_shrunk_win_rate | 25 | equal | 25 | 3874 | 16.7% | +54.78% | -0.79% | [-4.0%, +2.3%] | 3867 |
| topk_raw_win_rate | 100 | metric_proportional | 100 | 4082 | 17.6% | +54.70% | -0.84% | [-3.8%, +2.1%] | 4075 |
| topk_raw_win_rate | 100 | shrunk_metric_proportional | 100 | 4082 | 17.6% | +54.68% | -0.88% | [-3.8%, +2.1%] | 4075 |
| topk_clv | 200 | metric_proportional | 200 | 6588 | 28.4% | +50.83% | -0.88% | [-3.6%, +1.9%] | 6580 |
| topk_raw_win_rate | 50 | metric_proportional | 50 | 2859 | 12.3% | +56.80% | -0.93% | [-4.3%, +2.6%] | 2854 |
| topk_raw_win_rate | 50 | shrunk_metric_proportional | 50 | 2859 | 12.3% | +56.70% | -1.11% | [-4.5%, +2.4%] | 2854 |
| topk_raw_win_rate | 100 | equal | 100 | 4082 | 17.6% | +54.51% | -1.18% | [-4.1%, +1.8%] | 4075 |
| topk_clv | 200 | equal | 200 | 6588 | 28.4% | +50.64% | -1.37% | [-4.1%, +1.4%] | 6580 |
| topk_excess_win_rate | 25 | metric_proportional | 25 | 1117 | 4.8% | +49.24% | -1.54% | [-7.5%, +4.8%] | 1113 |
| topk_shrunk_win_rate | 100 | shrunk_metric_proportional | 100 | 5216 | 22.5% | +54.74% | -1.63% | [-4.3%, +1.0%] | 5211 |
| topk_shrunk_win_rate | 100 | metric_proportional | 100 | 5216 | 22.5% | +54.75% | -1.65% | [-4.3%, +1.0%] | 5211 |
| topk_shrunk_win_rate | 100 | equal | 100 | 5216 | 22.5% | +54.74% | -1.66% | [-4.3%, +1.0%] | 5211 |
| topk_shrunk_win_rate | 50 | metric_proportional | 50 | 4529 | 19.5% | +54.12% | -1.74% | [-4.8%, +1.3%] | 4524 |
| topk_shrunk_win_rate | 50 | equal | 50 | 4529 | 19.5% | +54.03% | -1.88% | [-4.7%, +0.9%] | 4523 |
| topk_shrunk_win_rate | 50 | shrunk_metric_proportional | 50 | 4529 | 19.5% | +54.01% | -1.91% | [-5.0%, +1.1%] | 4524 |
| topk_excess_win_rate | 25 | shrunk_metric_proportional | 25 | 1117 | 4.8% | +49.06% | -1.98% | [-7.9%, +4.3%] | 1113 |
| topk_excess_win_rate | 25 | equal | 25 | 1117 | 4.8% | +48.97% | -2.08% | [-8.0%, +4.1%] | 1113 |
| topk_raw_win_rate | 200 | equal | 200 | 5202 | 22.4% | +54.69% | -2.76% | [-5.5%, -0.0%] | 5195 |
| topk_raw_win_rate | 200 | shrunk_metric_proportional | 200 | 5202 | 22.4% | +54.69% | -2.79% | [-5.5%, -0.1%] | 5195 |
| topk_raw_win_rate | 200 | metric_proportional | 200 | 5202 | 22.4% | +54.67% | -2.82% | [-5.6%, -0.1%] | 5195 |

## Favorite-bias diagnosis: raw win rate vs. excess win rate

H2 (`reports/05_skill_persistence.md`) found raw win rate is the best single next-window settlement-ROI predictor of the four candidates tested, but flagged it as likely partly a favorite-bias artifact: buying $0.90 favorites racks up wins with ~0 ROI. Excess win rate (dollar-weighted mean(1[won] − entry price)) is the price-adjusted version of the same idea. Same K and weighting scheme, both views:

| K | weighting | season: raw-WR ROI | season: excess-WR ROI | rolling: raw-WR ROI | rolling: excess-WR ROI |
|---:|---|---:|---:|---:|---:|
| 25 | equal | -4.13% | -18.52% | +0.95% | -2.08% |
| 25 | metric_proportional | -4.13% | -18.52% | +0.75% | -1.54% |
| 25 | shrunk_metric_proportional | -4.13% | -18.52% | +0.87% | -1.98% |
| 50 | equal | -4.35% | -0.97% | -0.77% | +1.40% |
| 50 | metric_proportional | -4.57% | -0.97% | -0.93% | +0.68% |
| 50 | shrunk_metric_proportional | -4.57% | -0.97% | -1.11% | +0.71% |
| 100 | equal | -3.80% | +0.01% | -1.18% | -0.22% |
| 100 | metric_proportional | -3.57% | +0.01% | -0.84% | -0.09% |
| 100 | shrunk_metric_proportional | -3.57% | -0.62% | -0.88% | +0.13% |
| 200 | equal | -0.84% | +5.94% | -2.76% | -0.27% |
| 200 | metric_proportional | -0.66% | +5.41% | -2.82% | +0.09% |
| 200 | shrunk_metric_proportional | -0.66% | +5.42% | -2.79% | +0.12% |

## ROI by market type (closest contender vs. baselines)

**Season fold** (cell = mean ROI (n))

| portfolio | moneyline | nrfi | prop | spread | total |
|---|---:|---:|---:|---:|---:|
| topk_clv K=100 equal | +2.67% (1074) | +10.99% (198) | +22.34% (3) | +2.36% (1653) | +3.26% (1824) |
| baseline_current_list | +3.65% (1272) | +15.79% (156) | -100.00% (2) | +2.57% (2944) | +1.38% (3742) |
| baseline_market_favorite | +0.41% (1457) | -0.22% (1135) | -4.49% (419) | -1.76% (5468) | +1.21% (9339) |
| baseline_all_wallets_equal | -0.26% (1362) | -1.40% (1198) | -1.56% (141) | -0.71% (4453) | +5.07% (6585) |

**Rolling folds pooled** (cell = mean ROI (n))

| portfolio | moneyline | nrfi | prop | spread | total |
|---|---:|---:|---:|---:|---:|
| topk_clv K=100 equal | -1.60% (915) | -0.68% (575) | +150.00% (1) | +6.66% (694) | +7.02% (1229) |
| baseline_current_list | +2.72% (912) | +14.59% (133) | -100.00% (1) | +2.52% (2565) | +2.38% (3205) |
| baseline_market_favorite | +0.96% (1019) | -1.65% (872) | -1.42% (197) | -1.62% (4637) | +2.85% (6856) |
| baseline_all_wallets_equal | +1.91% (957) | -2.37% (919) | +19.25% (34) | -0.90% (3777) | +5.26% (5012) |

## Conclusion: does any portfolio beat market-favorite (or the other baselines)?

**No. No selector+weighting combination beats every baseline (market-favorite, all-wallets-equal, current-134-list) simultaneously on both the season fold and the pooled rolling folds with a ROI 95% CI that excludes zero.** Per-protocol detail:

Candidates that beat all three baselines **on the season fold only** (CI excludes zero there, but failed the rolling-folds test):

- `topk_excess_win_rate` K=200 equal: ROI +5.94% [+0.1%, +11.8%], n_priced=1202, coverage 3.5% — but rolling: -0.27% [-3.3%, +2.8%]
- `topk_clv` K=100 shrunk_metric_proportional: ROI +3.21% [+0.1%, +6.5%], n_priced=4752, coverage 13.9% — but rolling: +2.79% [-0.8%, +6.4%]
- `topk_clv` K=100 equal: ROI +3.15% [+0.0%, +6.4%], n_priced=4752, coverage 13.9% — but rolling: +3.38% [-0.1%, +6.9%]

No candidate beat all three baselines on the pooled rolling folds.

Most consistent cross-protocol contender (maximizes the worse of its two view ROIs): `topk_clv` K=100 equal: ROI +3.15% [+0.0%, +6.4%], n_priced=4752, coverage 13.9% (season); rolling pooled +3.38% [-0.1%, +6.9%], n_priced=3414. Its point estimates beat market-favorite in both views, but its rolling-fold CI does not exclude zero, so it does not clear the pre-registered bar.

This is a valid, decisive research result, not a null result to paper over: it is consistent with H1 (past ROI barely predicts), H2's own caveat (win rate is the *least-bad of four weak signals*, not a strong one), and H7 (skill is real at the season level but diffuse -- 97.5% top-decile turnover). Wallet-selection-based portfolios, even with shrinkage/ensembling per H7's recommendation, are not demonstrably beating simple price-based or crowd-based baselines on this dataset.

## Observations and caveats

- **The unselected crowd itself is a strong baseline.** All-wallets-equal scores +2.04% [+0.7%, +3.4%] on the season fold and +2.18% [+0.7%, +3.6%] on the rolling folds — both CIs exclude zero, and both beat market-favorite. Its by-market-type split shows the edge is concentrated in **totals** markets (see table above), i.e. the aggregate pre-game money flow direction on totals carries information relative to the closing candle price. Caveat before celebrating: entry is modeled at the last pre-game candle close with zero slippage/spread; a thin totals book could absorb much of a ~2-5% edge. This is exactly the liquidity/orderbook question Phase 3 owns.
- **L1-logistic selected zero wallets on the season fold** (inner-val accuracy preferred the all-zero model at C=0.01 over any sparse nonzero fit — 2025 stakes do not linearly generalize to 2026 outcomes). On the shorter rolling folds it kept ~500 wallets and landed near the crowd baseline, i.e. it converges toward 'everyone' rather than finding a special subset — same diffuse-skill story as H7.
- **CLV top-K is non-monotonic in K** (K=25 negative, K=50/100 positive, K=200 fading toward the crowd) and its K=50/100 CIs straddle or barely clear zero in one view only. Treat the CLV-selector rows as suggestive, not established — with ~66 specs compared, one or two borderline-significant CIs are expected under the null (multiple-comparison caution).
- **Raw win rate is confirmed favorite-biased as a selection metric** (see diagnosis table): its top-K portfolios pick $0.80-0.95 favorite buyers whose settlement ROI is flat-to-negative, and excess win rate reverses much of that at K≥50. H2's ranking (raw win rate best-of-four at predicting *next-window wallet ROI*) does not transfer to portfolio construction, where the price paid is what matters.

## Files written

- `data/lake/pregame_fills.parquet` — intermediate cache (pre-game fills, all seasons)
- `data/lake/portfolio_selection_raw.json` — every portfolio-fold result (scalars only)
- `data/trader_portfolio.json` — winning spec (or the honest non-beat verdict) + deployable members/weights retrained on all available data
- `reports/06_portfolio.md` — this report
