# #1636 — position-vs-portfolio risk (marginal risk contribution)

Status: spec. Follows #591 (PM committee). The risk layer is single-instrument
standalone (β vs SPY). This adds the **true sizing metric**: how much a candidate
moves the *existing book's* risk — covariance vs current holdings. **On-read, not
persisted** (the book is dynamic — current weights change with every trade; a
versioned evidence row like `instrument_risk_metrics` would be stale on the next
fill). Reuses the `risk_metrics.py` primitives (`simple_returns`, `ols_beta`,
`annualized_vol`); no new estimator math.

## Source rule (portfolio theory — cited, not first-principles)

Markowitz portfolio risk. For a book with weights `w` and return covariance `Σ`,
portfolio variance is `σ_p² = wᵀΣw`. The contribution of an asset is governed by
its covariance with the *portfolio*:

- **Portfolio beta** of a candidate `c`: `β_{c,p} = cov(r_c, r_p) / var(r_p)` —
  how the candidate co-moves with the book. `β > 1` amplifies book risk, `β < 1`
  dampens, `β < 0` hedges. (This is exactly `ols_beta(r_c, r_p)` with the
  portfolio series as the regressor — same closed form already shipped.)
- **Correlation** `ρ_{c,p}` (= `sign(β)·√r²` from the same fit) — the
  diversification signal independent of magnitude: low/negative ρ diversifies.
- **Marginal contribution to risk** (MCR) of adding `c` at the margin:
  `MCR_c = β_{c,p} · σ_p = cov(r_c, r_p)/σ_p` (annualized). This is the standard
  Euler/risk-decomposition marginal term: to first order, adding a small weight
  `δw` of `c` (funded pro-rata from the book) changes portfolio vol by
  `δσ_p ≈ δw · (MCR_c − σ_p)`. So `MCR_c < σ_p` (equivalently `β_{c,p} < 1`) means
  the candidate is **risk-reducing per unit weight** at the current book; `> σ_p`
  means risk-adding. (Refs: Markowitz 1952; standard MCR/CCR decomposition,
  `∂σ_p/∂w_i = (Σw)_i/σ_p`.)

This is the candidate-vs-book metric. (Component contribution to risk of each
*held* position — `CCR_i = w_i·β_{i,p}·σ_p`, `Σ CCR = σ_p` — is a natural
extension but out of scope here; the issue asks for the candidate sizing metric.)

## Weighting convention (documented)

**Current market-value weights applied to historical returns.** `w_i = mv_i / Σ
mv` where `mv_i` is the mark-to-market value from
`portfolio.py::_load_positions` (the canonical portfolio-page source: quote
`last>0` → `cost_basis` fallback, #1428) — chosen explicitly so weights match
the portfolio UI/AUM (Codex ckpt-1: do NOT mix in `valuation.py`'s
quote→close→cost ladder). **This is a current-exposure covariance estimate, NOT
the book's realized historical return** — today's weights over past returns. The
docstring + endpoint + UI copy must say so; nobody should read `r_p` as a
backtest of the actual book. We do not reconstruct time-varying historical
weights (not cleanly available; the operator's decision is about *today's*
sizing). MCR's `δσ_p ≈ δw·(MCR−σ_p)` assumes the add is funded **pro-rata from
the existing book** (normalized weights `w' = (1−δw)w + δw·e_c`), not cash-funded
expansion — stated in the endpoint/UI.

## Compute (`app/services/portfolio_risk.py`)

`compute_portfolio_relative_risk(conn, candidate_instrument_id) -> PortfolioRelativeRisk | None`:

1. Load open positions + mark-to-market weights (`current_units > 0`).
2. For each holding, `load_close_series` → `simple_returns` → `{date: ret}`.
3. **Portfolio return series** `r_p`: over the dates where **all** holdings have a
   return (the common window — so the weighted sum always spans the same asset
   set), `r_p[t] = Σ_i w_i · r_i[t]`. (Decimal throughout, matching risk_metrics.)
4. Candidate: `load_close_series` → `simple_returns`.
5. `ols_beta(candidate_returns, r_p)` → `β_{c,p}`, `r²`, `n_obs` (aligned
   intersection of candidate ∩ portfolio dates — the helper already does this).
   `correlation = sign(β)·√r²`. `σ_c`, `σ_p = annualized_vol(...)` on the aligned
   window. `MCR = β·σ_p` (None when β or σ_p is None).
6. `already_held` + `current_weight` when the candidate is in the book.

## Quality flags — own `PortfolioRiskStatus` Literal (Codex ckpt-1)

NOT `RiskStatus` (closed; lacks these). A dedicated
`PortfolioRiskStatus = Literal["ok", "empty_book", "book_history_unavailable",
"insufficient_history", "single_holding_is_candidate"]`:

- `empty_book` → no open positions (metric undefined; nulls, `holdings_count=0`).
- `book_history_unavailable` → the portfolio return series is empty: no holding
  has a usable price series, or the holdings' histories don't overlap into a
  common window (distinct from the *candidate's* `insufficient_history` — the
  book itself couldn't be constructed; Codex ckpt-1).
- `single_holding_is_candidate` → the only holding IS the candidate (β to itself
  is trivially 1).
- `insufficient_history` → fewer than `MIN_RETURNS_VOL_BETA = 60` aligned
  candidate∩portfolio obs.
- `ok` otherwise. NULLs never coerced to 0; `correlation`/`portfolio_beta`/`MCR`
  are `None` (not 0) when β / r² / var(r_p) is null (#1581 honest-status).

The service **always returns a payload** (status carries the degraded cases);
`None` is not part of its contract — the endpoint owns the 404 for an unknown
symbol (Codex ckpt-1).

## Window consistency (Codex ckpt-1, load-bearing)

`σ_p`, `σ_c`, and `β` are all computed over the **same** candidate∩portfolio
date intersection — `σ_p` is NOT the vol of the full `r_p` series. Otherwise
`MCR = β·σ_p` mixes two windows and is inconsistent. A test pins this.

## Endpoint (`app/api/instruments.py` or `portfolio.py`)

`GET /instruments/{symbol}/portfolio-risk` → `PortfolioRelativeRisk`:
`{symbol, as_of_date, status, holdings_count, already_held, current_weight,
portfolio_beta, correlation, candidate_vol, portfolio_vol,
marginal_risk_contribution, n_obs}`. On-read (no persistence). `snapshot_read`
so positions + all price series come from one committed view. 404 unknown
symbol; 200 with `status="empty_book"` when the book is empty.

## Frontend (thin card on the risk page)

A "vs your portfolio" card on `RiskPage` (2nd `useAsync` to `/portfolio-risk`):
plain-language verdict — `portfolio_beta < 1` ⇒ "dampens your book's swings",
`> 1` ⇒ "amplifies", `< 0` ⇒ "hedges"; correlation as a diversification line;
MCR vs portfolio_vol. Per-card empty/flag state keyed on `status` (`empty_book` →
"You hold no positions yet"). Avoids shipping a dead endpoint. types.ts mirror +
fetcher.

## Tests

- `portfolio_risk.py` pure-ish: portfolio-return weighting (weighted sum on the
  common date window), β/corr/MCR wiring vs a constructed 2-3 holding book,
  `empty_book` / `single_holding_is_candidate` / `insufficient_history` flags,
  NULL passthrough. Extract the weighting + assembly into pure helpers fed
  synthetic series (no DB), per the repo's pure-test preference.
- Endpoint: one integration test (book with holdings → sane payload; empty book →
  `empty_book` not zeros).
- `RiskPage.test.tsx`: renders the card; empty-book state; verdict mapping.

## Dev-verify

Dev book = 5 holdings (GME 48.6% / QQQ 18.8% / VOO 17.4% / BBBY 11.6% / IEP 3.5%).
Run `compute_portfolio_relative_risk` for a candidate (e.g. AAPL, and a held one
GME) on dev; confirm `portfolio_beta` / correlation / MCR are sane (GME β≈1+ as
the dominant holding; a diversifier < 1). Hit `/instruments/AAPL/portfolio-risk`.
Record figures + SHA.

## Out of scope (note)

- Component contribution to risk (CCR) per held position + a book-risk
  decomposition view (extension).
- Wiring MCR into the portfolio-manager sizing / scoring path (a model change —
  same gate class as #1633).
