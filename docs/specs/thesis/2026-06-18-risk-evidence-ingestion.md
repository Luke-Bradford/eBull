# #1632 — thesis engine ingests instrument_risk_metrics as structured risk evidence

Status: spec. Follows #591 (PR-B shipped the `instrument_risk_metrics` two-layer table + `risk_v1`). Read-side only.

## Goal

Feed the persisted, versioned, quality-flagged risk scalars into the thesis
writer + critic LLM context so a long thesis is grounded in realized risk
(drawdown / vol / beta / Calmar / tail) and the critic's falsification kit can
attack it on downside-risk grounds. **No schema / migration / job change** —
`_assemble_context` gains one more read block, exactly like fundamentals /
filings / news.

## Source rule (the contract this must honour)

From the #591 design (`docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md`)
+ data-engineer honest-status discipline + prevention-log "degrade honestly,
never fabricate":

- The risk row is the **auditable evidence**: a thesis citing "beta 1.3"
  resolves to `{value, window_key, as_of_date, metric_version, status}`. So the
  context block MUST carry `metric_version` + `as_of_date` (+ benchmark symbol
  for beta/excess) alongside the scalars, and the prompts MUST instruct the LLM
  to cite them.
- **Honest status passthrough.** Every scalar already carries a per-metric
  status (`ok | insufficient_history | partial_window | benchmark_missing |
  benchmark_insufficient_history | invalid_price_chain | stale`). The block
  passes the status through verbatim next to each metric family; the prompts
  instruct the LLM to NOT treat a flagged (non-`ok`) metric as a precise number
  — a thin-history annualized figure is provisional, a `benchmark_missing` beta
  is absent, not zero. NULLs are never coerced to 0 (mirrors `_to_float`, which
  returns None).
- **Two distinct empty states (Codex ckpt-1 HIGH).** A *thin-history* instrument
  still has `_current` rows — null scalars + flagged statuses
  (`insufficient_history` / `partial_window`); those pass through verbatim
  (status carries the signal). Only a *never-computed* instrument (no rows at
  all — not yet in a refresh scope) yields `risk_metrics = None`, exactly like
  `analyst_estimates`. Tests assert BOTH: flagged-row passthrough AND
  None-on-no-rows.
- **Units / signs (Codex ckpt-1 MED).** All returns/ratios are FRACTIONS
  (0.10 = 10%, not percent); drawdown / `var_5` / `worst_day` are SIGNED losses
  (negative); basis is **price-return** (TR deferred #1635). The block carries a
  `note` stating this and the prompts repeat it so the LLM cannot misread
  `-0.52` as "-0.52%" or as a gain.
- Reproducibility: `risk_v1` rows are append-only + version-bumped, so a stored
  thesis's risk citation stays reproducible. We read `_current` filtered to
  `RISK_METRICS_VERSION` (imported, no magic string). A risk citation must name
  `{window_key, as_of_date, metric_version}` — the prompts require all three
  (Codex ckpt-1 HIGH: version cannot be dropped, else the append-only series
  can't resolve the cited row).

## Changes

### `app/services/thesis.py`

1. Import `RISK_METRICS_VERSION` from `app.services.risk_metrics`.
2. `_assemble_context`: add a `risk_metrics` block — **ONE** SELECT (no second
   statement — Codex ckpt-1 MED snapshot concern) from
   `instrument_risk_metrics_current c LEFT JOIN instruments b ON b.instrument_id
   = c.benchmark_instrument_id`, WHERE `c.instrument_id` AND
   `c.metric_version = RISK_METRICS_VERSION`, ordered by window order
   (`WINDOW_KEYS` from `risk_metrics.py` via a CASE / `array_position`, NOT the
   api-layer `_RISK_WINDOW_ORDER`). Per-window subset:
   `window_key, as_of_date` (**per window** — Codex MED, no constraint enforces
   sameness), `benchmark_symbol` (= `b.symbol`), `cagr, excess_cagr_vs_spy,
   vol_annualized, beta, beta_r2, calmar, max_drawdown, current_drawdown, var_5,
   worst_day` + per-family statuses (`cagr_status, excess_cagr_status,
   vol_status, beta_status, drawdown_status, distribution_status,
   calmar_status`). Decimals → `_to_float`; dates → `str`.
   Block shape: `{"metric_version": RISK_METRICS_VERSION, "basis_note":
   "<fractions not percent; losses negative; price-return basis, not
   total-return; non-ok status = do not treat as precise>", "windows": [...]}`.
   `metric_version` is top-level (filtered, so shared by construction);
   `as_of_date` rides each window. Returns `None` when **no rows** (never
   computed — like `analyst_estimates`); thin-history instruments DO have rows
   (flagged statuses) and pass through.
3. Add `"risk_metrics": risk_metrics` to the returned context dict.

### Prompts

4. `_WRITER_SYSTEM`: add "recent realized-risk metrics (windowed, versioned,
   quality-flagged; fractions not percent; losses negative; price-return basis)"
   to the input list + a rule: use risk to support/contradict the long (deep max
   drawdown / high beta / low Calmar / fat-tail var_5 = downside context);
   respect status flags (do not cite a non-`ok` metric as precise;
   `benchmark_missing` beta is absent, not 0); a CAGR is price-return (no
   dividends) so don't over-read it for high-yield names; **when citing a risk
   figure name all of `{window, as_of_date, metric_version}`** (Codex ckpt-1
   HIGH — version is load-bearing for reproducibility).
5. `_CRITIC_SYSTEM`: add risk metrics to the falsification kit — the critic
   should raise realized-risk objections the memo ignored (e.g. "thesis ignores
   a 52% peak-to-trough drawdown and beta 1.8 — the downside is larger than the
   memo admits"), respecting the same status-flag + sign + version-citation
   honesty.

## Tests (`tests/test_thesis.py`)

- `_assemble_context` includes a `risk_metrics` block with `metric_version` +
  `as_of_date` + windows when `_current` rows exist (db-tier or mocked cursor).
- Returns `None`/absent when no rows (thin-history instrument) — no fabricated
  zeros.
- Honest passthrough: a `partial_window` / `benchmark_missing` row carries its
  status verbatim into the context.

## Dev-verify

Run `_assemble_context` (or `generate_thesis`) on AAPL on dev; confirm the
`risk_metrics` block renders with `risk_v1`, an `as_of_date`, three windows, and
beta_status etc. Record the figure + SHA in the PR.

## Out of scope

No schema/migration/job. No new risk math. No scoring change (#1633 gated). No
total-return (#1635 deferred). Critic storage unchanged (`critic_json`, per
settled-decisions).
