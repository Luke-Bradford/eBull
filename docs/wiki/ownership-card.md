# Ownership card

The ownership rollup endpoint shows "who owns what" for a given
instrument: institutional / insider / blockholder / fund / treasury /
ESOP slices, plus a memo overlay for short interest (planned #915).

## Endpoint

```
GET /instruments/{symbol}/ownership-rollup
```

No auth required. Returns a `slices[]` array with one entry per
ownership category, plus a `coverage[]` array showing per-category
freshness.

## Slices

Each slice carries `{category, total_shares, pct_of_outstanding, filer_count, source, last_obs_at, period_end}`.

| Category | Source | Cadence | Filing form | Notes |
|---|---|---|---|---|
| `institutions` | SEC 13F-HR | T+45 quarterly | 13F-HR / 13F-HR/A | All institutional managers ≥$100M AUM. ETFs filtered to `etfs` slice via filer-type classifier. |
| `etfs` | SEC 13F-HR | T+45 quarterly | 13F-HR | Subset of `institutions` where `filer_type = 'ETF'`. |
| `insiders` | SEC Form 4 | T+2 daily | 3 / 4 / 5 / 4/A | Beneficial owners > 10%, officers, directors. Two-axis: direct + indirect. |
| `blockholders` | SEC 13D / 13G | T+10 / T+45 | 13D / 13G / 13D/A / 13G/A | Activist (13D) or passive (13G) ≥5% holders. |
| `funds` | SEC NPORT-P / N-CSR | 60-day lag / semi-annual | NPORT-P / N-CSR | Mutual fund + ETF holdings below 13F threshold. **N-CSR audited beats NPORT-P unaudited within same period_end.** |
| `treasury_def14a` | SEC DEF 14A | Annual | DEF 14A | Treasury shares + officer/director compensation grants. |

Coverage gates:
- A slice is shown only if its source has at least one observation
  in the last freshness window.
- `coverage[].state` reports per-category state: `fresh` /
  `stale` / `missing`.

## What "current" means here

Each slice reads from `ownership_<category>_current` — the
materialised "what's true now" view. Observations land first in
`ownership_<category>_observations` (append-only event log).

The refresh writer (`refresh_<category>_current`) applies:
- **Source priority** — N-CSR audited beats NPORT-P unaudited.
- **filed_at tie-break** — same-period amendments use the most
  recent `filed_at`.

## Read-only invariants

- The rollup query NEVER reads from the legacy `institutional_holdings`
  or `insider_transactions` tables. Those readers were removed in
  #905 (cutover commit). All reads are via the `*_current` tables.
- Short interest is never rendered as a pie wedge — memo overlay
  only (per #915 acceptance criterion 4).

## Coverage banner states

The frontend shows a coverage banner per category. State machine
documented in spec
`docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md`
(Phase 2 — #923 — pending).

## When numbers move

After a parser change, run the operator runbook at
[`runbooks/runbook-after-parser-change.md`](runbooks/runbook-after-parser-change.md).
This is non-negotiable per CLAUDE.md DoD clauses 8-12.
