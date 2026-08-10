# Point-in-time price-shock mechanism classifier

Date: 2026-08-10  
Issue: #2507  
Classifier: emitted by `strategy_shock_mechanism.CLASSIFIER_VERSION`

## Purpose

A large candle is an observation, not a strategy. This outcome-blind router is
evaluated before continuation or reversal candidates and emits exactly one:

1. `known_fundamental_catalyst`;
2. `known_market_or_sector_move`;
3. `no_known_catalyst_liquidity_candidate`;
4. `unknown`.

The classifier predicts neither direction nor profit. Its canonical definition
is JSON-serialised and SHA-256 versioned before any candidate outcome is read.

## Frozen precedence and mathematics

1. A material 10-K/10-Q or material/critical structured 8-K known inside the
   declared event window routes to the fundamental class.
2. Otherwise, use a market/sector model fitted only through a timestamp before
   the decision. Let

   ```text
   residual       = raw return - prior-fitted expected market/sector return
   residual_z     = residual / prior residual volatility
   ```

   An absolute residual z-score below 2 is consistent with the prior-fitted
   market/sector explanation. Two is a preregistered routing boundary, not a
   claim of profitable significance and not a parameter to tune on outcomes.
3. A residual shock can be labelled `no_known_catalyst` only when SEC filings,
   issuer releases and market news all explicitly cover the full event window.
4. The liquidity candidate also requires decision-time as-traded price,
   trailing median dollar volume, relative volume, realised volatility,
   spread, completed intraday confirmation and a fresh non-active halt state.
   Missing or future inputs produce named `unknown` refusals.

The liquidity label means “eligible for a separately preregistered research
test,” never “buy,” “sell” or “pending.”

## SEC knowledge time

Exact `sec_filing_manifest.accepted_at` is used when present. When only a filed
date exists, the event becomes known no earlier than 09:30 New York time on the
following regular session. Weekends, NYSE holidays and early-close calendars
are respected. A UTC-midnight `filed_at` retains its SEC civil date; converting
that placeholder instant to New York would incorrectly move it to the prior
date.

10-K/10-Q filings are material event identities. An 8-K is included only when
its parsed item code joins to a `material` or `critical` code. An unparsed or
informational-only 8-K cannot silently become a material catalyst.

## Measured current coverage

Dev measurements on 2026-08-10 show why a fail-closed class is necessary:

```text
issuer 10-K/10-Q/8-K manifest rows since 2024: 168,624
with exact accepted_at:                           8,452  (~5.0%)

non-tombstoned structured 8-K filings:           65,792
with at least one parsed item:                    15,919 (~24.2%)
material/critical item rows:                      12,056

news_events:                                      45,208
source: Yahoo Finance only
window: 2026-06-21 through 2026-08-10
```

Consequences:

- date-only SEC history is usable only under the conservative next-session
  rule;
- absence of a parsed 8-K item is not evidence of no material event;
- the current Yahoo window is neither historical nor completeness-certified;
- therefore the application cannot currently claim broad “no catalyst” event
  coverage. Residual moves requiring that claim must remain `unknown` until a
  defensible prospective coverage contract exists.

## Storage boundary

This change adds no rolling feature table and stores no every-instrument
heartbeat. A future fired/refused decision snapshot may retain the classifier
version, mechanism, reason, causal catalyst identities, residual z-score and
missing-input codes. Model histories and repeated no-op classifications do not
belong in the database.

## What this does not prove

The router does not rehabilitate the rejected broad residual-reversal result.
Each routed family still needs a separately frozen entry, exit, cost and
walk-forward contract. Winner/loser differences found later are diagnostics;
they cannot mutate this classifier or validate a new subset on the same
outcomes.
