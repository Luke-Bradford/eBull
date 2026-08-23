# R6 point-in-time spine correction 2: do not overstate failed conditions (#2900)

Status: **FROZEN BEFORE THE FINAL VERDICT RUN**.

The first valid verifier run confirmed the decisive system-version overwrite,
but its matrix overstated three independent conditions. A family is refused
when any required condition fails; it is neither necessary nor accurate to
label every condition failed.

This correction supersedes these declaration matrix cells:

- `research_prices/public_clock` becomes `PASS[P0]`: a completed prior-session
  `bar_date < D` has a causal daily public clock. The family remains refused by
  missing source vintages, future-adjusted values, non-causal quarantine and
  non-PIT population.
- `derived_fundamentals/public_clock` becomes `PASS[D0]`: derived period rows
  carry `filed_date`. They remain refused because the tables are rebuilt and
  select today's normalization/restatement winner.
- `ownership_observations/public_clock` becomes `PASS[O0]` for filing-based
  families such as 13F, qualified that DEF 14A still fails `O2`. All ownership
  record writers remain refused because same-key rewashes discard prior
  payload versions; the arm cannot reconstruct an old research state.
- `historical_population/public_clock` becomes `PASS[H2]`, prospectively only.
  The append-only membership record has effective dates, but begins in 2026
  with unknown imported starts and cannot populate the archive window.

New probes `P0`, `D0` and `O0` bind those positive claims. No registry status,
decision date, mutation, comparison, threshold or overall verdict rule changes.
The earlier valid run is diagnostic only; the final result must be regenerated
after this correction and its implementation are committed cleanly.
