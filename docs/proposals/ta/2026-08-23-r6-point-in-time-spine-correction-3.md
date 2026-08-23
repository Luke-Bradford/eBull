# R6 point-in-time spine correction 3: bind probe semantics (#2900)

Status: **FROZEN BEFORE THE NEXT VERDICT RUN**.

The latest independent review identified two evidence-attribution defects. The
verdict remains unchanged, but the final run must not proceed with either:

- `derived_fundamentals/historical_population` and
  `dimensional_xbrl/historical_population` must cite `P2,P5`, as frozen in the
  declaration. `D1` proves destructive derived-table rebuilds and `X1` proves
  missing XBRL dimension identity; neither examines the historical population.
- Source anchors must be checked in parsed executable Python or uncommented
  DDL, not raw file substrings. Python modules must resolve and parse; comments,
  docstrings and statically dead `if False` bodies cannot satisfy a probe.
  Positive and negative schema claims must also bind to the live schema.

No registry status, decision date, mutation, comparison, threshold or overall
verdict rule changes. The previous runs are diagnostic only; the next result
must be generated after this correction and implementation are committed
cleanly.
