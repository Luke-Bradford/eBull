# OpenFIGI sweep — negative-status writeback + drain + cadence (#740)

Status: draft 2026-06-11 · Owner: S6 coverage track · Issue: #740 (acceptance), builds on #1233 PR-1b, #1349 grain.

## 1. Problem (measured on dev, 2026-06-10/11)

CUSIP coverage is stuck at **2,949 / 5,109 = 58%** of CIK-mapped tradable
instruments (acceptance in #740: ≥80%). Spot-check failures: ADBE, ABNB,
ACN, BRK.B have no `external_identifiers` cusip row → their 13F/N-PORT
observations were never materialised → **2,393 CIK-mapped tradables show
zero institutional ownership** on the chart.

Root cause is a cursor that never advances in
`sweep_unresolved_cusips_via_openfigi` (`app/services/cusip_resolver.py:1410`):

- `_select_unresolved_bulk_cusips` is `ORDER BY u.cusip LIMIT 1000`
  (cusip_resolver.py:1238-1249).
- Per-cusip negative outcomes — "OpenFIGI has no US-common-stock mapping"
  and "ticker returned but no unique `instruments.symbol` match" — leave
  `resolution_status IS NULL`.
- So every run re-selects the SAME alphabet-head 1000. Measured run
  2026-06-10 15:37 UTC: candidates 1000, OpenFIGI mappings 25, promotions
  **2**, batch never passed cusips starting `005…`. Backlog: **54,685**
  distinct sweep-eligible CUSIPs.
- No steady-state cadence: the job runs as bootstrap stage S13 + manual
  trigger only.

Second, share-class tickers can never match: OpenFIGI returns the
slash convention — probed 2026-06-11 keyed: CUSIP `084670702` →
ticker **`BRK/B`** ("BERKSHIRE HATHAWAY INC-CL B") — while
`instruments.symbol` is `BRK.B`. `_find_instrument_by_ticker`
(cusip_resolver.py:1256) is exact-match → permanent miss for every
share-class security.

## 2. Decisions already taken (operator, 2026-06-11)

- **No auto-retry of negative statuses in v1.** Statuses are terminal;
  escape hatch = manual SQL reset (runbook §7). Backlog is ~95%
  OTC/foreign securities that will never enter the eToro universe.
- Settled decisions preserved: #532/OpenFIGI approval (CUSIP→ticker
  direction only, `openfigi` Lane, keyed via `OPENFIGI_API_KEY`),
  #1102 (`is_primary=FALSE` non-CIK extid writes with ON CONFLICT
  predicate), #819 (variants resolve by exact symbol → no collision).

## 3. Schema — sql/192

Widen the `resolution_status` CHECK (DROP/ADD pattern of sql/112 +
sql/168; table is ~62k rows, instant):

- `'openfigi_unknown'` — resolver call succeeded; OpenFIGI returned no
  US-primary common-stock mapping for this CUSIP.
- `'openfigi_no_instrument'` — mapping returned; normalised ticker has
  no unique `is_tradable` `instruments.symbol` match (not in universe,
  or ambiguous).

Existing values (`unresolvable`, `ambiguous`, `conflict`,
`manual_review`, `resolved_via_extid`, `resolved_via_openfigi`) keep
their semantics — the legacy fuzzy-name statuses are NOT reused, so the
route that disposed a row stays auditable.

## 4. Sweep changes (`app/services/cusip_resolver.py`)

1. **Ticker normalisation before symbol match**: `BRK/B` → `BRK.B`
   (replace `/` with `.` ONLY — space normalisation deliberately
   omitted until empirically needed; Codex ckpt-1 #2). Applied only to
   the OpenFIGI-returned ticker; `instruments.symbol` untouched. The
   resolver's existing `exchCode='US' AND securityType='Common Stock'`
   filter (openfigi_resolver.py:239) keeps preferreds/units/warrants
   out of this path; unit tests pin that `ABC WS`, `ABC/U`, `BRK/PB`
   style tickers do NOT promote (no-match → `openfigi_no_instrument`),
   `BRK/B→BRK.B` matches, plain `ADBE` passes through.
2. **Negative writeback** in the per-cusip loop:
   - mapping is None AND the batch's resolver call succeeded →
     tombstone `'openfigi_unknown'`.
   - mapping present, `_find_instrument_by_ticker` returns None →
     tombstone `'openfigi_no_instrument'`.
   - Whole-batch `api_errors` (resolver raised) → rows stay NULL
     (transient; retried next pass). This is the existing behaviour,
     now load-bearing: NULL = "not yet decided", never "decided no".
3. **Drain loop**: `sweep_unresolved_cusips_via_openfigi` gains
   `max_passes: int = 1`. Each pass selects the next `limit` candidates
   (cursor advances by construction now that every candidate gets a
   terminal status or promotes). Loop exits early on empty selection or
   any `api_errors` (no point hammering a failing API). The S13/scheduled
   invoker passes a tier-derived budget (corrected per Codex ckpt-1 #1:
   keyed = 100 jobs/POST → a 1000-cusip pass is 10 POSTs): keyed →
   `max_passes=60` = ≤600 POSTs ≈ 2.5 min at 25 req/6s (covers the
   54,685 backlog in one run); unkeyed → `max_passes=3` = 300 POSTs ≈
   12 min at 25 req/min, on the dedicated `openfigi` lane (cap=1, no
   other consumer to starve), weekly cadence drains the rest over time.
4. **Extid sweep widening**: `sweep_bulk_cusips_resolved_via_extid`
   predicate `resolution_status IS NULL` → also covers the two new
   negative statuses. Scope honesty (Codex ckpt-1 #3): "mapped by any
   route" means any route that writes `external_identifiers` with
   `provider IN ('sec', 'openfigi')` — the SEC-list backfill, the fuzzy
   resolver, and manual runbook upserts all use `provider='sec'`; that
   invariant is what the sweep predicate (cusip_resolver.py:521-526)
   and `load_bulk_cusip_map` already assume, and it stays unchanged.
5. **Promotion tombstone widening** (Codex ckpt-1 #4):
   `_tombstone_bulk_rows_for_cusip` currently updates only
   `resolution_status IS NULL` rows; when promoting it must also flip
   rows already carrying one of the two new negative statuses (the
   other `(cusip, source)` sibling may have been negatively marked in
   an earlier pass) — otherwise a stale negative survives until the
   next extid sweep. Legacy fuzzy statuses stay untouched.
6. `OpenFigiSweepReport` gains `passes` + per-status tombstone counters;
   invoker logs them and stamps `row_count = promoted`.

## 5. Cadence

Add `cusip_resolver_post_bulk_sweep` to `SCHEDULED_JOBS`: weekly Sunday
06:00 UTC (one hour after `cusip_universe_backfill` Sun 05:00, which
feeds the pre-sweep extid tombstone), Lane `openfigi` (cap=1, sql/165),
`catch_up_on_boot=False`, prereq `_bootstrap_complete`, NOT exempt from
the universal gate. The bootstrap S13 stage entry is unchanged; the
source-registry conflict check (sources.py:473) accepts dual membership
when both paths declare the same lane (`openfigi`) — pinned by test.
Tests/allowlists that currently treat the job as bootstrap-only (e.g.
the `tests/test_jobs_runtime.py` not-in-SCHEDULED_JOBS comment block)
move it to the scheduled cohort (Codex ckpt-1 #6).

## 6. Re-materialisation (operator runbook)

Negative-status drain only fixes the MAPPING. Observations for
newly-mapped CUSIPs were skipped during past bulk ingests, so after the
first drained run:

1. `POST /jobs/sec_13f_ingest_from_dataset/run` and
   `POST /jobs/sec_nport_ingest_from_dataset/run` (idempotent re-read of
   the cached bulk archives; resolves with the new map).
2. Ownership rollup refresh follows the standard write-through path.

## 7. Escape hatch (no-auto-retry v1)

Re-eligible a status class manually, e.g. after a universe expansion:

```sql
UPDATE unresolved_13f_cusips
   SET resolution_status = NULL
 WHERE resolution_status = 'openfigi_no_instrument';
```

Documented here; no code path.

## 8. Acceptance (#740 + ETL DoD)

- `SELECT` coverage of cusip-bearing instruments among CIK-mapped
  tradables ≥ 0.80 after drain + re-ingest on dev.
- Panel: AAPL/GME/MSFT/JPM/HD retain correct CUSIPs; ADBE + BRK.B gain
  rows (BRK.B via the `BRK/B` normalisation, provider='openfigi').
- Cross-source: one figure (e.g. ADBE or BRK.B top-holder units) checked
  against EDGAR/independent source post-re-ingest.
- `/instruments/ADBE/ownership-rollup` renders institutional data.
- Tests: pure-tier table tests for normalisation (incl. the
  `ABC WS` / `ABC/U` / `BRK/PB` no-promote pins) + status
  classification + drain-loop exit conditions (mock resolver, existing
  `tests/test_cusip_resolver_openfigi.py` patterns), plus the
  rowcount-0 / concurrent-existing-extid promote path (Codex ckpt-1
  #5 — a "successful" pass must never leave a selected row NULL);
  registry/lane parity pins; one db-tier test ONLY if a genuinely-new
  SQL mechanism emerges (CHECK widen is covered by migration replay +
  dev backfill evidence).

## 9. Out of scope

- XBRL `dei:Cusip` extraction (#740 option a) — unnecessary if the
  drain closes the gap; revisit only if acceptance still fails.
- Auto-retry windows for negative statuses (operator: v1 terminal).
- The legacy (`source IS NULL`) fuzzy-resolver partition — untouched.
