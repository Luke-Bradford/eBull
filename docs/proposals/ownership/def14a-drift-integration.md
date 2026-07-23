# #966 (re-scoped): DEF 14A drift detector — production wiring + rollup chip

2026-07-23. Original #966 items (1)/(2) are WON'T DO — superseded by the
owner-once reconcile (#1941); premise-verification map on the issue thread.
This spec covers the live remainder: (a) wire `def14a_drift.detect_drift`
into production, (b) surface >=5% drift as an operator-visible chip on the
ownership rollup, (c) live-verify the original acceptances 1-3.

## Source rule

Documented basis (unchanged by this spec, cited for the record): the
DEF 14A side is the Reg S-K **Item 403** "Security Ownership of Certain
Beneficial Owners and Management" table (Schedule 14A Item 6 incorporates
it), whose figures are **Rule 13d-3** beneficial-ownership totals as of the
proxy record date; the Form 4 side is the **Section 16(a)** transaction
stream whose cumulative position the reconcile maintains. The two are the
same shares restated through different lenses (prevention-log #1851/#1852:
overlapping restatements MAX, never SUM) — so a divergence is a COVERAGE
signal (missed/late Form 4s, ingest gap), never a holdings correction.
Drift mechanics (5%/25% thresholds, normalised-name match) were settled
in issue #769 PR3 and are NOT changed; this spec is wiring + a read-only
surface.

## What ships

### (a) Writer wiring — three per-filing loci + weekly repair

1. New `run_drift_detection_best_effort(conn, *, instrument_ids, accession_number)`
   in `app/services/def14a_ingest.py`. Mirrors `apply_exec_comp_best_effort`
   exactly: SAVEPOINT-isolated (`with conn.transaction()`), broad-except,
   `logger.exception` on failure, never propagates — drift is an augment,
   never a gate on the holdings outcome (#1700 per-section isolation).
   Internally: `detect_drift(conn, instrument_id=iid)` per sibling.
2. Call it at ALL THREE def14a write loci (#817), after the observation
   write-through:
   - `def14a_ingest.py` legacy per-accession path (after the sibling loop,
     next to `apply_exec_comp_best_effort`).
   - `manifest_parsers/def14a.py` manifest-drain path (same position).
   - `rewash_filings.py` def14a rewash branch (Codex ckpt-1 HIGH: rewash
     writes `def14a_beneficial_holdings` directly, it does NOT re-run the
     manifest parser — hook it explicitly).
3. Weekly repair: one `detect_drift(conn)` (global) call in the
   `ownership_observations_backfill` job (`scheduler.py`), logged via the
   existing report line.
4. **Stale-alert purge (Codex ckpt-1 HIGH).** `detect_drift` today
   evaluates only the LATEST (instrument, holder) accession and
   clears/upserts only that accession's alert row — alert rows from
   superseded accessions linger and a severity-scoped read would surface
   them. Fix inside the detector loop: for each holder evaluated, DELETE
   this (instrument, holder)'s alert rows with
   `accession_number != <latest>` before the clear/upsert branch. One
   locus of truth; every writer converges because every writer runs the
   same detector. Full-pop assert post-seed: zero alerts whose accession
   is not that holder's latest.
5. **Orphan purge (Codex ckpt-2 HIGH).** A rewash DELETE+re-INSERT can
   rename or drop a holder from `def14a_beneficial_holdings` entirely —
   that holder is then never re-visited by the detector and their alert
   row would linger forever. `detect_drift` ends with a scoped DELETE of
   alerts whose (instrument, holder) no longer exists in the typed
   table at all.
6. **Stale-write guard (Codex ckpt-2 MED).** Writers serialize per
   accession only — two detectors processing different accessions of
   the same issuer can interleave read→purge→write such that the loser
   re-mints a stale alert after the winner's purge. `_upsert_alert` is
   now a conditional `INSERT … SELECT … WHERE accession = <holder's
   latest at write time>` (subquery mirrors the selector's ordering),
   making the stale write a no-op in either commit order.

### (b) Read surface — rollup chip

1. New frozen dataclass in `ownership_rollup.py`:

   ```text
   Def14ADriftInfo(
       worst_severity: Literal["warning", "critical"],
       alert_count: int,          # warning+critical only
       chip: str,                 # operator copy, server-owned
       holders: tuple[str, ...],  # top 3 holder names by drift_pct
   )
   ```

   `OwnershipRollup.def14a_drift: Def14ADriftInfo | None = None` (last
   field, default None — existing constructors unchanged, mirrors
   `dual_class_denominator` precedent). None when no warning/critical
   alerts. `info`-severity alerts are EXCLUDED — unmatched-name noise is
   already visible as the `def14a_unmatched` slice; chip would duplicate.
2. Assembly reads `def14a_drift_alerts` scoped to the instrument
   (severity IN warning/critical), ORDER BY drift_pct DESC. Uses the
   existing `(instrument_id, detected_at)` index for the instrument
   scope; the residual severity filter walks that instrument's handful
   of rows (table is small — alerts, not observations). No new index,
   no detector invocation at read time.
   **Drift assembly is denominator-independent (Codex ckpt-1 MED):** it
   runs BEFORE the shares-outstanding short-circuit, and
   `OwnershipRollup.no_data(...)` carries `def14a_drift` too — a
   coverage-integrity signal must not vanish exactly when the rollup is
   otherwise degraded.
3. API: `_Def14ADriftModel` + `def14a_drift` field on the rollup response
   model (`instruments.py`), serialized like `concentration`.
4. FE: chip in `OwnershipPanel.tsx` next to the float-concentration chip;
   amber for warning, red for critical; copy comes from the server
   (`chip`), no client-side threshold logic (operator-ui convention:
   server-owned copy).

### (c) Live verification (dev DB, recorded on the PR)

- Acceptance 1: AAPL DEF 14A officer total vs insiders slice within ±2%.
- Acceptance 2: one live CEO/officer with no Form 4 in 12 months whose
  DEF 14A row surfaces on the rollup (matched → insider MAX, or
  unmatched → def14a_unmatched slice).
- Acceptance 3: one live proxy 5%-holder absent from blockholder_filings
  that surfaces (def14a_unmatched or family channel).
- Chip: after a global `detect_drift(conn)` seed run, hit
  `/instruments/{sym}/ownership-rollup` for a name with a warning+ alert
  and confirm the chip payload.

## Tests (lean)

- Pure: `Def14ADriftInfo` assembly from alert rows (severity filter, top-3
  ordering, copy) — table-test the pure helper.
- One DB test: ingest-path hook writes alerts + rollup read surfaces them
  (reuse existing def14a test seeding shape from `tests/test_def14a_drift.py`).
- FE: extend existing OwnershipPanel test for chip render (both severities
  and absent-state).

## Out of scope

- `holder_role` threading into rollup classification (filed separately).
- Any change to detector thresholds/matching (settled #769 PR3).
- Backfill of historical alert state beyond the weekly global run — the
  seed run at dev-verify populates current state; alerts self-clear.

## Full-population verification note

Acceptances 1-3 are single-name EXISTENCE demonstrations (each proves a
path renders once); the SAFETY claims are verified full-population
(Codex ckpt-1 FLAG addressed):

1. Seed run: `detect_drift(conn)` walks EVERY def14a holder row;
   DriftReport counts recorded on the PR.
2. Stale-purge invariant, full-pop SQL: zero rows in
   `def14a_drift_alerts` whose accession is not that
   (instrument, holder)'s latest — asserted post-seed over the whole
   table.
3. Chip correctness, full-pop: script iterates EVERY instrument that has
   a warning/critical alert post-seed, builds the rollup for each, and
   asserts (a) `def14a_drift` present with matching worst-severity +
   count, (b) info-only instruments yield `def14a_drift=None`, (c)
   no_data-state instruments still carry the chip. Counts recorded on
   the PR.
