# #1687 — Insider DQ: future-dated `insider_transactions.txn_date`

## Problem

`insider_transactions` rows carry impossible `txn_date` values (e.g. LQDT
`2035-01-10`, TMUS `2026-06-17`). Operator-visible symptoms:

- **Insider list** (`list_insider_transactions`, `ORDER BY txn_date DESC`)
  pins the impossible row to the top.
- **Insider summary** (`get_insider_summary`, `MAX(txn_date)` + a
  `txn_date >= CURRENT_DATE - 90d` window) reports it as the "latest"
  activity and folds future rows into the 90d net/buy/sell aggregates.
- **Ownership rollup** — the Form 4 write-through
  (`_record_form4_observations_for_filing`) picks the *latest* txn per
  `(filer, nature)` by `txn_date DESC` and writes `period_end = txn_date`,
  so a future typo (a) becomes the group winner with the wrong share
  balance and (b) wins `ownership_insiders_current`
  (`refresh_insiders_current` orders `period_end DESC`). 126 current
  observations carry `period_end > filed_at` today.

## Source rule

Securities Exchange Act **§16(a)** + **Rule 16a-3(a) (17 CFR 240.16a-3(a))**
+ **Form 4 General Instructions (Instr. 1)**: a Form 4 is due before the end
of the **2nd business day following the day the reportable transaction is
executed** ⇒ the execution date normally precedes/equals the filing date.

**Exception — early filing.** SEC permits *early* reporting; the **EDGAR
Ownership XML Technical Specification** (`sec.gov/info/edgar/ownershipxmltechspec.htm`)
defines `transactionTimeliness/value`: `'E'` = early, `'L'` = late
(optional). When a filer voluntarily reports **early**, the reported
transaction date may legitimately *postdate* the filing date. Repo's
settled treatment: `sql/057_insider_transactions_richness.sql:262-265` —
"`E` = filed early (before the event), `L` = filed late".

**Source for the date-column semantics** — the **Form 4** itself
(`sec.gov/files/form4.pdf`, General Instructions + table headers):
Table I col. 3 "Transaction Date" and col. 3a "Deemed Execution Date, if
any" are *execution* dates (governed by the Rule 16a-3(a) 2-business-day
rule ⇒ `≤ filed_at`); Table II "Date Exercisable" (`exercise_date`) and
"Expiration Date" (`expiration_date`) are *future* derivative milestones
and are therefore exempt.

Invariant:

```
txn_date              <= filed_at   UNLESS transaction_timeliness = 'E'
deemed_execution_date <= filed_at   UNLESS transaction_timeliness = 'E'
```

**Exempt date columns (legitimately future):** `exercise_date` (Table II
"Date Exercisable") and `expiration_date` (option expiry) describe future
derivative milestones, not the reported transaction. The guard MUST NOT
touch them.

Anchor = `filed_at` (SEC-stamped acceptance date, `sec_filing_manifest.filed_at`,
#1233 — authoritative, not filer-typed). Threshold is strict `>` (same-day
filing is valid). No hand-waved "slack".

## Finding: FILER SOURCE TYPO, not parser mis-parse

Raw `form4_xml` carries the impossible date verbatim in
`<transactionDate><value>` while the same filing's `periodOfReport` /
`signatureDate` are correct. **Full-population check (not a sample):** of
the 80 non-`E` `txn_date` violators, all **75 with a retained raw payload
have the stored date present verbatim** as `<value>…</value>` in the source
XML; **0** parser-invented dates. The 5 remainder are retention-swept (same
blank-timeliness class). ⇒ **Re-ingest cannot fix it.** We do not invent a
corrected date (`periodOfReport` ≠ this txn's date on multi-txn forms —
that would be a heuristic).

## Full-population verification (dev DB, 1,016,755 rows)

| Date column | rows `> filed_at` | of which `timeliness='E'` | treatment |
|---|---|---|---|
| `txn_date` | 92 | **12 (all code-J, gap 14–18 d — LEGIT early)** | flag the **80 non-E** rows |
| `deemed_execution_date` | 19 | 0 (all blank) | NULL the **19** |
| `exercise_date` | 14,851 | — | **LEGIT — exempt** |
| `expiration_date` | 92,663 | — | **LEGIT — exempt** |

The 12 `E` rows are tightly clustered (one filing agent, code J, 14–18 day
gaps) — deliberate early reports of near-future transactions, **not**
typos. The 80 non-E violators span gaps of 1–3651 days; 0 are unjoined to
the manifest. `txn_date_invalid` flags only the 80.

Contaminated observations (`source='form4'`, `period_end > filed_at`,
`known_to IS NULL`): **126** = 92 derived from a flaggable non-E txn + 24
derived from a legit `E` txn (**must NOT clean**) + 10 unmatched-on-join
(`period_end > filed_at`, not E — also contaminated). Cleanup keys off the
flag / non-E predicate so the 24 `E`-derived observations are preserved.

## Design

Row-level treatment (a typo'd txn coexists with valid sibling txns under
the same accession), not filing-level tombstone.

1. **Schema (`sql/205`)** — add
   `insider_transactions.txn_date_invalid BOOLEAN NOT NULL DEFAULT FALSE`.
   `txn_date` is NOT NULL ⇒ keep the raw (bad) value for audit + flag the
   row. `deemed_execution_date` is nullable ⇒ a violation is quarantined to
   NULL (no invent). Same migration backfills existing rows (idempotent
   UPDATEs):
   - `txn_date_invalid = TRUE` where `txn_date > filed_at`
     AND `transaction_timeliness IS DISTINCT FROM 'E'` (join
     `sec_filing_manifest`).
   - `deemed_execution_date = NULL` where `deemed_execution_date > filed_at`
     AND `transaction_timeliness IS DISTINCT FROM 'E'`.

2. **Parse-time guard** — pure helper
   `evaluate_insider_date_validity(txn_date, deemed_execution_date, filed_at,
   transaction_timeliness) -> (txn_date_invalid: bool,
   deemed_execution_date: date | None)`:
   - `filed_at is None` (no anchor) ⇒ `(False, deemed unchanged)`.
   - `transaction_timeliness == 'E'` ⇒ `(False, deemed unchanged)` (early).
   - `txn_date > filed_at.date()` ⇒ `txn_date_invalid = True`.
   - `deemed_execution_date > filed_at.date()` ⇒ drop to `None`.

   Add `txn_date_invalid: bool = False` to `ParsedTransaction`. In
   `upsert_filing` (the persist chokepoint that resolves `filed_at`),
   sanitize `parsed.transactions` through the helper **before** both the DB
   insert loop **and** `_record_form4_observations_for_filing`. The DB loop
   writes the flag (INSERT + `ON CONFLICT DO UPDATE` so rewash re-applies);
   the observation builder skips `txn.txn_date_invalid` in its
   latest-per-group selection so the real latest valid txn wins and
   `period_end` is correct.

3. **Readers** — exclude flagged rows:
   - `get_insider_summary` — `AND NOT it.txn_date_invalid` (fixes
     `MAX(txn_date)` + the 90d window).
   - `list_insider_transactions` — `AND NOT it.txn_date_invalid` (fixes
     `ORDER BY txn_date DESC`).
   - `ownership_observations_sync.sync_insiders` — `AND NOT it.txn_date_invalid`
     in the insider select so the periodic / rebuild re-derivation also
     excludes invalid txns.

   **Not filtered** (deliberate): `insider_form3_ingest` "did this filer
   transact?" EXISTS and `ownership_drillthrough` reconciliation COUNT both
   want the row present (it is a real transaction, only its date is wrong).

4. **No `_PARSER_VERSION` bump** — cleanup is a targeted SQL UPDATE; the bad
   rows' other fields are correct. Not bumping avoids a mass re-ingest;
   future filings get the guard via INSERT, rewash re-applies via ON
   CONFLICT. ⇒ **no `sec_rebuild` needed.**

## Cleanup of existing contaminated ownership observations (backfill)

`refresh_insiders_current` picks the winner by `period_end DESC` among
`known_to IS NULL` obs, and the corrected re-derivation writes a *different*
`period_end` (different natural key) — so re-running the sync alone leaves
the future-`period_end` obs winning. Cleanup must explicitly supersede it:

1. (migration) flag txns + NULL deemed (above).
2. (backfill, post-window) supersede contaminated form4 observations,
   anchored on the **manifest** `filed_at` (the obs's own `filed_at` may
   carry the legacy `txn_date`-midnight fallback) with obs-`filed_at` only
   as a last resort when no manifest row exists, and preserving the legit
   `E` cohort by a positively-keyed `(accession, period_end)` match:

   ```sql
   UPDATE ownership_insiders_observations o
   SET known_to = now()
   FROM ... -- LEFT JOIN sec_filing_manifest m ON m.accession_number = o.source_accession
   WHERE o.source = 'form4' AND o.known_to IS NULL
     AND o.period_end > COALESCE(m.filed_at, o.filed_at)::date
     AND NOT EXISTS (
       SELECT 1 FROM insider_transactions it
       WHERE it.accession_number = o.source_accession
         AND it.txn_date = o.period_end
         AND it.transaction_timeliness = 'E');
   ```

   Verified cohort (dev): **102 superseded** (101 manifest-anchored + 1
   fallback) / **24 `E`-derived preserved**.
3. (backfill) re-run `sync_insiders` (now excludes flagged txns → records
   the correct winner obs) + `refresh_insiders_current` for affected
   instruments → `ownership_insiders_current` re-picks the real latest.

This is an observation re-derivation, **not** `sec_rebuild` (no SEC fetch).

## Tests

Pure-logic table test of `evaluate_insider_date_validity`: txn after /
equal / before filed; deemed after / equal / before / NULL; `filed_at`
None; `transaction_timeliness='E'` exempts both txn and deemed; same-day
boundary. One DB test: seed a non-E future txn + an E future txn + a same-day
txn; assert the migration/guard flags only the non-E future row and leaves
the E row + same-day row alone.

## DoD (clauses 8-12) — executed post-#1710-window

- Apply `sql/205` on dev; verify the source-rule invariant
  `txn_date > sec_filing_manifest.filed_at::date AND transaction_timeliness
  IS DISTINCT FROM 'E' AND NOT txn_date_invalid` → **0** (this catches the
  past-but-future-at-filing rows the `> CURRENT_DATE` check misses).
  Secondary operator-visible check: `txn_date > CURRENT_DATE AND NOT
  txn_date_invalid` → **0**. Raw bad `txn_date` retained for audit; the 12
  `E` rows untouched.
- Run the observation backfill; verify
  `ownership_insiders_observations` with `period_end > filed_at AND known_to
  IS NULL AND <non-E>` → **0**; the 24 `E`-derived obs remain.
- Smoke panel AAPL/GME/MSFT/JPM/HD insider endpoints render.
- Cross-source: SEC EDGAR direct on one of the 21 accessions (raw
  `<transactionDate>` already pulled — confirms typo).
- Hit `/{symbol}/insider_transactions` + `/ownership-rollup` for an affected
  instrument; confirm the future row no longer surfaces.
