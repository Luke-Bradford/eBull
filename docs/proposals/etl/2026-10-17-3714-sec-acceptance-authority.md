# #3714: SEC acceptance time comes from the EDGAR header, and only from the header

Status: spec rev 4, 2026-10-17. Rung: judgement artefact (Codex ckpt-1), then a corpus change.

History:
- **Rev 1** drew 31 findings. Its `filed_at` half moved to **#3869**, its bundle-clock half to **#3870**.
- **Rev 2** drew 35 findings. Rev 3 adopts the simpler design Codex proposed there: the header is the sole writer
  of `accepted_at`, and it carries its own evidence. That removes the two-authority merge, the quarantine race and
  the old-writer deploy barrier.
- **Rev 3** drew 36 findings, resolved in rev 4 (disposition table at the end).

## Scope

`sec_filing_manifest.accepted_at` only.
- `filed_at` is not changed. Its writers and its value stay as they are (#3869).
- The bundle clock is #3870.

## Source rule

- **Authority: `<ACCEPTANCE-DATETIME>YYYYMMDDHHMMSS`** in `<accession>.hdr.sgml`, EDGAR's archived submission
  header (the URL shape already used by `manifest_parsers/sec_tender.py:94`).
  - The *EDGAR PDS Technical Specification* (rev. 2013-07-22, `sec.gov/files/info/edgar/pdsdissemspec072213.pdf`)
    defines the acceptance fields of the dissemination control block: *"Acceptance Time — Time (EST) at which the
    submission was accepted by EDGAR. Format is HHMMSS."*
  - That the archived tag carries the same clock, and that "EST" means **Eastern local time with DST**, is an
    **empirical interpretation**, labelled as such. It rests on two observations:
    - **the Atom feed**: its explicit-offset `<updated>` matched the header on 50 of 50 live summer entries
      (`-04:00`);
    - **AAPL's earnings 8-Ks** read `163033` (2026-01-29), `163023` (2025-01-30) and `163025` (2025-07-31). The
      same local wall time in both seasons is consistent with an Eastern-local clock. This is corroboration only.
  - The header is converted with `ZoneInfo("America/New_York")`. A wall time that does not exist (spring-forward
    gap) or is ambiguous (fall-back fold) has no single UTC instant, so it is classified `invalid_value` and left
    unwritten.
- **Atom `<updated>` and submissions JSON `acceptanceDateTime`** are not written to `accepted_at`.
  - JSON: the value of one filing moves between captures (Measurements).
  - Atom: it agrees on the sample, but no published contract for the field has been identified. The cost of dropping it
    is stated in Design 5.
- **A header without the tag** (some old filings, for example `0000733269-99-000026`) means no header-derived
  acceptance is available, and `accepted_at` stays NULL.

## Measurements

These are samples, except where marked full-population. Commands are in
`PYTHONPATH=. uv run python -m scripts.measure_3714_acceptance --help`. Offsets are surface − header in seconds,
and a negative offset means earlier than the header acceptance. Raw outputs are attached to the PR.

1. **`manifest --n 300`**, a sample of the 637,635 rows that have `accepted_at`:
   - 290 headers `found`: 270 exact, 19 early by 14,400 or 18,000 s, 1 late.
   - 9 `absent` and 1 `invalid`.
2. **`captures`** on those 290 rows. Disjoint tuples of (manifest, 08-22, 10-06, 10-17), each minus the header:

   | tuple | rows |
   |---|---:|
   | (0, 0, 0, 0) | 165 |
   | (0, 0, +4h, +4h) | 42 |
   | (0, 0, 0, +4h) | 25 |
   | (0, 0, +5h, +5h) | 16 |
   | (0, 0, 0, +5h) | 10 |
   | (0, absent, +4h, +4h) | 5 |
   | (0, absent, absent, +4h) | 5 |
   | (−4h, 0, 0, 0) | 7 |
   | (−5h, −5h, −5h, −5h) | 4 |
   | (−4h, −4h, −4h, −4h) | 4 |
   | (−4h, −4h, 0, 0) | 2 |
   | (−5h, 0, 0, +5h) | 1 |
   | (0, −5h, −5h, −5h) | 1 |
   | (−5h, 0, 0, 0) | 1 |
   | (+4h, absent, +4h, +4h) | 1 |
   | (0, −4h, 0, 0) | 1 |

   - **Observed:** the JSON value of one filing differs between captures. It went from early to correct
     (3 rows) and from correct to late (94 rows). No single row shows all three states.
   - **Observed:** our stored value disagrees with all three captures on 9 rows where the captures agree with
     each other, so its provenance is undetermined.
3. **`cik 320193 --n 40`** (the live JSON, `recent` block): 40 of 40 are late, 24 by 4 h and 16 by 5 h.
4. **`manifest --since 2026-10-05T11:51:38Z --n 80`** (population 9,219): 71 exact, 9 late by 4 h, 0 early.
5. **`hours`, full population**, counting out-of-window anomalies rather than proven errors:
   - stored values before 06:00 ET: 24,665;
   - after 22:00:00 ET: 153.
   - A header check of 20 of the before-06:00 rows: 7 found, all early by 4/5 h; 13 with no tag.

## Readers of `accepted_at`

All three are research inputs, and none is a live trading path.

| reader | use | when NULL |
|---|---|---|
| `strategy_shock_mechanism.load_material_sec_catalysts` | `event_window_start <= accepted_at <= decision_at` | next session open after `filed_at`'s UTC date. ⚠ Its SQL pre-filter `filed_at <= decision_at` uses the untrusted `filed_at`. That residual belongs to #3869. |
| `pead_candidate` → `pead_outcomes.earliest_entry_date` | before 09:30 ET is the same day, otherwise the next day | XBRL `filed_date + 1` |
| `insider_purchase_candidate` → `insider_purchase_outcomes` | refuses when `acceptance.date() >= entry_date` | the guard is skipped. ⚠ `.date()` takes the date in the returned value's own zone, and psycopg returns the session zone. Normalising to New York belongs with this reader's next change, not this slice. |

## Design

**Invariant.** Every `accepted_at` write goes through the refill job and is backed by stored header evidence.
- **Transitional form:** a non-NULL `accepted_at` either equals the conversion of the row's `found` check, or is
  the row's unchanged value in the frozen legacy baseline (2) and that row has no final check yet.
- **Converse:** every `found` check under the current policy version has its conversion in `accepted_at`.
- Both forms are census queries in `scripts.census_3714_acceptance`. They are NULL-safe and treat a row with no
  check as unchecked.

1. **Sole-writer enforcement in the database.**
   - **Trigger:** a `BEFORE INSERT OR UPDATE` trigger on `sec_filing_manifest` resets `NEW.accepted_at` to the
     old value (NULL on INSERT) unless the transaction has set `SET LOCAL ebull.acceptance_writer = 'header'`.
   - **Who sets it:** only the refill job does.
   - **Effect on old writers:** any old or forgotten writer is silently held off, not failed, so a discovery job
     on old code keeps running and simply stops moving the column. That closes the old-writer ordering hole:
     there is no cutover window to coordinate.
   - **API change:** `FilingIndexRow.accepted_at` and `record_manifest_entry(accepted_at=…)` are removed, with
     their 12 pass-through call sites. The upsert no longer names the column. The JSON and Atom parsers keep
     computing `filed_at` exactly as today (#3869).
2. **Frozen legacy baseline.**
   - **Migration:** it creates the trigger and, in the **same transaction**, runs
     `INSERT INTO sec_acceptance_legacy (accession_number, legacy_accepted_at, source, baselined_at) SELECT … WHERE accepted_at IS NOT NULL`.
     It also writes a single-row `sec_acceptance_epoch (epoch timestamptz)` = `now()`, inserted once; a rerun
     never moves it.
   - **What the baseline is:** the durable denominator for completion, the cohort definitions, and the rollback
     source. No other snapshot table exists.
   - **Applying it early on the dev DB at pre-push smoke time is safe:** it only copies values and stops
     non-header writes.
3. **Evidence table `sec_acceptance_checks`.**
   - **Columns:**
     - `accession_number` PK;
     - `outcome`;
     - `raw_value text`, the tag text exactly as found, even when invalid;
     - `acceptance_utc timestamptz`;
     - `url text NOT NULL`;
     - `http_status int`;
     - `diagnostic text`;
     - `policy_version int NOT NULL`;
     - `attempts int NOT NULL CHECK (attempts >= 1)`;
     - `checked_at`;
     - `next_retry_at`.
   - **Outcomes, final** (the header itself answers):
     - `found`: one valid tag inside one header block;
     - `absent`: a valid header block with no tag;
     - `invalid_value`: the tag is present, but its value is not exactly 14 digits, is not a calendar time, or is a
       DST gap or fold wall time.
   - **Outcomes, retryable** (only our observation failed): `transport_error`, `http_error` (non-200, including
     429 and 503), and `bad_response` (a 200 whose body is not exactly one `<SEC-HEADER>…</SEC-HEADER>` block
     with `<ACCESSION-NUMBER>` equal to the request, or one with a duplicated tag).
   - **Constraints:**
     - `found` ⇔ `acceptance_utc IS NOT NULL`;
     - `found` ⇒ `raw_value ~ '^[0-9]{14}$'`;
     - a final outcome ⇒ `next_retry_at IS NULL`;
     - a retryable outcome ⇒ `next_retry_at IS NOT NULL`.
   - **Retry policy:** backoff of 1 h × 2^(attempts−1), up to 6 attempts. After that, `next_retry_at` moves
     7 days out, so the row is reconsidered, never abandoned.
   - **Policy version:** `policy_version` bumps whenever the parser or the conversion changes. A bump makes every
     final check with an older version eligible again.
4. **Header fetch.**
   - **URL:** `https://www.sec.gov/Archives/edgar/data/{int(manifest.cik)}/{accession-no-dashes}/{accession}.hdr.sgml`,
     built from the manifest `cik`, not from `primary_document_url`, which may be NULL.
   - **Measured:** in the 300-row manifest sample this shape returned a valid SGML header for the requested
     accession on 299 of 300; 1 was `bad_response` and 0 were HTTP errors.
   - **Client:** fetched through `SecFilingsProvider`, the `sec_edgar.py` client whose throttle shares
     `_PROCESS_RATE_LIMIT_CLOCK` with the manifest worker in the jobs process. The PR verifies that identity.
   - **Accounting:**
     - a timeout of 30 s;
     - no client-level retry;
     - one wire request = one attempt, incremented in the same commit as the outcome;
     - a crash before commit repeats one request, which is accepted.
   - **Conversion:** one function, `raw_value` → UTC through `America/New_York`, which refuses a gap or fold by
     `fold`/round-trip checks. Its tests come from fixtures independent of the parser: a summer value, a winter
     value, both DST boundaries, and 2026-03-08 02:30 / 2026-11-01 01:30.
5. **Job `sec_acceptance_refill`.**
   - **Scheduling:** a singleton job (the runtime's no-overlap lock) every 10 minutes, with a budget of 500
     requests per fire, which is ≤ 1 req/s averaged. It catches up by backlog, not by missed fires.
   - **Ready work:** no check row; or a retryable check whose `next_retry_at <= now()`; or a final check with an
     old `policy_version`.
   - **Cohorts and capacity:**
     - **T1**, ≤ 40 % of the budget: priority forms (`sec_8k`, `sec_10k`, `sec_10q`, `sec_form3`, `sec_form4`,
       `sec_form5`) with `created_at >= epoch`.
     - **T2**: baseline rows of priority forms.
     - **T3**: other baseline rows.
     - **Capacity:** unused T1 capacity passes to T2 and then T3. T3 receives ≥ 20 % of the budget whenever it
       has ready work, so T2 cannot starve it.
     - **Order within a cohort:** accession order.
   - **Measured arrival:** priority rows created in the last 7 days is a figure to report in the PR, from the
     census.
   - **Per row**, after the fetch, in one transaction:
     - `SELECT … FROM sec_filing_manifest WHERE accession_number = %s FOR UPDATE`;
     - lock and re-read the check row, and stop if a final check with the current policy now exists;
     - upsert the check. A retryable outcome never overwrites a final one, enforced by the upsert's `WHERE`;
     - `SET LOCAL ebull.acceptance_writer = 'header'`;
     - **on `found`:** `accepted_at` = the conversion;
     - **on `absent` / `invalid_value`:** `accepted_at = NULL`;
     - **on a retryable outcome:** `accepted_at` is untouched, and a legacy value stays until a final outcome.
   - **Coverage cost of dropping Atom:**
     - non-priority rows created after the epoch get no `accepted_at`. No reader reads those forms, and that is
       accepted.
     - priority rows wait for T1, at most one fire behind under normal load. The census reports the age of the
       oldest unchecked T1 row.

## Acceptance

- **Unit tests:**
  - parser outcomes on independent fixtures, including truncated body, wrong accession, duplicated tag, 13/15
    digits, a tag outside the block, and every HTTP and transport branch;
  - the conversion and its DST cases;
  - the upsert's final-over-retryable rule.
- **DB tests:**
  - the trigger: a write without the GUC is held off, with the GUC it lands;
  - cohort selection and the ready-work predicate;
  - the per-row transaction for each outcome class.
- **Gates (`census_3714_acceptance`, accession-level output attached).** Each is reported against the frozen
  baseline counts, and every non-final state is listed:
  - **After T2 has no ready work:**
    - per state, for the T2 baseline: `found`, `absent`, `invalid_value`, retry-waiting, exhausted (≥ 6
      attempts), and never-attempted, which must be 0;
    - both invariant queries return 0 violations over the T2 rows;
    - per reader: before and after **decisions** (admitted or rejected, entry date or knowledge time) for every
      event whose input changed, including the insider guard's eligibility flips. Each changed non-NULL input now
      equals its header value.
  - **After T3:** the same over the whole baseline. Then a fresh `manifest --n 300` drawn from the **baseline**
    population (not from non-NULL rows) reports every outcome. With `found` ≥ 270 of 300, the gate is 0 non-zero
    offsets among the `found` rows.
- **Docs:**
  - `docs/data-sources/sec-bulk-archives.md:65` is corrected in the spec PR;
  - post the skill text for `sec-edgar.md` §7.8 and `:581` on #2403, because `.claude/**` is write-refused in a
    loop worktree.

## Ckpt-1 disposition (round 3, 36 findings; the round cap is 3, so these are resolved here, not re-reviewed)

| findings | resolution |
|---|---|
| 1, 7, 22, 30, 31 | Transitional and converse invariants; frozen baseline; per-state gates (Design invariant, 2, Acceptance) |
| 2, 3, 36 | DB trigger enforces the sole writer (Design 1). No T4 repair cohort is needed. |
| 4, 5, 34 | Row locks and a final-wins upsert (Design 5); test wording fixed |
| 6, 21 | Snapshot table dropped; the baseline is the rollback source. Rollback = restore `legacy_accepted_at` under the GUC for rows still holding the header value, and delete their checks. |
| 14, 15, 16, 18, 20, 35 | Outcome taxonomy split final and retryable; raw value, URL and diagnostic kept; constraints (Design 3). The old `invalid` maps to `bad_response`. |
| 17 | The URL comes from the manifest CIK; measured 299/300 valid, 1 `bad_response` (Design 4) |
| 19 | `policy_version` re-eligibility (Design 3) |
| 23, 24, 25, 27, 28, 29 | Epoch row, ready-work predicate, capacity shares, schedule, shared limiter, accounting (Design 4-5) |
| 26 | Coverage loss stated and accepted (Design 5) |
| 32, 33 | Baseline-drawn sample with a threshold; reader decisions rather than raw direction (Acceptance) |
| 8, 9, 10, 11, 12, 13 | Wording in the Source rule and Measurements tightened: empirical interpretation; Apple is corroboration only; gap/fold refused because its UTC is undefined; "no contract identified"; offsets are relative to the header acceptance, and the acceptance-as-knowledge-time policy is each reader's own, unchanged here; provenance undetermined |

## Not in scope

- `filed_at` and the shock pre-filter (#3869);
- the bundle clock (#3870);
- the insider reader's date zone (noted above);
- back-filling `accepted_at` for priority-form rows that never had a value. 8-K holds 376k such rows. The job's
  cohorts make that one predicate away if a reader needs it.
