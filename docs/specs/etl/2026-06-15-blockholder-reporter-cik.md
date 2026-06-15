# 13D/G blockholder `reporter_cik` — source from the document, not the manifest

Issue: #1638 (part of #788 — ownership card trustworthy). Live spec. **PR1 of 2**
(PR2 = concentration cross-slice dedup; see "Bug A" below — operator-scoped 2026-06-15).

## Problem

`ownership_blockholders_current.reporter_cik` carries the **subject company's own
issuer CIK** for 733/800 dev rows (≈93%), with the correct reporting-person **name**.
GME: `reporter_name='Cohen Ryan'`, `reporter_cik='0001326380'` — but `0001326380`
is GameStop's issuer CIK.

### Root cause (cross-layer, verified)

`#1628` flipped the 13D/G manifest from *filer*-keyed to *issuer*-keyed so
subject→instrument resolution works via issuer-CUSIP. But the edgartools adapter
still feeds `manifest.cik` into `BlockholderFiling.primary_filer_cik`
(`_schedule13_adapter.py:233`), whose documented meaning is "the EDGAR submitter" —
so post-#1628 that field silently became the **issuer CIK** on the drain path. The
observation writer copies it verbatim (`blockholders.py:863`).

The correct identity is in the document and **already parsed/stored**:
- per-reporter `<reportingPersonCIK>` → `BlockholderReportingPerson.cik` and
  `blockholder_filings.reporter_cik` (Cohen `0001767470`). Present on modern 13D;
  **absent on modern 13G** (443/443 dev rows null — mandate 13G omits it).
- `<filerCredentials><cik>` → the EDGAR submitter (the filer of record). Present on
  every successfully-parsed filing; never the issuer (verified Vanguard `0000102909`,
  Janus `0001274173`, BlackRock `0002012383`, all ≠ their issuers).

Empirical survey of all 800 stored accessions (edgartools `Schedule13{D,G}.parse_xml`
+ adapter): 13D = 342/369 carry a non-null chosen-reporter CIK; 13G = 443/443 null;
chosen-reporter CIK is **never** the issuer CIK (0 rows).

## Fix — resolve reporter identity from the document (two write paths)

There are **two** writers into `ownership_blockholders_observations` for 13D/G — both
must stop writing the issuer CIK:

1. **Write-through drain** (`_record_13dg_observation_for_filing`, the canonical path
   per #873.C) — the 733 bad rows came from here.
2. **Legacy mirror** (`sync_blockholders`, run only by the manual one-shot
   `ownership_observations_backfill`; the *daily* `ownership_observations_sync` job is
   the repair sweep, not this) — writes `reporter_cik = blockholder_filers.cik` =
   issuer post-#1628 → a manual reintroduction vector (Codex ckpt-1 HIGH).

### Resolution rule (pure function, table-tested)

`resolve_blockholder_reporter_identity(reporting_persons, *, document_filer_cik)`:

1. `chosen = max(reporting_persons, key=aggregate_amount_owned DESC NULLS LAST)` —
   the existing selection; one observation per accession (preserves #837 joint-filer
   collapse).
2. `reporter_cik = chosen.cik or document_filer_cik`:
   - per-reporter CIK when the XML carries it (modern 13D — the disclosing person's
     own CIK, e.g. Cohen `0001767470`);
   - else the document `<filerCredentials><cik>` (modern 13G — the filer IS the
     beneficial owner; and the 27 multi-party 13D where the largest-aggregate reporter
     is a natural person with no CIK — the filer-of-record is the group's stable,
     joinable identity, never the issuer, never an agent for 13D/G per sec-edgar §3.7).
3. `reporter_name = chosen.name`, `aggregate/percent = chosen.*` (the largest disclosed
   beneficial position — most operator-useful label).
4. Return `None` (skip the observation) when: no reporting persons; chosen has no
   aggregate (mirrors the existing `aggregate_amount_owned IS NOT NULL` write-side
   guard, prevention-log "mirror write-time guards"); or both CIKs null (unjoinable).

**Group-filing caveat:** for a multi-party 13D whose largest-aggregate reporter has no
CIK, `reporter_name` (largest reporter) and `reporter_cik` (filer-of-record) may name
different members of the same filing group. This is inherent to the #837 one-row-per-
accession collapse; the filer-of-record CIK is the correct joinable group identity.
Affects 27/800 dev rows. Strictly better than the issuer-CIK bug (always wrong) or
skipping (hides a real large position).

### Path 1 — write-through
`_record_13dg_observation_for_filing` calls the resolver with
`document_filer_cik=filing.document_filer_cik`. New in-memory field
`BlockholderFiling.document_filer_cik` (NO DB column):
- **edgartools adapter** sets it via `extract_filer_cik_from_primary_doc(raw_xml)`
  (new helper reading `<filerCredentials><cik>`, zero-padded; `None` on absent/malformed).
- **in-house `parse_primary_doc`** sets it `= primary_filer_cik` (there it already IS
  the document filerCredentials).
`primary_filer_cik` is left as-is (still the `blockholder_filers` PK / ingest-log key).

### Path 2 — legacy mirror
`sync_blockholders` reads the per-row `blockholder_filings.reporter_cik` (already the
correct per-person CIK, written by `_upsert_filing_row(person.cik)`), `DISTINCT ON
(accession) ORDER BY aggregate_amount_owned DESC NULLS LAST`, and writes
`reporter_cik = bf.reporter_cik`, `reporter_name = bf.reporter_name`. When
`bf.reporter_cik IS NULL` (all 13G + the 27 multi-party 13D) it **skips** (orphan-log)
— the write-through is the canonical populator for those rows; the mirror must never
write the filer/issuer CIK as the reporter. (The typed tables carry no
filerCredentials column, so the mirror cannot reproduce the document-filer fallback;
deferring those rows to write-through is correct, not a coverage loss.)

### Other
- Bump `_PARSER_VERSION_13DG` `"13dg-primary-v2"` → `"13dg-primary-v3"`
  (`blockholders.py:76`) to force `sec_rebuild` re-drain.
- Update `record_blockholder_observation` docstring (#837 note: identity is the chosen
  reporting person / document-filer fallback, NOT the primary filer).

## Out of scope (noted, not fixed here)

- `primary_filer_cik` / `blockholder_filers.cik` are the issuer CIK on the drain path
  (typed-table mis-keying). Not operator-visible (rollup reads `_current` ←
  observations, which this fix corrects). Latent follow-up, not #1638.
- **Bug A — concentration double-count (#1640, PR2).** The rollup *deliberately* keeps 13D/G
  (beneficial) and Form 4 (direct) in separate pie-wedge slices and `_compute_concentration`
  **sums** them (`ownership_rollup.py:1386`, #837/#788-P0b) — so Cohen's insider 38.3M +
  blockholder 36.8M both count regardless of `reporter_cik`. Correct identity (this PR)
  is the **prerequisite**; PR2 adds a concentration-level cross-slice dedup-by-CIK
  (keeps the "show both slices" display, de-dups only the sum) → GME ≈40%.

## Backfill (the append-only trap) — Codex ckpt-1 HIGH/MED fixes folded

The observation natural key includes `reporter_cik`. Re-draining with a corrected CIK
**INSERTs** a new row; the stale issuer-CIK row stays valid (`known_to IS NULL`). The
`_current` MERGE is `DISTINCT ON (reporter_cik, ownership_nature)` — it would keep
**both** → a fix that worsens the double-count. Blockholder observations have **no
known_to supersession wired** (verified), so re-drain alone does not retire stale rows.

`scripts/backfill_1638_retire_issuer_cik_blockholders.py` (run on dev; idempotent):

1. Retire stale rows in one statement, capturing affected instruments:
   ```sql
   UPDATE ownership_blockholders_observations o
   SET known_to = clock_timestamp(), ingested_at = clock_timestamp()  -- bump ingested_at so
   -- the refresh/repair watermark (MAX(ingested_at)) advances past the expiry (Codex HIGH-3)
   FROM external_identifiers ei
   WHERE o.known_to IS NULL
     AND o.source IN ('13d','13g')
     AND ei.instrument_id = o.instrument_id
     AND ei.provider = 'sec' AND ei.identifier_type = 'cik'
     AND lpad(ei.identifier_value, 10, '0') = lpad(o.reporter_cik, 10, '0')
   RETURNING o.instrument_id;
   ```
   Exact bug signature; 0 legitimate self-filings (a 13D/G is never filed by the issuer
   on itself — confirmed chosen.cik never == issuer).
2. **Immediately** `refresh_blockholders_current` for each RETURNING instrument →
   MERGE `WHEN NOT MATCHED BY SOURCE THEN DELETE` drops the stale `_current` rows now
   (no window where `_current` holds both; Codex MED-3).
3. `POST /jobs/sec_rebuild/run {"source":"sec_13d"}` + `{"source":"sec_13g"}` →
   re-drain re-parses stored raw (no SEC re-fetch) → correct observations + per-accession
   refresh.

## Tests

- **Pure** `tests/test_blockholder_reporter_identity.py` (no DB): 13D chosen-with-CIK →
  Cohen `0001767470`; 13G null-cik → `document_filer_cik`; multi-party 13D null-chosen-cik
  → `document_filer_cik`; all-null aggregate → None; empty → None; both CIKs null → None;
  tie → list order.
- **Adapter** `tests/test_manifest_parser_sec_13dg.py`: `build_filing_from_edgartools_dict`
  populates `document_filer_cik` from `raw_xml`; None when `raw_xml=None`.
- **(db) invariant** `tests/test_blockholders_ingester.py`: after the drain on a GME-shape
  fixture, no active observation has `reporter_cik == issuer_cik`; mirror skips 13G.

## DoD

8. Panel AAPL/GME/MSFT/JPM/HD: `_current.reporter_cik` ≠ issuer CIK after backfill.
9. Cross-source: GME Cohen `reporter_cik → 0001767470` (his Form 4 CIK); Vanguard 13G →
   `0000102909`.
10. Backfill run on dev (retire + refresh + sec_rebuild) — record counts.
11. `/instruments/GME/ownership-rollup`: blockholder Cohen row shows `0001767470`.
    (Concentration stays 48.37% until PR2 — by design.)
12. PR records each + commit SHA.
