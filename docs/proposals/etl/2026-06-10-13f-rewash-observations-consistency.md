# 13F rewash ↔ observations consistency (#953 + #954)

Status: plan for review. Both bugs live in `app/services/rewash_filings.py::_apply_13f_infotable`; latent today (CUSIP sweep recovers identical CUSIPs), fire on any parser fix that changes/drops a CUSIP or on duplicate XML rows.

## #954 — dedupe mismatch

Rewash builds `resolved: list[(instrument_id, holding)]` without collapsing duplicate
`(instrument_id, exposure_kind)` rows. Typed table keeps FIRST dup
(`_upsert_holding` ON CONFLICT DO NOTHING on partial unique index
`(accession, instrument, COALESCE(is_put_call,'EQUITY'))`); observations recorder
(`record_institution_observation` ON CONFLICT DO UPDATE) keeps LAST. First ingest
already dedupes at `institutional_holdings.py:1597-1633` (`resolved_by_key.setdefault`).

Fix: mirror the first-ingest dedupe in the rewash resolution loop —
`resolved_by_key: dict[tuple[int, str], tuple[int, ThirteenFHolding]]`, key
`(instrument_id, put_call if in ("PUT","CALL") else "EQUITY")`, `setdefault` keep-first,
`resolved = list(resolved_by_key.values())`. Side effect: `inserted` count becomes exact
(prevention-log "ON CONFLICT DO NOTHING counter overcount").

## #953 — stale observations on CUSIP change

Rewash DELETEs+reinserts `institutional_holdings` for the accession, but write-through
only records observations for the NEW resolved set and refreshes
`ownership_institutions_current` only for NEW instruments. If a parser fix drops
instrument A: A's `ownership_institutions_observations` row for the accession stays
live (`known_to IS NULL`) and A's `_current` is never re-MERGEd → rollup keeps stale figure.

Fix (DELETE, not tombstone): inside the existing `acquire_13f_accession_write_lock`
window, next to the typed-table DELETE:

1. `SELECT DISTINCT instrument_id FROM ownership_institutions_observations
   WHERE source = '13f' AND source_document_id = %(accession)s` → `prior_instrument_ids`.
2. `DELETE FROM ownership_institutions_observations
   WHERE source = '13f' AND source_document_id = %(accession)s` — mirrors the typed-table
   DELETE; same tx so no visibility gap.
3. Write-through re-records the new set (unchanged recorder).
4. Refresh `_current` over `prior_instrument_ids | {new instrument ids}` — dropped
   instruments get re-MERGEd; their stale `_current` row falls out via the
   `WHEN NOT MATCHED BY SOURCE ... DELETE` arm.

Why DELETE not `known_to` tombstone: `record_institution_observation`'s ON CONFLICT
DO UPDATE never clears `known_to`; a tombstoned row re-asserted by a later rewash would
stay invisible to the `_current` MERGE (`WHERE known_to IS NULL`) forever — silent data
loss. DELETE keeps both layers symmetric (typed table already does DELETE+INSERT).

Identity note: within one accession, observation identity
`(instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind)`
reduces to `(instrument_id, exposure_kind)` — filer_cik/nature('economic')/period_end are
constant. The accession-scoped DELETE also sweeps any historical drift rows (e.g. obs
written under an older period_end for the same accession).

## Out of scope (note in PR)

Live re-ingest path (`_ingest_single_accession`) has the symmetric staleness for
operator-forced re-ingest — but its typed table is ON CONFLICT DO NOTHING (no DELETE),
so typed + obs stay consistent with each other there. #953 is scoped to rewash.

## Tests (DB tier, `tests/test_rewash_filings.py`, existing fixture patterns)

1. `test_13f_infotable_rewash_dedupes_duplicate_xml_rows` (#954 Done): parser emits two
   rows for same (instrument, EQUITY) with different shares → exactly one row in
   `institutional_holdings` AND one in `ownership_institutions_observations`, both
   carrying the FIRST row's shares/value.
2. `test_13f_infotable_rewash_clears_stale_observations_for_dropped_instrument`
   (#953 Done, sharpened per Codex ckpt-1): seed typed holding + observation (via
   `record_institution_observation`) + `_current` (via `refresh_institutions_current`)
   under instrument A, PLUS a period-drifted obs row for the same accession (older
   `period_end` — distinct conflict identity, so the upsert alone would never clear it);
   rewash parser emits only instrument B → A's obs rows for the accession gone (both
   period_ends), A's `_current` empty, B present in obs AND `_current` (pins both the
   refresh-union fan-out and the MERGE delete arm).

## Gates

ruff / pyright / fast tier / smoke + `pytest -m db` (DB/SQL change) + targeted
`pytest tests/test_rewash_filings.py`. Codex ckpt-2 pre-push with financial-plumbing /
data-engineer / data-scientist / adversarial lenses (pre-pr-fresh-agent-review).

Dev-verify: no schema change, no backfill needed. Divergence probe ran 2026-06-10
(dev DB): 2,856,833 obs rows / 24,734 accessions exist with NO typed rows — ALL
classified `accession_absent_from_typed` (bulk dataset path `sec_13f_dataset_ingest.py`
writes observations only, by design — #807 bulk-first). Instrument-level drift
(`accession_in_typed_diff_instruments`, the #953 damage class): **zero rows** — bug
confirmed latent, nothing to repair.

Codex ckpt-2 (2026-06-10): no correctness bugs in diff. Pre-existing touched-path gap
(rewash + legacy ingest lack PRN filter + pre-2023 VALUE ×1000 that the manifest parser
applies) → DEFERRED #1566, including the post-#953 interplay (rewash re-record can
re-insert PRN rows bulk seeding excluded).

Cross-source (2026-06-10, SEC EDGAR direct): Vanguard Q4-2025 AAPL — dev obs
1,279,051,701 shares traces EXACTLY to the SOLE-discretion infotable row in accession
0000102909-26-000031 (data path validated). But the filing carries 7 AAPL rows
(sub-manager splits) summing 1,426,283,914 — the settled keep-first dedupe drops the
other 6 (10.3% undercount on AAPL's largest holder). Filed #1567 (systematic, affects
all multi-sub-manager filers, all ingest paths). #954 deliberately preserves keep-first
for layer consistency; the semantic fix is #1567's.
