# #840 — Phase 1 schema unification: implementation plan

**Status:** draft v2 (post-Codex plan review 2026-05-04).
**v1 → v2 changes:** addressed 7 Codex findings (partitioning baseline, institutional backfill identity, refresh atomicity, schema-shape test, #840.E rollback, history dedup contract, ongoing-ingest write-through).
**Spec:** `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md` (Phase 1, §Data model design).
**Author:** Claude (Opus 4.7) on 2026-05-04.

## Goal

Land the schema redesign required by the spec: per-category `_observations` (immutable, append-only) + `_current` (materialised, rebuilt from observations) tables with uniform source-neutral provenance and two-axis dedup (`source` × `ownership_nature`).

## Sub-PR breakdown

The issue is too large for one PR. Split into 6 sub-PRs landing on a single feature branch (`feature/840-schema-unification`) merged to `main` per sub-PR. Each sub-PR is self-contained, reviewable, and reversible.

### #840.A — foundation: provenance shape + insiders observations/current

- New SQL migration adds:
  - `ownership_nature` text-CHECK enum (`'direct' | 'indirect' | 'beneficial' | 'voting' | 'economic'`).
  - `ownership_insiders_observations` table partitioned by `period_end` (range, quarterly).
  - `ownership_insiders_current` table.
- New service module `app/services/ownership_observations.py` providing:
  - `record_insider_observation(conn, *, instrument_id, holder_cik, holder_name, ownership_nature, source, source_document_id, source_accession, source_url, filed_at, period_start, period_end, shares, ingest_run_id)` — INSERT into observations.
  - `refresh_insiders_current(conn, instrument_id)` — deterministic rebuild of `_current` from observations applying source priority + nature axes.
- Backfill migration: rewrites existing `insider_transactions` + `insider_initial_holdings` rows through `record_insider_observation` so the new tables are populated on day one.
- Tests: round-trip (observe → refresh → read), dedup correctness within nature (Form 4 wins direct), no-cross-nature dedup.
- Rollup endpoint NOT yet wired to read from `_current` — that's #840.E. Old read paths stay functional throughout.

### #840.B — institutions observations/current

Same shape as #840.A for institutions. Backfill rewrites `institutional_holdings`.

### #840.C — blockholders observations/current

Same shape. Backfill rewrites `blockholder_filings`. Identity keys on primary `filer_cik` per the #837 fix lessons.

### #840.D — treasury + DEF 14A observations/current

Treasury observations sourced from `financial_periods.treasury_shares` (XBRL DEI / us-gaap). DEF 14A observations sourced from `def14a_beneficial_holdings`.

### #840.E — rewire `/instruments/{symbol}/ownership-rollup` to read from `_current`

`get_ownership_rollup` switches its read path:
- Old: per-source SQL UNIONs against insider_transactions / blockholder_filings / institutional_holdings / def14a_beneficial_holdings + Python dedup.
- New: single read across `ownership_*_current` tables.

This is the highest-risk sub-PR. Strict acceptance:
- Every existing rollup test still passes.
- p95 latency for AAPL <500ms (acceptance #6).

### #840.F — `/instruments/{symbol}/ownership-history` endpoint

New endpoint sourced from `*_observations`. Acceptance: Vanguard AAPL history returns one row per quarter going back as far as we have data.

## Codex findings (v2)

1. **Partitioning baseline.** Existing data has `period_end` going back to 2010s. Quarterly partitions from 2020-01-01 only would fail inserts on legacy backfill. **Fix:** at migration time, scan `MIN(period_end)` from each legacy source and create partitions from that floor through next-quarter +1. Plus a default partition for any escaped row (with audit query at end of migration to assert default partition is empty — fail loud if not).

2. **Institutional backfill identity.** `institutional_holdings` carries `filer_id` (BIGSERIAL FK to `institutional_filers`); the new API takes `filer_cik`. **Fix:** backfill explicitly JOINs `institutional_filers` to resolve filer_id→cik, validates parent rows exist, fails loud on orphans. Document the resolution path in `record_institution_observation` docstring.

3. **`_current` refresh atomicity.** "Delete old + insert new" exposes empty/partial state under concurrent reads. **Fix:** `refresh_<cat>_current(instrument_id)` wraps DELETE + INSERT in a single transaction AND acquires a per-instrument advisory lock (`pg_advisory_xact_lock(hashtext('refresh_<cat>') ## instrument_id)`). UNIQUE index on the natural key on `_current` is non-negotiable — guards against race-induced duplicates if the lock is ever bypassed.

4. **Schema-shape test.** "Drift is visible" is not enough. **Fix:** `tests/test_840_provenance_block_uniformity.py` queries `information_schema.columns` for every `ownership_*_observations` table and asserts each carries the EXACT provenance block columns (name, type, nullability, CHECK constraint on `source`). Fails CI on drift.

5. **#840.E rollback hardening.** Env-var fallback alone insufficient. **Fix:** `OWNERSHIP_ROLLUP_READ_FROM_CURRENT` defaults to `False` even on first deploy. Both code paths run in production initially. Dual-read parity test: `tests/test_840_e_dual_read_parity.py` calls both old and new code paths against AAPL/GME fixtures and asserts identical OwnershipRollup output (same slice categories, same total_shares per slice, same provenance fields, same freshness as_of). Flag flips ON only after operator confirms parity in dev. Old read paths stay live for 1 release cycle minimum so a deploy rollback is single env-var flip, no DB rebuild.

6. **History endpoint must apply dedup over time.** Spec says diff endpoint returns time-bucketed running deduped totals; v1 plan said raw observation history. **Fix:** `/instruments/{symbol}/ownership-history` applies the same source × ownership_nature dedup logic per time bucket, not just `ORDER BY period_end`. One row per `(period_end, ownership_nature)` after dedup. Tests assert Cohen's GME beneficial 13D/A and direct Form 4 BOTH render on the same date — different natures, both surface.

7. **Ongoing ingest write-through.** Sub-PRs A-D land write-side, but if production ingesters keep writing to the old typed tables (insider_transactions etc.) without also recording observations, `_current` goes stale between backfill and #840.E. **Fix:** within each sub-PR (A-D), modify the corresponding ingester to write observations on every new ingest BEFORE the backfill runs. Backfill catches up history; the live ingester keeps `_current` fresh going forward. #840.E flipping reads then sees up-to-date data.

## Provenance block — concrete column shape

Rather than a Postgres composite type (clunky to upsert / index), each `_observations` table embeds the columns directly:

```sql
source                  TEXT NOT NULL CHECK (source IN ('form4','form3','13d','13g','def14a','13f','nport','ncsr','xbrl_dei','10k_note','finra_si','derived')),
source_document_id      TEXT NOT NULL,
source_accession        TEXT,
source_field            TEXT,
source_url              TEXT,
filed_at                TIMESTAMPTZ NOT NULL,
period_start            DATE,
period_end              DATE NOT NULL,
known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
known_to                TIMESTAMPTZ,
ingest_run_id           UUID NOT NULL,
```

A reusable SQL fragment (`docs/sql/provenance_block.sql`?) is overkill for v1 — the column list is short enough to repeat per table, and DDL drift between categories is something we WANT to be visible (any divergence = an explicit decision).

## Two-axis dedup model

Within a single category's `refresh_<cat>_current`:

1. SELECT all observations for the instrument with `known_to IS NULL` (i.e., not superseded — initially every row).
2. Group by `(natural_key, ownership_nature)`.
3. Within each group, pick the highest-priority observation by:
   - `source_priority` (form4=1 < form3=2 < 13d=3 < 13g=3 < def14a=4 < 13f=5 < nport=6 < ncsr=6 < ...)
   - then `period_end DESC` (newer wins)
   - then `filed_at DESC` (final tie-break)
4. Insert / upsert one row per `(natural_key, ownership_nature)` into `_current`.
5. The `_current` table replaces wholesale on each refresh — older rows for the same key are deleted.

Cross-nature: NEVER deduped. A holder with `(direct, form4, 38M)` AND `(beneficial, 13d, 75M)` produces TWO rows in `_current`. The rollup bucketing logic distinguishes them.

## Partitioning strategy

`*_observations` tables partitioned by `period_end` quarterly:
- One partition per quarter going back to 2020-01-01.
- New partitions auto-created via migration on each quarter rollover (cron job).

Defer:
- BRIN vs btree benchmark on `period_end` (waits for #842 N-PORT data).
- Per-issuer sharding (Codex pushed back; not needed yet).

## Migration safety

Each sub-PR's backfill is a one-shot script committed under `scripts/backfill_840_<category>.py`. Idempotent — `record_<cat>_observation` upserts on the natural key with a `WHERE known_to IS NULL` guard.

The old read paths (rollup endpoint reading from `insider_transactions` etc.) stay functional through every sub-PR until #840.E flips the read switch. This means:
- Sub-PRs A-D land write-side only; no operator-visible behaviour change.
- Sub-PR E flips reads in one atomic deploy.
- Sub-PR F adds the new history endpoint.

## Test surface

Each sub-PR includes:
- `test_<category>_observations_record_and_refresh` — round-trip.
- `test_<category>_dedup_within_nature` — same-nature observations collapse to one `_current` row.
- `test_<category>_dual_render_across_natures` — different-nature observations both surface (Cohen-on-GME case for blockholders+insiders).
- `test_<category>_idempotent_refresh` — second refresh = no change.
- Schema sanity: every observations row has `source_document_id NOT NULL`; every `_current` row has a corresponding observations row.

#840.E adds: full rollup integration test covering AAPL/GME with the new read path.

#840.F adds: history endpoint integration test (Vanguard AAPL quarterly).

## Acceptance criteria → sub-PR mapping

1. Spec invariants (provenance + reproducibility) — covered by #840.A foundation + per-category sub-PRs.
2. Two-axis dedup test (Cohen GME dual-render) — covered by #840.C.
3. History endpoint — covered by #840.F.
4. Coverage banner 6 states — covered by #840.E (banner state machine update).
5. Existing tests still pass — every sub-PR runs the full ownership-rollup test suite.
6. <500ms AAPL rollup smoke — covered by #840.E.

## Out of scope

- New ingest categories (Phases 3-6 file separate tickets).
- BRIN benchmark (deferred).
- Per-issuer sharding (deferred).

## Risks + mitigations

| Risk | Mitigation |
|---|---|
| Backfill overruns (44k DEF 14A + 13F holdings) | Per-instrument transactions; bounded chunk size. |
| Read-path regression in #840.E | Run full rollup test suite + p95 latency check; revert flag (env var) so the new read path can be disabled in prod without rollback. |
| Partition cron drift | Use the existing scheduler pattern; create next-quarter partition 30 days in advance. |
| Cross-team unaware of new tables | Tag `_observations` tables with COMMENT ON TABLE pointing at the spec. |

## Order of operations

1. Write this plan.
2. Codex review the plan (mandatory checkpoint per CLAUDE.md before first task).
3. Apply Codex feedback.
4. Ship #840.A (foundation + insiders).
5. Ship #840.B (institutions).
6. Ship #840.C (blockholders).
7. Ship #840.D (treasury + DEF 14A).
8. Ship #840.E (rewire rollup) — full pre-flight + post-flight smoke.
9. Ship #840.F (history endpoint).
10. Close #840 + update memory.

Each sub-PR follows the standard branch / push / Codex pre-push / poll review / merge cycle.
