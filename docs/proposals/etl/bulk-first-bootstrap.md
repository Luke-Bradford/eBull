# Bulk-first bootstrap — extract every CIK we know about from `submissions.zip`

**Status**: Proposal · 2026-05-25 · draft 2 (Codex-1 BLOCKERs folded — 3 BLOCKING + 3 IMPORTANT)
**Owner**: TBD
**Source memos**:
- `docs/data-sources/sec-bulk-archives.md` — full SEC bulk archive catalogue (7700 words, ~16 archives)
- `.scratch/bulk_zip_current_state_data_engineer.md` — end-to-end trace of current S7-S12 + S16 + S27 wiring
- `.scratch/bulk_zip_gap_analysis.md` — ranked inventory of bulk-eligible HTTP walks
**Related tickets**: #1336 (S16 single-source fix — supersede with this broader spec) · #1335 (first-install UX epic)

---

## 1. Problem

S7 `sec_bulk_download` pulls `submissions.zip` (~1.5 GB) at first-install. The archive contains the *complete* SEC EDGAR submissions index — one `CIK<10>.json` per CIK for **every** entity that has ever filed. S8 `sec_submissions_ingest` reads the zip but only emits rows for **issuer** CIKs that have a universe `instrument_id` (`app/services/sec_submissions_ingest.py:257` — `WHERE i.is_tradable = TRUE`). Every non-issuer CIK in the same zip is silently counted into `archive_entries_skipped` (`:285`) and discarded.

Downstream, S16 `sec_first_install_drain` then re-fetches the same data via per-CIK HTTP at the shared SEC 10 req/s budget — for the **exact CIK cohorts already sitting in the local zip**:

- ~8.7k institutional-filer CIKs (post-#1010 cohort bound) → S16 walks these via `_iter_in_universe_subjects` (`app/jobs/sec_first_install_drain.py:245-254`)
- 0 blockholder-filer CIKs today (`blockholder_filers` empty until 13D/G manifest worker writes seeds), but the cohort grows over time — S16 walks these too (`:256-264`)

Run #7 receipts: **S16 = 65 min** of `sec_rate`-lane wall-clock under the lane's max_concurrency=1 cap. Run #8 (in flight at time of writing): S16 at 77 min and still running.

**Codex-1 correction (draft 2)**: Draft 1 also attributed `S14 = 48 min` to filer walks; that was wrong. S14 `sec_submissions_files_walk` ONLY walks issuer CIKs (`app/services/sec_submissions_files_walk.py:106` — `WHERE i.is_tradable=TRUE`) for secondary-page overflow. Its wall-clock is unrelated to filers. S14 is out of scope for this spec — see §11.

**Codex-1 correction (draft 2)**: Draft 1 also referenced "N-PORT trust CIKs (`sec_nport_filer_directory`)" as a cohort to extract from `submissions.zip`. NPORT trusts are NOT iterated by S16 (`_iter_in_universe_subjects` yields issuer + institutional_filer + blockholder_filer only). They're walked elsewhere (S27 `sec_n_csr_bootstrap_drain`). Adding NPORT trust extraction to the bulk path is a separate concern with its own downstream consumer mapping — out of scope here, tracked as follow-up in §11.

The same broad pattern applies to several other bulk archives SEC publishes that eBull does NOT consume today (per `docs/data-sources/sec-bulk-archives.md` §2):

- `ncen` quarterly (~16 MB / q) — fund operating data already extracted from per-filing HTTP in `ncen_classifier.py:472-541`
- Stage 22 `sec_13f_recent_sweep` top-up — already bulk-loaded for older quarters via S10; the current-quarter top-up still HTTP-walks per filer
- Other bulks (`fsds`, `nmfp`, `rr1`, `vip`, `bdc`, `formd`, `rega`, `cf`, `ta`, `fsn`) — out of scope for first-install but listed in `docs/data-sources/sec-bulk-archives.md` for completeness

This spec is the **architectural fix**: widen the S8 universe filter to include all filer cohorts we already track, then chain the secondary opportunities.

## 2. Goals

1. **Bootstrap first-install in ≤ 30-45 min** (down from current ~75-180 min). Critical-path target = sec_rate lane drained in ≤ 15 min instead of the current 60-90 min.
2. **No HTTP round-trip for data already in a local bulk zip.** Make this a structural invariant — codified at the ingester layer, not policy.
3. **Document every SEC bulk archive end-to-end.** Operator + future-author have one place to look up "what's in each zip + how we use it + what the gotchas are". Owned at `docs/data-sources/sec-bulk-archives.md`.
4. **Preserve retention boundaries.** No source's data-retention horizon changes; this is a wall-clock optimisation only.
5. **Preserve eToro-CIK scope discipline.** Continue to NOT walk SEC-wide CIK cohorts that don't relate to the eToro universe. Filer cohorts in scope are limited to `institutional_filers` and `blockholder_filers` — the exact same sets `_iter_in_universe_subjects` (`app/jobs/sec_first_install_drain.py:245-264`) walks today. NPORT trust CIKs from `sec_nport_filer_directory` are NOT in scope for this spec (see §11 — needs `ManifestSubjectType` enum addition).

**Non-goals.** Adopting bulk archives for sources we don't consume (`fsds`, `nmfp`, etc.); changing retention horizons; introducing fund-trust as a new universe entity; replacing steady-state per-CIK polling.

## 3. The cohort widening — S8's `_load_cik_to_instrument` becomes `_load_known_cik_subjects`

**Codex-1 BLOCKER fold**: `ManifestSubjectType` (`app/services/sec_manifest.py:137-143`) is the closed set `issuer | institutional_filer | blockholder_filer | fund_series | finra_universe`. There is no `nport_trust`. The bulk-path filer cohort uses **existing types only** — no schema migration, no enum addition. Concretely:

- `institutional_filer` for both 13F filer CIKs (from `institutional_filers`) — current S16 already writes this subject_type via the HTTP path.
- `blockholder_filer` for 13D/G filer CIKs (from `blockholder_filers`) — current S16 already writes this subject_type.
- NPORT trusts are out of scope here (see §1 + §11).

Today (`app/services/sec_submissions_ingest.py:232-264`):

```python
def _load_cik_to_instrument(conn) -> dict[str, list[tuple[int, str]]]:
    """Returns {cik: [(instrument_id, symbol), ...]} for tradable instruments only."""
    ... WHERE ei.provider='sec' AND ei.identifier_type='cik' AND i.is_tradable=TRUE
```

Proposed replacement (~60 LOC; the existing `_load_cik_to_instrument` becomes `_load_known_cik_subjects`):

```python
from typing import Literal

# Mirrors the closed ManifestSubjectType set; nport_trust deliberately absent
# (see §1 — NPORT trust extraction is a follow-up).
CikRole = Literal['issuer', 'institutional_filer', 'blockholder_filer']

@dataclass(frozen=True)
class CikSubject:
    """One CIK-role binding. Multimap-valued so the same CIK can appear in
    multiple roles (e.g. self-filing fund that's also a 13F filer)."""
    subject_type: CikRole
    subject_id: str             # str(instrument_id) for issuer; cik for filers
    instrument_id: int | None   # only set when subject_type='issuer'
    symbol: str | None          # only set when subject_type='issuer'


def _load_known_cik_subjects(conn) -> dict[str, list[CikSubject]]:
    """Replaces _load_cik_to_instrument. Returns {cik: [CikSubject, ...]}
    for every CIK eBull tracks across issuer + filer cohorts.

    Multimap shape preserves share-class siblings on the issuer side (#1102)
    AND lets a single CIK appear as BOTH issuer + filer (rare; supported).
    """
    out: dict[str, list[CikSubject]] = {}
    # Issuer rows (existing behaviour preserved)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ei.identifier_value, ei.instrument_id, i.symbol
            FROM external_identifiers ei
            JOIN instruments i ON i.instrument_id = ei.instrument_id
            WHERE ei.provider='sec' AND ei.identifier_type='cik'
              AND i.is_tradable=TRUE
        """)
        for cik_raw, iid, sym in cur.fetchall():
            cik = str(cik_raw).zfill(10)
            out.setdefault(cik, []).append(CikSubject('issuer', str(int(iid)), int(iid), sym))
    # Institutional filer rows (new) — same cohort S16's _iter_in_universe_subjects walks
    with conn.cursor() as cur:
        cur.execute("SELECT cik FROM institutional_filers")
        for (cik_raw,) in cur.fetchall():
            cik = str(cik_raw).zfill(10)
            out.setdefault(cik, []).append(CikSubject('institutional_filer', cik, None, None))
    # Blockholder filer rows (new; empty until 13D/G seeds blockholder_filers)
    with conn.cursor() as cur:
        cur.execute("SELECT cik FROM blockholder_filers")
        for (cik_raw,) in cur.fetchall():
            cik = str(cik_raw).zfill(10)
            out.setdefault(cik, []).append(CikSubject('blockholder_filer', cik, None, None))
    return out
```

**Caller signature update** (Codex-1 IMPORTANT fold): `ingest_submissions_archive` at `app/services/sec_submissions_ingest.py:267-271` accepts `cik_to_instrument: dict[str, list[tuple[int, str]]] | None`. Replace with `cik_to_subjects: dict[str, list[CikSubject]] | None` — same shape, richer values. Three callsites:

- `app/services/sec_submissions_ingest.py:281-282` — the default-None branch calls `_load_cik_to_instrument(conn)` → rename to `_load_known_cik_subjects(conn)`
- `app/services/sec_bulk_orchestrator_jobs.py` — sec_submissions_ingest_job wrapper (line lookup TBD) — pass-through, no logic change
- Test fixtures — update mock signatures

**`SubmissionsIngestResult` field addition** (Codex-1 IMPORTANT fold): the dataclass at `app/services/sec_submissions_ingest.py:66-90` carries `archive_entries_seen`, `instruments_matched`, `filings_upserted`, `profiles_upserted`, `parse_errors`, `archive_entries_skipped`. Add: `filer_manifest_rows_upserted: int = 0` (default for back-compat). Bumped by the new filer-path writer in §4.

## 4. The branched per-entry path — S8's `_ingest_one` gets a filer-subject sibling

Today (`app/services/sec_submissions_ingest.py:348-357`):

```python
for instrument_id, symbol in matched_instruments:
    _ingest_one(conn, instrument_id=instrument_id, cik_padded=cik, symbol=symbol, payload=payload, result=result)
```

Proposed:

```python
for subject in matched_subjects:
    if subject.subject_type == 'issuer':
        _ingest_one_issuer(conn, instrument_id=subject.instrument_id, cik_padded=cik,
                           symbol=subject.symbol, payload=payload, result=result)
    else:
        _ingest_one_filer(conn, subject_type=subject.subject_type,
                          subject_id=subject.subject_id, cik_padded=cik,
                          payload=payload, result=result)
```

Where `_ingest_one_filer` (~80 LOC, new):

- Parses the same `payload` dict (submissions.json structure is identical regardless of CIK role)
- For each `(accession_number, form, filed_at, primary_document)` triple in `payload.filings.recent`:
  - Maps `form → source` via `map_form_to_source` (`app/services/sec_manifest.py`)
  - Skips forms outside the filer's expected set per `_FILER_COHORT_FORMS` (see formal list below — `13F-HR` / `13F-HR/A` for institutional, `SC 13D/G` variants for blockholder)
  - Calls `record_manifest_entry(conn, accession_number, cik=cik_padded, form=form, source=source, subject_type=subject_type, subject_id=subject_id, instrument_id=None, filed_at=..., accepted_at=..., primary_document_url=..., is_amendment=is_amendment_form(form))`
  - Bumps `result.filer_manifest_rows_upserted`

The filer-path writer **does NOT touch `filing_events`** — that table is keyed by `instrument_id` and only meaningful for universe issuers. The filer cohort writes directly to `sec_filing_manifest`, which is exactly what S16's HTTP path does today (`app/jobs/sec_first_install_drain.py:355-385`).

**Form-filter rationale per cohort** (Codex-1 IMPORTANT fold — verified against `_FORM_TO_SOURCE` at `app/services/sec_manifest.py:873-929`):

- `institutional_filer`: **`13F-HR`, `13F-HR/A`**. These are the only 13F forms `_FORM_TO_SOURCE` maps (`:894-895`). The historical `13F-NT` (notice-only filer) is NOT mapped — the HTTP path also drops it.
- `blockholder_filer`: **`SC 13D`, `SC 13D/A`, `SC 13G`, `SC 13G/A`, `SCHEDULE 13D`, `SCHEDULE 13D/A`, `SCHEDULE 13G`, `SCHEDULE 13G/A`** (`:885-892`). Both shorthand and legacy long-form variants.

Cohort form-filter constants are NEW — add `_FILER_COHORT_FORMS` keyed by `ManifestSubjectType` in `app/services/sec_manifest.py` next to the existing `_FORM_TO_SOURCE` map:

```python
_FILER_COHORT_FORMS: dict[ManifestSubjectType, frozenset[str]] = {
    "institutional_filer": frozenset({"13F-HR", "13F-HR/A"}),
    "blockholder_filer": frozenset({
        "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A",
        "SCHEDULE 13D", "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A",
    }),
}
```

**Codex-1 NIT fold**: Draft 1 referenced `KNOWN_FILING_AGENT_CIKS` and `_FILER_FORM_ALLOWLIST` as living "in `sec_manifest.py`". Correct location for `KNOWN_FILING_AGENT_CIKS` is `app/providers/implementations/sec_edgar.py:98`. `_FILER_FORM_ALLOWLIST` does NOT exist (draft 1 hallucination — `_FORM_TO_SOURCE` is the closest real construct). Removed both references.

## 5. Downstream stage impact

Once S8 emits manifest rows for filer cohorts, the downstream stages that previously HTTP-walked them collapse to no-op fast-paths.

### S13 `cusip_resolver_post_bulk_sweep` (~7 sec)

No change. S13 reads `unresolved_13f_cusips` populated by S10 (bulk 13F dataset) — independent of the per-CIK submissions walk.

### S14 `sec_submissions_files_walk` — UNCHANGED (Codex-1 BLOCKER fold)

Draft 1 claimed S14 walks filer CIKs and that the bulk path shrinks its cohort. **False.** `app/services/sec_submissions_files_walk.py:106` lists only **issuer** CIKs: `external_identifiers JOIN instruments WHERE i.is_tradable=TRUE`. The 48 min Run-#7 wall-clock on this stage is entirely issuer secondary-page overflow (the long-tail of issuers with >1000 historical accessions), NOT filer-related. S14 already gates no-overflow CIKs at `:217` via a sentinel.

S14 is out of scope for this spec. **Zero wall-clock savings claimed for S14.** Tracked as separate follow-up in §11.

### S15 `filings_history_seed` (~9-10 min today)

Issuer 730-day window backfill, unchanged in this spec. Gap memo §2 #2 proposes a *separate* bulk-first migration (use `master.idx` quarterly walks instead of per-CIK submissions HTTP) — out of scope here; track as follow-up.

### S16 `sec_first_install_drain` (~65 min today)

**Codex-1 BLOCKER fold**: Draft 1 proposed `seed_filer_manifest_from_bulk_submissions` querying by `bootstrap_run_id`. That doesn't fit — `record_manifest_entry` takes no `bootstrap_run_id` arg (`app/services/sec_manifest.py:207-272`) and `sec_filing_manifest` carries no run-id column. There's no lineage to query.

**Revised approach**: gate inside the per-subject loop using a **per-CIK existence check**. The bulk path emits manifest rows BEFORE S16 runs (S16's cap `cik_mapping_ready` from S6 is already satisfied — and §9 P1 + P2 sequencing puts the bulk filer-writer in front of S16's dispatch). So when S16's iterator hits an `institutional_filer` CIK, a single-row SELECT on `sec_filing_manifest` tells us whether bulk has already populated it:

```python
# S16 fast-path extension — replaces the draft 1 lineage idea.
# Mirrors the existing skip_issuer_http gate (sec_first_install_drain.py:318)
# but the gate decision is per-CIK, sourced from manifest existence not bootstrap_run_id.

def _bulk_already_seeded(conn, cik: str, subject_type: ManifestSubjectType) -> bool:
    """True iff bulk path emitted at least one manifest row for this filer CIK."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM sec_filing_manifest
             WHERE cik = %s AND subject_type = %s
             LIMIT 1
            """,
            (cik, subject_type),
        )
        return cur.fetchone() is not None

# Inside _iter_in_universe_subjects loop at app/jobs/sec_first_install_drain.py:340:
if skip_issuer_http and subject.subject_type == 'issuer':
    ciks_skipped += 1
    continue
if subject.subject_type in ('institutional_filer', 'blockholder_filer'):
    if _bulk_already_seeded(conn, cik, subject.subject_type):
        ciks_skipped += 1
        continue
# Otherwise fall through to the existing HTTP fetch path
```

The existence check is a `LIMIT 1` SELECT on `sec_filing_manifest WHERE cik=%s AND subject_type=%s`. Existing indexes (`sql/118_sec_filing_manifest.sql:127`): `idx_manifest_cik(cik, source, filed_at)` (leading column = cik) and `idx_manifest_subject(subject_type, subject_id, form, filed_at)`. The query uses `idx_manifest_cik` for the cik probe; `subject_type` is filtered against the ~5-30 rows per CIK heap-side. **No new composite index needed.** At 12k iterations × ~1ms per probe = ~12s overhead total. Worst case (no bulk seed available, e.g. legacy run): every check returns false, S16 falls back to the existing HTTP path. Safe fallback. If post-launch profiling shows the heap filter is hot, add `idx_manifest_cik_subject(cik, subject_type)` as a small follow-up — out of P2 scope.

**Cohort coverage**: works for `institutional_filer` and `blockholder_filer` — exactly the two cohorts S8's filer-writer emits per §4. NPORT trust CIKs are NOT in `_iter_in_universe_subjects` (`app/jobs/sec_first_install_drain.py:221-264` yields only the 3 cohorts above), so they don't need a fast-path gate.

Expected S16 wall-clock: **65 min → 1-2 min** when bulk path is present (12k existence checks + idempotent S6 issuer fast-path). Legacy fallback: unchanged 65 min.

### S27 `sec_n_csr_bootstrap_drain` — DEFERRED (Codex-1 BLOCKER fold)

S27 walks N-PORT trust CIKs (cohort sourced separately from `sec_nport_filer_directory`). This spec does NOT extract NPORT trust manifest rows from `submissions.zip` because:

1. `ManifestSubjectType` has no `nport_trust` variant — adding one is a schema migration + cross-cutting CHECK constraint update, out of scope for "wall-clock optimisation" framing.
2. `fund_series` exists as a related variant (`app/services/sec_manifest.py:141`) but its semantic scope is fund-share-classes, not trust-CIKs. Conflating would distort the existing manifest taxonomy.

S27's bulk-first migration is a separate spec — see §11 follow-up. Wall-clock saved here: **zero**.

### Net critical-path savings (revised)

**~55-60 min on the `sec_rate` lane**, all attributable to S16 (filer-path fast-path). S14 unchanged. S27 deferred. Tier 1 floor drops from ~75-90 min → ~15-30 min for the `institutional_filer` + `blockholder_filer` cohort path. Other long-pole stages (issuer secondary pages via S14, 13F sweep via S22, fundamentals sync via S25) ride on their existing optimisations.

## 6. eToro CIK scope rules

The operator's invariant: *"we still just want what the eToro CIK list provides"*. Map onto the cohort sources in scope:

| Cohort | In bulk path? | Reason |
|---|---|---|
| Issuers (`is_tradable=TRUE` ∩ `external_identifiers.provider='sec'`) | YES | Direct eToro universe — ~5,105 CIKs today |
| `institutional_filers` | YES | Already tracked because they hold issuers in our universe. Cohort recency-bounded via `last_13f_hr_at` filter (#1010) — *not* holdings-overlap-bounded. ~8.7k post-bound. |
| `blockholder_filers` | YES | Tracked because they file 13D/G against our issuers. Cohort currently empty pending 13D/G manifest worker output. |
| `sec_nport_filer_directory` (NPORT trusts) | **NO — deferred** | Requires `ManifestSubjectType` enum addition (`nport_trust` does not exist today, `:137-143`). Tracked as own spec in §11. ~3-4k cohort. |
| Random SEC CIKs outside above sets | NO | Skipped at `_load_known_cik_subjects` dictionary lookup. Same skip-and-continue path that handles the universe-gap case today. |

**Risk** (gap memo §2 #1): "13F filers who hold instruments in eToro universe but aren't themselves in it" — tightening to holdings-overlap creates a chicken-and-egg with bootstrap. Recommendation: keep the looser recency-only cohort definition (matches HTTP-path scope today) — the bulk path mirrors HTTP scope exactly. If post-launch we want to tighten cohort to holdings-overlap, the same trim applies symmetrically to both paths.

## 7. Retention boundaries — unchanged

The bulk path is a wall-clock optimisation, NOT a retention-policy change. Per-source retention floors (current-state memo §4) stay where they live today:

| Source | Retention floor | Where it lives | Bulk-path behaviour |
|---|---|---|---|
| Filings (filing_events / sec_filing_manifest) | 10 years | `app/services/sec_manifest.py` retention helpers | Bulk emits manifest rows; existing retention helpers prune on parse |
| XBRL facts (financial_facts_raw) | 20 years (loosest cap) | `app/services/fundamentals/__init__.py:_within_retention` | Already-bulk; unchanged |
| 13F | 8 quarters bootstrap, full history nightly | StageSpec params `bootstrap_orchestrator.py:1126-1149` | Bulk-first widens institutional_filer cohort coverage at S8 (this spec); horizon unchanged |
| N-PORT | 8 quarters | StageSpec params | **Unchanged in this spec** — NPORT trust extraction from bulk submissions.zip is deferred (§11) |
| Form 4 | 3 years | `app/services/manifest_parsers/sec_form4.py:_within_retention` | Already-bulk via S11 `sec_insider_ingest_from_dataset`; unchanged |
| Form 5 | 18 months | sibling parser | Unchanged |
| N-CSR | 730 days | `app/services/manifest_parsers/sec_n_csr.py:n_csr_retention_cutoff` | **Unchanged in this spec** — S27 N-CSR drain still HTTP-walks NPORT-trust accessions; deferred (§11) |
| Form 3 | unbounded (initial-holdings only) | parser | Unchanged |

**No new retention constants ship with this spec.** If/when a follow-up tightens horizons, it lands in the existing parser-side `_within_retention` modules — the bulk consumer doesn't care.

## 8. Document every source — `docs/data-sources/sec-bulk-archives.md`

Already written (~7700 words, sibling deliverable from this session). H2 per archive, greppable. Covers:

- **Currently consumed (5)**: `submissions.zip`, `companyfacts.zip`, `form13f`, `nport`, `form345`
- **Eligible-but-not-consumed (11)**: `ncen`, `fsds`, `nmfp`, `rr1`, `vip`, `bdc`, `formd`, `rega`, `cf`, `ta`, `fsn`
- **Out of scope (1)**: EDGAR log files (discontinued 2017-06)

Per archive: canonical URL, refresh cadence, format, size, coverage, primary keys, dedup rules, update semantics, gotchas, eBull current use, gap status.

**Operator usage**: when adding/auditing a SEC source, read this doc FIRST to confirm bulk-eligibility. If yes, use the bulk-first writer pattern in §4. If no, the per-source spec at `docs/etl/sources/<name>.md` documents the HTTP path.

## 9. Implementation phases

Smallest-first sequencing. Each phase is shippable independently.

| Phase | Scope | Estimate | Saves |
|---|---|---|---|
| **P1 — cohort widening + filer writer in S8** | §3 `_load_known_cik_subjects` + §4 `_ingest_one_filer` + `_FILER_COHORT_FORMS` constants + `SubmissionsIngestResult.filer_manifest_rows_upserted`. ~160 LOC across `sec_submissions_ingest.py` + `sec_manifest.py`. | ~1.5 days | 0 (writer only; S16 still HTTP-walks until P2) |
| **P2 — S16 fast-path extension** | §5 `_bulk_already_seeded` per-CIK existence check + skip-when-true gate. ~30 LOC `sec_first_install_drain.py`. Uses existing `idx_manifest_cik(cik, source, filed_at)` — no new index needed. | ~0.5 day | **~55-60 min** on critical path (S16 65min → 1-2min) |
| **P3 — `data-sources/sec-bulk-archives.md` adoption** | Wire the doc into the per-source spec contract (`docs/etl/sources/README.md`) so future SEC sources MUST consult the catalogue before designing the fetch path. ~30 LOC doc + lint script. | ~0.5 day | 0 (process gate) |
| **P4 — deferred follow-ups** | Per gap memo §2 #2-#6: `master.idx` 730d backfill (S15) ~6min; N-CEN classifier when wired (#1313) ~20min; 13F top-up ~5-10min; S14 issuer-pages cohort tightening ~variable; S27 NPORT-trust extraction (requires `ManifestSubjectType` enum migration). Each ~80-150 LOC. | ~2-3 days | ~10-25 min cumulative (each follow-up its own ticket) |

**~2-3 days end-to-end for P1+P2** delivering ~95% of the wall-clock win (~55-60 min on critical path). P3 is process discipline. P4 is a backlog of follow-up wins, each separately ticketed.

## 10. Risks

1. **Filer-vs-issuer subject_type disambiguation.** A single CIK can appear in BOTH `institutional_filers` AND have a tradable instrument. The multimap design in §3 handles this — each role emits its own writer branch — but the test surface needs to exercise the multi-role case. Mitigation: integration test with a known multi-role CIK (e.g. a self-managing asset manager whose ADR trades).
2. **Bulk-zip refresh lag.** `submissions.zip` is refreshed nightly (per `docs/data-sources/sec-bulk-archives.md` §1.1). Amendments accepted intraday won't show until next refresh. Already-handled by Layer 1/2/3 steady-state Atom feed for current-day filings — verify the bulk path doesn't suppress in-flight amendments by re-checking the manifest UPSERT semantics: `record_manifest_entry` ON CONFLICT preserves any in-flight `ingest_status` (already documented behaviour, used by S16's HTTP path today).
3. **Manifest row volume.** P1 adds ~6k new manifest rows per filer × ~30 accessions / filer = ~180k new rows in `sec_filing_manifest`. These are exactly the rows S14/S16 would have INSERTed anyway from per-CIK fetches. **Net manifest row cost = zero.** Partition state unchanged (`sec_filing_manifest` is already partitioned by `filed_at` range per sql/177-equivalent).
4. **Cohort scope drift.** If `institutional_filers` cohort grows significantly (recency window relaxes), bulk-path manifest writes scale linearly. Bound today: ~8.7k institutional_filer CIKs + ~0 blockholder CIKs (empty until 13D/G manifest worker outputs). Even 4× = ~35k CIKs is still bounded by zip walk throughput, not by SEC budget. No new bottleneck. NPORT trust cohort scaling is unrelated to this spec (deferred §11).
5. **Form-filter false negatives.** If a filer cohort files a form NOT in `_FILER_COHORT_FORMS` (e.g. a 13F filer also files a 10-K as an issuer), the bulk path would skip it. Mitigation: a CIK that's both filer + issuer hits BOTH writers (the multimap exposes both roles), so the issuer-path picks up the 10-K and the filer-path picks up the 13F. Edge case: a CIK in `institutional_filers` that's NOT in the issuer universe but files a non-13F form — that form gets dropped by the bulk path. Acceptable: the HTTP path today also doesn't pick it up (the cohort-iter in `_iter_in_universe_subjects` is form-agnostic but the downstream manifest worker filters by `_FORM_TO_SOURCE` map).

## 11. Out of scope (each gets its own follow-up ticket)

**Codex re-verification post-draft 4** (2026-05-25 evening): the agent-written gap memo classified S19/S20/S23/S25 as "no-ops post-bulk". Direct reading of `app/workers/scheduler.py` invokers showed this is wrong — all three open `SecFilingsProvider` HTTP clients and actively fetch. Memo over-classified. Filed as follow-ups:

- **S19 `sec_insider_transactions_backfill`** — `app/workers/scheduler.py:5495-5526`. Round-robin tail-cohort backfill (25 instruments × 50 oldest filings / tick) for instruments with deep Form 4 backlogs past the 8-quarter bulk window. Bulk source: `insider-transactions-data-sets/*_form345.zip` (S11). Question for the follow-up: does the bulk insider dataset retention horizon cover everything S19 would HTTP-fetch? If yes, S19 is fully redundant with S11; if no, the cohort that bulk-misses is real tail history (decade+ old Form 4s). ~5-15 min wall-clock saved when wired. Own ticket.
- **S20 `sec_form3_ingest`** — `app/workers/scheduler.py:4233-4262`. Daily Form 3 ingest. Bulk source: `form345` includes Form 3 (the "3" in 345 is Form 3). Likely fully-redundant with S11 bulk pass. ~5-15 min wall-clock saved when wired. Own ticket.
- **S23 `sec_n_port_ingest`** — `app/workers/scheduler.py:5305-5400`. Walks `sec_nport_filer_directory` cohort + HTTP-fetches each trust's pending NPORT-P accessions. Same trust cohort as S27. Bulk source: `nport quarterly` (S12). Partial redundancy: S12 loads current quarter; S23 fills gaps for trusts with deep history. ~5-10 min wall-clock saved when wired. Own ticket.
- **S25 `fundamentals_sync`** — already-bulk via S9 companyfacts.zip + 120d-cadence per-CIK API top-up. Ran ~12 min on Run #8 — much less wasteful than the 3 above. Low priority; if optimised, would land via tighter top-up cadence not bulk reuse.
- **NPORT trust manifest extraction from bulk submissions.zip.** Requires `ManifestSubjectType` enum addition (`nport_trust`) + cross-cutting SQL CHECK constraint migration + S27 fast-path. ~5-8 min wall-clock saved when wired. Own spec.
- **S14 secondary-pages cohort tightening.** S14 walks issuer secondary pages — unrelated to filer bulk-path. The 48 min Run-#7 wall-clock is its own optimisation problem (cohort tightening + parallelism). Own spec.
- **S15 `filings_history_seed` master.idx 730d backfill** (gap memo §2 #2). ~6 min saved. ~80 LOC. Own ticket.
- **S22 `sec_13f_recent_sweep` current-quarter top-up** via bulk. ~5-10 min saved when bulk and HTTP are both correct. Own ticket.
- **N-CEN classifier when wired (#1313)** — saves ~20 min once a production caller exists. Own ticket.
- **New bulk archives** (`fsds`, `nmfp`, `rr1`, etc.). Documented in `docs/data-sources/sec-bulk-archives.md` for completeness. No active consumer demand.
- **Retention horizon changes**. Bulk-first preserves every per-source retention floor.
- **Cohort scope tightening** (e.g. holdings-overlap-bounded institutional_filers). Would also tighten HTTP path symmetrically.
- **Steady-state Atom / per-CIK polling**. Unchanged.
- **DERA `financial-statement-and-notes-data-sets` text-block adoption**. New capability for thesis-engine evidence; out of this spec.
- **Form ADV adoption**. Lives at separate host (`adviserinfo.sec.gov`); requires its own provider client. Defer.

## 12. Open questions

1. Should the filer-cohort manifest writer use the **same** `record_manifest_entry` helper S16 uses, or a dedicated bulk-path variant? Recommend: same helper — consistency + UPSERT idempotency guarantees the rerun-safe property.
2. Should `KNOWN_FILING_AGENT_CIKS` (the cross-cutting filing-agent list at `app/providers/implementations/sec_edgar.py:98`) be referenced by the bulk path's filer-CIK lookup? Recommend: YES — explicit skip inside `_ingest_one_filer` (`if cik in KNOWN_FILING_AGENT_CIKS: return`). The check currently lives inside `refresh_cik_sidecar` at `sec_submissions_ingest.py:151` and applies only on the sidecar/issuer path — the new filer writer must add the equivalent guard, NOT inherit it implicitly. Filing-agent CIKs file paperwork on behalf of others (they ARE the filer at the protocol level) so they would otherwise be incorrectly emitted as institutional_filer manifest rows.
3. Where does the `_FILER_COHORT_FORMS` map live? Recommend: next to the existing `_FORM_TO_SOURCE` map in `app/services/sec_manifest.py`. Single source of truth for cohort-form mapping.
4. Should P1 land BEFORE the #1335 UX epic's P1 progress telemetry? Recommend: **yes, P1 of bulk-first lands first**. Cuts critical path immediately; first-install UX is built on faster-baseline assumptions.

## 13. References

- `docs/data-sources/sec-bulk-archives.md` — bulk archive catalogue (7700w)
- `.scratch/bulk_zip_current_state_data_engineer.md` — current-state trace
- `.scratch/bulk_zip_gap_analysis.md` — gap inventory + ranked opportunities
- `.claude/skills/data-sources/sec-edgar.md` — SEC endpoints + identifiers
- `.claude/skills/data-engineer/SKILL.md` — schema invariants
- `app/services/sec_submissions_ingest.py:232-264` — current `_load_cik_to_instrument`
- `app/jobs/sec_first_install_drain.py:221-264` — S16 cohort iterator
- `app/services/sec_manifest.py` — form-to-source map, record_manifest_entry, cohort helpers
- Tickets: #1336 (S16 single-source fix — supersede with this) · #1335 (first-install UX epic)
- Memory: `project_1233_run7_receipts.md` (S14 48m / S16 65m baseline) · `project_1233_run2_measurement.md` (S8 issuer-only timing)
