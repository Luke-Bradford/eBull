# PR11 — SEC Schedule 13D / 13G activation + 3y retention cap design

> Created: **2026-05-21** during PR11 brainstorming under #1233 (data-retention rubric umbrella).
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
> Parent spec: `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.8.
>
> Status: **REVISED v4 post-Codex-1c** — Codex 1a (3B + 4H + 3M) + Codex 1b (2B + 3H + 2M + 1L) + Codex 1c (2B + 1H + 3L) findings folded in. Two operator scope decisions also folded (2026-05-21): (i) adopt `edgartools.Schedule13D.parse_xml` / `Schedule13G.parse_xml` for the XML parse path; (ii) align retention floor with SEC's XBRL mandate effective date `2024-12-19` (no library covers pre-mandate HTML; cap = `max(today - 3y, 2024-12-19)` is honest 100%). Pending Codex 1d spec review, then operator sign-off, then implementation plan.

## 0. Why this design exists

PR11 sits inside the #1233 data-retention rubric as **§4.8 13D/G blockholders** — described in the parent spec as "activate dormant pipeline + 3y historical + current-state cap at the parser." The parent spec's volume estimate (`0 ingested; pipeline not yet active`) is materially correct: every code surface (schema, manifest parser, observations sync, API endpoint, tests) is wired and exercised by unit tests, but **no discovery path enqueues universe-issuer-relevant SC 13D/G filings into `sec_filing_manifest`**, so the worker never fires and the observations / current tables stay empty universe-wide.

The chicken-and-egg root cause (audited 2026-05-21):

1. SEC SC 13D/G is filed BY the 5%+ holder, NOT by the issuer. AAPL's `data.sec.gov/submissions/CIK0000320193.json` does NOT contain SC 13D filings against AAPL.
2. Existing daily-index reconcile (`app/jobs/sec_daily_index_reconcile.py`) catches SC 13D/G by filer CIK and tries to resolve subject via `default_subject_resolver` (`app/jobs/sec_atom_fast_lane.py:57-101`), which checks `blockholder_filers` (auto-populated by parser on first successful ingest — chicken-and-egg with no bootstrap seed) + `blockholder_filer_seeds` (operator-curated, **currently empty universe-wide**).
3. With both tables empty on bootstrap, the resolver returns no subject; daily-index discovery silently drops the row; worker is never invoked; `blockholder_filers` is never auto-populated; the bootstrap path is forever dormant.

PR11 unblocks this by adding a **universe-issuer-CIK-driven discovery path** (no hardcoded filer seed list, per operator mandate "the data we pull is dictated based on what instruments eToro provide; everything else should populate downstream of that as a filter") + a 3y `filed_at` cap at every writer chokepoint following the PR5–PR8 placement-invariant lint-guard pattern + a same-PR retirement of the dormant filer-seed code path so no tech debt is left behind.

## 1. Mandate

This spec is delivered under the following operator-stated constraints (2026-05-21):

1. **100% complete in PR11** — no follow-up tickets for in-scope work, no "we'll come back to it later".
2. **No tech debt** — dormant code paths retired in the same PR that ships the live one.
3. **No hardcoded lists** — the data we pull is dictated by what eToro provides; the SEC universe filters downstream of `instruments.country='US' AND is_tradable=TRUE` (PR1, settled).
4. **Sensible depth** — 3y per parent spec §4.8; no "century of data".

These constraints are load-bearing for the design calls below.

## 2. Audit baseline

### 2.1 Code surface as of 2026-05-21

| Layer | File:line | Status |
|---|---|---|
| Schema — filer registry | `sql/095_blockholder_filers_filings.sql:71` | LIVE (auto-populated by parser on successful ingest) |
| Schema — raw chain | `sql/095_blockholder_filers_filings.sql:78` | LIVE (one row per reporter-per-accession; populated by live manifest parser) |
| Schema — seed table | `sql/096_blockholder_filer_seeds_and_log.sql:30` | DORMANT (empty universe-wide; read by dormant ingester) |
| Schema — attempt log | `sql/096_blockholder_filer_seeds_and_log.sql:63` | LIVE (written by both live manifest parser + dormant ingester) |
| Schema — observations | `sql/115_ownership_blockholders_observations.sql:27` | LIVE (append-only; written by manifest parser via `_record_13dg_observation_for_filing`) |
| Schema — current snapshot | `sql/115_ownership_blockholders_observations.sql:102` | LIVE (write-through via `refresh_blockholders_current`) |
| Provider | `app/providers/implementations/sec_13dg.py:455` | LIVE (`parse_primary_doc()` — pure XML→BlockholderFiling) |
| Manifest parser | `app/services/manifest_parsers/sec_13dg.py:95` | LIVE (registered to `sec_13d` + `sec_13g`; uses lower-level helpers from `blockholders.py`) |
| Service — live helpers | `app/services/blockholders.py` (`_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`, `_archive_file_url`, `_record_ingest_attempt`, etc.) | LIVE (used by manifest parser) |
| Service — dormant entrypoints | `app/services/blockholders.py:869` (`ingest_all_active_filers`), `:804` (`ingest_filer_blockholders`) | DORMANT (per-filer walker; reads seed table; never called in production) |
| Observation sync | `app/services/ownership_observations_sync.py:414` (`sync_blockholders`) | LIVE (called by `sync_all`); pre-existing path may need cap gate per PR11 |
| API endpoint | `app/api/instruments.py:3563` (`GET /{symbol}/blockholders`), `:4054` (rollup category) | LIVE (renders empty for every instrument today) |
| Bootstrap stage | `app/workers/scheduler.py` `_BOOTSTRAP_STAGE_SPECS` | **MISSING — no 13D/G bootstrap stage exists** |
| Scheduler cron stage | `app/workers/scheduler.py` | **MISSING — no scheduled 13D/G discovery job** |
| Atom fast-lane resolver | `app/jobs/sec_atom_fast_lane.py:57-101` | LIVE (drops 13D/G when filer unknown — gap above) |
| Daily-index reconcile | `app/jobs/sec_daily_index_reconcile.py:73-99` | LIVE (drops 13D/G when filer unknown — same gap) |
| Tests | `tests/test_sec_13dg_parser.py` (443 LOC), `tests/test_manifest_parser_sec_13dg.py` (627 LOC), `tests/test_blockholders_ingester.py` (955 LOC), `tests/test_api_blockholders.py` (516 LOC) | Mixed — parser + manifest + API tests stay live; ~50% of ingester tests exercise dormant entrypoints (retire in PR11) |
| Operator ingest rebuild | `app/api/operator_ingest.py:312` | LIVE entry in payload allow-list (already exposes `blockholder_filings` for `POST /jobs/sec_rebuild/run`) |

### 2.2 Universe baseline (post-PR1)

```text
universe   = 12,417 instruments
US filers  =  5,174 (country='US' + is_tradable=TRUE + has primary CIK in external_identifiers)
```

PR11's discovery cohort is the 5,174 US-tradable issuers. The same filter is applied at the discovery seed (no implicit dependency on PR1's lint guard; the SELECT here is explicit + grep-friendly for the PR11 lint guard).

### 2.3 Empirical SEC API verification (2026-05-21)

Run before this spec was written, per "verify before asserting":

```text
$ curl -sS -H "User-Agent: eBull luke.bradford@hotmail.co.uk" \
    "https://efts.sec.gov/LATEST/search-index?q=&forms=SC%2013D&ciks=0001326380&dateRange=custom&startdt=2023-05-21&enddt=2026-05-21"
{ hits.total.value = 3, hits.hits = [
    SC 13D/A 2024-06-11 0000921895-24-001394 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
    SC 13D/A 2024-05-24 0001193805-24-000707 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
    SC 13D/A 2023-06-13 0000921895-23-001480 ciks=['0001326380','0001822844'] names=['GameStop Corp. (GME)', 'RC Ventures LLC'],
] }

$ curl ... "...forms=SC%2013G&ciks=0001326380" (no date filter)
{ hits.total.value = 110, hits.hits[0:3] = [
    SC 13G/A 2024-02-13 ... names=['GameStop Corp. (GME)', 'VANGUARD GROUP INC'],
    SC 13G/A 2024-01-26 ... names=['GameStop Corp. (GME)', 'BlackRock Inc. (BLK)'],
    ...
] }
```

Confirmed:

- `efts.sec.gov/LATEST/search-index` is a **public JSON endpoint** (no auth, returns JSON, honours `User-Agent`). Same back-end as the EDGAR full-text-search UI at `efts.sec.gov`.
- `ciks={cik}` param matches **any CIK on the filing** — for SC 13D/G this includes the subject (issuer) AND the filer (5%+ holder). For our universe-CIK seed, the issuer is always `ciks[0]` (first position is the queried CIK) and the filer CIK(s) are `ciks[1:]`.
- `dateRange=custom&startdt=YYYY-MM-DD&enddt=YYYY-MM-DD` filters server-side on `file_date` — this is the 3y cap chokepoint A (discovery query).
- Default page size = 100. Active-blockholder-heavy issuers (e.g. GME has 110 SC 13G all-time) may need pagination via `&from={N}&size=100`. With the 3y window, the median issuer fits one page; long-tail outliers may need 2-3 pages.
- `display_names[]` carries entity labels for auto-seeding `blockholder_filers` without a follow-up fetch.

This endpoint is the load-bearing discovery primitive for PR11.

## 3. Design

### 3.1 Discovery layer (NEW)

**New file**: `app/services/sec_13dg_discovery.py`

**Manifest schema constraints (verified against [sql/118_sec_filing_manifest.sql] + [app/services/sec_manifest.py:194-231])** — load-bearing for every design call below:

- `accession_number` is the **sole PK**. Joint 13D/G filings (one accession, multiple reporters) CANNOT be enqueued as multiple rows — one row per accession.
- CHECK constraint enforces `(subject_type='issuer' AND instrument_id IS NOT NULL) OR (subject_type<>'issuer' AND instrument_id IS NULL)`. A `subject_type='blockholder_filer'` row **MUST have `instrument_id = NULL`** — the issuer linkage cannot ride along on the manifest row directly.
- `cik` is `NOT NULL TEXT`. For `subject_type='blockholder_filer'` rows it is the **filer CIK**.
- `subject_id` is the canonical identity for the subject_type (filer CIK for blockholder_filer).
- `record_manifest_entry` raises `ValueError` on either CHECK violation at the Python layer ([sec_manifest.py:223-231]).

These constraints make `subject_type='blockholder_filer'` the only schema-legal choice for 13D/G manifest rows (consistent with the existing live daily-index path at [sec_atom_fast_lane.py:57-101] + [manifest_parsers/sec_13dg.py:28-35]). Issuer linkage continues to be resolved at parse-time via CUSIP→instrument lookup in `external_identifiers`, exactly as today — PR11 does NOT change parser semantics for issuer resolution. The silent CUSIP-unresolved gap is by-design at the schema layer (audit trail preserved with `instrument_id=NULL` per [manifest_parsers/sec_13dg.py:31-35]) and tracked by #740 (CUSIP backfill epic); PR11 inherits that contract.

**Responsibilities**:

1. Read the universe-issuer-CIK cohort via the canonical SELECT (post-PR1):

   ```sql
   SELECT DISTINCT i.instrument_id, ei.identifier_value AS cik
   FROM instruments i
   INNER JOIN external_identifiers ei
       ON ei.instrument_id = i.instrument_id
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
   WHERE i.country = 'US'
     AND i.is_tradable = TRUE
   ORDER BY i.instrument_id
   ```

2. For each issuer CIK, page through `efts.sec.gov/LATEST/search-index` with:
   - `forms = SC 13D,SC 13D/A,SC 13G,SC 13G/A` (URL-encoded as `SC%2013D,SC%2013D%2FA,SC%2013G,SC%2013G%2FA`)
   - `ciks = {zero-padded 10-digit issuer CIK}`
   - `dateRange = custom`, `startdt = _resolve_discovery_startdt(...)`, `enddt = today.isoformat()` — see §3.5 for steady-state startdt derivation
   - `from = 0; size = 100`; advance `from` by 100 until `len(hits) < size`

3. For each `hit._source`, defensively parse the filer set without positional assumptions (Codex 1a MEDIUM #8):
   - Extract `accession = adsh`, `form = form`, `file_date = file_date`, `cik_list = ciks` (raw, zero-padded 10-digit), `name_list = display_names`
   - **Defensive filer extraction**: `filer_ciks = [c for c in cik_list if c.lstrip('0') != issuer_cik_unpadded]` — no positional assumption about issuer index, tolerates issuer not-in-position-0 + duplicate CIK entries. **Cardinality assertions**: if `len(filer_ciks) == 0` → log + skip (issuer-only result, anomalous); if `len(filer_ciks) == 1` → standard case; if `len(filer_ciks) >= 2` → joint filing, all filer CIKs are seeded into `blockholder_filers`.
   - **Archive-owner CIK derivation (REVISED v4 per Codex 1c BLOCKING #1)**: the manifest's `cik` field MUST be a value the parser can pass to `_archive_file_url(cik, accession, "primary_doc.xml")` and get back a valid SEC archive path. The v3 spec proposed using the accession-number prefix CIK; Codex 1c correctly caught that this is WRONG — `sec_edgar.py:83-104` enumerates `_KNOWN_FILING_AGENT_CIKS` (including `0001193125` Donnelley, `0001437749` Edgar Agents, `0001571049` DFIN, `0001185185` Workiva) for whom the accession-number prefix is the agent's CIK but the archive directory lives under the issuer or filer CIK — accession-prefix-as-archive 404s for these. Apple's 13G/A `0001193125-24-036431` IS Donnelley-submitted; using prefix would 404.
   - **Correct derivation**: pick the first CIK in `ciks[]` that is NEITHER the issuer NOR a known agent: `archive_owner_cik = next((c for c in cik_list if c.lstrip('0') != issuer_cik_unpadded and c not in _KNOWN_FILING_AGENT_CIKS), None)`. If `None` (rare — should only occur if the only non-issuer CIK is an agent), fall back to issuer CIK (always in `ciks[]`; empirically verified to serve the same archive directory). The chosen CIK becomes manifest `cik` + `subject_id`.
   - **Why this works**: `_KNOWN_FILING_AGENT_CIKS` exists precisely because SEC archives are mounted under the REPORTING entity's CIK (issuer for 10-K, filer for 13F/13D), not the SUBMITTER's CIK. Empirical confirmation: AAPL `0001193125-24-036431` (Donnelley-submitted) — directory served under issuer-CIK 320193 and filer-CIK 1067983 (200), but accession-prefix 1193125 was tested above as 404 on `primary_doc.xml` (the dir returns 200 because directory listings are mounted under multiple CIKs as redirects).
   - **Auto-seeding `blockholder_filers`**: only seed CIKs that are NEITHER issuer NOR known agent. Filing agents are infrastructure, not reporters; seeding them into the resolver table would pollute future resolver lookups + violate the table semantic. The 5%+ reporting persons surface in the parser's per-XML expansion downstream regardless.
   - **Defensive name extraction**: `display_names[]` is positionally aligned with `ciks[]` per empirical observation. For each filer CIK, map to its name via `name_list[cik_list.index(filer_cik)]` rather than `name_list[1]`. If the name doesn't parse cleanly (rare bracketed-LLC labels), fall back to `f"CIK {filer_cik}"` — the resolver only joins on CIK; name is operator-visible label only.
   - For each filer CIK + its display name: UPSERT `blockholder_filers (cik, name)` via `_upsert_filer` from `app/services/blockholders.py` (idempotent ON CONFLICT (cik) DO UPDATE name). The archive-owner CIK is ALSO seeded (it may not be in `filer_ciks` — filing agents typically aren't reporting persons).
   - INSERT into `sec_filing_manifest` via `record_manifest_entry` with:
     - `subject_type = 'blockholder_filer'`
     - `subject_id = archive_owner_cik` (first non-issuer, non-agent CIK from `ciks[]` per the derivation above)
     - `cik = archive_owner_cik` (same — schema requires `cik NOT NULL`; for blockholder_filer this is the resolver lookup AND the archive-owner identity)
     - `instrument_id = None` (per CHECK constraint)
     - `source = 'sec_13d'` if form starts with `SC 13D` else `'sec_13g'`
     - `accession_number = adsh`
     - `filed_at = file_date` parsed at UTC midnight
     - `primary_document_url = None` (parser rebuilds canonically via `_archive_file_url`)
     - `status = 'pending'`
   - ON CONFLICT on PK `accession_number`: existing row stays (matches `record_manifest_entry`'s ON CONFLICT DO UPDATE pattern). Re-discovery is idempotent.

4. Issuer linkage propagation (NEW for PR11, addresses Codex 1a BLOCKING #2; revised for share-class siblings per Codex 1b BLOCKING #2):
   - At discovery time we know `(issuer_instrument_id, issuer_cik)` from the universe walk seed.
   - **Share-class sibling handling**: this repo allows multiple instruments to share one SEC CIK ([sql/099_unresolved_13f_cusips.sql:60], [sql/103_instrument_symbol_history.sql:8]) — examples: Alphabet GOOG + GOOGL on CIK 1652044; Berkshire BRK.A + BRK.B on CIK 1067983. The discovery query `SELECT DISTINCT instrument_id, cik` returns ONE ROW PER SIBLING for these issuers. The same accession is therefore discovered N times (once per sibling) when N share the issuer CIK.
   - PR11 writes the mapping into a NEW side-table `sec_13dg_discovery_issuer_hint` keyed on `(accession_number, instrument_id) PRIMARY KEY` (multi-row per accession), NOT on `accession_number` alone. Each (accession, sibling) pair gets its own hint row.
   - Parser strategy (REVISED — Codex 1b BLOCKING #2): the parser must NOT use the hint to bypass CUSIP resolution outright (a bypass would route the SC 13D against GOOG-A onto the GOOGL-C sibling). Instead, the hint acts as a **universe-membership cross-validator**:
     1. Parse the XML, extract `filing.issuer_cusip`.
     2. Resolve `filing.issuer_cusip` to `instrument_id_from_cusip` via `_resolve_cusip_to_instrument_id` (today's path).
     3. Load the hint set: `hint_ids = SELECT instrument_id FROM sec_13dg_discovery_issuer_hint WHERE accession_number = ?`.
     4. **CASE A (happy)**: `instrument_id_from_cusip in hint_ids` → confirmed universe-relevant; write observation with `instrument_id_from_cusip`. This is the typical case and preserves share-class correctness via CUSIP.
     5. **CASE B (CUSIP unresolved + 1 hint)**: `instrument_id_from_cusip is None and len(hint_ids) == 1` → use the single hint as fallback (closes the silent CUSIP-unresolved gap for single-class issuers).
     6. **CASE C (CUSIP unresolved + N>1 hints)**: `instrument_id_from_cusip is None and len(hint_ids) > 1` → ambiguous share-class case; write `instrument_id=NULL` with `blockholder_filings_ingest_log.error="cusip_unresolved_with_ambiguous_hint"` for operator audit. Existing #740 backfill will retroactively resolve this once the CUSIP→instrument mapping lands in `external_identifiers`.
     7. **CASE D (CUSIP resolved but NOT in hints)**: `instrument_id_from_cusip is not None and instrument_id_from_cusip not in hint_ids` → discrepancy log + still write with `instrument_id_from_cusip` (CUSIP is more specific than hint; hint may be a stale universe entry — e.g. instrument was delisted between discovery and parse).
     8. **CASE D (CUSIP resolved but NOT in hints) — REVISED v4 per Codex 1c HIGH universe-scope leak**: `instrument_id_from_cusip is not None and instrument_id_from_cusip not in hint_ids`. The v3 spec wrote with `instrument_id_from_cusip` and logged a discrepancy. Codex 1c correctly caught that this leaks outside the current tradable universe — if CUSIP resolves to a delisted or non-US sibling on a shared CIK, PR11 writes an observation against an instrument that violates the §6.1/§6.2 universe filter. **Corrected v4 behaviour**: cross-check `instrument_id_from_cusip` against the current universe (`SELECT 1 FROM instruments WHERE instrument_id = ? AND country = 'US' AND is_tradable = TRUE`). If the CUSIP-resolved instrument IS in the current universe → write with that `instrument_id` + log discrepancy with hint_ids (the hint may simply be stale — e.g. universe re-sync removed an instrument between discovery and parse). If the CUSIP-resolved instrument IS NOT in the current universe → write `instrument_id=NULL` with `blockholder_filings_ingest_log.error="cusip_resolved_outside_universe (instrument=%d hints=%s)"`. The raw chain row still persists (audit trail preserved per existing schema); the observation layer stays universe-clean.
     9. **CASE E (no hint at all)**: legacy daily-index path or operator rebuild from no-hint source → CUSIP-only resolution as today, no PR11 regression.
   - **Hint UPSERT semantics (Codex 1b HIGH)**: `INSERT INTO sec_13dg_discovery_issuer_hint (accession_number, instrument_id, issuer_cik) VALUES (...) ON CONFLICT (accession_number, instrument_id) DO UPDATE SET discovered_at = NOW(), issuer_cik = EXCLUDED.issuer_cik`. Idempotent on re-discovery; refreshes `discovered_at` so the freshness operator can observe recent scan activity.
   - **Atomicity (Codex 1b HIGH)**: per-accession discovery writes both the manifest row AND every applicable hint row inside a single `conn.transaction()` block. The manifest row never becomes worker-visible (`status='pending'`) until the hint row(s) are committed. This pins the close of the silent-gap window — the worker cannot race ahead of the hint write and re-introduce the CUSIP-only fallback for a universe-discovered accession.

5. Rate-limit: shared SEC 10 req/s budget via `_MIN_REQUEST_INTERVAL_S` + `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` in [app/providers/implementations/sec_edgar.py:55-80] (process-wide single-instance throttle shared by every `SecFilingsProvider`). Discovery uses `SecFilingsProvider.fetch_search_index_json` (new sibling method) so HTTP calls flow through the same throttle as parser fetches. Per-issuer query cost = 1 search-index request + 0-2 pagination requests for outlier issuers. Bootstrap = ~5,200 + ~200 outlier-page = ~5,400 requests = **~9 min wall-clock** under shared budget — fits inside default `max_runtime_seconds=3600` with 6× headroom.

6. Return a `DiscoveryResult` dataclass:

   ```python
   @dataclass(frozen=True)
   class DiscoveryResult:
       issuers_scanned: int
       accessions_discovered: int
       manifest_rows_inserted: int
       manifest_rows_skipped_existing: int  # PK conflict on re-discovery
       filers_upserted: int
       hints_written: int                    # sec_13dg_discovery_issuer_hint inserts
       rows_skipped_outside_cap: int         # always 0 since discovery query is already capped; surfaces explicit invariant
       elapsed_seconds: float
   ```

**Why one file** (not split discovery + ingest module): the discovery layer is pure HTTP + SELECT + INSERT and does NOT call the parser. It enqueues manifest rows; the existing `sec_manifest_worker` drains them. Putting discovery in `sec_13dg_discovery.py` keeps the load-bearing live module (`blockholders.py`) focused on parse + write helpers; `sec_13dg_discovery.py` mirrors the shape of N-CSR discovery introduced under PR8.

### 3.2 Cap chokepoints (3y `filed_at`)

Helper module additions in `app/services/blockholders.py` (canonical module; matches PR8 N-CSR helper placement); REVISED v4 to incorporate the SEC XBRL mandate floor:

```python
INSIDER_BLOCKHOLDERS_RETENTION_YEARS = 3

# SEC adopted final amendments to Rule 13d-1/13d-2 + Schedule 13D/13G
# mandating structured-XML (inline XBRL) submissions for all Schedule 13
# filings, effective 2024-12-19. Filings made BEFORE this date are
# HTML-only and not parseable by edgartools.Schedule13D/Schedule13G
# (skill_edgartools.md G11) or by any extant library in this repo.
# PR11 honours "100% complete" by capping retention at the more-recent of
# (today - 3y) and the mandate effective date — every filing inside the
# window is guaranteed parseable. By 2027-12-19 the 3y floor catches up
# and the function reverts to plain (today - 3y).
SEC_SCHEDULE_13_XML_MANDATE_DATE = date(2024, 12, 19)

def blockholders_retention_cutoff() -> date:
    """Inclusive lower bound on filed_at: max of 3y-floor and XML mandate."""
    today = datetime.now(tz=UTC).date()
    three_year_floor = today - timedelta(days=365 * INSIDER_BLOCKHOLDERS_RETENTION_YEARS)
    return max(three_year_floor, SEC_SCHEDULE_13_XML_MANDATE_DATE)

def blockholders_within_retention(filed_at: datetime | None) -> bool:
    """Inclusive predicate; treats NULL filed_at as outside retention (defensive)."""
    if filed_at is None:
        return False
    return filed_at.date() >= blockholders_retention_cutoff()
```

**Why date not datetime**: the mandate floor is calendar-day granular (SEC's 2024-12-19 effective date is a date, not an instant); helper returns `date` so the comparison is unambiguous across timezones. Discovery's `&startdt=` query param expects ISO date.

Chokepoint matrix (REVISED post-Codex-1a — chokepoints C + F corrected):

| # | Chokepoint | File / function | Gate kind | Test pin |
| --- | --- | --- | --- | --- |
| **A** | Discovery query | `app/services/sec_13dg_discovery.py::_build_query_params` | `&startdt = _resolve_discovery_startdt()` — derived from `MAX(blockholders_retention_cutoff(), watermark_from_freshness_index)` so steady-state windowing degrades to the 3y floor on outage (Codex 1a MEDIUM #10) | `test_discovery_query_uses_helper_cutoff` + `test_steady_state_watermark_degrades_to_3y_floor` |
| **B** | Manifest worker pre-fetch | `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (BEFORE `fetch_document_text` AND BEFORE `store_raw` to save SEC HTTP budget) | If not `blockholders_within_retention(row.filed_at)` → tombstone with `error="retention floor"` and skip fetch | `test_parse_13dg_tombstones_pre_cap_accession` |
| **C** | Observations sync | `app/services/ownership_observations_sync.py::sync_blockholders` | Gate directly on the raw chain's own `filed_at` column: `WHERE bf.filed_at >= blockholders_retention_cutoff()`. **Do NOT use a `filing_events.filing_date >= cutoff` predicate** — a LEFT JOIN with that predicate null-rejects rows missing a `filing_events` entry (Codex 1a HIGH #4 + Codex 1b PR10b lesson). `blockholder_filings.filed_at` is the source of truth at the raw layer ([sql/095_blockholder_filers_filings.sql:124]). | `test_sync_blockholders_excludes_pre_cap_rows` + `test_sync_blockholders_includes_rows_without_filing_events_entry` |
| **D** | Refresh-current | `app/services/ownership_observations.py::refresh_blockholders_current` | **UNCAPPED** — per parent spec §6.3 "refresh-current is exempt from the cap; capping it would actively delete pre-wipe pre-cap rows" (mirror of §4.5 `refresh_institutions_current` precedent) | `test_refresh_current_keeps_pre_cap_observations_intact` |
| **E** | Bulk dataset | n/a — SEC publishes no 13D/G bulk archive | no gate needed | n/a |
| **F** | Rewash | `app/services/rewash_filings.py::_apply_blockholders` (the `primary_doc_13dg` ParserSpec; **CORRECTED — Codex 1a BLOCKING #3 caught this; the function exists at [rewash_filings.py:571] and is currently uncapped**) | **EXPLICIT BRANCH ORDER (Codex 1b MEDIUM)**: (i) FIRST query `SELECT COUNT(*) FROM blockholder_filings WHERE accession_number = ?` to determine branch — non-zero ⇒ HAPPY PATH (uncapped per parent spec §6.3); zero ⇒ RESCUE PATH. (ii) ONLY in the RESCUE PATH branch, derive `accession_filed_at` from `raw_doc` or its `filing_events` join and short-circuit return `False` if not `blockholders_within_retention(accession_filed_at)` (skip the rewash; would otherwise re-introduce pre-cap observations through the back door). (iii) HAPPY PATH branch proceeds with DELETE + re-INSERT exactly as today; the retention helper is NOT invoked. Lint invariant H pins the branch-order placement. | `test_rewash_13dg_happy_path_uncapped_for_existing_rows` (rows exist + accession pre-cap → still rewashes) + `test_rewash_13dg_rescue_path_skips_pre_cap_accession` (zero rows + accession pre-cap → returns False, no DELETE, no INSERT) + `test_rewash_13dg_rescue_path_writes_post_cap_accession` (zero rows + accession post-cap → normal rescue write) |
| **G** | One-shot single accession | `_ingest_single_accession` is not 13D/G-aware; operator-rebuild via `POST /jobs/sec_rebuild/run` re-enqueues manifest rows which then pass through gate B | no separate gate | covered by B |

The cap is **filed-at based** (matches PR8 N-CSR precedent), not `period_of_report` based, because:

- 13D/G has no `period_of_report` — the cover page records `date_of_event` (the day the 5% threshold was crossed) which lags `filed_at` by up to 10 calendar days.
- Discovery uses `file_date` from efts.sec.gov which IS the `filed_at` source of truth.
- Cap helper and discovery query speak the same vocabulary; gate B is defensive (catches any future writer that bypasses discovery — e.g. a manual `POST /jobs/sec_rebuild/run` against `sec_13d` source that re-enqueues pre-cap rows from a stale freshness index).

### 3.3 Manifest worker integration

No new worker code. Existing `sec_manifest_worker` already drains pending `sec_13d` + `sec_13g` rows via `_parse_13dg` (registered at `app/services/manifest_parsers/__init__.py:59`). PR11 changes inside `_parse_13dg`:

1. ADD: pre-fetch retention gate (chokepoint B above). Reuses `blockholders_within_retention(row.filed_at)` import from `app.services.blockholders`.
2. ADD: hint-cross-validated CUSIP resolution (REVISED per Codex 1b BLOCKING #2 to honour share-class sibling semantics). The 5-case branch logic from §3.1 step 4 lives inside `_parse_13dg`. CUSIP resolution remains the primary instrument_id source for the share-class-aware path; the hint is consulted as (a) a universe-membership validator and (b) a single-row fallback when CUSIP unresolves on a single-class issuer. Ambiguous CUSIP-unresolved + multi-hint cases write `instrument_id=NULL` with explicit `blockholder_filings_ingest_log.error="cusip_unresolved_with_ambiguous_hint"` so operator audit surfaces the case without silent loss.
3. UNCHANGED: store_raw (still BEFORE parse per #938 raw-payload invariant), parse, upsert into `blockholder_filers` + `blockholder_filings`, observation write-through, refresh-current.
4. UNCHANGED: `subject_type='blockholder_filer'` + `instrument_id IS NULL` semantics — the schema CHECK at [sql/118_sec_filing_manifest.sql] enforces this and PR11 preserves it. The discovery layer guarantees `blockholder_filers` is auto-seeded BEFORE the manifest row is INSERTed (so the legacy resolver path continues to succeed for filers that re-appear via daily-index); `_upsert_filer` inside the parser body remains idempotent via ON CONFLICT (cik) DO UPDATE.

The hint side-table + the 5-case parser logic together close BLOCKING #1 (schema-incompatible manifest shape) AND BLOCKING #2 (silent CUSIP gap) AND Codex 1b BLOCKING #2 (share-class sibling routing) without changing the manifest schema invariants.

**Parser library adoption (NEW v4 per operator scope call)**: PR11 replaces the in-house XML parser at `app/providers/implementations/sec_13dg.py::parse_primary_doc` with `edgartools.Schedule13D.parse_xml(xml) → Schedule13D` / `Schedule13G.parse_xml(xml) → Schedule13G` for the manifest-worker path. Edgartools' parser is canonical, Pydantic-validated, and tracks SEC schema updates. The retention floor `max(today - 3y, 2024-12-19)` GUARANTEES every filing inside the window is post-XML-mandate and parseable by edgartools — so the parser library coverage gap (skill_edgartools.md G11: pre-2024-12-19 HTML returns `None`) is closed by construction at the cap layer, not the parser layer.

**Library version + risk acknowledgment**: edgartools is already pinned at `5.30.2 (<5.31.0 ceiling)` in this repo (currently used only for 13F static parsers per skill_edgartools.md). The Pydantic validation cliff (#932) means a future edgartools upgrade can break drop-in compatibility; PR11 documents the version pin in the parser module docstring + adds a pinned version test (`tests/test_edgartools_version_pin.py` extension or new file) so a CI break surfaces immediately.

**Provider function disposition**: `parse_primary_doc` in `app/providers/implementations/sec_13dg.py` is RETAINED but deprecated — kept for backward compat with `rewash_filings.py::_apply_blockholders` (which is itself updated to call edgartools' parser in the same PR). After PR11 merges, a follow-up housekeeping ticket can fully delete `parse_primary_doc` and its `BlockholderFiling` dataclass once all callers route through edgartools. PR11 does NOT delete it — keeping the diff focused on activation + cap.

**Pre-mandate HTML filings are explicitly out of scope**: the cap floor `2024-12-19` makes pre-mandate filings unreachable by construction. No tombstoning needed because no manifest row is enqueued for pre-mandate accessions (discovery's `&startdt=` query honours the floor; gate B's `blockholders_within_retention` is the defensive backstop). If a legacy daily-index discovery path (no PR11 hint) enqueues a pre-mandate accession, gate B tombstones it with `error="retention floor"` — explicit, not silent.

### 3.4 Cleanup — dormant code retirement (in same PR)

The operator mandate "no tech debt, no coming back later" requires retiring the dormant filer-seed-driven path in the same PR that activates the live one. Concretely:

**DELETE from `app/services/blockholders.py`**:

- `ingest_all_active_filers` (entry-point that walks `blockholder_filer_seeds`)
- `ingest_filer_blockholders` (per-filer walker; dependency-only of `ingest_all_active_filers`)
- `_list_active_filer_seeds` (helper that reads the seed table)
- `seed_filer` (operator-facing helper that wrote to the seed table)
- Any other helper whose sole caller surface is the deleted set (audit during impl)

**EDIT `scripts/seed_holder_coverage.py` (Codex 1a HIGH #5 caught this — full retirement breaks the script's 13F-HR / CUSIP-resolver / N-CEN paths)**:

- Remove the 13D/G blockholder seeding block: `_BLOCKHOLDER_SEEDS` constant, the `seed_blockholder_filer` import + call, the "Seeding blockholder_filer_seeds..." print block, and the "Ingesting 13D/G blockholders..." print block + `ingest_all_blockholders` call.
- KEEP every other path in the script:
  - 13F-HR `institutional_filer_seeds` seeding + `ingest_all_institutional` invocation (#730 path — out of PR11 scope)
  - ETF `etf_filer_cik_seeds` over-tagging
  - CUSIP resolver (#781)
  - N-CEN classifier (#782)
- Add a one-line script docstring update noting "13D/G blockholders are now universe-driven via the bootstrap `sec_blockholders_discovery` stage (#1233 PR11) — no operator seeding required."
- Update the script's CLI help text accordingly.

**DROP** (new migration `sql/15X_drop_blockholder_filer_seeds.sql`):

```sql
-- Drop dormant filer-seed table; PR11 retires the operator-curated
-- seed mechanism in favour of universe-issuer-CIK-driven discovery
-- via efts.sec.gov. The seed table was empty universe-wide (never
-- populated outside dev smoke tests). All downstream consumers
-- (ingest_all_active_filers, ingest_filer_blockholders, the atom
-- fast-lane / daily-index reconcile subject resolver's seed-list
-- lookup branch, scripts/seed_holder_coverage.py blockholder block)
-- are removed in the same PR.
DROP INDEX IF EXISTS idx_blockholder_filer_seeds_active;
DROP TABLE IF EXISTS blockholder_filer_seeds;
```

**ADD** (same-PR new migration `sql/15Y_create_sec_13dg_discovery_issuer_hint.sql`) — schema revised for share-class siblings per Codex 1b BLOCKING #2:

```sql
-- Discovery-time issuer hint table. When the new universe-CIK-driven
-- discovery layer enqueues a 13D/G accession into sec_filing_manifest,
-- it also writes one hint row per universe-member (accession, instrument_id)
-- pair so the manifest worker parser can (a) confirm universe-membership
-- and (b) fall back to a single hint when CUSIP resolution fails for a
-- single-class issuer. Multi-row per accession PK shape handles
-- share-class siblings on a shared CIK (GOOG/GOOGL, BRK.A/BRK.B,
-- per sql/099/103 documented sibling semantics).
--
-- Legacy daily-index path writes no hint rows; the parser falls back
-- to CUSIP-only resolution as today.
CREATE TABLE IF NOT EXISTS sec_13dg_discovery_issuer_hint (
    accession_number  TEXT NOT NULL,
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    issuer_cik        TEXT NOT NULL,
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (accession_number, instrument_id)
);
CREATE INDEX IF NOT EXISTS idx_sec_13dg_discovery_issuer_hint_accession
    ON sec_13dg_discovery_issuer_hint (accession_number);
CREATE INDEX IF NOT EXISTS idx_sec_13dg_discovery_issuer_hint_instrument_id
    ON sec_13dg_discovery_issuer_hint (instrument_id);
```

`tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` updated to drop `blockholder_filer_seeds` and add `sec_13dg_discovery_issuer_hint` (per the "When a migration adds OR drops any table with a FK relationship, update `_PLANNER_TABLES`" prevention-log entry).

The atom fast-lane resolver and daily-index reconcile resolver are also edited: the `blockholder_filer_seeds` lookup branch is removed; the `blockholder_filers` lookup branch stays (now the SOLE resolution path; populated by PR11's discovery layer upstream of the manifest insert).

**KEEP**:

- `app/services/blockholders.py` lower-level helpers (`_upsert_filer`, `_upsert_filing_row`, `_record_13dg_observation_for_filing`, `_resolve_cusip_to_instrument_id`, `_archive_file_url`, `_record_ingest_attempt`, etc.) — actively used by the live manifest parser at [manifest_parsers/sec_13dg.py:60-69].
- `blockholder_filers` table — auto-populated by discovery + parser via `_upsert_filer`; required for resolver lookup; required for `blockholder_filings` FK.
- `blockholder_filings` table — raw chain; populated by parser; read by `sync_blockholders` and ownership rollup.
- `blockholder_filings_ingest_log` table — written by parser via `_record_ingest_attempt`; required for operator audit.
- All `ownership_blockholders_*` tables and refresh paths.
- All parser, provider, API, and rollup code paths.

**Test impact**:

- `tests/test_blockholders_ingester.py`: ~50% of cases reference `ingest_all_active_filers` or `ingest_filer_blockholders`; those cases are deleted. The remaining cases (lower-level helper tests + sync tests) stay.
- `tests/test_manifest_parser_sec_13dg.py`: unchanged shape; adds new test cases for (a) chokepoint B tombstone branch, (b) issuer-hint short-circuit, (c) issuer-hint absent → CUSIP fallback.
- `tests/test_sec_13dg_parser.py`: unchanged.
- `tests/test_api_blockholders.py`: unchanged.
- NEW `tests/test_sec_13dg_discovery.py`: covers the new discovery module — fake `efts.sec.gov` response fixtures including:
  - happy-path single filer + single-class issuer
  - joint filing with 2 filers (all seeded; manifest `cik`/`subject_id` = accession-prefix CIK)
  - joint filing with issuer CIK NOT in `ciks[0]` (defensive filer extraction, Codex 1a MEDIUM #8)
  - duplicate CIK in `ciks[]` (dedup)
  - no-CIK natural-person filer (name-only fallback, no manifest row for that filer — only CIK-bearing filers are seedable)
  - exactly 100 hits page 1 followed by empty page 2 (pagination boundary, Codex 1a MEDIUM #9)
  - re-discovery idempotency on second run (zero new manifest rows, zero new hint rows, `discovered_at` REFRESHED — Codex 1b HIGH idempotency)
  - 3y window enforcement at query level (no out-of-window hits reach the manifest writer)
  - steady-state watermark = `MAX(bf.filed_at)` per-issuer derivation (NOT from DFI — Codex 1b HIGH coherence) + degrades to 3y floor for issuer with zero prior ingest (Codex 1a MEDIUM #10)
  - share-class sibling: same accession discovered N times under N sibling instrument_ids → N hint rows written, ONE manifest row (Codex 1b BLOCKING #2)
  - manifest + hint atomicity: a worker thread reading `status='pending'` mid-discovery is blocked by the `conn.transaction()` until both writes commit (Codex 1b HIGH atomicity)
  - archive-owner CIK derivation: `cik` field on manifest = first non-issuer, non-agent CIK from `ciks[]` (Codex 1c BLOCKING #1 — accession-prefix returns 404 for filing-agent submissions like Donnelley/Edgar Agents/DFIN/Workiva)
  - filing-agent CIK in `ciks[]`: agent is NOT seeded into `blockholder_filers`; manifest `cik` falls through to the first non-issuer-non-agent CIK
  - issuer-only result (no non-issuer CIK in `ciks[]`): defensive skip + warn log (anomalous; should not occur in practice but pinned for safety)
- NEW `tests/test_manifest_parser_sec_13dg.py` additions: 5-case hint-cross-validation branch — CASE A (CUSIP-in-hints happy path), CASE B (single-hint fallback), CASE C (multi-hint ambiguous writes NULL + log), CASE D (CUSIP-not-in-hints discrepancy log + trust CUSIP), CASE E (no hint → CUSIP-only legacy path). One test per case; assertion on resulting `instrument_id` value AND on `blockholder_filings_ingest_log.error` content for CASE C.
- NEW `tests/test_ownership_observations_sync_blockholders_cap.py` (or fold into existing): covers chokepoint C gate — `bf.filed_at >= cutoff` predicate; row WITHOUT `filing_events` entry still syncs; row WITH pre-cap `bf.filed_at` excluded.
- NEW `tests/test_rewash_blockholders_cap.py` (or fold into existing): covers chokepoint F — happy path uncapped for accessions with existing `blockholder_filings` rows; rescue path skipped for pre-cap accession with zero existing rows.

### 3.5 Bootstrap stage + scheduler wiring

**Rate-limit surface (corrected — Codex 1a HIGH #7 + Codex 1b LOW symbol names)**: there is no `app/services/sec_rate_limit.py` module. The actual throttle lives in `app/providers/implementations/sec_edgar.py:55-80` via the following SYMBOLS (verified at the cited line numbers, 2026-05-21):

- `_MIN_REQUEST_INTERVAL_S = 0.11` — conservative inter-request floor (≈0.11 s ⇒ ≤9.1 req/s, safely under SEC's 10 req/s ceiling)
- `_PROCESS_RATE_LIMIT_CLOCK: list[float] = [0.0]` — process-wide last-request timestamp (mutable single-element list so multiple `SecFilingsProvider` instances share the same wall-clock cursor)
- `_PROCESS_RATE_LIMIT_LOCK: threading.Lock` — process-wide lock guarding read-modify-write of `_PROCESS_RATE_LIMIT_CLOCK`

Every `SecFilingsProvider` instance reuses these process-wide singletons via the constructor's `min_request_interval_s=_MIN_REQUEST_INTERVAL_S, shared_last_request=_PROCESS_RATE_LIMIT_CLOCK, shared_throttle_lock=_PROCESS_RATE_LIMIT_LOCK` injection at [sec_edgar.py:244-252]. Discovery uses `SecFilingsProvider` for ALL HTTP — including a NEW method `fetch_search_index_json(ciks, forms, startdt, enddt, from_offset, size)` that hits `efts.sec.gov/LATEST/search-index` — so discovery's requests share the 10 req/s budget with parser fetches automatically. The `lane="sec_rate"` on the bootstrap stage serializes the JOB (one discovery job at a time); the provider-internal throttle serializes REQUESTS across all SEC-touching jobs.

**Discovery startdt resolution** (revised twice — Codex 1a MEDIUM #10 + Codex 1b HIGH watermark coherence): the per-issuer watermark cannot be sourced from `data_freshness_index` because DFI for `sec_13d`/`sec_13g` is keyed by `subject_type='blockholder_filer' + subject_id=filer_cik` (the live daily-index path's identity grain) — not by issuer CIK. PR11's discovery is per-ISSUER not per-filer, so the DFI key shape doesn't match. The correct watermark source for per-issuer scans is the raw chain's own `blockholder_filings.issuer_cik + filed_at` data we've already ingested:

```python
def _resolve_discovery_startdt(
    conn: psycopg.Connection,
    *,
    mode: Literal["bootstrap", "steady_state"],
    issuer_cik: str | None = None,
) -> date:
    """Pick discovery window start, with 3y floor as the hard ceiling.

    Bootstrap: always full 3y floor.
    Steady-state: derive from MAX(blockholder_filings.filed_at) for this
    issuer_cik (the chain we've already ingested), minus a 7d safety
    overlap. CLAMP to the 3y floor so a watermark gap (issuer with no
    prior 13D/G ingest) does not silently reduce coverage.

    data_freshness_index is NOT consulted here — DFI's blockholder_filer
    key shape is filer-side, not issuer-side, and would not match the
    per-issuer scan grain (Codex 1b HIGH watermark coherence).
    """
    floor = blockholders_retention_cutoff().date()
    if mode == "bootstrap":
        return floor
    if issuer_cik is None:
        return floor
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(filed_at)::date
            FROM blockholder_filings
            WHERE issuer_cik = %s
              AND filed_at IS NOT NULL
            """,
            (issuer_cik,),
        )
        row = cur.fetchone()
    watermark = row[0] if row and row[0] else floor
    return max(floor, watermark - timedelta(days=7))
```

This protects against (a) the silent-gap risk where a >14d job outage would let recent filings slip through a flat sliding window AND (b) an issuer with zero prior 13D/G ingest (no `blockholder_filings` rows) silently scanning a tight window. The 3y floor is the absolute upper bound; the watermark only ever NARROWS the window for the common case where steady-state ran recently AND has accumulated chain rows for the issuer.

**NEW bootstrap stage** in `app/workers/scheduler.py` `_BOOTSTRAP_STAGE_SPECS`:

```python
"sec_blockholders_discovery": StageSpec(
    stage_key="sec_blockholders_discovery",
    lane="sec_rate",                # shared 10 req/s SEC budget
    job_name="sec_blockholders_discovery_job",
    order=...,                       # post-PR1, post-PR5 (DEF14A discovery); pre-N-PORT — set during impl
    description="Universe-issuer-CIK-driven SC 13D/G discovery via efts.sec.gov, 3y cap",
    prerequisite="sec_universe_sync",  # depends on country='US' + is_tradable being populated
    params={"mode": "bootstrap"},   # full-cohort 3y scan
    max_runtime_seconds=3600,        # 1h default — should drain in ~10min for 5,174 issuers under shared budget
)
```

**NEW scheduler job** `sec_blockholders_discovery_job` (nightly, parallel to other SEC discovery jobs):

```python
@register_job("sec_blockholders_discovery_job", JobLane.SEC_RATE)
def sec_blockholders_discovery_job(params: dict[str, Any]) -> JobResult:
    """Walk universe US-tradable issuer CIKs; enqueue SC 13D/G within 3y window to manifest."""
    mode = params.get("mode", "steady_state")  # bootstrap stage overrides via StageSpec.params
    result = discover_sec_13dg_for_universe(  # NEW in app/services/sec_13dg_discovery.py
        mode=mode,
        ...
    )
    return JobResult(...)
```

The steady-state job dispatches with empty params → defaults to `mode="steady_state"` which uses the watermark-derived startdt clamped to the 3y floor. The bootstrap stage explicitly dispatches `mode="bootstrap"` for the full-3y scan.

**`data_freshness_index` integration**: NEW entries for `sec_13d` and `sec_13g` lanes per #863 — three-tier polling (fresh / stale / dormant). The discovery job + worker drain update `data_freshness_index.last_observed_at` automatically for the filer-keyed (subject_type='blockholder_filer', subject_id=filer_cik) rows. DFI is NOT consulted by `_resolve_discovery_startdt` (the DFI key grain is filer-side; PR11's discovery is per-issuer — see §3.5 helper body above). Discovery instead derives its per-issuer watermark from `MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?` clamped to the retention floor.

### 3.6 Lint guard

NEW `scripts/check_13dg_retention.sh` (pre-push hook; PR5-style awk block walker; mirror of `check_n_csr_retention.sh`):

Invariants (REVISED post-Codex-1a — D corrected, H added for rewash):

- **A — helpers present**: `app/services/blockholders.py` defines exactly one `def blockholders_retention_cutoff(` and one `def blockholders_within_retention(`. (Greps for the literal `def ` prefix on both names; fails if count ≠ 1 or if defined outside `blockholders.py`.)
- **B — discovery query uses helper**: `app/services/sec_13dg_discovery.py` contains a call to `blockholders_retention_cutoff()` AND a call to `_resolve_discovery_startdt(` (the watermark-clamped variant) AND the `_resolve_discovery_startdt` body references `blockholders_retention_cutoff()` (so the 3y floor cannot be bypassed by accident). Empty-grep `wc -l` guard per PR10a Codex iter 1 lesson.
- **C — manifest gate placed BEFORE fetch + BEFORE store_raw**: `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` calls `blockholders_within_retention(` on a line whose line number precedes the first `fetch_document_text(` AND the first `store_raw(` invocation inside the same function block. Awk-based block walker (PR4 Codex 1c lesson).
- **D — sync gates on `bf.filed_at` directly, NOT on `filing_events.filing_date`**: `app/services/ownership_observations_sync.py::sync_blockholders` body contains both (a) a literal `bf.filed_at >= ` predicate (or equivalent column-qualified comparison against the cutoff helper) AND (b) `blockholders_retention_cutoff()`; AND simultaneously FORBIDS the substring `fe.filing_date >=` or `filing_events.filing_date >=` anywhere in the function body. This pins the Codex 1a HIGH #4 lesson: `LEFT JOIN ... WHERE fe.filing_date >= cutoff` null-rejects rows missing a `filing_events` entry, so the gate must be on the raw chain's own column.
- **E — refresh-current is exempt**: forbid any reference to `blockholders_retention_cutoff` or `blockholders_within_retention` inside `refresh_blockholders_current(` function block (the §4.5 13F-HR precedent: capping refresh would actively delete pre-wipe rows from `_current`).
- **F — no append writers outside the helper-gated chokepoints**: forbid raw `INSERT INTO ownership_blockholders_observations` and raw `INSERT INTO blockholder_filings` outside (a) `app/services/blockholders.py` lower-level helpers and (b) the manifest parser. Catches future PRs that add a side-path writer skipping the cap.
- **G — dormant entrypoints stay deleted**: forbid the literal symbols `ingest_all_active_filers`, `ingest_filer_blockholders`, `_list_active_filer_seeds`, and `seed_filer` (the 13D/G variants) from re-appearing anywhere under `app/` AND under `scripts/` (except as comment-only mentions in the retirement note inside `scripts/seed_holder_coverage.py`). Catches accidental resurrection.
- **H — rewash rescue-path gated (NEW post-Codex-1a)**: `app/services/rewash_filings.py::_apply_blockholders` function body contains a reference to `blockholders_within_retention(` (the rescue-path gate) AND the call precedes any `DELETE FROM blockholder_filings` or `_upsert_filing_row(` invocation inside the same function. Distinguishes happy-path (existing rows already in `blockholder_filings`) from rescue-path (zero rows → would re-introduce pre-cap observations through the back door).
- **I — discovery uses provider throttle (no raw HTTP) — REVISED per Codex 1b MEDIUM**: `app/services/sec_13dg_discovery.py` MUST satisfy ALL of (i) POSITIVE: `from app.providers.implementations.sec_edgar import SecFilingsProvider` appears in the import block; (ii) POSITIVE: the discovery body calls `provider.fetch_search_index_json(` at least once; (iii) NEGATIVE: no `import httpx`, no `import requests`, no `import urllib`, no `from httpx`, no `from requests`, no `from urllib`, no aliased imports (e.g. `import httpx as` / `import requests as`); (iv) NEGATIVE: no direct reach into `app.providers.implementations.sec_edgar._client` or other underscore-prefixed provider internals. Catches future PRs that add a side-channel fetch bypassing the 10 req/s budget.
- **J — discovery uses `record_manifest_entry` (no raw INSERT) — REVISED per Codex 1b MEDIUM**: `app/services/sec_13dg_discovery.py` MUST satisfy ALL of (i) POSITIVE: `from app.services.sec_manifest import record_manifest_entry` appears in the import block; (ii) POSITIVE: the discovery body calls `record_manifest_entry(` at least once; (iii) NEGATIVE: no `INSERT INTO sec_filing_manifest` (raw SQL), no `UPDATE sec_filing_manifest`, no `cur.execute(...sec_filing_manifest...` patterns, no dynamic SQL-string concatenation against the manifest table name. All manifest writes route through `record_manifest_entry` from `app.services.sec_manifest` so the schema CHECK + idempotency contracts are honoured.
- **K — hint table writes are atomic with manifest writes (NEW per Codex 1b HIGH)**: `app/services/sec_13dg_discovery.py` body contains a `conn.transaction()` block (or equivalent context-manager wrapping a psycopg connection's transaction primitive) AND BOTH `record_manifest_entry(` AND `INSERT INTO sec_13dg_discovery_issuer_hint` (or its helper wrapper) appear within the SAME block scope. AWK-based block walker verifies the call-pair nesting per the PR4 Codex 1c lesson. Catches a future PR that splits the writes across separate commits, re-introducing the silent-gap window where the worker can race the hint write.
- **L — hint UPSERT uses ON CONFLICT (NEW per Codex 1b HIGH idempotency)**: the SQL string in `app/services/sec_13dg_discovery.py` that writes to `sec_13dg_discovery_issuer_hint` MUST contain `ON CONFLICT (accession_number, instrument_id) DO UPDATE SET discovered_at` so re-discovery refreshes the hint cleanly. Catches a future PR that switches to `INSERT ... ON CONFLICT DO NOTHING` (stale `discovered_at` would mask freshness signal).

Wired into `.githooks/pre-push` after the existing PR10b `check_form3_latest_per_pair.sh` and `check_form5_retention.sh` invocations.

### 3.7 Migration

NEW `sql/15X_drop_blockholder_filer_seeds.sql`:

```sql
-- Drop dormant filer-seed table; PR11 retires the operator-curated
-- seed mechanism in favour of universe-issuer-CIK-driven discovery
-- via efts.sec.gov. The seed table was empty universe-wide (never
-- populated). All downstream consumers of the seed list
-- (ingest_all_active_filers, ingest_filer_blockholders, the atom
-- fast-lane subject resolver's seed-list lookup branch) are removed
-- in the same PR. `_PLANNER_TABLES` updated to remove the dropped
-- table.
DROP INDEX IF EXISTS idx_blockholder_filer_seeds_active;
DROP TABLE IF EXISTS blockholder_filer_seeds;
```

`tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` updated to remove `blockholder_filer_seeds` (per the "When a migration adds OR drops any table with a FK relationship, update `_PLANNER_TABLES`" prevention-log entry).

The atom fast-lane resolver and daily-index reconcile resolver are also edited: the seed-list lookup branch is removed; the `blockholder_filers` lookup branch stays (now the SOLE resolution path; populated by PR11's discovery layer upstream of the manifest insert).

### 3.8 Acceptance criteria (per CLAUDE.md ETL clauses 8-12)

1. **Smoke-tested against panel** (AAPL, GME, MSFT, JPM, HD) on dev DB after merge. Expected: GME has ≥3 SC 13D/A (RC Ventures activist trail) + multiple SC 13G (Vanguard/BlackRock); AAPL likely has 0-1 SC 13D + handful of SC 13G; MSFT/JPM/HD have institutional 13G filings.
2. **Cross-source verified**: At least one observation row's `(filer_cik, filed_at, percent_of_class)` cross-checked against SEC EDGAR direct (`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<issuer>&type=SC+13`).
3. **Discovery executed first, THEN rebuild as needed** (corrected — Codex 1a HIGH #6):
   - PR11 adds NEW universe rows; `POST /jobs/sec_rebuild/run` only re-enqueues EXISTING manifest rows. The correct post-merge sequence is:
     1. Trigger the new bootstrap stage: `POST /jobs/sec_blockholders_discovery_job/run` with `{"mode": "bootstrap"}` (or invoke via the bootstrap-state-machine S?? stage if the operator is doing a full re-run; otherwise the explicit `/run` triggers the same job body).
     2. Wait for discovery to drain (~9 min wall-clock under 10 req/s shared budget; observable via `data_freshness_index` + `sec_filing_manifest` `pending` count for `source IN ('sec_13d','sec_13g')`).
     3. Manifest worker drains discovered accessions through `_parse_13dg` (already-live; no operator intervention).
     4. Only AFTER discovery + worker drain does `POST /jobs/sec_rebuild/run` make sense — for re-parsing accessions with stale `parser_version` (the rewash happy path) or for retrying transient failures.
   - PR description records (a) the discovery job invocation + final result counts (`issuers_scanned`, `accessions_discovered`, `manifest_rows_inserted`, `hints_written`) and (b) any subsequent rebuild invocations + outcomes.
4. **Operator-visible figure verified**: `GET /instruments/GME/blockholders` and `/ownership-rollup?category=blockholders` render the RC Ventures + Vanguard/BlackRock entries post-discovery + post-worker-drain. PR description records the rendered figures.
5. **PR description records the verification step + commit SHA** for each of clauses 1-4.

### 3.9 Provenance + parent spec amendments

Same PR amends `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`:

- §4.8 — replace "Current volume: 0 ingested (table exists; pipeline not yet active)" with "Volume: backfilled in PR11 (#TBD) via efts.sec.gov universe-issuer-CIK discovery + 3y cap"
- §4.8 — replace "Ingest depth cap: 3y historical at the parser + current state always" with the concrete chokepoint matrix (mirror format from §4.5/§4.6/§4.7/§4.8 PR-shipped sections)
- §7 PR11 entry — replace "13D/G activate dormant pipeline with 3y historical + current-state cap at the parser" with the SHIPPED summary (chokepoint coverage, dormant-code retirement, lint guard, migration)
- §11 Codex review gate — add PR11 entry to the cadence log
- §12 Handover — mark PR11 SHIPPED; PR12 (`ownership_*_current` size audit) remains as the final spec PR

## 4. Risks + mitigations

| Risk | Mitigation |
| --- | --- |
| `efts.sec.gov` endpoint changes shape / undocumented | Fallback to `data.sec.gov/submissions/CIK{filer}.json` per-filer walk via a `blockholder_filers` view of known filers (post-bootstrap, when the table has been seeded); if endpoint flat-out unavailable, discovery is no-op and operator paged via #863 freshness index. Discovery has explicit contract test against fixture response shapes so a silent endpoint drift fails CI before production. |
| Pagination edge case — issuer with exactly 100 or 200 results in 3y window | Loop until `len(hits) < size`; fixture test pins the exact-100-hits boundary (Codex 1a MEDIUM #9). Smoke-test confirms 99th-percentile fits in 2 pages. |
| Joint-filing identification — `ciks[]` may have >2 entries, issuer not in position 0, duplicate CIKs, no-CIK natural-person filers, or filing-agent CIKs | Defensive extraction per §3.1 step 3: filter `[c for c in cik_list if c.lstrip('0') != issuer_cik_unpadded]`; dedupe via set; pick FIRST non-issuer, non-agent CIK (`c not in _KNOWN_FILING_AGENT_CIKS` per `sec_edgar.py:101`) as the manifest's `cik`/`subject_id` (idempotent on re-discovery via stable iteration order); auto-seed only non-agent CIKs into `blockholder_filers`. No-CIK natural-person filers can't be seeded (no PK) — those are surfaced inside the parsed Schedule13D/G `reportingPersons` rows at parse-time and written to `blockholder_filings` with `reporter_cik=NULL, reporter_no_cik=TRUE`. Fixture tests pin each shape including the agent-CIK case. |
| SEC rate-limit budget contention with other lanes during bootstrap | Stage uses `lane="sec_rate"` (job-level serialization); requests further serialized by `_PROCESS_RATE_LIMIT_LOCK` in `sec_edgar.py` (10 req/s ceiling). 5,400-request bootstrap discovery scan = ~9min wall-clock; fits inside default `max_runtime_seconds=3600` with 5-6× headroom. |
| Auto-populated `blockholder_filers.name` is sometimes a joint-filing label (e.g. "RC Ventures LLC and Ryan Cohen") | Accept name as-is from `display_names[i]` matched to its own CIK index (positional alignment per empirical observation); the resolver only joins on CIK; name is operator-visible label only — no functional impact. Fallback to `f"CIK {filer_cik}"` if display name is missing for a filer CIK index. |
| Resurrection of dormant seed-driven path by future PR | Lint guard G forbids re-introduction of the deleted symbols (`ingest_all_active_filers`, `ingest_filer_blockholders`, `_list_active_filer_seeds`, `seed_filer`); migration drops the seed table so the resurrection would also need to re-add the schema (loud, not silent). Pre-push hook fails the push BEFORE the change lands. |
| Existing `blockholder_filings` rows from prior smoke tests survive PR11 with stale parser_version | Per parent spec §6.3, existing rows untouched until pre-wipe; PR11 introduces no `parser_version` bump; if prior smoke-test rows exist they're either correct (live manifest path semantics preserved) or rebuilt by post-merge `POST /jobs/sec_rebuild/run` (which routes through the new chokepoint F rewash gate — rescue-path skip for pre-cap accessions, happy-path uncapped for existing rows). |
| Silent CUSIP-unresolved gap for universe-discovered accessions (Codex 1a BLOCKING #2) | NEW `sec_13dg_discovery_issuer_hint` side-table per §3.4 carries `(accession_number, instrument_id)` PK rows (multi-row for share-class siblings per Codex 1b BLOCKING #2). Parser cross-validates CUSIP-resolved `instrument_id` against the hint set via §3.1 step 4's 5-case branch: CASE A (CUSIP-in-hints) is happy path; CASE B (single-hint fallback) closes the single-class CUSIP gap; CASE C (multi-hint ambiguous) writes NULL with explicit audit log; CASE D (CUSIP-not-in-hints) trusts CUSIP and logs discrepancy. Legacy daily-index path continues to fall back to CUSIP only (no hint rows written; no regression for that path; #740 backfill epic continues to own the legacy gap). |
| Share-class siblings on shared CIK (GOOG/GOOGL, BRK.A/BRK.B) silently routed to wrong instrument (Codex 1b BLOCKING #2) | Hint table PK is `(accession_number, instrument_id)` — N rows per accession for N siblings. Parser uses CUSIP as the primary share-class disambiguator (today's path); hint only consulted as universe-membership validator + single-hint-fallback. CASE C explicit NULL + log surfaces the ambiguous-multi-sibling case for operator audit instead of silent mis-routing. Test fixture pins the GOOG/GOOGL shape. |
| Hint-table write races manifest-row pending status (Codex 1b HIGH atomicity) | Discovery writes manifest row + ALL applicable hint rows in a single `conn.transaction()` block. Manifest row never becomes worker-visible (`status='pending'` committed) until hints are committed. Lint invariant K pins the call-pair nesting; test fixture asserts via psycopg `SERIALIZABLE` isolation that a concurrent worker thread blocks until the discovery transaction commits both writes. |
| Hint-table re-discovery duplicates / stale `discovered_at` (Codex 1b HIGH idempotency) | UPSERT clause `ON CONFLICT (accession_number, instrument_id) DO UPDATE SET discovered_at = NOW(), issuer_cik = EXCLUDED.issuer_cik` per §3.4 migration. Lint invariant L pins the `ON CONFLICT ... DO UPDATE` shape (forbids `DO NOTHING` which would mask freshness). Test fixture pins idempotent re-discovery — second pass writes 0 new manifest rows + 0 new hint rows but DOES advance `discovered_at`. |
| Watermark coherence for per-issuer steady-state (Codex 1b HIGH) | `_resolve_discovery_startdt` per §3.5 derives watermark from `MAX(blockholder_filings.filed_at) WHERE issuer_cik = ?` (chain-derived, per-issuer-grain), NOT from `data_freshness_index` (DFI is filer-keyed, wrong grain). Clamped to 3y floor for issuers with zero prior ingest. |
| Pre-existing HTML-only 13D/G parser gap surfaces at scale post-activation | §3.3 out-of-scope note + new follow-up ticket filed at PR merge (HTML body parser fallback OR edgartools adoption). PR11 inherits the gap; tombstone counter will spike post-merge — explicitly called out in the PR description so the operator-visible delta is interpretable. |
| Steady-state >14d job outage silently shrinks coverage | `_resolve_discovery_startdt` clamps watermark to 3y floor (`max(floor, watermark - 7d)`); on first run after outage, the watermark is stale → falls back to 3y floor → discovery re-covers the whole window. Codex 1a MEDIUM #10 lesson. |
| Rewash rescue-path re-introduces pre-cap accessions via the back door | Chokepoint F gate in `_apply_blockholders` distinguishes happy-path (existing rows present → uncapped, preserves §6.3 contract) from rescue-path (zero rows → would re-introduce pre-cap observations → SKIPPED if accession is outside retention). Lint invariant H pins the placement. |
| `scripts/seed_holder_coverage.py` retirement breaks the 13F-HR / CUSIP-resolver / N-CEN paths (Codex 1a HIGH #5) | PR11 surgically removes only the 13D/G blockholder block from the script (constant, import, prints, ingest call); every other path is preserved. Script's CLI help text updated to note 13D/G is now universe-driven via bootstrap stage. |

## 5. Out of scope

- **Filer-side 13D/G discovery** (per-FILER-CIK submissions walk, e.g. "every SC 13D Carl Icahn ever filed regardless of which issuer"). The retired dormant path attempted this; it's incomplete coverage by design (only the seeded filer set) and is replaced by the complete universe-issuer-CIK-driven discovery in PR11. If a future ticket genuinely needs filer-side coverage (e.g. surfacing every activist filing by Pershing Square across all issuers, including non-universe ones), that's a separate epic.
- **Form 144 / Form D / NT 10-Q** etc. (other metadata-only forms — parent spec §4.14 covers them under `filing_events` 10y cap).
- **Active blockholder alerts** (parent spec §5.3 — future alert epic). PR11 lands the data; the alert wire-up is its own ticket.
- **Frontend chart redesign** (parent spec §10 — separate epic).
- **#1010-style filer-recency cohort bound**. 13D/G filers are not a curated cohort (no `blockholder_filers.last_active_at` parallel to `institutional_filers.last_13f_hr_at`); discovery is universe-issuer-CIK-driven so the "shed inactive filers" cohort bound doesn't apply.

## 6. Implementation sequencing (preview — full plan in writing-plans output)

1. Schema migrations:
   - `sql/15X_drop_blockholder_filer_seeds.sql`
   - `sql/15Y_create_sec_13dg_discovery_issuer_hint.sql`
   - `_PLANNER_TABLES` update (both add + drop).
2. Helper additions in `app/services/blockholders.py` (`blockholders_retention_cutoff`, `blockholders_within_retention`, `INSIDER_BLOCKHOLDERS_RETENTION_YEARS = 3`).
3. NEW `app/services/sec_13dg_discovery.py` (discovery module + `discover_sec_13dg_for_universe` entry-point + `_resolve_discovery_startdt` watermark helper + `DiscoveryResult` dataclass).
4. NEW `SecFilingsProvider.fetch_search_index_json` method in `app/providers/implementations/sec_edgar.py` (or sibling) — single HTTP entrypoint for efts.sec.gov so `_PROCESS_RATE_LIMIT_LOCK` is honoured.
5. Cap gate B in `app/services/manifest_parsers/sec_13dg.py::_parse_13dg` (pre-fetch, pre-store_raw); REPLACE in-house `parse_primary_doc` call with `edgartools.Schedule13D.parse_xml(primary_xml)` / `Schedule13G.parse_xml(primary_xml)` dispatch on the manifest source (`sec_13d` vs `sec_13g`); REPLACE today's CUSIP-only resolution with the 5-case hint-cross-validated branch (CASE A CUSIP-in-hints / CASE B single-hint fallback / CASE C multi-hint ambiguous NULL+log / CASE D CUSIP-universe-revalidated / CASE E legacy no-hint).
6. Sync gate C in `app/services/ownership_observations_sync.py::sync_blockholders` (`bf.filed_at >= cutoff` predicate; NO `fe.filing_date` predicate).
7. Rewash gate F in `app/services/rewash_filings.py::_apply_blockholders` — EXPLICIT BRANCH ORDER: (i) `SELECT COUNT(*) FROM blockholder_filings WHERE accession_number = ?`; (ii) zero-count rescue path applies retention helper; (iii) non-zero happy path proceeds uncapped (Codex 1b MEDIUM rewash branch-order pin). REPLACE in-house `parse_primary_doc` call with `edgartools.Schedule13D.parse_xml` / `Schedule13G.parse_xml` to match the live manifest-worker path.
8. Resolver edits in `app/jobs/sec_atom_fast_lane.py` + `app/jobs/sec_daily_index_reconcile.py` (remove seed-list lookup branch; keep `blockholder_filers` lookup).
9. DELETE dormant code from `app/services/blockholders.py` (`ingest_all_active_filers`, `ingest_filer_blockholders`, `_list_active_filer_seeds`, `seed_filer`).
10. EDIT `scripts/seed_holder_coverage.py` (surgical 13D/G block removal; preserve 13F-HR / CUSIP-resolver / N-CEN paths).
11. NEW scheduler job `sec_blockholders_discovery_job` + bootstrap stage `sec_blockholders_discovery` in `app/workers/scheduler.py` + `data_freshness_index` entries for `sec_13d` + `sec_13g`.
12. NEW lint guard `scripts/check_13dg_retention.sh` with placement invariants A-L (incl. K hint-atomicity-in-same-conn-transaction-block and L hint-UPSERT-uses-ON-CONFLICT-DO-UPDATE) + wire into `.githooks/pre-push`.
13. NEW + UPDATED tests:
    - parser cap + issuer-hint short-circuit + CUSIP fallback (`tests/test_manifest_parser_sec_13dg.py` additions)
    - discovery module — fixture-driven `efts.sec.gov` responses, joint filings, pagination boundary, dedup, watermark degradation (`tests/test_sec_13dg_discovery.py` NEW)
    - sync cap on `bf.filed_at`, LEFT JOIN preserves rows without `filing_events` entry (`tests/test_ownership_observations_sync_blockholders_cap.py` NEW or fold)
    - rewash happy-path uncapped + rescue-path gated (`tests/test_rewash_blockholders_cap.py` NEW or fold)
    - refresh-current uncapped invariant
    - dormant symbol absence (lint-as-test or integration-test grep)
    - delete dormant entrypoint test cases in `tests/test_blockholders_ingester.py`
14. Amend parent spec (`docs/superpowers/specs/2026-05-19-data-retention-rubric.md` §4.8, §7, §11, §12).
15. PR description with the ETL clause 8-12 evidence + Codex 2 pre-push review.
