# ETL source-to-sink reference

**Purpose.** Authoritative per-source contract covering every data stream in the eBull ETL: origin → manifest → parser → observation → current → endpoint → chart. Plus the watermarking model, retry posture, smoke gate, and verification query.

**The integrity floor.** Every source eBull consumes MUST have a complete row here. Operator + future-agent reads this to answer: "where does X come from, when does it refresh, how do we know it's correct."

**Maintenance.** Skill `data-engineer/etl-source-to-sink-template.md` (NEW) lint-checks that:
1. Every entry in `ManifestSource` Literal at `app/services/sec_manifest.py:106-122` has a per-source file here.
2. Every per-source file has the 13 required sections (template below).
3. Every per-source file has a corresponding smoke test row at `tests/smoke/test_etl_source_to_sink.py`.

CI gate: `scripts/check_etl_source_docs.sh` enforces.

---

## Sources covered

| Source | File | Class | Tier |
|---|---|---|---|
| sec_form3 | [sec_form3.md](sec_form3.md) | SEC manifest | insider |
| sec_form4 | [sec_form4.md](sec_form4.md) | SEC manifest | insider |
| sec_form5 | [sec_form5.md](sec_form5.md) | SEC manifest | insider |
| sec_13d | [sec_13d.md](sec_13d.md) | SEC manifest | blockholder |
| sec_13g | [sec_13g.md](sec_13g.md) | SEC manifest | blockholder |
| sec_13f_hr | [sec_13f_hr.md](sec_13f_hr.md) | SEC manifest | institutional |
| sec_def14a | [sec_def14a.md](sec_def14a.md) | SEC manifest | proxy / treasury / esop |
| sec_n_port | [sec_n_port.md](sec_n_port.md) | SEC manifest | funds |
| sec_n_csr | [sec_n_csr.md](sec_n_csr.md) | SEC manifest | funds |
| sec_n_cen | [sec_n_cen.md](sec_n_cen.md) | SEC ad-hoc | filer-type classification |
| sec_10k | [sec_10k.md](sec_10k.md) | SEC manifest | filings text |
| sec_10q | [sec_10q.md](sec_10q.md) | SEC manifest (synth no-op) | filings text |
| sec_8k | [sec_8k.md](sec_8k.md) | SEC manifest | events + dividends |
| sec_xbrl_facts | [sec_xbrl_facts.md](sec_xbrl_facts.md) | SEC manifest (synth no-op) | fundamentals |
| finra_short_interest | [finra_short_interest.md](finra_short_interest.md) | FINRA caller-owned | bimonthly short-interest |
| finra_regsho_daily | [finra_regsho_daily.md](finra_regsho_daily.md) | FINRA caller-owned | daily RegSHO volumes |
| company_tickers | [company_tickers.md](company_tickers.md) | SEC bulk reference | CIK ↔ ticker bridge |
| company_tickers_mf | [company_tickers_mf.md](company_tickers_mf.md) | SEC bulk reference | classId ↔ instrument |
| company_tickers_exchange | [company_tickers_exchange.md](company_tickers_exchange.md) | SEC bulk reference | ticker ↔ exchange |
| sec_13f_securities_list | [sec_13f_securities_list.md](sec_13f_securities_list.md) | SEC bulk reference | 13F Official List |
| etoro_candles | [etoro_candles.md](etoro_candles.md) | broker REST | market data |

---

## Forms NOT ingested by the manifest + why

`map_form_to_source` (`app/services/sec_manifest.py`) returns `None` for any
form absent from `_FORM_TO_SOURCE`, and the discovery paths skip it. That is
intentional for the forms below. "Where instead" names the pipeline that *does*
recognise the form, if any — most are recorded as a metadata-only
`filing_events` row (`SEC_METADATA_ONLY` in `app/services/filings.py`), which is
a **different taxonomy** from the manifest source map: a form can be
metadata-only at the `filing_events` tier yet still be ingested by the manifest
worker (Form 5 is — see note).

| Form(s) | Why no manifest source | Where instead |
|---|---|---|
| `6-K`, `6-K/A` | Foreign private issuer interim report — no manifest parser, and deliberately excluded from fundamentals (typically lacks structured XBRL, so companyfacts yields no new rows — `FUNDAMENTALS_FORMS` in `app/services/fundamentals/__init__.py`). | `filing_events` metadata-only. |
| `20-F`, `20-F/A`, `40-F`, `40-F/A` | Foreign private issuer annual — no manifest ownership/insider parser. **Fundamentals ARE ingested** for these, but via the companyfacts (`sec_xbrl_facts`) path keyed by CIK (`FUNDAMENTALS_FORMS`), not by form→source discovery. | Fundamentals: companyfacts / XBRL; `filing_events` metadata-only otherwise. |
| `S-1`, `S-3`, `S-4`, `F-1/3/4`, `424B2/3/4/5/7/8` | Registration / prospectus capital-action filings — no ownership, fundamentals, or insider payload to parse. | New-instrument discovery is via `company_tickers`, not form discovery; `filing_events` metadata-only. |
| `PRE 14A`, `PRER14A` | Preliminary proxy **drafts** (#1320) — the definitive `DEF 14A` that follows is what we ingest; mapping the draft seeded 6k+ rows the parser then tombstoned. | `filing_events` metadata-only; the `DEF 14A` → `sec_def14a`. |
| `13F-NT`, `13F-NT/A` | Institutional **notice-only** (manager reports nothing this quarter) — no holdings table. | `filing_events` metadata-only, used for institutional-filer classification. **Note:** dropping NT at discovery is the known gap behind stale-parent 13F double-counts — see `docs/review-prevention-log.md` (Vanguard/AAPL). |
| `144` | Proposed restricted-share sale notice — intent, not an executed transaction. | `filing_events` metadata-only (insider-overhang signal). |
| `11-K` | Employee stock-purchase-plan annual report — no instrument-level ownership. | `filing_events` metadata-only. |
| `25`, `25-NSE`, `15-12B/12G/15D`, `15F*` | Delisting / deregistration — terminal-state signal, no recurring data. | `filing_events` metadata-only. |
| `CORRESP` | SEC ↔ filer correspondence — free text, no structured data. | `filing_events` metadata-only (rare red-flag signal). |
| `D`, `D/A` | Private-placement notice (Reg D) — irrelevant to public-equity investing. | Dropped entirely (not even `filing_events`). |

> **Form 5 is the exception that proves the two-taxonomy split:** `5` / `5/A`
> are `SEC_METADATA_ONLY` at the `filing_events` tier **and** mapped in
> `_FORM_TO_SOURCE` → `sec_form5` (the manifest worker parses them via
> `manifest_parsers/insider_345._parse_form5`). The two sets are independent
> axes; do not assume disjointness.

---

## Template — required sections per source file

```markdown
# <source_name>

**Class.** SEC manifest | SEC ad-hoc | SEC bulk reference | FINRA caller-owned | broker REST.
**Form / endpoint.** <e.g. "Form 4 — insider transactions" / "data.sec.gov/submissions/CIK{cik}.json">

## 1. Origin
URL pattern + HTTP shape + content type + provider module + provider class.

## 2. Watermarking model
What column / row / external_data_watermarks key drives "what is new since last fetch?"
Conditional-GET? `If-Modified-Since`? Pagination cursor? Watermark column on `sec_filing_manifest` / `data_freshness_index` / `external_data_watermarks`.

## 3. Retry posture
404 = benign skip? 403 = also-benign (FINRA pattern)? 429 = back-off (rate limit)? 5xx = exponential? per-fetcher max_retries.

## 4. Bootstrap path
Which `_BOOTSTRAP_STAGE_SPECS` stage seeds it. Cap requirements. Lane. Expected wall-clock band.

## 5. Steady-state path
Which `SCHEDULED_JOBS` cron fires post-bootstrap. Cadence. Lane.

## 6. Manifest insert
Row creation logic. `sec_filing_manifest.source` value. `subject_type` + `subject_id` shape. Option C `filed_at` gate behaviour.

## 7. Parser
Module path + parser class + version + `requires_raw_payload` flag. What it extracts. Drop / skip rules.

## 8. Observation insert
`*_observations` table + column shape + PK + tombstone semantics.

## 9. Current table refresh
`refresh_*_current` helper. MERGE writer (PG17 per #1255). Drift-repair sweep coverage (`_CATEGORIES`).

## 10. Operator-visible endpoint
`/instruments/<symbol>/...` or `/system/...` route + response shape.

## 11. Verification queries
Sample SQL the operator runs to confirm a known instrument has expected data. Smoke command. Cross-source check (gurufocus / marketbeat / etc).

## 12. Smoke test
Path under `tests/smoke/test_etl_source_to_sink.py`. What it asserts.

## 13. Known gotchas
Source-specific traps: rate limits, date-format quirks (e.g. DD-MMM-YYYY), mandate-date cutovers (e.g. 13F PRN drop pre-2023-01-03 + VALUE-cutover 2023-01-03), schema-version cliffs.
```

---

## Cross-cutting invariants

These hold for EVERY source. Skill-enforced via lint.

1. **Every observation write triggers `_current` refresh inline.** No trigger-based write-through (per `data-engineer/SKILL.md §write-through`).
2. **Manifest source enum is the registry.** `ManifestSource` Literal at `app/services/sec_manifest.py:106-122` lists every SEC + FINRA source. Ad-hoc bypasses (currently only `sec_n_cen`) MUST appear in [sec_n_cen.md](sec_n_cen.md) §1 with explicit rationale + alternative path documented.
3. **Rate-limit pools disjoint.** SEC sources on `sec_rate` (10 req/s shared); FINRA on `finra` lane (1 req/s shared); broker on `etoro`.
4. **Idempotent re-ingest.** Every parser path must produce identical output given identical input (deterministic). Re-running ingest never duplicates observations (UPSERT on natural key) or corrupts `_current` (MERGE-based).
5. **Tombstones preserve audit.** Soft-delete via `known_to` column; never hard-DELETE observations.
6. **Two independent status columns on `sec_filing_manifest`.** Source-of-truth at `app/services/sec_manifest.py:132-133`:
   - **`ingest_status`** (`IngestStatus = Literal["pending", "fetched", "parsed", "tombstoned", "failed"]`) — per-accession lifecycle. Normal happy-path: `pending → fetched → parsed`. Intentional skip (retention drop / synth no-op): `pending → tombstoned`. Transient + permanent failure both surface as `failed` with `next_retry_at` set by `_failed_outcome`. Allowed transitions enforced at `app/services/sec_manifest.py:140`.
   - **`raw_status`** (`RawStatus = Literal["absent", "stored", "compacted"]`) — independent of `ingest_status`. Tracks whether the raw payload (XML / iXBRL / HTML) was persisted to `raw_filings.payload`. `absent` = never stored (synth no-op parsers + `requires_raw_payload=False`); `stored` = persisted; `compacted` = retention-sweep replaced payload with hash-only stub.
   Per-source files document the exact transitions in §6 (Manifest insert).
7. **`_CATEGORIES` daily sweep is the integrity floor.** Per `app/jobs/ownership_observations_repair.py:69` — all 7 ownership categories reconcile within 24h regardless of which writer-path populated.

---

## Smoke test

`tests/smoke/test_etl_source_to_sink.py` — parametrized over every source. Tests in scope:

1. **`test_source_has_spec_file`** — every source has a per-source `.md` file.
2. **`test_source_spec_has_required_sections`** — each per-source file contains the 13 required section headers.
3. **`test_ad_hoc_source_has_architectural_exception_section`** — ad-hoc sources (`sec_n_cen`) carry the `## 0. Architectural exception` header.
4. **`test_manifest_source_has_registered_parser`** — every `ManifestSource` Literal entry has a parser registered via `registered_parser_sources()`.

The structural invariants above are the spec-vs-code contract gate. Beyond them, the per-source files §11 (Verification queries) + §12 (Smoke test) describe operator-runnable checks (live HTTP fetches, DB row counts, cross-source comparisons against gurufocus / marketbeat / etc.) — those are intentionally NOT in the import-time smoke gate so the pytest run stays fast + DB-free. Live ETL behaviour is gated by the runbooks under `app/runbooks/` instead (see `docs/operator/runbooks/run-8-readiness.md`).

---

## See also

- `data-engineer/SKILL.md` §write-through — the canonical statement of pipeline invariants.
- `data-engineer/etl-endpoint-coverage.md` — 5-layer wiring matrix (Layer 1/2/3/4 + manifest + freshness).
- `data-sources/sec-edgar.md` — SEC-side gotchas (DD-MMM-YYYY, 13F PRN/SH, 13D/G XML mandate, etc.).
- `data-sources/edgartools.md` — library-version pinning + Pydantic validation cliff.
- `metrics-analyst/SKILL.md` — per-metric source→transform→table→endpoint→chart mapping.
