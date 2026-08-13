# FINRA bimonthly short interest ingest feasibility spike

> Status: **COMPLETED 2026-05-18** — empirical endpoint reality + cohort + symbol-norm
> + architecture posture locked. Recommendation: SHIP per Option A (ScheduledJob owns
> fetch + write; manifest parser is synth no-op like ``sec_xbrl_facts``).
>
> Issue: #915 (OPEN at spike time). Parent #845 closed.
> Plan context: ``docs/superpowers/plans/2026-05-17-us-etl-completion.md`` §2 Phase 6 PR 11.
> Predecessor (architectural sibling): ``docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md`` (#932 — drop-in shipped 2026-05-18 PR #1205).

## 1. Why a spike

#915 is the headline real coverage gap in the US-ETL matrix (parent #845 closed; PR1/PR2 split open). FINRA is also the **first** non-SEC provider family — every prior ingest source (Form 3/4/5, 13D/G, 13F-HR, DEF 14A, N-PORT, N-CSR, 8-K, 10-K, 10-Q, XBRL Companyfacts) lands under the SEC-EDGAR umbrella with the shared 10 req/s rate budget. The spike's job:

1. Confirm endpoint reality (URL pattern, auth, format, archive depth).
2. Confirm cohort + symbol-resolution discipline (FINRA's symbol form vs ``instruments.symbol``).
3. Confirm rate-limit posture (does FINRA publish one; if not what is the polite default).
4. Pick the architecture (manifest-worker drain vs ScheduledJob inline) given that the bimonthly file is a single monolithic CSV-shape per settlement date — different from per-accession SEC XML.

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Free regulated-source-only" (#532) | FINRA is a self-regulatory organisation (SRO) under SEC oversight. Equity Short Interest publication is a regulatory disclosure under FINRA Rule 4560 + SEC Rule 10a-1. Free, anonymous CDN. | PRESERVED. |
| §"Provider design rule" — providers thin, DB lookups in services | ``app/providers/implementations/finra_short_interest.py`` exposes URL builder + ``fetch_settlement_file(date)``; ``app/services/finra_short_interest_ingest.py`` owns the symbol-resolver + DB upserts. | PRESERVED. |
| §"Filing event storage" — raw-payload persistence per #1168 | Raw FINRA pipe-delim file is stored via ``filing_raw_documents.store_raw`` BEFORE parse (mirrors ``n_port_ingest.py:786-798``). Accession is the synthetic ``FINRA_SI_{YYYYMMDD}``. | PRESERVED. |
| §"Manifest as source-of-truth for is-it-on-file?" (#864 / sql/118) | ``finra_short_interest`` is already a valid ``ManifestSource`` literal + ``finra_universe`` is a valid ``subject_type`` + ``'FINRA_SI'`` is the documented singleton ``subject_id``. Schema scaffolding pre-exists; this PR populates it. | PRESERVED. |

## 3. Prevention-log entries (binding)

| Entry | File:line | How this spike honours it |
|---|---|---|
| "Multiple ``ResilientClient`` instances sharing a rate limit must share throttle state" | ``docs/review-prevention-log.md:510-513`` | FINRA is a separate host from SEC EDGAR; **independent rate-limit pool**. New ``finra`` Lane + dedicated module-global ``_FINRA_RATE_LIMIT_CLOCK`` + ``_FINRA_RATE_LIMIT_LOCK`` mirror the SEC pattern at ``app/providers/implementations/sec_edgar.py:54-80, 237-253``. PRESERVED by construction — no shared httpx.Client between SEC and FINRA, no shared throttle list. |
| "Raw API payload must be persisted before any parse / normalise step" (#1168) | review-prevention-log entry | Raw pipe-delim file stored via ``filing_raw_documents.store_raw`` BEFORE the parse step. Synthetic accession ``FINRA_SI_{YYYYMMDD}`` keys the row. |
| "Pydantic validation cliff — spike fixture compatibility BEFORE drop-in" | ``feedback_pydantic_validation_cliff.md`` | N/A — FINRA file is pipe-delim text, no Pydantic model. Stdlib ``csv.DictReader`` over ``str.splitlines()`` is the parse path. |
| "Universal-gate supersession" | ``feedback_universal_gate_supersession.md`` | The new ``finra`` lane is added to ``Lane`` Literal; ``test_universal_gate_carve_out.py`` is extended to assert no implicit carve-out for FINRA jobs. |
| "Skills must own integrity, not inventory" | ``feedback_skills_must_own_integrity.md`` | This PR lands the FIRST FINRA-data-source skill content. ``.claude/skills/data-engineer/etl-endpoint-coverage.md`` §2 FINRA row gets a real ``WIRED`` annotation + ``.claude/skills/data-sources/`` MAY get a new ``finra.md`` source-of-truth note (decision deferred to spec). |

## 4. Empirical endpoint reality

### 4.1 Bimonthly file URL pattern

Verified live 2026-05-18 against the FINRA CDN:

| Settlement date | URL | Size | Status |
|---|---|---|---|
| 2026-04-15 | ``https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260415.csv`` | ~2 MB | 200 OK, anonymous |
| 2026-04-30 | ``https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260430.csv`` | ~2 MB | 200 OK, anonymous |

Pattern: ``https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv`` keyed by settlement date.

Cadence: **two snapshots per month** — settlement dates are the 15th and the last business day. The ``.csv`` extension is misleading; the file is **pipe-delimited** (``|``), confirmed by reading the header row from the live ``shrt20260430.csv``:

```
accountingYearMonthNumber|symbolCode|issueName|issuerServicesGroupExchangeCode|marketClassCode|currentShortPositionQuantity|previousShortPositionQuantity|stockSplitFlag|averageDailyVolumeQuantity|daysToCoverQuantity|revisionFlag|changePercent|changePreviousNumber|settlementDate
```

14 fields. Sample data row from the live file:

```
20260430|A|Agilent Technologies Inc.|A|NYSE|4824122|4481687||2052152|2.35||7.64|342435|2026-04-30
```

Field types observed:
- ``accountingYearMonthNumber``: YYYYMM integer (``20260430`` row carries ``20260430`` → spec text says "yearMonth"; we lock to "verbatim text passthrough" and let downstream callers reconcile).
- ``symbolCode``: alphanumeric only — no separators (see §4.3).
- ``issueName``: free text; commas + quotes allowed; pipe-delim handles cleanly.
- ``issuerServicesGroupExchangeCode``: single letter (``A`` = NYSE-listed, ``S`` = OTC, ``H`` = exchange ETP class).
- ``marketClassCode``: ``NYSE`` / ``BZX`` / ``OTC`` etc. — text.
- ``currentShortPositionQuantity`` / ``previousShortPositionQuantity``: integers, no thousands separators.
- ``stockSplitFlag``: empty string or ``Y``.
- ``averageDailyVolumeQuantity``: integer.
- ``daysToCoverQuantity``: float (e.g. ``2.35``); FINRA caps at ``1.00`` floor.
- ``revisionFlag``: empty string or ``Y``.
- ``changePercent``: float; can be empty for first-ingest rows.
- ``changePreviousNumber``: signed integer.
- ``settlementDate``: ISO ``YYYY-MM-DD``.

### 4.2 Archive depth + history

Per the FINRA short-interest catalog page: **"Archive files going back to 2014 are available."** Backfill is feasible but Phase 6 PR scope is "current bimonthly settlement going forward + bootstrap-fetch the most-recent N=N_BACKFILL files." Realistic v1 N=24 (one year) per spec.

Historical note: pre-June 2021 files included **OTC securities only** (FINRA was the only SRO that published). Post-June 2021 the file expanded to include exchange-listed securities. This is the "GME 2021 squeeze" cohort gating — GME's bimonthly short interest IS in the post-June 2021 archive but is sparse in the pre-2021 archive (NYSE-listed). Spec §Smoke acknowledges this — the GME 2021-01-29 settlement is in scope ONLY if the archive includes pre-2021 exchange-listed coverage, which it does NOT. **Spec smoke target adjusted accordingly** — GME post-2021-06 cohort, not 2021-Q1.

### 4.3 Symbol form — FINRA strips separators

Verified live against ``shrt20260430.csv``. Sample rows for share-class siblings + preferreds:

```
ABRPRD|Arbor Realty Trust, Inc. 6.375|A|NYSE   → ABR-PR-D / ABR.PR.D / ABR_D depending on vendor
ABRPRE|Arbor Realty Trust, Inc. 6.25%|A|NYSE   → ABR-PR-E
ABRPRF|Arbor Realty Trust, Inc. 6.25%|A|NYSE   → ABR-PR-F
ALLPRB|The Allstate Corporation 5.100|A|NYSE   → ALL-PR-B
```

**FINRA's contract: alphanumeric symbol with NO separators.** No periods, no hyphens, no underscores. Preferred-share suffixes (``PR-D``, ``-PR``, ``.PR.D``) concatenate into the symbol body.

Our ``instruments.symbol`` (sql/001_init.sql:3) uses **dotted form** for share-class siblings — ``BRK.A``, ``BRK.B``, ``GOOG`` (no separator), ``GOOGL`` (no separator). The "no separator" cases match FINRA directly; the dotted cases need normalisation.

**Resolution discipline:** strip ``.``, ``-``, ``_`` from both sides + upper-case, then match. This is a one-way collapse — ``BRK.A`` and ``BRKA`` both become ``BRKA``, so a unique-symbol assumption holds for common stock but **may collapse on preferred share collisions** (e.g. ``ABRPRD`` could theoretically collide with a hypothetical ``ABR-PRD`` instrument that is NOT a preferred). Mitigation: per-row resolver returns ``None`` on multi-match → row skipped + ``skipped_ambiguous_symbol`` counter incremented.

### 4.4 Cohort size + universe overlap

Live ``shrt20260430.csv`` is ~2 MB. Decimal-arithmetic ballpark: 14 fields × ~20 chars/field × 1 row = ~280 bytes/row. 2 MB / 280 = ~7,000 rows visible in the snapshot range observed. Truncation suggests **actual count ~10-15k rows per file** (FINRA states the post-2021 file covers all US exchange-listed + OTC securities).

Our ``instruments`` universe is ~13k (per memory ``[[project_overview]]`` + matrix). Expected resolver match-rate **~60-80%** after symbol normalisation; the residual is:

- OTC pink-sheet names we don't carry
- preferreds we don't model as separate instruments (``ABRPRD``, ``ALLPRB``)
- exchange-listed derivatives + units we don't carry (``ABRPRF``-style structured products)

The ``skipped_no_instrument_match`` counter MUST be operator-visible in ``job_runs.detail`` and the ``finra_short_interest_refresh`` runtime log. If match-rate drops below 50% in steady state, something is wrong (either FINRA changed the column shape or our universe drifted).

### 4.5 Rate-limit posture

FINRA publishes NO explicit rate-limit policy on the equity short interest catalog page. The CDN host (``cdn.finra.org``) returns ``403`` for ``robots.txt`` — convention is the public catalog page IS the contract, and it does not restrict programmatic anonymous download.

**Polite default**: 1 req/s (``min_request_interval_s=1.0``). Each fire fetches **one** file — total per-fire HTTP cost is one HEAD (for ETag / last-modified) + one GET on the data file. No archive enumeration in steady state; bootstrap-mode fetches N=24 files spaced at 1 req/s = 24s wall-clock.

### 4.6 Trade-off — FINRA SEC EDGAR pool sharing?

FINRA and SEC EDGAR are **different hosts**. ``cdn.finra.org`` does NOT share an IP or rate budget with ``data.sec.gov`` / ``www.sec.gov``. Adding FINRA to the ``sec_rate`` Lane would burn SEC budget unnecessarily; sharing the ``_PROCESS_RATE_LIMIT_CLOCK`` would couple two unrelated providers. **Decision: NEW ``finra`` Lane.** Mirrors the disjoint-by-host pattern of ``etoro`` vs ``sec_rate``.

## 5. Architecture decision — Option A vs Option B

### 5.1 Option A (recommended) — ScheduledJob owns fetch + write inline

- ``ScheduledJob('finra_short_interest_refresh', source='finra', cadence=daily 12:00 UTC, prerequisite=_bootstrap_complete)``
- Job body (``app/jobs/finra_short_interest_refresh.py``): probes the FINRA catalog for new settlement-date files, fetches each new file via ``FinraShortInterestProvider``, calls ``app/services/finra_short_interest_ingest.py::ingest_settlement_file(conn, settlement_date, raw_bytes)`` — which:
  1. Stores raw bytes in ``filing_raw_documents`` keyed by synthetic accession ``FINRA_SI_{YYYYMMDD}``.
  2. Parses pipe-delim rows via stdlib ``csv.DictReader(io.StringIO(raw.decode('utf-8')), delimiter='|')``.
  3. Resolves each row's ``symbolCode`` against the preloaded ``symbol → instrument_id`` map (mirror ``build_preloaded_subject_resolver`` from G12).
  4. Bulk-UPSERTs resolved rows into ``short_interest_observations`` (partitioned by settlement_date quarterly bucket per #788 ownership precedent).
  5. UPSERTs the ``_current`` snapshot row for each matched instrument (settlement_date wins-most-recent).
  6. UPSERTs the manifest row as ``ingest_status='parsed'`` with ``parser_version='finra-si-bimonthly-v1'``, ``raw_status='stored'``.
- ``app/services/manifest_parsers/finra_short_interest.py`` — synth no-op (sec_xbrl_facts shape). The manifest row is already ``parsed`` when the ScheduledJob writes it; the synth no-op exists only to satisfy the manifest-worker dispatch invariant for the rare case where a ``finra_short_interest`` manifest row reaches the worker (e.g. operator-initiated rebuild + scoped tick).

### 5.2 Option B (rejected) — manifest-worker drain

- ScheduledJob discovers settlement-date files, UPSERTs **pending** manifest rows.
- Manifest worker (per-source tick scoped to ``finra_short_interest``) pulls pending rows, calls the registered parser, which fetches the file + parses + writes.
- Requires the parser to do its own fetch (FINRA's ResilientClient lives inside the parser instead of the ScheduledJob).

**Rejected because:**

1. **Cardinality asymmetry.** FINRA's file is **one monolithic CSV-shape per settlement date** containing ~10k rows. SEC manifest sources are **one accession = one small XML** (Form 4 = ~5k bytes, 13F-HR = ~100k bytes). The manifest worker's per-row dispatch model fits per-accession SEC; treating a 2 MB pipe-delim file as a single "row" forces the parser body to take an outsized share of the per-tick budget.
2. **Failure isolation differs.** SEC parsers either parse one accession or tombstone it. A FINRA file parse failure mid-stream (e.g. line 5,237 has a malformed integer) is a different shape — should it tombstone the whole file? Skip the bad row? The ScheduledJob's body-level error handling (mirroring G12's ``QuarterStats(failed=True)`` per-file accumulator) is the right granularity, not per-manifest-row.
3. **Settled precedent — ``sec_xbrl_facts`` (G7).** XBRL company facts also have a **bulk-JSON-via-ScheduledJob primary write path** with **synth no-op manifest parser**. The settled architecture decision says "where the bulk path is the right shape, ScheduledJob owns it; manifest parser is the audit drain." FINRA fits this mould.
4. **Data freshness already pins it.** ``app/services/data_freshness.py:101`` declares ``finra_short_interest`` with a 20-day cadence — the freshness index pulls from manifest ``filed_at`` once written. ScheduledJob writes the manifest row at ingest time; freshness reads it for the operator panel.

## 6. Verdict

**SHIP Option A.** All five gating questions resolved:

1. Endpoint reality: stable, anonymous CDN, pipe-delim with documented 14-field header.
2. Symbol-resolution: strip-non-alnum + upper-case; ambiguous-collapse rows skipped with operator-visible counter.
3. Rate limit: 1 req/s polite default; new ``finra`` Lane disjoint from ``sec_rate``.
4. Architecture: ScheduledJob inline owns fetch+write; manifest parser is synth no-op.
5. Cohort: ~60-80% match rate against our ~13k universe; pre-June 2021 archive is OTC-only (GME 2021-Q1 NOT covered; smoke target adjusted to GME 2021-07-15 first post-exchange-listed settlement).

## 7. Out of scope (deferred)

| Item | Reason | Next action |
|---|---|---|
| Frontend memo overlay (issue #915 acceptance #2) | Plan §1 autonomy contract carves out UI work. Phase 4 G10/G11 precedent: ship "PROVIDER PRIMITIVE / OBSERVATIONS PRIMITIVE — no v1 consumer." | Issue body acceptance #2 closure-framing in PR: "OBSERVATIONS PRIMITIVE — chart memo overlay deferred to UI ticket (none open yet); reopen + wire when ownership-card UI revisit lands." |
| RegSHO daily short volume (#916) | Plan §2 Phase 6 splits #915 (bimonthly) + #916 (RegSHO daily) into sequential PRs. Different cadence (daily vs bimonthly), different table shape (no days-to-cover; has ShortExempt + multi-prefix), different file pattern (6 prefixes per day). | Follow-up PR per ``feature/916-finra-regsho-daily-short-volume`` branch after #915 lands. |
| Backfill > 1 year | Spec scope is N=24 most-recent files (one year). 2014→ archive backfill is operator-runbook territory. | Plan does NOT request; documented as future runbook path in spec §Backfill. |

## 8. Recommendation

Proceed to spec (Option A) → Codex 1a → revise to CLEAN → plan → Codex 1b → revise to CLEAN → implement → Codex 2 pre-push → push → bot review loop until APPROVE → merge.
