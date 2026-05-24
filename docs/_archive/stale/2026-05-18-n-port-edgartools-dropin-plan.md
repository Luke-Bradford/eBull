# #932 — N-PORT EdgarTools `FundReport` parser drop-in (impl plan)

> Status: DRAFT 2026-05-18.
>
> Spec: `docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md` (CLEAN through Codex 1a r4).
> Spike: `docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md` (CLEAN through Codex 1a r3).
> Branch: `fix/932-nport-edgartools-spike` (current; will be the impl branch — single PR bundles spike + spec + plan + impl).

## 0. Scope guardrail

Parser-only per #932 issue body. Touchpoints:

- `app/services/n_port_ingest.py` — parse function body + version constant + lazy-import helpers.
- `app/services/manifest_parsers/sec_n_port.py` — UNCHANGED beyond auto-propagation of the version constant.
- `tests/test_n_port_ingest.py` — 7 tests touched + 1 new + 1 helper.
- `tests/test_manifest_parser_sec_n_port.py` — happy-path assertions updated + version-bump assertion added.
- `tests/fixtures/sec/nport_p_test_fund.xml` — REPLACED with real Vanguard 500 fixture.
- `tests/fixtures/sec/nport_p_missing_series.xml` — REWRITTEN to clear EdgarTools structural requirements while still missing `<seriesId>`.
- `.claude/skills/data-sources/edgartools.md` §G3 — REWRITTEN.
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/feedback_pydantic_validation_cliff.md` — UPDATED.
- `docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md` — already on branch (spike doc).
- `docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md` — already on branch (spec).
- This plan doc.

NO changes to:

- Schema (`ownership_funds_*` tables).
- Ingester body (`_ingest_single_accession` write-side guards / tombstone / log).
- Operator-visible rollup endpoint (deferred to #919).
- Submissions-index walker (`parse_submissions_index`).
- DB helpers (`_existing_accessions_for_fund_filer`, `_record_ingest_attempt`, `_resolve_cusip_to_instrument_id`).

## 1. Task ordering

### T1 — Fetch canonical Vanguard 500 fixture

**Goal:** obtain the real Vanguard 500 Index Fund (series `S000002277`) NPORT-P primary doc + record the golden-replay anchor values.

**Steps:**

1. Write a one-shot probe script at `/Users/lukebradford/Dev/eBull/.scratch_nport_fetch.py` (gitignored — deleted post-task):

   ```python
   from app.config import settings
   from app.providers.implementations.sec_edgar import SecFilingsProvider
   from app.services.n_port_ingest import (
       _archive_file_url,
       _submissions_url,
       parse_submissions_index,
   )

   TARGET_SERIES = "S000002277"  # Vanguard 500 Index Fund
   CIK = "0000036405"
   OUT_PATH = "tests/fixtures/sec/nport_p_test_fund.xml"

   with SecFilingsProvider(user_agent=settings.sec_user_agent) as sec:
       subs = sec.fetch_document_text(_submissions_url(CIK))
       refs = parse_submissions_index(subs)
       from edgar.funds.reports import FundReport
       for ref in refs:
           primary = sec.fetch_document_text(
               _archive_file_url(CIK, ref.accession_number, "primary_doc.xml")
           )
           if primary is None:
               continue
           # Parse-then-check (NOT a string search): EdgarTools' parser
           # strips namespaces internally, so prefixed XML
           # (<nport:seriesId>...) parses to general_info.series_id
           # cleanly. A literal `<seriesId>...` string search would miss
           # prefixed payloads.
           try:
               parsed = FundReport.parse_fund_xml(primary)
           except Exception:  # noqa: BLE001 — probe-time tolerance
               continue
           if (parsed["general_info"].series_id or "").strip() != TARGET_SERIES:
               continue
           open(OUT_PATH, "w").write(primary)
           print(f"saved {ref.accession_number}: {len(primary)} bytes; series_id={parsed['general_info'].series_id}")
           break
   ```

2. Run: `uv run python /Users/lukebradford/Dev/eBull/.scratch_nport_fetch.py`. Expected: one Vanguard 500 NPORT-P primary_doc.xml saved at `tests/fixtures/sec/nport_p_test_fund.xml`. SEC HTTP request count: 1 submissions JSON + N primary docs until S000002277 matched (typically ≤10 candidates per Vanguard registrant per quarter).

3. Probe parse_fund_xml against the saved fixture and record the golden-replay anchor values:

   ```python
   from edgar.funds.reports import FundReport
   from decimal import Decimal
   xml = open("tests/fixtures/sec/nport_p_test_fund.xml").read()
   parsed = FundReport.parse_fund_xml(xml)
   inv = parsed["investments"]
   print("count:", len(inv))
   top = max(inv, key=lambda x: x.value_usd if x.value_usd is not None else Decimal(0))
   print("top:", top.name, top.cusip, top.balance, top.units, top.value_usd, top.payoff_profile, top.asset_category)
   print("sum_value_usd:", sum(x.value_usd for x in inv if x.value_usd is not None))
   print("period_end:", parsed["general_info"].fiscal_year_end)
   print("series_id:", parsed["general_info"].series_id)
   print("series_name:", parsed["general_info"].series_name)
   ```

4. Record the 7 anchor values in this plan doc's T1-RESULTS section below. These become the literal assertions in `test_golden_replay_first_row_count_total`.

5. Delete `/Users/lukebradford/Dev/eBull/.scratch_nport_fetch.py`. Verify `git status` shows only `tests/fixtures/sec/nport_p_test_fund.xml` modified.

**Acceptance:** T1-RESULTS table below is filled in with non-placeholder values + the fixture file is on disk.

#### T1-RESULTS

Captured 2026-05-18 via `.scratch_nport_fetch.py` (deleted post-task). Risk-register fallback fired: `S000002277` is not under CIK 36405 (the synthetic fixture's claim was wrong); selected the most recent NPORT-P with ≥100 holdings + complete `valUSD` coverage instead.

| Field | Value |
|---|---|
| Accession | `0000036405-26-000074` |
| Filed date (submissions-index `filingDate`) | `2026-02-26` |
| `period_of_report` (submissions-index `reportDate`) | `2025-12-31` |
| `general_info.cik` | `'0000036405'` |
| `general_info.series_id` | `'S000002840'` |
| `general_info.series_name` | `'VANGUARD VALUE INDEX FUND'` |
| `general_info.fiscal_year_end` | `'2025-12-31'` (string text; `_safe_iso_date` → `date(2025, 12, 31)`) |
| Holdings count | `323` |
| Top holding by `value_usd` — name | `'JPMorgan Chase & Co'` |
| Top holding CUSIP | `'46625H100'` |
| Top holding shares (`balance`) | `Decimal('24052035.00000000')` |
| Top holding `units` | `'NS'` |
| Top holding `value_usd` | `Decimal('7750046717.70000000')` |
| Top holding `payoff_profile` | `'Long'` |
| Top holding `asset_category` | `'EC'` |
| Top holding `issuer_category` | `'CORP'` |
| Sum of all `value_usd` | `Decimal('217419611080.71000000')` |
| Holdings with `value_usd is None` | `0` |

**T4 / T5 / T10 assertion source-of-truth:** every assertion that references `series_id` / `series_name` / `period_end` / top-holding fields / total `value_usd` / holdings count MUST cite these values verbatim. Bonus: the top holding (JPM CUSIP `46625H100`) is in the standard smoke panel (`AAPL` / `GME` / `MSFT` / `JPM` / `HD`) — the manifest happy-path test can seed JPM's CUSIP mapping and assert at least one landed row using the canonical smoke-panel CUSIP.

### T2 — Write the new missing-series fixture

**Goal:** rewrite `tests/fixtures/sec/nport_p_missing_series.xml` per spec §4.2 concrete shape.

**Steps:**

1. Replace file content with the spec §4.2 XML verbatim (regName + regCik + regFileNumber + regStreet1 all present with non-empty text; `<seriesId>` deliberately omitted; 9 fundInfo Decimal fields = 0; returnInfo with empty monthlyTotReturns + 3 empty othMon tags; filerInfo + issuerCredentials with cik/ccc).

2. Verify behaviour: `uv run python -c "from edgar.funds.reports import FundReport; from app.services.n_port_ingest import parse_n_port_payload, NPortMissingSeriesError; xml = open('tests/fixtures/sec/nport_p_missing_series.xml').read(); FundReport.parse_fund_xml(xml); print('parse_fund_xml CLEAN')"` — confirms EdgarTools doesn't crash on this fixture. (Wrapper not yet implemented at this task; we're verifying the fixture alone.)

3. After T3 lands, verify `parse_n_port_payload(xml)` raises `NPortMissingSeriesError`.

**Acceptance:** fixture written; T2 verification command succeeds.

### T3 — Implement the wrapper

**Goal:** replace `parse_n_port_payload` body per spec §3.1 verbatim.

**Steps:**

1. Modify `app/services/n_port_ingest.py`:

   - Bump `_PARSER_VERSION_NPORT = "nport-v1"` → `"nport-v2-edgartools"` (line 75).
   - Add module-level `from typing import Any` (already imported).
   - Replace `parse_n_port_payload(xml: str) -> NPortFiling` body (lines 358-454) with the spec §3.1 implementation. The function signature + docstring (rewritten per spec §3.1) preserve the same public contract.
   - Add three module-level factory functions after `parse_n_port_payload`:
     - `_edgar_fund_report() -> Any` (lazy import of `FundReport`).
     - `_pydantic_validation_error() -> type[Exception]` (lazy import of `ValidationError`).
     - `_header_issuer_cik(header: Any) -> str | None` (best-effort attribute walk).
   - Remove unused helpers: `_stripns`, `_find_text`, `_children_by_local_name`, `_decimal_or_none`. Grep-verify no other caller:

     ```bash
     # Removed helpers — must return ZERO matches after removal
     # (definitions + all call-sites gone).
     rg -n "\b(_stripns|_find_text|_children_by_local_name|_decimal_or_none)\b" app tests

     # Parser callers + version constant — must cover the manifest
     # adapter's direct symbol import at
     # app/services/manifest_parsers/sec_n_port.py:60-68 plus the call
     # site at :182.
     rg -n "parse_n_port_payload|_PARSER_VERSION_NPORT" app tests
     ```

     - First command (removed helpers): expect **zero matches** after removal.
     - Second command (`parse_n_port_payload` + `_PARSER_VERSION_NPORT`): expect call-sites at `app/services/n_port_ingest.py` (definition + version-constant def), `app/services/manifest_parsers/sec_n_port.py` (direct import + 7 version-constant uses), `tests/test_n_port_ingest.py`, `tests/test_manifest_parser_sec_n_port.py`. No other callers.
   - Remove module-level `import xml.etree.ElementTree as ET` (line 50) AND the `# noqa: S405` comment if `ET` is no longer referenced anywhere in the file. Verify via grep.
   - Keep `from decimal import Decimal, InvalidOperation` — both still used (Decimal for typing, InvalidOperation for the wrapper's catch clause).

2. Verify import side-effect remains:

   ```bash
   uv run python -c "import app.services.n_port_ingest; print('module import clean')"
   ```

   No EdgarTools side-effect on bare module import.

3. Verify lazy-import path:

   ```bash
   uv run python -c "
   from app.services.n_port_ingest import parse_n_port_payload, _edgar_fund_report
   fr = _edgar_fund_report()
   print('FundReport:', fr.__name__)
   "
   ```

**Acceptance:** wrapper body in place; both verification commands succeed.

### T4 — Update existing N-PORT tests

**Goal:** apply the test surgery in spec §5.

**Steps:**

1. **`TestParseNPortPayload::test_extracts_header_and_holdings`** — rewrite to assert against T1-RESULTS anchor values. Tolerant assertions on series name (case-insensitive startswith). `filed_at is None` (per wrapper contract).

2. **`TestParseNPortPayload::test_raises_missing_series_id`** — UNCHANGED (uses the new missing-series fixture from T2).

3. **`TestParseNPortPayload::test_raises_on_malformed_xml`** — verify behaviour with `"<not><well><formed>"`. If EdgarTools' `recover=True` parser silently succeeds and the failure surfaces only inside the wrapper's normalisation try-block (via `KeyError` on `parsed["general_info"]` or similar), the test still passes — the wrapper's catch-list converts to `NPortParseError`. If it doesn't surface as `NPortParseError`, change the malformed input to `"<edgarSubmission>X</edgarSubmission>"` or similar that DOES survive recover-mode but fails normalisation.

4. **`TestParseNPortPayload::test_runs_offline_no_network`** — unchanged structure; uses T1 fixture; same `urllib.request.urlopen` + `httpx` patches; asserts `parsed.series_id == <T1-RESULTS series_id>` (literal from T1-RESULTS, NOT hardcoded `"S000002277"` — same source-of-truth rule as the risk-register fallback note).

5. **`TestParseNPortPayload::test_golden_replay_first_row_count_total`** — NEW. Locks: top-by-`value_usd` holding's cusip + shares + value_usd, total holdings count, and `sum(h.value_usd for h in parsed.holdings if h.value_usd)`. All values from T1-RESULTS table.

6. **`TestIngestFundNPort` — three refactored tests**:

   For each of `test_ingest_drops_non_equity_short_unresolved`, `test_idempotent_reingest`, `test_amendment_uses_submissions_filed_at_when_header_missing`:

   - Replace the `fixture_xml = ...` line with construction of an `NPortFiling` via the new `_make_filter_path_filing()` helper (or two filings for the amendment test).
   - Replace the `fetcher = _FakeFetcher({primary_url: fixture_xml, ...})` with a `_FakeFetcher` that still returns a real (or stub) XML for the primary_url (so `raw_filings.store_raw` writes a row), but with `monkeypatch.setattr("app.services.n_port_ingest.parse_n_port_payload", lambda xml: filing)` per-test. Use a small stub XML or copy the T2 missing-series fixture (the parser is monkeypatched so its content doesn't matter — the bytes are stored in `filing_raw_documents` but never re-parsed in-test).

   **Monkeypatch target rule** (guardrail):
   - For ingester-driven tests through `_ingest_single_accession`: target `app.services.n_port_ingest.parse_n_port_payload` (module-local global lookup).
   - For manifest-adapter-driven tests: target `app.services.manifest_parsers.sec_n_port.parse_n_port_payload` — the adapter imports the symbol with `from app.services.n_port_ingest import parse_n_port_payload` at line ~60, binding it into its own namespace. A service-side monkeypatch does NOT affect the manifest adapter's reference. Current T4 + T5 plan does not require manifest-side monkeypatching (manifest tests stay on the real parser), but the guardrail is documented here for future work.
   - For `test_ingest_drops_non_equity_short_unresolved`, add assertion `assert row[0] == 1 and parser_version_row[0] == "nport-v2-edgartools"` on the `filing_raw_documents` row (parser-version bump regression guard).

7. **`TestIngestFundNPort::test_missing_series_id_tombstones_failed`** — UNCHANGED (real parser path through the updated missing-series fixture).

8. **Add `_make_filter_path_filing` helper** at module-level per spec §5.8.

9. **TestFundsSchema / TestIngestAllStatusPrecedence / TestManifestFormCodes / test_accession_ref_dataclass_is_frozen** — UNCHANGED.

10. **TestParseSubmissionsIndex / TestRecordFundObservation** — UNCHANGED.

**Acceptance:** `uv run pytest tests/test_n_port_ingest.py -x -q` passes.

### T5 — Update manifest-parser tests

**Goal:** apply spec §5.7.

**Steps:**

1. `tests/test_manifest_parser_sec_n_port.py`:

   - `_FAKE_NPORT_XML` continues to point at the same path; the file content changed in T1.
   - Happy-path test: row-count + specific-CUSIP assertions need updating to match the real-fixture distribution. Seed CUSIP mappings for the top-N holdings from T1-RESULTS (at least AAPL + MSFT + the actual top holding). Assert at least 1 holding row landed in `ownership_funds_current` (NOT a specific count — the real Vanguard 500 has ~500 holdings, the seeded CUSIPs determine landed rows; assertion should be tolerant).
   - Parser-version assertion: add `parser_version == "nport-v2-edgartools"` to every `filing_raw_documents` assertion site.
   - Missing-series tombstone test: UNCHANGED structure; the fixture content updated in T2 still raises `NPortMissingSeriesError`.
   - Malformed-XML test: same posture as T4 §3.

2. Run: `uv run pytest tests/test_manifest_parser_sec_n_port.py -x -q`.

**Acceptance:** all manifest-parser tests pass.

### T6 — Skill update

**Goal:** rewrite `.claude/skills/data-sources/edgartools.md` §G3 per spec §6.1.

**Steps:**

1. Replace the §G3 block. Verify in-file references (line numbers, links) remain valid.
2. Add a back-reference to the spike doc: `docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md`.

**Acceptance:** rg confirms `#932` + `2026-05-18-n-port-edgartools` appear in §G3.

### T7 — Memory update

**Goal:** update `feedback_pydantic_validation_cliff.md` per spec §6.2.

**Steps:**

1. Add a section noting the #932 case where the OBSERVED failure was structural (synthetic fixture) but a SEPARATE internal Pydantic cliff remains theoretically possible.
2. Cross-link to `[[us-source-coverage]]` and the spike doc.

**Acceptance:** memory file mentions #932 + the structural-vs-Pydantic distinction.

### T8 — Local gates

**Goal:** pass the pre-push checklist.

**Steps:**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/test_n_port_ingest.py tests/test_manifest_parser_sec_n_port.py -x -q
uv run pytest tests/smoke/test_app_boots.py -x -q
uv run pytest -x -q
```

All six must pass.

Pyright-specific check: the wrapper uses `Any` for `_edgar_fund_report() -> Any` to defer the FundReport type-import. If pyright complains about return-type inference at the call site, add `# type: ignore[no-any-return]` only at the specific call site, never module-wide.

**Acceptance:** all six commands exit 0.

### T9 — Codex 2 pre-push review

**Goal:** non-negotiable per CLAUDE.md Codex checkpoint #2.

**Steps:**

```bash
codex exec "Review diff on branch fix/932-nport-edgartools-spike against:
  - spec: docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md
  - spike: docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md
  - plan: docs/superpowers/plans/2026-05-18-n-port-edgartools-dropin-plan.md

Focus on:
- Wrapper try/except scope (does any path bypass the catch list?).
- _PARSER_VERSION_NPORT bump propagation through manifest adapter
  (direct import at app/services/manifest_parsers/sec_n_port.py:60-68
  + 7 call-sites).
- Manifest adapter holds a direct-imported parser reference; verify no
  service-side monkeypatch accidentally relies on patching the adapter
  too.
- Test refactor coverage: do the monkeypatched tests still exercise
  every assertion the original tests exercised?
- Fixture replacement: does the new missing-series fixture actually
  clear EdgarTools structural requirements (filerInfo + 9 fundInfo
  decimals + othMon1/2/3 + GeneralInfo non-Optional text fields)?
- Helper cleanup: are the removed helpers truly unused?
- PR body field coverage: Closes #932 / What / Why / Security model /
  Test plan / ETL DoD clauses #8-#12 framing / golden fixture
  accession + anchor values / rollback path.
Reply terse."
```

Fix anything real before pushing.

**Acceptance:** Codex CLEAN OR all findings addressed.

### T10 — Push + PR

**Goal:** open PR with self-contained body per CLAUDE.md PR discipline.

**Steps:**

1. `git add -A` then `git status` to confirm staged set matches §0 scope.
2. Commit:

   ```text
   feat(#932): adopt EdgarTools as N-PORT FundReport parser drop-in

   Wraps app/services/n_port_ingest.py::parse_n_port_payload around
   edgar.funds.reports.FundReport.parse_fund_xml (lazy-imported, same
   pattern as #925's 13F-HR drop-in). Preserves the NPortFiling /
   NPortHolding public dataclass surface so the manifest-adapter +
   ingester bodies stay unchanged.

   ...
   Closes #932.

   Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
   ```

3. `git push -u origin fix/932-nport-edgartools-spike`.
4. `gh pr create` with body covering:
   - **Closes #932** (auto-close trigger word; CI enforced per `feedback_pr_auto_close_required.md`).
   - **What changed**: wrapper, fixture replace (named accession + anchor values from T1-RESULTS), missing-series fixture rewrite, parser-version bump (`nport-v1` → `nport-v2-edgartools`), skill + memory update.
   - **Why**: per #932 issue body + spike findings.
   - **No schema migration**: parser-only scope; `ownership_funds_*` tables untouched.
   - **Security model**: no new HTTP path; rate-limit pool preserved (`SecFilingsProvider._http`); raw-payload persistence ordering at `_ingest_single_accession:786-798` unchanged; EdgarTools `parse_fund_xml` is pure-string-in / dict-out.
   - **Manifest adapter propagation**: `app/services/manifest_parsers/sec_n_port.py` direct-imports `_PARSER_VERSION_NPORT` (line 61) + `parse_n_port_payload`; the version bump auto-propagates to 7 call-sites (lines 87 / 111 / 155 / 166 / 205 / 403 / 410). No adapter changes needed.
   - **Test plan**: `pytest tests/test_n_port_ingest.py tests/test_manifest_parser_sec_n_port.py` + golden replay test pins T1 anchor values.
   - **ETL DoD clauses #8-#12 framing**: smoke N/A (no operator-visible figure surfaces N-PORT in v1; rollup deferred to #919); cross-source = independent raw XML spot-check (true cross-source N/A until #919); backfill = operator follow-up via `POST /jobs/sec_rebuild/run` body `{"source": "sec_n_port"}` post-merge.
   - **Rollback / backfill / rewash path**: revert via `git revert <merge SHA>` is safe (parser-version constant goes back to `nport-v1`, manifest worker re-drains via the same mismatch detection). Operator-side rewash post-merge is a single endpoint call (no schema rollback needed).
   - **Golden fixture anchor values**: T1-RESULTS table reproduced verbatim from plan (accession + filed date + count + top holding + total).

**Acceptance:** PR opened; Claude bot review fires.

### T11 — Bot review resolution

**Goal:** drive to APPROVE on latest commit + CI green.

**Steps:**

1. Poll `gh pr view <N> --comments` + `gh pr checks <N>`.
2. Triage each finding into FIXED / DEFERRED / REBUTTED per CLAUDE.md.
3. Fix in-scope per `feedback_fix_in_scope_default.md`. No new tech-debt tickets per plan §1 autonomy contract.
4. Re-push; restart poll loop.
5. On rebuttal-only round: Codex checkpoint #3 before merge.

**Acceptance:** APPROVE on most recent commit + CI green.

### T12 — Merge + handover

**Goal:** close the loop.

**Steps:**

1. Squash + merge.
2. Delete local + remote branch.
3. Update `docs/superpowers/plans/2026-05-17-us-etl-completion.md` Phase 5 close-out handover.
4. Update `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/MEMORY.md` if a new project memory file is needed (probably just an update to the existing US source coverage entry).

**Acceptance:** branch deleted; plan doc updated; main reflects the merge SHA.

## 2. Risk register

| Risk | Mitigation |
|---|---|
| T1 fetch fails for `S000002277` (e.g. CIK 36405 retired the series, or no recent NPORT-P) | Fall back to any other `S0000xxxxx` series under CIK 36405 (Vanguard Index Funds). **All T4 / T5 / T10 assertions are derived from T1-RESULTS verbatim** — if a fallback series is used, every `S000002277` literal in those tasks is rewritten to the actual chosen series_id (e.g. `parsed.series_id == "<actual_series_id>"`). T1-RESULTS is the single source of truth; no test or PR-body literal should hardcode `S000002277` independent of T1-RESULTS. The risk-mitigation does NOT relax to a "starts with S000" pattern — exact-equality lock is preserved against whatever series T1 selected. |
| Real-fixture has missing `<valUSD>` cells | Wrapper's `pydantic.ValidationError` catch routes to `NPortParseError` → accession-level tombstone. If observed during T1 probe, retry with a different accession (newer = less likely to have placeholder cells). |
| T4 §3 malformed-XML test surprises (recover-mode parser silently passes) | Spec §5.1 explicitly anticipates; impl pivots to a different malformed input that still fails. |
| Pyright complains about `Any`-typed FundReport return | Module-level helpers annotated as `Any`; pyright accepts. If a specific call-site complains, narrow with `# type: ignore[<rule>]` at that site only. |
| Manifest test's row-count assertion is too tight | Spec §5.7 allows tolerance — assert ≥ 1 landed row, not exact count. |
| `_make_filter_path_filing` helper carries an unintended invariant violation (e.g. CUSIP not length 9) | Per-test assertion fails loudly; helper is in same file as the test, fix-in-scope. |
| Bot flags missing operator-visible figure (ETL DoD #11) | PR body explicitly records N/A status — rollup is #919 deferred. REBUTTED with §"#919 deferred" reasoning. |
| Bot flags the parser-version bump as un-tested in legacy path | T4 §6 adds the explicit parser-version assertion in `test_ingest_drops_non_equity_short_unresolved`. |
| Manifest adapter direct-imports `parse_n_port_payload` into its own namespace; service-side `monkeypatch.setattr("app.services.n_port_ingest.parse_n_port_payload", ...)` does NOT affect the adapter | Manifest tests use the real parser (T5 keeps real-fixture path). T4 §6 guardrail rule documents this. Future contributor adding manifest-side monkeypatch must target `app.services.manifest_parsers.sec_n_port.parse_n_port_payload`. |
| T1 fixture selection misses prefixed XML (`<nport:seriesId>...`) | T1 step 1 uses `FundReport.parse_fund_xml(primary)` parse-then-check (NOT string search) — EdgarTools strips namespaces internally so prefixed payloads parse cleanly. |

## 3. Out of scope (explicit)

- Schema migrations.
- Ingester body changes (`_ingest_single_accession`, write-side guards).
- Rollup endpoint (#919).
- N-CSR / Form 3/4/5 / 13D-G EdgarTools drop-ins.
- New tech-debt tickets per plan §1 autonomy contract.
- Probing FundReport against non-Vanguard / amendment / oddball fund families (impl risk-accepts the Vanguard-equity-index shape; future regressions surface as accession-level tombstones with operator-visible counts).

## 4. References

- Spec: `docs/superpowers/specs/2026-05-18-n-port-edgartools-dropin.md`.
- Spike: `docs/superpowers/spikes/2026-05-18-n-port-edgartools-feasibility.md`.
- Issue: `gh issue view 932`.
- Parent plan: `docs/superpowers/plans/2026-05-17-us-etl-completion.md` §2 Phase 5 PR 10.
- Sibling PR #931: `gh pr view 931` (13F-HR drop-in, merged 2026-05-05, commit `0428dbf`).
