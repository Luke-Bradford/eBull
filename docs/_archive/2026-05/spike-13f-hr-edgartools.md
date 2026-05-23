# 13F-HR EdgarTools drop-in feasibility re-spike

> Status: **COMPLETED 2026-05-18** — findings + verdict gathered; recommendation locked.
>
> Issue: #925 (OPEN at spike time; closed by this PR).
> Plan context: `docs/superpowers/plans/2026-05-17-us-etl-completion.md` §2 Phase 5 PR 9.
> Predecessor: PR #931 (`feat(#925): adopt EdgarTools as 13F-HR parser drop-in`, merged 2026-05-05, commit `0428dbf`).

## 1. Context recap

Plan §2 Phase 5 PR 9 framed #925 as **"Current `sec_13f_hr.py` is hand-rolled. EdgarTools `Filing.obj()` returns a typed `ThirteenF` model that handles PRN/SH drop + 2023-01-03 VALUE cutover natively"** — and instructed: spike against the Berkshire 2024Q3 golden fixture before drop-in; if INFEASIBLE → close #925 REBUTTED + freeze hand-rolled parser; if FEASIBLE → drop-in + remove ~200 lines of orchestration.

Two empirical conditions invalidate that framing on inspection:

1. **The parser layer was already replaced by PR #931** (`feat(#925): adopt EdgarTools as 13F-HR parser drop-in`, merged 2026-05-05, commit `0428dbf`). `app/providers/implementations/sec_13f.py:20-54` is now a thin wrapper over EdgarTools' static parsers `edgar.thirteenf.parsers.primary_xml.parse_primary_document_xml` + `edgar.thirteenf.parsers.infotable_xml.parse_infotable_xml`. The wrapper preserves the public `ThirteenFFilerInfo` / `ThirteenFHolding` dataclass surface so the institutional-holdings service layer is unaffected.
2. **`Filing.obj()` is structurally incompatible with the manifest adapter's contract** — see §7 for the five binding rejections (rate-limit pool bypass, cutover-semantics divergence, no native PRN drop, no raw-payload persistence hook, no transient-vs-deterministic error classification + ingest-log audit trail).

The spike's job therefore reduces to: **document the empirical state of #925, certify that PR #931 satisfied the issue body verbatim, and explain why the plan §2 PR 9 alternative scope (manifest-adapter restructure on top of `Filing.obj()` / `ThirteenF`) is INFEASIBLE on independent grounds.**

The companion spike for PR 10 (#932, N-PORT EdgarTools `FundReport` drop-in) is a separate document. The N-PORT case has a live Pydantic validation cliff per `[[edgartools]]` memory; the 13F-HR case does not — every Berkshire fixture row parses cleanly through EdgarTools' static parsers without Pydantic-constructor crashes.

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Fundamentals provider posture" (#532) — free regulated-source only | EdgarTools is a parsing library, not a paid data source. | PRESERVED. |
| §"Provider design rule" — providers thin, DB lookups in services | Manifest adapter sits in services layer (`app/services/manifest_parsers/sec_13f_hr.py`); parser sits in providers layer (`app/providers/implementations/sec_13f.py`). Both layers currently conform; any restructure must preserve the split. | PRESERVED — verdict §7 keeps both layers untouched. |
| §"Filing event storage" — raw-payload persistence per #1168 | Manifest worker enforces `requires_raw_payload=True` for the 13F-HR parser; both `primary_doc.xml` and `infotable.xml` are stored in `filing_raw_documents` via `store_raw` inside savepoints BEFORE parse, so the invariant holds across success + parse-failure paths. | PRESERVED — the rejection of `Filing.obj()` is partly because EdgarTools' high-level path does not surface a hook to persist raw bytes into our table. |

Prevention log entries (binding):

| Entry | File:line | How it gates this spike |
|---|---|---|
| "Multiple `ResilientClient` instances sharing a rate limit must share throttle state" | `docs/review-prevention-log.md:510-513` + local SEC invariant at `app/providers/implementations/sec_edgar.py:54-80, 237-253` | `Filing.obj()` triggers SEC fetches via EdgarTools' own internal HTTP client (`HTTP_MGR`), which does NOT participate in the process-wide `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` that every `SecFilingsProvider` / `SecFundamentalsProvider` `ResilientClient` shares (#537 + #726). Adopting it would silently break the shared 10 r/s SEC budget — concurrent EdgarTools-driven fetches plus our existing ingest jobs would burst past the SEC fair-use limit, triggering UA throttling. **Hard reject.** |
| "Decimal(parsed.summary_page.total_value) boundary risk" | `docs/review-prevention-log.md:1175-1177` | Already enforced via `Decimal(str(table_value_raw))` in `parse_primary_doc`. No spike action — only cited to certify the wrapper continues to honour the rule. |
| "`_TYPE_CODE_FROM_LABEL.get(label, 'SH')` warn-on-miss" | `docs/review-prevention-log.md:1205-1207` | Already enforced via `logger.warning(...)` in `parse_infotable`. Same — wrapper-level invariant, no spike action. |
| "Raw-payload persistence per #1168 (PRN cohort)" | `docs/review-prevention-log.md:1170-1172` (and the contract documented in `sec_13f_hr.py:34-37`) | Manifest worker calls `store_raw` for both XML attachments in savepoints before parse. EdgarTools `Filing.obj()` provides no hook to insert raw bytes into our `filing_raw_documents` table; adopting it would force us to re-fetch + re-store outside the EdgarTools call, eliminating any wall-clock saving and adding a second SEC hit per accession. |

## 3. Empirical state of #925 / PR #931

### 3.1 Issue body (verbatim scope)

`gh issue view 925 --json title,body,state` returns the following scope for #925:

- Add `edgartools==5.30.2` (ceiling `<5.31.0`) to `[project].dependencies`.
- Replace `parse_primary_doc` + `parse_infotable` **internals** with EdgarTools `ThirteenF` parsing while preserving the existing `ThirteenFFilerInfo` / `ThirteenFHolding` dataclass output shapes (no service-layer change required).
- Golden-file fixture + assertion that EdgarTools-parsed shape matches the bespoke parser's output within ±0.1% (or exactly, where deterministic).

The issue itself does NOT scope a manifest-adapter restructure on top of `Filing.obj()` — that is an additive framing introduced in the plan §2 PR 9 entry.

### 3.2 What PR #931 actually shipped

- `pyproject.toml:21` — `edgartools==5.30.2` pinned.
- `uv.lock` — lock entries at lines 318 / 347 / 371 / 397 / 399.
- `app/providers/implementations/sec_13f.py:20-54` — module docstring: *"#925 — internals are now a thin wrapper over EdgarTools' `edgar.thirteenf.parsers.primary_xml.parse_primary_document_xml` and `edgar.thirteenf.parsers.infotable_xml.parse_infotable_xml`. EdgarTools is pinned tight … per the license/maintenance review at `.claude/codex-913-license.txt`."*
- `app/providers/implementations/sec_13f.py:76-80` — lazy-import `_edgar_parsers()` keeps the package's filesystem cache initialiser (`HTTP_MGR` mkdir of `~/.edgar/_tcache`) deferred to first-parse call, preserving the pure-parser contract on read-only `$HOME` (CI runners, Docker images).
- `tests/test_sec_13f_parser.py` — 20 tests including `TestBerkshireGoldenFile::test_primary_doc_round_trip` + `test_infotable_holdings_count_and_total` at lines 380-441. Cross-source-verified: accession `0000950123-24-011775`, filed 2024-11-14, period 2024-09-30, 121 holdings, total table value $266,378,900,503.
- `tests/fixtures/sec/13f/berkshire_2024q3_primary_doc.xml` + `tests/fixtures/sec/13f/berkshire_2024q3_infotable.xml` — golden replay fixtures.

### 3.3 Re-run of the golden-file replay

`uv run pytest tests/test_sec_13f_parser.py -x -q` against the EdgarTools 5.30.2 + Berkshire 2024Q3 fixtures: **20 passed, 0 failed.** No Pydantic-cliff crash, no shape divergence, no skipped tests. Replay confirms the wrapper's contract holds against the pinned library version.

### 3.4 Auto-close gap

PR #931 was merged 2026-05-05 — predates the PR auto-close CI enforcement gate (#942) by approximately one week (per memory `[[pr-auto-close-required]]`). The lack of a `Closes #925` magic word in PR #931's body is the reason the issue remained OPEN at the start of this spike; it is administrative drift, not residual engineering scope.

## 4. EdgarTools API surface analysis

### 4.1 What EdgarTools 5.30.2 exposes for 13F-HR

Probed live via `uv run python`:

```
from edgar.thirteenf import ThirteenF
from edgar.thirteenf.parsers.primary_xml import parse_primary_document_xml
from edgar.thirteenf.parsers.infotable_xml import parse_infotable_xml

ThirteenF.__init__(self, filing, use_latest_period_of_report=False)
ThirteenF public attrs: accession_number, compare_holdings, filing_date, filing_signer_name,
  filing_signer_title, form, get_manager_info_summary, get_portfolio_managers, has_infotable,
  holding_history, holdings, holdings_view, infotable, infotable_html, infotable_txt,
  infotable_xml, investment_manager, is_filing_signer_likely_portfolio_manager,
  management_company_name, manager_name, other_managers, parse_infotable_txt,
  parse_infotable_xml, parse_primary_document_xml, previous_holding_report, report_period,
  set_cache_provider, signer, to_context, total_holdings, total_value
```

`parse_primary_document_xml(xml: str)` returns a `PrimaryDocument13F` model with attrs `additional_information, cover_page, report_period, signature, summary_page`.

`parse_infotable_xml(xml: str) -> pandas.DataFrame` returns columns `['Issuer', 'Class', 'Cusip', 'Value', 'PutCall', 'InvestmentDiscretion', 'OtherManager', 'SharesPrnAmount', 'Type', 'SoleVoting', 'SharedVoting', 'NonVoting', 'Ticker']`.

Both are pure functions over XML strings — no SEC HTTP fetch occurs during parse. Lazy-import keeps `~/.edgar/_tcache` mkdir deferred.

`ThirteenF.__init__` takes a `Filing` object (not raw XML). The only documented construction path is via `Filing.obj()` on a filing the user has resolved through EdgarTools' own resolver (which fetches SEC index data through `HTTP_MGR`).

### 4.2 What `ThirteenF` does that the wrapper does NOT

Inspection of `edgar/thirteenf/models.py`:

| Line | Behaviour | Relevant to our adapter? |
|---|---|---|
| 28-30 | `_13F_VALUE_IN_THOUSANDS_CUTOFF = datetime(2022, 9, 30)` keyed by `report_period`. | YES — semantic divergence; see §5.1. |
| 365-367 | `df['Value'] = df['Value'] * 1000` when `_value_in_thousands` is true. | Same applies to the holdings DataFrame. |
| 461 | `categories=['Shares', 'Principal', '-']` — PRN rows preserved with `Type='Principal'`, NOT dropped. | YES — semantic divergence; see §5.2. |
| 484-485 | `if value and self._value_in_thousands: return value * 1000` — same scaling applied on `total_value`. | Same — diverges on our cutover semantic. |

### 4.3 What `ThirteenF` does NOT do

- **No raw-payload persistence hook.** Our manifest adapter stores `primary_doc.xml` + `infotable.xml` raw bytes into `filing_raw_documents` via `store_raw` for re-wash + parser-bump replay. `ThirteenF` exposes `infotable_xml` / `infotable_html` / `infotable_txt` as views on the parsed object, not as durable byte streams.
- **No shared rate-limit pool integration.** Fetches via EdgarTools' internal `HTTP_MGR` ignore `_PROCESS_RATE_LIMIT_LOCK` + `_MIN_REQUEST_INTERVAL_S` (the 10 r/s shared SEC budget enforcer).
- **No transient-vs-deterministic error classification.** Our adapter uses `is_transient_upsert_error` / `format_upsert_error` (#1131) to route psycopg `OperationalError` to a 1h backoff retry vs deterministic violations to tombstone. EdgarTools surfaces a single broad-exception flavour.
- **No ingest-log audit trail.** Our adapter writes `institutional_holdings_ingest_log` rows (`success` / `partial` / `failed`) for dashboard counts. EdgarTools has no equivalent.

## 5. Cutover-semantics + PRN drop divergence (binding)

### 5.1 VALUE cutover keying — EdgarTools vs eBull

| Dimension | EdgarTools 5.30.2 | eBull manifest adapter |
|---|---|---|
| Cutoff date | `datetime(2022, 9, 30)` | `date(2023, 1, 3)` |
| Keyed by | `report_period` | `filed_at` |
| Source | `edgar/thirteenf/models.py:28-30` | `app/services/manifest_parsers/sec_13f_hr.py:101` |

The choice of `filed_at`-keyed cutover (eBull) vs `report_period`-keyed (EdgarTools) is deliberate. The semantic invariant: filers report whatever unit the SEC requires *at the moment they file*, not at the moment the period closes. SEC Release 22.4.1 switched Column 4 from $thousands to whole dollars effective 2023-01-03; an amendment filed after that date uses the new unit even if its `period_of_report` is pre-cutover.

Concrete edge-case where the divergence matters:

- 13F-HR/A amendment, `period_of_report = 2022-09-30`, `filed_at = 2023-03-15`. The amendment was filed after Release 22.4.1 took effect (2023-01-03), so the filer entered Column 4 in whole dollars. eBull's adapter (`filed_at >= 2023-01-03 → no scaling`) treats the value correctly. EdgarTools' cutover (`report_period <= 2022-09-30 → scale by 1,000`) would multiply the value by 1,000 — three orders of magnitude — turning a $50B portfolio total into $50T. The bulk-dataset ingest path documents this same invariant at `app/services/sec_13f_dataset_ingest.py:316-322` ("Discriminate on FILED_AT … NOT period_end — a 2022Q4 restatement filed in March 2023 …").

The SEC Release 22.4.1 effective date is the canonical anchor; EdgarTools' 2022-09-30 is the *fiscal-quarter end immediately preceding* the release, which approximates the cutover for non-amendment filings but fails on the amendment edge-case. eBull's adapter is empirically correct; adopting EdgarTools' cutover would introduce a deferred-detonation bug invisible until a non-trivial pre-cutover amendment lands post-2023-01-03.

(The existing comment at `sec_13f_hr.py:91-100` describing this invariant was internally contradictory — "Pre-cutover amendments filed late still report thousands" conflicted with the immediately-following example "a 2022Q4 restatement landed in March 2023 would carry dollars". The code itself was correct; this spike PR fixes the comment to mirror the unambiguous wording at `sec_13f_dataset_ingest.py:316-322`.)

### 5.2 PRN drop divergence

EdgarTools relabels SEC's `sshPrnamtType` two-letter codes (`SH` / `PRN`) to `Shares` / `Principal` and preserves both as DataFrame rows — neither is dropped. The Berkshire 2024Q3 fixture is pure-equity (every row carries `Type='Shares'`), so it cannot empirically prove PRN preservation; the canonical source-code evidence is:

- `edgar/thirteenf/parsers/infotable_xml.py:37` — `shares_or_principal = {"SH": "Shares", "PRN": "Principal"}`.
- `edgar/thirteenf/parsers/infotable_xml.py:102-103` — `info_table['Type'] = shares_or_principal.get(ssh_prnamt_type)` (passes both `Shares` and `Principal` through without filtering).
- `edgar/thirteenf/models.py:458-462` — `holdings['Type'] = pd.Categorical(holdings['Type'], categories=['Shares', 'Principal', '-'])` (keeps `Principal` as a first-class category on `ThirteenF.holdings`).

eBull's adapter at `sec_13f_hr.py:411-414` drops `Type != 'SH'` rows explicitly, mirroring the bulk-dataset path at `app/services/sec_13f_dataset_ingest.py:311-314`. The drop is correct: PRN rows are dollar principal amounts, not share counts; silently writing them into `institutional_holdings.shares_or_principal` with `shares_or_principal_type='SH'` would corrupt every downstream slice that branches on the type column. PR #1133's prevention-log entry (`docs/review-prevention-log.md:1205-1207`) documents this exact failure mode at the parser-wrapper level.

The plan §2 PR 9 entry's claim "EdgarTools `Filing.obj()` returns a typed `ThirteenF` model that **handles PRN/SH drop … natively**" is empirically false — `ThirteenF.holdings` exposes the same DataFrame `Type` categorical with both `Shares` and `Principal` retained. The drop logic is service-layer-specific and remains in the manifest adapter.

### 5.3 Other structural divergences

Even if the cutover + PRN drop were waived as semantically equivalent (they are not), four further reasons make `Filing.obj()` adoption infeasible for the manifest adapter:

1. **Rate-limit pool bypass** — see §2 prevention-log entry line 510. Hard reject.
2. **No raw-payload persistence hook** — see §4.3.
3. **No transient-vs-deterministic error classification** — see §4.3; loss of `is_transient_upsert_error` would mis-route deterministic violations to retry loops.
4. **No ingest-log audit trail** — see §4.3; dashboard counts would silently de-converge.

## 6. Manifest-adapter restructure analysis

If we attempted the plan §2 PR 9 alternative scope — restructure `app/services/manifest_parsers/sec_13f_hr.py` on top of `Filing.obj()` / `ThirteenF` — what could plausibly be removed and what could not?

| Adapter responsibility | Lines (approx) | Replaceable by `ThirteenF`? |
|---|---|---|
| Archive `index.json` walk to find `primary_doc.xml` + `infotable.xml` filenames | ~30 lines (`_archive_file_url`, `parse_archive_index`) | NO — `ThirteenF` resolves attachments internally but via `Filing.obj()` fetch, bypassing our rate-limit pool. |
| Fetch `primary_doc.xml` via shared `SecFilingsProvider` | ~10 lines | NO — replacing this with `Filing.obj()` breaks rate-limit discipline. |
| `store_raw` of `primary_doc.xml` into `filing_raw_documents` (savepoint) | ~15 lines | NO — `ThirteenF` has no persistence hook. |
| Parse `primary_doc.xml` via wrapper | 1 line | (Already EdgarTools-wrapped at parser layer.) |
| Fetch + `store_raw` of `infotable.xml` (savepoint) | ~25 lines | NO — same reasons. |
| Parse `infotable.xml` via wrapper | 1 line | (Already EdgarTools-wrapped at parser layer.) |
| VALUE cutover (per-row scaling, `filed_at`-keyed, 2023-01-03 anchor) | ~28 lines | NO — EdgarTools cutover semantics diverge; adopting would regress amendment correctness. |
| PRN drop | ~4 lines | NO — EdgarTools does not drop PRN. |
| `_upsert_filer` / `_upsert_holding` / `_record_unresolved_cusip` / `_record_13f_observations_for_filing` / `refresh_institutions_current` | ~50 lines | NO — bound to eBull schema; EdgarTools has no equivalent. |
| `_record_ingest_attempt` audit log + `success` / `partial` / `failed` semantics | ~30 lines | NO — eBull-specific. |
| `is_transient_upsert_error` / `format_upsert_error` classification (#1131) | ~10 lines | NO — eBull-specific. |
| Savepoint discipline + per-step error routing | ~50 lines | NO — broader than EdgarTools' single-call contract. |

Total adapter is ~530 lines (not ~200 as the plan claimed). Zero lines can be removed by adopting `Filing.obj()` / `ThirteenF`. The plan's "remove ~200 lines" claim is unsupported.

## 7. Verdict

**#925 is DONE.** PR #931 (`feat(#925): adopt EdgarTools as 13F-HR parser drop-in`, merged 2026-05-05, commit `0428dbf`) shipped the issue body's full scope: EdgarTools dep pinned, parser internals wrapped, public dataclass surface preserved, golden-file fixture + replay test landed. The issue remained administratively OPEN because PR #931 predated the auto-close CI enforcement gate (#942).

**Plan §2 PR 9's alternative scope (manifest-adapter restructure on top of `Filing.obj()` / `ThirteenF`) is INFEASIBLE-AS-FRAMED on independent grounds** — five binding rejections:

1. **Rate-limit pool bypass** (`docs/review-prevention-log.md:510-513` + the local SEC invariant at `app/providers/implementations/sec_edgar.py:54-80, 237-253` — `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` per #537 + #726). Hard reject; the prevention-log entry is normative and the local invariant is the load-bearing implementation.
2. **VALUE cutover semantics divergence** (§5.1) — EdgarTools' `report_period`-keyed cutoff at `2022-09-30` would regress eBull's correct `filed_at`-keyed handling of post-2023-01-03 amendments, multiplying amendment-cohort dollar totals by 1,000.
3. **No native PRN/SH drop** (§5.2) — EdgarTools preserves PRN rows with `Type='Principal'`; the drop logic must remain in the adapter.
4. **No raw-payload persistence hook** (§4.3) — `requires_raw_payload=True` contract (#1168) cannot be satisfied through `Filing.obj()` alone.
5. **No transient-vs-deterministic error classification + ingest-log audit trail** (§4.3) — eBull-specific service-layer concerns with no EdgarTools equivalent.

The plan's "remove ~200 lines of hand-rolled parser code" claim is unsupported (§6) — the adapter is ~530 lines of orchestration, not parser; zero are replaceable.

## 8. Recommendation

1. **Close #925** with verdict comment referencing PR #931 + this spike doc. No code change required.
2. **Update plan §2 Phase 5 PR 9 handover** to record the empirical state + the verdict (DONE-AS-FRAMED-PRE-DATED, alternative scope REBUTTED).
3. **Annotate matrix row `sec_13f_hr`** in `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 to cite EdgarTools wrapper + PR #931 alongside the existing #1133 reference. The row's status was already WIRED; only the provenance annotation changes.
4. **No spike-doc-driven follow-up tickets.** The five binding rejections in §7 are documented here as anchors for any future reopen attempt; if EdgarTools' semantics ever converge (cutover keyed by `filed_at`, native PRN drop, rate-limit-pool integration hook), the verdict is REOPEN-ELIGIBLE.

### 8.1 What is NOT recommended

- **Adopting `Filing.obj()` / `ThirteenF` in the manifest adapter** — §5 + §6 reject. The wrapper layer already extracts the maximum value EdgarTools provides for 13F-HR.
- **Removing the eBull cutover handler in favour of EdgarTools'** — §5.1 regression for amendments.
- **Removing the PRN drop in favour of EdgarTools' default** — §5.2 corruption risk.
- **Re-running the golden-file replay against a fresh fixture** — the existing Berkshire 2024Q3 fixture is cross-source-verified, and the replay test (`tests/test_sec_13f_parser.py`) passes against EdgarTools 5.30.2. A second fixture adds no signal until EdgarTools' ceiling is lifted past `<5.31.0`.

### 8.2 Settled-decisions impact

| Decision (§2) | Outcome |
|---|---|
| §"Fundamentals provider posture" (#532) | PRESERVED. |
| §"Provider design rule" | PRESERVED — parser layer + service layer split unchanged. |
| §"Filing event storage" — raw-payload persistence per #1168 | PRESERVED — the rejection of `Filing.obj()` is partly because adopting it would have violated this invariant. |

## 9. Cross-spike note for PR 10 (#932 — N-PORT FundReport drop-in)

#932's scope is structurally narrower than the plan §2 PR 9 alternative framing: replace the stdlib-ElementTree NPORT-P parser at `app/services/n_port_ingest.py::parse_n_port_payload` with a thin wrapper over `edgar.funds.reports.FundReport.parse_fund_xml`, preserving the public `NPortFiling` / `NPortHolding` dataclass surface — mirroring the parser-only scope PR #931 actually shipped for 13F-HR. **#932 does NOT propose adopting `Filing.obj()` for the N-PORT manifest path**, which is the structurally infeasible variant rejected here.

The Pydantic validation cliff documented in `[[edgartools]]` memory + #932 applies to N-PORT (`FundReport.parse_fund_xml`), not to 13F-HR. The 13F-HR Berkshire 2024Q3 golden fixture parses cleanly through EdgarTools' static parsers (no Pydantic-constructor crash; §3.3 replay confirms). PR 10's spike must independently verify whether the existing N-PORT fixtures at `tests/fixtures/sec/nport_p_*.xml` clear `FundReport.parse_fund_xml`'s Pydantic constructor; that is a separate empirical question this spike does not address.

## 10. Execution constraints + out of scope

- No SEC HTTP fetches performed during the spike — all evidence is from existing fixtures + local source inspection + `uv run python` probes against the pinned `edgartools==5.30.2`.
- No new dependencies introduced.
- ETL DoD clauses #8-#12 — N/A. The spike is a feasibility verdict + docs/matrix update, not a parser / schema / observations / rollup change. No per-instrument figure changes hands.

Out of scope:

- The N-PORT spike (PR 10 / #932) — separate doc.
- The N-CSR feasibility re-spike — already complete at `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` (#918 verdict INFEASIBLE-CONFIRMED, synth no-op adopted via #1171).
- Any retrofit of cutover semantics — the eBull handler is correct as-is; no change needed.
- Any restructure of the manifest-adapter savepoint discipline — pinned by #1131 + #1168.

## 11. Verification commands (reproducible)

```
# Existing parser test suite
uv run pytest tests/test_sec_13f_parser.py -x -q

# Inspect EdgarTools surface
uv run python -c "
from edgar.thirteenf import ThirteenF
from edgar.thirteenf.parsers.primary_xml import parse_primary_document_xml
from edgar.thirteenf.parsers.infotable_xml import parse_infotable_xml
print('ThirteenF init sig:', ThirteenF.__init__.__doc__ or 'n/a')
print('parse_primary_document_xml is a pure XML->model function')
print('parse_infotable_xml returns pandas.DataFrame')
"

# Confirm PR #931 merge state
gh pr view 931 --json title,state,mergeCommit,mergedAt
```

All three reproduce the empirical state recorded in §3 + §4.
