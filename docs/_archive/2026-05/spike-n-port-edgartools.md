# N-PORT EdgarTools `FundReport` drop-in feasibility spike

> Status: **COMPLETED 2026-05-18** — findings + verdict gathered; recommendation locked.
>
> Issue: #932 (OPEN at spike time).
> Plan context: `docs/superpowers/plans/2026-05-17-us-etl-completion.md` §2 Phase 5 PR 10.
> Predecessor spike: `docs/superpowers/spikes/2026-05-18-13f-hr-edgartools-feasibility.md` (#925 — DONE-AS-FRAMED-PRE-DATED).
> Predecessor session pre-finding: empirical probe revealed `FundReport.parse_fund_xml` crashes on the synthetic fixture at `tests/fixtures/sec/nport_p_test_fund.xml`. This spike disambiguates parser-coverage cliff vs Pydantic validation cliff against a real-SEC-NPORT-P payload.

## 1. Context recap

Plan §2 Phase 5 PR 10 framed #932 as: **"Same shape as PR 9. Memory `[[edgartools]]` documents the Pydantic validation cliff. Pre-impl spike against existing N-PORT golden fixtures."**

Two empirical facts force the spike to disambiguate two distinct cliffs against a real SEC payload:

1. **Documented Pydantic cliff** at `.claude/skills/data-sources/edgartools.md` §G3: "`FundReport(**parse_fund_xml(xml))` raises `pydantic.ValidationError` on synthetic fixtures missing required fields (`<regCik>`, `<regStreet1>`). Required fields are non-`Optional`. Punted #932." The skill recommended path C (direct lxml ~150 LoC) if revisited.

2. **Pre-finding from PR #1203 / #925 closeout session** (recorded in handover): `FundReport.parse_fund_xml` crashes with `AttributeError: 'NoneType' object has no attribute 'find'` on the synthetic fixture at `tests/fixtures/sec/nport_p_test_fund.xml`. This is a **structural-element cliff**, not a Pydantic validation cliff — the parser unconditionally dereferences `header_el.find("filerInfo")` then walks deeper for `issuerCredentials`, and the synthetic fixture has no `<filerInfo>` block.

The spike's job: **fetch a real Vanguard NPORT-P primary doc via the shared `SecFilingsProvider` rate-limit pool, probe `FundReport.parse_fund_xml` against it, and determine whether either cliff is binding for production NPORT-P payloads.**

The companion spike for PR 9 (#925, 13F-HR EdgarTools drop-in) is at `docs/superpowers/spikes/2026-05-18-13f-hr-edgartools-feasibility.md`. The 13F-HR verdict was DONE-AS-FRAMED-PRE-DATED (PR #931 shipped 2026-05-05). #932 follows the same SPIKE-FIRST shape but is genuinely OPEN at the parser-replacement layer.

## 2. Settled-decisions check

| Decision | Relevance | Preservation |
|---|---|---|
| §"Fundamentals provider posture" (#532) — free regulated-source only | EdgarTools is a parsing library, not a paid data source. | PRESERVED. |
| §"Provider design rule" — providers thin, DB lookups in services | The wrapper sits in `app/services/n_port_ingest.py::parse_n_port_payload`, replacing the body but preserving the service-layer placement. Public `NPortFiling` / `NPortHolding` dataclass surface is unchanged. | PRESERVED. |
| §"Filing event storage" — raw-payload persistence per #1168 | The N-PORT ingester already stores raw `primary_doc.xml` via `raw_filings.store_raw` BEFORE parse at `app/services/n_port_ingest.py:786-798`. The wrapper does not change the fetch / store sequence; it only swaps the parse step. | PRESERVED — unchanged. |
| §"Source priority for fund metadata (#1171)" | Per `(instrument_id, period_end)` winner via `ORDER BY period_end DESC, filed_at DESC, source_accession DESC`. The wrapper preserves the `period_end` + `filed_at` invariants from #917 verbatim (period_end mandatory; filed_at falls back to submissions-index `filingDate`, never silently to `period_end` midnight). | PRESERVED. |

Prevention log entries (binding):

| Entry | File:line | How it gates this spike |
|---|---|---|
| "Multiple `ResilientClient` instances sharing a rate limit must share throttle state" | `docs/review-prevention-log.md:510-513` + local SEC invariant at `app/providers/implementations/sec_edgar.py:54-80, 237-253` | The wrapper does NOT introduce any HTTP fetch — `FundReport.parse_fund_xml(xml)` is a pure-string-in / dict-out function. The N-PORT ingester continues to fetch primary docs via `SecFilingsProvider.fetch_document_text` (10 r/s shared via `_PROCESS_RATE_LIMIT_CLOCK`). EdgarTools' `HTTP_MGR` is never invoked. **PRESERVED.** |
| "Raw API payload must be persisted before any parse / normalise step" (#1168) | `docs/review-prevention-log.md` (the #1168 entry) | `parse_n_port_payload` is called by `_ingest_single_accession` AFTER `raw_filings.store_raw` + `conn.commit()` at `app/services/n_port_ingest.py:786-798`. Wrapper does not change call ordering. **PRESERVED.** |
| "Pydantic validation cliff — spike fixture compatibility BEFORE drop-in" | `feedback_pydantic_validation_cliff.md` | This is the rule the spike honours. Outcome: the cliff is structural (missing `<filerInfo>` / `<fundInfo>` blocks in synthetic fixtures), not Pydantic-validation, but the discipline is the same — probe real-world fixtures BEFORE committing. |
| "EdgarTools internal module path is the only stability contract" | `.claude/skills/data-sources/edgartools.md` §G13 | `edgar.funds.reports.FundReport.parse_fund_xml` is an internal-looking path under the funds namespace. Mitigation: pin tight (already `edgartools==5.30.2` exact), golden-file replay, audit on every minor bump. The 13F-HR spike (`docs/superpowers/spikes/2026-05-18-13f-hr-edgartools-feasibility.md` §2 + §11) already documents this mitigation pattern for 13F-HR static parsers under `edgar.thirteenf.parsers`. |
| "Skills must own integrity, not inventory" | `feedback_skills_must_own_integrity.md` | The skill's §G3 statement about a "Pydantic validation cliff" is empirically wrong for `edgartools==5.30.2`: `parse_fund_xml` returns a `Dict[str, Any]` (not a Pydantic-constructed `FundReport`), and the crash on synthetic fixtures is structural (`AttributeError`), not Pydantic-validation. The skill needs updating in-scope with this spike's findings. |

## 3. Empirical state of #932

### 3.1 Issue body (verbatim scope)

`gh issue view 932 --json title,body,state` returns the following scope:

- Lazy-import `edgar.funds.reports` inside `parse_n_port_payload` to keep module import side-effect-free (#925 pattern).
- Wrap `FundReport.parse_fund_xml(xml)` output into existing `NPortFiling` / `NPortHolding` dataclasses.
- Preserve Codex-validated invariants from #917:
  - `period_end` mandatory (NPortParseError if missing)
  - `series_id` mandatory (NPortMissingSeriesError if missing — refuses to synthesise identity)
  - `cik` mandatory
  - `filed_at` must NOT default to `period_end` midnight inside the parser (the ingester layers in submissions-index `filingDate`)
  - `units` passthrough (downstream guard rejects non-NS even on EC)
  - `balance` None → drop row
- Replace hand-trimmed fixture at `tests/fixtures/sec/nport_p_test_fund.xml` with a real Vanguard 500 NPORT-P fixture; lock first-row + holdings count + total value via golden-file replay.

Out of scope per the issue body:

- Schema changes to `ownership_funds_*` tables.
- Ingester body changes (write-side guards, tombstone, log).
- Operator-visible rollup integration (deferred to #919).

### 3.2 Current parser shape

`app/services/n_port_ingest.py::parse_n_port_payload` (lines 358-454) — pure XML-in / dataclass-out, walks namespace-stripped local names via `_stripns()` + `_find_text()` + `_children_by_local_name()`. ~100 LoC of stdlib `xml.etree.ElementTree` walking + `Decimal` coercion. No network, no DB. Codex pre-impl review #1-#6 invariants enforced inline.

### 3.3 Existing synthetic fixture

`tests/fixtures/sec/nport_p_test_fund.xml` (4.3KB, 7 holdings) — modelled on a real Vanguard 500 Index Fund NPORT-P payload but **simplified flat shape** with:

- `<edgarSubmission>` root + `<headerData><submissionType>NPORT-P</submissionType></headerData>` only — **no `<filerInfo>` block, no `<issuerCredentials>`, no `<seriesClassInfo>`**.
- `<formData><genInfo>` carrying just `regCik / seriesId / seriesName / repPdEnd / filedAt`.
- **No `<fundInfo>` block** — neither `totAssets` nor `netAssets` nor `curMetrics` nor `returnInfo`.
- `<formData><invstOrSecs><invstOrSec>` × 7 with the 7 contract-exercising rows enumerated in the fixture XML comment.

The fixture documents itself as designed for the stdlib local-name walker:

> Real-world NPORT-P uses the `http://www.sec.gov/edgar/nport` namespace and an XBRL container. This fixture uses a simplified flat shape that the lxml-direct parser (which walks by local-name) accepts identically.

The simplified shape is the root cause of the EdgarTools parser-coverage cliff in §4.1 below.

## 4. EdgarTools API surface analysis

### 4.1 What EdgarTools 5.30.2 exposes for N-PORT

Probed via `uv run python` against the pinned `edgartools==5.30.2`:

```
from edgar.funds.reports import FundReport
print(FundReport.parse_fund_xml.__func__.__code__.co_firstlineno)  # -> 1213
```

`parse_fund_xml` is a classmethod **returning a `Dict[str, Any]`** (signature at `.venv/lib/python3.14/site-packages/edgar/funds/reports.py:1214`):

```python
@classmethod
def parse_fund_xml(cls, xml: Union[str, Any]) -> Dict[str, Any]:
    ...
    return {'header': header,
            'general_info': general_info,
            'fund_info': fund_info,
            'investments': investments_or_securities}
```

The four keys hold typed objects, NOT raw nested dicts:

- `header: Header` — `submission_type`, `is_confidential`, `filer_info` (`issuer_credentials` + `series_class_info`).
- `general_info: GeneralInfo` — `name, cik, file_number, reg_lei, street1, …, series_name, series_id, series_lei, fiscal_year_end, rep_period_date, is_final_filing`.
- `fund_info: FundInfo` — `total_assets, total_liabilities, net_assets, assets_invested, current_metrics, return_info, monthly_flow1/2/3, …`.
- `investments: List[InvestmentOrSecurity]` — per-row `name, lei, title, cusip, identifiers, balance, units, value_usd, pct_value, payoff_profile, asset_category, issuer_category, investment_country, debt_security, security_lending, derivative_info, …`.

**The skill's §G3 statement "`FundReport(**parse_fund_xml(xml))` raises pydantic.ValidationError" is empirically wrong at `edgartools==5.30.2`.** `parse_fund_xml` constructs `FundReport`-internal-typed objects already and returns them in a dict; there is no separate Pydantic-constructor step that the wrapper needs to invoke. The skill needs updating in-scope with this spike's findings.

### 4.2 Required-block dereferences (the actual cliff)

Inspection of `parse_fund_xml` body at `edgar/funds/reports.py:1213-1410`:

| Line | Dereference | Required structural element |
|---|---|---|
| 1241 | `header_el = root.find("headerData")` | `<headerData>` root child |
| 1242 | `filer_info_tag = header_el.find("filerInfo")` | `<headerData><filerInfo>` |
| 1244 | `issuer_credentials_tag = filer_info_tag.find(".//issuerCredentials")` | `<filerInfo><filer><issuerCredentials>` |
| 1245 | `series_class_info_tag = filer_info_tag.find(".//seriesClassInfo")` | `<filerInfo><filer><seriesClassInfo>` |
| 1259 | `form_data_tag = root.find("formData")` | `<formData>` root child |
| 1262 | `general_info_tag = form_data_tag.find("genInfo")` | `<formData><genInfo>` |
| 1306 | `fund_info_tag = form_data_tag.find("fundInfo")` | `<formData><fundInfo>` |
| 1325 | `return_info_tag = fund_info_tag.find("returnInfo")` | `<fundInfo><returnInfo>` |
| 1326 | `monthly_returns_tag = return_info_tag.find("monthlyTotReturns")` | `<returnInfo><monthlyTotReturns>` |
| 1334-1348 | `Decimal(_text(fund_info_tag, "totAssets" / "totLiabs" / "netAssets" / …))` | `<fundInfo>` must carry the 7 required Decimal fields |

A missing block at any of those lines raises `AttributeError: 'NoneType' object has no attribute …`. The synthetic fixture has no `<filerInfo>` so the crash fires at line 1244.

**Empirical scope of the "real NPORT-P always carries these blocks" claim:** The §5 probe sampled two Vanguard equity index funds (Value + Mid-Cap Value, both period 2025-12-31). The XSD requirements at 17 CFR §270.30b1-9 mandate §A general-info + §B fund-info + §C investments as required-by-rule, but the SEC EDGAR XSD itself includes optional sub-elements within those sections (e.g. `returnInfo / monthlyTotReturns / monthly_flow1/2/3` may be omitted for funds that have not yet operated a full quarter; `curMetrics` may be empty for funds without interest-rate-sensitive holdings). The verdict therefore is **feasible for the probed real NPORT-P shape; impl must golden-test against the canonical Vanguard 500 fixture AND retain the parse-failure tombstone path (`_AccessionOutcome(status='failed')` at `n_port_ingest.py:820-836`) so edge-shape funds that crash `parse_fund_xml` tombstone cleanly rather than aborting the filer batch.** The wrapper's `try/except` catch (§6) is the load-bearing mitigation for unprobed structural shapes; it is not "defensive programming for impossible states."

### 4.2.1 Pydantic cliff inside `parse_fund_xml` — separate from the structural cliff

Although `parse_fund_xml` returns a dict (not a Pydantic-constructed `FundReport`), the function constructs Pydantic `BaseModel` instances internally for each `<invstOrSec>` row at `reports.py:1376-1399`:

```python
investments_or_security = InvestmentOrSecurity(
    name=_text(investment_tag, "name"),
    ...
    value_usd=_opt_decimal(investment_tag, "valUSD"),
    ...
)
```

`InvestmentOrSecurity.value_usd: Decimal` is declared **non-Optional** at `edgar/funds/reports.py:346`. `_opt_decimal` returns `Decimal | None`. If a real NPORT-P row omits `<valUSD>` (e.g. a legally-empty mid-quarter row, or a confidential-pricing carve-out), `_opt_decimal` returns `None` and Pydantic raises `pydantic.ValidationError` mid-iteration — **aborting the entire dict construction**, so even well-formed surrounding holdings are lost.

The current eBull stdlib parser at `app/services/n_port_ingest.py:439` uses `_decimal_or_none(value_text)` and assigns `value_usd: Decimal | None` to `NPortHolding`; downstream the ingester treats `value_usd=None` as "write the row with NULL market_value_usd". So a real NPORT-P with a single missing `<valUSD>` cell:

- Current parser: writes all rows; the one with missing valUSD lands with `NULL` market_value_usd.
- EdgarTools wrapper: raises `NPortParseError`; **the whole accession tombstones to `status='failed'` and ZERO rows are written.**

This is a behavioural regression risk the impl PR MUST surface. Mitigations (impl-phase decision):

- **Catch + tombstone (current §6 wrapper)**: behaviour matches "missing valUSD = malformed filing"; loses well-formed rows in the same accession. Operator-visible tombstone counter goes up.
- **Catch + fall back to current stdlib parser**: keeps row-level resilience but couples the wrapper to the parser it's replacing. Complexity not worth it for a per-row edge case.
- **Pre-validate XML for missing `<valUSD>` before EdgarTools dispatch**: ad-hoc; defeats the simplification.

The §5 probe did NOT exercise this Pydantic cliff (both Vanguard fixtures had `valUSD` populated for every row). Frequency of missing-`<valUSD>` is unknown until the impl PR runs a broader probe across non-Vanguard fund families. Spec-phase MUST decide which mitigation lands, document the trade-off, and golden-test the chosen behaviour.

### 4.3 What `parse_fund_xml` does that we don't need

- `header.is_confidential` — eBull doesn't track confidential filings (we only ingest the public quarterly NPORT-P slice; monthly NPORT-MFP is confidential and out of scope per `n_port_ingest.py:6-11`).
- `general_info.street1 / city / zip_or_postal_code / phone / state / country` — operator address fields; we don't use.
- `fund_info.current_metrics / return_info / monthly_flow1/2/3` — interest-rate sensitivity + monthly returns + monthly cash flows; out of scope for ownership v1.
- `investment.debt_security / security_lending / derivative_info` — out of scope (we drop non-equity-common via the asset-category guard before any of these matter).

The wrapper consumes only the fields we already use: `general_info.{cik, series_id, series_name, fiscal_year_end}` (the last carrying `repPdEnd` text per the issue's documented risk) + `general_info.rep_period_date` (carrying `repPdDate`) + per-holding `{name, cusip, balance, units, value_usd, payoff_profile, asset_category, issuer_category}`.

### 4.4 `general_info.fiscal_year_end` field-name trap

Issue body §"Risks" calls this out:

> EdgarTools' `general_info.fiscal_year_end` confusingly stores `repPdEnd` text (not actual fiscal-year-end). Wrapper documents the mapping; golden-file replay locks behaviour against pin bumps.

Confirmed at `edgar/funds/reports.py:1300`:

```python
fiscal_year_end=_text(general_info_tag, "repPdEnd"),
rep_period_date=_text(general_info_tag, "repPdDate"),
```

The wrapper maps `general_info.fiscal_year_end` → `NPortFiling.period_end` (after `date.fromisoformat`) and documents the EdgarTools mis-naming so a future pin bump that renames the attribute (e.g. to `report_period_end`) surfaces as a failing golden replay rather than a silent semantic drift.

The same mapping also handles the empirical observation that `general_info.rep_period_date` carries `repPdDate` (the SEC report-period anchor date, typically same-or-adjacent to `repPdEnd`), which is a different field eBull does not consume.

## 5. Empirical probe — real Vanguard NPORT-P

### 5.1 Fetch via shared `SecFilingsProvider` rate-limit pool

Per the prevention-log entry on shared rate-limit state, the fetch uses `SecFilingsProvider.fetch_document_text` (not EdgarTools' `HTTP_MGR`, not direct `httpx`). Probe script (in-repo path required for `app.*` imports; cleaned up post-spike):

```python
from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.n_port_ingest import _archive_file_url, _submissions_url, parse_submissions_index

with SecFilingsProvider(user_agent=settings.sec_user_agent) as sec:
    submissions = sec.fetch_document_text(_submissions_url("0000036405"))  # Vanguard Index Funds
    refs = parse_submissions_index(submissions)
    for ref in refs[:2]:
        primary = sec.fetch_document_text(
            _archive_file_url("0000036405", ref.accession_number, "primary_doc.xml")
        )
        # ... probe FundReport.parse_fund_xml(primary)
```

Two real NPORT-P primary docs fetched (period_end 2025-12-31, filed 2026-02-26):

| Accession | Series | Series name | Size | Holdings |
|---|---|---|---|---|
| `0000036405-26-000074` | `S000002840` | Vanguard Value Index Fund | 317,699 bytes | 323 |
| `0000036405-26-000073` | `S000012757` | Vanguard Mid-Cap Value Index Fund | 191,526 bytes | 186 |

The CIK 36405 registrant (Vanguard Index Funds) holds ~313 NPORT-P accessions in its `recent[]` window (one per fund-series per quarter). The actual canonical "Vanguard 500 Index Fund" series is `S000002277` under this same CIK; impl-phase will iterate `recent[]` to grab a 500-series accession specifically per #932 issue body acceptance #4.

### 5.2 `FundReport.parse_fund_xml` outcome on each fixture

| Fixture | Result | First holding |
|---|---|---|
| Synthetic `nport_p_test_fund.xml` (4.3KB) | **CRASH** — `AttributeError: 'NoneType' object has no attribute 'find'` at `reports.py:1244` (missing `<filerInfo>`) | n/a |
| Vanguard Value Index `0000036405-26-000074.xml` (317KB) | **OK** — dict with 4 keys, `investments` count 323 | `name='United Airlines Holdings Inc' cusip='910047109' balance=Decimal('3015716.00000000') units='NS' value_usd=Decimal('337217363.12000000') payoff='Long' asset_cat='EC' issuer_cat=<set>` |
| Vanguard Mid-Cap Value `0000036405-26-000073.xml` (191KB) | **OK** — dict with 4 keys, `investments` count 186 | `name='United Airlines Holdings Inc' cusip='910047109' balance=Decimal('1938967.00000000') units='NS' value_usd=Decimal('216815289.94000000') payoff='Long' asset_cat='EC' issuer_cat=<set>` |

Both real fixtures parse cleanly with zero exceptions. All fields required by the wrapper (`name, cusip, balance, units, value_usd, payoff_profile, asset_category, issuer_category`) are populated as expected Python typed objects (`str`, `Decimal`, `str`, …).

### 5.3 What the probe confirms and what it does not

The **observed** crash on the synthetic fixture is structural (missing `<filerInfo>` block) — **not** an externally-visible `FundReport(**dict)` Pydantic validation cliff like the one the skill §G3 documents. The skill §G3 framing is wrong on the location of the cliff (it's internal to `parse_fund_xml`, not on a separate `FundReport(**dict)` step) but the *category* "Pydantic cliff" remains a live possibility per §4.2.1 — `parse_fund_xml` constructs `InvestmentOrSecurity` Pydantic models mid-iteration, and `value_usd: Decimal` is non-Optional. A real NPORT-P with a missing `<valUSD>` cell would raise `pydantic.ValidationError` inside `parse_fund_xml`, causing accession-level tombstone rather than per-row drop.

The §5 probe sampled two Vanguard equity index funds with `<valUSD>` populated for every holding. **The probe therefore confirms feasibility for the observed shape; it does NOT prove feasibility universally.** Broader probes across non-Vanguard families + amendments (NPORT-P/A) + edge-shape funds (newly-incepted, fund-of-funds, money-market) MUST land at impl-spec phase or be explicitly deferred with a tombstone-counter monitoring plan.

Therefore:

- **Path A (real-fixture drop-in)**: FEASIBLE for the probed Vanguard-equity-index shape. The skill's §G3 path-A "use full real-world XML in fixtures (25KB+ each)" is the correct primary fixture — and the real Vanguard fixtures are 191KB / 317KB, well above the 25KB threshold the skill named. The wrapper's try/except (catching `AttributeError`, `decimal.InvalidOperation`, `ValueError`, `TypeError`, `pydantic.ValidationError`) maps every observed-and-anticipated EdgarTools failure mode to `NPortParseError` → accession-level tombstone via `_ingest_single_accession`'s existing `NPortParseError` handler at `n_port_ingest.py:820-836`.
- **Path B (dict-only workaround)**: REDUNDANT. `parse_fund_xml` already returns a dict. There is no separate `FundReport(**dict)` constructor layer for the wrapper to skip. The skill §G3 path-B framing was incorrect.
- **Path C (direct lxml ~150 LoC rewrite)**: NOT NEEDED. The existing stdlib parser at `parse_n_port_payload` IS the direct-XML rewrite; the question of this spike is whether replacing it with the EdgarTools wrapper is feasible. It is, via Path A.
- **REBUTTED (close #932 INFEASIBLE + freeze stdlib parser)**: REJECTED. The observed synthetic crash is structural and is mitigated by switching to a real fixture (already required by issue body §4). The unobserved internal Pydantic cliff is mitigated by the wrapper's `try/except` + tombstone path. Neither blocks the drop-in.

## 6. Wrapper shape — implementation outline

The drop-in replaces only the body of `parse_n_port_payload`:

```python
def parse_n_port_payload(xml: str) -> NPortFiling:
    fund_report = _edgar_fund_report()  # lazy-import factory (#925 pattern)
    # Catch every exception class the EdgarTools parser can raise:
    #   - AttributeError: missing structural block (e.g. <filerInfo>,
    #     <fundInfo>, <returnInfo>, <monthlyTotReturns>); the synthetic
    #     fixture's failure mode, see §4.2.
    #   - decimal.InvalidOperation: malformed Decimal text in
    #     <totAssets> / <netAssets> etc. (`Decimal("bogus")` raises
    #     `InvalidOperation`, which inherits from `ArithmeticError`, NOT
    #     `ValueError`). reports.py:1334+ uses bare `Decimal(_text(...))`.
    #   - ValueError: other coercion failures (e.g. `int()` on non-numeric
    #     text inside the parser).
    #   - TypeError: edge-case None coercion downstream of an optional
    #     block dereference.
    #   - pydantic.ValidationError: missing required Decimal field in an
    #     <invstOrSec> row (e.g. <valUSD> absent → InvestmentOrSecurity
    #     constructor rejects non-Optional Decimal); see §4.2.1 for the
    #     accession-level tombstone semantics.
    try:
        parsed = fund_report.parse_fund_xml(xml)
    except (AttributeError, InvalidOperation, ValueError, TypeError, ValidationError) as exc:
        raise NPortParseError(f"NPORT-P EdgarTools parse failed: {exc}") from exc

    general_info = parsed["general_info"]
    # Mirror the current parser's regCik → header cik fallback at
    # app/services/n_port_ingest.py:384. `parse_fund_xml` exposes the
    # header issuer-credentials CIK via parsed["header"].filer_info.
    # issuer_credentials.cik. Prefer general_info.cik (regCik) but fall
    # back so the wrapper's contract matches the existing one.
    cik_text = general_info.cik or _header_issuer_cik(parsed["header"])
    series_id = general_info.series_id
    period_end_text = general_info.fiscal_year_end  # carries repPdEnd; see §4.4
    if not cik_text:
        raise NPortParseError("NPORT-P: missing regCik / header cik in header")
    if not series_id:
        raise NPortMissingSeriesError(
            "NPORT-P: missing seriesId in genInfo header; refusing to "
            "synthesise an identity."
        )
    if not period_end_text:
        raise NPortParseError("NPORT-P: missing repPdEnd in genInfo header")
    period_end = _safe_iso_date(period_end_text)
    if period_end is None:
        raise NPortParseError(f"NPORT-P: malformed repPdEnd={period_end_text!r}")

    # Codex pre-push review #1 (carried over from #917): do NOT default
    # filed_at to period_end midnight inside the parser. The ingester
    # layers in submissions-index filingDate. EdgarTools' parse_fund_xml
    # does not surface a header-level filedAt field — return None so the
    # ingester's documented fallback path fires.
    filed_at = None

    holdings: list[NPortHolding] = []
    for investment in parsed["investments"]:
        balance = investment.balance
        if balance is None:
            continue  # No balance = unparseable holding; skip
        holdings.append(
            NPortHolding(
                cusip=(investment.cusip or "").strip().upper(),
                issuer_name=investment.name or "",
                shares=balance,
                value_usd=investment.value_usd,
                payoff_profile=investment.payoff_profile or "",
                asset_category=investment.asset_category or "",
                issuer_category=investment.issuer_category or "",
                units=investment.units or "",
            )
        )

    return NPortFiling(
        filer_cik=_zero_pad_cik(cik_text),
        series_id=series_id,
        series_name=general_info.series_name or "",
        period_end=period_end,
        filed_at=filed_at,
        holdings=tuple(holdings),
    )


def _edgar_fund_report() -> Any:
    """Lazy import per #925 pattern — defer EdgarTools' filesystem-cache
    mkdir (`~/.edgar/_tcache`) until first parse call. Mirrors the lazy
    import at `app/providers/implementations/sec_13f.py:76-80`."""
    from edgar.funds.reports import FundReport
    return FundReport


def _header_issuer_cik(header: Any) -> str | None:
    """Best-effort dereference of parsed["header"].filer_info.
    issuer_credentials.cik. None if any intermediate is None."""
    try:
        return header.filer_info.issuer_credentials.cik or None
    except AttributeError:
        return None
```

`ValidationError` is `pydantic.ValidationError` — must import inside the wrapper file (lazy alongside `_edgar_fund_report` to keep module import side-effect-free). `InvalidOperation` is `decimal.InvalidOperation` (already in scope at module top — `from decimal import Decimal, InvalidOperation`).

### 6.1 What the wrapper deletes from the current parser

The current `parse_n_port_payload` body (lines 371-454, ~80 LoC of XML-walking):

- `ET.fromstring(xml)` + `ET.ParseError` handling — replaced by EdgarTools' lxml parse.
- `_stripns` / `_find_text` / `_children_by_local_name` / `_decimal_or_none` helpers — replaced by EdgarTools' typed `InvestmentOrSecurity` attribute access. These helpers stay only if other callers use them (grep verification at impl phase).
- Per-holding `_find_text(elem, local_name="cusip" / "name" / "balance" / "valUSD" / "payoffProfile" / "assetCat" / "issuerCat" / "units")` loop — replaced by per-holding attribute access.
- `Decimal(text.strip())` coercion + `InvalidOperation` handling — replaced by EdgarTools' `_opt_decimal` (already returns `Decimal | None` with the same semantics).

### 6.2 What the wrapper preserves

- All six #917 Codex pre-impl invariants (period_end mandatory, series_id mandatory, cik mandatory, filed_at never silently defaulted to period_end midnight, units passthrough, balance None drop).
- The cik fallback chain `regCik → header issuer_credentials.cik` (mirroring `app/services/n_port_ingest.py:384`).
- The public `NPortFiling` / `NPortHolding` dataclass surface — ingester body unchanged.
- The `_NPORT_FORM_TYPES` frozenset (`NPORT-P`, `NPORT-P/A`, `N-PORT`, `N-PORT/A`) — submissions-index walking is separate and not touched.
- The parser-version constant `_PARSER_VERSION_NPORT`. Bump to `"nport-v2-edgartools"` to trigger re-wash across **both** the manifest-worker rewash path (parser-version mismatch detection at #869) **and** the legacy `raw_filings.store_raw(parser_version=_PARSER_VERSION_NPORT, ...)` path at `app/services/n_port_ingest.py:786-793` — both consumers must observe the new constant for a parser-bump rewash to be complete. Impl PR's PR body documents the operator-side trigger (`POST /jobs/sec_rebuild/run` with `{"source": "sec_n_port"}` or equivalent).

### 6.3 Fixture replacement (issue body acceptance #4)

Replace `tests/fixtures/sec/nport_p_test_fund.xml` with a real Vanguard 500 Index Fund (series `S000002277`) NPORT-P primary doc, fetched via the same `SecFilingsProvider`-routed probe used in §5. Adjust existing tests in `tests/test_n_port_ingest.py` accordingly:

- Replace the per-row drop-counter assertions (which currently rely on the synthetic 7-row fixture's hand-crafted distribution) with golden-file assertions against the real fixture's first-row + holdings count + total value.
- Keep `tests/fixtures/sec/nport_p_missing_series.xml` as-is — its purpose is to exercise the `NPortMissingSeriesError` tombstone path, which is wrapper-agnostic (we still raise that error explicitly).

The existing fixture's behavioural coverage (non-equity drop, short drop, non-share-units drop, zero-shares drop, no-CUSIP drop) is exercised by the ingester at `_ingest_single_accession` lines 871-903 against parsed holdings — not by the parser. The replacement fixture's natural row distribution will exercise the equity-common-Long-NS happy path; the drop-counter paths get covered by per-test fixtures constructed in code (small `NPortFiling` dataclass instances passed directly to the ingester's helper functions) OR by smaller dedicated synthetic fixtures that exercise only the ingester's filtering logic (which doesn't touch the parser).

## 7. Decision rule per plan §2 Phase 5 PR 10

The plan's decision rule:

> Spike INFEASIBLE → close #932 REBUTTED + freeze stdlib-ElementTree at `app/services/n_port_ingest.py::parse_n_port_payload`; spike OK → drop-in (parser-only scope per #932 issue body — NOT the manifest-adapter restructure variant rejected in PR 9). Three workarounds enumerated at edgartools §G3 if Pydantic constructor is unreachable.

Outcome: **spike OK → DROP-IN.** The three workarounds at §G3 collapse to "Path A" (real fixture), since the observed synthetic-fixture cliff is structural (mitigated by switching to a real fixture) and the internal Pydantic cliff (§4.2.1) is handled by the wrapper's `try/except` → `NPortParseError` → accession-level tombstone semantics. The dict path is already the default `parse_fund_xml` return shape; Path B is redundant; Path C is not needed.

Manifest-adapter restructure framing is NOT applicable to #932 in the first place — #932 is parser-only scope per its issue body. The "rejected variant" in PR 9 was specific to 13F-HR (where the manifest adapter has ~530 lines of orchestration that #925's plan-§2-PR9 entry mistakenly framed as removable). N-PORT's manifest adapter (`app/services/n_port_ingest.py`) is structurally similar; the wrapper PR does NOT touch it.

## 8. Verdict + recommendation

**Path A (drop-in with real fixture) is FEASIBLE for the probed Vanguard-equity-index NPORT-P shape.** Real Vanguard NPORT-P payloads parse cleanly through `FundReport.parse_fund_xml`. The OBSERVED cliff on the synthetic fixture is structural (missing `<filerInfo>` block) and is mitigated by switching to a real-world fixture, which the issue body §4 already requires. A SEPARATE, UNOBSERVED internal Pydantic cliff (§4.2.1) remains theoretically possible if real-world NPORT-P rows omit `<valUSD>` — mitigated by the wrapper's `try/except` catch-all → `NPortParseError` → accession-level tombstone, but the regression-from-current-behaviour trade-off (per-row drop becomes per-accession drop) must be acknowledged in the impl PR.

### 8.1 Recommended impl scope

1. **Wrapper** at `app/services/n_port_ingest.py::parse_n_port_payload` — body replacement per §6.
2. **Lazy-import factory** `_edgar_fund_report()` mirrors `app/providers/implementations/sec_13f.py:76-80`.
3. **Parser-version bump** `_PARSER_VERSION_NPORT = "nport-v2-edgartools"` — triggers manifest-worker rewash on next manifest tick via the parser-version mismatch contract at #869.
4. **Fixture replacement** at `tests/fixtures/sec/nport_p_test_fund.xml` — Vanguard 500 Index Fund (series `S000002277`) real NPORT-P primary doc.
5. **Golden replay test** at `tests/test_n_port_ingest.py` — assert first-row + holdings count + total `value_usd` sum against the real fixture.
6. **Test surgery** for the drop-counter tests in `test_n_port_ingest.py` — extract the per-counter cases into in-code `NPortHolding` dataclass instances (which is what those tests should have been using all along, since they exercise the ingester not the parser).
7. **Helper cleanup** — remove `_stripns`, `_find_text`, `_children_by_local_name`, `_decimal_or_none` if no other caller depends on them (grep-verify at impl phase).
8. **Skill update** at `.claude/skills/data-sources/edgartools.md` §G3 — correct the Pydantic-validation-cliff framing to "structural-element cliff on synthetic fixtures missing `<filerInfo>` / `<fundInfo>`; mitigation = real-fixture goldens" + cite this spike doc.
9. **Memory refresh** at `feedback_pydantic_validation_cliff.md` — note the N-PORT case is structural not Pydantic, but the discipline (spike fixture compatibility BEFORE drop-in) is identical.

### 8.2 What is NOT recommended

- **Switching to `Filing.obj()` / `FundReport.from_filing(filing)`** — would trigger SEC fetches via EdgarTools' `HTTP_MGR`, bypassing eBull's shared rate-limit pool (prevention-log:510-513). Same hard rejection as the 13F-HR spike §2.
- **Removing the `NPortFiling` / `NPortHolding` public dataclass surface in favour of EdgarTools types directly** — would couple the ingester body to EdgarTools' internal type shapes (`GeneralInfo`, `InvestmentOrSecurity`), breaking the wrapper abstraction. The dataclass surface is the seam that lets the impl PR be parser-only.
- **Extending the wrapper to also consume `fund_info.total_assets / net_assets`** — out of scope per #932 issue body "Out of scope: Schema changes to `ownership_funds_*` tables". Fund-level NAV is a #919 deferred item; the wrapper consumes only the per-holding fields the ingester already uses.
- **Removing the existing `parse_submissions_index` walker or `_NPORT_FORM_TYPES` frozenset** — those are submissions-index logic, not primary-doc parser logic; out of scope.

### 8.3 Settled-decisions impact

| Decision (§2) | Outcome |
|---|---|
| §"Fundamentals provider posture" (#532) | PRESERVED. |
| §"Provider design rule" | PRESERVED — parser body change only; service-layer placement unchanged. |
| §"Filing event storage" — raw-payload persistence per #1168 | PRESERVED — fetch/store sequence unchanged. |
| §"Source priority for fund metadata (#1171)" | PRESERVED — `period_end` + `filed_at` semantics unchanged. |

## 9. Cross-spike note

The 13F-HR spike (`docs/superpowers/spikes/2026-05-18-13f-hr-edgartools-feasibility.md`) at §9 documents:

> #932's scope is structurally narrower than the plan §2 PR 9 alternative framing: replace the stdlib-ElementTree NPORT-P parser at `app/services/n_port_ingest.py::parse_n_port_payload` with a thin wrapper over `edgar.funds.reports.FundReport.parse_fund_xml`, preserving the public `NPortFiling` / `NPortHolding` dataclass surface — mirroring the parser-only scope PR #931 actually shipped for 13F-HR. **#932 does NOT propose adopting `Filing.obj()` for the N-PORT manifest path**, which is the structurally infeasible variant rejected here.
>
> The Pydantic validation cliff documented in `[[edgartools]]` memory + #932 applies to N-PORT (`FundReport.parse_fund_xml`), not to 13F-HR. The 13F-HR Berkshire 2024Q3 golden fixture parses cleanly through EdgarTools' static parsers (no Pydantic-constructor crash; §3.3 replay confirms). PR 10's spike must independently verify whether the existing N-PORT fixtures at `tests/fixtures/sec/nport_p_*.xml` clear `FundReport.parse_fund_xml`'s Pydantic constructor; that is a separate empirical question this spike does not address.

This spike answers that cross-reference partially: the existing synthetic fixture does NOT clear `parse_fund_xml`, and the OBSERVED failure mode is structural-element-missing (`<filerInfo>` block absent), not externally-visible Pydantic-validation. Real Vanguard NPORT-P primary docs clear `parse_fund_xml` without exception. A SEPARATE internal Pydantic cliff (§4.2.1) is theoretically possible on real-world filings with missing `<valUSD>` cells but has not been observed in the probed sample. The 13F-HR spike's "separate empirical question" is hereby answered for the Vanguard-equity-index NPORT-P shape; the broader probe-space remains an impl-PR concern.

## 10. Execution constraints + out of scope

- The probe issued 3 SEC HTTP requests (1 submissions JSON + 2 primary docs) at 10 r/s budget. No SEC fair-use violation.
- No new dependencies introduced. `edgartools==5.30.2` is already pinned at `pyproject.toml:21` (#925).
- The scratch probe script was created at the repo root for `app.*` import resolution, then deleted post-probe. The two fetched fixtures were moved to `/tmp/` for the spike PR (impl PR re-fetches the canonical Vanguard 500 fixture).
- ETL clauses #8-#12: NOT APPLICABLE to this spike PR (docs-only). The impl PR (drop-in) MUST satisfy clauses #8-#12 against the smoke panel (AAPL/GME/MSFT/JPM/HD) for any `ownership_funds_*` rollup endpoint that surfaces post-drop-in — although N-PORT ingest itself only touches the fund-side rollup (#919 deferred), so the standard smoke panel may not exercise an operator-visible figure change. Impl PR records this explicitly in §"ETL DoD clauses" of its PR body.

Out of scope:

- Schema changes to `ownership_funds_observations` / `ownership_funds_current` — #932 issue body §"Out of scope".
- Ingester body changes (`_ingest_single_accession` write-side guards, tombstone, log) — #932 issue body §"Out of scope".
- Operator-visible rollup integration — deferred to #919.
- N-CSR drop-in — #918 closed (synth no-op, `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` INFEASIBLE-CONFIRMED).
- Form 3/4/5 / 13D/G EdgarTools drop-ins — not in #932 scope.

## 11. Verification commands (reproducible)

```
# Reproduce synthetic-fixture cliff (returns AttributeError)
uv run python -c "
from edgar.funds.reports import FundReport
xml = open('tests/fixtures/sec/nport_p_test_fund.xml').read()
try:
    FundReport.parse_fund_xml(xml)
except Exception as e:
    print(f'CRASH: {type(e).__name__}: {e}')
"

# Inspect parse_fund_xml surface
uv run python -c "
from edgar.funds.reports import FundReport
import inspect
print(f'returns Dict[str, Any]: {inspect.signature(FundReport.parse_fund_xml).return_annotation}')
"

# Fetch real Vanguard NPORT-P via shared rate-limit pool (in-repo, then delete)
# See §5.1 for the script body. Save to .scratch_nport_probe.py at repo root, run, delete.

# Confirm pin
grep edgartools pyproject.toml
```

All four reproduce the empirical state recorded in §3 + §4 + §5.

## 12. Pre-impl checklist for the drop-in PR

- [ ] Branch `fix/932-nport-edgartools-dropin` (off main; spike doc lands first via the current branch `fix/932-nport-edgartools-spike` OR is bundled into the drop-in branch).
- [ ] Lazy-import factory `_edgar_fund_report()` added; module-level `from edgar...` import absent (proven by grep).
- [ ] `parse_n_port_payload` body replaced; `NPortFiling` / `NPortHolding` surface unchanged.
- [ ] `_PARSER_VERSION_NPORT` bumped to `"nport-v2-edgartools"`.
- [ ] `tests/fixtures/sec/nport_p_test_fund.xml` replaced with real Vanguard 500 (series S000002277) NPORT-P primary doc.
- [ ] Existing `tests/test_n_port_ingest.py` drop-counter tests rewritten to use in-code `NPortHolding` instances OR small dedicated synthetic fixtures targeted at the ingester filter paths.
- [ ] Golden replay test added: first-row issuer / cusip / shares / value_usd + holdings count + total holdings value sum.
- [ ] Skill `.claude/skills/data-sources/edgartools.md` §G3 updated.
- [ ] Memory `feedback_pydantic_validation_cliff.md` updated.
- [ ] Codex 1a on spec; Codex 1b on plan; Codex 2 pre-push on diff.
- [ ] PR body documents the parser-version bump + the operator-side rewash trigger (`POST /jobs/sec_rebuild/run` with `{"source": "sec_n_port"}` or equivalent).
- [ ] PR body satisfies ETL clauses #8-#12 framing (smoke panel: N-PORT ingest does not change an operator-visible figure in v1 since #919 is deferred; record this explicitly).
