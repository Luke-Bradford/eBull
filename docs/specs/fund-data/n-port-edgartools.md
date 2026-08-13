# #932 — N-PORT EdgarTools `FundReport` parser drop-in (spec)

> Status: DRAFT 2026-05-18.
>
> Issue: #932.
> Plan: `docs/_archive/2026-05-17-us-etl-completion.md` §2 Phase 5 PR 10.
> Spike: `docs/_archive/2026-05-18-n-port-edgartools-feasibility.md` (CLEAN through Codex 1a r3 — verdict FEASIBLE for the probed Vanguard-equity-index NPORT-P shape).
> Architectural predecessor: PR #931 (#925 13F-HR parser drop-in — merged 2026-05-05, commit `0428dbf`) — same lazy-import + wrapper + golden-replay shape. PR #1203 was the spike-only closeout for #925 administrative state; not architecturally relevant here.

## 1. Goal

Replace the stdlib-`xml.etree.ElementTree` body of `app/services/n_port_ingest.py::parse_n_port_payload` with a thin wrapper over `edgar.funds.reports.FundReport.parse_fund_xml`. Preserve the public `NPortFiling` / `NPortHolding` dataclass surface. Preserve every #917 Codex pre-impl invariant. Bump the parser-version constant to trigger manifest-worker rewash. Replace the hand-trimmed fixture with a real Vanguard 500 Index Fund (series `S000002277`) NPORT-P primary doc.

Scope is **parser-only** per #932 issue body. Manifest-adapter restructure is out of scope.

## 2. Settled-decisions preservation

Per the spike doc §2:

| Decision | Status |
|---|---|
| §"Fundamentals provider posture" (#532) — free regulated source only | PRESERVED. EdgarTools is a parsing library, not a data source. |
| §"Provider design rule" — providers thin, DB lookups in services | PRESERVED. Parser body change inside the existing service layer. |
| §"Filing event storage" — raw-payload persistence per #1168 | PRESERVED. `_ingest_single_accession` fetches → `raw_filings.store_raw` → `conn.commit()` → `parse_n_port_payload(...)`. No call ordering change. |
| §"Source priority for fund metadata (#1171)" — `ORDER BY period_end DESC, filed_at DESC, source_accession DESC` | PRESERVED. `filed_at` parser-side returns `None` so the ingester's submissions-index fallback fires; tie-breaks unchanged. |

Prevention-log entries (per spike §2): rate-limit pool (PRESERVED — wrapper does not fetch), raw-payload persistence (PRESERVED — no ordering change), Pydantic validation cliff (mitigated via wrapper `try/except` + tombstone), EdgarTools internal-path stability (mitigated via tight pin + golden replay).

## 3. Implementation contract

### 3.1 Wrapper body — `app/services/n_port_ingest.py::parse_n_port_payload`

Replaces the existing ~80 LoC of XML walking. The body is exactly what the spike doc §6 outlined; final approved form:

```python
def parse_n_port_payload(xml: str) -> NPortFiling:
    """Parse an NPORT-P primary doc XML into an NPortFiling.

    Wraps edgartools' edgar.funds.reports.FundReport.parse_fund_xml (#932,
    superseding the stdlib-ET parser from #917). The wrapper:

    * Lazy-imports edgartools at first call so module import remains
      side-effect-free (#925 pattern; mirrors sec_13f.py:76-80).
    * Catches every EdgarTools parser failure class (AttributeError,
      InvalidOperation, ValueError, TypeError, pydantic.ValidationError)
      and converts to NPortParseError so _ingest_single_accession's
      tombstone path at n_port_ingest.py:820-836 fires.
    * Preserves the six #917 Codex pre-impl invariants verbatim
      (period_end / series_id / cik mandatory; filed_at None passed to
      the ingester for submissions-index fallback; units passthrough;
      balance None drop).
    * Preserves the regCik → header issuer_credentials.cik fallback
      chain from the prior parser (n_port_ingest.py:384 pre-#932).

    Note on EdgarTools field naming: ``general_info.fiscal_year_end``
    confusingly stores ``repPdEnd`` text (the period_end), NOT the fund's
    actual fiscal-year-end. See spike doc §4.4. The golden replay test
    locks this mapping against pin bumps.

    Pure XML-in / dataclass-out. No network calls, no DB access.
    Raises NPortParseError on malformed XML or any parser failure
    listed above; raises NPortMissingSeriesError if the filing has no
    seriesId (refuses to synthesise identity per Codex review #2).
    """
    fund_report = _edgar_fund_report()
    pydantic_validation_error = _pydantic_validation_error()
    # Catch scope covers BOTH the EdgarTools parser call AND the
    # post-parse normalisation. Any KeyError (dict-shape drift on pin
    # bump), AttributeError (missing typed object attribute), or
    # int/Decimal coercion ValueError that fires during normalisation
    # must also surface as NPortParseError so _ingest_single_accession's
    # tombstone path at n_port_ingest.py:820-836 fires; otherwise the
    # filer-batch driver at ingest_fund_n_port catches with `Exception`
    # at a coarser layer and the per-accession tombstone is lost.
    # NPortMissingSeriesError is re-raised explicitly so its dedicated
    # tombstone path (different from the generic NPortParseError path)
    # fires.
    try:
        parsed = fund_report.parse_fund_xml(xml)

        general_info = parsed["general_info"]
        cik_text = (general_info.cik or "").strip() or _header_issuer_cik(parsed.get("header"))
        series_id_raw = (general_info.series_id or "").strip()
        period_end_text = (general_info.fiscal_year_end or "").strip() or None  # carries repPdEnd
        series_name = (general_info.series_name or "").strip()

        if not cik_text:
            raise NPortParseError("NPORT-P: missing regCik / header cik")
        if not series_id_raw:
            raise NPortMissingSeriesError(
                "NPORT-P: missing seriesId in genInfo header; refusing to "
                "synthesise an identity. Filing tombstoned for operator review."
            )
        if not period_end_text:
            raise NPortParseError("NPORT-P: missing repPdEnd in genInfo header")
        period_end = _safe_iso_date(period_end_text)
        if period_end is None:
            raise NPortParseError(f"NPORT-P: malformed repPdEnd={period_end_text!r}")

        # EdgarTools' parse_fund_xml does NOT surface a header-level
        # filedAt field. Returning None here triggers the ingester's
        # documented fallback to the submissions-index filingDate at
        # _ingest_single_accession (#917 Codex pre-push review #1).
        # Never default to period_end midnight inside the parser.
        filed_at: datetime | None = None

        holdings: list[NPortHolding] = []
        for investment in parsed["investments"]:
            balance = investment.balance
            if balance is None:
                # No balance = unparseable holding; parser-level drop.
                continue
            # Strip + upper categorical fields. The downstream
            # ingester guards (units != "NS", payoff_profile != "Long",
            # asset_category != "EC") are exact-equality so a stray
            # leading/trailing whitespace from a future EdgarTools
            # whitespace-preservation change would mis-drop valid rows.
            # CUSIP is also case-normalised to upper (mirrors the
            # pre-#932 stdlib parser's `.strip().upper()` contract).
            holdings.append(
                NPortHolding(
                    cusip=(investment.cusip or "").strip().upper(),
                    issuer_name=(investment.name or "").strip(),
                    shares=balance,
                    value_usd=investment.value_usd,
                    payoff_profile=(investment.payoff_profile or "").strip(),
                    asset_category=(investment.asset_category or "").strip(),
                    issuer_category=(investment.issuer_category or "").strip(),
                    units=(investment.units or "").strip(),
                )
            )

        return NPortFiling(
            filer_cik=_zero_pad_cik(cik_text),
            series_id=series_id_raw,
            series_name=series_name,
            period_end=period_end,
            filed_at=filed_at,
            holdings=tuple(holdings),
        )
    except NPortMissingSeriesError:
        # Distinct from generic NPortParseError tombstone path.
        raise
    except NPortParseError:
        # Already-classified parser error; preserve verbatim.
        raise
    except (AttributeError, KeyError, InvalidOperation, ValueError, TypeError, pydantic_validation_error) as exc:
        raise NPortParseError(f"NPORT-P EdgarTools parse failed: {exc}") from exc


def _edgar_fund_report() -> Any:
    """Lazy import: defer EdgarTools' filesystem-cache mkdir
    (`~/.edgar/_tcache`) until first parse call. Mirrors the lazy
    factory at app/providers/implementations/sec_13f.py:76-80."""
    from edgar.funds.reports import FundReport
    return FundReport


def _pydantic_validation_error() -> type[Exception]:
    """Lazy import: pydantic comes in via edgartools but we hold our
    own reference to its ValidationError type for the wrapper's catch
    list. Lazy to keep module import side-effect-free."""
    from pydantic import ValidationError
    return ValidationError


def _header_issuer_cik(header: Any) -> str | None:
    """Best-effort dereference of parsed['header'].filer_info.
    issuer_credentials.cik. None if any intermediate is None or missing.

    Mirrors the regCik → header cik fallback that the pre-#932 stdlib
    parser performed via _find_text(root, local_name='cik') at
    n_port_ingest.py:384."""
    if header is None:
        return None
    try:
        cik = header.filer_info.issuer_credentials.cik
    except AttributeError:
        return None
    if not cik:
        return None
    return cik.strip() or None
```

### 3.2 What changes in the module

- **Removed**: `_stripns`, `_find_text`, `_children_by_local_name`, `_decimal_or_none` helpers (lines ~327-355). Verified via grep no other caller. The module's `import xml.etree.ElementTree as ET` import + the `# noqa: S405` comment can also be removed once `ET.ParseError` is no longer referenced.
- **Added**: `_edgar_fund_report`, `_pydantic_validation_error`, `_header_issuer_cik` factories.
- **Modified**: `parse_n_port_payload` body (the only behaviour change). Public dataclasses + custom exceptions unchanged.
- **Bumped**: `_PARSER_VERSION_NPORT = "nport-v1"` → `"nport-v2-edgartools"`. This triggers manifest-worker rewash via parser-version mismatch detection (#869) AND the legacy `raw_filings.store_raw(parser_version=...)` path at `n_port_ingest.py:786-793` propagates the new tag forward.

### 3.3 Existing module shape preserved

- `_NPORT_FORM_TYPES` frozenset.
- `AccessionRef` / `NPortHolding` / `NPortFiling` / `IngestSummary` / `_AccessionOutcome` / `_MutableSummary` dataclasses.
- `NPortParseError` / `NPortMissingSeriesError` exceptions.
- `SecArchiveFetcher` Protocol.
- `_zero_pad_cik` / `_accession_no_dashes` / `_submissions_url` / `_archive_file_url` URL helpers.
- `parse_submissions_index` walker + `_safe_iso_date` / `_safe_iso_datetime` helpers.
- `_existing_accessions_for_fund_filer` / `_record_ingest_attempt` / `_resolve_cusip_to_instrument_id` DB helpers.
- `ingest_fund_n_port` / `ingest_all_fund_filers` / `_ingest_single_accession` / `iter_fund_observations` public + driver functions.

## 4. Fixture changes

### 4.1 Replace `tests/fixtures/sec/nport_p_test_fund.xml`

The current 4.3KB synthetic file is deleted. Replaced with a real **Vanguard 500 Index Fund (series `S000002277`)** NPORT-P primary doc, fetched via the shared `SecFilingsProvider` rate-limit pool. The selection rule:

1. Iterate `parse_submissions_index(SecFilingsProvider.fetch_document_text(_submissions_url("0000036405")))`.
2. Pick the most recent `NPORT-P` accession whose `general_info.series_id == "S000002277"` (verified via a one-off probe parse).
3. Save the primary_doc.xml as `tests/fixtures/sec/nport_p_test_fund.xml`.

Expected size ~200-350KB (per the §5 spike probe of sibling Vanguard funds: 191KB / 317KB).

### 4.2 Update `tests/fixtures/sec/nport_p_missing_series.xml`

The current 686-byte file lacks the structural blocks EdgarTools' parser dereferences unconditionally; the missing-series test would crash on a generic `NPortParseError` before reaching the dedicated `NPortMissingSeriesError` path.

**Required structural blocks** (empirically verified against `edgartools==5.30.2` at `edgar/funds/reports.py:1241-1348`):

| Path | What EdgarTools does | Mandatory in fixture |
|---|---|---|
| `<headerData>` | `header_el = root.find("headerData")` | YES |
| `<headerData><filerInfo>` | `filer_info_tag = header_el.find("filerInfo")` (NEXT line dereferences `.find(".//issuerCredentials")` so cannot be None) | YES |
| `<filerInfo>...<issuerCredentials>` | `.find(".//issuerCredentials")` — `IssuerCredentials(cik=str, ccc=str)`. Both fields are non-Optional Pydantic `str`; the tag must include `<cik>` + `<ccc>` text children (empty strings are accepted, missing children raise `ValidationError` mid-parse). | YES (descendant; nest under `<filer>` for canonical shape) |
| `<filerInfo>...<seriesClassInfo>` | `.find(".//seriesClassInfo")` — `SeriesClassInfo.from_xml(None)` handles None (returns `None`) | OPTIONAL |
| `<formData>` | `form_data_tag = root.find("formData")` | YES |
| `<formData><genInfo>` | `general_info_tag = form_data_tag.find("genInfo")` | YES |
| `<genInfo>` Pydantic-required text fields | `GeneralInfo` model (`edgar/funds/reports.py:126-145`) declares `name: str`, `cik: str`, `file_number: str`, `street1: str` as non-Optional. EdgarTools' `_text()` returns `None` for self-closing or absent tags; that None then fails Pydantic `str` validation. **Tags must be present AND carry non-empty text content** (`<regName>X</regName>`, not `<regName/>`). | YES — `<regName>`, `<regCik>`, `<regFileNumber>`, `<regStreet1>` all present with non-empty text |
| `<formData><fundInfo>` | `fund_info_tag = form_data_tag.find("fundInfo")` (NEXT line dereferences `.find("curMetrics")`) | YES |
| `<fundInfo>` 9 Decimal-required fields | `Decimal(_text(fund_info_tag, "<name>"))` raises `InvalidOperation` if None. Required fields: `totAssets`, `totLiabs`, `netAssets`, `assetsAttrMiscSec`, `assetsInvested`, `amtPayOneYrBanksBorr`, `amtPayOneYrCtrldComp`, `amtPayOneYrOthAffil`, `amtPayOneYrOther` | YES — each as `<name>0</name>` is sufficient |
| `<fundInfo><returnInfo>` | `return_info_tag = fund_info_tag.find("returnInfo")` (NEXT line `.find("monthlyTotReturns")`) | YES |
| `<returnInfo><monthlyTotReturns>` | `monthlyTotReturns.findall("monthlyTotReturn")` — empty list is OK | YES (empty self-closing tag is sufficient) |
| `<returnInfo><othMon1/2/3>` | `RealizedChange.from_xml(tag)` returns `None` when tag is None, then `ReturnInfo(other_mon1=RealizedChange, ...)` (non-Optional at `reports.py:232-236`) raises `pydantic.ValidationError` mid-parse. Empty self-closing tag → `from_xml(tag)` returns a `RealizedChange` instance (with `decimal_or_na` defaulting on missing attrs). | YES (each as `<othMon1/>` self-closing) |
| `<fundInfo><mon1Flow / mon2Flow / mon3Flow>` | `MonthlyFlow.from_xml(tag)` returns None when tag is None | OPTIONAL |
| `<formData><invstOrSecs>` | `investment_or_secs_tag = form_data_tag.find("invstOrSecs")` — `if not None:` iterates; empty self-closing tag is OK | OPTIONAL (recommended self-closing) |

The missing-series fixture **must** carry every YES row above with `<seriesId>` deliberately omitted under `<genInfo>` to exercise the `NPortMissingSeriesError` path. Expected file size grows from 686 bytes to ~1.5KB.

Concrete minimal shape (each numeric field as `0`; language hint `xml`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/nport">
  <headerData>
    <submissionType>NPORT-P</submissionType>
    <filerInfo>
      <filer>
        <issuerCredentials>
          <cik>0000036405</cik>
          <ccc>XXXXXXXX</ccc>
        </issuerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <genInfo>
      <regName>Vanguard Index Funds</regName>
      <regCik>0000036405</regCik>
      <regFileNumber>811-02652</regFileNumber>
      <regStreet1>PO Box 2600</regStreet1>
      <seriesName>Mystery Fund No Series Id</seriesName>
      <repPdEnd>2025-12-31</repPdEnd>
    </genInfo>
    <fundInfo>
      <totAssets>0</totAssets>
      <totLiabs>0</totLiabs>
      <netAssets>0</netAssets>
      <assetsAttrMiscSec>0</assetsAttrMiscSec>
      <assetsInvested>0</assetsInvested>
      <amtPayOneYrBanksBorr>0</amtPayOneYrBanksBorr>
      <amtPayOneYrCtrldComp>0</amtPayOneYrCtrldComp>
      <amtPayOneYrOthAffil>0</amtPayOneYrOthAffil>
      <amtPayOneYrOther>0</amtPayOneYrOther>
      <returnInfo>
        <monthlyTotReturns/>
        <othMon1/>
        <othMon2/>
        <othMon3/>
      </returnInfo>
    </fundInfo>
    <invstOrSecs/>
  </formData>
</edgarSubmission>
```

Impl PR's responsibility: verify this fixture clears EdgarTools structurally + still raises `NPortMissingSeriesError` (not generic `NPortParseError`) via the `test_raises_missing_series_id` test.

### 4.3 No additional fixtures

The ingester filter-path tests (non-equity drop, short drop, unresolved-CUSIP drop, PA-units drop, zero-shares drop) refactor to construct `NPortFiling` / `NPortHolding` instances directly in Python and monkeypatch `n_port_ingest.parse_n_port_payload` at the call-site. The parser is tested separately against the real Vanguard fixture; the ingester filter logic is tested without involving XML at all. Justified by:

- Cleanly separates parser-correctness tests from ingester-filter tests.
- Eliminates the maintenance burden of a hand-rolled synthetic NPORT-P matching EdgarTools' structural requirements.
- Test intent becomes obvious — the construct-`NPortFiling`-with-7-rows pattern says "this test exercises ingester filtering" without the reader scrolling through 100 lines of XML.

## 5. Test changes

### 5.1 `tests/test_n_port_ingest.py::TestParseNPortPayload`

- **`test_extracts_header_and_holdings`** — REWRITE. Assert against the new real Vanguard 500 fixture. Expected assertions:
  - `parsed.filer_cik == "0000036405"`
  - `parsed.series_id == "S000002277"`
  - `parsed.series_name` starts with `"Vanguard 500 Index Fund"` (case-insensitive — the SEC payload uses `"Vanguard 500 Index Fund"` or `"VANGUARD 500 INDEX FUND"`; the test should be tolerant).
  - `parsed.period_end == date(2025, 12, 31)` (or whatever the chosen fixture's `repPdEnd` is — locked at fixture-fetch time).
  - `parsed.filed_at is None` (per the wrapper's documented contract — filed_at is layered in by the ingester from submissions-index).
  - `len(parsed.holdings) >= 400` (Vanguard 500 has ~500 stocks; the actual count is locked at fixture-fetch time).
  - First-row assertions: top holding by `value_usd` is one of the AAPL / MSFT / NVDA / GOOGL / AMZN cohort with a 9-digit CUSIP + `units == "NS"` + `payoff_profile == "Long"` + `asset_category == "EC"`.

- **`test_raises_missing_series_id`** — KEEP. Uses the updated `nport_p_missing_series.xml` (§4.2). Asserts `NPortMissingSeriesError` raised.

- **`test_raises_on_malformed_xml`** — KEEP. Uses inline `"<not><well><formed>"`. Need to confirm EdgarTools' `etree.fromstring` (with `recover=True` fallback at `reports.py:1228-1230`) does not silently succeed — verify at impl time. If it does, change the malformed input to something that the recover-mode parser still rejects (e.g., trailing-garbage-after-root or a missing `</edgarSubmission>`).

- **`test_runs_offline_no_network`** — KEEP semantics. Asserts no `urllib.request.urlopen` / `httpx.get` / `httpx.Client.request` call during parse. Point at the new fixture path. EdgarTools' `parse_fund_xml` is documented as pure-XML-in / dict-out; the test guards against future regressions where a refactor introduces an HTTP call inside the lazy-import path.

- **`test_golden_replay_first_row_count_total`** — NEW. Golden-file replay: locks the most-valuable-by-`value_usd` holding's `cusip` + `shares` + `value_usd` AND the total `sum(value_usd)` AND the holdings count to exact values. This is the pin-bump regression guard. If EdgarTools renames a field or changes the Decimal coercion in a future minor bump, this test fails loudly.

### 5.2 `tests/test_n_port_ingest.py::TestParseSubmissionsIndex`

UNCHANGED. Submissions-index walker is independent of the parser.

### 5.3 `tests/test_n_port_ingest.py::TestRecordFundObservation`

UNCHANGED. DB writer tests use `record_fund_observation` directly.

### 5.4 `tests/test_n_port_ingest.py::TestIngestFundNPort`

**Three of the four tests REFACTOR; one remains integration.** Each test currently feeds the synthetic `nport_p_test_fund.xml` through `ingest_fund_n_port`, then asserts on the row-distribution outcome. After §4.3:

- **`test_ingest_drops_non_equity_short_unresolved`** — REFACTOR. Monkeypatched parser returns 7 holdings: AAPL (Long EC NS) + MSFT (Long EC NS) + ACME-DBT + SHRT-X (Short EC NS) + ZZZZ (Long EC NS unresolved CUSIP) + CVBND (Long EC PA) + ZRO (Long EC NS zero-shares). Assertions identical to current: `holdings_inserted == 2`, drop counters, `partial` status, AAPL row landing, sec_fund_series upsert, raw payload row. The raw-payload-row assertion additionally checks `parser_version == "nport-v2-edgartools"` (parser-version-bump regression guard — §7).
- **`test_idempotent_reingest`** — REFACTOR. Monkeypatched parser returns ANY valid 7-row `NPortFiling`. Assertion: second invocation's `_FakeFetcher.fetch_log` does NOT contain the primary_doc URL.
- **`test_amendment_uses_submissions_filed_at_when_header_missing`** — REFACTOR. Monkeypatched parser returns two different `NPortFiling`s (one per accession) with `filed_at=None` for both. Submissions-index filingDate `2026-03-15` vs `2026-02-26` drives the tie-break. Amendment wins; AAPL shares = `Decimal("1234567")`.
- **`test_missing_series_id_tombstones_failed`** — STAYS as integration test. Uses `nport_p_missing_series.xml` (real path: fixture XML → real EdgarTools parser → `NPortMissingSeriesError`). This is the only end-to-end test that proves the wrapper's `NPortMissingSeriesError` actually propagates through the tombstone path against a real EdgarTools parse. Do NOT stub it.

For the three patched tests, the monkeypatch target is `app.services.n_port_ingest.parse_n_port_payload` (called inside `_ingest_single_accession` at line 801). The patch is per-test scoped — applied via `monkeypatch.setattr(...)` inside each test function so other tests in the same module see the real parser.

### 5.5 `tests/test_n_port_ingest.py::TestFundsSchema` / `TestIngestAllStatusPrecedence` / `TestManifestFormCodes`

UNCHANGED. Schema-level + runner-level tests are parser-independent.

### 5.7 `tests/test_manifest_parser_sec_n_port.py`

The manifest-worker adapter at `app/services/manifest_parsers/sec_n_port.py` calls `parse_n_port_payload` through the same code path the legacy ingester uses, and the test file at `tests/test_manifest_parser_sec_n_port.py:39-40` reads both fixtures at module-import time. Touchpoints after §4 + §7:

- `_FAKE_NPORT_XML = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(...)` — points at the new real Vanguard 500 fixture. The happy-path test's row-count + AAPL/MSFT specific assertions will need updating to match the real-fixture distribution (the real fixture has ~500 Long-EC-NS rows; the test seeds CUSIP mappings for AAPL + MSFT — for the impl PR's spike-fetched accession, decide whether to seed additional cohorts or assert against the top-N).
- `_FAKE_NPORT_MISSING_SERIES_XML = (_FIXTURE_DIR / "nport_p_missing_series.xml").read_text(...)` — points at the updated missing-series fixture (§4.2). Behaviour unchanged: missing-series → `NPortMissingSeriesError` → audit log `failed`.
- **Parser-version assertion (NEW)**: every happy-path manifest test that asserts on `filing_raw_documents` rows MUST additionally assert `parser_version == "nport-v2-edgartools"`. Pin-bump regression guard parallel to §5.4.
- **Adapter wiring (UNCHANGED)**: `register_all_parsers` registration test stays identical; the adapter file at `app/services/manifest_parsers/sec_n_port.py` imports `_PARSER_VERSION_NPORT` via the `from app.services.n_port_ingest import _PARSER_VERSION_NPORT` symbol (line 61), so the bump auto-propagates to the adapter's 7 call-sites (lines 87, 111, 155, 166, 205, 403, 410).

### 5.8 Test helper — `_make_filter_path_filing`

Add a module-level helper in `tests/test_n_port_ingest.py` that constructs a 7-row `NPortFiling` with the canonical filter-path distribution, parameterised by `series_id` / `period_end` / `aapl_balance`:

```python
def _make_filter_path_filing(
    *,
    filer_cik: str = "0000036405",
    series_id: str = "S000002277",
    series_name: str = "Vanguard 500 Index Fund (TEST)",
    period_end: date = date(2025, 12, 31),
    filed_at: datetime | None = None,
    aapl_balance: Decimal = Decimal("1000000"),
) -> NPortFiling:
    ...
```

Used by the three patched tests in §5.4.

## 6. Skill + memory updates (in-scope)

Per the spike doc §8.1.

### 6.1 `.claude/skills/data-sources/edgartools.md`

§G3 "N-PORT Pydantic validation cliff" — REWRITE. The current text is wrong on the location of the cliff. Replace with:

```
### G3 — N-PORT structural + Pydantic cliffs

`FundReport.parse_fund_xml(xml)` returns a `Dict[str, Any]` with keys
`header / general_info / fund_info / investments`. Two failure modes:

1. **Structural cliff (observed on synthetic fixtures)**: the parser
   unconditionally dereferences `<filerInfo>` + `<fundInfo>` +
   `<returnInfo>` + `<monthlyTotReturns>` blocks. Missing any block →
   `AttributeError: 'NoneType' object has no attribute 'find'`. Hand-
   trimmed fixtures must include all four blocks.
2. **Internal Pydantic cliff (latent on real-world payloads)**:
   `InvestmentOrSecurity.value_usd: Decimal` is non-Optional at
   `edgar/funds/reports.py:346`. An NPORT-P row that omits `<valUSD>`
   raises `pydantic.ValidationError` mid-iteration, aborting the
   whole-accession parse. Unobserved on the Vanguard probe sample
   (#932 spike §5) but theoretically possible.

**Rule of thumb**: if a form's module imports `from pydantic import
BaseModel`, expect a validation cliff for synthetic fixtures AND for
real-world payloads with missing required Decimal cells. Wrap the
parser call AND the post-parse dict-key dereferences in `try/except
(AttributeError, KeyError, decimal.InvalidOperation, ValueError,
TypeError, pydantic.ValidationError)` and convert to a parser-specific
error type so accession-level tombstones fire. `KeyError` defends
against dict-shape drift on pin bump.

Pinned at edgartools 5.30.2 by `pyproject.toml:21` (#925). See
`docs/_archive/2026-05-18-n-port-edgartools-feasibility.md`
for the full empirical analysis.
```

### 6.2 `feedback_pydantic_validation_cliff.md` memory

UPDATE — add a section noting the #932 case where the OBSERVED failure was structural (synthetic fixture) but a SEPARATE internal Pydantic cliff remains theoretically possible per the InvestmentOrSecurity.value_usd Decimal non-Optional constraint. Mitigation pattern: wrap parser in try/except + convert to accession-level tombstone.

## 7. Parser-version bump + operator follow-up

`_PARSER_VERSION_NPORT = "nport-v2-edgartools"`. Affects:

- **Manifest worker** rewash path (#869) — `filing_raw_documents.parser_version != _PARSER_VERSION_NPORT` rows are reset to `pending` and re-drained on the next manifest tick.
- **Legacy `raw_filings.store_raw`** path at `n_port_ingest.py:786-793` — the legacy ingest path stamps the new version on every fresh write.

**Operator follow-up (post-merge)**:

1. Trigger `POST /jobs/sec_rebuild/run` with `{"source": "sec_n_port"}` to reset manifest rows + redrain. Estimated payload count: ~200-1000 N-PORT accessions across the active fund-filer universe; at 10 req/s shared SEC budget the rewash drains in ~20-100 seconds.
2. Confirm `n_port_ingest_log` rows shift from old-version-tombstone (if any) to fresh `success` / `partial` per the new parser.
3. Spot-check one operator-visible figure — but per ETL DoD §"Operator runbook" the only operator-visible N-PORT figure in v1 is the rollup endpoint deferred to #919, so no smoke-panel-visible figure changes hands. Document this explicitly in PR body.

## 8. ETL DoD clauses #8-#12

| Clause | Status |
|---|---|
| **#8 (smoke against 3-5 instruments)** | N/A in operator-visible sense — N-PORT does not surface to the standard AAPL/GME/MSFT/JPM/HD smoke panel in v1 (the rollup endpoint is deferred to #919). PR body records this. |
| **#9 (cross-source verify)** | **Independent raw XML spot-check** against the live SEC NPORT-P at `www.sec.gov/Archives/edgar/data/...` (not a true cross-source verification — that requires #919's rollup + an external source like Morningstar / SEC EDGAR Fund Browser — both deferred). PR body records this framing explicitly. True cross-source verification is N/A until #919 lands. |
| **#10 (backfill executed)** | Operator follow-up §7 records the `POST /jobs/sec_rebuild/run` invocation post-merge. |
| **#11 (operator-visible figure verified)** | N/A — no rollup endpoint surfaces N-PORT in v1; deferred to #919. PR body records this. |
| **#12 (PR description records verification + commit SHA for #8-#11)** | PR body satisfies via explicit annotations of N/A status + the §7 operator-trigger plan. |

## 9. Smoke + cross-source verification

The PR's "operator-visible figure" surface is currently empty (rollup endpoint deferred to #919). The PR body MUST therefore record:

- **What was exercised**: the canonical Vanguard 500 fixture's first-row + count + total `value_usd` golden replay (`test_golden_replay_first_row_count_total`).
- **What was NOT exercised**: rollup-endpoint smoke, operator-side panel verification — both deferred to #919.
- **Independent raw XML spot-check** (NOT true cross-source — mirrors §8 framing): one holding's `value_usd` from the live SEC EDGAR `www.sec.gov/Archives` raw NPORT-P primary doc, asserted equal to the parser's Decimal output. True cross-source verification against an independent source (Morningstar / SEC EDGAR Fund Browser) is N/A until #919's rollup endpoint lands.

## 10. Open questions

None. The spike doc resolved every open question. The spec is ready for Codex 1a review.

## 11. References

- Issue: `gh issue view 932`.
- Spike: `docs/_archive/2026-05-18-n-port-edgartools-feasibility.md`.
- Sibling PR #931: `gh pr view 931` (#925 13F-HR parser drop-in, merged 2026-05-05, commit `0428dbf`).
- Settled decisions cited: §"Fundamentals provider posture", §"Provider design rule", §"Filing event storage", §"Source priority for fund metadata (#1171)".
- Prevention log cited: rate-limit pool sharing (510-513), raw-payload persistence (#1168), Pydantic validation cliff (`feedback_pydantic_validation_cliff.md`), EdgarTools internal-path stability (`.claude/skills/data-sources/edgartools.md` §G13).
