# EdgarTools — library reference

> Read this before adding any new SEC parser. EdgarTools (`edgartools` on PyPI, `dgunning/edgartools` on GitHub) is the most active SEC-data library in the Python ecosystem (~2-3 patch releases / week). eBull pins **`edgartools==5.30.2`** (exact pin, no ceiling) at [pyproject.toml:21](../../../pyproject.toml#L21). Internal module paths are not part of the library's public stability contract — pin tight, keep golden-file replays, audit on every minor bump.

## Coverage matrix

Static parsers (no network, no Pydantic strict) — the **safe drop-in path**:

| Form | Entry point | Returns |
|---|---|---|
| 13F-HR primary doc | `edgar.thirteenf.parsers.primary_xml.parse_primary_document_xml(xml)` | `PrimaryDocument13F` dataclass |
| 13F-HR INFOTABLE | `edgar.thirteenf.parsers.infotable_xml.parse_infotable_xml(xml)` | `pd.DataFrame` (cols: `Issuer, Class, Cusip, Value, PutCall, InvestmentDiscretion, OtherManager, SharesPrnAmount, Type, SoleVoting, SharedVoting, NonVoting, Ticker`) |
| Form 3 / 4 / 5 | `Ownership.parse_xml(xml)` (BeautifulSoup) | dict → `Form3/Form4/Form5(**dict)` |
| Schedule 13D / 13G (post-2024-12-19) | `Schedule13D.parse_xml(xml)` / `Schedule13G.parse_xml(xml)` | dict → `Schedule13D(**dict)` |
| Form 144 | `Form144.from_filing(filing)` | `Form144` |
| Form D, EFFECT, MA-I, ATS-N | `FormD.from_xml(xml)` / `XmlFiling.from_filing(...)` | structured object |

Network-required (live filing fetch + identity required):

| Form | Entry point | Notes |
|---|---|---|
| 10-K / 10-Q / 8-K / 6-K / 20-F / 40-F | `Company("AAPL").latest_tenk` or `filing.obj()` | XBRL-backed reports |
| N-PORT (P / EX / /A) | `FundReport.parse_fund_xml(xml)` | **Pydantic strict — see Gotcha G3** |
| N-CSR | `FundShareholderReport.from_filing(filing)` | iXBRL OEF taxonomy. **No CUSIP at holding level** (#918 closeout). |
| N-CEN | `FundCensus.from_filing(filing)` | structured census |
| N-MFP2/3 | `MoneyMarketFund.from_filing(filing)` | money-market holdings |
| N-PX | `NPX.from_filing(filing)` | proxy voting record |
| DEF 14A family | `ProxyStatement.from_filing(filing)`; `Company.proxy_season(0)` | HTML extraction; quality variable |
| S-1 / F-1 / S-3 / 424B / 497K / CORRESP | `RegistrationS1.from_filing(filing)` etc. | added in 5.23-5.27 |
| Filings index walk | `get_filings(year, quarter, form=..., index=...)` | calendar year, NOT fiscal |
| Submissions API per CIK | `get_entity_submissions(cik)` → `EntityData` | wraps `data.sec.gov` |
| companyfacts | `Company("AAPL").facts` → `EntityFacts` | `time_series, get_concept, latest_periods, shares_outstanding, public_float` |
| XBRL statements | `XBRL.from_filing(filing)` / `XBRL.from_directory(path)` | full statement engine |
| Multi-filing stitching | `XBRLS.from_filings(c.latest("10-K", 5))` | 5y income statement stitched |

**Not covered**: pre-2024-12-19 HTML 13D/G; non-CMBS Form 10-D; MSRB / FINRA filings; real-time push feeds.

## API cheat-sheet

```python
# --- one-time setup -------------------------------------------------------
from edgar import set_identity
set_identity("Your Name your.email@example.com")  # required by SEC fair-use

# --- universal find ------------------------------------------------------
from edgar import find
find("AAPL")                  # -> Entity
find(320193)                  # -> Entity by CIK int
find("0000320193-26-000042")  # -> Filing by accession
find("S000012345")            # -> FundSeries

# --- companies / entities ------------------------------------------------
from edgar import Company
c = Company("AAPL")
c.cik; c.tickers; c.sic; c.is_foreign; c.fiscal_year_end
c.latest_tenk; c.latest_tenq                  # one round-trip each
c.get_filings(form="4", trigger_full_load=False).latest(10)
c.facts                                        # EntityFacts (cached)
c.shares_outstanding; c.public_float           # via facts

# --- 13F-HR (the static, network-free path eBull uses) -------------------
from edgar.thirteenf.parsers.primary_xml   import parse_primary_document_xml
from edgar.thirteenf.parsers.infotable_xml import parse_infotable_xml
primary  = parse_primary_document_xml(open("primary_doc.xml").read())
holdings = parse_infotable_xml(open("infotable.xml").read())  # pd.DataFrame

# --- Form 3/4/5 ---------------------------------------------------------
from edgar.ownership import Ownership, Form4
form4 = Form4(**Ownership.parse_xml(xml_string))
form4.issuer.cik; form4.reporting_owners
form4.non_derivative_table.transactions

# --- N-PORT (validation cliff!) ------------------------------------------
from edgar.funds.reports import FundReport
report_dict = FundReport.parse_fund_xml(xml_string)  # dict (safe)
fr = FundReport(**report_dict)                       # Pydantic — may raise

# --- companyfacts / XBRL -------------------------------------------------
facts = c.facts
facts.get_concept("us-gaap:Revenues")
facts.time_series("us-gaap:Revenues", periods=20)
from edgar.xbrl import XBRLS
XBRLS.from_filings(c.latest("10-K", 5)).statements.income_statement()

# --- bulk download for offline / containers -----------------------------
from edgar import download_edgar_data, use_local_storage
use_local_storage("/data/edgar")
download_edgar_data(submissions=True, facts=True, reference=True)  # ~7GB

# --- HTTP configuration --------------------------------------------------
from edgar import configure_http
configure_http(use_system_certs=True)            # corp networks
configure_http(timeout=30.0, proxy="http://...")
```

## Cache directories

| Root | Default | Env var | Created when |
|---|---|---|---|
| Edgar data dir | `~/.edgar/` | `EDGAR_LOCAL_DATA_DIR` | **mkdir at module import** (`edgar/httpclient.py:103-108, 323`) |
| Edgar cache dir | `~/.edgar_cache/` | `EDGAR_CACHE_DIR` | lazily on first search query |

`~/.edgar/_tcache` is hishel-File HTTP cache, no automatic TTL. Manual invalidation: `edgar.storage.clear_cache()`.

`download_edgar_data()` pulls submissions + facts + reference (~7 GB total). Does NOT pull SEC Data Sets archives or per-filing XBRL — for those use `download_filings('YYYY-MM-DD')` per day.

## Identity / authentication

- `set_identity("Name email@domain.com")` writes `EDGAR_IDENTITY` env var. Resets httpx client.
- `get_identity()` **prompts interactively** if missing — in non-TTY contexts blocks 60s then raises `TimeoutError`. **Always set `EDGAR_IDENTITY` before any edgar import in batch / cron.**
- Static parsers (`thirteenf.parsers.*`) never call `get_identity()` — fixture-safe.
- eBull does NOT call `set_identity()` — only static parsers used today. Our HTTP fetch goes through `app/providers/implementations/sec_edgar.py` with `settings.sec_user_agent` directly.

## Gotchas

### G1 — Filesystem mkdir at module import
`HOME=/nonexistent python -c "import edgar"` → `PermissionError`. Workaround: lazy-import inside a factory. Pattern at [app/providers/implementations/sec_13f.py:69-80](../../../app/providers/implementations/sec_13f.py#L69-L80):

```python
def _edgar_parsers() -> tuple[Any, Any]:
    from edgar.thirteenf.parsers.infotable_xml import parse_infotable_xml
    from edgar.thirteenf.parsers.primary_xml import parse_primary_document_xml
    return parse_primary_document_xml, parse_infotable_xml
```

### G2 — Interactive identity prompt in batch contexts
`unset EDGAR_IDENTITY; python -c "from edgar import get_filings; get_filings(2026,1)"` blocks 60s. Always export `EDGAR_IDENTITY` before HTTP-touching call.

### G3 — N-PORT Pydantic validation cliff
`FundReport(**parse_fund_xml(xml))` raises `pydantic.ValidationError` on synthetic fixtures missing required fields (`<regCik>`, `<regStreet1>`). Required fields are non-`Optional`. Punted #932.

Workaround options:
- **A** — use full real-world XML in fixtures (25KB+ each).
- **B** — use the dict path: `report_dict = FundReport.parse_fund_xml(xml)` then walk `report_dict["investments"]` directly. Skip the model layer.
- **C** — direct lxml parsing (`~150 LoC`, 10-20× faster, no validation cliff). Recommended for #932 if revisited.

**Rule of thumb**: if the form's module imports `from pydantic import BaseModel`, expect a validation cliff for synthetic fixtures. Spike fixture compatibility BEFORE committing to a drop-in.

### G4 — Type-label rewrite (`SH` → `Shares`, `PRN` → `Principal`)
`parse_infotable_xml` output's `Type` column has values `"Shares"` / `"Principal"`, NOT the SEC two-letter codes. eBull maps back at [app/providers/implementations/sec_13f.py:210-213](../../../app/providers/implementations/sec_13f.py#L210-L213) (`_TYPE_CODE_FROM_LABEL`).

### G5–G6 — Empty `<value>` and empty `<cusip>` rows tolerated
Empty value/sshPrnamt rows land as `0`/`0` (not dropped). Empty CUSIP row lands as `Cusip=""`. Filter both at the boundary ([app/providers/implementations/sec_13f.py:316-328](../../../app/providers/implementations/sec_13f.py#L316-L328)).

### G7 — `Decimal(float)` vulnerability
EdgarTools constructs `total_value = Decimal(child_text(summary_page_el, "tableValueTotal"))` at `edgar/thirteenf/parsers/primary_xml.py:80` — the input is already a string (XML text from `child_text`). A future regression to `Decimal(<float-or-non-str>)` would silently introduce IEEE-754 rounding. Defense-in-depth: re-wrap with `Decimal(str(...))` at the boundary. See [app/providers/implementations/sec_13f.py:251-263](../../../app/providers/implementations/sec_13f.py#L251-L263).

### G8 — Signature date timezone naivety
`parsed.signature.date` is a string `"MM-DD-YYYY"`. Attach UTC explicitly:
```python
return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
```

### G9 — Calendar-year vs fiscal-year in `get_filings(year, quarter)`
Returns 10-Ks **filed** in that calendar year, NOT covering that fiscal year. Use `period_of_report` for fiscal queries.

### G10 — `get_filings()` triggers full submissions load
Default pages every historical filing. For Berkshire / BlackRock that's dozens of round-trips. Use `trigger_full_load=False` to limit to first ~1000.

### G11 — Pre-2024-12-19 13D/G is HTML, not XML
`Schedule13D.from_filing(old_filing)` returns `None` because `parse_xml` requires `<edgarSubmission>` root which only exists in post-rule structured XML. Filter ingest by filing date `>= 2024-12-19`.

### G12 — N-CSR has no per-holding identifier of any kind
OEF iXBRL N-CSR taxonomy publishes fund-level + class-level + sector-axis facts ONLY. There is no holding-level CUSIP / ISIN / SEDOL / ticker / portfolio-issuer CIK in the iXBRL. The N-CSR primary HTML's Schedule of Investments lists positions as `Name`-`Shares`-`Value` columns with no machine-readable identifier. The N-CSR itself directs readers to N-PORT for structured per-issuer holdings. Don't use N-CSR for security-level rollups under any path (iXBRL, HTML, exhibit). Verified by raw-payload spike `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md` (4 iXBRL companions + 52 MB primary HTML across 3 sampled OEF families). #918 closed (synth no-op landed); the gap is a real taxonomy limitation, not an EdgarTools surface limitation.

### G13 — Internal module path is the only stability contract
13F static parsers live under `edgar.thirteenf.parsers` — internal-looking. Library may reorganise. Mitigation: pin tight, keep golden-replay tests, audit on every minor bump.

### G14 — Process-local rate limiter
Default `requests_per_second=9` (`edgar/httpclient.py:142`). Spawning N processes each calling `get_filings(...)` collectively hits SEC at N×9 r/s. SEC threshold is ~10 r/s per IP. Lower `EDGAR_RATE_LIMIT_PER_SEC=9/N` per process or centralise SEC fetches.

## Decision tree — when to use edgartools vs roll-our-own

```
Need to parse SEC data?
├─ Existing eBull parser/provider for it?  → use that, do not casually add edgartools
│
├─ Static parser exists in edgartools (no Pydantic strict)?
│   ├─ 13F-HR primary/infotable          → edgartools
│   ├─ Form 3/4/5                        → edgartools (BeautifulSoup, fixture-safe)
│   ├─ Schedule 13D/G post-2024-12-19    → edgartools
│   ├─ Form 144                          → edgartools
│   ├─ N-PORT                            → roll our own (Pydantic cliff)
│   └─ Anything else                     → investigate first
│
├─ Need live HTTP fetch + auth?
│   ├─ We have a wrapper (sec_edgar.py)  → use ours
│   └─ We don't                          → edgartools (lazy-import + set_identity at startup)
│
├─ Need complex domain object (TenK / XBRL.statements)?
│   └─ → edgartools (no realistic roll-our-own)
│
└─ Need bulk download / first-install drain?
    └─ download_edgar_data() covers submissions + companyfacts. NOT data-sets.
       For wider bulk: datamule. For narrow: write a date-range walker.
```

**Hard-stop criteria for adopting edgartools in a new path:**
1. Confirm offline behaviour (set `EDGAR_LOCAL_DATA_DIR` first if sandboxed).
2. Grep the form's module for `BaseModel` — if present, write fixture-compat spike before committing.
3. Confirm internal module paths exist at the pinned version.
4. Pin tight: `edgartools==X.Y.Z, <X.(Y+1).0`.
5. `set_identity()` at process startup if any HTTP path will fire.

**Roll-our-own when:**
- Endpoint is well-known and stable (`data.sec.gov/submissions/...`, `companyfacts/...`, archive `index.json`). 20-50 LoC.
- Need control over UA / retry / cache TTL / psycopg-friendly types.
- Pydantic validation cliff (N-PORT).
- Throughput must be coordinated across processes (eBull's shared throttle is at [app/providers/concurrent_fetch.py:74](../../../app/providers/concurrent_fetch.py#L74)).

## Comparison

| Need | edgartools | datamule | secedgar | direct httpx |
|---|---|---|---|---|
| 13F INFOTABLE → typed | **best** | partial (download only) | no | ~50 LoC |
| Form 3/4/5 transactions | **best** (full insider model) | partial | no | hard |
| 13D/13G structured XML | **best** (post-2024-12-19) | partial | no | possible |
| N-PORT XML | yes (Pydantic strict) | no | no | ~150 LoC viable |
| 10-K/10-Q XBRL | **best** (full statements engine) | no | no | painful |
| companyfacts JSON | wraps cleanly | no | no | ~20 LoC |
| Bulk archive download | yes (`download_edgar_data()`) | yes (faster on AWS path) | no | manual |
| Rate-limit aware HTTP | yes | yes | partial | DIY |
| Maintenance signal | very active | active | last release 2025-05 | n/a |

## Existing eBull usage map

Confirmed via `grep -rn "from edgar\b\|import edgar\b" app/ tests/`:

| Where | What |
|---|---|
| [app/providers/implementations/sec_13f.py:77-80](../../../app/providers/implementations/sec_13f.py#L77-L80) | lazy `_edgar_parsers()` factory imports both static parsers |
| [app/providers/implementations/sec_13f.py:237](../../../app/providers/implementations/sec_13f.py#L237) | `parse_primary_doc()` calls `edgar_parse_primary(xml)` |
| [app/providers/implementations/sec_13f.py:308-309](../../../app/providers/implementations/sec_13f.py#L308-L309) | `parse_infotable()` calls `edgar_parse_infotable(xml)` |

**No other code in eBull imports `edgar`.** All other SEC paths (`sec_edgar.py`, `sec_submissions.py`, `sec_daily_index.py`, `sec_13dg.py`, `sec_fundamentals.py`, `sec_getcurrent.py`) are direct httpx/lxml.

## Upgrade procedure

1. Read CHANGELOG between current pin and target.
2. Re-run `tests/test_sec_13f_*` and any other golden-replay tests.
3. Confirm `edgar.thirteenf.parsers.{primary,infotable}_xml.parse_*` exist at same paths with same signature.
4. Confirm DataFrame columns unchanged (`Issuer, Class, Cusip, Value, ...`).
5. Confirm `PrimaryDocument13F.summary_page.total_value` is still `Decimal | int` (not `float`).
6. Run `pre-push` gate (lint/typecheck/test/format).
7. Bump pin in `pyproject.toml` + `uv.lock` (exact pin, e.g. `edgartools==X.Y.Z`).

**Do not auto-upgrade from Renovate/Dependabot.** Pin tight, upgrade manually after smoke run.

## References

- PyPI: <https://pypi.org/project/edgartools/>
- GitHub: <https://github.com/dgunning/edgartools>
- Source files (installed venv): `.venv/lib/python*/site-packages/edgar/` (relative to repo root)
- `pyproject.toml:21` — pin
- Specs: `docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md` (edgartools dependency boundary)
- Memory: `feedback_pydantic_validation_cliff.md`
