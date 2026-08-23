# R6 point-in-time spine failure-test declaration (#2900)

Status: **FROZEN BEFORE LEAK TEST**. This is a
data-admissibility declaration, not a strategy arm. It creates no return,
haircut, benchmark or investability evidence.

## Question and pass bar

Can any field needed by the ordered R6 arms be ranked for a historical New
York decision session using only the state public at that decision, with the
same rank surviving a later ingest?

A field is admissible only if all four conditions hold:

1. **Public clock:** the stored source publication instant is known. Period end,
   settlement date and price-bar date are not substitutes for publication.
2. **System-version clock:** every correction, reparse and identifier change is
   retained append-only. An in-place `UPSERT` cannot reconstruct its predecessor.
3. **Point-in-time population:** membership and identity use no current
   tradability, eventual survival, later termination or timeless symbol list.
4. **Causal transform:** every input used to classify the final pre-decision
   observation is itself public before the decision. A next-bar quarantine is
   non-causal at the boundary.

Unknown fields and fields failing any condition are refused before SQL is
issued. A hash of today's mutable output is not a missing system-version clock:
it can detect drift, but it cannot reconstruct the state that produced the old
decision.

`D` is the opening auction of an actual New York exchange session. All
date-resolution source values must have `source_public_date < D`; same-date
values are excluded because the repository does not uniformly retain intraday
publication time. Weekend and holiday dates are refused. Research-price reads
also refuse `D > 2024-09-27`, the pinned archive capture date.

## Source rules

- SEC Companyfacts `filed` governs public availability; `start`/`end` describe
  the fact's period. See `.claude/skills/data-sources/sec-edgar.md` §§1, 2 and
  5 and the SEC's [EDGAR API contract](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).
- Form 13F reports quarter-end holdings later. Public availability is the
  filing date, not period end. Amendments, confidential-treatment releases,
  PUT/CALL rows and PRN units prevent a raw sum from being an ownership state.
  See the same skill §§2.1 and 7.1–7.2 and the
  [SEC Form 13F FAQ](https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/frequently-asked-questions-about-form-13f).
- Forms 3, 4 and 5 are initial ownership, changes and deferred/annual reporting,
  not interchangeable snapshots. Filing time governs knowability. See the
  skill §2.3 and the [SEC forms index](https://www.sec.gov/submit-filings/forms-index).
- Schedule 13D/G beneficial, voting and dispositive amounts overlap under Rule
  13d-3 and group reporting under Rule 13d-5; they are not additive. Machine
  coverage has a 2024-12-18 structured-XML floor. See the skill §§2.4 and 2.6.
- DEF 14A Item 403's stated `as of` date describes the ownership snapshot; the
  filing date says when it became public. The current observation writer sets
  `filed_at` to current wall-clock time for every row, whether or not the parsed
  `as_of_date` exists; only `period_end` branches on that date. The SEC public
  clock is therefore absent from every current DEF 14A observation row.
- FINRA short interest describes a designated settlement date and is provided
  for publication on the seventh business day after it. The current table
  stamps settlement-date midnight into `filed_at` and overwrites revisions.
  See [FINRA's data description](https://www.finra.org/finra-data/browse-catalog/equity-short-interest)
  and [reporting schedule](https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest).

## Frozen field inventory

The implementation registry must contain every input family named by the
current ordered arms and downstream gates. The allowed states are `eligible`
and `refused`; no implicit/default admission exists. This issue matrix is the
completeness authority:

| Issue | Required registry families |
|---|---|
| #2908 dilution/red flags | `fundamental_facts`, `derived_fundamentals`, `dimensional_xbrl`, `filing_red_flags`, `historical_population` |
| #2913 short interest | `finra_short_interest`, `fundamental_facts` (float), `historical_population` |
| #2901 quality | `fundamental_facts`, `derived_fundamentals`, `historical_population` |
| #2917 shareholder yield | `fundamental_facts`, `derived_fundamentals`, `historical_population` |
| #2916 momentum | `research_prices`, `historical_population` |
| #2910 low beta/volatility | `research_prices`, `historical_population` |
| #2902 valuation | `fundamental_facts`, `derived_fundamentals`, `dimensional_xbrl`, `live_etoro_state`, `historical_population` |
| #2903 ownership | `ownership_observations`, `historical_population` |
| #2904 combination / #2911 ML | union of every surviving upstream family; with none admissible, both are blocked |

The verifier derives the verdict from this non-vacuous probe matrix. `FAIL[x]`
means probe `x` proves that condition absent; `PASS[x]` means it is structurally
present but does not rescue another failed condition. Every probe must match
exactly one runtime/schema anchor unless its definition explicitly names more.

| Registry family | Public clock | System versions | PIT population/identity | Causal transform |
|---|---|---|---|---|
| `research_prices` | `FAIL[P1]` | `FAIL[P1]` | `FAIL[P2,P5]` | `FAIL[P3,P4]` |
| `fundamental_facts` | `PASS[F0]` | `FAIL[F1,F2]` | `FAIL[P2,P5]` | `PASS[F0]` |
| `derived_fundamentals` | `FAIL[D1]` | `FAIL[D1]` | `FAIL[P2,P5]` | `FAIL[D1]` |
| `dimensional_xbrl` | `FAIL[X1]` | `FAIL[X1]` | `FAIL[P2,P5]` | `FAIL[X1]` |
| `ownership_observations` | `FAIL[O2]` | `FAIL[O1,O2,O3]` | `FAIL[H2,H3]` | `FAIL[O3]` |
| `filing_red_flags` | `PASS[R0]` | `FAIL[R1]` | `FAIL[H2,H3]` | `FAIL[R1]` |
| `finra_short_interest` | `FAIL[N1]` | `FAIL[N1]` | `FAIL[N2,H2,H3]` | `PASS[N0]` |
| `live_etoro_state` | `FAIL[L1]` | `FAIL[L1]` | `FAIL[L1,H1]` | `FAIL[L1]` |
| `historical_population` | `FAIL[H1]` | `PASS[H2]` only prospectively | `FAIL[H1,H2,H3]` | `PASS[H2]` only prospectively |

| Intended input family | Frozen verdict | Governing reason |
|---|---|---|
| Research price OHLCV, adjusted close, momentum, beta, volatility and price-derived size proxies | **REFUSED** | The selection admits linked names using today's eToro `is_tradable`/type/exchange, admits unlinked names according to eventual termination at the 2024 capture, and applies a 2026 exchange-test symbol list timelessly. Stored B4 quarantine uses the next bar. Historical prices are retrospectively split-adjusted, changing the historical `$1` eligibility gate after a future split. No vendor publication/version history exists. |
| Direct `price_daily` or current eToro data | **REFUSED** | Current identity, tradability and quote state are execution facts, not a historical population. |
| Revenue, cost, profit, income, balance-sheet, cash-flow, capex, dividends, buybacks, debt and share-count concepts in `financial_facts_raw` | **REFUSED** | Same-accession values are updated in place and the daily retention sweep deletes older 10-K/Q accessions as new filings arrive. `filed_date < D` cannot reproduce the former rows. Only tracked concepts are retained. |
| `fundamentals_snapshot`, `financial_periods_raw`, `financial_periods`, `financial_periods_ttm`, current dilution and market-cap summaries | **REFUSED** | Snapshot `as_of_date` is statement period end, not publication; periods-raw is rebuilt from the mutable retained facts; canonical/current rows select today's normalization/restatement winner. |
| Per-share-class, segment or other dimensional XBRL facts | **REFUSED** | Companyfacts JSON as stored lacks the dimension/member needed to identify the security-class fact. |
| Institutional, insider, blockholder, DEF 14A, treasury, fund and ESOP observations or changes | **REFUSED** | Writers use `ON CONFLICT DO UPDATE`, advance only the latest `ingested_at`, and discard the prior payload. Rewash can rename, supersede, revive or change historical rows. Current identifier resolution and overlapping legal measures cannot reconstruct a historical owner state. |
| Filing red-flag score | **REFUSED** | Filing rows and scorer classifications are mutable/current-version inputs; the accession-deduped historical event state is not frozen. |
| FINRA short interest, ADV and days-to-cover | **REFUSED** | Stored `filed_at` is settlement date rather than publication; schedule history is absent; revisions overwrite values and publication time; symbol resolution is current. |
| Current eToro tradability, type, venue, ISA eligibility and symbol mapping | **REFUSED for historical ranks** | Live execution/mandate gates do not establish historical membership. They still apply before any live order. |
| `instrument_universe_membership` and dated symbol/identifier histories | **REFUSED for the frozen historical archive; prospectively usable only within observed coverage** | Membership is append-only and structurally PIT from its first observation, but migration 271 explicitly forbids a backfill: imported starts are unknown and coverage begins after the research-price archive ends. Dated symbol/identifier records do not supply the missing pre-observation membership population. |

These refusals govern historical R6 evidence under the current table contracts.
Reopening a field requires an append-only source-versioned store, overlapping
historical identity and population, and the causal source-specific state
builder. Forward-only membership may become usable prospectively once a full
window has accumulated; it cannot repair the frozen archive. Adding a lag
constant, copying today's rows, or pinning an output hash is insufficient.

## Fail-closed read-path contract

`app.services.research_point_in_time` will expose the registry and one query
boundary, `execute_r6_ranking`. It accepts a non-empty typed `R6RankingRequest`
containing the ranking identity, explicit NYSE `decision_session`, and the
inseparable registry-family set declared by that ranking identity. Callers
cannot supply a callback or override/under-declare the family set. The service
validates the session with `market_calendar.us_market_status`, refuses price
requests after the archive capture, and dispatches only to an internal reader
registered beside the identity. Empty, unknown and refused requests raise
`PointInTimeUnavailableError` before a connection or SQL is available. There
are no internal readers under this declaration, so every request refuses.

The registry is a closed mapping from the issue-level input families to their
status, source clock, system-version status, population status, causal status
and reason. Tests pin its exact key set. An unknown key raises separately.

## Frozen adversarial test

Decision date: `2020-01-15`. Exact counts and values remain unknown until this
declaration is frozen.

The dev-only verifier runs in rollback-only transactions and records:

1. Select the lowest existing `instrument_id`, then seed a deterministic 13F
   equity specimen through `record_institution_observation` with a unique
   declaration-fixed filer/document identity and `filed_at < D`. This makes the
   test independent of retained production depth. The compared tuple is exactly
   `(instrument_id, filer_cik, filer_name, filer_type, ownership_nature, source,
   source_document_id, source_accession, source_field, source_url, filed_at,
   period_start, period_end, known_from, known_to, ingest_run_id, shares,
   market_value_usd, voting_authority, exposure_kind, ingested_at)`, ordered by
   `(instrument_id, filer_cik, ownership_nature, period_end,
   source_document_id, exposure_kind)` using bytewise Python tuple order after
   retrieval. Datetimes are UTC ISO-8601, decimals are fixed-point strings and
   null is JSON null; canonical JSON uses sorted keys and compact separators.
2. A new observation with `filed_at > D`. The sequence must remain unchanged;
   this is the easy public-clock control.
3. Call the same production writer on the specimen's pre-D natural key,
   changing shares without changing `filed_at`. The sequence must change and a
   full-tuple comparator must raise. Querying `ingested_at <=` the original
   vintage must not recover the overwritten predecessor. This is the decisive
   system-version failure.
4. Executable probes, with source SHA-256s reported, bind these IDs:
   `F0` Companyfacts `filed_date`; `F1` facts conflict update; `F2` 10-K/Q
   delete sweep; `D1` destructive periods rebuild plus latest-winner derivation;
   `X1` raw schema/parser absence of dimension/member; `O1` every ownership
   record writer's conflict update; `O2` DEF 14A's unconditional wall clock;
   `O3` retention/supersession and current identifier resolution; `R0` filing
   event filed clock; `R1` mutable score/lookup without historical scorer
   version; `N0` settlement-date presence; `N1` settlement-derived `filed_at`
   plus conflict update; `N2` current symbol resolution; `L1` current-only eToro
   state; `H1` current validated-universe query plus eventual-termination
   admission; `H2` forward membership's no-backfill/imported-start contract and
   measured coverage non-overlap; `H3` symbol/external-identifier historical
   coverage; `P1` research bars lack observation/publication versions; `P2`
   current/eventual selection; `P3` B4 successor read; `P4` retrospective
   split-adjustment semantics; `P5` timeless 2026 exchange-test list.
5. `execute_r6_ranking` refuses every ranking identity plus unknown/empty
   requests; an AST guard fails if any `app/services/r6_*.py` ranking module
   reads a governed table outside `research_point_in_time.py`.
6. The sentinel identity is fixed as filer CIK `0000002900`, document/accession
   `R6-2900-PIT-SENTINEL`, nature `economic`, exposure `EQUITY`, period end
   `2019-12-31`. The verifier acquires transaction advisory lock
   `hashtextextended('r6-2900-pit-verifier', 0)`, selects the lowest existing
   instrument only after the lock, and refuses if the full natural key exists.
   Every comparison is sentinel-key scoped. One explicit outer transaction is
   unconditionally rolled back in `finally`; a newly opened connection proves
   the sentinel is absent. Any pre-state collision or missing instrument is a
   refusal, not cleanup.

The test **passes** only if the guard fails closed and all contamination is
detected. #2900's evidence verdict is nevertheless **FAIL / NO ADMISSIBLE
HISTORICAL FIELD** if no family satisfies the four-part bar. Detection is not
reconstruction, and a passing refusal test must not be reported as a usable PIT
spine.

## Reporting boundary

The result states declaration SHA-256 and commit, command, decision date,
complete registry, measured source census, before/after hashes, first unequal
tuple, source-code contract hashes, and rollback proof. The frozen census is:
row and distinct-instrument counts plus min/max public/period/system dates for
`financial_facts_raw`, every ownership observation table, FINRA short interest,
research price series/bars/quarantine coverage, universe membership, symbol
history and external identifiers; zero rows print explicit zero/null strata.
It must say explicitly
that Tier 2 is blocked rather than manufacture an arm result. No performance,
haircut, cost or benchmark output may be inferred from this diagnostic.
