# #3740 slice F — the forward capture for step 3's paper accrual

Status: **draft v3; Codex checkpoint 1 open (rounds 1–2 ran on v1 and v2).** v3 moves market data to Massive (formerly
Polygon.io) and applies round 2's protocol findings. Nothing has been built. v3's Massive premises are marked
**[to measure]** and need the operator's `MASSIVE_API_KEY`; round 3 runs after they are measured. v2, the text round 2
reviewed, is at commit `88c7248f`; raw findings are in `docs/research/3740-slice-f-ckpt1-findings.md`.

Parent: `docs/research/2026-10-10-3740-step3-vw-book.md` ("the parent"), §"Forward accrual", whose §"Sealing
invariants" (1–8) and §"Slice F's obligations" (1–12) this spec must meet. The parent's rules stand unless a row of
§"Declared exceptions" (X) or §"Amendments to the parent" (A) replaces one.

## What slice F is for

The step-3 book is confirmed on 24 forward months (parent premise 3). Every input to a forward formation or holding
month must be captured as it becomes available, bound so it cannot be replaced, and witnessed outside our control.
Slice F fixes, per input: the source, the rule that turns it into the parent's input, when it is acquired, how it is
bound and witnessed, how each failure ends, and what the dry run must show before the declaration. It computes nothing
about performance: no portfolio is formed, weighted or valued on any captured month before the 24th holding month's
inputs are bound (invariant 7); instrument-level validation is allowed (§"Dry run").

**Design in one paragraph.** Prices and corporate actions come from Massive: each session's unadjusted closes for all
US stocks in one call, bound after that session's close and before the next open; dated split events with ratios;
cash dividends with ex-dates and amounts; and a daily point-in-time ticker reference keyed by composite FIGI, with
type and CIK. Daily total returns, MAX and holding-month returns are computed by us from those documented inputs.
eToro supplies the population's listing and tradability only. SEC supplies accounting, shares, accession headers
(SIC) and Form 25 terminations. N-PORT, Ken French and JKP files are fetched by the capture itself. Every acquisition
is written once and receipted; receipts are hash-chained, logged in Sigstore's Rekor v2 transparency log and
time-stamped by an RFC 3161 authority.

## Why the route changed after round 2

Round 2 (53 findings, 46 BLOCKING) rejected the v2 market-data sources at their root:
- eToro's official close has no documented price basis, and per-observation checks are diagnostics, not the
  nominal-price contract obligation 6 requires (round 2, 11, 18, 19, 27, 28);
- eToro serves no dated corporate actions, so a measured vendor factor can neither date nor classify an action
  (20, 21, 29–31);
- PWB's monthly republish has no evidence cutoff or coverage contract (33–40).

These are properties of the sources, so v3 replaces them rather than adding rules. Massive's free "Stocks Basic" plan
documents what was missing (`massive.com/pricing` and the endpoint pages below, read 2026-10-10). The plan is limited
to 5 calls per minute, 2 years of history and end-of-day recency. **Decision (supervisor, under the 2026-10-08
delegation):** Massive is the market-data source; the eToro close, candle and rate kinds, PWB and FINRA are removed.

## Premises

Labels: **[doc]** cites documentation, **[code]** cites code, **[probe]** is printed by
`PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_sources`, **[to measure]** needs the key and is measured by
the plumbing test (§"Dry run") before round 3.

1. **[doc] Grouped daily.** `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` returns OHLC, volume and VWAP for
   "all U.S. stocks" on a date in one request. `adjusted=false` returns results "NOT adjusted for splits"; the
   response echoes `adjusted`. Each row has `T` (symbol), `c` (close) and `t` ("the end of the aggregate window").
   `include_otc` defaults to false. Basic plan: end-of-day recency, 2 years of history
   (`docs/rest/stocks/aggregates/daily-market-summary.md`).
2. **[doc] Splits.** `GET /stocks/v1/splits` returns `ticker`, `execution_date`, `split_from` (old shares),
   `split_to` (new shares), `adjustment_type` (`forward_split`, `reverse_split`, `stock_dividend`) and `id`. The
   execution date is documented exactly: "On the prior trading day, the post-market session is the last session that
   shows pre-split prices. On the execution date, all trading is already adjusted for the split." Filters on
   `execution_date` ranges; up to 5,000 rows per page with `next_url`. Updated daily; 2 years on Basic
   (`docs/rest/stocks/corporate-actions/splits.md`).
3. **[doc] Dividends.** `GET /stocks/v1/dividends` returns `ticker`, `ex_dividend_date`, `cash_amount` ("original
   dividend amount per share"), `currency`, `declaration_date`, `record_date`, `pay_date`, `distribution_type`
   (recurring, special, supplemental, irregular, unknown) and `id`, filterable on `ex_dividend_date` ranges, 5,000 per
   page. Updated daily; 2 years on Basic (`docs/rest/stocks/corporate-actions/dividends.md`). The deprecated
   `/v3/reference/dividends` and `/v3/reference/splits` are not used.
4. **[doc] Ticker reference.** `GET /v3/reference/tickers?date=&market=stocks&active=` lists tickers "available on
   that date", 1,000 per page, each with `ticker`, `type`, `cik`, `composite_figi`, `share_class_figi`,
   `primary_exchange`, `active`, `delisted_utc` and `last_updated_utc`
   (`docs/rest/stocks/tickers/all-tickers.md`). `GET /v3/reference/tickers/{ticker}?date=` adds `list_date` ("the date
   that the symbol was first publicly listed") and share counts (`ticker-overview.md`). `GET /v3/reference/tickers/types`
   lists the type codes and descriptions (`ticker-types.md`). `GET /vX/reference/tickers/{id}/events` returns
   `ticker_change` events for a ticker, CUSIP or composite FIGI (`ticker-events.md`).
5. **[to measure] Massive premises v3 depends on.** Each is a plumbing-test item with a pass rule (§"Dry run",
   plumbing test):
   - (a) the time after a session's close at which `adjusted=false` grouped daily for that session first returns a
     non-empty result on the Basic plan; the design needs it before 08:00 New York on the next session's date;
   - (b) SPY and IVV appear in grouped daily under the stocks market;
   - (c) the share of eToro type-5 US instruments whose symbol maps to a grouped-daily row on a recent session;
   - (d) a ticker delisted inside the last 2 years appears in grouped daily for sessions before its delisting
     (grouped daily is by date, so it should; the probe checks 20 names from `active=false`);
   - (e) the splits and dividends endpoints' behaviour on `execution_date`/`ex_dividend_date` filters, whether
     announced future events are listed before their date, and the share of rows with non-USD `currency`;
   - (f) the class-share symbol format in `T` (for example `BRK.B`) and the type codes returned by the types endpoint;
   - (g) the calls one daily cycle needs (§"Runtime"), against the 5-per-minute limit.
6. **[probe][code] SEC bulk files are overwritten daily** with no history (`app/services/sec_bulk_refresh.py:47-63`).
   v3 does not read the daemon's copies (§F4).
7. **[doc] EDGAR.** Company records (`submissions`) carry `sic`; filing lists carry `form`, `acceptanceDateTime` (UTC)
   and `accessionNumber` (`sec-edgar.md:59, 74, 1072-1074`). The daily form index lists each business day's accepted
   filings. An accession's header carries `STANDARD INDUSTRIAL CLASSIFICATION` (parent obligation 3).
8. **[code] Connections:** 27 usable, demand 24 plus reserve 3 (`app/db/pg_settings.py:127-230`).
9. **[code] B1 and factors:** `sec_nport_monthly_returns` has no scheduled writer; `french_reference_refresh` appends a
   snapshot per content change (`app/services/reference_data.py:1051-1176`). Neither is read by slice F.
10. **[doc] Rekor v2** (Sigstore's transparency log, GA 2025; `blog.sigstore.dev/rekor-v2-ga/`): a tile-backed log in
    the C2SP tlog-tiles layout, with one write endpoint, inclusion proofs and signed checkpoints. It has no search
    index and returns no integrated time; time evidence comes from an RFC 3161 timestamp authority. Shards are yearly
    and frozen when rotated, their URLs distributed through Sigstore's TUF `SigningConfig`. Monitoring reads tiles
    (`rekor-monitor`).
11. **[doc] JKP cutoffs.** `return_cutoffs.csv` was last modified 2026-04-16 with its last row 2025-12-31 (HTTP
    `Last-Modified` and the file's tail, 2026-10-10). Publication is in arrears by months (§F14).

## Runtime

- **One process outside the jobs daemon.** A launchd agent `com.ebull.forward-3740` runs
  `scripts/forward_3740_capture.py tick` every 10 minutes from a dedicated worktree at the declaration's git tag. The
  daemon runs `~/Dev/eBull` at `origin/main`, which changes with every merge, and a daemon lane would break the
  connection budget.
- **Tick preflight, in order.** Any failure is an integrity failure: retryable, no acquisition, recorded.
  1. The worktree HEAD equals the pinned tag and is clean.
  2. `uv.lock`'s sha256 equals the pinned one.
  3. A read-only checkout of `origin/main` (the ledger checkout) is fetched; its commit is recorded. It must contain
     the declaration file with the pinned sha256, and the trial's ledger file must hold no terminal entry for the
     trial. A terminal entry stops all acquisition.
  4. The `information_schema` hash over §"Schema interface" equals the pinned one (`SCHEMA_DRIFT` otherwise).
  5. Free disk under the capture root exceeds the next 30 days of the §"Dry run" disk projection.
  Every receipt records the code tag commit, the construction hash, the lock hash, the schema hash and the ledger
  commit.
- **Work queue.** A tick takes a `flock`, reads the due work in §"Windows" order, and runs each item with a request
  timeout of 120 s and a per-tick wall budget of 8 minutes. Unfinished items stay queued for the next tick, so long
  work cannot hold the lock past the next tick.
- **Database:** at most one connection, opened per step, from the 3-connection reserve. A refused connection is a
  retryable failure inside the window.
- **Massive:** one key used only by this process, so the 5-per-minute limit is not shared; the limiter allows 4 per
  minute. The daily cycle is about 1 grouped call, 12–15 reference pages, 2–4 split and dividend pages, and one
  ticker-overview call per newly seen composite FIGI; premise 5(g) measures it.
- **eToro:** two listing requests per session, under the shared 120/60 s quota. A refusal retries inside the window.
- **SEC:** at most 2 requests per second with the SEC User-Agent, a partition of the shared 10/s as
  `scripts/build_2282_form25_register.py` partitions it.

## Acquisition, binding and witness

### Attempt state machine

Every acquisition is an attempt with a fresh `attempt_id`. States and crash outcomes:

| state | entered when | a crash in this state means |
|---|---|---|
| `intent` | a `forward_capture_attempts` row (attempt id, kind, source id, period key, request) and the `record_holdout_access` row (`strategy_id="3740-step3-book"`, or `"3740-slice-f-dryrun"` in the dry run, `access_kind="read"`, `result_version=attempt_id`) commit in one transaction, before the first request (invariant 8) | `abandoned`: another attempt is allowed in the window |
| `persisting` | the response streams to `<attempt dir>/<component>.part` | `abandoned`; the `.part` file is kept as provenance and never parsed |
| `persisted` | each component's file is fsynced and renamed to its final name, and the attempt row is marked `persisted` with each file's sha256 | recovery: the next tick validates the persisted files; it never refetches |
| `receipted` | the per-kind validator ran on the persisted bytes and one `forward_capture_receipts` row commits with every component's sha256 and validation class | recovery: the next tick publishes the receipt |
| `witnessed` | the receipt's Rekor entry and RFC 3161 token are stored (§"Witness") | — |

- **Nothing parses unpersisted bytes.** Parsing reads final files only, so a crash before `persisted` cannot have
  shown the process any content, and the retry is not a choice between observations.
- **Validation classes**, per component, from a frozen validator per kind: `valid`; `transport_defect` (non-200,
  truncated, unparseable or empty body, or a response whose echo fields contradict the request, for example
  `adjusted` true or a date other than requested); `row_defect` (a parseable response with individual rows failing a
  rule; the rows are listed in the receipt).
- **First observation binds.** For each acquisition identity, the first persisted component classed `valid` or
  `row_defect` binds. Only `transport_defect` and `abandoned` permit another attempt in the window. A bound component
  is never refetched, and publication failure never permits reacquisition (§"Outcomes").
- **Abandoned attempts** are counted per kind; the dry run gates the count (§"Dry run").

### Binding registry

Two kinds of identity, each with its own table and unique index:
- **Acquisition identity** (invariant 3) = `(trial, kind, source_id, period_key, component)`, one row per component
  in `forward_capture_components`, with a partial unique index over rows classed `valid` or `row_defect`.
- **Selection identity** = `(trial, selection_kind, selection_key)`, one row in `forward_capture_selections` naming
  the bound components it reads by sha256 and the rule version. A selection is written once, when its rule's decision
  time passes, and never replaced.

| acquisition kind | source_id | period_key | component |
|---|---|---|---|
| `massive_grouped` | `massive:/v2/aggs/grouped/locale/us/market/stocks?adjusted=false` | session date | the response |
| `massive_tickers` | `massive:/v3/reference/tickers?market=stocks&active={true,false}` | session date | each page, in `next_url` order, plus the page count |
| `massive_overview` | `massive:/v3/reference/tickers/{ticker}` | composite FIGI | the response at its first observation |
| `massive_splits` | `massive:/stocks/v1/splits` | session date | each page of `execution_date` in [d − 30 days, d + 30 days] |
| `massive_dividends` | `massive:/stocks/v1/dividends` | session date | each page of `ex_dividend_date` in [d − 30 days, d + 30 days] |
| `massive_types` | `massive:/v3/reference/tickers/types` | session date | the response |
| `massive_events` | `massive:/vX/reference/tickers/{figi}/events` | (composite FIGI, session date) | the response |
| `etoro_listing` | `etoro:/market-data/instruments`, `/instrument-types`, `/exchanges` | session date | one per endpoint |
| `sec_daily_index` | `sec:/Archives/edgar/daily-index/form.{date}.idx` | index date | the file |
| `sec_header` | `sec:/Archives/edgar/data/{cik}/{accession}.hdr.sgml` | accession | the file |
| `sec_form25` | `sec:/Archives/edgar/data/{cik}/{accession}.txt` | accession | the complete submission |
| `sec_bulk` | `sec:companyfacts.zip`, `sec:submissions.zip` | session date | the zip's member for each superset CIK (every submissions page) |
| `sec_ticker_map` | `sec:company_tickers_exchange.json` | session date | the file |
| `nport_inventory`, `french_poll`, `jkp_poll` | the source's listing page or file | poll date | the response |
| `nport_dataset` | `sec:nport-data-sets/{quarter}` | (quarter, sha256) | the zip |
| `french_file` | `french:<dataset>` | sha256 | the file |
| `jkp_file` | `jkp:return_cutoffs.csv` | sha256 | the file |

| selection kind | key | decided when | rule |
|---|---|---|---|
| `b1_entry_close` | M_0 | end of s(M_0)'s `massive_grouped` window | §F11 |
| `b1_exit_close` | M_0 + 24 | end of s(M_0 + 24)'s window | §F11 |
| `b1_month` | holding month | first N-PORT dataset version containing the month is accepted | §F11 |
| `factor` | dataset | first accepted French file covering all 24 months | §F12 |
| `cutoff` | holding month | the month's returns cutoff | §F14 |
| `formation_inputs` | formation M | the end of s(M)'s last window | §"Formation resolution" |
| `month_status` | holding month | the month's returns cutoff | §F10 |
| `month_returns` | (holding month, security) | the month's returns cutoff, or later completion inside the nine-month limit | §F9 |
| `month_final` | holding month | every `month_returns` selection for the month exists | §F9 |

### Witness (invariant 5)

- **Chain.** Each receipt carries `prev_receipt_sha256`, the previous receipt's digest, from a genesis receipt pinned
  in the declaration. The chain links receipts only, so a publication failure never stalls it.
- **Log entry.** Each receipt's sha256 is signed with an Ed25519 key generated for the trial (a separate key for the
  dry run), whose public key is pinned in the declaration, and submitted to the current Rekor v2 shard named by
  Sigstore's TUF `SigningConfig`. Stored with the receipt: the signature, the returned entry and inclusion proof, the
  signed checkpoint, and the shard's public key from TUF.
- **Time.** The receipt digest is time-stamped by Sigstore's RFC 3161 authority (`timestamp.sigstore.dev`); the token
  and the authority's certificate chain are stored. The token's time is the witnessed time.
- **Pinned trust roots:** the TUF root used, each shard key and the timestamp authority's chain. Key rotation follows
  TUF; a key not reachable from the pinned root is untrusted.
- **Completeness.** Rekor v2 has no search index, so the verdict scans every tile of every shard from the genesis
  entry's index to the final checkpoint and lists every entry whose signature verifies under the trial key. The tiles
  are immutable and archived after a shard freezes. The listed set must equal the chain's logged receipts one to one.
  The scan's cost is measured in the dry run (§"Dry run" item 7); the verdict does not run until the scan completes
  over the whole range.
- **Verdict checks, in order:**
  1. **Chain integrity:** the hash chain is unbroken, every logged entry matches exactly one receipt, and no
     acquisition or selection identity has two bindings. Failure → `CAPTURE_AMBIGUOUS`.
  2. **Proof validity:** each inclusion proof verifies against a checkpoint signed by a trusted shard key, and the
     checkpoints are consistent (consistency proofs between successive stored checkpoints). Failure of one entry →
     that receipt is unwitnessed.
  3. **Admissibility:** a receipt is admissible when its entry verifies and its RFC 3161 time is at or before its
     witness deadline. An inadmissible receipt binds nothing for its input; its effect is scoped by §"Outcomes".
- **Limit.** The witness shows tampering with logged receipts; it does not detect a fetch made outside the protocol.

### Windows

All times America/New_York; sessions and early closes from `app/services/market_calendar.py` at its pinned
`RULE_SET_VERSION`. Every window has an absolute start and end; the witness deadline is the window end plus the lag.
An acquisition not persisted by the window end is final for that window (invariant 4).

| kind | window, per occurrence | witness lag | retry |
|---|---|---|---|
| `etoro_listing` | session d: 13:30 to close − 30 min (early close: 11:00 to close − 30 min) | 15 min | every tick |
| `massive_tickers`, `massive_types`, `massive_events`, `massive_overview` (new FIGIs) | session d: 13:30 to close − 20 min, after that day's `etoro_listing` | 15 min | every tick |
| `sec_ticker_map`, `sec_bulk`, `sec_header` (due accessions) | session d: 14:00 to close − 20 min; `sec_bulk` on s(M) and the two sessions before it only | 15 min | every tick |
| `massive_grouped`, `massive_splits`, `massive_dividends` | session d: close + 30 min to 08:00 on the next session's date | 60 min | every tick |
| `sec_daily_index` | for index date x: 06:00 on the calendar day after x to 06:00 seven calendar days later | 24 h | every tick |
| `sec_form25` | for each Form 25 or 25-NSE in a bound daily index for a superset CIK: from the index's binding to 7 calendar days later | 24 h | every tick |
| `nport_inventory`, `french_poll`, `jkp_poll` | daily, 06:00 to 23:59, from the dry run's start to the nine-month limit | 24 h | every tick that day |
| `nport_dataset`, `french_file`, `jkp_file` | the day a bound poll first shows a version not yet bound, to 23:59 seven days later | 24 h | every tick |

- **Due SEC headers:** each 10-K- or 10-Q-family accession for a superset CIK in a bound daily index, fetched in the
  first SEC window after that index binds; at the dry run's start, the latest such accession for every superset CIK
  (the header bootstrap).
- **Massive returns-window recovery.** A session whose `massive_grouped` window ended with no binding may be acquired
  again under a separate kind, `massive_grouped_recovery`, from the session after it through the month's returns
  cutoff. A recovery binding is admissible for holding returns, statuses and MAX lookback only, never as a formation's
  decision price, and is counted.
- **Discovery.** Every scheduled poll is a receipted, witnessed acquisition, failures included, so the order in which
  versions were first seen is the poll chain's order and cannot move. A version that appears and is replaced between
  two polls is never seen; that is stated, not ambiguous.
- **The nine-month limit** is 23:59 on the last calendar day of the ninth month after the 24th holding month.
- **Returns cutoff** of holding month m: the close of the tenth session after m's last session (the parent's).

**Pre-close fallback.** Only `etoro_listing`, `massive_tickers` and `sec_bulk` may fall back: one missed on s(M) uses
the latest binding of that kind from the five sessions before s(M), recorded in the formation's selection with its
age. Price and corporate-action kinds never fall back. None in range → the formation is missed with that kind's code.

### Outcomes

One table from validator result to state and code. A row's effect applies only within its scope.

| event | scope | effect | code |
|---|---|---|---|
| `transport_defect` or `abandoned` attempt | identity | another attempt in the window | — |
| window ends with nothing persisted | identity | no binding from that window (final) | — |
| receipt not admissible (late, unverifiable or never published) | its input | binds nothing; chain continues | per the input's row below |
| decision-time input missing after fallback (`etoro_listing`, `massive_tickers`, `sec_bulk`, `massive_grouped` for s(M), `massive_splits` or `massive_dividends` for s(M)) | formation | `formation_missed` | `INPUT_MISSING:<kind>` |
| population refusals needing no holdings | formation | `formation_missed` | `SESSION_MISSING`, `ME_INVALID`, `UNIVERSE_SHORT`, `MAX_EMPTY` |
| `b1_entry_close` not selected at its decision time | trial | terminal `REFUSED`, written to the ledger at once; all trial captures stop | `B1_UNDEFINED` |
| a returns, status, B1, factor or cutoff selection not made by the nine-month limit | trial | terminal `REFUSED` | `INPUT_UNAVAILABLE` |
| chain integrity failure (verdict check 1) | trial | refuses until re-declaration | `CAPTURE_AMBIGUOUS` |
| preflight failure | tick | retryable integrity failure; never a verdict | `SCHEMA_DRIFT`, `PREFLIGHT:<step>` |

Precedence at the verdict: chain integrity first, then admissibility, then the parent's verdict order. An input that
is inadmissible is treated as unbound for its row; it is never both `INPUT_UNAVAILABLE` and `CAPTURE_AMBIGUOUS`.

### Acquisition superset

Books are not formed before the verdict, so the capture covers a superset of every possible holding, reference
constituent and control holding:
- **Securities,** keyed by composite FIGI: every FIGI that, in any bound `massive_tickers` since the dry run's start,
  has a ticker matching (§F13) an instrument in any bound `etoro_listing` with `instrumentTypeID` 5 on exchanges 4, 5,
  20 or 33; plus SPY and IVV. Once in, never out. A FIGI enters at the first binding that shows it, and its due SEC
  work is scheduled in the same session's SEC window, which follows the listing and ticker windows.
- **Market-wide kinds** (`massive_grouped`, splits, dividends, tickers) are not filtered by the superset, so every
  security is covered from the dry run's start whether or not it is in the superset yet.
- **CIKs:** every CIK linked (§F13) to a superset FIGI on any bound session.

### Lookback (amendment A2)

A formation reads sessions before s(M_0): MAX's boundary month, the share-basis sessions of F5, and split and dividend
history. **Admissible lookback objects:** dry-run bindings of `massive_grouped`, `massive_grouped_recovery`,
`massive_tickers`, `massive_splits`, `massive_dividends`, `massive_overview`, `etoro_listing`, `sec_bulk`,
`sec_header` and `sec_daily_index`, plus one **bootstrap** acquisition at the dry run's start of the splits and
dividends with dates in the 24 months before it (Massive's Basic history). Conditions:
- the trial tag's validators re-run on the dry-run bytes; a component whose class changes is not admissible;
- coverage is continuous: every session from the dry run's start to s(M_0) has an admissible `massive_grouped` or
  recovery binding, else the declaration waits;
- the declaration pins the dry-run chain head and the bootstrap receipts.

This replaces v1's rule that dry-run captures are never trial inputs; the parent states no rule on it.

## Input rules

### F1. Security type (obligation 1)

- **Fields, structured only:** eToro `instrumentTypeID` (bound listing); Massive `type` for the FIGI on s(M) (bound
  `massive_tickers`); the CIK's SIC (F3).
- **Rule, in order:**
  1. `instrumentTypeID` 5, else `type_not_stock`; eToro exchange 19 (OTC) → `otc`.
  2. Massive `type` in the accepted set, else `type_rejected:<code>`. The accepted set is frozen from the first bound
     `massive_types`: the codes whose description is common stock or ordinary shares. ADR codes are rejected (JKP keeps
     CRSP share codes 10, 11 and 12; ADRs are 31). A `type` missing → `type_unknown`.
  3. SIC 6221, 6722 or 6726 → `type_fund`.
- **Primary listing and session:** Massive's grouped daily is the consolidated US close for the symbol; one FIGI is
  one security, so two eToro instruments mapping to one FIGI are one security (`duplicate_instrument` refuses both
  unless exactly one is on eToro exchange 4 or 5, which is kept). Every pair is listed in the dry run.
- **Validation (dry run, pass/fail), every superset security,** accepted rows included: Massive's class beside the
  cover-page `dei:Security12bTitle` keyword class of the CIK's latest 10-K or 10-Q (read from EDGAR at validation
  time; validation only, never an input). Every disagreement and every security with no cover title is adjudicated by
  hand against the filing in a committed file before the declaration. Pass: no adjudicated row where Massive's class
  is accepted and the security is not common equity.

### F2. Listing age (obligation 2; exception X8)

- **Source:** Massive `list_date` from `massive_overview` at the FIGI's first observation; first observation binds.
- **Rule:** eligible when `list_date` ≤ s(M) − 36 months. Missing → `listing_age_unknown`, not eligible to enter.
- **Exception (X8):** the parent names the Form 8-A 12(b) registration as candidate. Rounds 1–2 showed acceptance of
  a certification or 8-A is not the documented effectiveness rule (Form 8-A General Instruction A(c)) and does not
  show continuity. Massive's field is a documented first-listing date of the symbol. Ticker changes are the risk:
  every FIGI with a `ticker_change` event in its bound `massive_events` is listed in the dry run.
- **Validation (dry run, pass/fail):** for every superset security, `list_date` beside the EDGAR evidence (earliest
  matching CERT* acceptance, the 8-A12B's effectiveness under Instruction A(c) where determinable) and, for names in
  step 1's panel, the first Intrader bar. Every case where the sources disagree on eligibility at any dry-run
  formation is adjudicated, in both directions. Pass: no adjudicated case where `list_date` admits a security listed
  less than 36 months before that formation. False exclusions are counted, not gated.

### F3. SIC (obligation 3)

- **Source:** the `sec_header` of the CIK's latest 10-K- or 10-Q-family accession accepted before s(M), bound before
  s(M)'s close (the header bootstrap or the daily header capture).
- **Rule:** the header's `STANDARD INDUSTRIAL CLASSIFICATION` code; no code → `sic_null`; no bound header for that
  accession → `sic_unloaded`. Used for the REIT exclusion (6798), F1's fund rule and the FF-12 map.
- **This reproduces obligation 3 exactly** and needs no exception: the header bytes read are those available before
  s(M)'s close. A post-acceptance correction made before the capture is included, which is information available
  before the decision.
- **Reported (not gated):** at each dry-run formation, the header SIC beside the company-record SIC from the same
  session's `sec_bulk`, with REIT, fund and FF-12 effects of each disagreement.

### F4. Accounting (obligation 4)

- **Source:** `sec_bulk`: the capture downloads `companyfacts.zip` and `submissions.zip` from SEC itself in its window,
  records each response's `Last-Modified`, `ETag` and sha256, and binds the members for every superset CIK. The zips
  are not retained; the members are the bound bytes. The daemon's copies are not read.
- **Completeness:** every submissions page named in a CIK's main JSON (`filings.files`) must be present as a member,
  else `transport_defect`.
- **Rule:** step 1's `pit_fundamentals` bundle built from those members by step 1's code with only its input reader
  changed: evidence cutoff (acceptance New York date strictly before s(M)) and the four-month lag.
- **Freshness report:** each formation lists the 10-K- and 10-Q-family accessions for superset CIKs in the bound daily
  indexes accepted before s(M) that the bound members lack.

### F5. Shares and ME (obligation 5)

Step 1 §"Market equity" exactly on the F4 bundle: cover-count precedence, the 15-month age limit, blocked reads,
context-date and acceptance-date bases, no share lag. **The split product over (basis, s(M)]** uses Massive split
events: shares × Π (`split_to` / `split_from`) over events with `execution_date` in (b, s(M)], with b the basis date
and events selected from the latest `massive_splits` binding at or before s(M)'s window (bootstrap included).
Execution-date semantics (premise 2) put the first post-split session on `execution_date`, which is step 1's stamp
convention. ME = adjusted shares × P(s(M)), P the F6 close.
- **Fixtures** (slice 3): forward split, reverse split and stock dividend; an event on the basis date and one on s(M);
  context-date and acceptance-date bases; two events in the interval.
- **Population checks (dry run, pass/fail):** at each formation, every security whose ME changes by more than ×1.5
  or less than ×(2/3) from the previous formation while its split-adjusted price changes by less than ×1.2, listed
  and adjudicated. Pass: no adjudicated case caused by a missing or misdated split.

### F6. Raw close at a session d (obligation 6)

- **Source:** `massive_grouped` for d (`adjusted=false`), bound in d's post-close window; the row whose `T` is the
  FIGI's ticker on d in the bound `massive_tickers`.
- **Rule:** valid when the response is `valid`, the row exists, `c` > 0 and finite, and `t` falls on d in New York
  time. One row per ticker; a duplicate `T` → `row_defect` for that ticker.
- **This reproduces obligation 6** from an authoritative, documented unadjusted source with a dated observation; no
  exception is needed. The step-2 close was Intrader's raw trade close and this is Massive's consolidated daily close;
  both are trade closes, and the dry run reports their relation to eToro's official close (parity, not gated).
- **Dry run (pass/fail):** per ordinary session, the share of superset securities active in the bound tickers with a
  valid close; pass ≥ 99%.

### F7. Split events (obligation 7)

- **Source:** Massive split events (premise 2), bootstrap plus daily bindings; ticker mapped to FIGI on the day before
  `execution_date` (the pre-event ticker), and refused (`split_unmapped`) when the ticker maps to no FIGI that day.
- **Selection:** an event binds from the first binding that shows it (by `id`); a later change to an event with the
  same `id`, or an event first shown after its execution date, is recorded and reported, never applied to a past
  formation.
- **Independent enumeration (dry run, pass/fail):** two sources detect actions without reading Massive's events:
  1. sessions where the unadjusted close ratio P_d / P_{d−1} lies within 15% of a ratio n/m with n, m ≤ 20 and
     n/m ∉ [0.8, 1.25], and no Massive event exists (candidate missed events);
  2. eToro's back-adjusted candle history (`price_daily`), compared between two dry-run month-ends: a session whose
     stored close changes by a ratio outside [0.98, 1.02] marks an eToro-applied action.
  Every candidate from either source without a Massive event, and every Massive event that neither source shows, is
  adjudicated against the issuer's filing. Pass: no adjudicated missed or misdated split.

### F8. MAX inputs (obligation 8; exception X3)

- **Daily total return** for a security on adjacent sessions d − 1, d with valid F6 closes:
  gross g_d = (k_d · P_d + D_d) / P_{d−1}, net r_d = g_d − 1, where k_d = Π (`split_to` / `split_from`) over its
  events with `execution_date` = d (1 if none), and D_d = Σ `cash_amount` over its USD dividends with
  `ex_dividend_date` = d. `cash_amount` is per share on the basis before any split that day; a split and a dividend
  on the same day, or a non-USD dividend, makes r_d invalid (`action_ambiguous`, `dividend_currency`). Dividends are
  gross, pre-withholding (parent obligation 9).
- **Rule:** #3621's MAX at s(M): the largest r_d over the sessions of s(M)'s calendar month up to s(M), with the
  previous month's last session as the boundary bar; ≥ 15 returns; < 10 zero returns; returns only between bars on
  adjacent sessions; the return screen (−0.9, 3.0); screened names flagged before the missing-value rules.
- **Every input is bound before its cutoff:** each prior session's close and actions bind in that session's own
  post-close window, so before s(M)'s close; s(M)'s own close and actions bind in s(M)'s post-close window (parent
  invariant 1's decision-session package).
- **Exception (X3): the ratio screen.** #3621 screens an `adj_close / close` move beyond ×1.5 between consecutive
  usable bars, including across gaps, because the vendor's adjustment was unstamped. Here actions are dated events,
  so the screen becomes: a window is screened when it contains a session d, compared with the previous usable bar
  even across missing sessions, where an event exists and |ln(k_d · P_d / P_prev)| > ln 1.5 (the price did not move
  as the event says), or where r_d is `action_ambiguous` or `dividend_currency`.
- **Measurement before planning (parent slice 2):** on stage A and stage B, #3621's MAX as frozen beside MAX with the
  replacement screen applied to Intrader's stamped events (the combined treatment), per formation: the cutoff, the
  flagged-set size and the symmetric difference. The vendor change (Intrader closes to Massive closes) cannot be
  measured historically, because Massive's 2 years do not overlap the panel; it is stated as unmeasured.

### F9. Holding-month returns (obligation 9)

- **Total return** of an `observed` holding for month m: G = Π g_d over the sessions of m after s(m − 1) through
  s(m) (F8's formula, adjacent sessions only); R = G − 1. Gross relative and net return are both stored.
- **Inputs:** closes from `massive_grouped` (or `massive_grouped_recovery`) bindings; split and dividend events from
  the latest `massive_splits` and `massive_dividends` bindings at or before the month's returns cutoff. Using the
  cutoff's view picks up a dividend recorded late; it never reads evidence after the cutoff.
- **Validity:** every session in the month has a valid F6 close and a valid r_d. Otherwise the holding has no valid
  total return: an incomplete input under the parent (completed only by a recovery binding inside the returns window),
  never a coverage exit.
- **Selection:** at the returns cutoff, `month_returns` binds each holding's (status, G, inputs); `month_final` binds
  when the month's set is complete.
- **Independent reference (dry run, pass/fail), frozen before comparing,** stratified:
  - **daily returns:** for every superset security and dry-run session, r_d beside the return from eToro's stored
    `price_daily` closes (split back-adjusted, no dividends) on days with no dividend; pass: median absolute
    difference under 0.1 pp and no more than 0.5% of security-days beyond 2 pp, with every one beyond 10 pp
    adjudicated;
  - **splits:** F7's enumeration;
  - **dividends:** for every security with a Massive dividend in the dry run, the amount and ex-date beside the
    issuer's 8-K or press release for a random sample of 100 frozen by seed `3740-sliceF-div`, plus every special
    dividend; pass: no adjudicated wrong amount or ex-date beyond 1 cent or one session;
  - **terminations:** F10's table.

### F10. Statuses and terminations (obligation 10)

- **States per security per month, in order:**
  1. **Capture state:** a session with no admissible `massive_grouped` or recovery binding is `capture_missing` for
     every security. That is an incomplete input, never a status.
  2. **Source coverage:** a security is `source_ceased` from the first session after its last valid close when its
     FIGI is inactive in the bound `massive_tickers` (or shows `delisted_utc` on or before that session) by the
     returns cutoff. A security still active with no row on a session has an **interior gap** on that session.
  3. **Status,** step 1's rule: `terminal` when `source_ceased` and a matched Form 25 exists; `coverage_exit` when
     `source_ceased` with no matched Form 25, or on an interior gap; else `observed`. `end_bar` is the last valid close
     before the first ceased or gap session.
- **Termination evidence:** `TerminationEvidence` (`app/services/series_termination.py:150-166`) built per security:
  `linked` when a `sec_form25` accession accepted by the returns cutoff matches the security under
  `classify_form25_match` (`app/services/research_corpus_ingest.py:1566`) and the #2282 register's rules (dual CIK
  indexing, debt-lifecycle exclusion, the class named in the filing); `provision` from the filing, the latest
  amendment by acceptance taking precedence; `q_suffix` by `archive_symbol_candidates`' rule
  (`research_corpus_ingest.py:340`) on the last ticker. `classify_termination` (`series_termination.py:168`) then
  gives the class, with step 1's terminal value fractions for both arms.
- **Partial-month return** for `terminal` and `coverage_exit`: Π g_d through `end_bar`, from the same inputs as F9.
- **Daily index completeness:** a business day whose `sec_daily_index` is not bound by its window end is retried as
  a new acquisition until the nine-month limit; until it binds, every security that ceased in the month has an
  incomplete status.

### F11. B1 (obligation 11)

- **Returns:** each `nport_dataset` version is parsed with `load_3619_nport_returns`'s parser from the pinned tree.
  A dataset version is **accepted** when it parses and its IVV (`C000012040`) rows pass step 2's identity gate.
  `b1_month` for month m selects IVV's Item B.5.a return from the filing with the earliest acceptance in the first
  accepted version (by poll order) containing m; two filings with that acceptance time, or two values for m in it,
  refuse the month (`B1_AMBIGUOUS`, `INPUT_UNAVAILABLE` at the nine-month limit). Later versions are provenance only.
- **Bands:** `b1_entry_close` is SPY's F6 close at s(M_0), selected at the end of s(M_0)'s `massive_grouped` window
  (08:00 on the next session's date). Its receipt's witness deadline is that window's end plus 60 minutes, and the
  B1 gate is evaluated then: no returns-kind selection for month M_0 + 1 is made before the gate passes.
  `b1_exit_close` is SPY's close at s(M_0 + 24), likewise.

### F12. Factors (obligation 12)

Each `french_file` version is parsed with the pinned `reference_data` parser; it is **accepted** when step 2's checks
(unit, completeness) pass. `factor` for each dataset (five-factor, momentum) selects the first accepted version (by
poll order) whose observations cover all 24 forward months. Earlier and rejected versions are provenance only.

### F13. CIK link and ticker mapping (population step 2; exception X7)

- **eToro to Massive:** an eToro instrument maps to the FIGI whose ticker on s(M) in the bound `massive_tickers`
  equals the eToro symbol, after the class-suffix table frozen from premise 5(f). No match → `unmapped`; two →
  `map_ambiguous`.
- **CIK link:** the FIGI's `cik` on s(M) in the bound `massive_tickers`. Missing → `cik_missing`.
- **Exception (X7):** step 1 linked on Form 3/4/5 `issuerTradingSymbol` evidence, which the quarterly data sets lag by
  a quarter. Massive's link is dated by its reference snapshot.
- **Validation (dry run, pass/fail):** at each formation, the Massive CIK beside SEC's `company_tickers_exchange.json`
  bound the same session and beside step 1's linkage on its latest data sets. Every disagreement is adjudicated.
  Pass: no adjudicated case where Massive's CIK is wrong.

### F14. Return cutoffs (step 1 Amendment 3; exception X9)

- **Selection:** at month m's returns cutoff, `cutoff` binds the row for m from the latest accepted `jkp_file` bound by
  then, if it has one; otherwise the row of the latest month in that version (carry-forward). The selection is never
  replaced by a later version. A version is accepted when step 1's per-row check passes for every row. No accepted
  version, or a non-finite or inverted row → `INPUT_UNAVAILABLE` at the nine-month limit.
- **Exception (X9):** step 1 clips with the holding month's own row. JKP publishes months in arrears (premise 11), so
  forward months are usually carried. Clipping bounds a vendor error; it also caps real gains and floors real losses.
- **Measurement before planning (parent slice 2):** on stage A and stage B, the book's and the control median's G per
  arm with each month clipped by its own row beside clipped by the row a reader would have had at that month's returns
  cutoff (the last row in the JKP version then current; versions reconstructed from `Last-Modified` history where
  available, else a fixed lag equal to premise 11's). Printed: counts of clips changed, both arms, and the five months
  with the largest change. The declaration records the figures.

## Formation resolution (amendment A1)

The parent resolves a formation once the previous month's returns are bound, from the book's holdings. Holdings are
not formed before the verdict (invariant 7), so:
- **During the accrual,** `formation_inputs` for M binds, at the end of s(M)'s last window, the instrument-level inputs
  for the whole superset (F1–F8, F13, eligibility, MAX, ME, raw close, codes); population refusals that need no
  holdings mark `formation_missed` at once.
- **At the verdict,** the report forms the paths and applies the parent's holding-dependent rule (`PRICE_INVALID` for
  a holding with no valid close that was not realised) from those bindings. A formation missed this way counts toward
  `ACCRUAL_GAPS` exactly as if found during the accrual.

## Splicing (invariant 6)

| element | identity mapping | adjustment basis | overlap reconciliation | vendor precedence |
|---|---|---|---|---|
| historical ↔ forward | none: the forward path starts all cash at s(M_0) | — | — | — |
| eToro ↔ Massive | symbol on s(M) via the frozen suffix table, to composite FIGI | — | F13 and F9's daily-return comparison with eToro candles | eToro for listing and tradability; Massive for prices and actions |
| Massive closes ↔ actions | ticker on the session (FIGI-keyed) | unadjusted closes; dated events | F7's enumeration | Massive only |
| Massive ↔ SEC | F13's CIK | — | F13 table | SEC for accounting, SIC, Form 25 |
| B1 | IVV class `C000012040` | NAV total return | step 2's identity gate | N-PORT only |

## Schema interface (pinned runtime)

Captured bytes live on disk. The capture and verdict processes read and write only:
- `forward_capture_attempts` (`attempt_id`, `trial`, `kind`, `source_id`, `period_key`, `request`, `state`,
  `created_at`, `updated_at`);
- `forward_capture_components` (`attempt_id`, `component`, `sha256`, `path`, `bytes`, `validation_class`,
  `defect_rows`);
- `forward_capture_receipts` (`receipt_sha256`, `attempt_id`, `prev_receipt_sha256`, `code_commit`,
  `construction_hash`, `lock_hash`, `schema_hash`, `ledger_commit`, `witness_deadline`, `rekor_entry`,
  `rekor_checkpoint`, `tsa_token`, `created_at`);
- `forward_capture_selections` (`trial`, `selection_kind`, `selection_key`, `component_sha256s`, `rule_version`,
  `value`, `decided_at`);
- `strategy_holdout_accesses` (`access_id`, `strategy_id`, `strategy_version`, `result_version`, `access_kind`,
  `accessed_by`, `purpose`, `accessed_at`).

The code holds this list in one constant, with a test that the hashed `information_schema` slice covers exactly these
columns. Dry-run reports that read other tables (`price_daily`, `instrument_universe_membership`) run as separate
scripts outside the pinned interface; their output is evidence, never an input.

## Declared exceptions

| id | parent rule | forward rule | why | evidence before declaration |
|---|---|---|---|---|
| X3 | MAX ratio screen on unstamped `adj_close / close` | event-consistency screen (§F8) | actions are dated events here | F8 stage A/B measurement |
| X5 | tradability from `instrument_universe_membership` | `isTradable` in the bound `etoro_listing` at s(M) | the membership table is daemon state, rewritten outside the pinned runtime | parity: at each dry-run formation, the two predicates side by side with every disagreement and its reason |
| X7 | Form 3/4/5 symbol linkage | Massive `cik` on s(M) | the insider data sets lag a quarter | F13 table |
| X8 | Form 8-A 12(b) registration (candidate) | Massive `list_date` | acceptance is not effectiveness or continuity | F2 table |
| X9 | the holding month's JKP cutoffs | latest row available at the returns cutoff | JKP publishes in arrears | F14 measurement |

X1, X2, X4 and X6 of v2 are withdrawn: F3, F6, F9 and F5/F7 now reproduce their obligations.

**Amendments to the parent:** A1 (formation resolution); A2 (lookback). v2's A3 (entry needs a PWB mapping) is
withdrawn with PWB.

## Dry run (prospective)

**Plumbing test first,** on the dry-run key, measures premise 5 (a)–(g) and validates code; the spec's pass rules for
it: (a) grouped daily for every session of two weeks is available before 08:00 next-session-date; (b) SPY and IVV
present on every session; (c) at least 95% of eToro type-5 US instruments mapped; (d) at least 18 of 20 delisted names
present before their delisting; (e) documented; (f) suffix table frozen; (g) the daily cycle fits the window at 4
calls per minute. A failed item is an amendment with its own checkpoint 1.

**Then at least three consecutive month-ends** from the dry-run tag, trial `3740-slice-f-dryrun`, own Rekor key.
Accepted only if every item passes:
1. **Captures:** every mandatory window acquired and witnessed on time. A miss fails this item whatever its cause;
   transport defects are listed. Abandoned attempts: none outside the drills.
2. **Pass/fail validations:** F1, F2, F5, F6, F7, F9 (every stratum) and F13, each against its stated rule.
3. **Reported tables:** F3, F4 freshness, X5 parity, the parent's parity table.
4. **Measurements before planning:** F8 and F14 on stage A and B, printed and recorded.
5. **Event coverage:** at least five split events, one reverse split, one special dividend and one Form 25
   termination in the superset during the dry run; if not, the dry run extends month by month until they have
   occurred. F7's and F9's adjudications include the non-event cases they list, so quiet sessions are tested too.
6. **Witness drills,** on the dry-run key, each with its expected outcome: publication failure past its deadline
   (inadmissible, chain continues); publication retried after a timeout, with the duplicate submission's behaviour
   recorded (one entry, or two entries for one receipt, both matched); a second receipt for one identity
   (`CAPTURE_AMBIGUOUS`); a receipt deleted locally after logging (unmatched entry, `CAPTURE_AMBIGUOUS`); a deleted
   chain tail (the tile scan lists entries with no receipt); a branch (two receipts with one predecessor); a local
   timestamp edited (the RFC 3161 token disagrees); a crash in each attempt state (the stated outcome).
7. **Completeness scan cost:** the full tile scan over the dry run's log range, with its time and bytes, and the
   projection to 24 months plus the nine-month limit. Pass: the projection completes within 7 days on this machine.
8. **Disk projection** for the whole retained inventory (daily captures, failed-attempt bytes, provenance versions,
   the dry run and the nine-month tail), with peak usage. Pass: within the capture root's free space with 2× margin.
9. **No portfolio** formed, weighted or valued: instrument-level checks only.

A failed item is revised by an amendment with its own checkpoint 1; a revision that changes a historically
reproducible input re-runs the parent's slice 3 (planning).

## Build slices

1. This spec and the probe (this PR).
2. Attempts, components, receipts, selections, the artefact store, the Rekor v2 and RFC 3161 witness and the
   verdict-side checker with the tile scan, with the drills on a scratch key. Codex checkpoint 2.
3. The capture kinds and the F-rules with fixture tests; the launchd agent. Codex checkpoint 2.
4. The F8 and F14 historical measurements (with the parent's slice 2).
5. Plumbing test, then the prospective dry run, posted on #3740.

## Known limits

- Massive is one vendor for prices and actions; F7 and F9 enumerate its errors against eToro and filings, but a
  Massive error that eToro shares is not caught.
- The vendor change in MAX's daily closes is unmeasured historically (X3).
- Forward clipping uses carried JKP rows (X9).
- A structurally invalid total return for an observed holding (`dividend_currency`, `action_ambiguous` on a held
  name) cannot be completed and runs to `INPUT_UNAVAILABLE` under the parent's rule; the dry run counts how often
  each code occurs in the superset.
- The witness shows tampering with logged receipts; it does not detect a fetch made outside the protocol.

## Round 2 dispositions (for round 3's task A)

| finding | v3 |
|---|---|
| 1 witness completeness | Rekor v2 full tile scan by trial key; verdict waits for it; cost gated (dry run 7) |
| 2 timestamp authentication | RFC 3161 token, signed checkpoints, consistency proofs, pinned TUF root |
| 3 publication failure | chain links receipts only; verdict order chain → proof → admissibility |
| 4 crashes | attempt state table; no parsing of unpersisted bytes; persisted bytes recovered, not refetched |
| 5 registry | acquisition and selection identities, two tables, every selection listed |
| 6 manifests | `month_status`, `month_returns`, `month_final` selections |
| 7 discovery | every poll receipted and witnessed; poll order fixes version order |
| 8 SEC kinds | `sec_daily_index`, `sec_header`, `sec_form25`, `sec_bulk`, `sec_ticker_map` registered and scheduled |
| 9 Form 25 cutoff | daily index per date, retried to the nine-month limit; acceptance by cutoff separated from acquisition |
| 10 rates and fallback | eToro rates removed; fallback restricted to three metadata kinds |
| 11, 18, 19, 27 F6 basis | Massive `adjusted=false` grouped daily, dated, bound before the next open |
| 12 MAX cutoff | each session's inputs bound in its own post-close window |
| 13 B1 deadline | B1 close valid at once; gate at window end + 60 min, before any month M_0 + 1 selection |
| 14 final month | daily grouped and actions through the last returns cutoff; no candles needed |
| 15 refusal taxonomy | §"Outcomes" table with scope and precedence |
| 16 read interface | five tables with columns; ledger checks in preflight step 3 |
| 17 superset | FIGI-keyed, sticky, market-wide kinds unfiltered; SEC window after listing window |
| 18 A2 | admissible objects listed, validator re-run, continuous coverage, bootstrap |
| 19, 20 F1 | every security validated against the cover title; FIGI is the security identity |
| 21 F13 | Massive dated reference snapshot; validated against SEC map and step 1 |
| 22, 23 F2 | Massive `list_date`; validation both directions at the 36-month boundary |
| 24 F3 | accession header bound before close: obligation 3 exactly; X1 withdrawn |
| 25 F4 | capture downloads the zips itself; page completeness; freshness report from daily indexes |
| 26 F5 | dated split events; fixtures and ME discontinuity census |
| 28–31 F7 | dated Massive events; independent enumeration from prices and eToro candles, both directions |
| 32, 33 X3 | replacement screen defined; combined measurement; vendor change stated unmeasured |
| 34–40 F9 | total return from documented closes and actions; formula; stratified reference with pass rules; cutoff view |
| 41, 42 F10 | capture state, source cessation and interior gap separated; ordered predicates; `end_bar` |
| 43 F10 classifier | full `TerminationEvidence` construction with Q-suffix and amendment precedence |
| 44 F11/F12 | acceptance before selection; poll order; versions kept as provenance |
| 45, 46 F14 | X9 kept with a historical measurement; one selection at the cutoff, never replaced |
| 47 dry run | any capture miss fails; drills separate; event coverage with non-event cases |
| 48 runtime | own Massive key; per-tick budget, timeouts, queue |
| 49 disk | whole-inventory projection with margin |
| 50 drills | enumerated with outcomes |
| 51 X5 | parity table with reasons; X5 in the exception table |
| 52 probe | v2 probe items kept; Massive items join after the key (premise 5) |
| 53 A2 attribution | corrected: the rule was v1 of this spec |

## Checkpoint log

**Round 1 (44 findings: 37 BLOCKING, 5 WARNING, 2 NIT).** Applied in v2 (commit `88c7248f`); round 2 judged 10
applied, 32 partial, 2 not applied.

**Round 2 (53 findings: 46 BLOCKING, 5 WARNING, 2 NIT).** Source-validity findings led to the Massive route;
dispositions above. Round 3 runs after premise 5 is measured.
