# #3740 slice F — the forward capture for step 3's paper accrual

Status: **v4 below is SUPERSEDED: it rests on Massive, which the 2026-10-10 no-account rule removes (#3740,
2026-10-10 16:21Z). §"Route after the no-account rule" records which account-free sources have been assessed for
parent obligations 6, 7 and 9, what is still unassessed, and the next measurement (revised after checkpoint 1 round
5).** v4 is kept unchanged below it as the record
round 4 reviewed. Nothing has been built. Earlier versions: v2 `88c7248f` (round 2), v3 `dbcde782` (round 3), v4
`59742928` (round 4). Raw findings: `docs/research/3740-slice-f-ckpt1-findings.md`.

## Route after the no-account rule (2026-10-10)

**Rule.** Data comes only from sources needing no sign-up, no terms accepted in the operator's name, no subscription
and no payment: eToro (our broker), SEC EDGAR, FINRA public files, Fed/FRED, Nasdaq Trader's public symbol files and
the public research datasets already pinned (#3740, 2026-10-10 16:21Z).

**Measured** (`PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_account_free`, run 2026-10-10 ~17:45Z on the
dev DB, with response hashes printed). These are presence counts over what the dev DB stores, over all stored
instruments with the named XBRL facts, **not** over the parent's eligible top-1,000 population; they measure our
current pipeline, not what EDGAR could supply (round 5, 1–4).

| need (parent obligation) | account-free source | measured | meets it? |
|---|---|---|---|
| raw close at a session (6) | eToro, captured by us in the session's own window | design only. The parent admits this candidate only with eToro documentation and a capture-time contract that an own-session bar is unadjusted; neither exists (round 5, 22) | **unresolved** |
| dated split events with ratios (7) | eToro's served history, refetched against our own nominal captures | in one current `price_daily` vintage, four published split boundaries are compatible with split adjustment (NVDA 2024-06-07 → 06-10 ratio 1.0073, AVGO 07-12 → 07-15 1.0070, CMG 06-25 → 06-26 1.0079, WMT 02-23 → 02-26 1.0168; illustrative nominal ratios ignoring price moves: 0.1, 0.1, 0.02, 0.333). That is compatible with adjustment; it does not measure rescale timing, and forward detection cannot cover the 15-month basis history before capture starts (round 5, 23–24) | **unresolved**: corroboration at most; an independent dated inventory is still needed |
| cash dividends at the ex-date (9) | 8-K Item 8.01 regex parser (`app/services/dividend_calendar.py`) | of 1,182 stored instruments with XBRL `dps_declared` > 0 in periods ending ≥ 2025-07-01, 443 have ≥ 1 parsed 8-K dividend row with record date and amount (date predicate `coalesce(ex, record, declaration)` ≥ 2025-10-01) and 124 have ≥ 3 rows. Over all stored `dividend_events` rows with that predicate (1,634, no payer join), 29 carry an ex-date and 1,371 a record date | **pipeline gap, not source absence**: the parser reads Item 8.01 primary documents only; announcements also sit in Item 7.01/2.02 exhibits (round 5, 6). Record dates map to ex-dates only under the applicable exchange/FINRA rule version, with exceptions (round 5, 7; FINRA Rule 11140) |
| | XBRL `CommonStockDividendsPerShareDeclared` (`financial_periods.dps_declared`) | of 1,642 stored instruments with XBRL `dividends_paid` > 0 in periods ending ≥ 2025-07-01, 1,095 carry `dps_declared` > 0 and 547 do not; the fact is per fiscal period, by declaration, undated | **no** as a dated ledger; usable as a candidate detector |
| | PWB `adj_close` (parent's named candidate) | README at Hub sha `c64377a3`: "gated for download. Approval is tied to a Papers With Backtest subscription" (the Hub reports `gated=False` today; unauthenticated download succeeds); "Refreshed monthly"; the 2026-10-01 version covers to 2026-08-05 | **excluded by the rule**: the provider ties download to a subscription. Its lag (~8 weeks) also misses the returns cutoff |
| | eToro public dividend calendar (`etoro.com/investing/dividend-calendar/`; missed in the first draft, round 5, 5) | one snapshot (2026-10-10): 333 rows, 262 distinct suffix-free symbols (not a US or common-stock classifier); ex-dates 2026-05-01..2026-10-09, payment dates 2026-10-12..2026-11-23, so in this snapshot every row had gone ex and awaited payment; publication and removal policy unknown. Columns: symbol, sector, ex-date, payment date, annual and periodic amount; no record date, no currency. Page text: "The information provided below is indicative and subject to change." A marketing page, fetchable without an account | **unassessed**: coverage, amount basis, revisions and identity untested |
| | FINRA Daily List | OTC corporate actions incl. dividend ex-dates (round 5, 8); not the exchange-listed population | **no** for this population |

Also looked at and not adopted: Nasdaq's website calendar endpoint (`api.nasdaq.com/api/calendar/dividends`) answers
without an account with ex, record and payment dates and amounts, but it is an undocumented website backend, not a
published data product, and its terms of use were not found. It is outside the rule's list.

**Finding, as narrowed by round 5.** No account-free source has yet been *demonstrated* to supply obligation 9's
dividends, obligation 7's dated split inventory, or obligation 6's capture contract. That is a statement about what
has been assessed, not an impossibility claim (round 5, 1, 21).

**Withdrawn: the price-return lower bound as the route.** The first draft proposed reading condition 4 on a book
return that omits uncertain dividends. Round 5 showed it does not hold as stated: it breaks the matched-control
comparison unless controls keep complete total returns (11), it does not order condition 3's loadings or power (12),
it is unproven on the parent's net wealth path (13), "certain receipt" is not an executable credit rule (14), MAX's
daily total-return input remains (17), and its historical measurement would print quantities the parent's planning
slice forbids (18). It would need a full parent amendment with its own checkpoint 1, so it is a last resort, not the
next step.

**Next step: a measurement spec, written and checkpointed before anything is captured or compared.** Round 6
(27 findings: 17 BLOCKING, 8 WARNING, 2 NIT) rejected the first sketch of it (EDGAR and the eToro calendar scored
against a frozen 12-month inventory with a 99% coverage stop rule); the sketch and its threshold are withdrawn.
Decided now, under the 2026-10-08 delegation:
- **No independent account-free event reference is held.** The research corpus's dividend-adjusted `adj_close` is
  an adjusted series, not an event inventory (round 6, 1), and its series come from `paperswithbacktest/Stocks-Daily-Price`
  (two vintages: 7,870 and 7,693 series) and `icyDenev/Intrader` (22,879 series) by `research_price_series.vendor`,
  measured 2026-10-10. Whether Intrader's stamps are a dated distribution inventory with amounts is the first thing
  the measurement spec checks; if not, the reference is EDGAR issuer announcements reconciled two ways with the
  candidate, with disagreements adjudicated against the issuer's filing (round 6, 2, 11).
- **Two separate tests** (round 6, 5, 9): a historical, oracle-guided EDGAR content-feasibility audit, labelled as
  such; and a prospective common comparison in which every candidate (an EDGAR discovery process run without
  reference answers, and a daily capture of the eToro calendar) is scored on one frozen population and window, at
  the deadline each dependent input actually has (round 6, 8).
- **Outcomes** (round 6, 15–17): pass authorises only a slice F v5 design with its own checkpoint 1; route-fail is
  recorded as "this frozen route did not demonstrate the required coverage", and whether to try another route or stop
  #3740 is then a supervisor resource decision, stated as such; inconclusive extends once, by a bound the spec fixes.
- The spec fixes the event contract, ex-date rule table by venue and version, two-way reconciliation, combination
  policy, error budget (in the book's G, not event counts) and capture protocol per round 6, 3–4, 6–7, 10–14 and
  18–19, and the probe's reproducibility per 22–24.

Obligations 6 and 7 still need their own evidence either way (round 5, 22–24).

## What slice F is for

The step-3 book is confirmed on 24 forward months (parent premise 3). Every input to a forward formation or holding
month must be captured as it becomes available, bound so it cannot be replaced, and witnessed outside our control.
Slice F fixes, per input: the source, the rule that turns it into the parent's input, when it is acquired, how it is
bound and witnessed, how each failure ends, and what the dry run must show before the declaration. It computes nothing
about performance: no portfolio is formed, weighted or valued on any captured month before the 24th holding month's
inputs are bound (invariant 7); instrument-level validation is allowed (§"Dry run").

**Design in one paragraph.** Prices and corporate actions come from Massive (formerly Polygon.io): each session's
unadjusted closes for all US stocks in one call, bound after that session's close and before the next open; dated
split events with ratios and cash dividends with ex-dates, kept as a cumulative revisioned inventory; and a daily
point-in-time ticker reference keyed by composite FIGI, with type and CIK. Daily total returns, MAX and holding-month
returns are computed by us from those documented inputs. eToro supplies the population's listing and tradability.
SEC supplies accounting, shares, accession headers (SIC) and Form 25 terminations. N-PORT, Ken French and JKP files
are fetched by the capture itself. Every state transition of every acquisition, and every selection, is a canonical
record in one hash chain; each record is logged in Sigstore's Rekor v2 and time-stamped by an RFC 3161 authority.

## Why the route changed after round 2

Round 2 rejected the v2 market-data sources at their root: eToro's official close has no documented price basis;
eToro serves no dated corporate actions; PWB has no evidence cutoff or coverage contract. Round 3 judged the move to
Massive to resolve those findings (round 3 task A, 11, 13, 14, 27–29, 35–38). **Decision (supervisor, under the
2026-10-08 delegation):** Massive's free "Stocks Basic" plan (5 calls per minute, 2 years of history, end-of-day
recency; `massive.com/pricing`) is the market-data source.

## Premises

Labels: **[doc]** cites documentation, **[code]** cites code, **[probe]** is printed by
`PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_sources`, **[to measure]** is gated by the plumbing test.

1. **[doc] Grouped daily.** `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` returns OHLC, volume and VWAP for
   "all U.S. stocks" on a date. `adjusted=false` returns results "NOT adjusted for splits"; the response echoes
   `adjusted`, `resultsCount` and `status`. Each row has `T` (symbol), `c` ("the close price for the symbol in the
   given time period") and `t` ("the end of the aggregate window"). `include_otc` defaults to false. Basic plan:
   end-of-day, 2 years (`massive.com/docs/rest/stocks/aggregates/daily-market-summary.md`). The per-ticker
   open/close endpoint returns `close` separately from `afterHours` and `preMarket`
   (`…/aggregates/daily-ticker-summary.md`); the grouped endpoint does not document its session.
2. **[doc] Splits.** `GET /stocks/v1/splits`: `id`, `ticker`, `execution_date`, `split_from` (old shares),
   `split_to` (new shares), `adjustment_type` (`forward_split`, `reverse_split`, `stock_dividend`). "On the prior
   trading day, the post-market session is the last session that shows pre-split prices. On the execution date, all
   trading is already adjusted for the split." Range filters on `execution_date`; 5,000 rows per page with `next_url`
   (`…/corporate-actions/splits.md`).
3. **[doc] Dividends.** `GET /stocks/v1/dividends`: `id`, `ticker`, `ex_dividend_date`, `cash_amount` ("original
   dividend amount per share in the specified currency"), `currency`, `declaration_date`, `record_date`, `pay_date`,
   `distribution_type`, `frequency`; range filters on `ex_dividend_date`; 5,000 per page
   (`…/corporate-actions/dividends.md`).
4. **[doc] Ticker reference.** `GET /v3/reference/tickers?date=&market=stocks&active=`: tickers "available on that
   date", 1,000 per page, with `ticker`, `type`, `cik`, `composite_figi`, `share_class_figi`, `primary_exchange`,
   `active`, `delisted_utc` ("the last date that the asset was traded") (`…/tickers/all-tickers.md`).
   `GET /v3/reference/tickers/{ticker}?date=` adds `list_date` ("the date that the symbol was first publicly listed")
   (`…/tickers/ticker-overview.md`). `GET /v3/reference/tickers/types` lists type codes (`…/tickers/ticker-types.md`).
   `GET /vX/reference/tickers/{id}/events` returns `ticker_change` events for a ticker, CUSIP or composite FIGI
   (`…/corporate-actions/ticker-events.md`).
5. **[to measure] Massive premises,** each with its plumbing-test assertion (§"Dry run", plumbing test).
6. **[probe][code] SEC bulk files** are overwritten daily (`app/services/sec_bulk_refresh.py:47-63`); slice F
   downloads its own copies.
7. **[doc] EDGAR.** Company records carry `sic` and a filing list with `form`, `acceptanceDateTime` and
   `accessionNumber`; older filings are in `filings.files[]` pages (`.claude/skills/data-sources/sec-edgar.md:59-74,
   1045, 1072-1074`). An accession header carries `STANDARD INDUSTRIAL CLASSIFICATION` (parent obligation 3).
8. **[code] Connections:** 27 usable, demand 24 plus reserve 3 (`app/db/pg_settings.py:127-230`).
9. **[code] B1 and factors:** neither `sec_nport_monthly_returns` nor `french_reference_refresh`'s snapshots are read
   (`app/services/reference_data.py:1051-1176`).
10. **[doc] Rekor v2** (`sigstore/architecture-docs` `rekor-v2-spec.md`; `sigstore/rekor-tiles` `CLIENTS.md`): one
    entry type, `hashedrekord` v0.0.2; requests are canonicalised and deduplicated; no integrated time, so clients
    obtain RFC 3161 timestamps (Sigstore operates `timestamp.sigstore.dev`); checkpoints follow C2SP
    tlog-checkpoint and may carry witness cosignatures; the log is served as C2SP tlog-tiles; shards rotate and freeze,
    discovered through Sigstore's TUF `SigningConfig`. The algorithm registry lists `ecdsa-sha2-256-nistp256`; pure
    `ed25519` "cannot be used for HashedRekord entries" (`client-spec.md`).
11. **[doc] JKP cutoffs.** `return_cutoffs.csv` had `Last-Modified` 2026-04-16 and last row 2025-12-31 on 2026-10-10.

## Runtime

- **One process outside the jobs daemon.** A launchd agent `com.ebull.forward-3740` runs
  `scripts/forward_3740_capture.py tick` every 10 minutes from a dedicated worktree at the declaration's git tag.
- **Tick preflight, in order.** Any failure is an integrity failure: retryable, no acquisition, recorded as a
  `preflight` record (§"Records").
  1. Worktree HEAD equals the pinned tag and is clean; `uv.lock`'s sha256 equals the pinned one.
  2. A read-only checkout of `origin/main` is fetched; it must hold the declaration file with the pinned sha256, and
     the trial's ledger file must hold no terminal entry for the trial (a terminal entry stops all acquisition).
  3. The `information_schema` hash over §"Schema interface" equals the pinned one (`SCHEMA_DRIFT` otherwise).
  4. Free disk under the capture root exceeds the next 30 days of the §"Dry run" disk projection.
- **Work queue, by priority:** (1) witness publication of pending records; (2) work whose window ends soonest; (3)
  bootstrap work. Each item has a 120 s request timeout; a tick stops starting items after 8 minutes. Every item's
  progress is a record, so an interrupted item resumes from its last record.
- **Database:** one connection at a time from the 3-connection reserve.
- **Massive:** a key used only by this process; the limiter allows 4 calls per minute. Budget per session:
  1 grouped, 12–15 reference pages, 2–4 action pages, plus per-security calls only for newly seen FIGIs
  (`massive_overview`) and FIGIs whose ticker changed between two bound reference snapshots (`massive_events`).
  Bootstrap work (§"Lookback") uses leftover capacity. Premise 5(g) gates the total.
- **eToro:** two listing requests per session under the shared 120/60 s quota. **SEC:** at most 2 requests per
  second, a partition of the shared 10/s as `scripts/build_2282_form25_register.py` partitions it.

## Records, binding and witness

### Records

Every event is a **record**: canonical JSON (RFC 8785) whose bytes are written once to
`<capture root>/<trial>/records/<seq>.json` and whose sha256 is the record id. Every record holds `trial`, `seq`,
`prev_record_sha256`, `record_type`, `created_at`, and the provenance block (code tag commit, construction hash, lock
hash, schema hash, ledger commit). Record types:

| type | written | adds |
|---|---|---|
| `intent` | before the first request of an attempt, with the `record_holdout_access` row (invariant 8) | attempt id, identity (below), request |
| `component` | after a component's file is fsynced, renamed and its directory fsynced | attempt id, component name, sha256, bytes |
| `close` | when an attempt ends | attempt id, outcome (`complete`, `transport_defect`, `abandoned`), validator version, each component's class (`valid`, `transport_defect`, `row_defect`) and defective rows |
| `selection` | at a selection's decision time | selection identity, the component ids it reads, rule version, value |
| `preflight` | on a preflight failure | the failed step |

- **Nothing parses unpersisted bytes,** and a page is parsed for `next_url` only after its `component` record exists.
- **Recovery.** A tick first reconciles: a final file with no `component` record is adopted by writing that record (a
  completed observation is never discarded); a `.part` file is kept and never parsed; an attempt with an `intent` and
  no `close` gets `close: abandoned` once its components are reconciled. Only an attempt closed `transport_defect` or
  `abandoned` with no component classed `valid` or `row_defect` permits another attempt.
- **Pagination.** A paginated set binds as a unit: its components must all come from one attempt, ending at a page
  with no `next_url`. A failed page closes the attempt `transport_defect`; the next attempt starts at page 1. A ticker
  or event `id` repeated across pages is a `row_defect` row.
- **Database.** The records are the source of truth; the tables of §"Schema interface" index them. The verdict
  rebuilds the index from the record files and refuses if they disagree (`CAPTURE_AMBIGUOUS`).
- **Abandoned-run classifier** (parent invariant 8): an attempt whose `close` is `abandoned` is step 2's abandoned run
  for its attempt id. The verdict run writes its `evaluate` access row before reading any record.

### Binding registry

- **Acquisition identity** (invariant 3) = `(trial, kind, source_id, period_key, component)`. The first `component`
  record of that identity in an attempt closed with that component `valid` or `row_defect` binds it. A second binding
  of one identity → `CAPTURE_AMBIGUOUS`.
- **Selection identity** = `(trial, selection_kind, selection_key)`; one `selection` record each. A second →
  `CAPTURE_AMBIGUOUS`.

| acquisition kind | source_id | period_key | component |
|---|---|---|---|
| `massive_grouped` | `massive:grouped?adjusted=false` | session date | the response |
| `massive_grouped_recovery` | same | session date | the response |
| `massive_tickers` | `massive:tickers?market=stocks&active={true,false}` | session date | each page |
| `massive_ticker_asof` | `massive:tickers?ticker={t}&date={x}` | (ticker, date) | the response |
| `massive_overview` | `massive:tickers/{ticker}` | composite FIGI | the response |
| `massive_events` | `massive:tickers/{figi}/events` | (FIGI, session date) | the response |
| `massive_types` | `massive:tickers/types` | session date | the response |
| `massive_splits`, `massive_dividends` | `massive:stocks/v1/{splits,dividends}` | (session date, range) | each page |
| `etoro_listing` | `etoro:/market-data/instruments`, `/instrument-types`, `/exchanges` | session date | one per endpoint |
| `sec_daily_index` | `sec:daily-index/form.{date}.idx` | index date | the file |
| `sec_header` | `sec:{cik}/{accession}.hdr.sgml` | accession | the file |
| `sec_form25` | `sec:{cik}/{accession}.txt` | accession | the submission |
| `sec_bulk_zip` | `sec:{companyfacts,submissions}.zip` | session date | the zip |
| `sec_bulk_members` | derived from `sec_bulk_zip` | session date | one member per superset CIK (all pages) |
| `sec_ticker_map` | `sec:company_tickers_exchange.json` | session date | the file |
| `french_file`, `jkp_file` | the source file | poll date | the file (version = sha256) |
| `nport_inventory` | `sec:nport-data-sets` listing | poll date | the page |
| `nport_dataset` | `sec:nport-data-sets/{quarter}` | (quarter, listing sha256) | the zip |
| `tuf_signing_config` | Sigstore TUF `SigningConfig` | session date | the file |

| selection kind | key | decision time | rule |
|---|---|---|---|
| `formation_inputs` | formation M | end of s(M)'s last window | §"Formation resolution" |
| `month_inventory` | holding month m | with `formation_inputs` for s(m − 1) | §F9 |
| `b1_entry_close` | M_0 | end of s(M_0)'s `massive_grouped` window | §F11 |
| `month_status` | (m, security) | when §F10's evidence for m is complete | §F10 |
| `month_returns` | (m, security) | the month's returns cutoff | §F9 |
| `month_final` | m | every `month_inventory` security has both | §F9 |
| `b1_month` | m | first accepted N-PORT version containing m | §F11 |
| `factor` | dataset | first accepted French version covering all 24 months | §F12 |
| `cutoff` | m | first accepted JKP version holding m's row, else the nine-month limit | §F14 |

### Witness (invariant 5)

- **Chain.** `prev_record_sha256` links every record to its predecessor from a genesis record pinned in the
  declaration. Publication never blocks the chain.
- **Signing.** Each record id is signed with an ECDSA P-256 key (`ecdsa-sha2-256-nistp256`, SHA-256 prehash) generated
  for the trial (a separate key for the dry run), its public key pinned in the declaration, and submitted as a
  `hashedrekord` v0.0.2 entry to the active Rekor v2 shard named by the latest bound `tuf_signing_config`. Rekor
  deduplicates canonical requests, so a retried submission returns the same entry.
- **Time.** (1) The signature is time-stamped by Sigstore's RFC 3161 authority. (2) **Publication time:** after the
  entry is included, the capture fetches a checkpoint whose tree contains it, stores the inclusion proof, and
  time-stamps the checkpoint's note bytes. Both tokens are verified against the pinned authority chain and against the
  exact signed bytes (the token's message imprint equals the SHA-256 of those bytes).
- **Admissibility.** A record is admissible when its signature verifies, its inclusion proof verifies against a
  checkpoint signed by the shard key, and that checkpoint's timestamp is at or before the record's witness deadline.
- **Completeness scan, at the verdict.** For every shard the trial used (from the bound `tuf_signing_config` history):
  the range starts at the first trial entry's index in that shard and ends at the shard's final checkpoint if frozen,
  else at a checkpoint fetched at the verdict that is consistent with every stored checkpoint of that shard
  (consistency proofs). Every full and partial tile in the range is fetched, its hashes recomputed up to the
  checkpoint's root, and every entry whose signature verifies under the trial key is listed. The listed entries must
  equal the chain's records one to one (a deduplicated resubmission is one entry). Shards are independent trees, so
  each is verified on its own.
- **Verdict checks, in order:** (1) chain and index integrity: hash links unbroken, record files match the index,
  scanned entries equal records, no identity bound twice → else `CAPTURE_AMBIGUOUS`; (2) admissibility per record;
  (3) the parent's verdict order.
- **Limit.** The witness exposes tampering with records; it cannot detect a fetch made outside the protocol.

### Windows

All times America/New_York; sessions, opens and closes from `app/services/market_calendar.py` at its pinned
`RULE_SET_VERSION`; o is the open and c the close of session d (early closes included). An acquisition not persisted
by its window end is final for that window (invariant 4); the witness deadline is the window end plus the lag.

| kind | window, per session d unless stated | witness lag |
|---|---|---|
| `etoro_listing` | o + 30 min to o + 90 min | 15 min |
| `massive_tickers`, `massive_types`, `massive_events`, `massive_overview` (new FIGIs) | o + 90 min to o + 180 min | 15 min |
| `sec_ticker_map`, `sec_header` (due), `sec_bulk_zip` and `sec_bulk_members` (s(M) and the two sessions before) | o + 120 min to c − 20 min | 15 min |
| `massive_grouped`, `massive_splits` and `massive_dividends` (range d − 30 days to d + 30 days) | c + 30 min to 08:00 on the next session's date | 60 min |
| `massive_splits`, `massive_dividends` (range d − 400 days to d + 30 days) | first session of each week, same window | 60 min |
| `massive_grouped_recovery` | for a session whose `massive_grouped` window bound nothing: 08:00 on the next session's date to the returns cutoff of the month containing it | 24 h |
| `sec_daily_index` | for index date x: 06:00 on the next calendar day until it binds, through the nine-month limit | 24 h |
| `sec_form25` | for each Form 25, 25/A, 25-NSE or 25-NSE/A in a bound index for a superset CIK: from that index's binding until it binds, through the nine-month limit | 24 h |
| `french_file`, `jkp_file`, `nport_inventory`, `tuf_signing_config` | daily 06:00–23:59 from the dry run's start to the nine-month limit | 24 h |
| `nport_dataset` | the poll day a bound listing shows a dataset whose listed size or date differs from every bound version, to 23:59 seven days later | 24 h |
| `massive_ticker_asof`, bootstrap kinds (§"Lookback") | dry-run days, leftover capacity, until complete | 24 h |

On a 13:00 early close, o + 180 min is 12:30 and the SEC window ends 12:40; the calendar test in slice 3 checks every
window of every session in the pinned calendar for start < end and dependency order (listing → reference → SEC).

- **Due SEC headers:** each 10-K- or 10-Q-family accession for a superset CIK in a bound daily index, in the next SEC
  window; and, for every CIK on entering the superset (the dry run's start included), its latest such accession from
  its bound submissions member, in the same session's SEC window.
- **Dependency rule:** a security whose same-session SEC work is not bound by the SEC window end is `sec_pending` at
  that session: not eligible to enter at a formation on it, counted.
- **Discovery:** French and JKP polls bind the whole file, so a version is its bytes. N-PORT polls bind the listing
  page, and a new dataset's zip is acquired in the same tick; versions are ordered by poll record `seq`, then listing
  order. A version replaced between two polls is never seen; that is stated, not ambiguous.
- **The nine-month limit** is 23:59 on the last calendar day of the ninth month after the 24th holding month. The
  **returns cutoff** of month m is the close of the tenth session after m's last session.

**Pre-close fallback.** Only `etoro_listing`, `massive_tickers` and `sec_bulk_members` may fall back: one missed on
s(M) uses the latest binding from the five sessions before s(M), named in `formation_inputs` with its age. Price and
action kinds never fall back; none in range → `formation_missed`.

### Outcomes

| event | scope | effect | code |
|---|---|---|---|
| `transport_defect` or `abandoned` with nothing bindable | identity | another attempt in the window | — |
| window ends with nothing bound | identity | final for that window | — |
| a formation input missing after fallback, or its record not admissible | formation | `formation_missed` | `INPUT_MISSING:<kind>`, `WITNESS:<kind>` |
| population refusals needing no holdings | formation | `formation_missed` | `SESSION_MISSING`, `ME_INVALID`, `UNIVERSE_SHORT`, `MAX_EMPTY` |
| `b1_entry_close` not selected, or its record not admissible by its deadline | trial | terminal `REFUSED` at once; all trial captures stop | `B1_UNDEFINED` |
| a returns, status, B1, factor or cutoff selection, or a record it reads, not admissible | trial | refuses (the parent's sealing row) | `CAPTURE_AMBIGUOUS` |
| a returns, status, B1, factor or cutoff selection not made by the nine-month limit | trial | terminal `REFUSED` | `INPUT_UNAVAILABLE` |
| chain or index integrity failure | trial | refuses until re-declaration | `CAPTURE_AMBIGUOUS` |
| preflight failure | tick | retryable; never a verdict | `SCHEMA_DRIFT`, `PREFLIGHT:<step>` |

An input missing its acquisition is unbound (`INPUT_UNAVAILABLE` path); an input acquired but not witnessed in time is
a sealing failure (`CAPTURE_AMBIGUOUS`), as the parent's lifecycle table assigns.

### Acquisition superset

- **Securities,** keyed by composite FIGI: every FIGI that, in any bound `massive_tickers` since the dry run's start,
  maps (§F13) to an instrument in any bound `etoro_listing` with `instrumentTypeID` 5 on exchanges 4, 5, 20 or 33; plus
  SPY and IVV. Once in, never out.
- **Market-wide kinds** (grouped, reference, actions) are unfiltered, so every security is covered from the dry run's
  start whether or not it is in the superset yet.
- **CIKs:** every CIK linked (§F13) to a superset FIGI on any bound session.

### Lookback (amendment A2)

A formation reads sessions before s(M_0): MAX's boundary month, F5's share-basis interval (up to 15 months) and action
history. **Admissible lookback objects:** dry-run bindings of every acquisition kind above, plus these bootstrap
acquisitions at the dry run's start:
- `massive_splits` and `massive_dividends` over the 24 months before it (Massive's Basic history);
- `massive_ticker_asof` for each bootstrapped event's ticker on the session before its date (the pre-event mapping);
- `massive_overview` for every superset FIGI.

Conditions: the trial tag's validators re-run on the dry-run bytes (a component whose class changes is not
admissible); every dry-run record passes the witness checks on the dry-run key; every session from the dry run's start
to s(M_0) has a bound `massive_grouped` (recovery is not admissible for formation inputs, §F8); and the declaration
pins the dry-run chain head. This replaces v1 of this spec's rule that dry-run captures are never trial inputs.

## Input rules

### F1. Security type (obligation 1; exception X1)

- **Fields, structured only:** eToro `instrumentTypeID` (bound listing); Massive `type` for the FIGI on s(M); the
  CIK's SIC (F3); and for entry, the cover title cross-check below.
- **Rule, in order:**
  1. `instrumentTypeID` 5, else `type_not_stock`; eToro exchange 19 → `otc`.
  2. Massive `type` in the accepted set, else `type_rejected:<code>`; missing → `type_unknown`. The accepted set is
     frozen in slice 3 from the first bound `massive_types` as the codes described as common stock or ordinary shares;
     ADR codes are rejected (JKP keeps CRSP share codes 10, 11 and 12; ADRs are 31).
  3. SIC 6221, 6722 or 6726 → `type_fund`.
  4. **Cover cross-check, every formation:** the CIK's latest 10-K or 10-Q cover (`dei:Security12bTitle` for the
     class whose `dei:TradingSymbol` equals the ticker) classed by a frozen keyword table (rejections first:
     preferred, depositary, warrant, right, unit, note, debenture, bond, beneficial interest, partnership or LLC
     interest; then common stock, common shares, ordinary shares, capital stock). A rejection → `type_conflict`, not
     eligible to enter. No matching title → passes on rules 1–3 and is counted.
- **Exception (X1):** the parent lists SEC filer and SIC evidence after the eToro type. Massive's `type` is a
  structured security-level field; SEC evidence is kept as rule 3 and rule 4.
- **Identity:** one composite FIGI is one security. Two eToro instruments mapping to one FIGI → `duplicate_instrument`
  for both unless exactly one is on exchange 4 or 5, which is kept; every pair listed.
- **Validation (dry run, pass/fail):** every superset security's rule 1–4 outcome; every `type_unknown`,
  `type_conflict`, `duplicate_instrument` and no-title row, and a frozen random sample of 200 accepted rows (seed
  `3740-sliceF-type`), adjudicated by hand against filings. Pass: no adjudicated accepted row that is not common
  equity. Forward entrants get rules 1–4 automatically; adjudication is compatibility evidence, not a guarantee.

### F2. Listing age (obligation 2; exception X8)

- **Source:** Massive `list_date` from `massive_overview` at the FIGI's first observation (first observation binds).
- **Rule:** eligible when `list_date` ≤ s(M) − 36 months, and **none** of these holds (each → `listing_age_unknown`,
  not eligible to enter): `list_date` missing; a `ticker_change` event for the FIGI in any bound `massive_events`; the
  CIK has an exchange certification (CERT*) or Form 8-A12B accepted after `list_date` for a class whose title classes
  as common (a relisting or new class).
- **Exception (X8):** a symbol's first-listing date is a proxy for security listing age, kept only where no evidence
  of a ticker change or later listing exists.
- **Validation (dry run, pass/fail):** for every superset security, `list_date` beside the earliest matching CERT*
  acceptance and, for names in step 1's panel, the first Intrader bar. Every case where they disagree on eligibility
  at any dry-run formation is adjudicated, both directions. Pass: no adjudicated case where the rule admits a security
  listed less than 36 months before the formation. False exclusions and `listing_age_unknown` counts are reported.

### F3. SIC (obligation 3)

- **Source:** the `sec_header` of the CIK's latest 10-K- or 10-Q-family accession accepted before s(M), bound before
  s(M)'s close. The latest is taken from the bound submissions member plus bound daily indexes since it.
- **Rule:** the header's `STANDARD INDUSTRIAL CLASSIFICATION` code; none → `sic_null`; no bound header →
  `sic_unloaded`. Used for the REIT exclusion (6798), F1 rule 3 and the FF-12 map.
- **Header semantics:** the header is EDGAR's dissemination header for the accession (EDGAR PDS specification); the
  bytes read are those available before s(M)'s close.
- **Compatibility (parent obligation 3), reported:** for every 10-K- or 10-Q-family accession of a superset CIK in the
  four latest SUB quarters, the header SIC (read at validation time) beside SUB `sic`, with disagreements and
  unmatched accessions on both sides counted and listed. Frozen before comparing; compatibility evidence only.

### F4. Accounting (obligation 4)

- **Source:** `sec_bulk_zip` (the zips downloaded by the capture itself, with `Last-Modified`, `ETag` and sha256
  recorded; retained until the members' `close` record is witnessed, then deleted with a record) and
  `sec_bulk_members` (each superset CIK's members, every `filings.files[]` page present, else `transport_defect`).
  Extraction reads the retained zip, so a crash re-extracts the same bytes.
- **Rule:** step 1's `pit_fundamentals` bundle built from those members by step 1's code with only its input reader
  changed: evidence cutoff (acceptance New York date strictly before s(M)) and the four-month lag.
- **Freshness report:** each formation lists the 10-K- and 10-Q-family accessions for superset CIKs in bound daily
  indexes accepted before s(M) that the members lack.

### F5. Shares and ME (obligation 5)

Step 1 §"Market equity" exactly on the F4 bundle: cover-count precedence, the 15-month age limit, blocked reads,
context-date and acceptance-date bases, no share lag. **The split product over (b, s(M)]** is Π (`split_to` /
`split_from`) over the FIGI's split events with `execution_date` in (b, s(M)], from the action inventory as of s(M)'s
`massive_splits` decision (§F7), b the basis date. Execution-date semantics (premise 2) place the first post-split
session on `execution_date`, step 1's stamp convention. ME = adjusted shares × P(s(M)), P the F6 close.
- **Fixtures** (slice 3): forward split, reverse split, stock dividend; an event on b and one on s(M); context-date and
  acceptance-date bases; two events in the interval.
- **Reconciliation (dry run, pass/fail):** for every security with an event in (b, s(M)] and a later cover count
  filed during the dry run, predicted shares beside the later count; every difference beyond 20% adjudicated. Pass: no
  adjudicated case caused by a missing, misdated or wrong-ratio event. The ME jump census (ME changing beyond ×1.5
  between formations with split-adjusted price within ×1.2) is reported as a supplementary detector.

### F6. Raw close at a session d (obligation 6; exception X2)

- **Source:** `massive_grouped` for d (`adjusted=false`), bound in d's window; the row whose `T` is the FIGI's ticker
  on d in the bound `massive_tickers`.
- **Readiness (response validator):** `status` OK, `adjusted` false, every row's `t` on d in New York time,
  `resultsCount` at least 90% of the median of the previous 20 bound sessions (the first 20 sessions: of the
  plumbing-test median), and SPY present. Otherwise `transport_defect`: an empty or partial response is not ready and
  never binds.
- **Row rule:** a row is valid when `c` is finite and > 0. A duplicate `T` is a `row_defect` for that ticker only;
  rows of a `row_defect` component outside its defective rows are valid.
- **Exception (X2):** the parent's close is a market-on-close exchange print, and step 2 used Intrader's raw trade
  close. Massive documents the grouped close's split basis but not its session. **Validation (dry run, pass/fail):**
  on every dry-run session, for 15 superset securities drawn by seed `3740-sliceF-close:<d>`, the grouped `c` beside the
  per-ticker open/close endpoint's `close` (documented apart from `afterHours`). Pass: equal to 1e-6 relative on every
  pair, else the session semantics differ and the route is revised by amendment. eToro's official close is printed
  beside them as parity.
- **Dry run (pass/fail):** per ordinary session, valid closes for at least 99% of superset securities active in the
  bound tickers.

### F7. Split and dividend events (obligation 7)

- **Inventory.** Every bound `massive_splits` and `massive_dividends` row, keyed by `id`, kept as revisions: a
  revision is (id, content sha256, first record `seq` it appears in). An id that a later capture covering its date
  does not return is marked withdrawn at that capture.
- **As-of view at time T:** for each id, its latest revision first seen at or before T, unless withdrawn at or before
  T. A formation uses T = s(M)'s action decision (the end of its window); a holding month uses T = its returns cutoff.
- **Row validators:** splits need `id`, `ticker`, a valid `execution_date`, finite `split_from` > 0 and `split_to` > 0,
  and a known `adjustment_type`; dividends need `id`, `ticker`, a valid `ex_dividend_date`, finite `cash_amount` > 0
  and `currency`. A failing row is a `row_defect` affecting only its (ticker, date).
- **Mapping.** An event maps to the FIGI that held its `ticker` on the session before its date, from the bound
  `massive_tickers` for that session, or from `massive_ticker_asof` for bootstrapped events. No FIGI →
  `action_unmapped` for that (ticker, date).
- **Conflicts.** Two ids for one (FIGI, date, kind) with different ratios or amounts → `action_conflict` for that FIGI
  and date.
- **Independent enumeration (dry run, pass/fail):**
  1. sessions with |ln(P_d / P_prev)| > ln 1.6 and no split event in (prev, d] (candidate missed splits);
  2. eToro's stored `price_daily` closes exported to a frozen CSV (sha256 recorded) at two dry-run month-ends; a session
     whose stored close changes between the exports by a ratio outside [0.98, 1.02] marks an eToro-applied action;
  3. SEC companyfacts `CommonStockDividendsPerShareDeclared` per fiscal quarter beside the sum of Massive cash amounts
     with ex-dates in that quarter, for every superset security.
  Every candidate from 1–3 without a Massive event, and every Massive split that neither 1 nor 2 shows, is adjudicated
  against the issuer's filing. Pass: no adjudicated missed, misdated or wrong-ratio split, and no adjudicated missed
  dividend. Dividend ex-dates for every special dividend and a frozen sample of 100 (seed `3740-sliceF-div`) are
  checked against the issuer's announcement; pass: exact amount and exact ex-date.

### F8. MAX inputs (obligation 8; exception X3)

- **Daily total return** on consecutive usable bars p < q of one FIGI (adjacent sessions for a MAX return):
  g = (K · P_q + D) / P_p, r = g − 1, with K = Π (`split_to` / `split_from`) over its split events in (p, q] and D =
  Σ `cash_amount` over its USD dividends with ex-date in (p, q]. Gross, pre-withholding (parent obligation 9). The
  return is invalid when the interval holds an `action_conflict`, an `action_unmapped` or `row_defect` row, a non-USD
  dividend (`dividend_currency`), or a split and a dividend (`action_ambiguous`, because Massive does not document
  the share basis of `cash_amount` across a split).
- **Rule:** #3621's MAX at s(M): the largest adjacent-session r over the sessions of s(M)'s calendar month up to s(M),
  with the previous month's last session as the boundary bar; ≥ 15 returns; < 10 zero returns; the return screen
  (−0.9, 3.0); screened names flagged before the missing-value rules.
- **Cutoff:** a formation reads only `massive_grouped` bindings (never recovery) and the action as-of view at s(M)'s
  decision, so every input was bound before s(M)'s close or in its post-close window (parent invariant 1).
- **Exception (X3): the ratio screen.** Step 1 and #3621 screen consecutive usable bars p, q when the vendor's
  `adj_close / close` ratio moves beyond ×1.5 with no stamp in (p, q] (`3609-step1-factor-panel.md:962-967`). Here the
  adjustment is built only from dated events, so that ratio cannot move without a stamp and the screen is vacuous.
  Its error class (an unrecorded adjustment) appears instead as an unexplained raw price jump. Forward screen: a
  window is screened when it contains a pair p, q (across missing sessions) with no split event in (p, q] and
  |ln(P_q / P_p)| > ln 1.6, or an invalid return of the kinds above. Changed classes, stated: a genuine move beyond
  ×1.6 with no action is now screened (a false positive the old screen did not have); an unrecorded split under ×1.6
  passes.
- **Measurement before planning (parent slice 2):** on stage A and B, #3621's MAX as frozen beside MAX with the forward
  screen applied to Intrader's raw closes and stamps (the combined treatment), per formation: cutoff, flagged-set size,
  symmetric difference. The vendor change (Intrader to Massive closes) is unmeasured: Massive's 2 years do not overlap
  the panel.

### F9. Holding-month returns (obligation 9)

- **`month_inventory` for month m,** bound with `formation_inputs` for s(m − 1): the union of the universe (top 1,000
  by ME) at every captured formation from M_0 through s(m − 1), plus SPY. Every book, control and reference holding is
  a subset; completeness is judged against this set.
- **Total return** of an `observed` security for month m: G = Π g over consecutive sessions from s(m − 1) to s(m)
  (F8's formula), R = G − 1. Both are stored.
- **Inputs:** `massive_grouped` and `massive_grouped_recovery` bindings; the action as-of view at the returns cutoff.
- **Validity:** a valid close at s(m − 1) and at every session through s(m), and every g valid. Otherwise the return
  is incomplete. Price evidence ends at the returns cutoff (recovery's window end); an incomplete return then stays
  incomplete and the trial reaches `INPUT_UNAVAILABLE` at the nine-month limit if the security is a holding there.
- **Selections:** `month_returns` binds each inventory security's (status from §F10, G or partial return, inputs) at
  the returns cutoff, or later when its status completes; `month_final` binds when every inventory security has one.
- **Independent reference (dry run, pass/fail), frozen before comparing:**
  - **daily:** every superset security-session r beside the return from the frozen `price_daily` export (split
    back-adjusted, no dividends) on sessions with no dividend; pass: median absolute difference under 0.1 pp, at most
    0.5% of security-sessions beyond 2 pp, every one beyond 10 pp adjudicated; sessions missing in either source are
    counted by reason;
  - **monthly and partial:** for 30 security-months drawn by seed `3740-sliceF-month` (10 with a split, 10 with a
    dividend, 10 terminal or coverage exits), the return recomputed by hand from the bound bytes; pass: equal to 1e-9;
  - **actions:** §F7; **terminations:** §F10's table.

### F10. Statuses and terminations (obligation 10)

- **Capture state:** a session with no admissible `massive_grouped` or recovery binding is `capture_missing` for every
  security: an incomplete input, never a status.
- **Trading interval:** a FIGI's sessions with a valid close. **Cessation:** the FIGI is inactive in a bound
  `massive_tickers`, or shows `delisted_utc`, on or before the returns cutoff; the cessation session is the last valid
  close on or before `delisted_utc` (or before the first inactive snapshot).
- **Status, step 1's rule** (`3609-step1-factor-panel.md:978-988`; parent §"Returns window"):
  1. **`terminal`** when the series ceased in the month (the terminating event): partial return to `end_bar`, the
     cessation session, then `terminal_value_fraction(class, arm)`, both arms;
  2. **`coverage_exit`** when bars stop without cessation by the returns cutoff, or on an interior gap (a session with
     no row while the FIGI is active): partial return to `end_bar`, the last valid close before the first missing
     session; no resumption credit;
  3. **`observed`** otherwise.
- **Class:** `TerminationEvidence` (`app/services/series_termination.py:150-166`): `linked` and `provision` from the
  Form 25 evidence under `classify_form25_match` (`app/services/research_corpus_ingest.py:1566`) as it stands: a
  `conflict` (filings disagreeing on provision or suspension date) or `identity_unverified` (every filing before the
  FIGI's first bound session) leaves the security unlinked; `q_suffix` by `archive_symbol_candidates`' rule
  (`research_corpus_ingest.py:340`) on the last ticker. `classify_termination` (`series_termination.py:168`) gives the
  class; `LINKED_UNPARSED` and `UNKNOWN` are two-armed (`TWO_ARMED_CLASSES`).
- **Evidence completeness:** eligible evidence is every Form 25 family filing for the security's CIK accepted by the
  returns cutoff. `month_status` binds once every daily index through the cutoff and every eligible filing body is
  bound; until then the status is pending, and the nine-month limit applies.
- **Boundary cases:** no valid close after s(m − 1) and ceased → `terminal` with `end_bar` s(m − 1) (partial return 0
  before imputation); a missing close at s(m − 1) for a held security is the formation's `PRICE_INVALID` (parent); an
  invalid return before `end_bar` → incomplete.
- **Termination table (dry run, pass/fail):** every superset security that ceased, gapped or lost its ticker in the dry
  run: Massive `delisted_utc`, Form 25 result, class, status, eToro listing change, adjudicated by hand. Pass: no
  adjudicated wrong status or class. Slice 3 fixtures cover each class, the conflict and identity branches, Q-suffix,
  an interior gap and `capture_missing`.

### F11. B1 (obligation 11)

- **Returns:** each `nport_dataset` version is parsed with `load_3619_nport_returns`'s parser from the pinned tree and
  accepted when IVV's (`C000012040`) rows pass step 2's identity gate. `b1_month` for m selects IVV's Item B.5.a return
  from the filing with the earliest acceptance in the first accepted version (by poll order) containing m; two filings
  with that acceptance time, or two values for m in it → `B1_AMBIGUOUS` (`INPUT_UNAVAILABLE` at the limit).
- **Band:** `b1_entry_close` is SPY's F6 close at s(M_0), selected at the end of s(M_0)'s `massive_grouped` window; its
  witness deadline is that end plus 60 minutes, the B1 gate. No selection for month M_0 + 1 is made before the gate
  passes. B1's sale at s(M_0 + 24) is charged at the entry close's band (the parent's one-band rule), so no exit close
  is needed.

### F12. Factors (obligation 12)

Each `french_file` version is parsed with the pinned `reference_data` parser and accepted when step 2's checks (unit,
completeness) pass. `factor` for each dataset (five-factor, momentum) selects the first accepted version (by poll
order) covering all 24 forward months.

### F13. CIK link and ticker mapping (population step 2; exception X7)

- **eToro to Massive:** an instrument maps to the FIGI whose ticker on s(M) in the bound `massive_tickers` equals the
  eToro symbol under the class-suffix table frozen from premise 5(f). None → `unmapped`; two → `map_ambiguous`.
- **Missing FIGI:** a reference row with no `composite_figi` is `identity_missing`: not eligible to enter. A held
  security whose row loses its FIGI keeps its identity while its ticker and CIK are unchanged from the last session
  with a FIGI; otherwise its status is incomplete.
- **CIK link:** the FIGI's `cik` on s(M). Missing → `cik_missing`.
- **Exception (X7):** step 1 linked on Form 3/4/5 `issuerTradingSymbol` evidence, which the quarterly data sets lag.
- **Validation (dry run, pass/fail):** at each formation, Massive's CIK beside SEC's `company_tickers_exchange.json`
  bound the same session and step 1's latest linkage; every disagreement adjudicated. Pass: no adjudicated case where
  Massive's CIK is wrong.

### F14. Return cutoffs (step 1 Amendment 3; exception X9)

- **Selection:** `cutoff` for month m is decided at the first accepted `jkp_file` version holding m's row (that row is
  used), or at the nine-month limit if none has (the latest month's row of the latest accepted version is used,
  provided it is at most 18 months older than m; otherwise `INPUT_UNAVAILABLE`). Never replaced. A version is accepted
  when step 1's per-row check passes on every row.
- **Exception (X9):** step 1 clips with the holding month's own row; carry-forward applies only when JKP has not
  published the month by the limit.
- **Measurement before planning (parent slice 2), labelled scenarios:** on stage A and B, the book's and the control
  median's G per arm with own-month clipping beside clipping by the row lagged 4, 8 and 12 months; counts of changed
  clips and the five most-changed months. The actual historical availability effect is unmeasured (no retained
  versions); the declaration records the scenario figures.

## Formation resolution (amendment A1)

Holdings are not formed before the verdict (invariant 7), so `formation_inputs` for M binds, at the end of s(M)'s last
window, the instrument-level inputs for the whole superset (F1–F8, F13, eligibility, MAX, ME, raw close, codes) and
marks population refusals at once. At the verdict, the report forms the paths and applies the parent's holding-
dependent rule (`PRICE_INVALID` for a holding with no valid close that was not realised) from those bindings; a
formation missed this way counts toward `ACCRUAL_GAPS` exactly as if found during the accrual.

## Splicing (invariant 6)

| element | identity mapping | adjustment basis | overlap reconciliation | vendor precedence |
|---|---|---|---|---|
| historical ↔ forward | none: the forward path starts all cash at s(M_0) | — | — | — |
| eToro ↔ Massive | symbol on s(M), suffix table, to composite FIGI | — | F13; F9's daily comparison | eToro for listing and tradability; Massive for prices and actions |
| Massive closes ↔ actions | ticker on the session before the event, to FIGI | unadjusted closes; dated events | F7's enumeration | Massive only |
| Massive ↔ SEC | F13's CIK | — | F13 table | SEC for accounting, SIC, Form 25 |
| B1 | IVV class `C000012040` | NAV total return | step 2's identity gate | N-PORT only |

## Schema interface (pinned runtime)

Record files are the source of truth. The capture and verdict processes read and write only these index tables:
- `forward_capture_records` (`record_sha256`, `trial`, `seq`, `prev_record_sha256`, `record_type`, `attempt_id`,
  `path`, `created_at`), unique on (`trial`, `seq`);
- `forward_capture_components` (`record_sha256`, `trial`, `kind`, `source_id`, `period_key`, `component`,
  `attempt_id`, `sha256`, `bytes`, `path`, `validation_class`, `defect_rows`), with a partial unique index on
  (`trial`, `kind`, `source_id`, `period_key`, `component`) where `validation_class` is `valid` or `row_defect`;
  `validation_class` is written by the transaction that inserts the attempt's `close` record;
- `forward_capture_selections` (`record_sha256`, `trial`, `selection_kind`, `selection_key`, `component_sha256s`,
  `rule_version`, `value`), unique on (`trial`, `selection_kind`, `selection_key`);
- `forward_capture_witness` (`record_sha256`, `rekor_entry`, `inclusion_proof`, `checkpoint`, `signature_tsa`,
  `checkpoint_tsa`, `witnessed_at`);
- `strategy_holdout_accesses` (`access_id`, `strategy_id`, `strategy_version`, `result_version`, `access_kind`,
  `accessed_by`, `purpose`, `accessed_at`).

The code holds this list in one constant, with a test that the hashed `information_schema` slice covers exactly these
columns, indexes and constraints. Dry-run reports that read other tables (`price_daily`,
`instrument_universe_membership`) export frozen files first and run outside the pinned interface.

## Declared exceptions

| id | parent rule | forward rule | why | evidence before declaration |
|---|---|---|---|---|
| X1 | eToro type, then SEC filer and SIC evidence | eToro type, Massive `type`, SIC, cover cross-check | a structured security-level type exists | F1 validation |
| X2 | market-on-close exchange print (step 2: Intrader raw close) | Massive grouped close | grouped session undocumented | F6 per-session equality to the open/close `close` |
| X3 | MAX ratio screen on unstamped `adj_close / close` | unexplained-jump screen (§F8) | the ratio screen is vacuous on event-built adjustment | F8 stage A/B measurement |
| X5 | tradability from `instrument_universe_membership` | `isTradable` in the bound `etoro_listing` at s(M) | the membership table is daemon state outside the pinned runtime | parity at each dry-run formation, every disagreement with its reason |
| X7 | Form 3/4/5 symbol linkage | Massive `cik` on s(M) | the insider data sets lag a quarter | F13 table |
| X8 | Form 8-A 12(b) registration (candidate) | Massive `list_date` with the exclusions of §F2 | acceptance is not effectiveness or continuity | F2 table |
| X9 | the holding month's JKP cutoffs | own row when published by the nine-month limit, else carried ≤ 18 months | JKP publishes in arrears | F14 scenarios |

**Amendments to the parent:** A1 (formation resolution); A2 (lookback).

## Dry run (prospective)

**Plumbing test first,** on the dry-run key, over 20 consecutive sessions; each failure is an amendment with its own
checkpoint 1:
- (a) **timing:** for every session, the time of the first ready grouped response (§F6 readiness) under the actual
  polling policy, printed as a distribution; pass: every session ready before 08:00 on the next session's date;
- (b) SPY and IVV present on every session;
- (c) **usability:** every eToro type-5 US instrument on exchanges 4, 5 and 33 counted by reason (mapped with FIGI,
  CIK, accepted type and a grouped row; or the failing code); pass: at least 95% fully usable, every unusable one
  listed;
- (d) **delisted coverage:** every `active=false` stock of an accepted type with `delisted_utc` 30 to 700 days before
  the test, present in grouped daily on each of its last five trading sessions; pass: at least 99%, every miss
  adjudicated;
- (e) **actions:** range filters inclusive at both ends (asserted on events at the bounds); pagination returns every
  row once across `next_url`; required fields present at rates printed; announced future events listed before their
  date (yes or no, which sets whether the weekly long-range capture alone suffices); non-USD dividend share of the
  superset printed with the count of superset securities it would make structurally incomplete; pass: the first two
  hold;
- (f) the class-suffix table and the accepted type codes frozen;
- (g) **capacity:** a full day's queue, including overview and bootstrap work, under the daemon's normal load, with
  every window met at 4 calls per minute and the tick budget; the projection for 30% superset growth also fits.

**Then at least three consecutive month-ends** from the dry-run tag, trial `3740-slice-f-dryrun`, own Rekor key.
Accepted only if every item passes:
1. **Captures:** every mandatory window bound and witnessed on time. Any miss fails this item. Abandoned attempts: none
   outside the drills.
2. **Pass/fail validations:** F1, F2, F5, F6 (both), F7, F9 (every stratum), F10 and F13.
3. **Reported tables:** F3, F4 freshness, X5 parity, the parent's parity table, plumbing (e)'s structural exposure.
4. **Measurements before planning:** F8 and F14 on stage A and B, recorded.
5. **Event coverage:** at least five split events (one reverse), one special dividend, one Form 25 termination and one
   ticker change in the superset; otherwise the dry run extends month by month. F7's and F10's adjudications include
   non-event cases, so quiet sessions are tested too.
6. **Witness drills** on the dry-run key (or a local `rekor-tiles` instance where the drill needs a shard or
   checkpoint we cannot produce on the public log), each with its expected verdict:
   publication past its deadline → record inadmissible, chain continues, scoped outcome; resubmission → one entry;
   two records for one identity → `CAPTURE_AMBIGUOUS`; record file deleted after logging → scan finds an unmatched
   entry, `CAPTURE_AMBIGUOUS`; record edited then reverted → hash mismatch while edited, then a clean verify; deleted
   chain tail → unmatched entries; branch (two records with one predecessor) → `CAPTURE_AMBIGUOUS`; a stale terminal
   checkpoint → consistency failure, verdict refuses to run; a shard rotation → per-shard ranges verify; a timestamp
   token for other bytes → imprint mismatch, inadmissible; a selection row changed in the index → index rebuild
   mismatch, `CAPTURE_AMBIGUOUS`; a crash in every record state → the reconcile outcome of §"Records".
7. **Completeness scan cost:** the full scan over the dry run's range, time and bytes, projected to 24 months plus the
   nine-month limit. Pass: the projection completes within 7 days on this machine.
8. **Disk projection** for the whole retained inventory with peak usage. Pass: within free space with 2× margin.
9. **No portfolio** formed, weighted or valued.

Dry-run passes are compatibility evidence over the dry run's population; forward cases are handled by the
deterministic codes above, and the codes' counts are printed beside the verdict.

## Build slices

1. This spec and the probe (this PR).
2. Records, the artefact store, recovery, the Rekor v2 and RFC 3161 witness and the verdict-side checker with the tile
   scan, with the drills on a scratch key. Codex checkpoint 2.
3. The capture kinds, the action inventory and the F-rules with fixture tests; the calendar window test; the launchd
   agent. Codex checkpoint 2.
4. The F8 and F14 historical measurements (with the parent's slice 2).
5. Plumbing test, then the prospective dry run, posted on #3740.

## Known limits

- Massive is one vendor for prices and actions; F7 and F9 enumerate its errors against eToro and SEC, but an error
  both share is not caught.
- The vendor change in MAX's daily closes and the historical availability of JKP rows are unmeasured (X3, X9).
- A structurally invalid return on a held security (`dividend_currency`, `action_ambiguous`, `action_conflict`)
  cannot be completed and runs to `INPUT_UNAVAILABLE`; plumbing (e) and the dry run print the exposure.
- The witness exposes tampering with records; it cannot detect a fetch made outside the protocol.

## Round 3 dispositions (for round 4's task A)

| finding | v4 |
|---|---|
| 1 signing | ECDSA P-256 `hashedrekord` v0.0.2 (premise 10) |
| 2, 3, 10 selections, payload, attempts | every transition and selection is a canonical chained record (§"Records") |
| 4 schema | identity columns and partial unique index on the component table; witness table |
| 5 crash | reconcile adopts completed files; directory fsync; per-component records |
| 6 pagination | page records before parsing; one-attempt sets; restart at page 1 |
| 7 publication time | TSA over the signature and over the including checkpoint, imprint verified |
| 8 scan bounds | per-shard ranges, final or consistent checkpoints, tile hashes to the root |
| 9 duplicates | Rekor deduplication: one entry per record |
| 11 witness scope | parent's sealing row: inadmissible returns-side record → `CAPTURE_AMBIGUOUS` |
| 12, 13 windows | windows from the open; dependency order; `sec_pending` rule; calendar test |
| 14, 15 capacity | events only on ticker change; bootstrap uses leftover capacity; plumbing (g) on a full day |
| 16 recovery identities | recovery, as-of and bootstrap kinds registered with windows |
| 17 discovery | French and JKP polls bind bytes; N-PORT zip in the same tick; poll order |
| 18 MAX recovery | formations never read recovery bindings |
| 19 denominator | `month_inventory` |
| 20 delayed evidence | `month_status` waits for indexes and every eligible filing body, incl. /A forms |
| 21 amendments | `classify_form25_match` unchanged: conflict leaves unlinked |
| 22, 23 Q-suffix, cessation | step 1's terminal/coverage_exit split; cessation from `delisted_utc` |
| 24 boundaries | §F10 boundary cases |
| 25 late completion | price evidence ends at the returns cutoff; only SEC/N-PORT/French/JKP complete later |
| 26 historical mapping | `massive_ticker_asof` for bootstrapped events |
| 27, 28, 29 action inventory | cumulative revisioned inventory, as-of views, withdrawal, conflicts, FIGI mapping |
| 30 validators | §F7 row validators |
| 31 dividend basis | claim removed; split+dividend refused |
| 32 readiness | §F6 response validator |
| 33 row scope | defective rows only |
| 34 close session | X2 with per-session equality test |
| 35 missing FIGI | §F13 `identity_missing` |
| 36 F1 substitution | X1 declared; cover cross-check every formation |
| 37 listing history | §F2 exclusions |
| 38 SEC bootstrap | header bootstrap per new CIK; `sec_pending` |
| 39 SUB comparison | accession-level over the four latest SUB quarters |
| 40 ZIPs | retained zip, derived member records, recorded disposal |
| 41 ME | reconciliation against later cover counts; census supplementary |
| 42, 43 X3 | changed classes stated; cumulative interval (p, q] |
| 44 reference | frozen exports; missing counted; hand-computed monthly and partial sample |
| 45 dividends | SEC declared-dividend enumeration; exact ex-dates |
| 46 terminations | §F10 table and fixtures |
| 47 B1 exit | removed (one-band rule) |
| 48, 49 X9 | own row up to the limit; ≤ 18-month carry; lag scenarios labelled |
| 50–52 plumbing | assertions and frozen populations in (a)–(g) |
| 53 guarantees | dry-run passes stated as compatibility evidence; forward codes deterministic |
| 54 drills | enumerated with verdicts |
| 55 invariant 8 | `evaluate` row before reads; abandoned classifier on `close` |
| 56, 57 nits | status line; this table |
| round 2's 52 (probe) | fixed in `88c7248f`: an empty response or comparison set exits with a message, HTTP status is checked (`scripts/probe_3740_slice_f_sources.py:43-78, 118-120`); round 3 did not read the probe |

## Checkpoint log

- **Round 1 (44 findings: 37 BLOCKING).** Applied in v2.
- **Round 2 (53: 46 BLOCKING).** Source-validity findings moved market data to Massive (v3).
- **Round 3 (57: 43 BLOCKING, 12 WARNING, 2 NIT; on v3).** Task A: 17 of round 2's findings applied, 35 partial, 1
  not applied (52, the probe, which round 3 did not read). Applied in v4 (table above).
- **Round 4 (59: 39 BLOCKING, 18 WARNING, 2 NIT; on v4).** Task A: 22 of round 3's findings applied, 35 partial. Not
  applied yet. Finding counts per round (44, 53, 57, 59) are not falling; the route question this raises is on #3740
  (2026-10-10 handoff) and is decided before round 5.
- **Round 5 (25: 14 BLOCKING, 11 WARNING; on the route note, not v4).** Applied in the note's revision: claims
  narrowed to "not yet demonstrated" and labelled as pipeline presence counts (1–4, 6–8); the eToro dividend calendar
  added and measured (5); PWB excluded by the access rule rather than by inversion (9, 10); the price-return lower
  bound withdrawn as the route (11–20); obligations 6 and 7 marked unresolved (21–24). Findings 15, 16 and 25 concern
  the withdrawn route and lapse with it. Verbatim in the findings file.
- **Round 6 (27: 17 BLOCKING, 8 WARNING, 2 NIT; on the revised note).** Task A: round 5's 3, 8, 9, 22 applied; 1, 2,
  4–7, 10, 21, 23, 24 partial; 11–20, 25 lapsed. Applied now: claims and labels narrowed (20–22, 25–27 wording); the
  measurement sketch and its 99% rule withdrawn and replaced by the decisions above. Open for the measurement spec:
  1–19, 23, 24.
