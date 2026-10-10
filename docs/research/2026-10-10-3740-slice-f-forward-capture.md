# #3740 slice F — the forward capture for step 3's paper accrual

Status: **draft; Codex checkpoint 1 open after round 2, and the market-data route is changing (§"Route change after
round 2").** Nothing here has been built or run beyond the read-only probe in §"Measured premises".

## Route change after round 2 (2026-10-10)

Round 2 (53 findings, 46 BLOCKING) confirmed that the protocol revisions moved forward, but it rejected the market-data
route at its root. eToro's official close has no documented basis, and per-observation checks (roll, dated
confirmation, in-session anchor) are diagnostics, not the nominal-price contract obligation 6 requires (round 2,
findings 11, 27, 28). eToro serves no dated corporate actions, so F7's measured vendor factor cannot attribute or
classify an action (29–31). PWB's monthly republish has no evidence cutoff or documented coverage contract (34–41).
Each of those is a property of the sources, so further rule-writing on them will not close the checkpoint.

**Massive (formerly Polygon.io) documents what is missing, on its free "Stocks Basic" plan** (5 calls per minute,
2 years of history, end-of-day data, reference data and corporate actions; `massive.com/pricing`, 2026-10-10):
- `GET /v2/aggs/grouped/locale/us/market/stocks/{date}`: OHLC for all US stocks on a date in one call, with
  `adjusted=false` documented as "not adjusted for splits";
- `GET /stocks/v1/splits`: each split's `execution_date`, `split_from`, `split_to` and `adjustment_type`
  (`forward_split`, `reverse_split`, `stock_dividend`);
- `GET /v3/reference/tickers/{ticker}?date=`: point-in-time `type`, `cik`, `composite_figi`, `share_class_figi`,
  `list_date`, `delisted_utc`, `primary_exchange`, `active`;
- a dividends endpoint under corporate actions (fields to be read from its documentation page).

**Decision (supervisor, under the 2026-10-08 delegation): slice F's v3 takes forward raw closes, split events,
dividends, security type, CIK link, listing date and identity from Massive**, captured daily by the pinned runtime
(one grouped call per session plus corporate-action and reference calls fit inside 5 per minute). eToro stays the
source of tradability and the anchor for parity; PWB and the eToro-factor machinery (F6–F9 below) are dropped. The
protocol sections (state machine, registry, witness, windows, outcomes, superset, A1, A2) carry forward with round 2's
protocol findings (1–10, 15–18, 47–51) still to apply. The two years of history also cover the 15-month share-basis
lookback that X6 could not.

**Blocked on one human action:** a Massive account and API key. Creating one accepts Massive's terms in the
operator's name, so the loop cannot do it. Until the key exists, v3's premises (coverage of delisted names in grouped
daily, the dividends contract, the call budget) cannot be measured, and the spec stays open. Raw round-2 findings:
`docs/research/3740-slice-f-ckpt1-findings.md`.

The v2 text below is kept as the record that round 2 reviewed. Parent: `docs/research/2026-10-10-3740-step3-vw-book.md` ("the
parent"), §"Forward accrual", whose §"Sealing invariants" (1–8) and §"Slice F's obligations" (1–12) this spec must
meet. The parent's rules stand unless a row of §"Declared exceptions" (X) or §"Amendments to the parent" (A) replaces
one; each such row is a decision this checkpoint reviews.

## What slice F is for

The step-3 book is confirmed on 24 forward months (parent premise 3). Every input to a forward formation or holding
month must be captured as it becomes available, bound so it cannot be replaced, and witnessed outside our control.
Slice F fixes, per input: the source, the rule that turns it into the parent's input, when it is acquired, how it is
bound and witnessed, how a failure ends, and what the dry run must show before the declaration.

It computes nothing about performance. No portfolio is formed, weighted or valued on any captured month before the
24th holding month's inputs are bound (invariant 7); instrument-level validation is allowed (§"Dry run").

**Design in one paragraph.** Decision prices come from eToro: each session's official closing print, verified per
instrument against an in-session price and the next session's dated copy of the same field; adjustment factors and
MAX's daily returns come from eToro daily candles fetched once at each formation. Filings data come from SEC bulk
files captured before the formation's close. Holding-month total returns come from PWB's `adj_close`, a Yahoo-derived
archive of the same family as step 1's Intrader `adj_close`, and termination and coverage come from our own daily
closes, which are survivorship-free because they are recorded as they happen. B1 comes from N-PORT and factors from
Ken French, each fetched by the capture itself. Every acquisition is written once, receipted, and logged in a public
append-only transparency log.

## Measured premises (2026-10-10, read-only)

`PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_sources` prints items marked **[probe]** with their
populations. Items marked **[doc]** cite documentation; **[code]** cite code.

1. **[probe][doc] Official closes, one request.** `GET /api/v1/market-data/instruments/history/closing-price`
   (`api-reference/market-data/get-historical-closing-prices.md`) returned 16,681 instruments. Each row has
   `officialClosingPrice` ("Most recent official closing price", **undated**) and `closingPrices.daily` ("Official
   closing price from the previous trading day", **dated**). The portal states no adjustment basis and no price basis.
   On Saturday 2026-10-10 `daily.date` was 2026-10-08 for 11,621 rows.
   - Against `price_daily.close` (Bid-built eToro candles, `etoro-api.md:322-324`) for 2026-10-09, over 10,972
     instruments: median ratio − 1 = +0.0002, p1 −0.0147, p99 +0.0537, 573 beyond ±2%. The two are different prices,
     and that comparison does not date or validate either (§F6 does).
2. **[code][doc] Candles are back-adjusted at fetch and carry no dividends** (`app/services/market_data.py:783-790`,
   `etoro-api.md:331-332`). The portal index lists no dividend or corporate-action endpoint.
3. **[probe] PWB** (`paperswithbacktest/Stocks-Daily-Price`, Hugging Face) publishes each monthly update as a git
   commit; the probe prints the commit list. Of the tradable type-5 instruments on eToro exchanges 4, 5 and 33, the
   probe prints how many map to the stored `@2026-09-09` series (5,247 of 6,664 on 2026-10-10). `close` is
   split-adjusted and `adj_close` adds dividends (`docs/proposals/etl/2026-10-04-3619-total-return-splice.md:27`); each
   update re-bases history (`docs/review-prevention-log.md:11895`). The stored capture labelled `@2026-09-09` names HF
   revision `c64377a3` (`…-total-return-splice.md:331`), which the commit list dates 2026-10-01, and its last bar is
   2026-09-09: the relation between a commit's time and its data's last bar is not established, so F9 selects commits
   by the data they contain, never by commit date alone, and the dry run measures the lag.
4. **[probe][code] SEC bulk files are overwritten daily** with no history (`app/services/sec_bulk_refresh.py:47-63`);
   the probe prints their sizes and mtimes.
5. **[doc] EDGAR company record** (`submissions` JSON) carries `sic`, and its filing list carries `form`,
   `acceptanceDateTime` (UTC) and `accessionNumber` (`sec-edgar.md:59, 74, 1072-1074`). Exchange certifications of a
   listing are EDGAR form types (`CERTNYS`, `CERTNAS`, `CERTARCA`, `CERTAMEX`, `CERTBATS`).
6. **[probe][code] Instrument type** is eToro `instrumentTypeID` (5 Stocks, 6 ETF; `etoro_instrument_types`); no
   structured eToro field marks preferred, warrant, unit or ADR (the probe prints the name-marker count).
7. **[code] Connections:** 27 usable, demand 24 + reserve 3 (`app/db/pg_settings.py:127-230`); a daemon lane adds 2.
8. **[code] B1 and factors:** `sec_nport_monthly_returns` has no scheduled writer; `french_reference_refresh` appends
   a snapshot per content change (`app/services/reference_data.py:1051-1176`).
9. **[doc] Rekor** (Sigstore's public transparency log): append-only, no update or delete operation in its API, each
   entry with a server-set `integratedTime` and an inclusion proof; search by public key is offered as
   "EXPERIMENTAL … best effort only" (`openapi.yaml`, `/api/v1/index/retrieve`).

## Runtime

- **One process outside the jobs daemon.** A launchd agent `com.ebull.forward-3740` runs
  `scripts/forward_3740_capture.py tick` every 10 minutes from a dedicated worktree at a git tag. The daemon runs
  `~/Dev/eBull` at `origin/main`, which changes with every merge, and a daemon lane would break the connection budget.
- **Tick preflight, in order; any failure is an integrity failure (retryable, no acquisition):** the worktree HEAD
  equals the pinned tag and is clean; `uv.lock`'s sha256 equals the pinned one; a read-only checkout of `origin/main`
  is fetched (the ledger checkout) and its commit recorded; the `information_schema` hash over §"Schema interface"
  equals the pinned one (`SCHEMA_DRIFT` otherwise); free disk under the capture root exceeds twice the largest
  artefact of each due kind (10 GB before the first). Every receipt records the code tag commit, the construction
  hash, the lock hash, the schema hash and the ledger commit.
- **One tick at a time** (`flock`), idempotent; due work runs in the fixed order of §"Windows".
- **Database:** at most one connection, opened per step, from the 3-connection reserve. A refused connection is a
  retryable failure inside the window.
- **eToro quota:** 30 requests per minute by the capture's limiter. This does not reserve capacity under the shared
  120/60 s quota, so the plumbing test measures completion time for a full formation's candle fetch while the daemon
  runs, and the dry run must complete every formation fetch inside its window (§"Dry run").
- **SEC:** at most 2 requests per second, with the SEC User-Agent, a partition of the shared 10/s as
  `scripts/build_2282_form25_register.py` partitions it.

## Acquisition, binding and witness

### Attempt state machine and the first-observation rule

Every acquisition is an attempt with a fresh `attempt_id`:
1. `started`: `record_holdout_access` (`strategy_id="3740-step3-book"`, or `"3740-slice-f-dryrun"` in the dry run,
   `access_kind="read"`, `result_version=attempt_id`) is committed before the first source request (invariant 8).
2. `persisted`: each response's raw bytes are written with exclusive create and fsynced under
   `<RESEARCH_ROOT>/forward_3740/<trial>/<kind>/<period>/<attempt_id>/`, before any parsing.
3. `receipted`: one row in `forward_capture_receipts` per attempt, with every component's sha256 and its
   **validation class** from a frozen per-kind validator: `valid`, `transport_defect` (non-200, truncated or
   unparseable body, empty body) or `row_defect` (a parseable response whose individual rows fail a rule).
4. `witnessed`: the receipt is logged in Rekor (§"Witness").

**First observation binds.** For each binding identity, the first `persisted` response whose class is `valid` or
`row_defect` is the observation; a `row_defect` row stays invalid and is never re-fetched. Only a
`transport_defect`, or an attempt that ended before `persisted` (classified `abandoned` by step 2's abandoned-run
rule), permits another fetch inside the window. Publication failure never permits re-acquisition: a receipted
observation that misses its witness deadline is final and binds nothing (§"Outcomes").

### Binding registry

Identity (invariant 3) = `(trial, kind, source_id, period_key, component)`; a partial unique index on it over receipts
with a `valid` or `row_defect` component makes a second binding a database error, and the witness check
(§"Witness") makes it a verdict refusal.

| kind | source_id | period_key | component (granularity) |
|---|---|---|---|
| `etoro_listing` | `etoro:/market-data/instruments` + `/instrument-types` + `/exchanges` | session date | one per endpoint (whole response) |
| `etoro_rates` | `etoro:/market-data/instruments/rates` | session date | one per request batch of ≤ 100 ids, ids listed in the receipt |
| `etoro_close` | `etoro:closing-price` | session date | the whole response |
| `etoro_candles` | `etoro:candles/OneDay/1000` | formation month M | one per instrument |
| `sec_bulk` | `sec:companyfacts.zip`, `sec:submissions.zip` | session date | one per CIK member (companyfacts JSON; every submissions page) |
| `form25` | `sec:archives` | accession | the filing's complete submission text |
| `pwb_vintage` | `hf:paperswithbacktest/Stocks-Daily-Price@<commit>` | commit SHA | one per data file, plus the commit metadata |
| `nport_quarter` | `sec:nport-data-sets` | dataset quarter | the zip |
| `french` | `french:<dataset>` | response sha256 | the file |
| `jkp_cutoffs` | `jkp:return_cutoffs.csv` | response sha256 | the file |
| `formation_manifest` | derived | formation month M | the manifest |
| `month_manifest` | derived | holding month | the manifest |

Derived bindings reference the receipts they read by `receipt_sha256`.

### Witness (invariant 5)

- **Rekor.** Each receipt is logged as a `hashedrekord` entry over `receipt_sha256`, signed with an Ed25519 key
  generated for this trial (a separate key for the dry run), whose public key is pinned in the declaration. Rekor's
  `integratedTime` is the witnessed time; the entry's UUID and log index are stored in the next receipt.
- **Chain.** Each receipt carries `prev_receipt_sha256` and the previous entry's log index, from a genesis receipt
  pinned in the declaration.
- **Verdict checks:** every receipt has a Rekor entry whose inclusion proof verifies and whose `integratedTime` is
  before its witness deadline; the chain is unbroken; and the entries Rekor's key index returns for the trial key are
  exactly the chain's entries. An entry with no matching receipt, two receipts for one identity, or a chain break
  refuses (`CAPTURE_AMBIGUOUS`). Rekor cannot delete or edit an entry, so a deleted or discarded receipt is visible
  as an unmatched entry.
- **Limit.** The key index is experimental. If it is unavailable at the verdict, the run retries for 30 days and then
  reports `WITNESS_ENUMERATION_UNAVAILABLE` as an annotation beside the verdict: the chain and inclusion checks still
  hold, but a discarded branch would not be seen.
- **Drills** (dry run, on the dry-run key): a publication failure past its deadline, a duplicate log of one receipt,
  a second receipt for one identity, a receipt deleted locally after logging, and a broken chain; each must produce
  the outcome in §"Outcomes".

### Windows (America/New_York; sessions and early closes from `app/services/market_calendar.py` at its pinned `RULE_SET_VERSION`)

Every window has an absolute start and end computed from the calendar; the witness deadline is the window end plus
the stated lag. An acquisition not persisted by the window end is final for that window (invariant 4).

| kind | window, per occurrence | witness lag | retry |
|---|---|---|---|
| `etoro_listing`, `etoro_rates` | session d: 13:30 to close − 20 min (early close: 11:00 to close − 20 min) | 15 min | every tick until persisted |
| `sec_bulk` | s(M) and the two sessions before it: 09:30 to close − 20 min | 15 min | every tick |
| `etoro_close` | session d: close + 60 min to 08:00 on the next session's date | 60 min | every tick |
| `etoro_candles` | s(M): close + 60 min to 08:00 on the next session's date; work order: superset ids ascending | 60 min | per instrument, every tick |
| `form25` | daily at 06:00 for accessions accepted the previous calendar day, through the returns cutoff of the last holding month | 24 h | daily until persisted |
| `pwb_vintage`, `french`, `jkp_cutoffs`, `nport_quarter` | polled daily at 06:00 from the dry run's start to the nine-month limit; a new upstream version is acquired by 23:59 of the day it is first seen | 24 h | every tick that day |
| `formation_manifest`, `month_manifest` | from the close of the tenth session after the month's last session to the close of the fifteenth (statuses); returns components as their inputs bind, to the nine-month limit | 24 h | every tick |

The **nine-month limit** is 23:59 on the last calendar day of the ninth month after the 24th holding month.

**Pre-close fallback.** A pre-close kind missed on s(M) uses the latest binding of that kind from the five sessions
before s(M) (parent round 3, decision 2), recorded in the formation manifest with its age; none → the formation is
missed with that kind's reason.

### Outcomes

| event | effect |
|---|---|
| a `transport_defect` or abandoned attempt | another attempt in the window |
| a window ends with nothing persisted | that period has no binding from the window (final) |
| a receipted observation misses its witness deadline | binds nothing; final; the chain continues through it |
| a pre-close or `etoro_close`/`etoro_candles` binding missing for s(M), after fallback | `formation_missed` (reason: the kind), recoverable under the parent's `ACCRUAL_GAPS` count |
| `b1_entry_close` unbound at the end of s(M_0)'s `etoro_close` window | terminal `REFUSED` (`B1_UNDEFINED`) written to the ledger at once; all trial captures stop |
| a returns, B1, factor or cutoff input unbound or invalid at the nine-month limit | terminal `REFUSED` (`INPUT_UNAVAILABLE`) |
| a formation input bound but failing its witness check at the verdict | that formation missed (counts toward `ACCRUAL_GAPS`) |
| a returns, B1, factor or cutoff input failing its witness check at the verdict | `CAPTURE_AMBIGUOUS` |
| a chain break, an unmatched Rekor entry, or two receipts for one identity | `CAPTURE_AMBIGUOUS` |
| a preflight failure | retryable integrity failure; never a verdict |

### Acquisition superset (the parent's required set)

Books are not formed before the verdict, so holdings are unknown during capture. The capture therefore covers a
superset that contains every possible holding:
- **Instruments:** every instrument in any bound `etoro_listing` since the dry run's start with `instrumentTypeID` 5
  on exchanges 4, 5, 20 or 33, plus SPY and IVV; **once in, never out**, until the instrument's closes have been
  absent from 20 consecutive `etoro_close` responses after a Form 25 or a delisting from the listing.
- **CIKs:** every CIK named by F13's candidate map for a superset instrument.
- `etoro_close` covers all instruments in one response; `etoro_rates`, `etoro_candles` and `sec_bulk` cover the
  superset.

### Lookback (amendment A2)

A formation reads sessions before s(M_0): MAX's boundary month, the share-basis sessions of F5 and listing history.
Dry-run bindings of `etoro_close`, `etoro_rates`, `etoro_listing` and `sec_bulk` for sessions before s(M_0) are
**admissible lookback inputs**: they are pre-M_0 decision data, not outcome months, and are verified by the same
witness checks on the dry-run key. The declaration pins the dry-run chain head. The parent's "never inputs to the
trial" (round 1 of this spec) is replaced by this rule. Dry-run `etoro_candles` are never used (each formation
fetches its own).

## Input rules

Each entry: source, rule, sealing category (parent invariant 1), refusals, and the dry-run test.

### F1. Security type (obligation 1)

- **Structured fields first:** eToro `instrumentTypeID`; SEC's cover-page XBRL `dei:Security12bTitle`,
  `dei:TradingSymbol` and `dei:SecurityExchangeName` per Section 12(b) class (Form 10-K and 10-Q cover requirements,
  2019 onward). No eToro name text is used.
- **Source:** the bound `etoro_listing` (pre-close metadata) and the cover facts of the CIK's latest 10-K- or
  10-Q-family accession accepted before s(M), read from that accession's XBRL instance, fetched within the
  `sec_bulk` window of the session after its acceptance and bound under `sec_bulk` (one component per accession), so
  the bytes read are those available before s(M)'s close.
- **Rule, in order:**
  1. `instrumentTypeID` 5, else `type_not_stock`; exchange tag 19 (OTC) → `otc`.
  2. A cover class whose `dei:TradingSymbol` equals the eToro symbol **exactly** (upper case; eToro's `.` and SEC's
     `-` or `.` for class suffixes mapped by the frozen table in the code, collisions refused as `symbol_collision`)
     and whose `dei:SecurityExchangeName` is NYSE, NASDAQ, NYSEAMER, NYSEArca or CboeBZX; else `type_unknown`.
  3. The class's `dei:Security12bTitle` against a frozen keyword table, rejections first: preferred, depositary
     (ADR/ADS), warrant, right, unit, note, debenture, bond, beneficial interest, partnership or LLC interest →
     `type_rejected`; then common stock, common shares, ordinary shares or capital stock (any class letter or series)
     → accepted; else `type_unknown`.
  4. Funds and pools: the CIK's company-record SIC (F3) 6221, 6722 or 6726 → `type_fund`.
- **ADRs** are rejected (JKP keeps CRSP share codes 10, 11, 12; ADRs are 31).
- **Duplicates:** two superset instruments matching one (CIK, class) are one security; the one on exchange tag 4 or 5
  is kept over 33, over 20; a remaining tie refuses both (`duplicate_instrument`). Every pair is listed in the dry run.
- **Parser:** edgartools' XBRL cover reader if it reproduces a frozen fixture set (each keyword class, each exchange
  code, inline and plain XBRL); otherwise a parser of the XBRL instance, citing what it was compared against.
- **Dry run (pass/fail):** the class table over the whole superset at every dry-run formation, and a hand adjudication
  of **every** `type_unknown`, `type_rejected`, `type_fund` and `duplicate_instrument` row against the filing's cover
  page, recorded in a committed file before the declaration. Pass: no adjudicated row contradicts its class.

### F2. Listing age (obligation 2; exception X8)

- **Source:** the bound `sec_bulk` submissions pages (filing list with `acceptanceDateTime`), and the certification
  documents they name (immutable accessions).
- **Rule:** listing start = the acceptance date of the earliest exchange certification (`CERT*`) for the CIK whose
  certified class classifies as common under F1's keyword table and whose symbol matches; else the earliest Form
  8-A12B for such a class. Eligible when listing start ≤ s(M) − 36 months.
- **Missing history (X8):** listings older than EDGAR's certification filings have neither. For those, listing start
  = the first bar of the identity-mapped PWB series (F9), a security-level trading history, accepted only when F13's
  link holds over the whole interval (no other CIK linked to the symbol by any cover in it). Else
  `listing_age_unknown`: not eligible to enter, counted.
- **Dry run:** counts by branch at each formation; for every name where both EDGAR evidence and PWB's first bar exist,
  the two dates side by side, with disagreements beyond 12 months listed and adjudicated. Pass: no adjudicated
  disagreement in which the PWB date understates the listing age by more than 12 months.

### F3. SIC (obligation 3; exception X1)

- **Source:** `sic` in the CIK's company record, from the `sec_bulk` submissions binding used for s(M) (pre-close
  metadata, bound before s(M)'s close).
- **Rule:** step 1's codes: missing or empty `sic` → `sic_null`; CIK absent from the binding → `sic_unloaded`; else
  the four digits, for the REIT exclusion (6798), F1's fund rule and the FF-12 map.
- **Exception:** step 1 read SUB `sic` on the latest 10-K/10-Q accession. SUB publishes a quarter later, and an
  accession's header can change after acceptance (EDGAR PDS specification, post-acceptance corrections), so neither
  can be read point-in-time at s(M). The company record captured before s(M)'s close can. The adoption is frozen here;
  the comparison is reported, never used to switch: for every CIK in the first dry-run `sec_bulk` binding with an
  accession in step 2's SUB quarters (2021q3..2024q3), the record SIC beside the SUB SIC of its latest accession
  there, with the count of disagreements. Disagreements include genuine SIC changes since 2024, which the dry run
  cannot separate; the count is an upper bound on the source effect.

### F4. Accounting (obligation 4)

- **Source:** `sec_bulk`: in its window, the capture reads the zips `sec_*_bulk_refresh` last renamed into place,
  records each zip's sha256, ETag and mtime, and binds the zip members for every superset CIK. A zip whose mtime is
  before 00:00 on the session's date, or that changes during the read (sha256 before and after differ), is a
  `transport_defect`. The 3 GB zips are not retained; the members are the bound bytes.
- **Rule:** step 1's `pit_fundamentals` bundle built from those members by step 1's code with only its input reader
  changed: evidence cutoff (acceptance New York date strictly before s(M)) and the four-month lag. The formation
  manifest records which session's binding was used and the filings accepted after it and before s(M) that it
  therefore lacks (from the `form25` daily accession feed's index read), as the fallback's cost.

### F5. Shares and ME (obligation 5; exception X6)

Step 1 §"Market equity" on the F4 bundle: cover-count precedence, the 15-month age limit, blocked reads, context-date
and acceptance-date bases, no share lag. The split product over (basis, s(M)] is replaced by F7's factor:
ME = shares × F(b → s(M)) × P(s(M)), with b the last session on or before the basis date and P the F6 close. A
basis before the lookback's first `etoro_close` binding leaves ME missing (`basis_before_lookback`), counted.

### F6. Raw close at a session d (obligation 6; exception X2)

- **Source:** `etoro_close` (the decision session's price package for d, bound before the next open), the next
  session's `etoro_close`, and `etoro_rates` for d (in-session, pre-close).
- **Rule, per instrument, all three required:**
  1. **Rolled:** in d's response, `closingPrices.daily.date` is the session before d; the undated
     `officialClosingPrice` is then the candidate close for d.
  2. **Dated confirmation:** in the next session's response, `closingPrices.daily.date` is d and its price equals the
     candidate to 1e-9 relative, or differs by exactly the factor F7 measures between the two responses' candles (a
     re-base at the next open). Otherwise `close_unconfirmed`. A missing next-session response → `close_unconfirmed`.
  3. **In-session anchor:** the latest `etoro_rates` bid or last execution for the instrument in d's pre-close window
     lies within a factor of 1.15 of the candidate. A tradable price at d cannot be on a later corporate-action basis,
     so a candidate pre-adjusted for the next open fails this check whenever the action's factor exceeds 1.15.
     Otherwise `close_unanchored`.
- **Validity:** a close meeting 1–3 is valid at d; otherwise the instrument has no valid close at d. A price ≤ 0 is
  invalid.
- **Exception (X2):** step 2's closes were Intrader's raw trade closes. The forward close is eToro's official close,
  whose basis is undocumented; the rule above verifies date and basis per observation instead of relying on a vendor
  contract. Residual: a pre-adjustment by a factor under 1.15 (stock dividends, small splits) passes; F7's dry-run
  event table measures how often such actions occur.
- **Dry run (pass/fail):** per ordinary session, the share of superset instruments with a valid close (pass ≥ 99%);
  every F7 event over the dry run with the official closes on the sessions around it, showing whether any
  pre-adjustment occurred (pass: none passed the anchor).

### F7. Adjustment factors (obligation 7; exception X6)

- **Source:** `etoro_candles` fetched in s(M)'s post-close window (1,000 OneDay bars, back-adjusted to s(M)), and F6
  official closes bound in the lookback and accrual.
- **Factor:** F(b → s(M)) = med_b / med_M, with med_b the median over the five sessions ending at b of
  (official close at the session / candle close for that session in s(M)'s fetch), and med_M the same over the five
  sessions ending at s(M). Both medians need ≥ 3 valid F6 closes. Bias between the official print and the Bid candle
  cancels in the ratio. |ln F| < ln 1.05 → F = 1; otherwise F is applied as a share multiplier.
- **Why this selection is not circular:** F measures the vendor's own adjustment between two observations of one
  session, not a price move (`docs/review-prevention-log.md`, "A register SELECTED on the symptom under test").
- **Classification (X6):** eToro's adjustment is assumed to be splits and stock dividends only. The dry run tests
  that assumption: every F ≠ 1 at every dry-run formation, over the superset, adjudicated against the issuer's 8-K or
  press release and recorded in a committed file. Pass: every event is a split or stock dividend with the matching
  ratio to 2%. A non-split event fails the dry run and the rule is revised by amendment.
- **Inconsistent windows:** fewer than 3 valid closes in either median window, or a spread of the five ratios beyond
  2% inside either window → `factor_unresolved`, ME missing, counted.

### F8. MAX inputs (obligation 8; exception X3)

- **Rule:** #3621's MAX at s(M) on daily returns from s(M)'s `etoro_candles` fetch (one adjustment basis across the
  month by construction): close-to-close returns over the sessions of s(M)'s calendar month up to s(M), the previous
  month's last session as the boundary bar; ≥ 15 returns; < 10 zero returns; returns only between bars on adjacent
  sessions; #3621's return screen (−0.9, 3.0).
- **Exception (X3):** #3621 used Intrader daily total returns on trade closes, with a ratio screen on unstamped
  `adj_close/close` jumps. Forward MAX uses Bid-candle price returns, and the ratio screen becomes: a name whose F
  factor between s(M − 1) and s(M) is `factor_unresolved` is screened (flagged), keeping #3621's precedence (screened
  names flagged before the missing-value rules).
- **Measurement before planning (parent slice 2):** on stage A and stage B, at every formation, #3621's MAX three
  ways: as frozen; with price returns in place of total returns; and with the ratio screen removed. Printed per
  formation: the cutoff, the flagged-set size and the symmetric difference against the frozen set. The parent's
  planning uses #3621's MAX as frozen, so this measures the forward estimand's departure, and the declaration records
  the figures.

### F9. Holding-month returns (obligation 9; exception X4)

- **Status first** (F10) from F6 closes: `observed` when the instrument has a valid close at the month's last
  session.
- **Total return of an `observed` holding** for month m: PWB `adj_close` at the month's last session over `adj_close`
  at the previous month's last session, minus 1, from the **first** PWB commit (by commit time) whose data include a
  bar for the month's last session for at least 90% of the superset's mapped series. That commit binds for every
  name-month in m; no later commit is read for m.
- **Identity mapping** (§"Splicing"): eToro instrument ↔ PWB series by exact symbol, effective over the month, with
  one PWB series per symbol, confirmed by price reconciliation after basis conversion: the ratio PWB `close` /
  eToro candle close (from s(M + 1)'s fetch, both adjusted to near the same date) has a median within 0.98..1.02 and
  a spread under 2% over the month's sessions. A failed or ambiguous mapping is `pwb_unmapped`.
- **Entry eligibility:** a name is eligible to enter at s(M) only if mapped in the latest PWB commit bound before
  s(M)'s close (amendment A3).
- **Exception (X4):** a held `observed` name that the binding commit lacks or cannot map takes the F6 price return
  over the month (official closes, scaled by F between s(M) and s(M + 1)), flagged `dividend_unavailable` and counted
  per month. The parent requires a valid total return; the alternative, `INPUT_UNAVAILABLE` for the whole trial
  because one name lost vendor coverage, ends the trial on a data event unrelated to the strategy. A3 makes this
  residual case rare; the dry run measures it.
- **Independent reference** (frozen before comparing; dry run only): for every mapped name-month, PWB's monthly total
  return beside the F6 price return plus dividends from SEC records where available. Pass: median absolute difference
  under 0.25 pp and no more than 1% of name-months beyond 2 pp, else PWB is rejected as the returns source before the
  declaration.
- **Cutoff:** the PWB commit may post-date the tenth-session cutoff. Holding returns inform no decision, so a later
  vendor view of a past month is not look-ahead; the commit is bound when first seen.

### F10. Statuses and terminations (obligation 10)

- **Coverage** is our own: an instrument's closes stop when F6 yields no valid close at the month's last session.
  This record is survivorship-free because it is captured as the sessions happen.
- **Termination evidence:** Form 25 and 25-NSE filings (`form25` kind, accessions accepted by the month's returns
  cutoff), parsed and matched to the security by `scripts/build_2282_form25_register.py`'s rules (dual CIK indexing,
  debt-lifecycle exclusion, the class named in the filing, the provision); the provision gives step 1's
  `classify_termination` class.
- **Rule:** step 1's classifier: `terminal` when a matched Form 25 exists, with step 1's terminal value fractions for
  both arms applied to the return through the last valid close; `coverage_exit` otherwise, at the last valid close,
  an interior gap included.
- **Partial-month return to the last valid close:** PWB `adj_close` through that session where the binding commit has
  it, else the F6 price return (flagged as in X4).

### F11. B1 (obligation 11)

- **Returns:** the `nport_quarter` kind downloads each new quarterly N-PORT data set itself, parses it with
  `load_3619_nport_returns`'s parser from the pinned tree, and, for each forward month, binds IVV's (`C000012040`)
  Item B.5.a return from the filing with the earliest acceptance in the first data set that contains the month. Two
  filings with that acceptance time, or two values for the month in that filing, refuse the month (`B1_AMBIGUOUS`,
  terminal at the nine-month limit as `INPUT_UNAVAILABLE`).
- **Bands:** SPY's F6 close at s(M_0) is bound under the key `b1_entry_close` (from M_0's `etoro_close`; its dated
  confirmation arrives the next session, and the binding is valid only when it passes F6), and its exit close at
  s(M_0 + 24) likewise.

### F12. Factors (obligation 12)

The `french` kind downloads the five-factor and momentum monthly files itself, parses them with the pinned
`reference_data` parser, and binds, per dataset, the first download whose observations cover all 24 forward months.
Step 2's checks (unit, completeness) apply at binding and are re-verified at the verdict. The daemon's
`reference_data_snapshots` rows are not read.

### F13. CIK link (population step 2; exception X7)

- **Rule:** an instrument links to CIK c at s(M) when c's latest 10-K- or 10-Q-family accession accepted before s(M)
  has a cover class matching the instrument under F1 rule 2, and no other CIK's latest periodic cover in the bound
  data matches the same symbol (`link_ambiguous` otherwise). Candidate CIKs come from SEC's `company_tickers_exchange.json`,
  bound daily in `sec_bulk`'s window, and `external_identifiers`; the cover decides.
- **Exception (X7):** step 1 linked on Form 3/4/5 `issuerTradingSymbol` evidence from the quarterly Insider
  Transactions Data Sets, which do not contain the current quarter's filings at a monthly formation. The cover is
  the issuer's own statement of its trading symbol, structured and point-in-time by acceptance.
- **Dry run:** at each formation, the cover link beside step 1's linkage on the latest published data sets, counts
  of agreement and each disagreement listed.

### F14. Return cutoffs (step 1 Amendment 3; exception X9)

The `jkp_cutoffs` kind binds each new `return_cutoffs.csv` when first seen. A holding month's clip uses that month's
row from the first binding containing it; if none exists at the month's returns cutoff, the latest month's row in the
latest binding (carry-forward). The clip is a containment for vendor errors, not an economic quantity, so a recent
month's bounds serve it. Step 1's per-row consistency check and refusals apply; raw and clipped values are both kept.

## Formation resolution (amendment A1)

The parent resolves a formation once the previous month's returns are bound, from the book's holdings. Holdings are
not formed before the verdict (invariant 7), so slice F splits the work:
- **During the accrual,** in the manifest window, `formation_manifest` binds the instrument-level inputs for the whole
  superset (F1–F8, F13, eligibility, MAX, ME, raw close, refusals by code, coverage) and `month_manifest` binds every
  superset instrument's status for the month ending at s(M) and, as they arrive, the return components. Population
  refusals that need no holdings (`SESSION_MISSING`, `ME_INVALID`, `UNIVERSE_SHORT`, a missing pre-close kind) mark
  `formation_missed` at once.
- **At the verdict,** the report forms the paths from those bindings and applies the parent's holding-dependent rule
  (`PRICE_INVALID` for a holding with no valid close that was not realised) deterministically. A formation missed
  this way counts toward `ACCRUAL_GAPS` exactly as if found during the accrual; the outcome is the same, only its
  discovery moves to the verdict.

## Splicing (invariant 6)

| element | identity mapping | adjustment basis | overlap reconciliation | vendor precedence |
|---|---|---|---|---|
| historical ↔ forward | none: the forward path starts all cash at s(M_0) | — | — | — |
| official close ↔ candles | eToro `instrumentID` | official nominal at d (F6); candles adjusted to s(M) | F7 medians | official for levels; candles for factors and MAX |
| eToro ↔ PWB | exact symbol over the month, one series per symbol | PWB adjusted to its commit | F9 ratio test | PWB for holding total returns; F6 for status and level |
| eToro ↔ SEC | F13 cover link | — | F13 dry-run table | cover link only |
| B1 | IVV class `C000012040` | NAV total return | step 2's identity gate | N-PORT only |

## Schema interface (pinned runtime)

Captured bytes live on disk. The tables a capture or the verdict reads or writes are `forward_capture_receipts`
(all columns) and `strategy_holdout_accesses` (`access_id`, `strategy_id`, `strategy_version`, `result_version`,
`access_kind`, `accessed_by`, `purpose`, `accessed_at`). Slice F's code holds that list in one constant, with a test
that the hashed `information_schema` slice covers exactly those columns.

## Declared exceptions

| id | parent rule | forward rule | why | dry-run evidence |
|---|---|---|---|---|
| X1 | SUB `sic` of the latest accession | company-record `sic` bound before s(M) | SUB lags a quarter; headers can be corrected after acceptance | F3 comparison (upper bound) |
| X2 | Intrader raw trade close | eToro official close, verified per observation | no other per-session raw close covers the population | F6 shares and event table |
| X3 | MAX on Intrader total returns with the ratio screen | Bid-candle price returns; `factor_unresolved` screen | no dividend source before s(M)'s close | F8 historical measurement |
| X4 | a valid total return for every observed holding | F6 price return, flagged, for a held name the PWB commit lacks | one vendor gap would otherwise end the trial | F9 counts |
| X6 | step 1's split stamps | F7's measured vendor factor; ME missing before the lookback | no dated split source with ratios exists | F7 adjudication |
| X7 | Form 3/4/5 symbol linkage | cover-page `dei:TradingSymbol` | the insider data sets lag a quarter | F13 table |
| X8 | — (parent names 8-A as candidate) | CERT*/8-A12B, else PWB first bar | pre-EDGAR listings have no certification | F2 table |
| X9 | the holding month's JKP cutoffs | carry-forward when unpublished | JKP publishes in arrears | counts of carried months |

**Amendments to the parent:** A1 (formation resolution, above); A2 (lookback inputs from the dry run); A3 (entry
needs a PWB mapping at s(M)). X5 of round 1 (tradability from the bound listing instead of
`instrument_universe_membership`) stands: the membership table is written by the daemon from the same endpoint and
is printed as parity only.

## Dry run (prospective)

At least three consecutive month-ends before the declaration, from the dry-run tag, trial `3740-slice-f-dryrun`, its
own Rekor key. A retrospective plumbing test on stored data validates code first and measures artefact sizes and the
candle-fetch completion time under the daemon's load. **Accepted only if every item passes:**
1. every window acquired and witnessed on time, or each miss explained by a logged transport defect;
2. F1 adjudication, F2 adjudication, F6 valid-close share and pre-adjustment table, F7 adjudication, F9 reference
   comparison, each against the pass rule stated in its section;
3. F3, F13, X4 and X9 tables printed (reported, not gated);
4. the parent's parity table;
5. the witness drills of §"Witness", each with the expected outcome;
6. a disk projection for 24 formations within the capture root's free space;
7. at least one F7 event and one Form 25 termination in the superset during the dry run; if none occur, the dry run
   extends month by month until both have (a quiet period cannot pass these checks vacuously);
8. no portfolio formed, weighted or valued: instrument-level checks only.

A failed item is revised by an amendment with its own checkpoint 1; if the revision changes a historically
reproducible input, the parent's slice 3 (planning) is re-run.

## Build slices

1. This spec and the probe (this PR).
2. Receipts, artefact store, attempt state machine, Rekor witness and the verdict-side checker, with the drills on a
   scratch key. Codex checkpoint 2.
3. Capture kinds and the F-rules with fixture tests; the manifests; the launchd agent. Codex checkpoint 2.
4. The F8 historical measurement (with the parent's slice 2).
5. Plumbing test, then the prospective dry run, posted on #3740.

## Known limits

- The official close's basis is verified per observation, not by vendor contract; a pre-adjustment under a factor of
  1.15 passes F6 (X2).
- Holding returns rely on PWB, a monthly Yahoo-derived republish; a held name it lacks earns price return only (X4).
- Forward MAX is on Bid-candle price returns (X3).
- Rekor's key enumeration is experimental (§"Witness").
- The witness shows tampering; it does not prevent it.

## Checkpoint log

**Round 1 (44 findings: 37 BLOCKING, 5 WARNING, 2 NIT; verdict "not fit to be built").** Revisions:
- **1, 5 (witness):** Rekor append-only log with a chained receipt, inclusion proofs and key enumeration; outcome
  table separating late witnesses, misses and ambiguity.
- **2, 3, 4 (protocol):** binding registry with component granularity and a unique index; attempt state machine with
  the first-observation rule; absolute windows, polling and the nine-month timestamp.
- **6:** acquisition superset, sticky, with SPY and IVV.
- **7:** lookback amendment A2.
- **8:** formation resolution amendment A1; no interim weights.
- **9:** `b1_entry_close` refusal at its window end stops the trial.
- **10:** N-PORT and French fetched and bound by the capture itself, with deterministic selection.
- **11:** tick preflight and receipt provenance fields; schema interface listed.
- **12:** cover-page link (X7).
- **13, 14:** F1 taxonomy with fund SICs, exact symbol matching, duplicates rule, full adjudication.
- **15, 16:** F2 from certifications and class-matched 8-A12B, PWB fallback declared (X8).
- **17:** SIC from the company record bound before s(M) (X1).
- **18, 19:** F6's roll, dated confirmation and in-session anchor.
- **20–25:** F7 measures the vendor factor between two observations of one session at each formation; FINRA removed;
  adjudication of every event.
- **26, 27:** X3 restated with a screen and a three-way historical measurement before planning.
- **28–33:** holding returns from PWB `adj_close` (the analogue of step 1's Intrader `adj_close`), no dividend
  extraction, deterministic commit selection, identity mapping after basis conversion, entry restricted to mapped
  names (A3), an independent reference with pass rules.
- **34, 36:** statuses and coverage from our own closes; partial-month returns defined.
- **35:** Form 25 matched by the #2282 register's rules.
- **37:** JKP cutoffs captured, with carry-forward (X9).
- **38:** premises labelled by evidence type; the probe extended.
- **39:** quota completion measured in the plumbing test and gated in the dry run.
- **40:** `sec_bulk` handoff rules.
- **41, 42:** dry-run pass rules, event coverage, instrument-level checks allowed.
- **43, 44:** FINRA removed; slice numbering made explicit.

**Round 2 (53 findings: 46 BLOCKING, 5 WARNING, 2 NIT; checkpoint 1 open).** Round 1 dispositions: 10 APPLIED
(22–24, 28, 29, 38, 42–44), 32 PARTIAL, 2 NOT APPLIED (27, 34). The source-validity findings (11–14, 26–46) led to
§"Route change after round 2"; the protocol findings (1–10, 15–18, 47–51) apply to v3 unchanged.
