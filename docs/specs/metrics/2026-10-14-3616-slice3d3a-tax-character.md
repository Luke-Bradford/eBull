# Tax character and the fund withholding gate (#3616 slice 3d-3a)

Source: #3616 slice 3d, split by 3d-1 (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-3a owns
slice-3 checkpoint-1 round-1 findings 17, 18, 30, 44 and 45 (text in `var/research/3616/ckpt1_slice3_r1_final.txt`,
loop worktree) and removes 3d-2's `character_pending`. 3d-1's 3d-3 row is split three ways (Not in this slice):
**3d-3a** (this) decides character and computes shares; **3d-3b** dividends; **3d-3c** offshore-fund disposals.

**What this slice decides and builds.** Each tax security (3d-2 Design 2) gets a *tax character* from captured SEC
evidence about the security itself: **share** or **unestablished**. A share is computed exactly as 3d-2 computes it;
its gains and losses are capital. Every other security is **withheld**, including every exchange-traded fund. A fund
cannot be computed with 3d-2's identification at all: an interest in a non-reporting fund is a *relevant security*,
outside the s104 pool and identified differently (Source rules), and a reporting fund's cost depends on reported
income nobody has ingested (finding 17). Both are 3d-3c's. Nothing is shown to the operator (3d-4).

**The outcome on dev today:** AAPL and NXH are shares; AAPL's 2026-09-23 disposal is a capital loss of
£0.00753863550700. **SPY, which 3d-2 computed, is withheld**: 3d-2 identified it as a share (same-day, then 30-day,
then the pool), which is the wrong identification if it is a non-reporting fund, as fact 5 indicates. Run 1's SPY
figures are superseded, not corrected; 3d-3c computes them.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3a_character.py`, read-only, run on 2026-10-14. It resolves the effective
matching run with `effective_tax_matching` (run 1, over evidence run 2), then in one REPEATABLE READ transaction reads
that sealed run's rows and the mutable inputs (each member's symbol, type and primary SEC CIK), checks afterwards that
the effective run is unchanged, fetches each CIK's SEC submissions JSON and the cover filings Design 2 names, and
applies Design 2 through one function, `classify`, which the build moves into the service. Output
`var/research/3616/slice3d3a/population.txt`; bodies with SHA-256 beside it (gitignored; no account identifier).
`--universe` applies the submissions-level lines (entity type, SIC) to every Stocks instrument with a primary SEC CIK
(tradable or not, variants included) from the local `submissions.zip` of `sec_submissions_bulk_refresh` (archive
2026-10-10); output `universe.txt`.

1. **Members** (`instruments.instrument_type_id`, eToro's: 5 Stocks, 6 ETF; `external_identifiers` provider `sec`,
   type `cik`, `is_primary`). AAPL (1001, 8754), NXH (1181), GME (1699, 8761), ILMN (4077): type 5. SPY (3000, 3417),
   IUSA.L (3075), CSPX.L (3434): type 6. Primary CIKs sit on base rows only: AAPL 0000320193, NXH 0001130713, GME
   0001326380, ILMN 0001110803, SPY 0000884394; none for IUSA.L or CSPX.L. No component has two types or two CIKs.
2. **SEC submissions** (`data.sec.gov/submissions/CIK##########.json`, 2026-10-14). `entityType` and `sic`: AAPL
   operating 3571; NXH operating 5961; GME operating 5734; ILMN operating 3826; SPY `other`, empty. Energy Transfer
   ("Energy Transfer LP") and Enterprise Products ("ENTERPRISE PRODUCTS PARTNERS L.P."), both partnerships, are
   `operating` too: **no submissions field states legal form or security class** (checked: `entityType`, `sic`,
   `stateOfIncorporation`, `category`, `name`).
3. **Inline-XBRL covers carry the listed class.** SEC Release 33-10618 (March 2019) requires the 10-K, 10-Q and 8-K
   cover to state the title of each class registered under Exchange Act s12(b) with its trading symbol, tagged in
   inline XBRL (phased in from fiscal periods ending on or after 2019-06-15); the tags are `dei:Security12bTitle` and
   `dei:TradingSymbol`, one context per class. Read
   2026-10-14: AAPL 10-Q 2026-07-31 (`0000320193-26-000020`) "Common Stock, $0.00001 par value per share" / AAPL, plus
   six untraded notes; NXH 8-K 2026-10-06 (`0001140361-26-038800`) "Common stock, $0.0001 par value per share" / NXH,
   and at the holding start its 8-K 2025-11-14 (`0001140361-25-042343`) "Common Stock, $0.0001 par value per share" /
   BBBY (the symbol before the 2026-08 rename); GME 10-Q 2026-09-09 "Class A Common Stock" / GME; ILMN 8-K 2026-10-13
   "Common Stock, $0.01 par value" / ILMN. Energy Transfer's 10-K 2026-02-19: "Common Units" / ET (probe script,
   `/tmp/3d3a_cover/`, not committed; the build's tests carry it as a fixture).
4. **Run 1 under Design 2** (Script): AAPL, NXH, GME, ILMN → share; SPY, IUSA.L, CSPX.L → unestablished
   (`exchange_traded_fund`). Run 1 figures: AAPL disposal 2026-09-23 (2026/27), 1 identification, gain
   −£0.00753863550700, pool 0 / £0; NXH pool 1305.05709600 / £6048.55403340328000, no disposal; SPY disposal
   2026-09-23, 2 identifications, pool 21.66720200 / £12537.70775155264395; `unresolved_subject` dependencies GME 2,
   IUSA.L 1, CSPX.L 3, ILMN 1.
5. **SPY and HMRC's list** (first Script version, commit `7bdadc76`, bodies in `var/research/3616/slice3d3a/
   20261014T070010Z/`): HMRC's "Approved offshore reporting funds" list (GOV.UK, `public_updated_at` 2026-10-12,
   125,022 share-class rows, ceased funds kept) has no row for SPY by ISIN US78462F1030, by CUSIP 78462F103 or by
   name; it lists CSPX (IE00B5BMR087, from 22/03/2010) and IUSA (IE0031442068, from 12/06/2010). No CUSIP or ISIN is
   bound to SPY, IUSA.L or CSPX.L in `external_identifiers`; SPY's CUSIP sits unbound in `unresolved_13f_cusips`.
   Recorded for 3d-3c; nothing here relies on it.
6. **Coverage of the submissions-level lines** (`universe.txt`, 5,339 instruments): `entityType` other 1,098 (by
   annual form: 20-F 963, 40-F 102, neither 33: foreign private issuers); SIC 172 (6770 blank check 121, empty 49,
   6799 2); `investment` 1; no inline-XBRL cover filing in `filings.recent` 15; 4,053 reach the cover test, which the
   universe run does not perform (it would fetch a document per issuer). This is coverage, not correctness.

## Source rules

- **Shares.** Shares are chargeable assets (TCGA 1992 s21); identification is 3d-2's (ss104–106A).
- **When a holding is not an offshore fund.** TIOPA 2010 s355 makes an offshore fund a *mutual fund* of one of three
  non-UK forms; s356 condition C requires that "a reasonable investor would expect to be able to realise all or part
  of an investment on a basis calculated entirely or almost entirely by reference to" net asset value or an index.
  HMRC IFM12237 (read 2026-10-14): a secondary-market price near NAV counts only "as a result of arrangements being in
  place", for example ETF market makers who "create or redeem units". The share of an operating company has no such
  arrangement: its price is set by trading, not by reference to net asset value or an index.
- **Why funds need their own slice.** Interests in a non-reporting fund are *relevant securities* (TCGA s106A(10)):
  they "do not form part of a Section 104 holding" (s104(3)) and are identified with acquisitions in the following 30
  days first-in first-out, then with earlier acquisitions last-in first-out (s106A; HMRC CG51565, read 2026-10-14).
  The gain is an offshore income gain where SI 2009/3001 reg 17's Condition A or B holds, equal to the basic gain
  (regs 38–39), nil if a loss (reg 42; IFM13550: the loss "can be relieved only as a capital loss"), and deducted from
  the consideration of the same disposal for chargeable gains (reg 45(3)). A reporting fund is pooled, but reg 99
  (IFM13372, read 2026-10-14) treats reported income charged under reg 94, including excess reportable income, as
  acquisition expenditure incurred on the fund distribution date, with income received after a disposal treated as
  received immediately before it, and full equalisation reducing the purchase price; cash distributions are income,
  not cost. This replaces round 1's IFM12300 citation, the non-reporting introduction (finding 44). Condition B also
  needs that an election under reg 48(2) was not prevented by reg 48(5) and not made, over a material period starting
  no earlier than 1 January 1984 (reg 17(3)–(4)); income-transparent funds have reg 29's exception (IFM13470).
- **Evidence of the listed class.** SEC Release 33-10618 requires the 10-K, 10-Q and 8-K cover to name each s12(b)
  class and its trading symbol, in inline XBRL (fact 3). No source states how to establish from SEC data that a class
  is a company's share and that the issuer is not a fund; Design 2 fixes that by construction from the cover title,
  the SEC entity type and the SIC code, fails closed, and is frozen under the policy id.

## Design

### 1. Evidence capture (job `tax_character_evidence_refresh`)

A daily job, through the shared SEC rate lane with `settings.sec_user_agent`:
1. fetches the submissions JSON of every primary SEC CIK (`external_identifiers`, provider `sec`, type `cik`,
   `is_primary`) of every member of every component the effective generation names (3d-2 Designs 1–2);
2. from each `ok` submissions capture, takes the cover filings Design 2 reads (the newest cover filing, and the newest
   one on or before the security's first admitted acquisition date) and fetches each primary document not already
   captured: `https://www.sec.gov/Archives/edgar/data/<cik>/<accession without dashes>/<primaryDocument>`. A filed
   document does not change, so each accession is fetched once.

A submissions fetch is `ok` when the status is 200, the body is at most 16 MiB, it parses as a JSON object, and its
`cik`, zero-padded to 10 digits, equals the requested CIK. A cover fetch is `ok` when the status is 200 and the body
is at most 64 MiB. Anything else is `failed`, recorded, and the job fails after attempting every fetch.

Storage (migration `sql/490_tax_character.sql`), append-only by trigger (no UPDATE, no DELETE):
- `tax_character_contents`: `content_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`; `sha256 TEXT UNIQUE NOT
  NULL`; `body_gzip BYTEA NOT NULL`. A body is stored once.
- `tax_sec_fetches`: `fetch_id` identity PK; `kind` `submissions` or `cover`; `cik TEXT NOT NULL` (10 digits);
  `accession TEXT` (non-NULL iff `cover`); `url TEXT NOT NULL`; `requested_at`, `observed_at TIMESTAMPTZ NOT NULL`,
  `observed_at ≥ requested_at`; `outcome` `ok` or `failed`; `http_status SMALLINT` (NULL allowed: no response);
  `content_id` FK, non-NULL iff `ok`; `error TEXT`, non-NULL iff `failed`.

### 2. Character (`tax_character_v1`; findings 18, 30)

Character is decided per **tax security**, from every member of its component (finding 30): SPY's CIK is on 3000 and
its lots on 3417, and both map to 3000. A member of an unestablished identity component (3d-2 Design 2) is its own
withheld security; its character is `unestablished`, `identity_unestablished`, without evaluation.

**Reading the evidence.** The run has one `as_of`, the clock reading taken right after its snapshot starts; selection
takes its own. A CIK's submissions capture is its `ok` fetch with the greatest `(observed_at, fetch_id)` in `[as_of −
7 days, as_of]`, or *none*. From it, validated, else the capture is *invalid*: `entityType` a string; `sic` a string;
`filings.recent` with `accessionNumber`, `form`, `filingDate`, `primaryDocument` and `isInlineXBRL` all lists of one
length, each element respectively a string, a string, a `YYYY-MM-DD` date, a string, and 0 or 1. *Cover filings* are
the rows with form `10-K`, `10-Q` or `8-K` and `isInlineXBRL` 1, newest first by `(filingDate, accessionNumber)`. A
cover document's *classes* are its `ix:nonNumeric` facts named `dei:Security12bTitle` and `dei:TradingSymbol`, grouped
by `contextRef`, each the element's text with tags removed, HTML entities decoded and whitespace collapsed; a title is
*common* when, upper-cased, it begins with `COMMON STOCK` or `ORDINARY SHARES`, optionally after `CLASS <letter> `,
followed by a comma, a space or the end. The build compares edgartools' XBRL reader with this element reader on the
fixtures and uses edgartools if it returns the same classes.

The *symbol* is the upper-cased `instruments.symbol` of the member carrying the CIK.

Evaluated in order; the first line that applies decides.

| character | condition | `detail` |
|---|---|---|
| unestablished | members have more than one `instrument_type_id` (NULL counts as a value), or more than one primary CIK | `member_conflict` |
| unestablished | the type is 6 (ETF) | `exchange_traded_fund` |
| unestablished | the type is not 5 (Stocks) | `instrument_type` |
| unestablished | no primary CIK | `cik_unestablished` |
| unestablished | no submissions capture | `sec_capture_unavailable` |
| unestablished | the capture is invalid | `sec_capture_invalid` |
| unestablished | `entityType` ≠ `operating` | `entity_type` |
| unestablished | `sic` not four digits, or one of 6722, 6726, 6770, 6799 | `sic` |
| unestablished | no cover filing, or the newest has no `ok` cover capture | `cover_unavailable` |
| unestablished | in the newest cover, not exactly one class has the symbol, or its title is not common | `listed_class_not_common_stock` |
| unestablished | the security has an admitted acquisition, and no cover filing is dated on or before the first one, or it has no `ok` capture | `holding_start_cover_unavailable` |
| unestablished | in that holding-start cover, no class title is common, or a class with the symbol has a title that is not common | `holding_start_class_not_common_stock` |
| **share** | otherwise | NULL |

- **Why these tests establish a share outside the fund regime.** The cover's s12(b) title is security-specific
  evidence that the listed class is the registrant's common stock (or ordinary shares): the equity of a company, not
  partnership units (Energy Transfer's "Common Units"), a trust's units or shares of beneficial interest, notes or
  warrants. `operating` with an operating SIC marks the registrant as an operating company; SEC's investment-vehicle
  codes (6722 open-end funds, 6726 unit trusts and closed-end funds, 6770 blank checks, 6799 investors not elsewhere
  classified) and an empty SIC are withheld. Such a share is priced by trading, with no arrangement to realise it by
  reference to net asset value or an index, so s356 condition C fails and it is not an offshore fund. No document
  states the absence of an arrangement, so this is assumption **A6**, carried as a reason (Design 3).
- **Over the holding (finding 18).** The class is checked at both ends: the newest cover (today's listing, paired by
  symbol) and the newest cover on or before the first acquisition (paired by symbol when the symbol was the same then;
  NXH's was BBBY, fact 3). A change of legal form between those filings, and back, is not detected; it is not
  plausible enough to be worth a filing-by-filing walk, and the walk is the next policy if a case appears. The CIK
  bound today is taken as the issuer throughout: a change of an instrument's issuer binding is an identity change
  of the security, governed by 3d-2's evidence identity. A share has no reporting status or election to track; those
  exist only for funds (3d-3c).
- A foreign issuer that files 10-K, 10-Q or 8-K covers and passes is a share by the same argument; foreign private
  issuers filing 20-F or 40-F are SEC `other` and withheld (fact 6).

**Withholding.** 3d-2 Design 3 gains the cause `character_unestablished`: exactly one dependency row for a security
iff its character is `unestablished`, naming the `detail`, alongside any other cause. Withholding stays
whole-security. Every security row gets a character row, withheld ones included: character classifies the security
named by an admitted lot or an unresolved subject; whether its lots are computed remains 3c's and 3d-2's gate
(finding 45).

### 3. Routing and reasons

A computed security is a share, so each statutory disposal is a chargeable-gains disposal: `gain_gbp` is the capital
gain or, when negative, the allowable capital loss. No new amount is stored; the character row says how every figure
of the security is taxed. `character_pending` is no longer emitted. Every disposal and every pool of a share carries
`offshore_fund_exclusion_assumed` (A6), as 3d-1's always-present reasons do; no evidence discharges it under this
policy.

### 4. Storage (migration `sql/490_tax_character.sql`)

- `tax_matching_runs` gains `character_policy_id TEXT` and `as_of TIMESTAMPTZ` (both NULL allowed: runs before this
  slice). The unique key becomes `(evidence_run_id, matching_policy_id, class_policy_id, character_policy_id,
  inputs_digest)` with `NULLS NOT DISTINCT`. `tax_matching_runs_seal()` is replaced so its compared tuple includes
  both new columns (3d-2's trigger lists columns explicitly, so a new column is otherwise unprotected while sealing),
  and its refused-run check includes `tax_matching_characters`.
- `tax_matching_dependencies.reason` admits `character_unestablished`.
- **`tax_matching_characters`**: `(matching_run_id, tax_security_id)` PK, FK to the security row; `character TEXT NOT
  NULL` (`share`, `unestablished`); `detail TEXT`, non-NULL iff unestablished, from the table's codes or
  `identity_unestablished`; `cik TEXT` (the single CIK, NULL when none or several); `submissions_fetch_id`,
  `latest_cover_fetch_id`, `start_cover_fetch_id BIGINT` FKs to `tax_sec_fetches` (each NULL when not read). It takes
  3d-2's child guard (insert only while its run is `building`; no update or delete). Publication requires exactly one
  character row per security row, checked in code before the seal.

**`inputs_digest`** document `matching_inputs_v2`: v1's document plus `"character": [[tax_security_id, types, ciks,
symbol, sec], …]` sorted by security; `types` and `ciks` sorted lists (a NULL type as `null`). `sec` is `["none"]`
when there is not exactly one CIK, `["unavailable"]` with no capture, `["invalid"]` for an invalid capture, and
otherwise `["ok", entityType, sic, latest, start]`, where `latest` and `start` are `null` (none dated, or no `ok`
capture) or `[accession, classes]`, `classes` the `[contextRef, title, symbol]` triples sorted, absent values as
`null`. The extracted facts enter, not content hashes: a new Form 4 changes SEC's submissions document but no fact,
and makes no new run; a new 10-Q changes `latest` and makes one; a capture going stale changes `sec`.

**Selection.** `effective_tax_matching` computes the digest with its own `as_of` and also requires
`character_policy_id = TAX_CHARACTER_POLICY_ID`. Run 1 (NULL) is not selected; a capture gone stale since the last run
makes the current digest differ, so the result is *unavailable* until the next run, never the old run.

### 5. The job and the run

`tax_character_evidence_refresh` is registered daily before `tax_broker_evidence_refresh` (12:13 UTC), at a minute
off the :15/:30 connection peaks, so the matching inside the evidence job reads fresh captures. `run_tax_matching`
reads the character inputs inside its REPEATABLE READ snapshot (3d-2 Design 10), sets `as_of`, and decides characters
before identification; a withheld security is never identified. `classify` is pure and returns `(character,
detail)` with `detail` None for a share.

## Local policy (frozen under `TAX_CHARACTER_POLICY_ID = "tax_character_v1"`)

The rule table and its order, the validation of the submissions fields, the cover forms and the common-title grammar,
the SIC set, the 7-day window, the two-ends holding check, the single-CIK issuer assumption, and A6. Changing any is a
new policy id, which the run key admits as a new run for the same generation.

## Not in this slice

- **3d-3c, offshore-fund disposals** (finding 17's ERI gate is lifted there, not here): relevant-securities
  identification for non-reporting funds (s104(3), s106A; 30-day FIFO then LIFO, and whether same-day aggregation
  applies), separate disposals rather than a daily netted one before reg 42's floor, OIG routing (regs 17, 18, 38–45),
  reporting status per share class over the holding from HMRC's list (fact 5) with identifier validation, ERI and
  equalisation for reporting funds (reg 99), reg 29 transparency, Condition B and reg 48/100 transitions. Prerequisite
  inside it: binding SPY's CUSIP (fact 5). It is buildable now and is the next tax slice after 3d-3b.
- **3d-3b, dividends:** their income treatment, the evidence that one was paid (sync `totalFees` combines overnight
  fees and dividends in one number, so a zero does not prove no dividend), and an admission policy for orders whose
  observed `totalFees` is non-zero, which 3c's gate makes `unresolved`. Planning deadline: before SPY's December 2026
  ex-dividend date.
- Foreign private issuers, partnerships, LLCs, trusts and blank checks (withheld); 3d-4's view.

## Tests

Pure, on `sec_facts` validation, the cover reader, `classify` and the digest document:
1. **Each table line alone**, in order: two types; a NULL type beside 5; two CIKs; type 6; type 4; no CIK; no capture;
   invalid captures (integer `sic`, `isInlineXBRL` 2, unequal list lengths, `filingDate` "2020-01-01X"); `other`;
   SIC 6770, 6722, 6726, 6799, "", " 3571"; no cover filing; a cover without an `ok` capture; a cover with two classes
   carrying the symbol; titles "Common Units" (Energy Transfer fixture), "Shares of Beneficial Interest", "Warrants to
   Purchase Common Stock", "Common Shares"; a security with an acquisition and no cover on or before it; a start cover
   whose only common class is absent; a start cover pairing the symbol with "Common Units".
2. **Shares:** "Common Stock, $0.00001 par value per share" (AAPL fixture), "Class A Common Stock" (GME),
   "Ordinary Shares", and NXH's two covers (NXH today, BBBY at the start) give `share` with detail None.
3. **Cover reader:** attributes in either order; nested tags; `&#160;`; a fact without `contextRef` ignored; the
   untagged notes of AAPL's cover carry no symbol.
4. **Keying (finding 30):** a CIK on the base and lots on its `.RTH` variant give one character for the base; an
   unestablished identity's members get `identity_unestablished`.
5. **Freshness:** a capture exactly 7 days old is read, 7 days and 1 µs is not; two captures at one `observed_at`
   take the greater `fetch_id`; a capture after `as_of` is not read.
6. **Digest:** a new capture with identical facts gives an identical document; a new cover accession, a changed
   title, `sic` or `entityType` changes it; stale, invalid and absent captures give three different `sec` values; the
   bytes and SHA-256 of a pinned fixture.

DB:
7. **Capture:** `ok` and `failed` rows (a 404, a mismatched `cik`, a 16 MiB + 1 byte submissions body, a 64 MiB + 1
   byte cover); a repeated body stored once; a captured accession not fetched again; UPDATE and DELETE refused.
8. **Seal:** changing `character_policy_id` or `as_of` in the sealing update is refused; a character row inserted into
   a published run is refused; refusing a run that has a character row is refused.
9. **Run:** one character row per security; a withheld share (GME) has only its existing causes; a withheld ETF
   (CSPX.L) has both `unresolved_subject` and `character_unestablished`; a computed share's disposal and pool carry
   `offshore_fund_exclusion_assumed`, not `character_pending`; an incomplete character population raises before the
   seal and leaves no run.
10. **Selection:** run 1 is not selected after the build; a fresh v1 run is; with time moved 8 days past its capture
    the selection returns *unavailable*, and the next run publishes a new run.
11. **Job:** one failed fetch fails the job after the other fetches; the matching step still reads fresh captures.

## Acceptance (dev DB)

After deployment: one `tax_character_evidence_refresh` run, then `run_tax_matching`. For evidence run 2 (for a later
generation, rerun the Script and re-derive from its output the same way):

| security | character | status | expected |
|---|---|---|---|
| AAPL (1001) | share | computed | disposal 2026-09-23, 2026/27, gain **−£0.00753863550700**, 1 identification; reasons 3d-2's with `offshore_fund_exclusion_assumed` in place of `character_pending`; pool 0 / £0 |
| NXH (1181) | share | computed | pool 1305.05709600 / £6048.55403340328000, reasons 3d-2's plus `offshore_fund_exclusion_assumed` |
| SPY (3000) | unestablished, `exchange_traded_fund` | withheld | no disposal, identification or pool; 1 `character_unestablished` |
| GME (1699), ILMN (4077) | share | withheld | `unresolved_subject` 2 and 1, nothing else |
| IUSA.L (3075), CSPX.L (3434) | unestablished, `exchange_traded_fund` | withheld | `unresolved_subject` 1 and 3, plus 1 `character_unestablished` each |

Totals: 7 security rows (2 computed, 5 withheld), 7 character rows, 1 statutory disposal, 1 identification, 10
dependency rows (run 1's 7 plus 3), `withheld_all` false; `effective_tax_matching` returns the new run. The covers read
are fact 3's, or newer ones if filed since (rerun the Script).

## Full-population verification

Every security of the effective run and every member instrument was classified (fact 4), each with its fetched
evidence. The universe run (fact 6) measures how many Stocks reach the cover test; it is coverage, and the spec
claims no correctness from it.

## Finding dispositions

| finding | where |
|---|---|
| 17 ERI as a disposal-cost dependency | every fund withheld (Design 2, `exchange_traded_fund`); the reg 99 treatment in Source rules; ingestion in 3d-3c |
| 18 character over time | Design 2: the listed class at the holding's start and today, from dated, captured filings; reporting periods and elections only for funds, in 3d-3c |
| 30 keying and conflicts | Design 2 (component, `member_conflict`, `identity_unestablished`); test 4 |
| 44 citation | Source rules (IFM13372, reg 99) |
| 45 classification only of real holdings | Design 2, Withholding: character classifies; 3c's product gate and 3d-2's withholding decide what is computed |

## Security

No order or authorisation path changes. The job reads public, account-free SEC documents (operator rule 2026-10-10)
through the existing SEC rate lane; bodies are size-checked and validated before storage and live only in the DB. The
run stays a DB-only derivation.
