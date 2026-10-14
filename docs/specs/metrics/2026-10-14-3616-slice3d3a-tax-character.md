# Tax character and the fund withholding gate (#3616 slice 3d-3a)

Source: #3616 slice 3d, split by 3d-1 (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-3a owns
slice-3 checkpoint-1 round-1 findings 17, 18, 30, 44 and 45 (text in `var/research/3616/ckpt1_slice3_r1_final.txt`,
loop worktree) and removes 3d-2's `character_pending`. 3d-1's 3d-3 row is split three ways (Not in this slice):
**3d-3a** (this) decides character and computes shares; **3d-3b** dividends; **3d-3c** offshore-fund disposals.

**What this slice decides and builds.** Each tax security (3d-2 Design 2) gets a *tax character*: **share** or
**unestablished**. A share needs SEC evidence about the listed class itself: the s12(b) class title on the issuer's
inline-XBRL cover, at the holding's start and today, besides the issuer's SEC entity type and industry code. A share
is computed exactly as 3d-2 computes it; its gains and losses are capital. Every other security is **withheld**,
including every exchange-traded fund. A fund cannot be computed with 3d-2's identification at all: an interest in a
non-reporting fund is a *relevant security*, outside the s104 pool and identified differently (Source rules), and a
reporting fund's cost depends on reported income nobody has ingested (finding 17). Both are 3d-3c's. Nothing is shown
to the operator (3d-4).

**The outcome on dev today:** AAPL and NXH are shares; AAPL's 2026-09-23 disposal is a capital loss of
£0.00753863550700. **SPY, which 3d-2 computed, is withheld**: 3d-2 identified it as a share (same-day, then 30-day,
then the pool), which is the wrong identification if it is a non-reporting fund, as fact 5 indicates. Run 1's SPY
figures are superseded, not corrected; 3d-3c computes them.

**Reuse.** The evidence is already in the DB or fetched by existing code: `instrument_sec_profile` (SEC entity type
and SIC), `filing_events` (10-K, 10-Q and 8-K filings with their primary-document URLs), `instrument_symbol_history`,
and the DEF 14A recipient job's cover reader and cache (`def14a_recipients.load_cover`: SEC's extracted XBRL instance
`<stem>_htm.xml`, parsed with `SAFE_XML_PARSER` by `sec_cover_identity`, raw body stored by `store_raw`, terminal
outcomes cached by accession in `sec_cover_12b_fetches` / `sec_cover_12b_pairs`, transient failures never cached).
This slice adds no capture table.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3a_character.py`, read-only, run on 2026-10-14. It resolves the effective
matching run with `effective_tax_matching` (run 1, over evidence run 2), then in one REPEATABLE READ transaction reads
that sealed run's rows and the inputs Design 2 names, checks afterwards that the effective run is unchanged, reads
each needed cover from the cache or, when not cached, fetches it and parses it with `parse_cover_instance` without
writing, and applies Design 2 through one function, `classify`, which the build moves into the service. Output
`var/research/3616/slice3d3a/population.txt`; fetched bodies with SHA-256 beside it (gitignored; no account
identifier). `--universe` applies the profile lines (entity type, SIC) to every Stocks instrument with a primary SEC
CIK (tradable or not, variants included), from the local `submissions.zip` of `sec_submissions_bulk_refresh` (archive
2026-10-10); output `universe.txt`.

1. **Members** (`instruments.instrument_type_id`, eToro's: 5 Stocks, 6 ETF; `external_identifiers` provider `sec`,
   type `cik`, `is_primary`). AAPL (1001, 8754), NXH (1181), GME (1699, 8761), ILMN (4077): type 5. SPY (3000, 3417),
   IUSA.L (3075), CSPX.L (3434): type 6. Primary CIKs sit on base rows only: AAPL 0000320193, NXH 0001130713, GME
   0001326380, ILMN 0001110803, SPY 0000884394; none for IUSA.L or CSPX.L. No component has two types or two CIKs.
   Symbol history: NXH (1181) was BBBY until 2026-08-21.
2. **SEC profile** (`instrument_sec_profile`, from SEC submissions; `entity_type`, `sic`, `fetched_at`): AAPL
   operating 3571 (2026-10-09); NXH operating 5961, GME operating 5734, ILMN operating 3826 (all 2026-10-14); SPY
   `other`, no SIC (2026-09-12). Energy Transfer and Enterprise Products, both partnerships, are `operating` with SIC
   4922, and the BDCs Ares Capital, Main Street Capital and MVC Capital are `operating` with an empty SIC (bulk
   archive): **neither field states legal form or security class** (also checked: `stateOfIncorporation`, `category`,
   `name`). Profile age across all 5,360 rows on 2026-10-14 (`width_bucket` of
   `now() − fetched_at` in 5-day bins): 4,098 under 30 days, 523 from 30 to 35, 739 at 40 or more.
3. **Covers name the listed class.** SEC Release 33-10618 (March 2019) requires the 10-K, 10-Q and 8-K cover to state
   the title of each class registered under Exchange Act s12(b) with its trading symbol, tagged in inline XBRL (phased
   in from fiscal periods ending on or after 2019-06-15); `sec_cover_identity` reads the tags `dei:Security12bTitle`,
   `dei:TradingSymbol` and `dei:SecurityExchangeName` sharing one context, and `cover_pairs` keeps only unambiguous
   (title, symbol) pairs. Script, newest cover per security from `filing_events`: AAPL 10-Q 2026-07-31
   (`0000320193-26-000020`) ("Common Stock, $0.00001 par value per share", AAPL), the six notes on its cover having no
   symbol; NXH 8-K 2026-10-06 (`0001140361-26-038800`) ("Common stock, $0.0001 par value per share", NXH) and its
   warrants; GME 10-Q 2026-09-09 ("Class A Common Stock", GME) and its warrants; ILMN 8-K 2026-10-13 ("Common Stock,
   $0.01 par value", ILMN). At the holding start: AAPL's is the same 10-Q (on or before 2026-09-23); NXH's is its 8-K
   2025-11-14 (`0001140361-25-042343`) ("Common Stock, $0.0001 par value per share", BBBY). Every cover's
   `entity_ciks` equals the issuer's CIK. Energy Transfer's 10-K 2026-02-19 lists ("Common Units", ET) (probe under
   `/tmp/3d3a_cover/`, not committed; the build's tests carry it as a fixture). `filing_events` holds 119,469 SEC
   10-K/10-Q/8-K rows filed since 2025-01-01, every one with a `.htm` primary-document URL.
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
6. **Coverage of the profile lines** (`universe.txt`, 5,339 instruments): `entityType` other 1,098 (by most recent
   annual form: 20-F 963, 40-F 102, neither 33); SIC 172 (6770 blank check 121, empty 49, 6799 2); `investment` 1;
   4,068 reach the cover test, which the universe run does not perform. This is coverage, not correctness.

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

### 1. Evidence

No new capture. A daily job, `tax_character_evidence_refresh`, warms the cover cache: for every tax security of the
effective generation (3d-2 Designs 1–2) whose members are Stocks with one primary CIK, it calls `load_cover` for the
two covers Design 2 reads (the newest, and the newest filed on or before the security's first admitted acquisition),
with `SecFilingsProvider.fetch_document_text` as the DEF 14A recipient job does, on the shared SEC rate lane. A
`CoverUnresolved` (transport error, non-XBRL body) is not cached, so the next run retries it; the job fails after
trying every cover. The extracted instance is checked to be at most 16 MiB before parsing; `load_cover` gains that
bound for both callers, as a `CoverUnresolved`.

**Covers of a security**: the SEC rows of `filing_events` for its members with `filing_type` 10-K, 10-Q or 8-K,
distinct by `provider_filing_id`, newest first by `(filing_date, provider_filing_id)`. A cover's outcome is read from
the cache only; the run never fetches.

### 2. Character (`tax_character_v1`; findings 18, 30)

Character is decided per **tax security**, from every member of its component (finding 30): SPY's CIK is on 3000 and
its lots on 3417, and both map to 3000. A member of an unestablished identity component (3d-2 Design 2) is its own
withheld security; its character is `unestablished`, `identity_unestablished`, without evaluation.

**Inputs.** The run has one `as_of`, the clock reading taken right after its snapshot starts. The *symbol* is the
tax security's own row's symbol under `symbol_key` (trimmed, upper-cased, a trailing `.US` dropped); the *symbols
ever* are every member's current symbol and every `instrument_symbol_history` symbol of every member, under the same
key. The *profile* is the `instrument_sec_profile` row of the member carrying the CIK (two members with the same CIK:
any, the CIK decides); it is *current* when `as_of − 30 days ≤ fetched_at ≤ as_of` (fact 2 measures the refresh). A
cover is *usable* when its cached outcome is `pairs` and its `entity_ciks` contain the CIK. A title is *common* when,
with whitespace collapsed and upper-cased, it matches exactly
`^(CLASS [A-Z] )?(COMMON STOCK|ORDINARY SHARES)(,? (<amount> PAR VALUE|PAR VALUE <amount>|NO PAR VALUE)( PER SHARE)?)?$`,
`<amount>` being an optional `US`, an optional `$`, and a decimal number: so "Common Stock Warrants", "Common Stock
Units" and "Ordinary Shares of Beneficial Interest" are not common. `def14a_recipients.title_kind` is not reused: it
counts a trust's "Common Shares of Beneficial Interest" as common, which is right for proxy recipients and wrong here.

Evaluated in order; the first line that applies decides.

| character | condition | `detail` |
|---|---|---|
| unestablished | members have more than one `instrument_type_id` (NULL counts as a value), or more than one primary CIK | `member_conflict` |
| unestablished | the type is 6 (ETF) | `exchange_traded_fund` |
| unestablished | the type is not 5 (Stocks) | `instrument_type` |
| unestablished | no primary CIK | `cik_unestablished` |
| unestablished | no current profile | `sec_profile_unavailable` |
| unestablished | `entity_type` ≠ `operating` | `entity_type` |
| unestablished | `sic` not four digits, or one of 6722, 6726, 6770, 6799 | `sic` |
| unestablished | no cover | `cover_unavailable` |
| unestablished | the newest cover not cached | `cover_unavailable` |
| unestablished | the newest cover not usable | `cover_unusable` |
| unestablished | in it, not exactly one pair has the symbol, or that pair's title is not common | `listed_class_not_common_stock` |
| unestablished | the security has an admitted acquisition and no cover is filed on or before the first one, or that cover is not cached | `holding_start_cover_unavailable` |
| unestablished | that holding-start cover not usable | `holding_start_cover_unusable` |
| unestablished | in it, not exactly one pair has a symbol among the symbols ever, or that pair's title is not common | `holding_start_class_not_common_stock` |
| **share** | otherwise | NULL |

- **Why these tests establish a share outside the fund regime.** The cover's s12(b) title is security-specific
  evidence that the listed class is the registrant's common stock or ordinary shares: the equity of a company, not
  partnership units (Energy Transfer's "Common Units"), a trust's shares of beneficial interest, notes or warrants.
  `operating` with a four-digit SIC outside SEC's investment-vehicle codes (6722 open-end funds, 6726 unit trusts and
  closed-end funds, 6770 blank checks, 6799 investors not elsewhere classified) marks an operating company; the BDCs
  of fact 2 carry no SIC and are withheld. Such a share is priced by trading, with no arrangement to realise it by
  reference to net asset value or an index, so s356 condition C fails and it is not an offshore fund (IFM12237: a
  price near NAV counts only "as a result of arrangements being in place"). No filing states that an arrangement is
  absent; the evidence above is the most a public source gives, so the residue is assumption **A6**, named and carried
  as a reason (Design 3). Withholding every share would leave nothing computable without making any figure more
  correct.
- **Over the holding (finding 18).** The class is checked at both ends: today's listing paired by today's symbol, and
  the listing at the holding's start paired by a symbol the instrument has carried (NXH's was BBBY, fact 1). An
  unrelated common class on the start cover does not count: its symbol is not among the symbols ever. A change of
  legal form between those filings and back is not detected; a filing-by-filing walk is the next policy if a case
  appears. The CIK bound today is taken as the issuer throughout: a change of an instrument's issuer binding is an
  identity change of the security, which 3d-2's evidence identity governs. A share has no reporting status or
  election to track; those exist only for funds (3d-3c).
- A foreign issuer filing 10-K, 10-Q or 8-K covers that passes is a share by the same argument; issuers SEC types
  `other`, including those filing 20-F or 40-F (fact 6), are withheld.

**Withholding.** 3d-2 Design 3 gains the cause `character_unestablished`: exactly one dependency row for a security
iff its character is `unestablished`, naming the `detail`, alongside any other cause. Withholding stays
whole-security. Every security row gets a character row, withheld ones included: character classifies the security
named by an admitted lot or an unresolved subject; whether its lots are computed remains 3c's and 3d-2's gate
(finding 45).

### 3. Routing and reasons

A computed security is a share, so each statutory disposal is a chargeable-gains disposal: `gain_gbp` is the capital
gain or, when negative, the allowable capital loss. No new amount is stored; the character row says how every figure
of the security is taxed. `character_pending` is no longer emitted. Every disposal and pool of a share carries
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
  NULL` (`share`, `unestablished`); `detail TEXT`, non-NULL iff unestablished, one of the table's codes or
  `identity_unestablished` (CHECK); `cik TEXT` (the single CIK, else NULL); `latest_cover`, `start_cover TEXT` (the
  accessions read, else NULL). It takes 3d-2's child guard (insert only while its run is `building`; no update or
  delete). Publication requires exactly one character row per security row, checked in code before the seal.

**`inputs_digest`** document `matching_inputs_v2`: v1's document plus `"character": [[tax_security_id, types, ciks,
symbol, symbols_ever, profile, latest, start], …]` sorted by security. `types`, `ciks` and `symbols_ever` are sorted
lists (a NULL type as `null`); `profile` is `null` (none, or not current) or `[entity_type, sic]`; `latest` and
`start` are `null` (no such cover, not needed, or not cached) or `[accession, outcome, entity_ciks, pairs]`, both
lists sorted. Inputs enter, not dates: a profile refreshed with the same values makes no new run; a new filing, a
newly cached cover, or a profile going stale makes one.

**Selection.** `effective_tax_matching` computes the digest with its own `as_of` and also requires
`character_policy_id = TAX_CHARACTER_POLICY_ID`. Run 1 (NULL) is not selected; a profile gone stale since the last
run makes the current digest differ, so the result is *unavailable* until the next run, never the old run.

### 5. The job and the run

`tax_character_evidence_refresh` is registered daily before `tax_broker_evidence_refresh` (12:13 UTC), at a minute
off the :15/:30 connection peaks, so the matching inside the evidence job finds the covers cached. `run_tax_matching`
reads the character inputs inside its REPEATABLE READ snapshot (3d-2 Design 10), sets `as_of`, and decides characters
before identification; a withheld security is never identified. `classify` is pure and returns `(character,
detail)`, `detail` None for a share.

## Local policy (frozen under `TAX_CHARACTER_POLICY_ID = "tax_character_v1"`)

The rule table and its order, the common-title grammar, the SIC set, the 30-day profile window, the two-ends holding
check with symbol binding, the single-CIK issuer assumption, and A6. Changing any is a new policy id, which the run
key admits as a new run for the same generation.

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
- Foreign private issuers, partnerships, LLCs, trusts, BDCs and blank checks (withheld); 3d-4's view.

## Tests

Pure, on `classify`, the title grammar and the digest document:
1. **Each table line alone**, in order: two types; a NULL type beside 5; two CIKs; type 6; type 4; no CIK; no profile;
   a profile 30 days old exactly (current) and 30 days and 1 µs (not); one dated after `as_of` (not); `other`; SIC
   6770, 6722, 6726, 6799, "", " 3571", None; no cover; the newest cover uncached; its outcome `no_pairs`,
   `not_found`, and `pairs` without the CIK; two pairs with the symbol; the symbol's title "Common Units" (Energy
   Transfer fixture); an acquisition and no cover on or before it; the start cover uncached, `no_pairs`, without the
   CIK; a start cover with only an unrelated ("Common Stock", "UNRELATED") pair; a start pair ("Common Units", BBBY).
2. **Titles:** common: "Common Stock, $0.00001 par value per share", "Common stock, $0.0001 par value per share",
   "Class A Common Stock", "Common Stock, $0.01 par value", "Common Stock, par value $0.001 per share", "Ordinary
   Shares, US$0.0001 par value per share", "Common Stock, no par value"; not common: "Common Stock Warrants", "Common
   Stock Units", "Ordinary Shares of Beneficial Interest", "Common Shares", "Common Units", "Warrants to Purchase
   Common Stock", "Class A Common Stock and one-third of one Redeemable Warrant".
3. **Shares:** AAPL's cover and NXH's two covers (NXH today, BBBY at the start, BBBY in the symbols ever) give `share`
   with detail None; NXH without its history row gives `holding_start_class_not_common_stock`.
4. **Keying (finding 30):** a CIK on the base and lots on its `.RTH` variant give one character for the base; the
   symbol is the base's, not the variant's; two members with the same CIK are not a conflict; an unestablished
   identity's members get `identity_unestablished`.
5. **Digest:** an identical profile with a new `fetched_at` gives an identical document; a new cover accession, a
   changed pair, `sic` or `entity_type`, and a profile going stale each change it; the bytes and SHA-256 of a pinned
   fixture.

DB:
6. **Cover cache:** a 16 MiB + 1 byte instance raises `CoverUnresolved` and caches nothing; a transport error caches
   nothing and the next job run fetches again; a 404 caches `not_found`; a cached accession is not fetched again.
7. **Seal:** changing `character_policy_id` or `as_of` in the sealing update is refused; a character row inserted into
   a published run is refused; refusing a run that has a character row is refused.
8. **Run:** one character row per security; a withheld share (GME) has only its existing causes; a withheld ETF
   (CSPX.L) has both `unresolved_subject` and `character_unestablished`; a computed share's disposal and pool carry
   `offshore_fund_exclusion_assumed`, not `character_pending`; an incomplete character population raises before the
   seal and leaves no run.
9. **Selection:** run 1 is not selected after the build; a v1 run is; with time moved 31 days past the profile's
   `fetched_at` the selection returns *unavailable*, and the next run publishes a new run.
10. **Job:** one unresolved cover fails the job after the others are tried; the matching step reads what was cached.

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
are fact 3's, or newer ones if filed since (rerun the Script); a profile older than 30 days at the run withholds its
security until `fundamentals_sync` refreshes it.

## Full-population verification

Every security of the effective run and every member instrument was classified (fact 4), each with the evidence it
read. The universe run (fact 6) measures how many Stocks reach the cover test; it is coverage, and the spec claims no
correctness from it.

## Finding dispositions

| finding | where |
|---|---|
| 17 ERI as a disposal-cost dependency | every fund withheld (Design 2, `exchange_traded_fund`); the reg 99 treatment in Source rules; ingestion in 3d-3c |
| 18 character over time | Design 2: the listed class at the holding's start and today, bound by symbol history; reporting periods and elections only for funds, in 3d-3c |
| 30 keying and conflicts | Design 2 (component, `member_conflict`, `identity_unestablished`); test 4 |
| 44 citation | Source rules (IFM13372, reg 99) |
| 45 classification only of real holdings | Design 2, Withholding: character classifies; 3c's product gate and 3d-2's withholding decide what is computed |

## Security

No order or authorisation path changes. The job reads public, account-free SEC documents (operator rule 2026-10-10)
through the existing SEC rate lane and the existing cover cache; instance bodies are size-checked and parsed with the
repo's safe XML parser. The run stays a DB-only derivation.
