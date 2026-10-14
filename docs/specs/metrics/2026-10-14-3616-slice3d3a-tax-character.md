# Tax character, offshore-fund routing and the reportable-income gate (#3616 slice 3d-3a)

Source: #3616 slice 3d, split by 3d-1 (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-3a owns
slice-3 checkpoint-1 round-1 findings 17, 18, 30, 44 and 45 (text in `var/research/3616/ckpt1_slice3_r1_final.txt`,
loop worktree) and removes 3d-2's `character_pending`. 3d-1 gave 3d-3 dividends as well; they move to **3d-3b** (Not
in this slice): which broker field proves a dividend was paid is a separate evidence question, and one question per
checkpoint 1 keeps each spec closable.

**What this slice decides and builds.** Each tax security (3d-2 Design 2) gets a *tax character* from captured public
evidence: a **share**, a **non-reporting offshore fund**, a **reporting fund in the holding**, or **unestablished**.
Each statutory disposal of a computable security is then routed. A share's gain or loss is a capital gain or loss. A
non-reporting offshore fund's gain is an offshore income gain, charged as income, and its loss is a capital loss
only. A security that is unestablished, or that had reporting-fund status at any date from its first acquisition on,
is **withheld**: the second because its cost depends on reported income this slice does not ingest (finding 17's
gate). Nothing is shown to the operator (3d-4).

**The outcome on dev today:** AAPL and NXH are shares; AAPL's 2026-09-23 disposal becomes a capital loss of
£0.00753863550700. SPY, which 3d-2 computed, is **withheld**: no identifier for it is stored, so its absence from
HMRC's list cannot be checked (fact 4). With a bound CUSIP it would be a non-reporting offshore fund, and its
2026-09-23 gain of £0.86063438888195 an offshore income gain (fact 5).

## Facts established first

"Script" is `scripts/measure_3616_slice3d3a_character.py`, read-only, run on 2026-10-14 against the latest published
matching run (run 1, over evidence run 2) and live public sources. Its printout (`population.txt`) and the response
bodies with their SHA-256 are under `var/research/3616/slice3d3a/` (gitignored); the printout carries dates, amounts, symbols, CIKs and
CUSIPs, no account identifier. `--universe` applies Design 3's share rule to every tradable, non-variant Stocks
instrument with a primary SEC CIK, from the local `submissions.zip` that `sec_submissions_bulk_refresh` keeps
(archive dated 2026-10-10), output `universe.txt`.

1. **Instrument types and identifiers** (`instruments.instrument_type_id`, eToro's: 5 Stocks, 6 ETF;
   `external_identifiers`). AAPL (1001, 8754), NXH (1181), GME (1699, 8761) and ILMN (4077) are type 5; SPY (3000,
   3417), IUSA.L (3075) and CSPX.L (3434) are type 6. Primary SEC CIKs: AAPL 0000320193, NXH 0001130713, GME
   0001326380, ILMN 0001110803, SPY 0000884394; none for IUSA.L or CSPX.L. CUSIPs: AAPL 037833100, NXH 690370101
   (provider `openfigi`), GME 36467W109, ILMN 452327109; **none for SPY, IUSA.L or CSPX.L**. Only base rows carry
   identifiers; no `.RTH` variant does.
2. **SEC submissions JSON** (`data.sec.gov/submissions/CIK##########.json`, read 2026-10-14). `name`,
   `entityType`, and 10-K / N-CEN counts in `filings.recent.form`: AAPL "Apple Inc.", operating, 11 / 0; NXH
   "NEIGHBORHOOD INTELLIGENCE, INC.", operating, 9 / 0 (former names Overstock.com, Beyond, Bed Bath & Beyond); GME
   "GameStop Corp.", operating, 13 / 0; ILMN "ILLUMINA, INC.", operating, 8 / 0; SPY "SPDR S&P 500 ETF TRUST",
   **other**, 0 / 8. Energy Transfer ("Energy Transfer LP") and Enterprise Products ("ENTERPRISE PRODUCTS PARTNERS
   L.P."), both partnerships, are `operating` with 10-Ks: **no structured submissions field states legal form**
   (fields checked: `entityType`, `sic`, `stateOfIncorporation`, `category`).
3. **HMRC's list of reporting funds.** GOV.UK publication "Approved offshore reporting funds", content item
   `https://www.gov.uk/api/content/government/publications/approved-offshore-reporting-funds`, `public_updated_at`
   2026-10-12, "updated every month"; one `.ods` attachment (`20261006_Master-Weblist.ods`, 4,549,717 bytes;
   `content.xml` 147,393,474 bytes uncompressed). One sheet, 125,022 rows, one per share class, headers exactly:
   `Letter Group`, `Reporting Fund Ref`, `Parent Fund`, `HMRC Share Class Ref No.’s`, `Sub Fund Name`, `ISIN No`,
   `CUSIP No`, `Reporting Fund, with effect from`, `Ceased to be an RF on`. Ceased funds are kept: 46,726 rows carry a
   ceased date, 78,296 read `no data`. Dates are `dd/mm/yyyy`; 5,407 effective-from cells read `To Be Confimed`
   (sic) and one ceased cell reads `224-09-11`. No reporting-period-end, income or ERI column. GOV.UK keeps no
   earlier edition (old asset URLs serve the current file); the National Archives web archive holds captures.
   Rows: CSPX IE00B5BMR087 from 22/03/2010, IUSA IE0031442068 from 12/06/2010, VOO US9229083632 / 922908363 from
   01/01/2011, none ceased. SPY: **0 rows** by ISIN US78462F1030, by CUSIP 78462F103 and by name.
4. **SPY's identifier.** CUSIP 78462F103 is in `unresolved_13f_cusips` (issuer "STATE STR SPDR S&P 500 ETF T",
   resolution `openfigi_unknown`), not bound to instrument 3000. eToro's API documents no ISIN, CUSIP or FIGI field.
5. **The population under Design 3** (Script): AAPL, NXH, GME, ILMN → share; SPY, IUSA.L, CSPX.L → unestablished
   (`security_identifier_unestablished`). Of these only AAPL, NXH and SPY were computed by run 1. With 78462F103
   bound, SPY would match no list row, and SEC `other` with N-CEN filings would make it a non-reporting offshore fund.
6. **The share rule across the universe** (`universe.txt`): 5,332 instruments; share 3,959; `entityType` other
   1,096 (of which 962 file 20-F, 102 40-F, 32 neither: foreign private issuers); legal form unestablished 198
   (trailing words include BANCORP 20, TRUST 17, LP 12, LLC 9, L.P. 5, HOLDINGS 7, GROUP 6, and SPAC series
   numerals); no 10-K 78 (recent registrants, and ETFs typed as Stocks); `investment` 1.

## Source rules

- **Shares.** Shares are chargeable assets (TCGA 1992 s21) and their gains chargeable gains; identification is 3d-2's
  (ss104–106A).
- **Offshore fund.** TIOPA 2010 s355: a mutual fund constituted by a non-UK-resident body corporate, by a unit trust
  whose trustees are not UK resident, or by other arrangements under non-UK law creating co-ownership rights. s356:
  participants lack day-to-day control and "a reasonable investor would expect to be able to realise all or part of
  an investment on a basis calculated entirely or almost entirely by reference to" net asset value or an index.
  HMRC IFM12237 (read 2026-10-14): "Exchange Traded Funds (ETFs) are usually operated in such a way that the quoted
  prices are at NAV or very close to NAV … because market makers are able to create or redeem units"; a fund whose
  investors "could expect to receive NAV on selling their interest on a secondary market" is an offshore fund if the
  other conditions hold.
- **Which disposals give an offshore income gain** (SI 2009/3001 reg 17, legislation.gov.uk, read 2026-10-14):
  Condition A, the asset "is an interest in a non-reporting fund at the time of the disposal"; Condition B, an
  interest in a reporting fund that was previously non-reporting, held in a non-reporting fund during some of the
  *material period* (reg 17(4): from the day consideration was given for the acquisition to the day the fund became
  a reporting fund), where no election under reg 48(2) was made. IFM13510: an OIG arises on a non-reporting fund, or
  on a reporting fund "that has not had reporting fund status at a time in the period during which the investor held
  their interest".
- **Amount and nature** (regs 18, 33, 38, 39, 42, 45). The disposal is a TCGA disposal (reg 33), so 3d-2's
  identification applies unchanged. The OIG equals the basic gain, which for an income-tax payer is the TCGA gain
  computed "without regard to any charge to income tax arising under this Part" (reg 39). It is income arising at the
  disposal, charged under ITTOIA 2005 Part 5 Chapter 8 (reg 18). A loss makes the basic gain nil (reg 42); IFM13550:
  "any loss arising can be relieved only as a capital loss", which "cannot be offset against offshore income gains".
  In the chargeable-gains computation of the same disposal, "a sum equal to the offshore income gain shall be deducted
  from the sum which would otherwise constitute" the consideration (reg 45(3)), so a positive OIG leaves a chargeable
  gain of nil.
- **Reporting-fund disposals** (reg 99; IFM13372, read 2026-10-14; this replaces round 1's IFM12300 citation, which
  is the non-reporting introduction, finding 44): a chargeable-gains disposal, on which amounts charged under regs
  94–98 (reported income, including excess reportable income) are acquisition expenditure under TCGA s38(1)(a),
  incurred on the fund distribution date of their reporting period; income received after the disposal is treated as
  received immediately before it; with full equalisation, the equalisation amount is deducted from the purchase
  price. A reporting fund's cost therefore depends on the fund's per-period reports to investors.
- **Reporting status** is per share class with an effective date; HMRC's monthly list is the published record of it
  (fact 3).
- **No source** states how to read legal form from SEC data or how to bind an exchange listing to an ISIN. Both are
  fixed by construction in Design 3, fail closed, and frozen under the policy id.

## Design

### 1. Evidence capture (job `tax_character_evidence_refresh`)

A daily job captures two sources, each fetch stored whether or not it succeeds:
- **SEC submissions**, one request per CIK: every primary SEC CIK (`external_identifiers`, provider `sec`, type
  `cik`, `is_primary`) of every member of every component the effective generation names (3d-2 Designs 1 and 2).
  Requests go through the shared SEC rate lane with `settings.sec_user_agent`.
- **HMRC's list**: the GOV.UK content item, then its single `.ods` attachment (an item with zero or several `.ods`
  attachments is a failed fetch). User-Agent `eBull/1.0 (+repository URL)`. A body over 64 MiB, or a `content.xml`
  whose declared size exceeds 1 GiB, is a failed fetch, checked before reading further (validation at the boundary).

Storage (migration `sql/490_tax_character.sql`), append-only by trigger:
- `tax_character_contents`: `content_id` identity; `sha256` unique; `body_gzip BYTEA`.
- `tax_character_fetches`: `fetch_id` identity; `source` (`sec_submissions` or `hmrc_reporting_funds`); `key` (the
  10-digit CIK, or `''`); `requested_at`, `observed_at`; `outcome` (`ok` or `failed`); `http_status` (NULL allowed);
  `url` (for HMRC, the attachment URL); `source_updated_at` (HMRC `public_updated_at`, NULL allowed); `content_id`
  (non-NULL iff `ok`); `error` (non-NULL iff `failed`).

A body is stored once per SHA-256. The job fails when any fetch failed, after attempting all of them.

### 2. Character inputs of a security (findings 18, 30)

Character is decided per **tax security** (3d-2 Design 2), from every member of its component (finding 30): SPY's
evidence sits on 3000 and its lots on 3417, and both map to 3000. For each source the run reads the latest `ok` fetch
whose `observed_at` is within **7 days** of the run's own start; an older or absent one is *stale*. Inputs:
- `type`: the set of members' `instrument_type_id`;
- `cik`: the set of members' primary SEC CIKs; and from its fresh `sec_submissions` content: `name`, `entityType`,
  whether `filings.recent.form` contains `10-K`, whether it contains `N-CEN`;
- `cusip`: the set of members' CUSIPs (`external_identifiers` type `cusip`, any provider, upper-cased);
- `list_rows`: when `cusip` has one element, every row of the fresh HMRC list whose `CUSIP No` equals it, or whose
  `ISIN No` starts with `US` or `CA` and has it as characters 3–11 (ISO 6166: the NSIN of a US or Canadian ISIN is
  the CUSIP), as raw cell texts.

### 3. Character (`tax_character_v1`)

Evaluated in this order; the first matching line decides.

| character | condition | stored detail |
|---|---|---|
| unestablished | `type` not a single value, or `cik` or `cusip` has more than one value | `member_conflict` |
| **share** | `type` = {5}; one CIK, fresh; `entityType` = `operating`; a `10-K`; the name ends in a corporate legal-form word (below) | — |
| unestablished | `type` = {5}, any share condition fails | the failing condition |
| unestablished | `type` = {6} and `cusip` empty | `security_identifier_unestablished` |
| unestablished | `type` = {6}; the list stale, or its header row not exactly fact 3's | `reporting_list_unavailable` |
| unestablished | `type` = {6}; a matched row's effective-from or ceased cell neither `dd/mm/yyyy` (a valid date) nor, for ceased only, `no data` | `reporting_status_unparseable` |
| **reporting fund in the holding** | `type` = {6}; a matched row whose ceased cell is `no data` or a date ≥ the security's first admitted acquisition date (any matched row, when it has none) | — |
| **non-reporting offshore fund** | `type` = {6}; no such row; one CIK, fresh; `entityType` `other` or `investment`; an `N-CEN` | — |
| unestablished | anything else | `offshore_fund_unestablished` or the type found |

- **Corporate legal-form word.** The SEC `name`, upper-cased; SEC's trailing annotations `/XX/`, `/XX`, `\XX\` and
  `/NEW/` removed repeatedly; split on spaces and commas; the last token, dots removed, is one of `INC`,
  `INCORPORATED`, `CORP`, `CORPORATION`, `CO`, `COMPANY`, `LTD`, `LIMITED`, `PLC`, `NV`, `SA`, `AG`, `SE`, `PBC`.
  The structured fields carry no legal form (fact 2), so this is a text rule, fixed by construction to admit only
  corporate forms: a partnership, LLC or trust is withheld, and so is a corporation whose name lacks a form word (fact
  6). A false withhold costs coverage; a false share would misstate the character.
- **Why `10-K` and `operating`.** A US registered investment company files N-CEN, not 10-K; a foreign private issuer
  files 20-F or 40-F and SEC types it `other` (fact 6). v1 admits the domestic filer only: depositary receipts and
  foreign shares are withheld until a policy establishes them.
- **The fund rule's holding interval** is the security's first admitted acquisition date in the generation to the
  open future, so any reporting status from the first acquisition on withholds the whole security, including status
  first effective after a disposal and retroactive entries a later list shows. A row that ceased before the first
  acquisition does not: at every disposal the fund was non-reporting (Condition A), and its reports cover no period
  of this holding.
- **Elections.** A reg 48(2) election matters only under Condition B, which needs reporting status at the disposal;
  v1 withholds every such security, so no election input exists.
- **Why a US registrant with N-CEN is an offshore fund.** It is constituted under US law (s355) and, as an exchange
  traded fund (type 6), realises at or near NAV through creation and redemption (IFM12237, s356 condition C). The
  rule does not admit a closed-end fund typed as Stocks: type 5 never reaches the fund lines.

**Withholding.** 3d-2 Design 3 gains two causes, each a dependency row naming the detail:
- `character_unestablished`: the security is unestablished;
- `reportable_income_unestablished`: a reporting fund in the holding (finding 17's gate: its cost needs regs 94–99
  amounts per reporting period, and HMRC's list carries none, fact 3).

Withholding stays whole-security. A withheld security's character row is still stored (Design 6).

### 4. Routing a statutory disposal

| character | `treatment` | `offshore_income_gain_gbp` | `chargeable_gain_gbp` |
|---|---|---|---|
| share | `capital` | 0 | `gain_gbp` |
| non-reporting offshore fund, `gain_gbp` > 0 | `offshore_income_gain` | `gain_gbp` | 0 (reg 45(3)) |
| non-reporting offshore fund, `gain_gbp` ≤ 0 | `offshore_income_gain` | 0 (reg 42) | `gain_gbp`, a capital loss |

An `amount_missing` disposal has the treatment and both amounts NULL. The OIG is charged in the tax year of the
disposal (reg 18(1)), which is the disposal's `tax_year`. No arithmetic beyond a comparison and a copy: both amounts
are exact.

### 5. Reasons

- `character_pending` is no longer emitted.
- A disposal routed `offshore_income_gain` carries `reporting_status_as_listed`: HMRC's list can show status for a
  past date later, and Design 3 would then withhold the security. The reason is discharged by no evidence under
  this policy.
- A share disposal adds no reason.

### 6. Storage (migration `sql/490_tax_character.sql`)

- `tax_matching_runs` gains `character_policy_id TEXT` (NULL allowed: runs before this slice). The unique key
  becomes `(evidence_run_id, matching_policy_id, class_policy_id, character_policy_id, inputs_digest)` with `NULLS
  NOT DISTINCT`. Adding a nullable column changes no sealed row.
- `tax_matching_dependencies.reason` admits the two new causes.
- **`tax_matching_characters`**: `(matching_run_id, tax_security_id)` PK, FK to the security row; `character`
  (`share`, `non_reporting_offshore_fund`, `reporting_fund_in_holding`, `unestablished`); `detail` (NULL allowed,
  non-NULL iff unestablished); `cik`, `cusip` (NULL allowed); `sec_fetch_id`, `list_fetch_id` (NULL allowed: the
  fetches read); `list_rows JSONB` (the matched rows as read, `[]` when none). One row for every security row of the
  run.
- **`tax_disposal_treatments`**: `(matching_run_id, tax_security_id, uk_date)` PK, FK to the disposal; `treatment`;
  `offshore_income_gain_gbp`, `chargeable_gain_gbp` `NUMERIC(44,14)`, both NULL or both non-NULL; CHECKs (written
  `(…) IS TRUE`): `capital` ⇒ OIG = 0 when non-NULL; `offshore_income_gain` ⇒ OIG ≥ 0 and (OIG = 0 or chargeable =
  0). One row per disposal; that it equals Design 4 from the disposal's `gain_gbp` is checked in code before insert
  and asserted in the DB tests.
- Both child tables take 3d-2's child guard (insert only while the run is `building`, no update or delete).
- The `tax_character_*` capture tables of Design 1.

**`inputs_digest`** document version `matching_inputs_v2`: v1's document plus `"character": [[tax_security_id,
types, ciks, cusips, sec, list], …]` sorted by security, where `types`, `ciks` and `cusips` are sorted lists, `sec`
is `[name, entityType, has_10k, has_ncen]` or `null` (no single fresh CIK), and `list` is `null` when no fresh list
was needed or found, else the sorted matched rows. The extracted values enter the digest, not the content hashes: a
new 8-K changes SEC's document daily without changing a character. Staleness enters as `null`, so a capture outage
changes the digest and makes a new run.

**Selection.** `effective_tax_matching` also requires `character_policy_id = TAX_CHARACTER_POLICY_ID`; run 1 (NULL)
is no longer selected once a v1 run exists, and until then the result is *unavailable*, as for any policy change
(3d-2 Design 9).

### 7. The job and the run

`tax_character_evidence_refresh` is registered daily before `tax_broker_evidence_refresh` (12:13 UTC), at a minute
off the :15/:30 connection peaks. `run_tax_matching` reads the character inputs inside its REPEATABLE READ snapshot
(3d-2 Design 10), parses the HMRC content only when a security needs `list_rows`, and computes characters before
identification, so a withheld security is never identified. Parsing is the Script's stdlib reader: `zipfile` and
`xml.etree.ElementTree.iterparse` over `content.xml`, cell texts from `text:p`, `table:number-columns-repeated`
expanded; no new dependency.

## Local policy (frozen under `TAX_CHARACTER_POLICY_ID = "tax_character_v1"`)

The rule table and its order (Design 3), the legal-form word list and annotation pattern, the 10-K / N-CEN /
`entityType` tests, the CUSIP-or-NSIN match, the 7-day freshness, the open-future holding interval, the routing table
(Design 4) and `reporting_status_as_listed`. Changing any is a new policy id, which the run key then admits as a new
run for the same generation.

## Not in this slice

- **3d-3b, dividends:** income treatment, the evidence that a dividend was paid (sync `totalFees` is documented as
  "overnight fees and dividends" combined), and an admission policy for orders whose observed `totalFees` is
  non-zero, which 3c's gate now makes `unresolved`. Wake: before SPY's next ex-dividend date in December 2026, when a
  held position first accrues one; every observed `totalFees` on dev is 0 today.
- **Reported income (ERI) ingestion** for reporting funds, with equalisation. Wake: a reporting-fund security with an
  admitted lot (CSPX.L or IUSA.L once 3c resolves them, or #3781's switch of the core to CSPX.L).
- **SPY's CUSIP binding** (fact 4): an identity fix in the CUSIP resolver, not a tax rule. Until it lands SPY is
  withheld.
- Foreign private issuers, depositary receipts, partnerships, LLCs and trusts (withheld, fact 6); non-US funds
  without a stored CUSIP; Condition B and reg 48 elections; transparent funds (reg 16); 3d-4's view.

## Tests

Pure:
1. **Share rule:** each failing condition alone (type 6 member, two CIKs, stale capture, `other`, no 10-K); names
   "Apple Inc." and "GameStop Corp." pass; "Energy Transfer LP", "ENTERPRISE PRODUCTS PARTNERS L.P.", "X TRUST", "Y
   LLC" and "U.S. BANCORP /DE/" fail; "ACME INC /DE/", "Acme Inc./NV", "ACME CORP \DE\" and "ACME, INC. /NEW/" pass.
2. **Fund rule:** no CUSIP; two CUSIPs; header mismatch; stale list; `To Be Confimed`, `224-09-11` and 31/02/2020 in a
   matched row; a row effective before the first acquisition and not ceased, one ceased on the first-acquisition day,
   one effective after it (all reporting in the holding); one ceased the day before (not); no rows with SEC `other`
   and N-CEN (non-reporting); no rows with SEC `operating` (unestablished); a match by `CUSIP No`, by a `US` ISIN and
   by a `CA` ISIN; a `GB` ISIN with the same nine characters does not match.
3. **Keying (finding 30):** SPY's CIK on 3000 and lots on 3417 give one character for 3000; members with two CIKs,
   two CUSIPs or two types give `member_conflict`.
4. **Routing:** share gain and loss; fund gain £0.86063438888195 → OIG £0.86063438888195, chargeable 0; fund loss →
   OIG 0, chargeable the loss; zero; `amount_missing` → NULL amounts.
5. **ODS reader:** repeated columns and empty rows, a multi-paragraph cell, a covered cell; a 64 MiB body and a
   declared 1 GiB `content.xml` are refused before parsing.
6. **ISO 6166:** US0378331005 from 037833100; US78462F1030 from 78462F103.

DB:
7. **Capture:** an `ok` and a `failed` fetch row each; a repeated body stored once; UPDATE and DELETE refused.
8. **Run:** the character policy id in the key; a run's `inputs_document` reproduces its digest; an SEC content change
   that keeps the four extracted values makes no new run; a list change in a matched row makes one; a capture going
   stale makes one.
9. **Seal:** both new child tables refuse inserts outside `building`, and any update or delete.
10. **Selection:** run 1 (NULL character policy) is not selected; a v1 run is.
11. **Job:** a failed fetch fails the job after the other fetches ran; the matching step still uses the last fresh
    capture.

## Acceptance (dev DB)

After deployment, one `tax_character_evidence_refresh` run, then `run_tax_matching` over the effective generation
(evidence run 2, or a later one with the figures re-derived by rerunning both Scripts):

| security | character | status | expected |
|---|---|---|---|
| AAPL (1001) | share | computed | 2026-09-23 disposal `capital`, OIG 0, chargeable **−£0.00753863550700**; reasons as 3d-2's without `character_pending` |
| NXH (1181) | share | computed | pool 1305.05709600 / £6048.55403340328000, unchanged |
| SPY (3000) | unestablished, `security_identifier_unestablished` | withheld | no disposal, identification or pool; one `character_unestablished` dependency |
| GME (1699), ILMN (4077) | share | withheld | `unresolved_subject` as in 3d-2 |
| IUSA.L (3075), CSPX.L (3434) | unestablished, `security_identifier_unestablished` | withheld | `unresolved_subject` and `character_unestablished` |

Totals: 7 security rows (2 computed, 5 withheld), 7 character rows, 1 statutory disposal, 1 identification, 1
treatment, 10 dependency rows (3d-2's 7, plus 3 `character_unestablished`), `withheld_all` false.

## Full-population verification

The population is every security of run 1 and every member instrument (Script, fact 5); the universe measurement
(fact 6) covers every tradable Stocks instrument with an SEC CIK, so the share rule's coverage is measured, not
sampled. HMRC's list was read whole (125,022 rows) for the matches.

## Finding dispositions

| finding | where |
|---|---|
| 17 ERI as a disposal-cost dependency | Source rules (reg 99); Design 3, `reportable_income_unestablished`; Not in this slice |
| 18 character over time | Design 2 (share-class rows by identifier, captured versions), Design 3 (holding interval, elections) |
| 30 character keyed to the tax security, conflicts | Designs 2 and 3, `member_conflict`; test 3 |
| 44 citation | Source rules (IFM13372, reg 99) |
| 45 classification applies only to real holdings | 3c admits only lots it establishes as real (`excluded_non_share_product` and `unresolved` never reach matching); character classifies the security of an admitted lot |

## Security

No order or authorisation path changes. The capture job reads two public, account-free sources (operator rule
2026-10-10) and stores their bodies in the DB only; the repo is public and nothing captured is committed (HMRC's list
is published by GOV.UK; it is still kept out of the repo). The body size limits bound what an untrusted download can
cost before it is parsed. The run remains a DB-only derivation.
