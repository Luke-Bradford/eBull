# Reviewed fund determination (#3616 slice 3d-3c-2)

Source: #3616 slice 3d-3c, as 3d-3c-1 split it (`2026-10-14-3616-slice3d3c1-fund-identity.md`, "Not in this slice").
3d-3c-1 captures structured evidence and records a *candidate identity* and a *list search* per fund. Its
checkpoint 1 held (rounds 1–2, `var/research/3616/slice3d3c1/ckpt1_r<n>_final.txt`) that structured data cannot
establish the s355 limb and residence, s356's conditions, UK opacity or non-reporting status at a disposal. This slice
supplies those as a **reviewed, versioned determination** per fund: a committed document that answers each statutory
question from the fund's governing documents, quotes the passages it relies on, and is checked by code against the
stored documents before anything may use it.

**What this slice decides and builds.**
- The determination format and policy (`fund_determination_v1`), and SPY's first determination
  (`2026-10-14-3616-slice3d3c2-spy-determination-v1.json`, beside this spec; the build moves it unchanged).
- A step that loads determinations, captures the documents they cite, verifies every quote, and records per fund
  security whether a determination applies (Design 2).
- A pure rule giving a fund's reporting status at a disposal date (Design 3), which 3d-3c-3 calls.
- What clears an `unresolved` list search (Design 4).

It changes no character, no matching run and no figure. Every fund stays withheld as `exchange_traded_fund` (3d-3a).

**The outcome for SPY.** SPY is an offshore fund: a mutual fund under which property is held on trust by a trustee not
resident in the UK (s355(1)(b), s356 A–C met, s357 condition D not met). Its reporting status at its 2026-09-23
disposal is `non_reporting_provisional`: HMRC's list does not show it, but an application covering that date could
still be received until 2026-12-17. **Whether SPY is a transparent fund (reg 11) is unresolved**, so whether its gains
are offshore income gains (reg 17) or chargeable gains (reg 29) is unresolved (Determination, transparency).

## Facts established first

"Script" is `scripts/measure_3616_slice3d3c2_determination.py`, read-only, run on 2026-10-14. Its output is
`var/research/3616/slice3d3c2/population.txt` (loop worktree).
- It reads the determination draft and the SEC documents saved under `var/research/3616/slice3d3c2/spy/`, reduces
  each to text (`document_text`: tags dropped, `script` and `style` content dropped, entities unescaped, white space
  collapsed) and checks each quote, white space collapsed, as a substring. It prints each document's SHA-256 and
  length.
- In one REPEATABLE READ snapshot it reads the newest `tax_fund_observations` row per fund security, the CIK's
  governing filings in `filing_events`, the content item's `public_updated_at` and the security's lots, and applies
  Design 3 to each disposal date.

The documents were fetched from SEC EDGAR on 2026-10-14 (`spy/index.txt`, URL, accession, form, date, SHA-256). The
legal texts are under `var/research/3616/slice3d3c2/legal/`, each with its URL and fetch time: legislation.gov.uk's
current revised text, and gov.uk's content API for HMRC manuals.

1. **The governing documents.**
   - **Prospectus** dated 2026-01-26, 485BPOS `0001193125-26-022316` (Post-Effective Amendment No. 40). The Rule
     497(b) prospectus `0001193125-26-022775` (2026-01-26) is the same prospectus as filed; `0001193125-26-023648`
     (2026-01-27) is a Singapore wrapper incorporating it.
   - **Standard Terms**: the Amended and Restated Standard Terms and Conditions of Trust, effective 2004-01-27
     (485APOS EX-99.A1, `0000950136-04-000194`), and Amendments 1–7. Their subjects, read in full: 1 special
     dividends (Section 3.04); 2 the Exchange, Depository and NSCC definitions, successors and addresses; 3 the names
     of the Trust and its units; 4 the trustee, from 2017-06-16; 5 and 6 the settlement cycle (Sections 2.03, 3.04);
     7 the Trust's name, from 2026-01-26. None amends Sections 2.01, 2.02, 5.02, 10.04 or 10.05, which the
     determination quotes.
   - The original Trust Indenture of 1993-01-22 is not on EDGAR: the CIK's filing list begins 1996-01-25. It fixes the
     Mandatory Termination Date (2118-01-22, or 20 years after the last of eleven named lives), which the
     prospectus restates. Nothing below relies on it.
   - No CUSIP or ISIN appears in any of these documents. The binding to an ISIN is 3d-3c-1's candidate chain.
2. **Quotes.** All 19 quotes of the draft occur in their documents (Script: `quotes missing: 0`).
3. **SPY on dev.** Newest observation 1 (2026-10-14 19:34:05Z): `candidate`, CIK 0000884394, LEI
   549300NZAMSJ8FXPQQ63, ISIN US78462F1030, MIC ARCX, symbol SPY, search `not_found`, content fetch 5 with
   `public_updated_at` 2026-10-12T11:30:44+01:00. IUSA.L and CSPX.L are `cik_unestablished`.
4. **Governing filings since the newest reviewed one.** In `filing_events` for SPY's members, filings of the forms in
   Design 2's list dated on or after 2026-01-26 are exactly the four reviewed: 485BPOS and 497 of 2026-01-26, 497 of
   2026-01-27, N-CEN of 2026-03-13. SPY's other filings since then are N-PORT-P (2026-02-26, 2026-05-28, 2026-08-28)
   and N-30D (2026-05-29, the semi-annual report).
5. **SPY's lots** in evidence run 6: 8 lots, the first acquisition on 2026-09-18 and one disposal on 2026-09-23. The
   account is the operator's, a UK-resident individual (`.claude/CLAUDE.md`). Every lot is in the demo environment;
   3d-3 computes them as evidence of the method.
6. **Design 3 for that disposal** (Script): period of account ends 2026-09-30; application deadline 2026-12-17; bar
   2027-03-11; status `non_reporting_provisional`.
7. **HMRC's general list of foreign entities** (INTM180030) has seven USA rows: partnerships under the Uniform
   Partnership Act and the Uniform Limited Partnership Act, LLC, LLP, S corporation, REIT (opaque) and LLLP. It has
   none for a New York trust or a unit investment trust.

## Source rules

- **Offshore fund.** TIOPA 2010 s355(1): a mutual fund (a) constituted by a body corporate resident outside the UK,
  (b) "under which property is held on trust for the participants where the trustees of the property are not resident
  in the United Kingdom", or (c) constituted by other arrangements creating rights in the nature of co-ownership
  under the law of a territory outside the UK. s355(2) excludes partnerships from (c). SI 2009/3001 reg 3 takes the
  definition from FA 2008 s40A(2), whose (b) and (c) are the limbs reg 29(1)(a) names.
- **Mutual fund.** s356: arrangements meeting
  - A, that their purpose or effect is to let participants share in the property or its profits or income;
  - B, that participants do not have day-to-day control of the property's management (a right to be consulted or to
    give directions is not control, s356(5); IFM12233: an AGM vote is not control);
  - C, that "a reasonable investor participating in the arrangements would expect to be able to realise all or part of
    an investment ... on a basis calculated entirely, or almost entirely, by reference to" net asset value or an
    index.
  HMRC IFM12237: "It is not necessary that the investor obtains NAV directly from the fund. ... Exchange Traded Funds
  (ETFs) are usually operated in such a way that the quoted prices are at NAV or very close to NAV because market
  makers are able to create or redeem units. In that sense, such funds are open-ended and ETFs would be expected to
  qualify as mutual funds." IFM12282 says the same of ETFs.
- **Exceptions.** s357(1): not a mutual fund only if condition D is met *and* E or F. D: realisation on a NAV or
  index basis is expected "only in the event of the winding up, dissolution or termination of the arrangements".
  IFM12242: "open-ended arrangements ... cannot come within the exceptions provided by s357". s359 lets the Treasury
  make further exceptions by regulations; SI 2009/3001's contents list makes none in its Part 1. s360 treats each
  part of umbrella arrangements, and s361 each class of interest, as separate arrangements.
- **Residence of trustees.** ITA 2007 s475 (income tax) and TCGA 1992 s69 (CGT) treat the trustees as one person,
  UK resident when all trustees are UK resident (condition A / condition 1), or when they are mixed and a settlor
  condition holds. A non-UK trustee acting "as trustee in the course of a business which [it] carries on in the United
  Kingdom through a branch, agency or permanent establishment" is assumed UK resident for those conditions
  (s475(6), s69(2D)).
- **Reporting status.** reg 4(2): "In a period of account an offshore fund is a non-reporting fund unless it is a
  fund to which Part 3 ... applies". reg 12: a period of account is "any period for which accounts of the offshore
  fund are drawn up". Part 3 applies after HMRC accept an application (reg 57(1)–(2)), from the later of the first day
  of the first period of account named in it and the fund's establishment.
  - reg 54(2): the application must be received "before the later of (a) the end of the first period of account for
    which it is proposed that this Part should apply to the fund, and (b) the expiry of a period of 3 months beginning
    with the first day on which interests in the fund are made available to investors resident in the United
    Kingdom."
  - reg 55: HMRC respond within 28 days, by accepting, rejecting or asking for further information; further information
    is due within 28 days "or within such longer period as is agreed by HMRC"; HMRC then decide within 28 days.
  - reg 57(4): an application withdrawn under reg 54(3)(b) is treated as never made.
  - HMRC's list records approved reporting funds and is updated monthly (GOV.UK "Approved offshore reporting funds").
- **Transparency.** reg 11: a fund is transparent if UK-resident individual holders are chargeable, under a provision
  in ITTOIA 2005 s830(2), on the sums forming part of the fund's income referable to their interests. reg 29(1): no
  reg 17 charge on disposing of an interest in a s40A(2)(b) or (c) fund that is transparent, unless (2) at any time in
  the holding it held more than 5% by value in other non-reporting funds, or (3) it is non-reporting and "fails to
  make sufficient information available to participants ... to meet their tax obligations in the United Kingdom with
  respect to their shares of the income of the fund". IFM12254: in a transparent fund "investors are entitled to income
  as it arises ... regardless of whether income is actually distributed". IFM12252 and IFM13470 name "Baker" foreign
  unit trusts and FCPs as examples. INTM180040: the question is decided by "investigating the rights and duties of the
  members ... under the relevant foreign commercial law (i.e. examining any legislation and constitutional documents
  ...)". "How the foreign entity is taxed in another country is not relevant." IFM13470 on reg 29(3): sufficient
  information "would enable the UK investor to compute their final UK tax liability ... correctly", including their
  share of income arising and confirmation about holdings in non-reporting funds.
- **No source rule** fixes how long after an application deadline a list search becomes reliable, which documents
  must be re-read when a fund files again, or how a quote is matched in an HTML filing. Designs 1–3 fix these by
  construction and freeze them under the policy id.

## The determination for SPY

Each finding is one row of the JSON; its quotes are verbatim (fact 2). The reasoning below is the reviewed part and
is what checkpoint 1 tests.

**Arrangement (s360, s361): `single_arrangement_single_class`.** "The Trust is a unit investment trust that issues
Units", each a "unit of fractional undivided interest in the Trust". 3d-3c-1's candidate already requires the N-CEN to
report one series (`numOfExistingSeries` 1) whose series CIK is the registrant's. Fungible units are one class
(IFM12282, citing IFM12270).

**s355 limb: `b`.**
- Property on trust: the Trustee "holds and will hold the Trust Fund as Trustee for the use and benefit of all present
  and future Beneficial Owners" (Section 2.01); deposits are conveyed to it "in an irrevocable trust" (2.02); New York
  law governs the rights of the parties and the Beneficial Owners (10.05).
- The trustees: since 2017-06-16 one trustee, State Street Global Advisors Trust Company (Amendment 4), "a
  limited-purpose trust company organized under the laws of the Commonwealth of Massachusetts with its principal place
  of business at One Congress Street, Boston". The N-CEN gives its location as US-MA. So condition A of s475 / condition
  1 of s69 fails, and the mixed-trustee condition cannot arise with one trustee.
- Assumptions, stated: **A1**, the trustee's central management and control is not in the UK (the documents give its
  organisation and principal place, not its board's location); **A2**, it does not act as SPY's trustee through a UK
  branch (s475(6), s69(2D)); the documents place trustee functions with the trustee in Boston and its parent SSBT as
  custodian.
- Limb (c) is not excluded: units are "an undivided ownership interest in Portfolio Securities". Both limbs are in
  reg 29(1)(a), so nothing downstream depends on which. Limb (a) is not met: the Trust is not incorporated, and Section
  10.04(d) says nothing in it constitutes the Beneficial Owners "as partners or members of an association".
- `valid_from` is 2017-06-16: before it the trustee was State Street Bank and Trust Company, whose residence evidence
  this determination does not carry.

**s356 A: `met`.** Units are an undivided ownership interest in the portfolio, and holders receive "an amount
corresponding to the amount of any cash dividends declared on the Portfolio Securities ... net of fees and expenses".

**s356 B: `met`.** "No Beneficial Owner shall have any right to vote except as provided in Sections 9.01 and 10.01 or
in any manner otherwise to control the operation and management of the Trust Fund" (10.04(d)). Sections 9.01
(termination by two-thirds of holders) and 10.01 (amendments) are votes and directions, which s356(5) excludes.

**s356 C: `met`.** The Trust "continuously issues and redeems" Creation Units "at their once-daily NAV", and "Units are
listed individually for trading on the Exchange". That is IFM12237's case: secondary-market prices near NAV because
market makers create and redeem.

**s357 D: `not_met`.** "A redeemer may at any time tender SPDRs in Creation Unit size aggregations to the Trustee for
redemption" (10.04(b)), at NAV (prospectus, "Redemption"). Realisation on a NAV basis is not confined to termination,
so s357(1) cannot apply (IFM12242).

**Offshore fund.** Computed by code from the rows above, never stated: limb in {a, b, c}, A, B and C `met`, D
`not_met`, arrangement single. SPY: `true`.

**Period of account end: `09-30`.** "Effective as of September 30, 1997, the Trust's fiscal year end changed from
December 31 to September 30." The Trust also files a semi-annual report (N-30D, fact 4), whose half-years end on
31 March and 30 September; Design 3 uses the annual end, which is never earlier than the half-year end containing the
same date.

**Transparent fund (reg 11): `unresolved`.** The documents point both ways.
- For: units are "an undivided ownership interest in Portfolio Securities".
- Against: "The Trustee shall accrue all Income to the Trust as of the date on which the Trust is entitled to such
  Income as a holder of record" (3.01(a)); income is held uninvested, fees and expenses are deducted, and holders of
  record on the ex-dividend date "are entitled to receive an amount representing dividends accumulated on Portfolio
  Securities through the quarterly dividend period". A holder who disposes before that date receives none of the income
  that arose while they held.
- INTM180040 decides this from New York law on the Beneficial Owners' rights against the Trust and its income, not
  from the documents' labels and not from the Trust's US tax status (a RIC). No source we hold states that analysis.
  INTM180030 has no row for a New York trust (fact 7).
- **What would settle it:** a reading of New York law on a unitholder's entitlement to Trust income under these Standard
  Terms, with a citable source, or an HMRC view of this fund (a clearance under INTM180060, or a list entry).

**reg 29(3) sufficient information: `unresolved`.** The Trustee furnishes, with each distribution, "a statement
setting forth the amount being distributed, expressed as a dollar amount per Unit", and an audited annual report.
Whether that lets a UK holder compute their share of income arising (IFM13470's test) is not settled by the
documents. It matters only if the fund is transparent.

**The consequence, for 3d-3c-3.** reg 29 removes the reg 17 charge only for a transparent fund, and (2) or (3) can
restore it. With transparency unresolved, whether a disposal gives an offshore income gain or a chargeable gain is
unresolved. The identification does not depend on it: 3d-3a's Source rules make an interest in a non-reporting fund a
relevant security, and a transparent non-reporting fund is still non-reporting (reg 4(2)). If SPY were transparent,
its holders would also be charged on income as it arises, not on distributions, which bears on 3d-3b. 3d-3c-3 decides
how a figure is published with this unresolved; this slice only records it.

## Design

### 1. The determination format (`fund_determination_v1`)

A JSON document, committed under `app/services/tax_fund_determinations/`, one file per version. Keys, all required:
- `determination_id`: `fund_determination_v1:<cik>:<version>`, version a positive integer.
- `policy_id`: `fund_determination_v1`.
- `binding`: `cik` (ten digits), `lei` (ISO 17442), `isin` (ISO 6166), `mic` (`[A-Z0-9]{4}`), `ticker` (compared under
  `symbol_key`). It is the 3d-3c-1 candidate this determination was reviewed against.
- `valid_from`: a date. The determination covers disposal dates on or after it.
- `reviewed_filings`: the accessions of every governing filing (Design 2's form list) dated on or after the newest
  reviewed 485BPOS, all of which the reviewer read; `reviewed_through`: the newest of their dates.
- `documents`: each with `doc_id` (unique), `url`, `accession`, `form`, `filed` and `role`. A URL is
  `https://www.sec.gov/Archives/edgar/data/<cik as integer>/<accession digits>/<file>`, whose CIK is the binding's and
  whose accession digits are the document's accession.
- `findings`: exactly one per question, with `answer`, `sources` (strings) and `quotes` (each a `doc_id` from
  `documents` and a non-empty `text`). Questions and their allowed answers:

| question | answers |
|---|---|
| `arrangement` | `single_arrangement_single_class`, `unresolved` |
| `s355_limb` | `a`, `b`, `c`, `none`, `unresolved` |
| `s356_condition_a`, `s356_condition_b`, `s356_condition_c` | `met`, `not_met`, `unresolved` |
| `s357_condition_d` | `met`, `not_met`, `unresolved` |
| `period_of_account_end` | `MM-DD`, or `unresolved` |
| `transparent_fund` | `transparent`, `opaque`, `unresolved` |
| `reg29_sufficient_information` | `made_available`, `not_made_available`, `unresolved` |

- `cleared_list_rows`: a list (Design 4), empty for SPY.

Every answer other than `unresolved` needs at least one quote. A file is immutable once merged: the step stores its
SHA-256 and refuses a file whose id is stored with a different hash, so a change is a new version. The review record
is the merged PR (spec, checkpoint 1, review bot) that added the file.

`offshore_fund(det)` is true only when `s355_limb` is `a`, `b` or `c`, the three s356 conditions are `met`,
`s357_condition_d` is `not_met` and `arrangement` is `single_arrangement_single_class`; false when a limb is `none`
or any condition is `not_met` (or D `met`); otherwise unresolved.

### 2. Applicability, per fund security per step run

A step `refresh_fund_determinations(conn)` runs after `refresh_fund_evidence` in both invokers of
`run_tax_matching`. It:
1. loads every determination file, validates it (Design 1) and stores new ones (a stored id with a different hash
   fails the step);
2. fetches each cited document not yet stored, once: the SEC gate and user agent the fund step uses, no redirects,
   HTTP 200 only, a body of at most 8 MiB that `document_text` reads (strict UTF-8). A stored document is never
   fetched again; EDGAR archive documents do not change. A failed fetch stores nothing and is retried next run;
3. for each fund security, reads its newest `tax_fund_observations` row and records one determination observation.

The outcome, first line that applies:

| outcome | condition |
|---|---|
| `not_candidate` | the newest fund observation is not `candidate` |
| `no_determination` | no stored determination has the candidate's CIK |
| `binding_mismatch` | the newest version for the CIK has a binding that differs from the candidate's CIK, LEI, ISIN, MIC or ticker (`symbol_key`) |
| `documents_unavailable` | a cited document has no stored fetch |
| `quote_unverified` | a quote, white space collapsed, is not a substring of its stored document's `document_text` |
| `stale` | `filing_events` holds, for the security's members, a filing of form 485APOS, 485BPOS, 485BXT, 497, 497J, 497K, N-CEN, N-CEN/A, N-8F or N-8F/A dated on or after the earliest date in `reviewed_filings`' filings and not in `reviewed_filings` |
| `determined` | otherwise |

"Newest version" is the greatest version number for the CIK. A `stale` determination needs a new version that reads
the new filing; until then every disposal is withheld. Staleness is as complete as `filing_events`' SEC ingest for the
CIK, a coverage residual.

`document_text(body)`: decode strict UTF-8, parse with `html.parser`, drop tags and the content of `script` and
`style`, unescape entities, collapse every run of white space to one space and trim.

### 3. Reporting status at a disposal date

`reporting_status(det, fund_observation, list_published, first_acquisition, d)`, pure, for a `determined` security:
- `before_valid_from` if `d` < `valid_from`;
- `listed` if the search outcome is `listed` (a reporting fund: 3d-3c-4);
- `reporting_status_unestablished` if it is `unavailable`, or `unresolved` and not cleared (Design 4);
- otherwise (`not_found`, or `unresolved` cleared):
  - *deadline* = the later of the end of the period of account containing `d` (the first `period_of_account_end` on or
    after `d`) and the day before the date three calendar months after `first_acquisition` (the last day of "a period of
    3 months beginning with" it; a month without that day uses its last day);
  - *bar* = deadline + 84 days (reg 55's three 28-day periods);
  - `non_reporting` if `list_published` is after the bar, else `non_reporting_provisional`.

`list_published` is the `public_updated_at` of the observation's content fetch. `first_acquisition` is the earliest
acquisition date among the security's lots in the evidence run the observation names.

*Why this rule.* By reg 4(2) a fund is non-reporting unless Part 3 applies, and Part 3 applies to `d` only through an
application received by the deadline and then accepted (regs 54(2), 57). After the deadline no new application can
cover `d`. A list published after the bar has had the unextended reg 55 path to show an acceptance. The first
acquisition bounds the first UK availability from above, so it can only push the deadline later than the true one.

*Residuals, stated:* an extension agreed under reg 55(4)(b); a list whose data predates its publication; the 3d-3c-1
search's own residual (`not_found` is a statement about its tests); a change of accounting date not yet filed. A later
list that shows the fund turns the search to `listed` or `unresolved` (3d-3c-1 keeps either), which ends
`non_reporting` on the next run. `non_reporting_provisional` is the state 3d-3c-3 sees before the bar.

### 4. Clearing an `unresolved` search

An entry of `cleared_list_rows` is `{"list_fetch_id", "cells", "reason"}`: `cells` the nine strings of a stored
example, `reason` a non-empty statement of why the row is not this fund, with a quote where it relies on a document.
For the observation's search key, the search is *cleared* when every search row with a positive count has
`identifier_count` 0, every other positive count is at most 25 (so its examples are complete), and every example of
those rows equals the `list_fetch_id` and `cells` of an entry. An `identifier` match is never cleared. Clearing affects
only Design 3; the 3d-3c-1 observation is unchanged.

### 5. Storage (migration `sql/493_tax_fund_determinations.sql`)

- `tax_fund_fetches.source` gains `sec_governing_document` (request key = URL, unique for that source), stored through
  the existing `tax_fund_documents`.
- `tax_fund_determinations`, insert only: `determination_id TEXT PRIMARY KEY`, `policy_id`, `cik`, `version INTEGER`,
  `file_sha256` (64 hex), `body JSONB`, `loaded_at`; UNIQUE `(cik, version)`.
- `tax_fund_determination_observations`, insert only: `observation_id` identity, `observed_at`, `policy_id`,
  `tax_security_id`, `fund_observation_id` (FK `tax_fund_observations`), `determination_id` (FK, NULL for
  `not_candidate` and `no_determination`), `outcome` (CHECK on Design 2's codes), `refusal TEXT` (the mismatched field,
  the missing document, the failing quote, or the unreviewed accession), `offshore_fund BOOLEAN` and
  `transparent_fund TEXT` (non-NULL exactly when `determined`), `document_fetch_ids BIGINT[]` (non-NULL from
  `quote_unverified` on); UNIQUE `(observed_at, tax_security_id)`.

Matching reads none of these tables in this slice.

## Local policy (frozen under `fund_determination_v1`)

The question list and answers; `offshore_fund`'s rule; the URL form; `document_text`; quote matching by substring
after white-space collapse; the outcome table and its order; the governing-form list; staleness from the earliest
reviewed filing; the deadline, the 84-day bar and the first-acquisition bound; the clearing rule. Changing any of
these is a new policy id.

## Not in this slice

- **3d-3c-3:** non-reporting fund disposals: relevant-securities identification, the basic gain and its character,
  and how a figure is published while transparency is unresolved or the status provisional.
- **3d-3c-4:** reporting funds and the London-listed UCITS ETFs (their 3d-3c-1 outcome is `cik_unestablished`).
- A determination for any fund other than SPY, and SPY before 2017-06-16.
- Settling transparency (the evidence that would is named above).

## Tests

Pure:
1. Format: every required key; unknown question or answer refused; a non-`unresolved` answer without a quote refused;
   a quote naming an unknown `doc_id` refused; a URL whose CIK or accession digits disagree refused; the committed SPY
   file validates.
2. `offshore_fund`: true for SPY's rows; false for limb `none`, any condition `not_met` and D `met`; unresolved for
   each `unresolved` input.
3. `document_text`: tags and entities; `script` content dropped; white-space collapse; a quote split by a tag and a
   line break matches; invalid UTF-8 refused.
4. Outcome table: one case per row in order, including a binding differing only in ticker case under `symbol_key`
   (matches) and in ISIN (mismatch); staleness by an unreviewed 497 and not by an N-PORT-P or N-30D.
5. `reporting_status`: SPY's 2026-09-23 case (deadline 2026-12-17, bar 2027-03-11, provisional); published the day
   after the bar (`non_reporting`) and on it (provisional); a date after 30 September (next year's period); a first
   acquisition on the 31st (month-end rule); `before_valid_from`; `listed`; `unavailable`.
6. Clearing: all examples cleared; one uncleared; a count above 25; an `identifier` count.

DB (one module):
7. Insert-only triggers; a changed file under a stored id fails the step; a document fetched once and not again; a
   failed fetch stores nothing; each Design 2 outcome recorded with its refusal; matching unchanged after the step.

## Acceptance (dev DB)

1. Deploy, then run one `tax_broker_evidence_refresh`.
2. Expected: one `tax_fund_determinations` row (`fund_determination_v1:0000884394:1`, the committed file's hash);
   nine `sec_governing_document` fetches, one per cited URL; determination observations SPY `determined`
   (`offshore_fund` true, `transparent_fund` `unresolved`), IUSA.L and CSPX.L `not_candidate`.
3. `reporting_status` for SPY's 2026-09-23 disposal from the stored rows: `non_reporting_provisional`, deadline
   2026-12-17, bar 2027-03-11. If a newer list or governing filing has arrived, rerun the Script and re-derive.
4. Matching: run 6's three fund characters unchanged in the effective run.

## Full-population verification

The Script checks every quote of the only determination against its documents, and applies Design 3 to every disposal
of every fund security with a candidate observation (one security, one disposal). No population claim rests on a
sample.

## Security

No order or authorisation path changes. The step reads public SEC documents (no account; operator rule 2026-10-10),
bounded and without redirects, and parses them with `html.parser`, which neither resolves external entities nor
fetches. Determinations are committed files reviewed in their PR; the step refuses a changed file under a stored id.
Matching is unchanged.
