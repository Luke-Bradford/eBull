# Reviewed fund determination (#3616 slice 3d-3c-2)

Source: #3616 slice 3d-3c, as 3d-3c-1 split it (`2026-10-14-3616-slice3d3c1-fund-identity.md`, "Not in this slice").
3d-3c-1 captures structured evidence and records a *candidate identity* and a *list search* per fund. Its
checkpoint 1 held that structured data cannot establish the s355 limb and residence, s356's conditions, UK opacity or
non-reporting status at a disposal. This slice supplies those as a **reviewed, versioned determination** per fund: a
committed document that answers each statutory question from the fund's governing documents, says for each answer
whether it is *established*, *assumed* (resting on named assumptions) or *unresolved*, quotes the passages it relies
on, and is checked by code against hashed copies of its sources before anything may use it.

**What this slice decides and builds.**
- The determination format and policy (`fund_determination_v1`), and SPY's first determination
  (`2026-10-14-3616-slice3d3c2-spy-determination-v1.json`, beside this spec; the build moves it unchanged). Its legal
  sources are committed as snapshots under `docs/specs/metrics/sources/3616-slice3d3c2/` (Crown copyright, Open
  Government Licence).
- A step that loads determinations, captures the SEC documents and filing list they need, verifies every hash and
  quote, and records per fund security whether a determination applies (Design 2).
- A pure rule giving a fund's reporting status at a disposal date, and what it rests on (Design 3), which 3d-3c-3
  calls.

It changes no character, no matching run and no figure. Every fund stays withheld as `exchange_traded_fund` (3d-3a).

**The outcome for SPY.**
- **Offshore fund: `assumed`.** A mutual fund (s356 A–C met, s357 condition D not met) under which property is held on
  trust (s355(1)(b)). Its trustee's non-UK residence rests on two assumptions about where it is managed and acts (A1,
  A2). The terms rest on the missing 1993 Indenture not varying them (A3). No other s359 exclusion was found (A4).
- **Reporting status at its 2026-09-23 disposal: `non_reporting_assumed`.** By reg 4(2) a fund is non-reporting unless
  Part 3 applies, and HMRC's list shows no approval. No evidence can *establish* that no application covering the date
  has been or will be accepted, so the status rests on A5 as well as A1–A4.
- **Transparency (reg 11): `unresolved`.** So whether SPY's gains are offshore income gains (reg 17) or chargeable
  gains (reg 29) is unresolved.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3c2_determination.py`, read-only, run on 2026-10-14. Its output is
`var/research/3616/slice3d3c2/population.txt` (loop worktree). It validates the draft (Design 1), reduces each saved SEC
document with `document_text` and checks its hash and every quote, hashes each committed legal snapshot, and then, in
one REPEATABLE READ snapshot of the dev DB, applies Design 2 (reading the CIK's filing list from the saved submissions
JSON) and Design 3 to every fund security. It exits non-zero on any failed check.

The SEC documents were fetched from EDGAR on 2026-10-14 (`var/research/3616/slice3d3c2/spy/index.txt`: URL, accession,
form, date, body SHA-256). Two fetches of the same document on 2026-10-14 gave identical bytes, including SEC's
appended `script` element; the determination binds the hash of `document_text`, which drops it.

1. **The governing documents.**
   - **Prospectus** dated 2026-01-26, 485BPOS `0001193125-26-022316`. The Rule 497(b) filing `0001193125-26-022775`
     (2026-01-26) is the same prospectus: comparing sentences, the only ones it has that the 485BPOS lacks are its
     three filing-header lines. `0001193125-26-023648` (2026-01-27) is a Singapore offering wrapper incorporating the
     prospectus; its additional sentences are Singapore listing, dealing and tax information.
   - **Standard Terms**: the Amended and Restated Standard Terms and Conditions of Trust, effective 2004-01-27
     (485APOS EX-99.A1, `0000950136-04-000194`), and Amendments 1–7, each read in full:
     - 1: Section 3.04, special dividends;
     - 2: the Exchange, Depository and NSCC definitions, successors, and Section 10.06 addresses;
     - 3: the names of the Trust, and "SPDRs" renamed "Units" throughout (Sections 10.03, 10.06, Exhibit A named);
     - 4: the Trustee definition, State Street Global Advisors Trust Company from 2017-06-16;
     - 5: settlement days in Sections 2.03, 3.04 (the record date) and 5.02 (transfer on redemption);
     - 6: T+1 settlement in Sections 2.03, 3.04 and 5.02, and redemption requests made to the "Distributor and
       Trustee";
     - 7: the Trust's name, from 2026-01-26.
     None changes what the determination quotes Sections 2.01, 2.02, 3.01(a), 10.04(a) and (d) and 10.05 for. Quotes
     on redemption and income timing are taken from the 2026 prospectus, which states the amended terms.
   - The **Trust Indenture and Agreement** of 1993-01-22 and its 1996 amendment are not on EDGAR: the CIK's filing
     list begins 1996-01-25. The Standard Terms let an Indenture vary them for its series (assumption A3).
   - No CUSIP or ISIN appears in these documents. The binding to an ISIN is 3d-3c-1's candidate chain.
2. **Sources verify.** The draft has no format error; all 12 documents and 29 authority snapshots match their hashes;
   every quote, contrary quote and assumption evidence quote occurs in its document (Script: `format errors: []`,
   `source failures: 0`, `pending hashes: 0`).
3. **SPY's filings since the newest reviewed 485BPOS** (submissions JSON, 275 recent filings, no overflow files):
   485BPOS and 497 on 2026-01-26, 497 on 2026-01-27, N-CEN on 2026-03-13 (all four reviewed), N-PORT-P on 2026-02-26,
   2026-05-28 and 2026-08-28, and N-30D on 2026-05-29. `filing_events` on dev holds SPY's filings only to its last
   ingest on 2026-09-12, which is why Design 2 reads the filing list itself.
4. **SPY on dev.** Newest observation 1 (2026-10-14 19:34:05Z): `candidate`, CIK 0000884394, LEI
   549300NZAMSJ8FXPQQ63, ISIN US78462F1030, MIC ARCX, symbol SPY, search `not_found`. IUSA.L and CSPX.L are
   `cik_unestablished`.
5. **Script, Designs 2 and 3:** SPY `determined`; `offshore_fund` `assumed`; disposal 2026-09-23
   `non_reporting_assumed`, resting on A1–A5. IUSA.L and CSPX.L `not_candidate`.
6. **HMRC's general list of foreign entities** (INTM180030) has seven USA rows: partnerships under the Uniform
   Partnership Act and the Uniform Limited Partnership Act, LLC, LLP, S corporation, REIT (opaque) and LLLP. It has
   none for a New York trust or a unit investment trust.

## Source rules

Each is quoted from its committed snapshot (`docs/specs/metrics/sources/3616-slice3d3c2/<id>.txt`, URL and fetch time
on its first lines; legislation.gov.uk's current revised text, and HMRC manual pages with their update dates).

- **Offshore fund.** TIOPA 2010 s355(1): a mutual fund (a) constituted by a body corporate resident outside the UK,
  (b) "under which property is held on trust for the participants where the trustees of the property are not resident
  in the United Kingdom", or (c) constituted by other arrangements creating rights in the nature of co-ownership under
  the law of a territory outside the UK. IFM12228: (c) covers arrangements "not constituted by bodies corporate or
  where property is held on trust".
- **Mutual fund.** s356: conditions A (participants share in the property or its profits or income), B (no day-to-day
  control; a right to be consulted or to give directions is not control, s356(5); IFM12233) and C (a reasonable
  investor expects to realise "on a basis calculated entirely, or almost entirely, by reference to" NAV or an index).
  IFM12237: "It is not necessary that the investor obtains NAV directly from the fund"; ETFs whose quoted prices are at
  or near NAV "because market makers are able to create or redeem units" qualify. IFM12282 says the same of ETFs.
- **Exceptions.** s357(1): not a mutual fund only if condition D *and* condition E or F are met. D: realisation on a NAV
  or index basis is expected "only in the event of the winding up, dissolution or termination". IFM12242:
  "open-ended arrangements ... cannot come within the exceptions provided by s357". s359 lets the Treasury make
  further exclusions by regulations. s360 treats each part of umbrella arrangements, and s361 each class of interest,
  as separate arrangements; IFM12270 on classes.
- **Residence.** ITA 2007 s475 and TCGA 1992 s69: the trustees are one person, UK resident if all trustees are UK
  resident, or if they are mixed and a settlor condition holds; a non-UK trustee acting "in the course of a business
  which [it] carries on in the United Kingdom through a branch, agency or permanent establishment" is assumed UK
  resident for those conditions (s475(6), s69(2D)). INTM120030: a company is UK resident if incorporated in the UK or
  if "the central management and control of its business is in the UK".
- **Reporting status.** reg 4(2): "In a period of account an offshore fund is a non-reporting fund unless it is a fund
  to which Part 3 ... applies". Part 3 applies after HMRC accept an application, from the first day of the first period
  of account it names (reg 57(1)–(2)). An application is due before the later of the end of that period and "the
  expiry of a period of 3 months beginning with the first day on which interests in the fund are made available to
  investors resident in the United Kingdom" (reg 54(2)). HMRC respond within 28 days, may ask for further information
  due within 28 days "or within such longer period as is agreed by HMRC", then decide within 28 days (reg 55). A
  rejection quashed on appeal makes the fund reporting as if accepted (reg 56; IFM12426). Under reg 57(4), after a
  withdrawal under reg 54(3)(b) "the fund and its participants shall be treated as if the fund had never been a
  reporting fund". HMRC's list records approved reporting funds and is updated monthly.
- **Relevant securities.** TCGA 1992 s106A(10)(c): "securities which are interests in a non-reporting fund, within the
  meaning of the Offshore Funds (Tax) Regulations 2009 ... (see regulation 4(2))".
- **Transparency.** reg 11: a fund is transparent if UK-resident individual holders are chargeable, under a provision
  in ITTOIA 2005 s830(2), on sums forming part of the fund's income referable to their interests ((a)), or would be if
  UK-source income came from assets outside the UK ((b)). reg 29(1): no reg 17 charge on an interest in a s40A(2)(b) or
  (c) fund that is transparent, unless (2) it held more than 5% by value in other non-reporting funds at any time in the
  holding (with (4)'s exclusion of qualifying transparent funds), or (3) it is non-reporting and fails to make
  sufficient information available. IFM12254: in a transparent fund "investors are entitled to income as it arises".
  INTM180040: establish the members' rights "under the relevant foreign commercial law" and then apply the relevant UK
  charging provision; "How the foreign entity is taxed in another country is not relevant"; a general list entry may
  not hold for a particular entity. INTM180060: clearance.
- **No source rule** fixes how a determination records reasoning, how its sources are bound, which filings make it
  stale, or how a quote is matched in an HTML filing. Designs 1 and 2 fix these by construction under the policy id.

## The determination for SPY

The JSON is authoritative: each finding carries its answer, basis, assumptions, authorities, quotes (with locators),
contrary quotes, reasoning and what would resolve it. This section summarises it for review.

- **Arrangement: `single_arrangement_single_class`, assumed (A3).** One trust issuing one kind of Unit, each "a unit of
  fractional undivided interest in the Trust". 3d-3c-1's candidate already requires the N-CEN to report one series.
- **s355 limb: `b`, assumed (A1, A2, A3).** The Trustee "holds and will hold the Trust Fund as Trustee for the use and
  benefit of all present and future Beneficial Owners" (2.01), under New York law (10.05). One trustee since
  2017-06-16, organised in Massachusetts with its principal place of business in Boston, so not UK-incorporated. Its
  central management and control (A1) and the absence of a UK branch acting for the Trust (A2) are not shown by the
  documents. Limb (a) does not apply (no body corporate), and limb (c) is not an alternative for property held on
  trust (IFM12228).
- **s356 A, B, C: `met`, assumed (A3).** Holders own undivided interests in the portfolio and receive its income net
  of expenses (A). They have no right to control management; their votes on termination and amendment are directions
  (B, s356(5)). They cannot redeem below a Creation Unit (quoted as contrary evidence) but realise in the secondary
  market, which Authorized Participants keep near NAV by creating and redeeming at NAV (C, IFM12237).
- **s357 D: `not_met`, assumed (A3).** "The Trust continuously issues and redeems" at NAV, so realisation on a NAV
  basis is not confined to termination. s357(1) needs D, so it cannot apply whatever E and F would say.
- **s359: `none_found`, assumed (A4).** SI 2009/3001 makes no exclusion from the definition; no complete list of
  s359 instruments was consulted.
- **Transparent fund: `unresolved`.** For: each holder is "vested with all right, title and interest in the Trust
  Fund" (10.04(a)) and units are "an undivided ownership interest in Portfolio Securities". Against: income accrues "to
  the Trust" (3.01(a)), is held and reduced by expenses, and is paid as a distribution entitlement to holders of record.
  The distribution mechanics do not by themselves decide whether the underlying income belongs to holders as it
  arises. INTM180040 decides it from New York commercial law and then reg 11(a) and (b); no source held states that
  analysis. **Would resolve:** that provision-specific analysis with citable authority for the New York-law step, or
  HMRC's view of this fund (INTM180060).
- **reg 29(3) sufficient information, reg 29(2) holdings: `unresolved`.** Each matters only if the fund is transparent.

**Assumptions** (JSON `assumptions`, each with its evidence and residual): A1 trustee central management and control
not in the UK; A2 no UK branch acting for the Trust; A3 the 1993 Indenture does not vary the quoted terms; A4 no s359
exclusion; A5 no accepted application for a period containing the disposal date (Design 3).

**The consequence, for 3d-3c-3.** The identification follows from non-reporting status alone: s106A(10)(c) makes an
interest in a non-reporting fund a relevant security, and transparency does not change reg 4(2) status. Whether the
gain is an offshore income gain or a chargeable gain is unresolved, because reg 29 turns on transparency. If SPY were
transparent, its holders would also be charged on income as it arises, which bears on 3d-3b. Every figure 3d-3c-3
produces for SPY therefore rests on A1–A5 at least. Whether and how such a figure is published is 3d-3c-3's decision.

## Design

### 1. The determination format (`fund_determination_v1`)

A JSON document committed under `app/services/tax_fund_determinations/`, one file per version, read with duplicate
keys refused. Its keys are exactly:
- `determination_id` = `fund_determination_v1:<cik>:<version>`; `policy_id`; `cik` (ten digits); `version` (an
  integer ≥ 1, not a boolean).
- `binding`: `cik` (equal to `cik`), `lei` (`[A-Z0-9]{18}[0-9]{2}`), `isin` (`[A-Z]{2}[A-Z0-9]{9}[0-9]`), `mic`
  (`[A-Z0-9]{4}`), `ticker` (non-blank; compared under `symbol_key`). It is the 3d-3c-1 candidate the review used.
- `valid_from`, `reviewed_at`: ISO dates, `valid_from` ≤ `reviewed_at`. `valid_from` is the date of the newest
  prospectus reviewed; the determination applies to disposal dates on or after it.
- `refresh_forms`: forms whose new filing makes the determination stale, a superset of the governing forms (485APOS,
  485BPOS, 485BXT, 497, 497J, 497K, N-CEN, N-CEN/A, N-8F, N-8F/A). A version that resolves a finding from accounts
  or portfolio reports (reg 29) adds N-30D, N-CSR and NPORT-P.
- `reviewed_filings`: unique by accession, each `{accession, form, filed, documents}` with `form` a governing form,
  `filed` ≤ `reviewed_at`, and every `doc_id` naming a document of that accession.
- `documents`: unique `doc_id`, each `{doc_id, url, accession, form, filed, role, text_sha256}`. The URL is
  `https://www.sec.gov/Archives/edgar/data/<cik as integer>/<accession digits>/<one path segment>`. `text_sha256` is
  the SHA-256 of `document_text(body)` encoded UTF-8.
- `authorities`: each `{authority_id, url, version, text_sha256}`; the snapshot `<authority_id>.txt` is committed, its
  first line names the URL, and `text_sha256` is the SHA-256 of its bytes.
- `assumptions`: each `{assumption_id, statement, evidence, residual}`, `evidence` a list of quotes.
- `findings`: exactly one per question below, each `{question, answer, basis, assumptions, authorities, quotes,
  contrary, reasoning, would_resolve}`. A quote is `{doc_id, locator, text}` with a non-blank locator and text that
  is non-empty after collapsing white space.

| question | answers |
|---|---|
| `arrangement` | `single_arrangement_single_class`, `unresolved` |
| `s355_limb` | `a`, `b`, `c`, `none`, `unresolved` |
| `s356_condition_a`, `_b`, `_c` | `met`, `not_met`, `unresolved` |
| `s357_condition_d` | `met`, `not_met`, `unresolved` |
| `s359_exclusion` | `none_found`, `applies`, `unresolved` |
| `transparent_fund` | `transparent`, `opaque`, `unresolved` |
| `reg29_sufficient_information` | `made_available`, `not_made_available`, `unresolved` |
| `reg29_nonreporting_holdings` | `within_limit`, `exceeded`, `unresolved` |

Basis rules: `unresolved` answer if and only if `unresolved` basis, which needs `would_resolve`; `assumed` needs at
least one assumption; `established` has none. Every answer other than `unresolved` needs a quote, except
`s359_exclusion`, which is about legislation and cites its assumption. Every finding names at least one authority.

`offshore_fund(det)`:
- `false` if `s355_limb` is `none`, any s356 condition is `not_met`, or `s359_exclusion` is `applies`;
- otherwise `unresolved` unless the limb is `a`, `b` or `c`, all three s356 conditions are `met`, D is `not_met`,
  the arrangement is `single_arrangement_single_class` and `s359_exclusion` is `none_found` (D `met` is `unresolved`:
  E and F are not modelled);
- otherwise `assumed` if any of those findings is assumed, else `established`.

A file is immutable once merged: the step stores its SHA-256 and refuses a file whose id is stored with a different
hash. The review record is the merged PR (this spec's checkpoint 1, the review bot) that adds it.

### 2. Applicability, per fund security per step run

A step `refresh_fund_determinations(conn)` runs after `refresh_fund_evidence` in both invokers of
`run_tax_matching`, under the same advisory-lock pattern. It:
1. loads and validates every determination file (an invalid file, or a stored id with another hash, fails the step);
2. fetches each cited document not yet stored, once, and the CIK's submissions JSON
   (`https://data.sec.gov/submissions/CIK<cik>.json`) when its newest fetch is over 7 days old: the fund step's SEC gate
   and user agent, no redirects, HTTP 200 only, at most 8 MiB, strict UTF-8. A failed fetch stores nothing;
3. for each fund security, reads its newest `tax_fund_observations` row and records one determination observation.

The outcome, first line that applies:

| outcome | condition |
|---|---|
| `not_candidate` | the newest fund observation is not `candidate` |
| `no_determination` | no stored determination has the candidate's CIK |
| `binding_mismatch` | no version for the CIK has a binding equal to the candidate's (CIK, LEI, ISIN, MIC exactly; ticker under `symbol_key`) |
| `documents_unavailable` | a cited document of the selected version has no stored fetch |
| `document_changed` | a stored document's `document_text` hash differs from the version's `text_sha256` |
| `quote_unverified` | a quote, contrary quote or evidence quote, white space collapsed, is not in its document's `document_text` |
| `coverage_unavailable` | no submissions fetch within 30 days, or it is unsound (not an object with `filings.recent` as equal-length `form`, `filingDate` and `accessionNumber` lists), or it lists overflow `files` |
| `stale` | the newest submissions fetch lists a filing of a `refresh_forms` form, dated on or after the earliest `reviewed_filings` date, that is not reviewed |
| `determined` | otherwise |

The selected version is the greatest version whose binding equals the candidate's. A `stale` determination needs a
new version that reads the new filing; until then nothing may use it. Legal sources are bound by hash and reviewed at
`reviewed_at`; a change in the law is detected only when a new version is reviewed, which a new 485BPOS (filed each
January for SPY) forces. That is a stated residual.

`document_text(body)`: decode strict UTF-8; parse with `html.parser` (character references decoded once); drop
`script` and `style` content; replace each block-level tag boundary (`p`, `div`, `br`, `li`, `tr`, `td`, `th`,
`table`, headings and the other tags listed in the Script) with a space and drop other tags without a space; collapse
every run of white space to one space; trim.

### 3. Reporting status at a disposal date

`reporting_status(det, fund_observation, d)`, pure, for a `determined` security:
1. `before_valid_from` if `d` < `valid_from`;
2. `offshore_fund_unresolved` or `not_offshore_fund` unless `offshore_fund(det)` is `established` or `assumed`;
3. `reporting_status_unestablished` (reason `list_match`) if the search outcome is `listed` or `unresolved`: a match on
   any stored list version says nothing yet about the reporting interval at `d`, which 3d-3c-4 assesses;
4. `reporting_status_unestablished` (reason `search_unavailable`) if it is `unavailable`;
5. otherwise (`not_found`), `non_reporting_assumed`.

It returns the status with the assumptions it rests on: those of the findings `offshore_fund` used, and A5 for
`non_reporting_assumed`. There is no `non_reporting_established`: no evidence available can show that no application
covering `d` has been or will be accepted (A5's residual names the routes: reg 54(2) timing, an agreed reg 55
extension, a reg 56 appeal, list lag). A later list showing the fund turns the search to `listed` or `unresolved`,
which 3d-3c-1 keeps, so the next run gives `reporting_status_unestablished`.

### 4. Storage (migration `sql/493_tax_fund_determinations.sql`)

- `tax_fund_fetches.source` gains `sec_governing_document` and `sec_submissions`, both through `tax_fund_documents`;
  a unique index on `request_key` (the URL) where `source = 'sec_governing_document'`.
- `tax_fund_determinations`, insert only: `determination_id TEXT PRIMARY KEY`, `policy_id`, `cik`, `version`,
  `file_sha256`, `body JSONB`, `loaded_at`; UNIQUE `(cik, version)`; CHECKs that `determination_id` is
  `policy_id:cik:version` and that `body`'s `determination_id`, `cik` and `version` equal the columns.
- `tax_fund_determination_observations`, insert only: `observation_id` identity, `observed_at`, `policy_id`,
  `tax_security_id`, `fund_observation_id` (FK), `determination_id` (FK; NULL exactly for `not_candidate` and
  `no_determination`), `outcome` (CHECK on Design 2's codes), `refusal TEXT` (the mismatched field, document,
  quote, coverage reason or unreviewed accession), `submissions_fetch_id` (FK, NULL before `coverage_unavailable` is
  reached), `offshore_fund TEXT` and `transparent_fund TEXT` (non-NULL exactly when `determined`; `offshore_fund` one
  of `established`, `assumed`, `unresolved`, `false`); UNIQUE `(observed_at, tax_security_id)`. A trigger checks
  that `tax_security_id` equals the fund observation's.
- `tax_fund_determination_documents`, insert only: `(observation_id, doc_id)` primary key, `fetch_id` (FK, source
  `sec_governing_document`, request key equal to the document's URL in the determination body, by trigger), written
  for every document checked.

Matching reads none of these tables in this slice.

## Local policy (frozen under `fund_determination_v1`)

The format, its questions and answers, and the basis rules; `offshore_fund`'s rule; the URL form; `document_text`;
quote matching by substring after white-space collapse; the outcome table and its order; version selection; the
governing forms; staleness from the earliest reviewed filing date; the 7-day refresh and 30-day currency of the
filing list; Design 3's order. Changing any of these is a new policy id.

## Not in this slice

- **What clears an `unresolved` list search** (3d-3c-1 assigned it here). No fund has one: SPY's search is
  `not_found`. A clearing rule must restore the completeness `not_found` requires and verify its own evidence; it is
  written with the first fund that needs it, under a new policy id.
- **3d-3c-3:** non-reporting fund disposals: relevant-securities identification, the basic gain, its character, and
  how a figure resting on assumptions and an unresolved character is published.
- **3d-3c-4:** list matches and reporting funds, including the London-listed UCITS ETFs (`cik_unestablished`).
- Determinations for other funds; SPY before 2026-01-26; settling transparency.

## Tests

Pure:
1. Format: the committed SPY file validates. Refused: a missing or extra key; a duplicate JSON key; an id
   disagreeing with policy, CIK or version; a boolean version; an invalid date or `valid_from` after `reviewed_at`;
   `refresh_forms` missing a governing form; a URL whose CIK or accession digits disagree or with a nested path; a
   duplicate `doc_id` or reviewed accession; a reviewed document of another accession; an unknown question or answer;
   a missing question; `unresolved` answer with another basis; `assumed` with no assumption; `established` with one;
   `unresolved` without `would_resolve`; a non-`unresolved` answer without a quote; an unknown assumption, authority
   or `doc_id`; a blank locator; a quote of only white space.
2. `offshore_fund`: SPY's rows give `assumed`; all established gives `established`; limb `none`, a condition
   `not_met` or an `applies` exclusion give `false`; D `met` gives `unresolved`; each `unresolved` input gives
   `unresolved`.
3. `document_text`: a quote split by `<b>` inside a word matches without a space; a quote across `<p>` matches with
   one; `&amp;amp;` decodes to `&amp;`; `script` content dropped; NBSP collapses; invalid UTF-8 refused.
4. Outcome table: one case per row in order, including a ticker differing only in case (matches), an ISIN differing
   (mismatch), two versions of which only the older binds (the older is selected), a changed document hash, an
   unverified contrary quote, a submissions fetch 31 days old, an unsound submissions body, overflow `files`, and
   staleness from an unreviewed 497 but not from an N-PORT-P or N-30D.
5. `reporting_status`: SPY's 2026-09-23 case (`non_reporting_assumed`, resting on A1–A5); `before_valid_from`;
   `offshore_fund` `unresolved` and `false`; `listed`; `unresolved`; `unavailable`; an all-established determination
   resting on A5 alone.

DB (one module):
6. Insert-only triggers; the determination CHECKs; a changed file under a stored id fails the step; a document fetched
   once and not again; a failed fetch stores nothing; the submissions refresh after 7 days; each Design 2 outcome
   recorded with its refusal and document rows; the security trigger; matching unchanged after the step.

## Acceptance (dev DB)

1. Before deployment, record the row counts of the three new tables (empty) and the `sec_governing_document` and
   `sec_submissions` fetch counts (zero).
2. Deploy, run one `tax_broker_evidence_refresh`, and record its `job_runs` id.
3. Expected for that run: one `tax_fund_determinations` row, `fund_determination_v1:0000884394:1`, whose
   `file_sha256` is the committed file's; 12 `sec_governing_document` fetches whose `document_text` hashes equal the
   JSON's; one `sec_submissions` fetch; determination observations SPY `determined` (`offshore_fund` `assumed`,
   `transparent_fund` `unresolved`) with 12 document rows, IUSA.L and CSPX.L `not_candidate`. If SPY has filed a
   governing form since 2026-10-14, the expected SPY outcome is `stale`.
4. `reporting_status` for SPY's 2026-09-23 disposal from the stored rows: `non_reporting_assumed`, resting on A1–A5.
5. Matching: the effective run's characters, identifications and dependencies are unchanged by the step (compared
   before and after).

## Full-population verification

The Script checks every hash and quote of the only determination and applies Designs 2 and 3 to every fund security
with an observation (three securities, one disposal). This verifies the determination against its sources and the
rules against the stored rows; it does not verify the legal reasoning, which is this spec's checkpoint 1.

## Finding dispositions

Checkpoint 1 round 1 (`var/research/3616/slice3d3c2/ckpt1_r1_final.txt`, loop worktree), `r1.k`:

| findings | disposition |
|---|---|
| r1.1 (residence assumed) | `basis` per finding; residence rests on A1 and A2; `offshore_fund` is `assumed`, never `established` |
| r1.2 (s357) | D `met` gives `unresolved` |
| r1.3, r1.39 (84-day bar) | removed; non-reporting is only ever `assumed` (A5, with reg 56 added) |
| r1.4, r1.10, r1.14, r1.15 (availability bound, month arithmetic, period of account) | removed with the deadline; no period-of-account finding |
| r1.5 (ever-listed) | `listed` and `unresolved` give `reporting_status_unestablished` (`list_match`); 3d-3c-4 assesses intervals |
| r1.6, r1.28 (clearing) | removed from this slice; a clearing rule must restore completeness and verify its evidence |
| r1.7 (interval) | `valid_from` is the newest reviewed prospectus's date, 2026-01-26; staleness covers later filings |
| r1.8, r1.29 (storage) | `offshore_fund` four-valued text; observation/security trigger; a document junction table; determination CHECKs |
| r1.9 (prerequisites) | Design 3 refuses unless `offshore_fund` is `established` or `assumed` |
| r1.11 (limb (c)) | removed; IFM12228 cited |
| r1.12 (Indenture) | assumption A3 on every finding resting on the Standard Terms |
| r1.13 (amendments) | each amendment's sections listed; redemption and income-timing quotes taken from the 2026 prospectus |
| r1.16 (reg 29(2), (4)) | `reg29_nonreporting_holdings`, unresolved |
| r1.17, r1.18 (transparency) | 10.04(a) added; distribution mechanics not decisive; reg 11(a) and (b); a list entry is supporting only |
| r1.19 (s359) | `s359_exclusion`, assumed (A4), with the search boundary stated |
| r1.20, r1.37 (source binding) | `text_sha256` per document; legal snapshots committed with URL headers and hashes; s69 recaptured whole |
| r1.21 (evidence types) | `authorities` separate from SEC `documents` |
| r1.22 (reviewed filings) | structured, each with its documents |
| r1.23 (reasoning) | reasoning, assumptions, contrary evidence and `would_resolve` in the JSON |
| r1.24 (refresh triggers) | governing forms required; accounts and portfolio forms added by a version resolving a reg 29 finding |
| r1.25 (coverage) | the step fetches the CIK's submissions JSON; `coverage_unavailable`; law-change residual stated |
| r1.26 (versions) | selection by binding |
| r1.27 (validation) | Design 1's invariants and test 1 |
| r1.30–r1.33 (Script) | the Script validates, applies Design 2 in order, derives the boundary from the draft, uses `symbol_key` and exits non-zero |
| r1.34 (normalisation) | references decoded once; inline tags join, block tags separate |
| r1.35, r1.36 (tests, acceptance) | rewritten; acceptance scoped to the run with a baseline and a matching comparison |
| r1.38 (reg 57(4)) | quoted |
| r1.40 (quotes) | locators on every quote; fuller sentences |
| r1.41 (timestamp) | converted to UTC |

## Security

No order or authorisation path changes. The step reads public SEC documents and SEC's submissions JSON (no account;
operator rule 2026-10-10), bounded and without redirects, and parses them with `html.parser` and `json`, neither of
which resolves external entities or fetches. Determinations are committed files reviewed in their PR; the step refuses
a changed file under a stored id. Matching is unchanged.
