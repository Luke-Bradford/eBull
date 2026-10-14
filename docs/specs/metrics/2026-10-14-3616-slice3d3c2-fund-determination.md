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
  Government Licence); the build stores them content-addressed (Design 1).
- A step that loads determinations, verifies their authorities, captures the SEC documents and filing list they need,
  verifies every hash and quote, and records per fund security whether its determination applies, under a run that
  consumers use only if it succeeded (Design 2).
- A pure rule giving a fund's reporting status at a disposal date and the assumptions it rests on (Design 3), which
  3d-3c-3 calls.

It changes no character, no matching run and no figure. Every fund stays withheld as `exchange_traded_fund` (3d-3a).

**The outcome for SPY.**
- **Offshore fund: true, assumed.** A mutual fund (s356 A–C met, s357 condition D not met) under which property is held
  on trust (s355(1)(b)). The trustee's non-UK residence rests on where it is managed and acts (A1, A2); the terms rest
  on the unlocated 1993 Indenture not varying them (A3); no other exclusion was found (A4).
- **Reporting status at its 2026-09-23 disposal: `non_reporting_assumed`**, resting on A1–A9. By reg 4(2) a fund is
  non-reporting unless Part 3 applies. No available evidence can *establish* that Part 3 does not apply at a date
  (A5); the 3d-3c-1 list search found no matching row.
- **Transparency (reg 11): unresolved**, so whether SPY's gains are offshore income gains (reg 17) or chargeable gains
  (reg 29) is unresolved.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3c2_determination.py`, read-only, run on 2026-10-14. Its output is
`var/research/3616/slice3d3c2/population.txt`, with per-source diagnostics in `population_sources.txt` (loop
worktree). It prints its run time, its own SHA-256, the determination file's SHA-256 (bytes), the invocation and the
submissions JSON's SHA-256 and fetch date. It validates the draft (Design 1), stops on a format error, reduces each
saved SEC document with `document_text`, checks every hash and quote, and then, in one REPEATABLE READ snapshot of the
dev DB, applies Designs 2 and 3 to every fund security observed in the selected fund run. It exits non-zero on any failed check.

The SEC documents were fetched from EDGAR on 2026-10-14 (`var/research/3616/slice3d3c2/spy/index.txt`). Two fetches of
one document on that day gave identical bytes, including SEC's appended `script` element; the determination binds the
hash of `document_text`, which drops it.

1. **The governing documents.**
   - **Prospectus** dated 2026-01-26, 485BPOS `0001193125-26-022316`. The Rule 497(b) filing `0001193125-26-022775`
     is the prospectus as filed: compared by sentence both ways, the 497 adds three filing-header lines, and the
     485BPOS adds its Form S-6 facing sheet and N-8B-2 cross-reference sheet (its first ~20,000 characters) and its
     Part II (from its back cover on). `0001193125-26-023648` (2026-01-27) is a Singapore offering wrapper
     incorporating the prospectus; its additional sentences are Singapore listing, dealing and tax information.
   - **Standard Terms**: the Amended and Restated Standard Terms and Conditions of Trust, effective 2004-01-27
     (`0000950136-04-000194`), which "wholly incorporate" the 1993 Standard Terms as amended five times, and
     Amendments 1–7 since, each read in full:
     - 1: Section 3.04, special dividends;
     - 2: the Exchange, Depository and NSCC definitions, successors, and Section 10.06 addresses;
     - 3: the Trust's names, and "SPDRs" renamed "Units" throughout;
     - 4: the Trustee definition, State Street Global Advisors Trust Company from 2017-06-16;
     - 5: settlement days in Sections 2.03, 3.04 (record date) and 5.02 (transfer on redemption);
     - 6: T+1 settlement in Sections 2.03, 3.04 and 5.02, and redemption requests made to the "Distributor and
       Trustee";
     - 7: the Trust's name, from 2026-01-26.
   - Two earlier amendments located on EDGAR were also read: 1997-09-30 (`0000950130-97-004319`) replaced Trust
     Indenture Section 2F (taxable year to 30 September) and amended the Distributor definition and Section 3.05;
     1999-01-25 (`0000950130-99-000390`) added a misweighted-portfolio paragraph to Section 5.02.
   - None of these changes what the determination quotes Sections 2.01, 2.02, 3.01(a), 10.04(a) and (d) and 10.05 for.
     Quotes on redemption, income timing and investment are from the 2026 prospectus, which states the amended terms.
   - The **Trust Indenture and Agreement** of 1993-01-22 and its 1996-01-19 amendment were not located in the reviewed
     corpus. The searched boundary is the CIK's EDGAR filing list, which begins 1996-01-25 (assumption A3).
   - No CUSIP or ISIN appears in these documents. The ISIN binding is 3d-3c-1's candidate chain.
2. **Sources verify** (Script): no format error; 14 documents and 34 authority snapshots match their hashes; all 35
   quotes, contrary quotes and evidence quotes occur in their sources. The N-CEN is bound by its bytes, every other
   document by its `document_text`.
3. **SPY's filings since 2026-01-26** (submissions JSON, 275 filings, no overflow files): 485BPOS and 497 on
   2026-01-26, 497 on 2026-01-27, N-CEN on 2026-03-13 (all reviewed), NPORT-P on 2026-02-26, 2026-05-28 and
   2026-08-28, N-30D on 2026-05-29. `filing_events` on dev holds SPY's filings only to its last ingest, 2026-09-12,
   which is why the step reads the filing list itself.
4. **SPY on dev.** Newest fund observation 1 (2026-10-14 19:34:05Z): `candidate`, CIK 0000884394, LEI
   549300NZAMSJ8FXPQQ63, ISIN US78462F1030, MIC ARCX, symbol SPY, search `not_found`. IUSA.L and CSPX.L are
   `cik_unestablished`.
5. **Script, Designs 2 and 3:** SPY `determined`; offshore fund true, assumed (A1–A4); disposal 2026-09-23
   `non_reporting_assumed`, resting on A1–A9. IUSA.L and CSPX.L `not_candidate`.
6. **INTM180030** has seven USA rows: partnerships under the Uniform Partnership Act and the Uniform Limited
   Partnership Act, LLC, LLP, S corporation, REIT (opaque) and LLLP. It has none for a New York trust or a unit
   investment trust.

## Source rules

Each is quoted from its committed snapshot (`docs/specs/metrics/sources/3616-slice3d3c2/<id>.txt`; URL and fetch time
on its first lines; legislation.gov.uk's current revised text, HMRC manual pages with their update dates).

- **Offshore fund.** TIOPA 2010 s355(1): a mutual fund (a) constituted by a body corporate resident outside the UK,
  (b) "under which property is held on trust for the participants where the trustees of the property are not resident
  in the United Kingdom", or (c) constituted by other arrangements creating rights in the nature of co-ownership under
  the law of a territory outside the UK. IFM12228: (c) covers arrangements "not constituted by bodies corporate or
  where property is held on trust".
- **Mutual fund.** s356: conditions A (participants share in the property or its profits or income), B (no day-to-day
  control; a right to be consulted or to give directions is not control, s356(5); IFM12233) and C (a reasonable
  investor expects to realise "on a basis calculated entirely, or almost entirely, by reference to" NAV or an index).
  IFM12237: "It is not necessary that the investor obtains NAV directly from the fund"; ETFs whose quoted prices are at
  or near NAV "because market makers are able to create or redeem units" qualify; IFM12282 likewise.
- **Exceptions.** s357(1): not a mutual fund only if condition D *and* condition E or F are met. D: realisation on a NAV
  or index basis is expected "only in the event of the winding up, dissolution or termination". IFM12242:
  "open-ended arrangements ... cannot come within the exceptions provided by s357". s359: the Treasury may exclude
  arrangements by regulations. s360 and s361: each part of umbrella arrangements, and each class of interest, is
  separate; IFM12270 on classes.
- **Residence.** ITA 2007 s475 and TCGA 1992 s69: the trustees are one person, UK resident if all trustees are UK
  resident, or if they are mixed and a settlor condition holds; a non-UK trustee acting "in the course of a business
  which [it] carries on in the United Kingdom through a branch, agency or permanent establishment" is assumed UK
  resident for those conditions (s475(6), s69(2D)). INTM120030: a company is UK resident if incorporated in the UK or
  if "the central management and control of its business is in the UK"; INTM120060 states the case-law test (De Beers).
- **Reporting status.** reg 4(2): "In a period of account an offshore fund is a non-reporting fund unless it is a fund
  to which Part 3 ... applies". If HMRC accept an application, the fund becomes a reporting fund on the later of the
  first day of the first period of account named in it and the day the fund is established (reg 57(1)), and Part 3
  then applies until it ceases under Chapter 11 (reg 57(2)–(3)). The application is due before the later of the end of
  that first period and "the expiry of a period of 3 months beginning with the first day on which interests in the fund
  are made available to investors resident in the United Kingdom" (reg 54(2)). HMRC respond within 28 days and may ask
  for further information, due within 28 days "or within such longer period as is agreed by HMRC" (reg 55). A
  rejection quashed on appeal makes the fund reporting as if accepted (reg 56; IFM12426). After a withdrawal under
  reg 54(3)(b), "the fund and its participants shall be treated as if the fund had never been a reporting fund"
  (reg 57(4)). HMRC's list records approved reporting funds and is updated monthly.
- **Relevant securities.** TCGA 1992 s106A(10)(c): "securities which are interests in a non-reporting fund, within the
  meaning of the Offshore Funds (Tax) Regulations 2009 ... (see regulation 4(2))".
- **Transparency.** reg 11: a fund is transparent if UK-resident individual holders are chargeable, under a provision
  in ITTOIA 2005 s830(2), on sums forming part of the fund's income referable to their interests ((a)), or would be if
  UK-source income came from assets outside the UK ((b)). reg 29(1): no reg 17 charge on an interest in a s40A(2)(b) or
  (c) fund that is transparent, unless (2) it held more than 5% by value in other non-reporting funds at any time in the
  holding ((4) disregards qualifying transparent funds), or (3) it is non-reporting and fails to make sufficient
  information available. IFM12254: in a transparent fund "investors are entitled to income as it arises". INTM180040:
  establish the members' rights "under the relevant foreign commercial law" and apply the relevant UK charging
  provision; "How the foreign entity is taxed in another country is not relevant"; a general list entry may not hold
  for a particular entity. INTM180060: clearance.
- **No source rule** fixes how a determination records reasoning, binds its sources, which filings make it stale, or
  how a quote is matched in an HTML filing. Designs 1 and 2 fix these by construction under the policy id.

## The determination for SPY

The JSON is authoritative: each finding carries its answer, basis, assumptions, authorities, quotes and contrary
quotes (with locators), reasoning and what would resolve it. This section summarises it for review.

- **Arrangement: `single_arrangement_single_class`, assumed (A3).** One trust issuing one kind of Unit; "Each unit of
  fractional undivided interest in the Trust is referred to as a “Unit”." 3d-3c-1's candidate requires one series.
- **s355 limb: `b`, assumed (A1, A2, A3).** The Trustee "holds and will hold the Trust Fund as Trustee for the use and
  benefit of all present and future Beneficial Owners" (2.01), under New York law (10.05). One trustee since
  2017-06-16, organised in Massachusetts with its principal place of business in Boston, so not UK-incorporated. Where
  its highest-level control is actually exercised (A1, INTM120060, including control by its parent) and whether it
  acts for the Trust through a UK branch (A2) are not shown. Limb (a) does not apply (no body corporate), and limb (c)
  is not an alternative for property held on trust (IFM12228).
- **s356 A, B, C: `met`, assumed (A3).** Holders own undivided interests in the portfolio and receive its income net
  of expenses (A). They have no right to control management; their votes on termination and amendment are directions
  (B, s356(5)). They cannot redeem below a Creation Unit, but realise in the secondary market, which the
  creation/redemption feature "is designed to make it likely" trades close to NAV, though disruptions or volatility
  "may result in trading prices that differ significantly" (both quoted). The normal case the arrangements are designed
  for is IFM12237's condition C.
- **s357 D: `not_met`, assumed (A3).** The Trust "continuously issues and redeems" Creation Units "at their once-daily
  NAV", so realisation on a NAV basis is not confined to termination; s357(1) needs D.
- **Other exclusions: `no_applicable_exclusion`, assumed (A4).** Investment bond arrangements (s355(4); TCGA s151W,
  ITA s564U; defined in ITA s564G) and regulations under s359. s564G was read but not applied in full: its subsection
  (2) allows in-kind and reduced redemption payments and the Trust has a termination date, so neither settles it, and
  conditions (1)(e) and (1)(i) were not analysed. The s359 search covered SI 2009/3001's contents (snapshot
  `ofr2009_contents`). Both rest on A4.
- **Transparent fund: `unresolved`.** For: "each Beneficial Owner thereof shall be deemed to be a beneficiary of the
  Trust ... and vested with all right, title and interest in the Trust Fund" (10.04(a)), and units are "an undivided
  ownership interest in Portfolio Securities". Against: income accrues "to the Trust" (3.01(a)), is held and reduced by
  expenses, and the holders of record on the ex-dividend date "are entitled to receive an amount representing dividends
  accumulated" over the quarter. Those mechanics do not by themselves decide whether the underlying income belongs to
  holders as it arises. INTM180040 decides it from New York commercial law and then reg 11(a) and (b); no source held
  states that analysis. **Would resolve:** that provision-specific analysis with citable authority for the New
  York-law step, or HMRC's view of this fund (INTM180060).
- **reg 29(3) sufficient information and reg 29(2) holdings: `unresolved`.** Both are holding-dependent, which policy
  v1 does not model (Design 1); both matter only if the fund is transparent. The JSON records the evidence for (2): the
  prospectus says the Trust "only holds constituent securities of the Index" and "is not authorized to invest in ...
  any other registered or unregistered funds", with the section 12(d)(1) ceilings as contrary evidence.

**Assumptions** (JSON, each with evidence and residual): A1 trustee control not in the UK; A2 no UK branch acting for
the Trust; A3 the 1993 Indenture and its 1996 amendment do not vary the quoted terms; A4 no other exclusion. Policy
assumptions, carried by every determination and added by Design 3: A5 Part 3 does not apply to the Trust at the
disposal date; A6 the committed legal sources state the law in force at the disposal date; A7 the units disposed of
are interests in the class bound today at every date from `valid_from` to the disposal; A8 no governing filing on or
after `monitor_from` and before the disposal or cutoff is missing from the inventory read; A9 no change to the
governing terms that would change a finding took effect before the disposal without appearing in the reviewed
documents, whether a change of terms or a fact or action under existing powers such as a change of trustee (Amendment 6,
filed 2025-01-27, made changes effective in 2024).

**The consequence, for 3d-3c-3.** Identification follows from non-reporting status alone: s106A(10)(c) makes an
interest in a non-reporting fund a relevant security, and transparency does not change reg 4(2) status. Whether the
gain is an offshore income gain or a chargeable gain is unresolved, because reg 29 turns on transparency. If SPY were
transparent, its holders would also be charged on income as it arises, which bears on 3d-3b. Every figure for SPY
rests on A1–A9 at least. Whether and how such a figure is published is 3d-3c-3's decision.

## Design

### 1. The determination format (`fund_determination_v1`)

A JSON document committed under `app/services/tax_fund_determinations/`, read as UTF-8 bytes with duplicate keys
refused. Validation is total: a value of the wrong type is a format error, never an exception. Every object's keys are
exactly those listed. Document and authority ids share one namespace, `[a-z0-9_]+`, unique; assumption ids are
`A<n>_[a-z0-9_]+`, unique.
- `determination_id` = `fund_determination_v1:<cik>:<version>`; `policy_id`; `cik` (ten digits); `version`, the
  integer 1. **Policy v1 holds one version per CIK**: how a later version takes over or confirms an earlier one's dates
  is not in this slice.
- `binding`: strings `cik` (equal to `cik`), `lei` (`[A-Z0-9]{18}[0-9]{2}`), `isin` (`[A-Z]{2}[A-Z0-9]{9}[0-9]`),
  `mic` (`[A-Z0-9]{4}`), `ticker` (non-blank; compared under `symbol_key`): the 3d-3c-1 candidate the review used.
- Dates (ISO, valid): `valid_from_document` names the newest reviewed document whose canonical form is `485BPOS`, and `valid_from` is
  its `filed`; `reviewed_at` ≥ `valid_from`; `review_by` is `reviewed_at` one year on (29 February maps to 28 February);
  `monitor_from` = `valid_from`. Every document's `filed` is ≤ `reviewed_at`.
- `refresh_forms`: strings, a superset of the governing forms (485APOS, 485BPOS, 485BXT, 497, 497J, 497K, N-CEN,
  N-CEN/A, N-8F, N-8F/A).
- `reviewed_filings`: the complete review manifest, `{accession, form, filed, documents}`, one per accession read, of
  any form; every document belongs to exactly one entry, with the same accession, form and date.
- `documents`: `{doc_id, url, accession, form, exhibit, filed, role, content_sha256}`, strings except `exhibit` (the
  exhibit type, or null for a primary document); `form` is the filing's canonical form and equals its manifest entry's. The URL parses to scheme
  `https`, host `www.sec.gov`, no query, fragment or parameters, and path `/Archives/edgar/data/<cik as
  integer>/<accession digits>/<non-empty segment>`. `content_sha256` is the SHA-256 of the body's bytes for a path
  ending `.xml` (whose attributes carry its facts) and of `document_text(body)` encoded UTF-8 otherwise.
- `authorities`: `{authority_id, url, version, content_sha256}`, the URL `https` with a host, the hash the SHA-256 of
  the snapshot's bytes (a hash is 64 hex digits; `PENDING` is accepted only by the Script's `--fill` draft mode); the snapshot's
  first line names the URL (or, for an HMRC manual page, its content-API URL).
- `assumptions`: `{assumption_id, statement, evidence, residual}`, non-blank prose, `evidence` a list of quotes. The
  policy assumptions `A5_part3_not_applying`, `A6_law_current`, `A7_identity_continuity`, `A8_inventory_complete` and
  `A9_terms_continuity` are required.
- `findings`: one per question, `{question, answer, basis, assumptions, authorities, quotes, contrary, reasoning,
  would_resolve}`. A quote is `{source_id, locator, text}`, `source_id` a document or an authority, locator and text
  non-blank after collapsing white space.

| question | answers |
|---|---|
| `arrangement` | `single_arrangement_single_class`, `unresolved` |
| `s355_limb` | `b`, `none`, `unresolved` (v1 supports trust arrangements only; limbs (a) and (c), with s355(2)'s partnership and s355(3)'s LLP qualifications, are a successor policy's) |
| `s356_condition_a`, `_b`, `_c` | `met`, `not_met`, `unresolved` |
| `s357_condition_d` | `met`, `not_met`, `unresolved` |
| `other_exclusions` (s359, s355(4)) | `no_applicable_exclusion`, `applies`, `unresolved` |
| `transparent_fund` | `transparent`, `opaque`, `unresolved` |
| `reg29_sufficient_information`, `reg29_nonreporting_holdings` | `unresolved` only (holding-dependent) |

Hashes are lowercase hexadecimal. A document URL is ASCII printable characters only (U+0021 to U+007E), equals its canonical parsed form, and its last path segment
is not `.` or `..` and contains no percent-encoding (authority URLs are provenance labels, never fetched by the step).

Basis rules: an `unresolved` answer if and only if an `unresolved` basis, which needs `would_resolve`; `assumed` needs
an assumption, other bases have none; every resolved answer has a quote; every finding has reasoning and an authority.

`offshore_fund(det)` returns an answer, a basis and the assumptions it rests on:
- **false** if `s355_limb` is `none`, any s356 condition is `not_met`, or `other_exclusions` is `applies`: basis
  `established` (no assumptions) if any such finding is established, else `assumed` with the assumptions of the first
  such finding in the table's question order;
- otherwise **unresolved**, basis `unresolved`, unless the limb is `b`, the s356 conditions are `met`, D
  is `not_met`, the arrangement is single and `other_exclusions` is `no_applicable_exclusion` (D `met` is unresolved:
  E and F are not modelled);
- otherwise **true**: `assumed` with the union of those findings' assumptions if any is assumed, else `established`.

**Immutability.** A determination file is immutable once merged: the step stores its SHA-256 and refuses a file whose
id is stored with another hash. Authority snapshots are stored by content hash and never replaced. The review record is
the merged PR (this spec's checkpoint 1 and the review bot) that adds the file.

### 2. Applicability

A step `refresh_fund_determinations(conn, fund_observed_at)` runs after `refresh_fund_evidence` in both invokers of
`run_tax_matching`, under the same advisory-lock pattern. `fund_observed_at` is the `observed_at` the fund step wrote in
the same job run, or none if that step failed or wrote nothing.
1. It inserts and commits a `tax_fund_determination_runs` row (`run_id`, the job's `job_runs` id, `fund_observed_at`,
   `started_at`, `status` `running`). Its *cutoff* is the clock reading after step 3's fetches. A cited document's
   evidence is its newest fetch, by `(fetched_at, fetch_id)` with `fetched_at` ≤ cutoff, **whose content hash is the
   version's**; the filing list's is its newest fetch with `fetched_at` ≤ cutoff, whatever its content.
2. It loads every determination file and its authority snapshots and validates both (Design 1, each snapshot's hash
   and URL line), and records each loaded determination against the run; only those are *active* for the run (a stored
   determination whose file is gone is not). Any failure, or a stored id with another hash, sets the run `failed` with
   the reason and raises.
3. It fetches, through the fund step's SEC gate and user agent, no redirects, HTTP 200 only, at most 8 MiB, strict
   UTF-8 for non-XML: each cited document with no stored fetch of the expected hash (at most once per URL per run),
   and the CIK's submissions JSON (`https://data.sec.gov/submissions/CIK<cik>.json`) when no fetch is stored or its
   newest is over 7 days old (elapsed time). A failed fetch stores nothing.
4. In one transaction it records one observation per fund security (below), with its document rows, and sets the run
   `succeeded` with the cutoff. Code checks first that every referenced fetch has `fetched_at` ≤ cutoff and every fund
   observation `observed_at` = the run's `fund_observed_at`.

The newest run is the greatest `run_id`. Consumers use only the newest run, and only if it `succeeded`; otherwise every
security is refused with `run_failed`. A finished run is sealed: no row is added under it, and no run is deleted. The outcome per security, first line that applies:

| outcome | condition |
|---|---|
| `fund_observation_unavailable` | no fund observation with `observed_at` = `fund_observed_at` |
| `not_candidate` | the fund observation is not `candidate` |
| `no_determination` | no active determination has the candidate's CIK |
| `binding_mismatch` | its binding differs from the candidate's (CIK, LEI, ISIN, MIC exactly; ticker under `symbol_key`) |
| `documents_unavailable` | a cited document has no stored fetch with `fetched_at` ≤ cutoff |
| `document_changed` | it has such fetches, none with the version's content hash |
| `quote_unverified` | a quote, contrary quote or evidence quote, white space collapsed, is not in its source's text (`document_text` for a document; the snapshot, white space collapsed, for an authority) |
| `review_out_of_date` | the cutoff is before `reviewed_at` or after `review_by` (a version is used for nothing after its review deadline until a successor policy reviews it again) |
| `coverage_unavailable` | the newest submissions fetch is missing, dated after the cutoff, or older than 30 days at it; or unsound: not an object whose `cik` is the CIK, whose `filings.files` is a list and whose `filings.recent` has equal-length `form`, `filingDate` and `accessionNumber` lists with non-blank forms, valid dates no later than the fetch's `fetched_at` UTC date (itself no later than the cutoff) and unique accession numbers; or `files` is non-empty; or a reviewed filing on or after `monitor_from` is absent or listed with another form or date |
| `stale` | an unreviewed governing accession is recorded for the determination (below) |
| `determined` | otherwise |

**Unreviewed accessions.** Recording a discovery needs less than admitting a determination. Right after reading the
newest submissions fetch, before any observation and in its own committed transaction, the run records in
`tax_fund_determination_unreviewed` each listed filing of a `refresh_forms` form filed on or after `monitor_from` and
not in `reviewed_filings`, whenever the body is an object whose `cik` is the CIK and whose `filings.recent` lists are
of equal length, and the row's form, date and accession are well formed and its date is no later than the submissions
fetch's `fetched_at` UTC date (a future-dated row is recorded nowhere; coverage refuses its inventory). Currency, overflow
`files` and the reviewed anchors are not required for this; they are required for `determined`. Each record carries the
`submissions_fetch_id` and the run that found it, which must be `running` and have loaded the determination. Records are
permanent and survive a later failure of the run, so staleness latches even if a
later inventory omits the accession, an earlier-priority refusal hid it, or the discovering inventory was incomplete or
old.

The submissions JSON is SEC's own filing inventory for the CIK. That it is complete, and that nothing was filed between
its fetch and the disposal or cutoff, is assumption A8, which Design 3 carries. A stale determination is used for
nothing until a successor policy reviews the new filing (Not in this slice).

`document_text(body)`: decode strict UTF-8; parse with `html.parser` (character references decoded once); drop
`script` and `style` content; a block-level tag boundary (the Script's `BLOCK_TAGS`: `p`, `div`, `br`, `li`, `tr`,
`td`, `th`, `table`, headings, SGML `document`/`text`/`type` and the rest) becomes a space, any other tag joins its
neighbours; collapse every run of white space (Unicode, so NBSP too) to one space; trim.

### 3. Reporting status at a disposal date

`reporting_status(security, d, evaluated_at)`, pure, over the newest run. Every time is a timezone-aware instant
normalised to UTC; `reviewed_at`, `review_by` and filing dates are UTC calendar dates, compared inclusively.
1. `no_run` if there is none; `run_failed` if it did not succeed; `not_observed` if it has no row for the security
   (no older row is used);
2. the security's outcome if it is one of the first four (they carry no determination);
3. `review_out_of_date` if `evaluated_at`'s date is after the determination's `review_by`, however recent the run (a
   run admitted before the deadline is not reused after it); this precedes every other determination-bearing outcome;
4. the security's outcome if it is any other non-`determined` outcome;
5. `outside_interval` unless `valid_from` ≤ `d` ≤ the run's cutoff date;
6. `not_offshore_fund` if `offshore_fund` is false, `offshore_fund_unresolved` if unresolved;
7. `reporting_status_unestablished`, reason `list_match`, if the fund observation's search is `listed` or
   `unresolved` (a match on a stored list version; 3d-3c-4 assesses the reporting interval);
8. `reporting_status_unestablished`, reason `search_unavailable`, if it is `unavailable`;
9. otherwise (`not_found`), `non_reporting_assumed`.

From step 6 on it returns the assumptions it rests on: those of `offshore_fund`, A6, A7, A8 and A9, and A5 for
`non_reporting_assumed`. There is no established non-reporting status: no available evidence can show that Part 3 does
not apply at `d`, given reg 57(3)'s continuation, reg 54(2)'s timing, an agreed reg 55 extension, a reg 56 appeal, the
list's lag and the search's own residual (A5). A later list version showing the fund under the same search key turns
the search to `listed` or `unresolved`, which 3d-3c-1 keeps; a changed ISIN or name key starts a new key, and A5's
residual covers that.

### 4. Storage (migration `sql/493_tax_fund_determinations.sql`)

- `tax_fund_fetches.source` gains `sec_governing_document` and `sec_submissions`, both through `tax_fund_documents`.
- `tax_fund_determination_runs`: `run_id` identity, `job_run_id` (FK `job_runs`), `fund_observed_at` (NULL when the
  fund step failed), `started_at`, `cutoff` (NULL until `succeeded`), `status` (`running`, `succeeded`, `failed`),
  `failure TEXT`; a trigger allows only `running` → `succeeded` or `failed`, once, and refuses deletion.
- `tax_fund_determination_run_loads`: primary key `(run_id, determination_id)`, each a foreign key to its parent: the
  run's active determinations.
- `tax_fund_determination_unreviewed`, insert only, committed in its own transaction while its run is `running`:
  primary key `(determination_id, accession)`, `form`, `filed`, `first_run_id` and `submissions_fetch_id`;
  `(first_run_id, determination_id)` is a foreign key to `tax_fund_determination_run_loads` and `submissions_fetch_id`
  one to its fetch; a trigger checks that the fetch's source is `sec_submissions` with the determination's CIK URL as
  request key, and that `filed` is no later than the fetch's `fetched_at` UTC date.
- Every child table of a run (loads, discoveries, observations, document rows) has a trigger refusing an insert under
  a run that is not `running`, and refusing update and delete. A committed discovery outlives the run's later failure.
- `tax_fund_determinations`, insert only: `determination_id TEXT PRIMARY KEY`, `policy_id`, `cik` UNIQUE, `version`
  (CHECK = 1), `file_sha256`, `body JSONB`, `loaded_at`; CHECKs that `determination_id` is `policy_id:cik:version` and
  that the body's `determination_id`, `policy_id`, `cik` and `version` equal the columns.
- `tax_fund_authority_snapshots`, insert only: `content_sha256 TEXT PRIMARY KEY`, `body BYTEA`, CHECK the hash is the
  body's. `tax_fund_determination_authorities`, insert only: `(determination_id, authority_id)` primary key,
  `determination_id` (FK) and `content_sha256` (FK); code checks it equals the body's.
- `tax_fund_determination_observations`, insert only: `observation_id` identity, `run_id` (FK), `policy_id`,
  `tax_security_id`, `fund_observation_id` (FK; NULL only for `fund_observation_unavailable`), `determination_id`
  (FK; NULL exactly for the first four outcomes), `outcome` (CHECK on Design 2's codes), `refusal TEXT`,
  `submissions_fetch_id` (FK; NULL for outcomes before `coverage_unavailable`, or when no fetch exists),
  `offshore_fund` (`true`, `false`, `unresolved`) and `offshore_basis` (`established`, `assumed`, `unresolved`),
  non-NULL exactly when `determined`; UNIQUE `(run_id, tax_security_id)`. Triggers check that `policy_id` equals the
  determination's, and that the determination is loaded for the run; that the fund observation's `tax_security_id` equals this row's, its `observed_at` is the run's
  `fund_observed_at`, and its `policy_id` is `fund_identity_v1` (the one identity policy v1 accepts); and that `submissions_fetch_id`'s source is
  `sec_submissions` with the CIK's URL as request key. Code checks before insert that `offshore_fund` and
  `offshore_basis` equal `offshore_fund(body)`.
- `tax_fund_determination_documents`, insert only: `(observation_id, doc_id)` primary key, `observation_id` (FK) and
  `fetch_id` (FK). No foreign key here cascades; parents are insert-only. A trigger
  checks that `doc_id` is a document of the observation's determination, that the fetch's source is
  `sec_governing_document` and its request key that document's URL; code checks the fetch's content hash equals the
  body's. A `determined` observation has one row per document.

Matching reads none of these tables in this slice.

## Local policy (frozen under `fund_determination_v1`)

The format, its questions, answers and basis rules; trust arrangements only; one version per CIK; the policy assumptions; `offshore_fund`'s
rule; the URL form; `content_sha256` and `document_text`; quote matching by substring after white-space collapse; the
run gate; the outcome table and its order; the 7-day refresh and 30-day currency of the filing list; `review_by` one
year after `reviewed_at`; Design 3's order and interval. Changing any of these is a new policy id.

## Not in this slice

- **Version succession.** How a later determination of the same fund confirms, corrects or supersedes an earlier
  one's dates, how holding-dependent findings are modelled, and limbs (a) and (c). They are designed with SPY's next
  governing filing, under a new policy id; until then a stale v1 withholds SPY. The reviewed recent 485BPOS filings
  were made in January (2025-01-27, 2026-01-26), which suggests, without establishing, the next; a 497 supplement or
  other governing filing can come at any time, and `review_by` (2027-10-14) ends v1 regardless.
- **What clears an `unresolved` list search** (3d-3c-1 assigned it here). No fund has one: SPY's search is
  `not_found`. A clearing rule must restore the completeness `not_found` requires and verify its own evidence.
- **3d-3c-3:** non-reporting fund disposals: identification, the basic gain, its character, and how a figure resting
  on assumptions and an unresolved character is published.
- **3d-3c-4:** list matches and reporting funds, including the London-listed UCITS ETFs (`cik_unestablished`).
- Determinations for other funds; SPY before 2026-01-26; settling transparency.

## Tests

Pure:
1. Format: the committed SPY file validates. Refused, one case each: a wrong type at each level (including a null LEI
   and null `documents`, reported, not raised); a missing or extra key at each level; a duplicate JSON key; a duplicate
   or malformed document, authority or assumption id; an id disagreeing with policy, CIK or version; version 2; a
   boolean version; an invalid date; `valid_from_document` not the newest 485BPOS, or `valid_from` not its date;
   `review_by` not one year on (and 2028-02-29 mapping to 2029-02-28); a document filed after `reviewed_at`;
   `refresh_forms` missing a governing form or holding a number; a URL with another CIK, accession or host, a query,
   a fragment, an empty or nested last segment, a last segment of `.` or `..` or holding percent-encoding; a URL
   holding a space, tab, newline, other ASCII control or DEL, U+0080, U+0085, U+00A0 or U+2003, or not equal to its
   canonical parsed spelling (a URL whose last segment holds `!` and `~`, the printable-ASCII bounds, is accepted); a
   document in no manifest entry or in two; a manifest entry naming a
   document of another accession or date; a malformed hash; a missing policy assumption; an unknown question or
   answer; a missing question; a resolved holding-dependent finding; `unresolved` answer with another basis; `assumed`
   with no assumption; `established` with one; `unresolved` without `would_resolve`; a resolved answer without a
   quote; blank reasoning; an unknown assumption or authority, or a document named as an authority; a blank locator;
   a quote of only white space; blank evidence.
2. `offshore_fund`: SPY gives true, assumed, A1–A4; all established gives true, established; an assumed `not_met`
   gives false, assumed, with its assumptions; an established and an assumed negative together give false,
   established; D `met` gives unresolved, basis unresolved; an unresolved s356 condition gives unresolved; an
   unresolved transparency changes nothing.
3. `document_text` and `content_sha256`: a quote split by `<b>` inside a word matches without a space; across `<p>`
   with one; `&amp;amp;` decodes to `&amp;`; `script` content dropped; NBSP collapses; an SGML `<PAGE>` joins; invalid
   UTF-8 refused; an N-CEN whose `reportEndingPeriod` or `fundsTickerSymbol` attribute changes changes its hash.
4. Outcome table: one case per row in order, including a ticker differing only in case (matches) and an ISIN
   differing; a stored fetch with another hash (refetched once; `document_changed` if the new one differs too); an
   unverified contrary quote and an unverified authority quote; submissions missing, 31 days old, dated after the
   cutoff, naming another CIK, with a string for a list, with unequal lists, with `files` absent, `false` or
   non-empty, with a duplicated or future-dated accession, and listing a reviewed accession under another form or
   date; staleness from an unreviewed 497 but not from an NPORT-P or N-30D absent from `refresh_forms`, and from an
   N-30D present in it; staleness kept when a later inventory omits the accession (latched); an older matching
   document fetch beside a newer mismatching one (`determined`, the matching fetch used) and the reverse; a cutoff
   before `reviewed_at` and after `review_by` (`review_out_of_date`); a determination whose file is gone from the
   deployment (`no_determination`); across two runs, a new 497 in a sound inventory while a cited document is
   unavailable (the accession recorded despite `documents_unavailable`), then the document recovered and the accession
   absent from the next inventory (`stale`); an accession found in an inventory with overflow `files`, and in one 40
   days old, then omitted by an admissible inventory (`stale`); a run that fails after recording a discovery, followed by
   a successful run whose inventory omits it (`stale`); an unreviewed 497 dated after the submissions fetch's date and on
   the cutoff's date (not recorded; `coverage_unavailable`), then a corrected inventory without it (`determined`); a
   submissions fetch exactly 30 days before the cutoff (admitted) and 30 days and one second before it
   (`coverage_unavailable`); two submissions fetches on the cutoff's day, the earlier sound and the later unsound (the
   later is newest: `coverage_unavailable`), and two with equal `fetched_at` (the greater `fetch_id` is newest).
5. `reporting_status`: SPY's 2026-09-23 case (`non_reporting_assumed`, A1–A9); no run (`no_run`); a security absent
   from the newest run (`not_observed`, although an older run has it); a failed newest run (`run_failed`, although an
   older run succeeded); each of the first four outcomes returned before the date test; a date before
   `valid_from` and after the cutoff (`outside_interval`); a `stale` determination passes its
   outcome through; false and unresolved `offshore_fund` (exactly A6–A9 plus the negative finding's assumptions); `listed`; `unresolved`;
   `unavailable`; an all-established determination resting on exactly A5–A9; a run that
   succeeded before `review_by`, evaluated after it with no later run (`review_out_of_date`); `review_out_of_date`
   preceding `stale` and `documents_unavailable`.

DB (one module):
6. Insert-only, transition, deletion and sealing triggers, including `tax_fund_determination_unreviewed` (a row added
   under a finished run refused; a row whose run did not load its determination refused; a row dated after its fetch's
   date refused; a row committed before its run fails survives the failure); the determination and authority CHECKs; a second version for a CIK refused; a
   changed file under a stored id, or an authority snapshot with a wrong hash, sets the run `failed` and leaves no
   observation; a document fetched once and not again; a failed fetch stores nothing; the submissions not refetched at exactly 7
   days old and refetched at 7 days and one second; each Design 2 outcome recorded with its refusal; a `determined` observation with one document row per
   document, and a document row naming a document of another determination refused; the fund-observation policy and
   security triggers, inserted with the real `fund_identity_v1` and `fund_determination_v1` rows; a failed fund step
   gives `fund_observation_unavailable`; matching unchanged after the step.

## Acceptance (dev DB)

1. Before deployment, record the row counts of the eight new tables (zero) and the `sec_governing_document` and
   `sec_submissions` fetch counts (zero).
2. Deploy, run one `tax_broker_evidence_refresh`, and record its `job_runs` id, the determination run and its cutoff.
3. Rerun the Script against the same evidence: its documents, the step's submissions body saved beside them,
   `--fund-observed-at` the run's `fund_observed_at` and `--cutoff` the run's cutoff. On a fresh deployment no earlier
   run exists, so latched staleness cannot differ; the Script compares dates where the step compares timestamps (local
   copies carry fetch dates only), so a fetch on the cutoff's own day is compared as that day. The expected rows are the Script's outcomes. For today's evidence that is: one determination
   (`fund_determination_v1:0000884394:1`, the committed file's hash); 34 authority links; 14 `sec_governing_document`
   fetches whose content hashes equal the JSON's; one `sec_submissions` fetch; run `succeeded`; SPY `determined`
   (`offshore_fund` `true`, `offshore_basis` `assumed`) with 14 document rows; IUSA.L and CSPX.L `not_candidate`;
   `reporting_status` for SPY's 2026-09-23 disposal `non_reporting_assumed` resting on A1–A9. Any other Script
   outcome (a new filing, list match, fetch failure) is expected instead, with its refusal.
4. The timestamp rules the Script compares by date, re-evaluated on the stored rows by query at full precision.
   - Provenance, for every outcome: every fetch the run references has `fetched_at` ≤ cutoff; a non-NULL
     `submissions_fetch_id` is the newest `sec_submissions` fetch for the CIK with `fetched_at` ≤ cutoff; each document
     row's fetch is the newest of its URL with the version's hash and `fetched_at` ≤ cutoff.
   - Applicability: walk Design 2's whole table in order, from `fund_observation_unavailable`, at full precision,
     reading the newest `sec_submissions` fetch for the CIK with `fetched_at` ≤ cutoff (not the observation's column,
     which is NULL for earlier rows); the temporal rows compare timestamps (`reviewed_at` ≤ cutoff's UTC date ≤
     `review_by`; a submissions fetch present, cutoff − `fetched_at` ≤ 30 days, every `filingDate` ≤ its `fetched_at`
     UTC date). The first row that applies is the expected outcome; for `determined`, every preceding refusal predicate is false, so the final `otherwise` row applies.
   - Where the walk and the Script disagree (the Script compares the temporal rows by date), the walk's outcome is
     expected and the disagreement is recorded on the PR.
5. Matching: the effective run's characters, identifications and dependencies, compared before and after, are
   unchanged.

## Full-population verification

The Script checks every hash and quote of the only determination and applies Designs 2 and 3 to every fund security
*observed* in the chosen fund run (three securities, one disposal); a security missing from that run is not found by
it. It is a narrower probe than the step: one determination, date granularity, no run history. It verifies the determination against its sources and the
rules against the stored rows, not the legal reasoning, which is this spec's checkpoint 1.

## Finding dispositions

Rounds 1–3 (`var/research/3616/slice3d3c2/ckpt1_r<n>_final.txt`, loop worktree), `rN.k`:

| findings | disposition |
|---|---|
| r1.1–r1.6, r1.8–r1.15, r1.17–r1.24, r1.28, r1.30, r1.32–r1.35, r1.37–r1.39, r1.41; r2.1, r2.2, r2.4, r2.6, r2.7, r2.9, r2.12, r2.13, r2.17–r2.22, r2.25–r2.32, r2.34, r2.36–r2.38 | resolved (r2 or r3) |
| r1.7, r1.26, r2.5, r3.2, r3.3, r3.4 (history) | policy v1 holds one version per CIK; succession, renewal and correction are a successor policy's (Not in this slice); `review_by` bounds when a version may be used (`review_out_of_date`), and the run's cutoff bounds the disposal dates it answers |
| r1.16, r2.8, r3.11 (reg 29 per holding) | holding-dependent findings must be `unresolved` under v1; the evidence for (2) is recorded in the JSON |
| r1.25, r2.10, r2.11, r3.16 (coverage) | `files` must be a list, timestamps no later than the cutoff, unique accessions, reviewed anchors matched by form and date; completeness of SEC's inventory is a stated residual |
| r1.27, r2.15, r2.16, r3.17, r3.18 (validation) | total validation; id and hash grammars; unique manifest membership; the newest-485BPOS anchor; `review_by` exactly one year on |
| r1.29, r2.23, r3.1, r3.8, r3.23 (storage) | identity-policy check against `fund_identity_v1`; authority content and junction tables; document rows checked for membership and hash |
| r1.31, r2.14, r3.20, r3.25 (Script result) | one-version Script with Design 3's order; A6 and A7 returned for every evaluated status |
| r1.36, r3.24 (acceptance) | expected rows are the Script's outcomes on the step's evidence |
| r1.40, r2.35, r3.26 (quotes) | the 10.04(a), record-date and furnishing sentences quoted whole |
| r2.3, r3.19 (upstream) | the step requires its job run's fund observation; the Script is described as a narrower probe |
| r2.24, r3.6, r3.15 (run identity) | `tax_fund_determination_runs` with status and cutoff; consumers use only a succeeded newest run |
| r2.33, r3.27 (header) | Python version, module and argv as a JSON list |
| r4.1 (document selection) | a document's evidence is its newest fetch with the version's hash; the filing list's is its newest fetch |
| r4.2 (review deadline) | `review_out_of_date` when the cutoff is outside `reviewed_at`..`review_by`; A6's residual restated |
| r4.3 (staleness) | latched across runs |
| r4.4 (inventory) | policy assumption A8, carried by Design 3 |
| r4.5, r4.6 (run binding, sealing) | `fund_observed_at` on the run; newest run by `run_id`; fetch and observation checks before success; sealed runs; no deletion |
| r4.7 (active set) | run loads; only determinations loaded this run are active |
| r4.8 (no run) | `no_run` and `not_observed` |
| r4.9–r4.12, r4.18, r4.20, r4.21, r4.23 (Script) | one cutoff; `--fund-observed-at`; fails on an empty fund run; missing files and lists become refusals; root validated first; `PENDING` only under `--fill`; authority host required; the classification printed only for a `determined` security |
| r4.13 (future review) | covered by `review_out_of_date` |
| r4.14 (forms) | canonical `form` plus `exhibit`; manifest form agreement; the anchor by canonical form |
| r4.15 (limbs) | v1 supports trust arrangements only (`b`, `none`, `unresolved`) |
| r4.16 (exclusions) | partly: ITA s564G, s564U and TCGA s151W read and snapshotted, SI contents snapshotted; their application rests on A4 (r5.1) |
| r4.17 (next filing) | stated: any governing filing can come at any time, and `review_by` ends v1 |
| r4.19 (order) | the table's question order |
| r4.22 (2.01) | quoted whole |
| r5.1 (investment bonds) | the exclusion rests on A4, which now states s564G's unapplied conditions; the incorrect disqualifiers removed |
| r5.2, r5.3 (time, replay) | elapsed-time rules for the step; `--cutoff` for replay; the Script's date granularity and absent run history stated |
| r5.4 (population) | the claim restricted to observed securities |
| r5.5 (source failures) | unreadable or undecodable documents, authorities and index become refusals |
| r5.6 (test) | exactly A5–A9 (r7.4) |
| r5.7, r5.8 (storage, retrieval) | run-loads keys; initial submissions fetch |
| r5.9, r5.10 (grammar) | no `.`, `..` or percent-encoding in the last segment; lowercase hex |
| r5.11, r5.12 (wording) | A2's residual and the January filing pattern restated |
| r6.1 (effective before filing) | policy assumption A9, with Amendment 6 as its evidence |
| r6.2 (reuse after deadline) | Design 3 takes `evaluated_at` and refuses after `review_by` |
| r6.3 (time) | aware instants normalised to UTC; inclusive UTC calendar dates; the Script refuses a naive `--cutoff` |
| r6.4 (source boundary) | one guarded index reader; an empty authority snapshot is refused |
| r6.5 (latch) | unreviewed accessions recorded from every sound inventory, whatever the outcome |
| r6.6, r6.11 (dispositions) | corrected |
| r6.7 (FKs) | stated, none cascading |
| r6.8 (termination) | "a specified lifetime term and termination provisions", with the prospectus's lifetime-term and cash-at-NAV termination sentences quoted (r11) |
| r6.9 (URL rule) | scoped to document URLs |
| r6.10 (population) | the opening states observed securities |
| r6.12 (docstring) | the cutoff override described |
| r7.1 (facts under unchanged terms) | A9 extended to facts and actions under existing powers, trustee identity included |
| r7.2 (order) | Design 3's steps 1–4: run, the first four outcomes, the review deadline, then other refusals |
| r7.3, r7.4, r7.5 (tests) | evaluation-after-deadline, A5–A9 and two-run latch tests; the Script's replay scope stated |
| r7.6 (selector) | `--fund-observed-at` needs an offset and is normalised to UTC |
| r7.7 (URL spelling) | control characters refused; a document URL must equal its canonical form |
| r7.8 (s564H) | `would_resolve` includes s564G(3) and s564H |
| r8.1, r8.2 (latch) | discoveries need only a well-formed body for the CIK, are committed before observations with their submissions fetch and run, and survive run failure |
| r8.3 (URL characters) | ASCII printable only |
| r8.4 (docstring) | corrected |
| r9.1 (sealing) | no exemption: discoveries are sealed like other run children and committed while the run is `running` |
| r9.2 (discovering run) | `(first_run_id, determination_id)` references the run's loads |
| r9.3 (future rows) | a discovery's date must not follow its fetch's UTC date, checked in code and by trigger |
| r9.4 (URL tests) | the character, canonical-spelling, dot-segment and percent-encoding cases are in Test 1 |
| r1.25, r1.36, r2.10, r3.16, r3.24, r4.9, r5.2, r6.3 (Script date granularity, r12) | acceptance step 4 checks the step's timestamp rules on the stored rows, so the Script's date-level replay is not the only comparison |
| r1.7, r1.26, r2.5, r3.2, r3.3 (succession, r12) | deferred by design: with one version and no second governing filing, no succession case exists to measure; v1 fails closed (`stale`, `review_out_of_date`) until a successor policy is written against SPY's next filing |
| r4.16 (exclusions, r12) | carried as assumption A4, `assumed` basis, with the resolving work in `would_resolve`; every outcome that rests on it lists it |
| r13.16 (acceptance vs refusals) | acceptance step 4 splits provenance (every outcome) from applicability (Design 2 walked at full precision; its first applying row is expected); a disagreement with the Script is expected and recorded |
| r14.17 (acceptance precedence) | the acceptance walk starts at Design 2's first row and reads the newest submissions fetch, so earlier refusals keep their precedence; the document and quote refusals with otherwise valid temporal evidence are Test 4 cases, and IUSA.L and CSPX.L give the live `not_candidate` rows |
| r15.18 (wording) | corrected: `determined` is the `otherwise` row |
| r6.3 (boundary cases, r13) | exact 30-day, 30-days-plus-one-second, same-day and equal-timestamp fetch cases in Test 4 |
| r10.1 (future-row boundary) | coverage, discovery and the Script all bound filing dates by the submissions fetch's UTC date |
| r6.13 (index) | Amendment 6's effective dates distinguished in the research index |
| r3.5 (unresolved basis) | `offshore_basis` `unresolved` |
| r3.7 (refusal order) | the first four outcomes are returned before the date test |
| r3.9 (identity continuity) | policy assumption A7 |
| r3.10 (upper bound) | `outside_interval` after the run's cutoff date; use after `review_by` is `review_out_of_date` |
| r3.12 (s355(4)) | `other_exclusions` covers s359 and the investment bond provisions, assumed (A4) |
| r3.13 (mixed negatives) | an established negative wins |
| r3.14 (persistence) | stated for an unchanged search key; a new key is in A5's residual |
| r3.21 (exit status) | the Script exits non-zero when a candidate security with a determination is not `determined` |
| r3.22 (XML) | an `.xml` document is bound by its bytes |
| r3.28 (docstring) | corrected |

## Security

No order or authorisation path changes. The step reads public SEC documents and SEC's submissions JSON (no account;
operator rule 2026-10-10), bounded and without redirects, and parses them with `html.parser` and `json`, neither of
which resolves external entities or fetches. Determinations and their authority snapshots are committed files reviewed
in their PR; the step fails its run on a changed file under a stored id or a snapshot whose hash differs, and consumers
use only a succeeded run. Matching is unchanged.
