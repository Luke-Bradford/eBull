# Fund evidence and candidate identity (#3616 slice 3d-3c-1)

Source: #3616 slice 3d-3c, as 3d-3a named it (`2026-10-14-3616-slice3d3a-tax-character.md`, "Not in this slice").

**The split, revised at checkpoint 1.** The first two drafts admitted a fund to a non-reporting character from
structured data. Checkpoint 1 held, in rounds 1 and 2 (`var/research/3616/slice3d3c1/ckpt1_r<n>_final.txt`), that
structured data cannot establish:
- the s355 limb and residence;
- s356's conditions A and B;
- UK opacity (INTM180040 reads governing documents);
- non-reporting status at a disposal (reg 57).

Rounds 3 and 4 accepted the narrowing below as sound. 3d-3c is now:
- **3d-3c-1** (this): capture the public structured evidence about each held fund, and record two *observations*:
  - its **candidate identity**: a registration, LEI and ISIN joined through one chain (Design 2);
  - its **list search**: whether this policy's tests find the candidate in the captured versions of HMRC's
    reporting-funds list.
  It changes no character, no matching run and no figure. Every fund stays withheld as `exchange_traded_fund`
  (3d-3a).
- **3d-3c-2:** a reviewed, versioned *fund determination* per fund, bound to a 3d-3c-1 candidate. It reads the fund's
  governing documents, residence evidence and status evidence against TIOPA ss355–357, reg 29 and INTM180040. It
  sets the gate on reporting status, and it decides what clears an `unresolved` search. It is a judgement artefact
  with its own checkpoint 1.
- **3d-3c-3:** disposals of a determined non-reporting fund.
- **3d-3c-4:** reporting funds and the London-listed UCITS ETFs.

**The outcome on dev today:** SPY observes `candidate`: LEI 549300NZAMSJ8FXPQQ63, ISIN US78462F1030, N-CEN
`0001410368-26-023904`, listing SPY on ARCX. Its list search gives `not_found`. IUSA.L and CSPX.L observe
`cik_unestablished`. Matching is untouched.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3c1_fund_identity.py`, read-only, run on 2026-10-14.
- It reads the effective evidence generation as `refresh_cover_instances` does (`_read_inputs(bodies=False)`,
  `security_members`), in one REPEATABLE READ snapshot.
- It fetches the evidence, then applies this spec's readers and decisions: `read_ncen`, `read_gleif_record`,
  `read_gleif_isins`, `read_figi`, `read_reporting_list`, `identity_outcome` and `list_observation`.
- It prints every input and outcome to `var/research/3616/slice3d3c1/population.txt`; `universe.txt` holds the
  coverage counts.
- Hand checks are in `checks.txt`. Bodies with SHA-256 are in `var/research/3616/slice3d3c1/<UTC stamp>/`
  (gitignored, loop worktree).
- It measures one fresh capture, and prints the capture preconditions it checks (content item, attachment size,
  HTTP status). It buffers responses rather than streaming them, and has no rate gate or deadline. Stored-history
  behaviour (search reuse, assessment states, outcomes over several versions) is the build's; tests 2, 6 and 7 cover
  it.

1. **Fund securities.** Evidence run 5 has 7 securities; three have only ETF members (`instrument_type_id` 6).
   - SPY (3000): members 3000 and 3417, country US, primary CIK 0000884394.
   - IUSA.L (3075) and CSPX.L (3434): country GB, no primary CIK in `external_identifiers`.
   - In effective matching run 5, all three are `unestablished` `exchange_traded_fund`.
2. **SPY's N-CEN filings.** `filing_events` holds 8 SEC rows for SPY's members, all `N-CEN`, none `N-CEN/A`. Their
   report dates are the 31 Decembers of 2018 to 2025, each before its filing date.
   - The newest is `0001410368-26-023904`, filed 2026-03-13, with report date 2025-12-31.
   - Across dev, all 56 `N-CEN` and 8 `N-CEN/A` rows have `primary_document_url`
     `https://www.sec.gov/Archives/edgar/data/<cik>/<accession digits>/xslFormN-CEN_X01/primary_doc.xml`.
   - The raw XML, the same URL without `xslFormN-CEN_X01/`, has root `{http://www.sec.gov/edgar/ncen}edgarSubmission`.
     Each container the reader uses occurs once. No leaf text the reader uses has surrounding whitespace
     (`checks.txt`).
   - `formData/generalInfo` carries the period as attributes: `reportEndingPeriod` 2025-12-31, equal to the DB
     report date, and `isReportPeriodLt12` N.
   - `read_ncen` gives: LEI 549300NZAMSJ8FXPQQ63 (ISO 17442 mod 97 = 1); one listing, ticker SPY on MIC ARCX;
     `isRegistrantLastFiling` N; trustee country US; registrant country US. It also confirms an S-6
     classification, one series, and a `seriesCIK` equal to the registrant's CIK.
3. **GLEIF.**
   - LEI record (`read_gleif_record`): registration ISSUED, entity ACTIVE, category FUND. Its names are "SPDR S&P 500
     ETF TRUST" and the previous legal name "SPDR Trust, Series 1". Its `successorEntity` is `{"lei": null, "name":
     null}`, and `successorEntities` is empty.
   - Printed beside the reader: `legalForm` `{"id": "8888", "other": "Fund"}`, jurisdiction US-NY, legal address US.
   - ISIN relationship, first page of 200 (`read_gleif_isins`): one entry, US78462F1030 (total 1, last page 1). Its
     ISO 6166 check digit is valid.
   - The NSIN, 78462F103, appears as a CUSIP in `unresolved_13f_cusips`, unbound.
4. **OpenFIGI.** `/v3/mapping`, keyless:
   - The job `{"idType": "ID_ISIN", "idValue": "US78462F1030", "micCode": "ARCX"}` returns one row: ticker SPY,
     `exchCode` UP, composite FIGI BBG000BDTBL9.
   - The same ISIN with `micCode` XNAS, and an unknown ISIN (US0000000002) with `exchCode` US, each return `{"warning":
     "No identifier found."}` (`checks.txt`).
5. **HMRC's list.**
   - **The capture.** The GOV.UK content item "Approved offshore reporting funds" has `public_updated_at`
     2026-10-12T11:30:44+01:00 and one `.ods` attachment, `20261006_Master-Weblist.ods`, with `file_size` 4,549,717.
     The received body is that size. Its zip has five members, and `content.xml` is 147,393,474 bytes.
   - **The reader.** `read_reporting_list`, with no earlier version, finds the list usable: 125,022 data rows and
     15,931,480 characters of logical text.
   - **Structure** (`checks.txt`):
     - `office:spreadsheet` holds one `table:table`. The table's children are 10 `table:table-column` and 125,024
       `table:table-row`, and every row is a direct child of the table.
     - Cells carry only the attributes `office:value-type` (`string`), `table:style-name` and
       `table:number-columns-repeated`. There is no `office:string-value`, no span, no formula and no covered cell.
     - Cells hold only `text:p`. Paragraphs hold only empty `text:s` elements, 3,411 of them, with `text:c` at most
       17.
     - The longest cell text is 185 characters before `text:s` expansion.
     - The only repeated row is a trailing empty row repeated 923,553 times. Empty cells repeat up to 16,384 times.
   - **Header:** Letter Group, Reporting Fund Ref, Parent Fund, HMRC Share Class Ref No.’s, Sub Fund Name, ISIN No,
     CUSIP No, "Reporting Fund, with effect from", "Ceased to be an RF on".
   - **Rows.** Every data row has a Reporting Fund Ref `[A-Z][0-9]{4}` and a non-empty Parent Fund or Sub Fund Name.
     125,021 share-class refs have the form `[A-Z][0-9]{4}-[0-9]{4}`; one is `J00550013`.
   - **ISIN cells**, read with ODF text semantics:
     - 106,461 are a valid ISIN exactly;
     - 186 are valid after trimming;
     - 184 have ISIN shape and a wrong check digit;
     - 52 are other 12-character strings;
     - 306 are other text;
     - 17,833 read `no data`.
     So 18,375 rows have no valid ISIN after trimming.
   - **CUSIP cells:** 123,517 `no data`, 1,152 of CUSIP shape, 353 other. 17,610 rows have `no data` in both
     identifier columns, 8,084 of them with no cessation date.
   - 577 ISIN cells start `US`.
   - **SPY.** `list_observation` gives `not_found`, with no match of any kind. Its name keys are `SPDRSP500` and
     `SPDR1`. Nine rows name "SPDR S&P 500" in their fund names. All are share classes of "SSGA SPDR ETFs Europe I
     plc" with valid IE ISINs, which the name test skips (Design 2).
6. **Coverage** (`universe.txt`): 3,365 ETF instruments. 41 have a primary SEC CIK, all US, and 4 of those have an
   N-CEN in `filing_events`. A missing CIK or filing is a DB coverage fact, not evidence about the fund.

## Source rules

- **Identifiers.**
  - ISIN (ISO 6166): `[A-Z]{2}[A-Z0-9]{9}[0-9]`, with the check digit by Luhn over the letters-to-digits string. The
    check digit does not catch every error: a letter expands to two digits, so US78462N1030 is as valid as
    US78462F1030, and so is US78462F1048. The prefix is the country code of the issuer or of the allocating agency.
  - For a US or Canadian ISIN, the NSIN is the CUSIP.
  - LEI (ISO 17442): `[A-Z0-9]{18}[0-9]{2}`, with the whole string, letters converted, mod 97 = 1.
  - CIK: one to ten digits, compared and stored zero-padded to ten.
  - MIC (ISO 10383): a venue's four-character code.
- **GLEIF** (LEI-CDF 3.1).
  - Registration statuses: `PENDING_VALIDATION`, `ISSUED`, `DUPLICATE`, `LAPSED`, `MERGED` (deprecated), `RETIRED`,
    `ANNULLED`, `CANCELLED`, `TRANSFERRED`, `PENDING_TRANSFER`, `PENDING_ARCHIVAL`.
  - The ISIN relationship is the GLEIF/ANNA mapping of ISINs to issuer LEIs. It is issuer-level: one LEI can carry
    many share classes.
- **N-CEN** is the annual census report of a registered investment company (rule 30a-1).
  - Its `generalInfo` gives the reporting period's end, and whether the period is shorter than 12 months.
  - Item B.4 marks the registrant's last filing.
  - Its `exchangeTradedFund` item gives each listing's exchange (as a MIC) and ticker, and the authorised
    participants (those with an agreement to order creation units).
  - An `N-CEN/A` is a complete amended report.
- **OpenFIGI.** A mapping request carries up to 10 jobs keyless, each an identifier with optional filters, among them
  `micCode`. The response is a list in job order, each item `data` or `warning`/`error`. Keyless use allows 25
  requests a minute, and the response headers report the remaining budget and reset time.
- **HMRC's list** is the published record of approved reporting funds, with ceased funds kept.
  - Under reg 57, approval makes a fund reporting from the first day of the period its application names.
  - Under reg 54(2) (IFM12424), that application can arrive up to the end of that period.
  - So the list is published evidence of approvals and their dates. A search of it records what a captured version
    contains; it does not itself decide a fund's status at a date, least of all from a fund's absence.
- **ODF** (OpenDocument 1.3, part 3).
  - §6.1.2 white space, applied to a paragraph and its descendants:
    1. `text:span` and `text:a` are replaced by their content;
    2. tab, CR and LF in character data become spaces;
    3. leading and trailing spaces of the result are removed;
    4. runs of spaces collapse to one;
    5. `text:s` (`text:c` spaces, default 1), `text:tab` and `text:line-break` are then read as the white space they
       represent.
  - `text:span` and `text:a` contribute their text.
  - A cell's string value may instead sit in `office:string-value`.
- **No source rule** joins an exchange ticker to an ISIN, or decides which list rows could be a fund. Design 2 fixes
  both by construction, and freezes them under a policy id.

## Design

### 1. Capture

**Step and population.** A step `refresh_fund_evidence(conn)` runs after `refresh_cover_instances` in both invokers of
`run_tax_matching` (3d-3a Design 5).
- It reads the effective evidence generation as cover refresh does. In the usual sequence, a security introduced by
  a new generation is therefore observed before that generation is matched. If a second invocation fails on the lock
  ("concurrent refresh"), its generation can be matched before it is observed; the next run observes it. Matching
  reads no observation, so the order changes nothing it computes.
- Its population is every security whose members are all type 6. With no effective generation, it records nothing
  and succeeds.

**Concurrency and transactions.**
- The step takes `pg_try_advisory_lock` on a constant key, on its own connection. If another invocation holds the
  lock, the step fails at once ("concurrent refresh"), and the job's next run retries it.
- It releases the lock in a `finally`.
- Each stored fetch commits on its own. Searches and observations commit in one transaction after every fetch has
  been tried.
- Only then does the step raise if anything failed. A failure never rolls back stored evidence, and matching still
  runs.

**Requests.**
- No redirects are followed; a 3xx is a failed fetch.
- The step has an absolute 20-minute deadline on a monotonic clock. Each request's total time, and each wait on a
  rate gate, is capped at the lesser of 60 s and the time remaining. Streaming reads, decompression, parsing and
  searching check the deadline as they go. Fetches still due at the deadline fail, and work it interrupts is left
  undone (Design 2's `unavailable`).
- SEC requests use `settings.sec_user_agent` on the shared SEC rate gate. GLEIF and GOV.UK requests are sequential.
- **OpenFIGI.** One request per fund security, holding one job per ISIN of its set (at most 10), in the order of the
  GLEIF set's entries.
  - The job shape is frozen: `{"idType": "ID_ISIN", "idValue": <isin>, "micCode": <the N-CEN MIC of the symbol's
    listing>}`.
  - The request key is that job list as JSON, in that order. The response is decoded against the stored job list,
    position by position, and never re-sorted.
  - Requests are at least 2.4 s apart, and the response's reset header is recorded with the fetch.
  - A 429 fails that fetch and stops OpenFIGI for the rest of the step.
  - **Not coordinated with other callers.** `openfigi_resolver` keeps its own per-instance limiter, and no shared
    limiter exists in the repo. This is an accepted availability limit, not solved. The step sends at most one
    request per due fund. A collision fails that fetch. The observation still uses the newest stored mapping while it
    is current; when none is (expired, or the ISIN set changed), the observation is `isin_mapping_unavailable` until a
    later run succeeds. A shared limiter for both callers is a separate change to the resolver.
- Every response is read as a stream and abandoned past its source's limit.

**When a fetch is due.**
- N-CEN documents are fetched once per accession.
- A mutable source is due when its request key has no stored fetch, or its newest is over 7 days old or dated after
  now.
- A list is due only for the newest content-item fetch, and only when that fetch has no child list fetch. Older
  childless content fetches are superseded and never retried. This is the list's exception to the 7-day rule.

**Soundness.** A response is stored only when it is sound for its source. A *failed fetch* stores nothing: a non-2xx,
an unsound or over-limit body, a transport error, or the deadline.

| source | request key | due for | sound when | limit |
|---|---|---|---|---|
| `sec_ncen` | accession | each fund's newest N-CEN (Design 2) whose URL passes `ncen_xml_url` | parses with `SAFE_XML_PARSER`; root `edgarSubmission` in the N-CEN namespace; one `headerData/submissionType`, exactly `N-CEN` | 16 MiB |
| `gleif_record` | LEI | the LEI that `read_ncen` reads from a stored newest N-CEN | `read_gleif_record` returns a record | 1 MiB |
| `gleif_isins` | LEI | same | `read_gleif_isins` returns a set | 1 MiB |
| `openfigi_mapping` | the jobs as canonical JSON | the newest stored `gleif_isins` set (1 to 10 entries), on the MIC of the symbol's N-CEN listing | `read_figi` returns one result per job | 1 MiB |
| `hmrc_content_item` | `/government/publications/approved-offshore-reporting-funds` | any fund security with a `candidate` outcome | a JSON object; `public_updated_at` in RFC 3339 form with a time zone, and not after the fetch time; `details.attachments` a list with exactly one object whose URL ends `.ods`; that URL is `https://assets.publishing.service.gov.uk/…`, with no credentials, port, query or fragment; its `file_size` a positive JSON integer | 1 MiB |
| `hmrc_reporting_list` | the attachment URL | above | exactly the parent's `file_size` bytes; a zip | 32 MiB |

- `ncen_xml_url` accepts only
  `https://www.sec.gov/Archives/edgar/data/<cik>/<accession without dashes>/[xslFormN-CEN_X01/]primary_doc.xml`. The
  `<cik>` must be the security's CIK, and the directory must be its accession. It returns the URL without the
  rendering segment. Any other URL is not fetched, and observes `registration_unavailable`.
- `gleif_isins` is requested with `page[size]=200` and `page[number]=1` only, so a set is one atomic body.
- **N-CEN filings of a security:** its members' SEC `filing_events` rows with `filing_type` `N-CEN` or `N-CEN/A`,
  consolidated by accession. The security is `registration_metadata_conflict` when:
  - an accession has two forms, two filing dates or two report dates;
  - a report date is NULL or after its filing date;
  - a filing date is after the observation day.
  Otherwise the representative URL is the smallest.

### 2. Observations (`fund_identity_v1`)

One observation per fund security per step run.
- `observed_at` is the clock reading taken after every fetch has been tried.
- A source's *newest* fetch is the greatest `(fetched_at, fetch_id)` with `fetched_at ≤ observed_at`. It is
  evaluated as it is, with no fallback to an older one.
- It is *current* when `fetched_at ≥ observed_at − 30 days`.

**The chain.**
1. The fund's own N-CEN names its LEI and its listings (MIC, ticker).
2. GLEIF/ANNA lists the ISINs issued under that LEI.
3. OpenFIGI, asked about each of those ISINs on the MIC where the N-CEN lists the symbol, returns the tickers.

A candidate needs exactly one ISIN that OpenFIGI lists as the symbol on that MIC, with every other ISIN of the set
resolved. The binding is therefore (ISIN, MIC, ticker), agreed by the issuer's filing and OpenFIGI. GLEIF supplies
the candidates. It is one chain, not independent attestations.

**Candidate identity.** Evaluated in order; the first line that applies is the outcome. If none applies, the outcome
is `candidate`.

| outcome | condition |
|---|---|
| `cik_unestablished` | the members' primary CIKs, padded, are not exactly one valid value |
| `registration_metadata_conflict` | Design 1's consolidation conflict |
| `registration_unavailable` | the newest report period (the greatest report date across both forms) has no `N-CEN`; or its newest `N-CEN` by `(filing_date, accession)` fails `ncen_xml_url`, or has no stored fetch |
| `registration_amended` | an `N-CEN/A` has that report period |
| `registration_unusable` | `read_ncen` refuses the document, or its `reportEndingPeriod` is not the report date |
| `registration_final` | `isRegistrantLastFiling` is `Y` |
| `fund_listing_unmatched` | the listings do not carry the security's symbol (under `symbol_key`) exactly once |
| `lei_record_unavailable` | no current `gleif_record` for the LEI |
| `lei_record_unsupported` | any of: registration status not `ISSUED`, `PENDING_TRANSFER` or `PENDING_ARCHIVAL`; entity status not `ACTIVE`; category not `FUND`; a successor (a non-null `successorEntity.lei` or `.name`, empty strings included, or a non-empty `successorEntities`); or a legal name with an empty name key |
| `isin_unavailable` | no current `gleif_isins` for the LEI |
| `isin_set_unsupported` | `lastPage` is not 1; `total` differs from the entry count; no entries; more than 10; or an entry repeats |
| `isin_set_unusable` | an entry is not a valid ISIN exactly (no trimming or case change) |
| `isin_mapping_unavailable` | no current `openfigi_mapping` whose request key is this set's jobs |
| `isin_unresolved` | any job's result is `unresolved`: a warning, an empty `data`, or a row whose `ticker` is missing, not a string, or only white space |
| `isin_unestablished` | not exactly one ISIN has a row whose `ticker` (under `symbol_key`) is the symbol |

**Readers.** Every scalar is compared exactly, with no trimming. An element read as a scalar must have no child
elements; its value is its whole character content.

`read_ncen(body, cik)`:
- **Containers.** Each of these occurs exactly once, in the N-CEN namespace, at its path under the root:
  - `headerData` and `formData`;
  - `formData/generalInfo` and `formData/registrantInfo`;
  - `formData/unitInvestmentTrust`, its `notRegistrantSeparateInsuranceAccount`, and that element's `seriesCIKs`;
  - `formData/exchangeSeriesInfo`, its `exchangeTradedFund`, and that element's `securityExchanges`.
- **Fields.**
  - `headerData/submissionType`: exactly one, `N-CEN`.
  - `generalInfo`: attribute `reportEndingPeriod` (`YYYY-MM-DD`) and attribute `isReportPeriodLt12` (`Y` or `N`). The
    second is recorded only.
  - `registrantInfo/registrantCik`: exactly one, all digits, equal to the CIK once padded.
  - `registrantInfo/registrantLei`: exactly one, ISO 17442.
  - `registrantInfo/registrantClassificationType`: exactly one, `S-6`.
  - `registrantInfo/isRegistrantLastFiling`: exactly one, `Y` or `N`.
  - `notRegistrantSeparateInsuranceAccount/numOfExistingSeries`: exactly one, `1`.
  - `seriesCIKs/seriesCIK`: exactly one, whose attribute `cik`, padded, is the CIK.
  - `securityExchanges/securityExchange`: one or more, each with attributes `fundExchange` (a MIC,
    `[A-Z0-9]{4}`) and `fundsTickerSymbol` (non-empty).
- **Listings** are stored as `[mic, ticker]` pairs with the ticker as filed; the symbol comparison uses `symbol_key`
  of it.
- **Recorded, not judged:** trustee countries (`notRegistrantSeparateInsuranceAccount/trustees/trustee/trusteeStateCountry`
  attribute `trusteeCountry`), `registrantcountry` and the period flag. They go to 3d-3c-2.

`read_gleif_record(body, lei)`:
- The body is a JSON object with `data.id` equal to the LEI.
- `data.attributes.entity` and `data.attributes.registration` are objects.
- The registration status, entity status and category are strings.
- `entity.legalName.name` is a string.
- `entity.otherNames`, when present, is a list of objects with string `name`.
- `entity.successorEntity`, when present, is an object whose `lei` and `name` are each null or a string.
- `entity.successorEntities`, when present, is a list.
- An absent key means none. Anything else is unsound.

`read_gleif_isins(body)`:
- `data` is a list of objects, each with a string `attributes.isin`.
- `meta.pagination` has JSON integers (not booleans) `currentPage`, `lastPage` and `total`, with `currentPage` 1.

`read_figi(body, jobs)`:
- The body is a JSON list of exactly `jobs` objects, in job order.
- Each object's keys are exactly `data` (a list) or exactly `warning` (a string); anything else, `error` included, is
  unsound.
- A result is `unresolved` for a warning, for an empty `data`, or when any row's `ticker` is not a string with at least
  one non-white-space character. Otherwise it is that job's tickers, kept as received; the symbol comparison uses
  `symbol_key`.

**List search.** Done only for a `candidate`, for its *search key*: the policy, the ISIN and the sorted set of name
keys.
- Each stored list version is first *assessed*: read once per policy, giving usable (with its row count) or unusable
  (with its refusal). A version with no assessment yet is *pending*.
- Each usable version is then searched once per search key. Every row is tested, every match is counted by method, and
  the first 25 rows of each method, in row order, are kept as examples. The counts are complete; only the examples
  are bounded.
- A later observation with the same key reuses the stored results. A changed ISIN or name key is a new key, which
  searches every usable version again.

*Match tests.* Each row of a version is tested in this order, and the first test that applies is the row's match.
The *targets* are the ISIN and, for a US or CA ISIN, its NSIN.

| method | condition |
|---|---|
| `identifier` | an ISIN No or CUSIP No cell, upper-cased and stripped of non-alphanumerics, **equals** a target |
| `identifier_in_text` | such a cell **contains** a target |
| `near_identifier` | the trimmed, upper-cased ISIN cell is one substitution or one adjacent transposition from the ISIN; or, for a US or CA ISIN, the trimmed, upper-cased CUSIP cell is that close to the NSIN |
| `name` | the row's name key contains one of the fund's name keys, and the row does not hold a valid ISIN (after trimming) with a different country prefix from the fund's |

*Outcome* for the observation, over the list versions stored up to `observed_at`, from the completed searches:
- `listed`: a completed search with an `identifier` count above 0.
- `unresolved`: otherwise, a completed search with any other count above 0.
- `unavailable`: otherwise, when any of these holds:
  - the newest content fetch is not current;
  - its child list is missing or unusable;
  - any stored version is pending, or is usable and not yet searched for this key (the deadline came first).
- `not_found`: otherwise. Every stored version is assessed, every usable one is searched for this key, and no row of
  any of them matched this policy's tests.

`listed` and `unresolved` stand on completed searches even while other versions are pending; `not_found` needs complete
coverage.

Matches are evidence about the rows and stay in the search rows. A `listed` or `unresolved` key keeps that outcome
for as long as its versions are stored. Clearing it needs a new policy id, which searches afresh, or 3d-3c-2's
reviewed determination. Nothing in these tables is updated.

*Why the name test skips a valid ISIN from another country.* This is a deliberate local exclusion, not a proof of
identity: neither a valid check digit nor the prefix shows that the row's identifier was entered correctly. It exists
because issuers list many foreign share classes under one name; SPY's nine SPDR UCITS rows are Irish classes of
another fund. The same-country case is searched by name, so a row naming the fund under a mistyped US ISIN
(US78462F1048 is valid and two edits away) is `unresolved`. A row naming the fund under a valid foreign ISIN would be
missed. That residual is why the outcome is `not_found`, a statement about this search and not about the list.

*Name keys.* A name key is built by Unicode upper-casing, taking the ASCII `[A-Z0-9]+` words (any other character
separates words), dropping the generic words THE, TRUST, FUND, FUNDS, ETF, INC, PLC, LLC and SERIES, and
concatenating the rest. The fund's keys come from GLEIF's legal name and every
`otherNames` entry, with empty keys dropped. A row's key comes from its Parent Fund, HMRC Share Class Ref and Sub Fund
Name, joined. Because name keys split on every non-alphanumeric character, the amount of white space cannot change a name match;
white space handling still matters for the exact header and the stored examples.

**A usable list** (`read_reporting_list(body, baseline)`):
- **Archive.** The zip holds exactly one `content.xml`, which declares at most 256 MiB, is unencrypted, and is stored
  or deflated. It is streamed, counting decompressed bytes and stopping past 256 MiB. Any archive or decompression
  failure (an unsupported method, a CRC error, truncated data) is an unusable assessment with that refusal, never an
  unhandled error.
- **Ancestry.** Exactly one `table:table`, whose parent is `office:spreadsheet` and whose children are only
  `table:table-column` and `table:table-row`. Every `table:table-row` is a child of it.
- **Cells.**
  - A row's children are `table:table-cell` only. None is covered or spanned, or has a formula.
  - A cell's only `office:` attribute is `office:value-type`, absent or `string`; so `office:string-value` is refused.
  - A cell's children are `text:p` only. A paragraph contains character data, `text:span` and `text:a` (recursively),
    and `text:s`, `text:tab` and `text:line-break`. The last three must be empty. Any other element makes the list
    unusable.
- **Text.** Each paragraph is processed by the §6.1.2 algorithm across its inline elements (Source rules). The
  explicit elements are kept as typed tokens until the last step, never as characters, so all character data is
  preserved. `text:c`
  is an integer from 1 to 4,096. Paragraphs are joined with a line feed.
  - A cell's raw character data, summed over its paragraphs, is at most 65,536 characters.
  - Its logical text, with expansions and separators, is at most 4,096 characters, counted before anything is
    expanded.
  - A version's logical text in total, counted in characters with each cell multiplied by its column repeat (up to
    column nine), is at most 67,108,864 characters.
- **Repeats and limits.**
  - `number-columns-repeated` is an integer from 1 to 16,384, and `number-rows-repeated` from 1 to 1,048,576. Repeats
    are positioned arithmetically, never materialised.
  - A row with content does not repeat, and no non-empty cell lies past column nine. Empty rows are skipped.
  - At most 1,000,000 data rows are read.
- **Rows.** Row 0 is exactly fact 5's header. Every data row has a Reporting Fund Ref `[A-Z][0-9]{4}` and a non-empty
  Parent Fund or Sub Fund Name.
- **Size.** At least 100,000 data rows when no earlier version is usable, by fetch order. Otherwise at least 90% of
  the largest earlier usable version. Versions are assessed strictly in ascending fetch order, and none is assessed
  while an earlier one is pending; each assessment stores the baseline it used. These floors are anomaly detection, not proof of completeness: a
  count-preserving loss of content passes them.

A version's assessment (verdict, refusal, row count) is computed once per policy and stored, so baselines need no
reparse. Work cut off by the deadline is stored as nothing: the version stays pending, or the search stays undone, and
the observation is `unavailable`. Historical versions are parsed one at a time, and their rows are released before the
next.

### 3. Storage (migration `sql/492_tax_fund_evidence.sql`)

All five tables are insert only: a trigger refuses update and delete.

**`tax_fund_documents`**
- `body_sha256 TEXT PRIMARY KEY CHECK (body_sha256 ~ '^[0-9a-f]{64}$' AND body_sha256 = encode(sha256(body), 'hex'))`;
- `body BYTEA NOT NULL CHECK (octet_length(body) BETWEEN 1 AND 33554432)`.
- Written with `ON CONFLICT DO NOTHING`.

**`tax_fund_fetches`**
- `fetch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`;
- `source TEXT NOT NULL CHECK (source IN (…six…))`;
- `request_key TEXT NOT NULL CHECK (request_key <> '')` and `url TEXT NOT NULL CHECK (url <> '')`;
- `fetched_at TIMESTAMPTZ NOT NULL`;
- `body_sha256 TEXT NOT NULL REFERENCES tax_fund_documents`;
- `parent_fetch_id BIGINT REFERENCES tax_fund_fetches UNIQUE`;
- `rate_reset TEXT`, OpenFIGI's reset header as received;
- `CHECK ((parent_fetch_id IS NOT NULL) = (source = 'hmrc_reporting_list'))`.

Also:
- a unique index on `request_key` where `source = 'sec_ncen'`;
- a trigger that refuses a list whose parent is not an `hmrc_content_item`, or whose `request_key` and `url` differ
  from the attachment URL in the parent's body.

**`tax_fund_list_versions`** (the assessments; a list fetch with no row for the policy is pending)
- `list_fetch_id BIGINT NOT NULL REFERENCES tax_fund_fetches`, and `policy_id TEXT NOT NULL`;
- `usable BOOLEAN NOT NULL`, `data_rows INTEGER CHECK (data_rows BETWEEN 1 AND 1000000)`, `refusal TEXT`,
  `baseline_rows INTEGER` (the floor's basis, NULL for the bootstrap floor) and `assessed_at TIMESTAMPTZ NOT NULL`;
- PRIMARY KEY `(list_fetch_id, policy_id)`;
- `CHECK (usable = (data_rows IS NOT NULL) AND usable = (refusal IS NULL))`;
- a trigger requires the fetch's source to be `hmrc_reporting_list`.

**`tax_fund_list_searches`**
- `policy_id TEXT NOT NULL`, `isin TEXT NOT NULL` (an ISIN by CHECK), `name_keys TEXT[] NOT NULL`;
- a *canonical name-key set*, checked here and on observations: non-empty, no NULL or empty element, strictly ascending
  (so no duplicates);
- `list_fetch_id BIGINT NOT NULL`;
- `searched_at TIMESTAMPTZ NOT NULL` (completion time);
- `identifier_count`, `identifier_in_text_count`, `near_identifier_count` and `name_count`, each `INTEGER NOT NULL
  CHECK (… >= 0)`: complete counts;
- `examples JSONB NOT NULL`, a JSON array of objects `{"method", "key", "cells"}`, with `method` one of the four and
  `cells` an array of nine strings, and `key` the target or name key that matched (a string). It holds, per method,
  exactly `least(count, 25)` examples, the first in row order. A trigger checks the per-method numbers, and that the
  four counts sum to no more than the version's `data_rows`;
- PRIMARY KEY `(policy_id, isin, name_keys, list_fetch_id)`;
- FK `(list_fetch_id, policy_id)` to a `tax_fund_list_versions` row, and a trigger requires it usable.

**`tax_fund_observations`**
- `observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`;
- `observed_at TIMESTAMPTZ NOT NULL`, `policy_id TEXT NOT NULL`, `evidence_run_id BIGINT NOT NULL`;
- `tax_security_id BIGINT NOT NULL`, `member_ids BIGINT[] NOT NULL`, `symbol TEXT`;
- `outcome TEXT NOT NULL`, with a CHECK on `candidate` or the table's codes;
- **evaluated inputs**, kept for every outcome that reached them, NULL otherwise:
  - `cik TEXT`, `newest_period DATE`, `amendment_accessions TEXT[]`;
  - `ncen_accession TEXT`, `ncen_report_date DATE`, `ncen_source_url TEXT` (as stored in `filing_events`),
    `ncen_fetch_id BIGINT`;
  - `lei TEXT`, `record_fetch_id BIGINT`, `isins_fetch_id BIGINT`, `figi_fetch_id BIGINT`;
  - `listings JSONB` (the `[mic, ticker]` pairs), `mic TEXT`, `isin_set TEXT[]`;
  - `trustee_countries TEXT[]`, `registrant_country TEXT`, `short_period TEXT`;
- `isin TEXT` (the bound ISIN) and `name_keys TEXT[]`;
- `search_outcome TEXT`, one of `listed`, `unresolved`, `unavailable` or `not_found`;
- `content_fetch_id BIGINT`, the newest content fetch at `observed_at` (NULL if none), and `list_fetch_id BIGINT`, its
  child list (NULL if none);
- `refusal TEXT`, a structured reason where the outcome has one (for example `registration_unavailable`'s
  "unsupported URL" or "no stored document", or `unavailable`'s "pending version");
- `evaluated_at TIMESTAMPTZ NOT NULL`, when the decision completed (after `observed_at`, the source cutoff);
- `coverage JSONB`, for a candidate: every stored list fetch at `observed_at` with its state when evaluated
  (`pending`, `unusable` or `searched`), so an `unavailable` observation stays explained after later runs;
- UNIQUE `(observed_at, tax_security_id)`.

**State rules**, each written NULL-safe:

| outcome | evaluated inputs | `isin`, `name_keys`, `search_outcome`, `content_fetch_id`, `list_fetch_id` |
|---|---|---|
| `candidate` | all non-NULL: `cik`, `ncen_accession`, `ncen_report_date`, `ncen_fetch_id`, `lei`, `record_fetch_id`, `isins_fetch_id`, `figi_fetch_id`, `listings`, `mic`, `isin_set` | `isin`, `name_keys`, `search_outcome` non-NULL, with `isin = ANY(isin_set)`; the fetch pair as below |
| any other | as far as evaluation reached; NULL beyond | all NULL |

For a candidate:
- `list_fetch_id` non-NULL requires `content_fetch_id` non-NULL;
- `not_found` requires both, and its list usable (trigger);
- `unavailable` allows a content fetch with no list, or a list that is unusable or pending;
- `listed` and `unresolved` allow any pair, since completed searches decide them.

A trigger checks every fetch id's source, and that its request key belongs to this observation:
- the accession, for `ncen_fetch_id`;
- the LEI, for the two GLEIF ids;
- the jobs built from `isin_set` and `mic`, for `figi_fetch_id`;
- `list_fetch_id`'s parent, for `content_fetch_id`.

Code checks, before insert, that `isin_set` equals the entries of the `isins_fetch_id` body, in order. It also checks
that `search_outcome` follows from the stored assessments and search rows by Design 2's rules: `not_found` needs an
assessment for every stored list fetch and a search row for every usable one, and `listed`/`unresolved` need a
search row whose counts decide it. An `unavailable` observation needs no search row.

Matching reads none of these tables in this slice.

### 4. The job

`tax_broker_evidence_refresh` becomes the evidence step, then `refresh_cover_instances`, `refresh_fund_evidence` and
`run_tax_matching`.
- Each outcome is captured, and the job fails if any step failed.
- `daily_tax_reconciliation` runs the last three.
- Matching's snapshot, inputs, digest and selection are unchanged.

## Local policy (frozen under `FUND_IDENTITY_POLICY_ID = "fund_identity_v1"`)

Changing any of these is a new policy id:
- the identity table and its order;
- the readers' paths, attributes, types and cardinalities, and the S-6 single-series form;
- the GLEIF status set;
- the one-page ISIN set of at most 10, and the chain binding;
- the OpenFIGI job shape;
- the list's usability rules and limits;
- the match tests and their order;
- the name keys and generic words;
- the search key and the outcome precedence;
- the 30-day currency, the 7-day refresh and the step deadline.

## Not in this slice

- **3d-3c-2, the fund determination.** A reviewed reading of each fund's governing documents, residence evidence and
  status evidence, covering:
  - the s355 limb and residence;
  - s356 A to C and s357, including creation and redemption at NAV;
  - UK opacity under INTM180040 and reg 29;
  - continuity over the holding (N-CEN F.6 counts series at period end; the short-period flag);
  - identity continuity across LEI or ISIN changes;
  - the gate on reporting status, given reg 57;
  - what clears an `unresolved` search.
- **3d-3c-3:** non-reporting fund disposals.
- **3d-3c-4:** reporting funds and the London-listed UCITS ETFs.
- Open-end and multi-series registrants, which `read_ncen` refuses.
- Fund distributions (3d-3b).

## Tests

**Fixtures.** All are committed, each with its SHA-256 and provenance:
- SPY's N-CEN XML, an SEC public record;
- GLEIF's two JSON bodies, published under CC0;
- synthetic OpenFIGI bodies in fact 4's shapes;
- small synthetic ODS files built by a test helper.

No test touches the network.

Pure:
1. **Each identity line alone, in order.**
   - CIK: none; two; `884394` and `0000884394` on two members (one value, passes); a non-digit CIK.
   - Metadata: an accession with two forms, two filing dates or two report dates; a NULL report date; a report date
     after its filing date; a filing date tomorrow.
   - Registration:
     - a newest period with only an `N-CEN/A`; an `N-CEN/A` for the newest period; one for an older period (passes);
     - URLs under another CIK, under another accession's directory, or on another host; a raw `primary_doc.xml`
       (passes).
   - `read_ncen`, once per condition:
     - cardinality: two `registrantInfo` containers splitting the fields; an empty duplicate `exchangeTradedFund`;
       two `registrantCik`;
     - `reportEndingPeriod` missing, malformed, or different from the DB report date;
     - registrant: another CIK; an LEI failing mod 97; a letter in a check position; a padded or lower-case LEI;
     - form: `N-1A`; two series; a `seriesCIK` naming another CIK; another namespace; `submissionType` `N-CEN/A`;
     - last filing: `isRegistrantLastFiling` missing, then `Y`;
     - listings: a `fundExchange` of `ARCX1`; a missing `fundsTickerSymbol`; a ticker `spy` (stored as filed, compared
       by `symbol_key`);
     - a `registrantLei` with a child element.
   - Listings: the symbol absent; present twice.
   - GLEIF record:
     - each refused registration status, an `INACTIVE` entity, the category `GENERAL`;
     - successors: by LEI, by name only, and `{"lei": "", "name": null}`;
     - a legal name of only generic words;
     - unsound: `otherNames` not a list; a non-string alias; a numeric successor LEI.
   - ISIN set:
     - unsupported: `lastPage` 2; `total` 2 with one entry; 11 entries; no entries; a repeated ISIN;
     - unsound: `currentPage` 2; `total` as `true`;
     - unusable: a bad check digit; a trailing space; lower case.
   - Mapping:
     - no fetch; a fetch for another job list, or for the same ISINs in another order (unavailable);
     - a response decoded against reversed jobs gives the wrong association, which the order rule prevents;
     - a body with `error`, or with two results for one job (unsound);
     - a matching ISIN beside one whose result is a warning, an empty `data`, `ticker: ""`, `ticker: "   "` or a
       missing `ticker` (unresolved);
     - two ISINs both listing the symbol (unestablished).
2. **Match tests and outcome.**
   - `identifier`: the ISIN with a non-breaking space; in lower case; the NSIN in either column.
   - `identifier_in_text`: `XUS78462F1030Y`. The NSIN is not searched for an IE ISIN.
   - `near_identifier`: US78462N1030 on a row naming SPY; an adjacent transposition; the NSIN with one substitution.
   - `name`:
     - matched: a row with neither identifier named "SPDR S&P500 ETF Tr"; one matching only "SPDR Trust, Series 1";
       a row named "SPDR S&P 500 ETF TRUST" under US78462F1048; a name written with `text:s` between words;
     - not matched: a row with a valid IE ISIN naming "SPDR S&P 500".
   - Counts: an `identifier` match after 100 `name` matches gives `listed`, with all counts complete and 25 examples
     per method; reordered rows give the same counts.
   - Outcome:
     - `listed` in an older version and absent in the newest stays `listed`;
     - `unresolved` likewise;
     - a new alias makes a new key, which searches every stored version;
     - a version not searched before the deadline makes the outcome `unavailable`, and is searched by the next run;
     - a version whose assessment the deadline interrupted stays pending, and blocks `not_found` but not an
       established `listed`;
     - currency: the newest content fetch 30 days old exactly (current), and 30 days and 1 µs (unavailable); a newer
       unusable list with an older usable one (unavailable).
3. **List usability**, each condition alone.
   - Archive: no `content.xml`; two; a declared or actual size past 256 MiB.
   - Ancestry: two tables; a table outside `office:spreadsheet`; a row outside the table; a `table:table-row-group`.
   - Cells and text:
     - a covered cell; a spanned cell; a formula; a `float` value type; an `office:string-value`;
     - a `text:note`; a `text:s` holding text; a `text:s` holding a child;
     - white space: `A <text:span> B</text:span>` reads `A B`; leading and trailing spaces are removed; a
       whitespace-only span; `text:s text:c="2"` survives collapsing;
     - 65,537 characters of raw data in a cell, across two paragraphs; 67,108,865 characters of logical text in a
       version;
     - literal U+E000 and U+E001 characters in a cell, beside real `text:s` and `text:tab` (preserved);
     - archives: an unsupported compression method, an encrypted `content.xml`, a CRC error and truncated data (each
       an unusable assessment; the next version is still assessed);
     - assessment order: a pending larger predecessor blocks assessing a later version; on resumption, the later
       version is assessed against it.
   - Limits:
     - `text:c` of 0, -1, and 1,000,000,000,000;
     - 4,098 empty paragraphs in one cell;
     - a cell of 4,097 characters;
     - `number-rows-repeated` of 1,048,577, and `number-columns-repeated` of 0;
     - 1,000,001 data rows.
   - Rows: a repeated row with content; content in column ten; a row without a fund ref.
   - Size: a header-only first version; 99,999 rows first; 89% of the largest earlier usable version; an
     unusable-then-small-then-complete sequence (the bootstrap floor applies until a version is usable).
   - Passes: a short row; 16,384 repeated empty cells; 923,553 repeated empty rows; white space written as spaces,
     tabs, newlines or `text:s`, which gives the same name key.
4. **Readers on fixtures:** facts 2–4 and `candidate`. Check digits: US78462F1030, US78462N1030, US78462F1048,
   IE00B5BMR087 and IE0031442068 are valid; KYG4330C1097 is not.

DB:
5. **Capture.**
   - Bootstrap from empty tables fetches every due source.
   - An unchanged re-fetch adds a fetch row and no document row.
   - A new content fetch gets its list. A same-URL list with new bytes and a new size is captured under its new
     parent. A superseded childless parent is not retried.
   - Each of these stores nothing, fails the step after the others are tried, and keeps every stored fetch:
     - a 200 HTML page as N-CEN; a 301;
     - an ODS of the wrong size;
     - a content item with two `.ods` attachments, or a future `public_updated_at`;
     - a GLEIF body of 1 MiB + 1 byte;
     - a 429 (no further OpenFIGI request in that step);
     - the deadline.
   - Refused: a second `sec_ncen` fetch of an accession; a list whose parent is not a content item, or whose URL
     differs from the parent's attachment; two lists with one parent; any update or delete.
   - A second invocation while one holds the lock fails at once, and leaves the first's commits intact.
   - A security added by a new evidence generation is observed before that generation is matched.
6. **Observations and searches.**
   - SPY gives `candidate`/`not_found`. CSPX.L gives `cik_unestablished` with NULL search columns.
   - `registration_final` keeps its accession, fetch id and LEI.
   - Each version is searched once per key: a second observation adds no search rows.
   - Refused:
     - a `cik_unestablished` row with a `search_outcome` or a fetch pair;
     - a name-key array with a duplicate, an empty string or a NULL; a usable assessment with 0 rows; counts summing
       past `data_rows`;
     - an attachment URL on another host, over HTTP, or with a port;
     - a `not_found` row with no list fetch, or with an unusable one;
     - a `candidate` missing any required column;
     - an `isin` outside `isin_set`;
     - a `record_fetch_id` naming another LEI's fetch, or a `figi_fetch_id` for other jobs;
     - a search row on an unusable version, with eight cells, or with fewer examples of a method than
       `least(count, 25)`;
     - two observations of one security at one `observed_at`.
   - The effective matching run, and its digest, are identical before and after a fund refresh.
7. **Currency matrix**, table-driven over `gleif_record`, `gleif_isins`, `openfigi_mapping` and `hmrc_content_item`:
   - each at 30 days exactly, and at 30 days and 1 µs;
   - each dated after `observed_at`;
   - a failed refresh with an older current fetch;
   - an unchanged re-fetch restoring currency;
   - a newer fetch failing a rule, with an older passing one (no fallback).

## Acceptance (dev DB)

1. **Before deployment**, record the row counts of the five tables. The migration creates them empty, and a
   scheduled run may fill them before the acceptance job.
2. **Run one `tax_broker_evidence_refresh`**, with no new evidence generation since the Script's run.
   - If any source has changed since 2026-10-14 (a new N-CEN, GLEIF record, ISIN set, OpenFIGI result or list
     version), rerun the Script and re-derive the table below from its output.
   - Expected results are the newest observation per security, whichever run wrote it.

| security | outcome | search | expected |
|---|---|---|---|
| SPY (3000) | `candidate`: LEI 549300NZAMSJ8FXPQQ63, ISIN US78462F1030, MIC ARCX, N-CEN `0001410368-26-023904` (2025-12-31) | see below | |
| IUSA.L (3075), CSPX.L (3434) | `cik_unestablished` | NULL | no evaluated inputs |

- **From a fresh store** (the counts in step 1 all zero):
  - one fetch each of the N-CEN, the GLEIF record, the GLEIF ISINs, the OpenFIGI mapping, the content item and the
    list (whose parent is the content fetch);
  - one usable list version of 125,022 rows;
  - one search row.
- **From a fresh store**, SPY's search outcome is `not_found`, with one search row for (`fund_identity_v1`,
  US78462F1030, {SPDR1, SPDRSP500}) whose four counts are 0.
- **From an existing store**, step 1 also records every stored list fetch, its assessment and baseline, and the
  search rows for that key. The expected outcome is derived from those by Design 2's rules:
  - a newly fetched version is assessed against the stored baseline, so a larger earlier version can make today's
    unusable;
  - only a version that is new, or not yet searched for the key, adds a search row, with counts 0 if it is today's
    body;
  - an older version with a match gives `listed` or `unresolved`;
  - when nothing is due, no fetch or search row is added, and the observation reuses the stored results.
- **Matching:** run 5's three fund characters are unchanged in the effective run after the job.
- The acceptance query and its output are posted on #3616.

## Full-population verification

The Script runs this spec's readers and decisions on every fund security of the effective generation (facts 1–4). It
applies every match test to all 125,022 rows of the list, for the one candidate (fact 5). Fact 6 is coverage only. The
Script measures a fresh capture. Stored-history behaviour (reuse of searches, outcomes over several versions) is the
build's, and tests 2, 6 and 7 cover it.

## Finding dispositions

3d-3a draft, round 1: finding 6 is answered by Design 2 and finding 16 by Designs 1 and 3. Findings 4, 5 and 9 move
to 3d-3c-2 by construction of the split.

Rounds 1–4 (`rN.k` is round N finding k):

| findings | disposition |
|---|---|
| r1.2–4, r1.14–16, r2.2–4, r2.14–16 | out of scope: 3d-3c-2's determination; no character changes |
| r1.5, r2.5 | the registration status allowlist (r4 confirmed) |
| r1.17–19, r2.17–19 | resolved in rounds 2–3 (no direction or finality claim) |
| r1.4, r2.4, r2.34, r2.36, r2.37, r3.6, r4.6 (periods, amendments, lifecycle) | the newest period across both forms, needing an original and no amendment; `reportEndingPeriod` reconciled with the DB report date; the short-period flag recorded; `registration_final`; successors by LEI or name; continuity is 3d-3c-2's |
| r1.1, r2.1, r3.1, r4.1 (false absence) | four match tests; the name test covers rows without a valid ISIN and rows with a same-country ISIN; the residual is stated, and the outcome renamed `not_found` |
| r3.2, r4.12 (heuristic presence) | `identifier` needs equality; containment is `identifier_in_text` and unresolved |
| r1.6, r2.6, r3.3, r4.2, r4.13 (stickiness, lineage) | lineage removed; searches stored per (policy, ISIN, name keys, version); a new key searches every version; outcomes over all stored versions; LEI is not a lineage; identity continuity is 3d-3c-2's |
| r4.3 (state matrix) | Design 3: matches live in search rows; the NULL-safe state rules |
| r1.21, r2.21, r3.19, r3.20, r4.4 (constraints) | Design 3's DDL; one FIGI fetch per set, bound to `isin_set` and `mic` by trigger; `isin ∈ isin_set`; search rows tied to usable versions; match shape |
| r1.7–8, r1.26, r2.7–8, r2.26, r2.30–31, r2.35 | not applicable: matching is unchanged; test 6 asserts it |
| r1.9–10, r2.9–10, r2.33, r3.10, r4.7 (set, mappings) | one atomic page of at most 10; JSON integers only; malformed entries, unresolved results and empty tickers prevent a candidate |
| r1.11, r1.20, r2.11, r2.20, r2.32, r3.14 | resolved in round 4 (parent-based list scheduling) |
| r1.12–13, r2.12–13, r3.8, r3.9, r3.15, r4.14 (contracts) | exact paths, singleton containers, exact scalars, typed JSON; the bootstrap floor until a version is usable |
| r2.38, r3.17, r4.5, r4.8, r4.9 (ODF) | ancestry enforced; `office:string-value` refused; inline elements empty; §6.1.2 white space; separators charged to the budget |
| r1.25, r2.25, r3.16, r3.18, r4.15 (bounds) | per-source limits; decompressed bytes; bounded text and repeats; a row cap; a step deadline after which unsearched versions give `unavailable`; each version searched once per key; complete counts with 25 examples per method (r5.1); assessments stored |
| r1.22, r2.22, r3.13 | `pg_try_advisory_lock` for the step; per-fetch commits; the failure raised after commit |
| r1.23, r2.23, r3.7 | resolved in round 4 (URL binding, no redirects) |
| r1.24, r2.24, r3.22, r4.16 (OpenFIGI budget) | one batched request per fund per run; spacing; a 429 stops OpenFIGI for the step; the reset header recorded; the shared budget and retry stated |
| r3.26, r4.11 (market binding) | the job is filtered to the N-CEN listing's own MIC, so the binding is (ISIN, MIC, ticker) |
| r1.27, r2.27, r3.4, r4.10, r4.18 (provenance) | the Script implements the table in order with full provenance; it now emits the 13F rows, the effective characters, the GLEIF fields and raw filing counts; nine *rows*, not pairs; fresh capture distinguished from stored history |
| r3.5, r3.11, r3.12, r3.21, r3.23, r3.24 | resolved in round 4 |
| r2.39, r3.25, r4.17 (acceptance) | fresh-store and existing-store expectations separated; every mutable source in the change check |
| r1.28–29, r2.28–29, r4.19 | Source rules: grammars, the full GLEIF enumeration with `MERGED` deprecated |
| r5.1 (capped matches) | complete per-method counts; 25 examples per method in row order, checked by trigger |
| r5.2 (deadline and insertion) | assessments stored separately, a version with none is pending; `not_found` needs complete coverage; `unavailable` needs none; established positives stand |
| r5.3 (reader vs spec) | MIC grammar enforced; scalar elements without children; tickers stored as filed |
| r5.4 (Script scope) | the Script's fresh-capture scope and its unenforced capture rules stated; HMRC preconditions checked and printed |
| r5.5 (state matrix) | Design 3's state rules: search columns NULL for every non-candidate; the fetch pair per search outcome |
| r5.6 (existing-store acceptance) | expected outcome derived from recorded assessments and searches |
| r5.7 (white space) | the §6.1.2 algorithm across inline elements, as quoted in Source rules; claim narrowed to name matches |
| r5.8 (aggregate limits) | raw and logical per-cell caps, a per-version logical cap, deadline checks every 1,000 rows, examples bounded while counting |
| r5.9 (OpenFIGI coordination) | accepted residual, stated in Design 1: a collision delays an observation and never changes one; a shared limiter is a resolver change |
| r5.10 (unavailable provenance) | accession, report date and URL kept before the availability test; `refusal` column |
| r5.11 (ordered association) | jobs in GLEIF entry order, request key in that order, decoding by position against the stored jobs; name-key ASCII rule stated |
| r5.12 (generation guarantee) | narrowed: the usual order, and the concurrent-refresh exception, which changes nothing matching computes |
| r5.13–15 (wording) | the exclusion described as local; the list as published evidence; Script docstrings updated |
| r6.1 (blank ticker) | a ticker needs a non-white-space character; whitespace-only alternative tested as unresolved |
| r6.2 (budgets) | per-cell raw budget across paragraphs; per-version logical budget in characters with repeats; both in the Script; the real version measured at 15,931,480 |
| r6.3 (sentinels) | typed tokens; literal private-use characters preserved and tested |
| r6.4 (archive failures) | supported features stated; every failure an unusable assessment |
| r6.5 (normalisation) | tickers kept as filed; CUSIP near-miss upper-cased, stated |
| r6.6 (unavailable provenance) | `newest_period`, `amendment_accessions` and `ncen_source_url` kept |
| r6.7 (HMRC validator) | RFC 3339 with a zone, nested types checked first; same in the Script |
| r6.8 (assessment order) | ascending fetch order, no skipping a pending version, baseline stored |
| r6.9 (invariants) | row range, count sum, canonical name-key sets, example key defined |
| r6.10 (pending history) | `evaluated_at` and per-fetch `coverage` on the observation; completion times on assessments and searches |
| r6.11 (deadline) | absolute monotonic deadline; requests and waits capped by remaining time; checks inside streaming and parsing |
| r6.12 (OpenFIGI wording) | the accepted limit described as it behaves, including `isin_mapping_unavailable` |
| r6.13 (existing-store acceptance) | expectation derived with the stored baseline; the nothing-due case |
| r6.14 (attachment origin) | HTTPS on the GOV.UK asset host only, no credentials, port, query or fragment |
| r6.15 (timing) | removed |

## Security

No order or authorisation path changes. The step reads public documents from SEC, GLEIF, OpenFIGI (keyless) and
GOV.UK, none needing an account (operator rule 2026-10-10).
- Every body is bounded while it is read, and `content.xml` while it is decompressed.
- XML is parsed with the repo's safe parser, with no entity resolution and no network access.
- No redirects are followed.
- Matching is unchanged.
