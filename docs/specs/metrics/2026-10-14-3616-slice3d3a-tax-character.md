# Tax character and the fund withholding gate (#3616 slice 3d-3a)

Source: #3616 slice 3d, split by 3d-1 (`2026-10-14-3616-slice3d1-consideration-basis.md`, "3d split"). 3d-3a owns
slice-3 checkpoint-1 round-1 findings 17, 18, 30, 44 and 45 (text in `var/research/3616/ckpt1_slice3_r1_final.txt`,
loop worktree) and removes 3d-2's `character_pending`. 3d-1's 3d-3 row is split three ways (Not in this slice):
**3d-3a** (this) decides character and computes shares; **3d-3b** dividends; **3d-3c** offshore-fund disposals.

**What this slice decides and builds.** Each tax security (3d-2 Design 2) gets a *tax character* from captured SEC
evidence: **share** or **unestablished**. A share is computed exactly as 3d-2 computes it; its gains and losses are
capital. Every other security is **withheld**, including every exchange-traded fund. A fund cannot be computed with
3d-2's identification at all: an interest in a non-reporting fund is a *relevant security*, outside the s104 pool and
identified differently (Source rules), and a reporting fund's cost depends on reported income nobody has ingested
(finding 17). Both are 3d-3c's. Nothing is shown to the operator (3d-4).

**The outcome on dev today:** AAPL and NXH are shares; AAPL's 2026-09-23 disposal is a capital loss of
£0.00753863550700. **SPY, which 3d-2 computed, is withheld**: 3d-2 identified it as a share (same-day, then 30-day,
then the pool), which is the wrong identification if it is a non-reporting fund, as fact 4 indicates. Run 1's SPY
figures are superseded, not corrected; 3d-3c computes them.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3a_character.py`, read-only, run on 2026-10-14. It resolves the effective
matching run with `effective_tax_matching` (run 1, over evidence run 2), reads its securities, disposals,
identifications, dependencies and pools and each member's type and primary SEC CIK, fetches each CIK's SEC
submissions JSON, and applies Design 2 through one function, `classify`, which the build moves into the service.
Output `var/research/3616/slice3d3a/population.txt`; bodies with SHA-256 beside it (gitignored; no account
identifier). `--universe` applies `classify` to every Stocks instrument with a primary SEC CIK (tradable or not,
variants included) from the local `submissions.zip` of `sec_submissions_bulk_refresh` (archive 2026-10-10), with the
holding start unknown so every former name counts; output `universe.txt`.

1. **Members** (`instruments.instrument_type_id`, eToro's: 5 Stocks, 6 ETF; `external_identifiers` provider `sec`,
   type `cik`, `is_primary`). AAPL (1001, 8754), NXH (1181), GME (1699, 8761), ILMN (4077): type 5. SPY (3000, 3417),
   IUSA.L (3075), CSPX.L (3434): type 6. Primary CIKs sit on base rows only: AAPL 0000320193, NXH 0001130713, GME
   0001326380, ILMN 0001110803, SPY 0000884394; none for IUSA.L or CSPX.L. No component has two types or two CIKs.
2. **SEC submissions** (`data.sec.gov/submissions/CIK##########.json`, 2026-10-14; `name`, `entityType`, `sic`, 10-K
   in `filings.recent.form`): AAPL "Apple Inc.", operating, 3571, yes; NXH "NEIGHBORHOOD INTELLIGENCE, INC.",
   operating, 5961, yes, former names through the holding (from 2025-11-14) "BED BATH & BEYOND, INC."; GME "GameStop
   Corp.", operating, 5734, yes; ILMN "ILLUMINA, INC.", operating, 3826, yes; SPY "SPDR S&P 500 ETF TRUST", `other`,
   no SIC, no 10-K. Energy Transfer ("Energy Transfer LP") and Enterprise Products ("ENTERPRISE PRODUCTS PARTNERS
   L.P."), both partnerships, are `operating` with 10-Ks: **no structured submissions field states legal form**
   (checked: `entityType`, `sic`, `stateOfIncorporation`, `category`).
3. **Run 1 under Design 2** (Script): AAPL, NXH, GME, ILMN → share; SPY, IUSA.L, CSPX.L → unestablished
   (`exchange_traded_fund`). Run 1 figures: AAPL disposal 2026-09-23 (2026/27), 1 identification, gain
   −£0.00753863550700, pool 0 / £0; NXH pool 1305.05709600 / £6048.55403340328000, no disposal; SPY disposal
   2026-09-23, 2 identifications, pool 21.66720200 / £12537.70775155264395; dependencies GME 2, IUSA.L 1, CSPX.L 3,
   ILMN 1 (`unresolved_subject`).
4. **SPY and HMRC's list** (first Script version, commit `7bdadc76`, bodies in `var/research/3616/slice3d3a/
   20261014T070010Z/`): HMRC's "Approved offshore reporting funds" list (GOV.UK, `public_updated_at` 2026-10-12,
   125,022 share-class rows, ceased funds kept) has no row for SPY by ISIN US78462F1030, by CUSIP 78462F103 or by
   name; it lists CSPX (IE00B5BMR087, from 22/03/2010) and IUSA (IE0031442068, from 12/06/2010). No CUSIP or ISIN is
   bound to any of SPY, IUSA.L or CSPX.L in `external_identifiers`; SPY's CUSIP sits unbound in
   `unresolved_13f_cusips`. Recorded for 3d-3c; nothing here relies on it.
5. **Coverage of the share rule** (`universe.txt`, 5,339 instruments): share 3,569; `entityType` other 1,098 (foreign
   private issuers: among the 1,096 tradable non-variant ones, 962 file 20-F, 102 40-F, 32 neither); a former name
   without a form word 260 (an upper bound: the holding start is unknown here); SIC 172 (6770 blank check 121, none
   49, 6799 2); no form word 161; no 10-K 78; `investment` 1. This measures how many stocks the rule admits, not
   whether an admitted one is correct; correctness rests on the rule's construction (Design 2).

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
- **No source** states how to read legal form, or an issuer's business, from SEC data. Design 2 fixes both by
  construction, fails closed, and is frozen under the policy id.

## Design

### 1. Evidence capture (job `tax_character_evidence_refresh`)

A daily job fetches the SEC submissions JSON of every primary SEC CIK (`external_identifiers`, provider `sec`, type
`cik`, `is_primary`) of every member of every component the effective generation names (3d-2 Designs 1–2), through
the shared SEC rate lane with `settings.sec_user_agent`. A fetch is `ok` when the status is 200, the body is at most
16 MiB, it parses as a JSON object, and its `cik`, zero-padded to 10 digits, equals the requested CIK; anything else
is `failed`, recorded, and the job fails after attempting every CIK.

Storage (migration `sql/490_tax_character.sql`), append-only by trigger (no UPDATE, no DELETE):
- `tax_character_contents`: `content_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`; `sha256 TEXT UNIQUE NOT
  NULL`; `body_gzip BYTEA NOT NULL`. A body is stored once.
- `tax_sec_submission_fetches`: `fetch_id` identity PK; `cik TEXT NOT NULL` (10 digits); `requested_at`,
  `observed_at TIMESTAMPTZ NOT NULL`, `observed_at ≥ requested_at`; `outcome` `ok` or `failed`; `http_status
  SMALLINT` (NULL allowed: no response); `content_id` FK, non-NULL iff `ok`; `error TEXT`, non-NULL iff `failed`.

### 2. Character (`tax_character_v1`; findings 18, 30)

Character is decided per **tax security**, from every member of its component (finding 30): SPY's CIK is on 3000 and
its lots on 3417, and both map to 3000. The run has one `as_of`, the clock reading taken right after its snapshot
starts; selection takes its own. A CIK's capture is the `ok` fetch with the greatest `(observed_at, fetch_id)` in `[as_of
− 7 days, as_of]`, or *none*. Facts read from it: `name`, `entityType`, `sic`, whether `filings.recent.form` contains
`10-K`, and `formerNames` (`name`, `from`, `to`).

Evaluated in order; the first line that applies decides.

| character | condition | `detail` |
|---|---|---|
| unestablished | members have more than one `instrument_type_id` (NULL counts as a value), or more than one primary CIK | `member_conflict` |
| unestablished | the type is 6 (ETF) | `exchange_traded_fund` |
| unestablished | the type is not 5 (Stocks) | `instrument_type` |
| unestablished | no primary CIK | `cik_unestablished` |
| unestablished | no capture | `sec_capture_unavailable` |
| unestablished | `entityType` ≠ `operating` | `entity_type` |
| unestablished | no `10-K` | `no_10k` |
| unestablished | `sic` empty, or one of 6722, 6726, 6770, 6799 | `sic` |
| unestablished | `name` lacks a legal-form word | `legal_form_unestablished` |
| unestablished | a former name whose `to` date is on or after the security's first admitted acquisition date (or has no or an unparseable `to`, or the security has no admitted acquisition) lacks a legal-form word | `legal_form_unestablished_in_holding` |
| **share** | otherwise | — |

- **Legal-form word.** Upper-case the name; remove SEC's trailing annotations `/XX/`, `/XX`, `\XX\` and `/NEW/`
  repeatedly; split on spaces and commas; the last token with its dots removed is one of `INC`, `INCORPORATED`,
  `CORP`, `CORPORATION`, `CO`, `COMPANY`, `LTD`, `LIMITED`, `PLC`, `NV`, `SA`, `AG`, `SE`. Structured fields carry no
  legal form (fact 2), so this is a text rule, built to admit only corporate forms: a partnership, LLC or trust is
  withheld, and so is a corporation whose name lacks a form word. A false withhold costs coverage; a false share
  would misstate a figure.
- **Why these tests establish a share outside the fund regime.** `operating` and a 10-K mark an issuer reporting as an
  operating company; SEC's own investment-vehicle SIC codes (6722 open-end funds, 6726 unit trusts and closed-end
  funds, 6770 blank checks, 6799 investors not elsewhere classified) are withheld, as are issuers without a SIC. What
  remains is an operating company's share, priced by trading with no NAV or index arrangement, so s356 condition C
  fails and it is not an offshore fund (Source rules). A foreign issuer that files a 10-K and passes is a share by the
  same argument; foreign private issuers on 20-F or 40-F are SEC `other` and withheld (fact 5).
- **Over time (finding 18).** The former-name line covers changes of legal form during the holding. The CIK bound
  today is taken as the issuer for the whole holding: a change of the instrument's issuer binding is an identity
  change of the security, which 3d-2's evidence identity governs, not this slice. A share's character has no
  reporting status or election to track; those exist only for funds (3d-3c).

**Withholding.** 3d-2 Design 3 gains the cause `character_unestablished`, one dependency row per withheld security
naming its `detail`. Withholding stays whole-security. Every security row gets a character row (Design 4), withheld
ones included: character classifies the security named by an admitted lot or an unresolved subject; whether its
lots are computed remains 3d-2's and 3c's gate (finding 45).

### 3. Routing and reasons

A computed security is a share, so each of its statutory disposals is a chargeable-gains disposal: `gain_gbp` is the
capital gain or, when negative, the allowable capital loss. No new amount is stored: the character row says how
every figure of the security is taxed. `character_pending` is no longer emitted; no reason replaces it.

### 4. Storage (migration `sql/490_tax_character.sql`)

- `tax_matching_runs` gains `character_policy_id TEXT` (NULL allowed: runs before this slice) and `as_of
  TIMESTAMPTZ` (NULL allowed, same). The unique key becomes `(evidence_run_id, matching_policy_id, class_policy_id,
  character_policy_id, inputs_digest)` with `NULLS NOT DISTINCT`. 3d-2's seal trigger already refuses every update
  outside `state`, `reason`, `counts` and `withheld_all`, so neither new column can change after insert; a DB test
  asserts it.
- `tax_matching_dependencies.reason` admits `character_unestablished`.
- **`tax_matching_characters`**: `(matching_run_id, tax_security_id)` PK, FK to the security row; `character TEXT NOT
  NULL` (`share`, `unestablished`); `detail TEXT`, non-NULL iff unestablished; `cik TEXT` (the single CIK, NULL when
  none or several); `fetch_id BIGINT` FK to `tax_sec_submission_fetches` (NULL when no capture was read). It takes
  3d-2's child guard (insert only while its run is `building`; no update or delete). Publication requires exactly one
  character row per security row, checked in code before the seal and asserted in the DB tests.

**`inputs_digest`** document `matching_inputs_v2`: v1's document plus `"character": [[tax_security_id, types, ciks,
sec], …]` sorted by security, `types` and `ciks` sorted lists (a NULL type as `null`). `sec` is `["none"]` when there
is not exactly one CIK, `["unavailable", cik]` when it has no capture, and otherwise `["ok", cik, name, entityType,
sic, has_10k, former_names]`, `former_names` as captured `[name, from, to]` triples in source order. The extracted
facts enter, not the content hash, so a new 8-K that changes SEC's document but no fact makes no new run; a capture
going stale changes `sec` and so the digest.

**Selection.** `effective_tax_matching` computes the digest with its own `as_of` and also requires
`character_policy_id = TAX_CHARACTER_POLICY_ID`. Run 1 (NULL) is not selected; a capture gone stale since the last run
makes the current digest differ, so the result is *unavailable* until the next run, never the old run.

### 5. The job and the run

`tax_character_evidence_refresh` is registered daily before `tax_broker_evidence_refresh` (12:13 UTC), at a minute
off the :15/:30 connection peaks, so the matching run inside the evidence job reads a fresh capture. `run_tax_matching`
reads the character inputs inside its REPEATABLE READ snapshot (3d-2 Design 10), sets `as_of`, and decides characters
before identification; a withheld security is never identified.

## Local policy (frozen under `TAX_CHARACTER_POLICY_ID = "tax_character_v1"`)

The rule table and its order, the legal-form words and annotation pattern, the SIC set, the 7-day window, the
former-name overlap test, and the single-CIK issuer assumption (Design 2). Changing any is a new policy id, which the
run key admits as a new run for the same generation.

## Not in this slice

- **3d-3c, offshore-fund disposals** (finding 17's ERI gate is lifted there, not here): relevant-securities
  identification for non-reporting funds (s104(3), s106A; 30-day FIFO then LIFO, and whether same-day aggregation
  applies), separate disposals rather than a daily netted one before reg 42's floor, OIG routing (regs 17, 18, 38–45),
  reporting status per share class over the holding from HMRC's list (fact 4) with identifier validation, ERI and
  equalisation for reporting funds (reg 99), reg 29 transparency, Condition B and reg 48/100 transitions. Prerequisite
  inside it: binding SPY's CUSIP (fact 4). It is buildable now and is the next tax slice after 3d-3b.
- **3d-3b, dividends:** their income treatment, the evidence that one was paid (sync `totalFees` combines overnight
  fees and dividends in one number, so a zero does not prove no dividend), and an admission policy for orders whose
  observed `totalFees` is non-zero, which 3c's gate makes `unresolved`. Planning deadline: before SPY's December 2026
  ex-dividend date.
- Foreign private issuers, partnerships, LLCs, trusts and blank checks (withheld); 3d-4's view.

## Tests

Pure, on `classify` and the digest document:
1. **Each table line alone**, in order: two types; a NULL type beside 5; two CIKs; type 6; type 4; no CIK; no capture;
   `other`; no 10-K; SIC 6770, 6722, 6726, 6799, empty; "Energy Transfer LP", "ENTERPRISE PRODUCTS PARTNERS L.P.",
   "X TRUST", "Y LLC", "U.S. BANCORP /DE/" (unestablished); "Apple Inc.", "GameStop Corp.", "ACME INC /DE/",
   "Acme Inc./NV", "ACME CORP \DE\", "ACME, INC. /NEW/" (share).
2. **Former names:** an LP name ending the day before the first acquisition (share); ending on it (unestablished);
   with no `to` (unestablished); a security with no admitted acquisition and an LP former name (unestablished); NXH's
   "BED BATH & BEYOND, INC." over its holding (share).
3. **Keying (finding 30):** a CIK on the base and lots on its `.RTH` variant give one character for the base.
4. **Freshness:** a capture 7 days old exactly is read, 7 days and 1 µs is not; two captures at one `observed_at`
   take the greater `fetch_id`; a capture after `as_of` is not read.
5. **Digest:** a new capture with identical facts gives an identical document; a changed `sic`, name or former name
   changes it; a stale capture gives `["unavailable", cik]`; the document's bytes and SHA-256 for a pinned fixture.

DB:
6. **Capture:** an `ok` and a `failed` fetch row each (a 404, a mismatched `cik`, a 16 MiB + 1 byte body); a repeated
   body stored once; UPDATE and DELETE refused.
7. **Run:** a run stores `character_policy_id`, `as_of` and one character row per security; an UPDATE of either new
   run column is refused after insert; a child insert into a published run is refused; a withheld security has a
   `character_unestablished` dependency and no disposal, identification or pool.
8. **Selection:** run 1 is not selected after the build; a fresh v1 run is; with time moved 8 days past its capture
   the selection returns *unavailable* and the next run publishes a new run.
9. **Job:** one failed CIK fails the job after the other CIKs are fetched; the matching step still reads the fresh
   captures.

## Acceptance (dev DB)

After deployment: one `tax_character_evidence_refresh` run, then `run_tax_matching`. For evidence run 2 (for a later
generation, rerun the Script and re-derive from its output the same way):

| security | character | status | expected |
|---|---|---|---|
| AAPL (1001) | share | computed | disposal 2026-09-23, 2026/27, gain **−£0.00753863550700**, 1 identification; reasons 3d-2's less `character_pending`; pool 0 / £0 |
| NXH (1181) | share | computed | pool 1305.05709600 / £6048.55403340328000 |
| SPY (3000) | unestablished, `exchange_traded_fund` | withheld | no disposal, identification or pool; 1 `character_unestablished` |
| GME (1699), ILMN (4077) | share | withheld | `unresolved_subject` 2 and 1 |
| IUSA.L (3075), CSPX.L (3434) | unestablished, `exchange_traded_fund` | withheld | `unresolved_subject` 1 and 3, plus 1 `character_unestablished` each |

Totals: 7 security rows (2 computed, 5 withheld), 7 character rows, 1 statutory disposal, 1 identification, 10
dependency rows (run 1's 7 plus 3), `withheld_all` false; `effective_tax_matching` returns the new run.

## Full-population verification

Every security of the effective run and every member instrument was classified (fact 3). The universe run (fact 5)
measures the rule's admission rate over every Stocks instrument with a CIK; it is coverage, and the spec claims no
correctness from it.

## Finding dispositions

| finding | where |
|---|---|
| 17 ERI as a disposal-cost dependency | every fund withheld (Design 2, `exchange_traded_fund`); the reg 99 treatment stated in Source rules; ingestion in 3d-3c |
| 18 character over time | Design 2: former names over the holding; captured, dated evidence; reporting periods and elections only for funds, in 3d-3c |
| 30 keying and conflicts | Design 2 (component, `member_conflict`); test 3 |
| 44 citation | Source rules (IFM13372, reg 99) |
| 45 classification only of real holdings | Design 2, Withholding: character classifies; 3c's product gate and 3d-2's withholding decide what is computed |

## Security

No order or authorisation path changes. The job reads one public, account-free source (operator rule 2026-10-10)
through the existing SEC rate lane; bodies are size-checked and validated before storage and live only in the DB. The
run stays a DB-only derivation.
