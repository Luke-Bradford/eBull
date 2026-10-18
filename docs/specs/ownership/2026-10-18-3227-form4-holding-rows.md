# #3227 — a Form 4's Table I holding lines reach the insider observation

Status: spec (2026-10-18). Residual of #3227 named on #3879 ("Form 4 `nonDerivativeHolding` rows
are not stored"). Rung: corpus change; this document is the judgement artefact (Codex
checkpoint 1).

## Problem

A Form 4 Table I carries two kinds of row:
- `nonDerivativeTransaction`: a transaction and the balance after it;
- `nonDerivativeHolding`: a balance with no transaction.

Neither insider writer keeps the second kind when the filing also reports a transaction:

- **XML path.** `parse_form_4_xml` (`app/services/insider_transactions.py:493`) reads only
  transactions. A Form 4 with holdings and no transaction returns `None` and is tombstoned.
- **DERA path.** `sec_insider_dataset_ingest`'s `NONDERIV_HOLDING` loop skips every accession
  that has a `NONDERIV_TRANS` row (`app/services/sec_insider_dataset_ingest.py:799`).

So a `(holder, D/I)` series that a filer reports only as a holding never updates from that
filing. `ownership_insiders_current` keeps the balance from the last filing that transacted in
it.

Example: SLSN, Bradford T. Whitmore, `0000933430-25-000004` (filed 2025-06-02).
- Its `I` lines transact through Strandler and Whitmore Holdings.
- Its `D` line is a holding: 108,141.
- The `direct` key still holds 25,467,020, from `0000933430-24-000002` (2024-02-12).

## Source rule

From the SEC Form 4 General Instructions (`https://www.sec.gov/files/form4.pdf`, read
2026-10-18), verbatim:

- 4(c)(i): *"Report acquisitions or dispositions and holdings of non-derivative securities in
  Table I."*
- 4(a)(i): *"Report total beneficial ownership following the reported transaction(s) for each
  class of securities in which a transaction was reported."*
- The note to 4(a)(i): *"The amount of securities beneficially owned following the reported
  transaction(s) specified in Column 5 of Table I and Column 9 of Table II should reflect those
  holdings reported or required to be reported by the date of the Form. Transactions and holdings
  eligible for deferred reporting on Form 5 need not be reflected in the month end total unless
  the transactions were reported earlier or are included on this Form."*
- 4(b)(iii): *"Report transactions in securities beneficially owned directly on a separate line
  from those beneficially owned indirectly. Report different forms of indirect ownership on
  separate lines."*

The EDGAR Ownership XML Technical Specification §4.3.8.2, as quoted at
`sec_insider_dataset_ingest.py:844-850`, says holdings reported in `4` submissions carry no
`<transactionCoding>`. A holding row is therefore column 5 alone.

**What the rule settles:**
- A holding row is a stated balance for its class and form of ownership.
- It sits on its own line, by D/I and by form of indirect ownership, exactly like a Form 3 line.
- The balance reflects holdings "reported or required to be reported by the date of the Form".
  The note's second sentence means it may omit changes deferred to Form 5. It is the filer's
  attested balance, not necessarily the economic position.

**What it does not settle:**
- **Which date "the date of the Form" is.** The XML carries no per-holding date. The form's own
  dated attestation is the signature block (`ownerSignature/signatureDate`), so the spec reads
  "the date of the Form" as the signature date, bounded by the filing's other dates. This is an
  interpretation, fixed and frozen in (3), not a quotation.
- **How a holding line combines with transaction lines of the same series.** This slice leaves
  that case unchanged (mixed groups, below).

## Full-population verification (dev, 2026-10-18, read-only)

Every count in this section comes from:

```bash
PYTHONPATH=. uv run python -m scripts.census_3227_form4_holding_rows --examples 1 \
  --show 0000933430-25-000004 --show 0000752642-26-000124 --show 0001505732-26-000116
```

It was run at 2026-10-18 03:04Z, in one `REPEATABLE READ` snapshot, from the script as committed
in this PR (base `c73495e9`).

The script reads every stored `form4_xml` payload whose text contains `erivativeHolding` (either
table). It takes holdings, transactions, dates and owners **from the payload itself**, with the
production helpers. It attributes lines to `filers[0]`, as every XML writer does, and applies the
Change section's rule.

**Cohort.**
- 129,234 payloads, one per accession. 117,680 carry a non-derivative holding (229,513 lines). The
  other 11,552 carry derivative holdings only (parsed, not inferred); 341 of those are tombstoned,
  and 285 of the 341 pass the replay gates in (4).
- Excluded from the non-derivative cohort (these counters cover only it): 2 with no
  `insider_filings` row; 3,308 with no manifest `filed_at`; 8,976 outside the Form 4 retention
  window. The document type is read from the payload, as the parser does; 469
  payloads disagree with their `insider_filings.document_type` (none falls outside 4/4A).
- Holding lines beside a transaction of the same filer and D/I (mixed, out of scope): 12,796 `D`
  and 43,612 `I`.
- 4 holding-only lines carry no share count (value-only or unreadable), counted after the gates
  and the mixed filter; their 4 groups refuse.
- 0 written groups lack a numeric filer CIK.

**Holding-only groups**, one per `(instrument, accession, filer, D/I)`:

| verdict | D live | D tombstoned | I live | I tombstoned |
|---|---:|---:|---:|---:|
| one line | 23,290 | 856 | 45,443 | 238 |
| several, summed | 270 | 20 | 18,222 | 143 |
| refused: multi-title | 783 | 43 | 1,260 | 20 |
| refused: equal holding amounts | 13 | 1 | 4,134 | 17 |
| refused: equal to a transaction balance | 2 | 0 | 2 | 0 |
| refused: joint filing | 121 | 0 | 426 | 15 |
| refused: a line with no share count | 1 | 0 | 3 | 0 |

Every written group has a `period_end` under rule (3).

**Effect on `_current`.** The script takes, per `(instrument, filer, D/I)` key, the latest written
group by `(period_end, filed_at)`. It compares that group with the stored winner by the winner
order: source rank, then `period_end` DESC, then `filed_at` DESC, with PostgreSQL's NULLS FIRST
(0 keys hit a NULL). This is an approximation: it does not run the amendment filter or
de-collision. The A/B, not this table, is the proof.

| verdict | keys | instruments |
|---|---:|---:|
| the holding wins and its value differs (stale key) | **3,600** | **1,588** |
| a new key (no `_current` row today) | 4,214 | 1,817 |
| the holding wins, same value | 3,966 | 1,831 |
| the holding loses to the current winner | 5,716 | 1,971 |
| ties the current winner on every order key (the tail decides) | 69 | 38 |
| wins by source rank over a later-dated `form3` winner | 3 | 3 |

The stale keys split by today's winner:
- `form3`: 1,385 (2 of them DERA rows);
- DERA `form4`: 366;
- XML `form4`: 1,849.

**De-collision.** Clause 1 of `_INSIDER_DUAL_PIPELINE_DECOLLISION` drops a DERA winner once an
XML winner shares its instrument, holder and accession. 989 tombstoned accessions have a written
holding-only group. Over every accession where a holding group wins its key in the comparison
above:
- 558 DERA `_current` winners sit on the very key the holding wins, so they are replaced there;
- 91 DERA winners over 66 instruments sit on another key of the same holder and accession, and
  would be dropped. 59 of them equal the amount of some XML holding observation from that
  accession, which is not proof that it covers them. A DERA row on a key that another accession's
  holding wins is counted as replaced, not dropped. The A/B lists all 91 (residual below).

**Examples** (`--show`):
- **SLSN** `D`: one line, 108,141, `period_end` 2025-06-02. Today's winner is `form4`
  `0000933430-24-000002`, `period_end` 2024-02-08, 25,467,020.
- **BWFG** `0001505732-26-000116`: five `D` lines of Common Stock, summed to 40,919. Footnotes
  F1–F4 make four of them restricted grants (400 · 534 · 1,200 · 1,455); the fifth is 37,330. The
  key holds 41,908 today, from a March DERA `:NDT:` line.
- **UMH** `0000752642-26-000124`: the `D` lines are 37,674.78 common and 500 "Preferred D", so the
  group is refused as multi-title and writes nothing. `form3_observation_shares`'s last-line
  fallback would have written 500, a different class; that is why a refused group writes nothing
  here. Its `I` lines (500 · 763.72 · 648.14, common) sum to 1,911.86. The equal-amount guard is
  per title, so the preferred 500 does not refuse the common sum. Today the `indirect` key holds
  500, from `0000752642-26-000096`.

## Change

### 1. Parse

`parse_form_4_xml` also returns the filing's holdings, via `_extract_holdings`:
- non-derivative and derivative, in document order;
- `row_num` shared across both tables;
- every line attributed to `filers[0]`.

It returns a filing when it has transactions **or** holdings. A holdings-only Form 4 is therefore
no longer tombstoned; `upsert_filing`'s `ON CONFLICT … DO UPDATE` sets `is_tombstone = FALSE`.
Form 5 is unchanged (its parser keeps holdings out of scope). The parser version is bumped.

### 2. Store

A new table, `insider_form4_holdings`:
- has the columns of `insider_initial_holdings`, keyed `(accession_number, row_num)`;
- is written by `upsert_filing` beside the transactions, with the same
  `ON CONFLICT (accession_number, row_num) DO UPDATE` upsert that `insider_transactions` uses.

`insider_initial_holdings` is not reused. Its readers treat every row as a Form 3:
- the `form3` derivation in `sync_insiders` (`ownership_observations_sync.py:476`);
- de-collision clause 2 (`ownership_observations.py:355`);
- the amendment filter (`ownership_observations.py:552-562`);
- the drill-through counts (`ownership_drillthrough.py:429`);
- the rollup (`ownership_rollup.py:3444`);
- the holder-name resolver (`holder_name_resolver.py:175`);
- the instruments API (`instruments.py:3379`).

### 3. Observe

One helper decides each holding-only group's value. Both Form 4 observation writers call it: the
live `_record_form4_observations_for_filing` and the weekly `derive_form4_observations`. A
live-only fix would be reverted by the weekly re-derivation (#3874).

- **Scope.** Form 4 and 4/A.
- **Holding-only group.** One filer's non-derivative `D` holding lines, or `I` holding lines, in
  one accession, where the accession has **no** non-derivative transaction line for that filer and
  D/I.
  - Any transaction line counts, whether or not today's filter would use it.
  - A transaction line with no D/I counts as `D`, because both writers label it `direct`
    (`insider_transactions.py:1792`, `ownership_observations_sync.py:380`).
  - Where a transaction line exists, today's rule governs and the holding lines are ignored.
- **Value.** A group with any line that has no share count (the SEC value branch, or an
  unreadable amount) refuses: its share total is unknown. Otherwise:
  - **One line:** its shares.
  - **Several lines:** their sum, when `form3_observation_shares`'s conditions hold:
    - the D/I is known;
    - the filing has one owner;
    - every line is ≥ 0;
    - the group has one non-blank title;
    - no two equal amounts among the filer's holding lines of that title, `D` and `I` together.

    One condition is added: no amount in the group equals a column-5 balance on one of the
    filer's **transaction** lines of that title in the same accession. A Form 4, unlike a Form 3,
    carries balances on transaction rows, and a holding equal to one may be the same block
    restated (Instruction 4(b)(iv)). Transaction balances repeating among themselves (100 → 90 →
    100) are a sequence, not a snapshot, so they do not refuse. The Form 3 conditions are reused
    because a holding table is the same simultaneous per-class snapshot as a Form 3 table, under
    the same instruction (Form 4 4(b)(iii), Form 3 5(b)(iii)).
  - **Refused:** no observation from this group. The value is never a partial sum, and never
    the last line. Today's value for the key stands, except where de-collision drops a DERA row
    of the same accession (residual below).
- **Date (`period_end`).** By construction, from the interpretation above:
  1. the signature date, when `period_of_report ≤ signature date ≤ filed date`;
  2. else `period_of_report`, when it is on or before the filed date;
  3. else no observation.

  The filed date is the UTC date of `sec_filing_manifest.filed_at`. The bounds refuse a typo
  in either date that would otherwise dominate the `period_end DESC` order, the #1687 hazard.
- **Gates, identical in both writers.**
  - A holding observation needs a manifest `filed_at`, and that date must pass
    `form4_within_retention`.
  - The live writer's transaction-date fallback for a missing `filed_at` does not apply to
    holdings. The weekly writer's `filing_events` fallback does not apply either.
- **Observation fields.**
  - `source='form4'`.
  - `source_document_id = source_accession = accession`.
  - `ownership_nature` is `direct` for `D` and `indirect` for `I`.
  - `filed_at` is the manifest timestamp.
  - The holder name is `_primary_filer_name`, in both writers.
- **The weekly writer's unit selection widens.**
  - Today it selects units that have an eligible transaction line.
  - Holding units are added from `insider_form4_holdings`: those with a non-derivative holding
    and a manifest `filed_at` in retention.
  - `since` applies to `period_end` under rule (3), and `LIMIT` applies to the union.
  - For a selected unit, the writer loads its holdings, all of its transaction lines (for the
    holding-only test and the guard), and `insider_filings.signature_date` / `period_of_report`.

### 4. Backfill

A script re-parses the stored `form4_xml` payloads containing `erivativeHolding` (129,234) that
pass the manifest parser's own pre-fetch gates: a manifest `filed_at` within the Form 4 retention
window (`manifest_parsers/insider_345.py:153-173`), taken on the UTC date as the weekly writer
does. (The manifest parser calls `.date()` on the session-timezone value; dev returns `Etc/UTC`,
so the two agree here, and the build pins UTC explicitly.) The 8,976 out-of-retention and 3,308 undated
payloads are skipped, so the replay cannot write transactions the weekly writer would exclude. The
rest run through the production parse and upsert path, with no refetch. SEC filings are
immutable per accession, the reasoning behind `_parse_form4`'s stored-body reuse
(`manifest_parsers/insider_345.py:177-183`).

This also turns live the derivative-holdings-only tombstones that pass those gates (285 of 341), so
history matches what new ingest does. The script then runs `sync_insiders` with no `since` and no `LIMIT`, and then the
batch `_current` refresh.

### Unchanged

- **The DERA ingest.** Its `:NDH:` rows carry the relationship flag in `ownership_nature`, not
  D/I (`sec-edgar.md`, "ownership_nature is an OVERLOADED column"). Lifting its holdings skip on
  transaction accessions would add relationship-keyed balances beside XML ones.
- **`form4_observation_shares`** (the `I` vehicle sum) and its 5c sources.
- **`INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER`**, which matches transaction fields.
- **De-collision.** Clause 1 is unchanged; its new effect is measured above.

## Accepted residuals (not fixed here)

- **Mixed groups** (12,796 `D` and 43,612 `I` lines): a holding line beside transaction lines of
  the same filer and D/I.
  - SLSN's `I` series is one: its "By Grace Investments, LP" line is a holding, beside the
    Strandler and Whitmore Holdings transactions.
  - The boundary is classless, so a preferred-stock transaction also blocks a common-stock
    holding of the same D/I. The key holds no class, so a per-class boundary would still need a
    rule for combining two classes on one key.
  - The next slice folds holding lines into `form4_observation_shares` as one-line forms.
- **Refused groups** (2,106 multi-title, 4,165 equal holding amounts, 4 equal to a transaction
  balance, 562 joint filings, 4 with a line lacking a share count; summed from the table above). Each writes
  nothing. Multi-title is the separate multi-title residual.
- **Nested blocks.** The sum conditions refuse equal amounts, not a block included inside a larger
  unequal one (100 and an included 40 give 140). Instruction 4(b)(iv) lets an indirect line
  restate an entity's whole holding, so such overlap can exist. This is the residual the Form 3
  line sum already accepts (#3874).
- **One-line groups of another class.** A single "Class B" holding lands on the same classless key
  as an earlier "Class A" balance. The Form 3 and Form 4 transaction writers already do the same
  (multi-title residual).
- **Form 5 deferral.** Per the 4(a)(i) note, a correct holding balance may omit changes deferred to
  Form 5. The observation records the attested balance.
- **Late and backdated filings.** The winner order is `period_end` first. A holding dated by its
  signature can lose to an earlier-filed form with a later date, or beat a later-filed form with
  an earlier one. Transaction observations already behave this way.
- **Amendments.** A 4/A's holding observation competes with its original's on `period_end`, then
  `filed_at`. A correcting 4/A signed earlier than its original loses. An amendment that removes a
  holding line, or turns a written group into a refusal, leaves the original's observation
  standing. The transaction-field amendment filter does not see holdings. The A/B lists every key
  where a 4/A and its original both write a holding observation.
- **De-collision partial coverage.** Once a holding observation wins a key, clause 1 drops every
  DERA winner of that holder on the same accession, on any key. That includes a DERA row standing
  for a refused or mixed line, which then loses today's value. The census counts 91 such winners
  on another key over 66 instruments. Whether an XML observation carries each one's balance needs
  class and D/I, which the DERA `_current` row does not hold, so the A/B lists all 91 for
  inspection. Accessions whose transactions already produce an XML winner behave this way today.
- **Source rank over date.** `form4` outranks `form3` whatever the date. A holding-only Form 4
  older than a key's `form3` winner therefore takes the key. The census counts 3 such keys.
- **Joint filings.** A one-line group on a joint filing is written to `filers[0]`, as the Form 3
  and Form 4 transaction writers do. A multi-line joint group refuses.
- **Not in this slice:** Form 5 holdings (869 payloads), and holding lines as 5c sources.

## Acceptance

Corpus-change evidence per `.claude/CLAUDE.md`, recorded on the build PR with SHAs:

1. **Unit tests**, both directions:
   - the parser returns holdings, and a holdings-only Form 4 (non-derivative, or derivative only)
     is a filing, not `None`;
   - one-line `D` (SLSN), a multi-line sum (BWFG), and a multi-title refusal that writes nothing
     (UMH);
   - an equal-amount refusal triggered by a transaction balance;
   - a mixed group, and a transaction with no D/I covering `D`; each keeps today's value;
   - every `period_end` branch, including a future signature date and a future `period_of_report`;
   - the retention and missing-`filed_at` gates;
   - a 4/A; and a Form 5, unchanged.
   - A DB test drives both writers on one stored holdings-only filing and one mixed filing, and
     gets equal observations from each.
2. **Full-population A/B** over the 129,234 accessions, against the real refreshers. Report:
   - observations added, changed, unchanged and removed. Changed and removed must be 0: a
     holding-only group has no observation today. Anything else is listed;
   - `_current` keys added, changed and removed, from both refreshers;
   - rollups moved;
   - `ownership_history` points added and removed;
   - newly oversubscribed instruments (gate: 0, or each inspected);
   - each of the 91 dropped DERA winners, inspected against the filing for whether an XML
     observation carries its balance, and the 558 replaced on the same key;
   - the 3 source-rank keys, the 69 ties and the 4/A keys, each listed.

   The A/B's key verdicts are compared with this census's. Any difference beyond new filings
   between the two runs is explained.
3. **Smoke panel** (AAPL, GME, MSFT, JPM, HD): unchanged, or each move explained.
4. **Cross-source against the EDGAR Form 4 rendering** (sec.gov HTML, not our stored XML):
   - SLSN `D` 108,141;
   - BWFG `D` 40,919;
   - UMH's two `D` titles;
   - one formerly tombstoned holdings-only filing.
5. **Backfill and verify**, each with 0 differences:
   - every stored Form 4/4A observation equals the writers' recompute;
   - stored `_current` equals a fresh batch refresh.
6. **Live re-check after the backfill:**
   - `/instruments/SLSN/ownership-rollup` shows Whitmore's direct slice at 108,141;
   - the insider filings list and summary for one formerly tombstoned, zero-transaction filing
     render without error.

## Security

No authorisation surface: an ingest change on SEC public data, with no new endpoint or input.
