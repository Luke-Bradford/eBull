# #3146 — the insider `_current` projection has no within-filing line order

Status: proposal (revised after Codex ckpt-1, 26 findings). Refs #3146, #2794, #2385/#2386,
#788, #2788.

## The defect

`refresh_insiders_current` / `refresh_insiders_current_batch`
(`app/services/ownership_observations.py:341` and `:2062`) end their `DISTINCT ON`
`ORDER BY` with `source_document_id ASC`. When one filing reports several Table I lines for
the same `(holder_identity_key, ownership_nature)` on the same date, every prior key ties —
same source, same `period_end` (= `TRANS_DATE`), same `filed_at`, same accession — so the
balance that becomes "current" is decided by **string order on a DERA surrogate key**.

## Source rule

1. **What Form 4 fixes: the semantic target.** General Instruction 4(a)(i)
   (`https://www.sec.gov/files/form4.pdf`, p.7): *"Every transaction must be reported even
   though acquisitions and dispositions are equal. Report **total beneficial ownership
   following the reported transaction(s)** for each class of securities in which a
   transaction was reported."* The Note adds that Column 5 of Table I *"should reflect those
   holdings reported or required to be reported by the date of the Form"*; 4(a)(ii): *"Each
   transaction should be reported on a separate line."*
   ⇒ the figure we want is the one stated **after all transactions reported in that filing**,
   not an arbitrary intermediate line.

2. ⚠ **What Form 4 does NOT fix: the order of the lines.** The instructions impose no
   chronological or positional ordering on Table I rows — `grep -i 'chronolog' ` over the
   form's own instruction text returns nothing, and 4(a)(ii) requires only that each
   transaction occupy a separate line. **There is no published formulation**, so per
   `.claude/CLAUDE.md` ("Source-rule before design") the rule is fixed **by construction**
   and said so explicitly rather than attributed to a citation that does not carry it.

3. **The construction: document order, which the XML carries and the DERA dataset does not.**
   - `NONDERIV_TRANS_SK` is defined by the DERA Insider Transactions Data Sets readme §5.3
     (`https://www.sec.gov/files/insider_transactions_readme.pdf`) as *"Non-derivative
     transaction **surrogate key**"*, keyed with `ACCESSION_NUMBER` (§3 item 3). **No ordinal,
     line-number or sequence column exists in that table.** The documented rule therefore does
     not licence ordering on the SK — which is exactly why #3146 forbids a bare
     `source_document_id DESC`.
   - The XML does carry order, and this repo already honours it: `sql/056:25-28` stores
     `insider_transactions.txn_row_num` ("the row index disambiguates them") and
     `insider_transactions.py:1617` selects the greatest `(txn_date, txn_row_num)` before
     writing one observation per `(filer, nature)` per accession. This is implementation
     precedent, not source authority — cited as the in-repo settled behaviour the two paths
     must agree on, not as a rule.
   - The bulk DERA path (`sec_insider_dataset_ingest.py:660`) instead writes **one observation
     per transaction line** and leaves the choice to the projection. That is the second-writer
     gap, the same shape as #2790.

4. **Bridging the two, measured rather than assumed.** Ascending `NONDERIV_TRANS_SK` equals
   ascending XML document order on every accession both stores hold:
   `scripts/audit_3146_insider_line_order.py --order-rule` reports accessions tested,
   concordant, discordant, and — as a **negative control** — the same comparison with one
   SK pair deliberately transposed, which MUST report discordance or the check is
   structurally unable to fail. Exclusions (non-1:1 `(accession, date, balance)` matches,
   NULL balances, parser-rejected lines) are reported with counts and reasons, not dropped
   silently. Concordance is reported **twice**: over all multi-line accessions, and restricted
   to the same-date groups that actually exercise this tie-break.
   ⚠ **Scope of that evidence, stated:** it covers accessions this deployment holds parsed XML
   for. It is not evidence about the 91% cohort below, nor a guarantee the SEC will keep
   assigning SKs in document order; `--order-rule` is re-runnable as the drift detector.

## Full-population verification of the premise

`scripts/audit_3146_insider_line_order.py --census` — every figure computed at run time:

- The tie is **DERA-vs-DERA only**. Splitting every live `_current` row that has a tied
  sibling observation (tied on *all* of source-priority, `period_end`, `filed_at` and
  `source`, per Codex #18, with `known_to IS NULL` on both sides) by provenance yields
  `(winner DERA, sibling DERA)` and `(winner XML, sibling DERA)` and **no XML-vs-XML pair** —
  the XML path's own reduction never emits two rows for one key per accession, and #788's
  de-collision means a DERA row never beats an XML sibling on the same CIK.
- Groups whose tied set holds more than one distinct balance — counted NULL-aware, since
  `count(DISTINCT shares)` cannot see a NULL-vs-value disagreement (Codex #19).
- **XML document order reaches only 8.9% of those groups**; the rest sit on accessions this
  deployment holds no parsed XML for at all, spanning `period_end` 1998 … 2026. A fix that
  resolved the order by reading `insider_transactions` would miss ~91% of the population —
  it is rejected for that reason, not on complexity.

## The change

One shared constant used by both the single-instrument and the batch projection
(prevention-log §"DISTINCT ON / ROW_NUMBER without a UNIQUE final tie-break diverges between
single-key and bulk plans" — the two forms must agree), replacing `source_document_id ASC`
with:

```sql
    -- accession prefix of the doc id; NOT NULL because source_document_id is a PK column
    split_part(source_document_id, ':', 1) ASC,
    -- last Table I line of the filing (#3146). ':NDT:' ONLY — see below.
    (CASE WHEN source_document_id ~ ':NDT:[0-9]+$'
          THEN split_part(source_document_id, ':NDT:', 2)::numeric END) DESC,
    source_document_id ASC
```

- **Derived from `source_document_id`, not `source_accession`** (Codex #1/#2/#5).
  `source_accession` is nullable and nothing constrains it to equal the doc-id prefix, so
  ordering on it would move cross-filing winners and put NULL accessions last. The doc id is
  part of the table's primary key, so it is NOT NULL, and its prefix is the accession for all
  three writers (`{accession}` for XML, `{accession}:NDT:{sk}` / `{accession}:NDH:{sk}` for
  the bulk path — accession numbers contain no `:`).
- **Cross-accession order is unchanged by construction**, which is what confines the blast
  radius: accession numbers are fixed-width (`0001234567-25-001234`), so no prefix is a strict
  prefix of another, and prefix-lexical order is exactly today's doc-id-lexical order between
  filings. Verified as an A/B assertion, not just asserted.
- **`:NDT:` only** (Codex #6). `:NDH:` rows are Form 3 / holdings lines, which are a
  simultaneous snapshot per class and ownership form, not a transaction sequence — "the last
  line" has no meaning there, and the concordance measured above is about
  `NONDERIV_TRANS_SK`, which says nothing about `NONDERIV_HOLDING_SK`. Holdings rows keep
  today's ordering exactly.
- A missing or `"0"` SK yields doc id `…:NDT:0`, which sorts last among that filing's DERA
  rows rather than first. An XML row has no `:NDT:` segment, so the CASE yields NULL and
  `DESC` (NULLS FIRST in Postgres) keeps **today's XML-row-wins behaviour** on a mixed tie.
  `NULLS LAST` would invert it and hand the group to a DERA row that #788's de-collision may
  then drop — deleting the key outright, since the MERGE prunes `WHEN NOT MATCHED BY SOURCE`.
- `::numeric`, not `::bigint`: Postgres `numeric` has no practical digit bound, so an
  unexpectedly long SK cannot overflow (Codex #23).

No schema change, no migration, no new column, no re-parse — the observations already carry
the SK inside the doc id. Rollback is the reverted constant plus the same projection refresh.

## Acceptance

1. **Ground truth, and not a vacuous one** (Codex #20/#21). The XML oracle is evaluated
   **only on keys with no XML sibling**, because where the plain XML observation already wins
   the key, agreement says nothing about the SK ordering. On those keys the new winner must
   equal the XML path's own pick — `post_transaction_shares` of the greatest
   `(txn_date, txn_row_num)` non-derivative line, reproducing that path's filters
   (`txn_date_invalid` excluded, NULL `post_transaction_shares` excluded). Old-winner and
   new-winner agreement are reported separately; the old rule is expected to disagree on most.
2. **Full-population A/B over the final, post-de-collision set** (Codex #3/#4): a FULL OUTER
   comparison of `(instrument_id, holder_identity_key, ownership_nature)` keys — added,
   **deleted**, value-changed, provenance-changed — not an inner join on winners, which cannot
   see a key the de-collision drops. **Zero deleted keys is a release condition**, not an
   observation. Distinct-entity metric throughout; never a row count.
3. Single-instrument and batch projections must agree on the same input (the #2269 divergence
   check), asserted in a DB test.
4. Expected-winner fixtures (Codex #24): the lexical-vs-numeric boundary (`:NDT:999` vs
   `:NDT:1000`, where lexical and numeric disagree), an `:NDH:` group (must not move), a mixed
   XML/DERA tie (XML must still win), and a cross-accession tie (must not move).
5. Definition-of-Done clauses 8-11: golden panel `AAPL`, `GME`, `MSFT`, `JPM`, `HD`; one
   cross-source check against the SEC's own rendering of an affected filing; the projection
   refreshed for every affected instrument; `/instruments/{symbol}/ownership-rollup` rendered
   after.

## What this does NOT fix (stated, not silently inherited)

- **The `ownership_nature` overload (#2385/#2386).** A DERA row's nature is role-derived
  (`_map_relationship`: officer/director → `direct`), NOT Table I's D/I field, so one holder's
  Direct and Indirect lines still share one key and still compete. ⚠ Codex #8 is right that
  this cuts both ways: on a key that conflates two series, moving to the filing's last line
  can replace a previously-correct balance with one from the other series. The fix makes the
  rule principled and uniform; it cannot make a conflated key correct. The A/B reports how
  many changed keys are D/I-conflated so the exposure is sized rather than assumed away.
- **`SECURITY_TITLE`** is likewise dropped at ingest, so two classes reported in one filing
  collapse into one key. Form 4 Instruction 4(b)(iii) requires separate lines per indirect
  vehicle, which we also cannot separate (Codex #9).
- **Joint-filer attribution** (Codex #10): `<nonDerivativeTable>` is a sibling of
  `<reportingOwner>`, so Table I lines name no co-filer; the bulk path fans every line to
  every reporting owner. Unchanged here, and documented in sec-edgar skill §2.3.
- **Amendments** (Codex #12): a 4/A may restate only corrected lines. The last-line rule
  applies to whatever that filing reports, which is the same treatment the XML path already
  gives it — no better, no worse.
