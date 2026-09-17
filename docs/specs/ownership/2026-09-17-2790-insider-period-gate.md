# #2790 — insider observations with a `period_end` after their filing date

⚠ **This spec replaces a first draft that was killed at Codex checkpoint 1.** That draft proposed
a new global `period_end > filed_at` gate keyed on `source`. Three things were wrong with it and
all three are load-bearing: the gate **already exists** (#1687), it carries an **exemption** the
draft would have broken (`transaction_timeliness='E'`), and **`source` is not the form type**.
The withdrawn design is recorded in §6 rather than deleted.

## Source rule

**Form 4 / Form 5 — 17 CFR 240.16a-3(g):** *"Form 4 must be filed before the end of the second
business day following the day on which the subject transaction has been executed."* 16a-3(f)
puts Form 5 within 45 days **after** fiscal year end, reporting transactions **during** that
year. In both, the filing follows the reported date, so `period_end > filed_at` is impossible.

**Form 3 — Exchange Act §16(a)(2), 15 U.S.C. §78p(a)(2):** filed *"(A) at the time of the
registration of such security on a national securities exchange **or by the effective date of a
registration statement**…"* or *"(B) **within 10 days after** he or she becomes such beneficial
owner, director, or officer…"*. **Both are LATEST bounds.** Nothing sets an earliest bound, so a
Form 3 filed ahead of a known future event date is compliant and `period_end > filed_at` is the
**expected** order. Form 3 is therefore **not gated**, and no upper bound is invented for it.

Corroboration, not the basis: 123 of 165 `source='form3'` reject rows lead by exactly 1 day and
37 by 3 days. The `WT` cluster is the clause-(A) signature (three WisdomTree officers, one
`2011-07-25` event, all filed `2011-07-22`); `APOG` is clause (B) (Puishys named CEO effective
`2011-08-22`, filed `2011-08-08`).

**DERA field names — SEC Insider Transactions Data Sets readme, NONDERIV_TRANS table:**
`TRANS_DATE`, `DEEMED_EXECUTION_DATE`, `TRANS_TIMELINESS` (VARCHAR2(1)). Appendix 6.1 Timeliness
List: **`E` = Early, `L` = Late, empty = On-time** — identical semantics to the XML
`transactionTimeliness`, so #1687's exemption transfers verbatim rather than being re-derived.

## The premise is false, and that reframes the ticket

The issue says *"The insider path has none."* It has one.
`app/services/insider_transactions.py:274::evaluate_insider_date_validity` is this exact
invariant, citing the same reg, added by **#1687**. It sets `insider_transactions.txn_date_invalid`;
`:1609` (`if txn.txn_date_invalid: continue`) excludes flagged rows from the observation write for
precisely the reason this ticket gives; readers exclude them at `:2356` and `:2478`. 86 of
1,064,351 transactions are flagged.

It also carries an exemption the first draft would have broken: **`transaction_timeliness == 'E'`**
— an early filing may legitimately report a future date — pinned by
`tests/test_insider_transactions_ingest.py::test_early_filing_future_txn_is_kept`.

**So this is a second-writer gap, not a missing rule.** `app/services/sec_insider_dataset_ingest.py:336`
carries its own `INSERT INTO ownership_insiders_observations` for the bulk DERA drain, bypassing
`record_insider_observation`, and applies no date check: `:580` takes
`period_end = TRANS_DATE or PERIOD_OF_REPORT` unchecked. **998 of the 1,147 breaching rows (87%)
have no `insider_transactions` row at all.**

## ⚠⚠ `source` is not the form type — the correction predicate must not use it

`sec_insider_dataset_ingest._map_form_to_source` maps anything **not** starting with `3` to
`source='form4'`, including blank/unknown `DOCUMENT_TYPE`; and `ownership_observations_sync.py:213`
admits `document_type IN ('3','3/A')` into the transactions query, which also writes
`source='form4'`. Full-population attribution of the 1,147, resolving form type from
`sec_filing_manifest.form` and the `:NDT:` / `:NDH:` document markers:

| manifest `form` | marker | `source` | rows | live |
|---|---|---|---|---|
| (not in manifest) | `NDT` | form4 | 584 | 584 |
| 4 | `NDT` | form4 | 209 | 166 |
| (not in manifest) | `NDH` | **form3** | 165 | 165 |
| (not in manifest) | `NDH` | **form4** | **101** | **101** |
| 4 | none | form4 | 61 | 12 |
| 5 | `NDT` | form4 | 18 | 13 |
| 5 | none | form4 | 5 | 0 |
| 4/A | `NDT` | form4 | 3 | 3 |
| (not in manifest) | none | form4 | 1 | 0 |

**The 101 highlighted rows are why `source='form4'` is unsafe as a correction predicate** — it
would soft-delete them, and they are almost certainly Form 3 holdings whose DERA `DOCUMENT_TYPE`
was blank or unmapped. Evidence: a Form 3 has no transaction table, so `:NDT:` implies Form 4/5;
**all 101 sit on accessions that produced only `:NDH:` rows and never an `:NDT:` row** — the same
signature as the 165 known Form 3 rows (control: 165/165 also `NDH`-only).

⚠ That is a signature, not a proven form type. They are therefore **exempted**, which is the
conservative direction (keep data whose form cannot be established), and the count is recorded
rather than absorbed.

**Scope reconciles exactly: 778 correctable + 266 exempt = 1,044 live.**

## Design

### Gate (reuse, do not re-derive)

In `sec_insider_dataset_ingest`, call the existing `evaluate_insider_date_validity` — the same
function the XML path uses — passing `TRANS_TIMELINESS` and `DEEMED_EXECUTION_DATE` so both of
#1687's exemptions (`filed_at is None`, `timeliness == 'E'`) apply identically on both writers.

Applied **only when `form_upper.startswith(("4", "5"))`**, in both the transactions loop and the
holdings loop. A blank or unmapped `DOCUMENT_TYPE` is **not** gated — fail-open, because rejecting
a row whose form we cannot establish is the failure mode that loses Form 3 data. The gate's
coverage is bounded by that and the bound is stated rather than hidden.

New counter `rows_skipped_future_dated` on `InsiderIngestResult`, separate from
`rows_skipped_bad_data` for the same reason `rows_skipped_retention` is separate — an operator
reading the run summary must be able to tell a deliberate invariant rejection from malformed input.

### Correction

Soft-delete (`known_to`), never hard-delete: `refresh_insiders_current` selects
`WHERE instrument_id = %(iid)s AND known_to IS NULL` (`ownership_observations.py:320`), so the
re-derivation drops the rows from the current layer without destroying evidence.

Predicate — attribution, not `source`:

```
period_end > (filed_at AT TIME ZONE 'UTC')::date
AND known_to IS NULL
AND source_document_id NOT LIKE '%:NDH:%'          -- holdings rows are form-ambiguous: exempt
AND ( source_document_id LIKE '%:NDT:%'            -- a transaction row implies Form 4/5
      OR EXISTS (SELECT 1 FROM sec_filing_manifest m
                 WHERE m.accession_number = source_accession
                   AND m.form IN ('4','4/A','5','5/A')) )
```

Then `refresh_insiders_current_batch` over the affected instruments.

⚠ **Do NOT infer the intended year** for the 300–399 day band. That is a repair, not an ingest
guard, and a 2047 date does not identify what was meant at all.

## Acceptance

1. `period_end > filed_at::date` returns **0** among live rows attributable to Form 4/5 by the
   predicate above, in `ownership_insiders_observations` and `ownership_insiders_current`.
2. **The 266 exempt rows survive** (165 `source='form3'` + 101 `NDH`-only). A gate or correction
   that also removed them is over-broad, and this is the assertion that catches it.
3. The 2047-05-24 row (`instrument_id` 10510) is gone from `_current`.
4. Re-running the DERA ingest over a corrected accession does not re-create the row.
5. An `E`-timeliness DERA row with a future `TRANS_DATE` is **kept** — the #1687 exemption holds
   on the new writer too.

⚠ **Not claimed: "exactly N fewer `_current` rows, nothing else changed".** Removing a winner can
promote an older observation for the same key rather than delete the key, so the current-layer
delta is measured after the fact, not predicted. ⚠ `ownership_history.py:189` filters closed rows,
so `as_of_min/max` move retroactively for affected instruments — a read-path consequence, recorded
because it is not obvious from the storage change.

## Tests

Pure-logic table test over the DERA gate decision: form 3 / 4 / 5 / blank × lead-time ≤ 0 and > 0
× timeliness `E` / `L` / empty × `filed_at` present and absent. Explicitly including: a form 3 row
at +1 day is kept; a form 4 row at +1 day is rejected; a form 4 `E` row at +1 day is kept; a
blank-form row at +1 day is kept (documenting the fail-open bound rather than pretending it is
closed).

## 6. The withdrawn first draft, kept for the record

Global gate on `source = 'form4' AND period_end > filed_at`, refusing inside
`record_insider_observation` by `ValueError`. Withdrawn because:

1. #1687 already implements the rule on the XML path — it was reinvention.
2. It rejects `transaction_timeliness='E'` rows that SEC and an existing test say are legitimate.
3. `source='form4'` includes real Form 3 rows (the 101 above), so both the gate and the correction
   would have destroyed correct data.
4. ⚠ `record_insider_observation` is called at `insider_transactions.py:1643` **outside any
   per-row `try/except`**; the manifest caller wraps the whole filing in one transaction
   (`manifest_parsers/insider_345.py:288`), so a `ValueError` would roll back every typed row,
   observation and sibling refresh for that filing and tombstone it as a deterministic parse
   failure. The refusal mechanism was as wrong as the predicate.
