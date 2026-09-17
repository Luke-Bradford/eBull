# #2790 — the `E` exemption stands, keys on the wrong field, and the correction is 702 rows

Follow-up to `604e3c06` (PR #3145), which shipped the DERA ingest gate and left the stored
correction blocked on one question: **does #1687's `transaction_timeliness == 'E'` exemption
stand?** This spec answers it from the source, re-keys the exemption onto the field that actually
carries it, and corrects the stored rows.

## Source rule

1. **EDGAR Ownership XML Technical Specification v5.1 §3.6.8 (`TimelinessList`)** — the complete
   vocabulary: `E` = Early, `L` = Late, empty = On-time.
2. **Same spec §4.3.8.2, submission types "4" and "4/A"** — per-transaction rules:
   > *"The `<transactionFormType>` is mandatory and must be "4" or "5." … By definition, a "4"
   > transaction is on time. Provide no value for this case. By definition, a **"5" transaction is
   > early**. You do not have to provide a value of "E," but you can if you wish. A value other
   > than "E" will cause a SUSPENSE error."*

   So on a Form 4 submission a line is one of two things, and `<transactionTimeliness>` is an
   **optional marker of the second**: `transactionFormType = 5` is the fact, `E` is a hint.
   ⚠ The section is titled for submission types "4" and "4/A" — it says nothing about a line on a
   Form 5 submission, and this spec does not extend it there (§4.3.8.3 is a different rule set).
3. **DERA Form 345 dataset, `FORM_345_readme.htm` Appendix 6.1** — same vocabulary
   (`E` Early / `L` Late / Empty On-time) over `NONDERIV_TRANS.TRANS_FORM_TYPE` +
   `TRANS_TIMELINESS`, so the two pipelines speak one language.
4. **17 CFR 240.16a-3(g)** — a Form 4 is due before the end of the second business day *following*
   execution. **240.16a-3(f)** puts a Form 5 within 45 days *after the fiscal year end*, for
   transactions **during that fiscal year**.

### What each combination means for `period_end > filed_at`

| line's `transactionFormType` | submission type | bound | future date possible? |
| --- | --- | --- | --- |
| 4 | 4 / 4A | 16a-3(g): filing follows execution by ≤2 business days; §4.3.8.2 makes a "4" line on-time **by definition** | **no** |
| 5 | 4 / 4A | 16a-3(f): the line's deadline is 45 days after fiscal-year end, on a **different, later** filing. This Form 4 is a voluntary early disclosure and its own date does not bound the event | **yes** |
| 5 | 5 / 5A | 16a-3(f) again — but this *is* that filing, and it reports transactions **during the fiscal year already ended**. An event after this filing date was not in that year | **no** |
| 4 | 5 / 5A | a late-reported Form 4 transaction (§4.3.8.3); even further into the past | **no** |
| 3 | any | §16(a)(2) sets only *latest* bounds | **yes** — inherited from PR #3145, untouched here |

### The two filings that settle it

**Exempt side** — `0001127602-24-015987` (RANGE RESOURCES CORP / FUNK JAMES M), filed
**2024-05-20**, fetched from
`https://www.sec.gov/Archives/edgar/data/315852/000112760224015987/form4.xml`:

| line | `transactionFormType` | code | `transactionDate` | `transactionTimeliness` |
| --- | --- | --- | --- | --- |
| 0 | 4 | S | 2024-05-16 | *(empty — on time)* |
| 1 | **5** | J | **2024-06-03** | **E** |
| 2 | **5** | J | **2024-06-03** | **E** |

Footnote F2, verbatim:

> *"Transfer of these shares from an indirect holding to a direct holding is exempt from
> reporting, however the reporting person is voluntarily disclosing this information. The
> transaction effects a scheduled deferred compensation plan distribution with a distribution
> date of June 3, 2024."*

A §16-exempt, Form-5-eligible event with a **scheduled future distribution date**, volunteered 14
days ahead on a Form 4. The filing is well-formed and our storage of it is faithful.

**Correctable side** — `0001415889-23-002362` (ARCH CAPITAL GROUP LTD. / `ACGL`), `DOCUMENT_TYPE`
**5**, `PERIOD_OF_REPORT` 2022-12-31, filed **2023-02-13**. Its own `NONDERIV_TRANS` lines:

| `NONDERIV_TRANS_SK` | `TRANS_DATE` | form type | code | shares |
| --- | --- | --- | --- | ---: |
| 7627476 | **15-NOV-2022** | 5 | G | 32,080 |
| 7627477 | **15-NOV-2023** | 5 | G | 32,080 |
| 7627478 | **15-NOV-2023** | 5 | G | 54,219 |
| 7627479 | **15-NOV-2023** | 5 | G | 54,219 |

The same gift appears at both years inside one filing, on a Form 5 covering FY2022. Self-evident
year typo — and a form-type-5 line, which is why the exemption cannot key on form type alone.

### Verdict — and all three prior readings were wrong

- **#1687's exemption STANDS.** Removing it — the prior session's recommendation, and my own first
  reading (*"early still requires the event to have happened"*) — deletes correct filings.
- **`sql/057:264` is wrong in wording only**: `E` does not mean "filed early (before the event)".
  `:338` — "early (filed before the deadline)" — is right, and the deadline is **16a-3(f)**'s,
  which is why a future date survives it.
- **The exemption keys on the wrong field and is too NARROW.** `E` is optional on a form-type-5
  line: **22 of the 76** exempt rows carry a blank timeliness (54 carry `E`), so the gate shipped
  in `604e3c06` rejects those 22. That is a live regression, shipped today, and this spec is also
  its fix.
- **But form type alone is too WIDE** (Codex ckpt-1 #6): it would exempt 50 rows on Form 5
  submissions, including the ACGL typo above. The exemption needs both fields.

## Full-population measurement

Every breaching observation row resolved against SEC's own structured extract of the same filing —
all **81** cached `insider_<YYYY>q<N>.zip` archives (2006q1–2026q1), joined on
`(ACCESSION_NUMBER, NONDERIV_TRANS_SK)` from `source_document_id`'s `:NDT:` marker, plus
`SUBMISSION.DOCUMENT_TYPE` per accession.

Scope predicate is PR #3145's, unchanged (`scripts/audit_2790_insider_future_period.py`): **778**
Form 4/5-attributable live rows; 266 Form 3 / holdings rows are out of scope by that predicate and
are not re-adjudicated here.

`PYTHONPATH=. uv run python scripts/correct_2790_insider_future_period.py` (census mode, the
default, read-only) prints this and the SQL/archive evidence behind it:

| submission | line form type | timeliness | rows | treatment |
| --- | --- | --- | ---: | --- |
| 4 | 4 | *(blank)* | 637 | **correct** |
| 5 | 5 | *(blank)* | 47 | **correct** — 16a-3(f), the ACGL shape |
| 4/A | 4 | *(blank)* | 10 | **correct** |
| 5 | 4 | L | 4 | **correct** |
| 5 | 5 | L | 2 | **correct** |
| 5/A | 5 | *(blank)* | 1 | **correct** |
| 5 | 4 | *(blank)* | 1 | **correct** |
| 4 | 5 | E | 54 | exempt |
| 4 | 5 | *(blank)* | 20 | exempt |
| 4/A | 5 | *(blank)* | 2 | exempt |

**Correctable: 702. Exempt: 76. Unresolved: 0.** 702 + 76 = 778. ✅

The 12 rows with no `:NDT:` key (the XML write path stores the bare accession) resolve through the
secondary resolver — archive lines on that accession at the stored `period_end`, requiring
unanimity on the form type — and land in the `4 / 5 / E` exempt row above. Accession-level
agreement is **not** assumed: the RRC filings are mixed (one form-type-4 line and two form-type-5
lines), which is exactly why a non-unanimous date match stays unresolved instead of guessing.

The inherited framing ("24 `E` rows, 732 unresolvable") was an artefact of *storage*, not of the
source: `transactionFormType` is stored **nowhere** in this repo
(`rg -n 'transactionFormType|trans_form_type|TRANS_FORM_TYPE' app/ sql/ scripts/` → 0 hits), so the
field that decides the question was dropped by both parsers. The archives have always had it.

**Archive-resolution integrity, measured not assumed** (Codex ckpt-1 #26–#29): across all 81
archives, **zero** conflicting `SUBMISSION.DOCUMENT_TYPE` for one accession and **zero**
conflicting `TRANS_FORM_TYPE` for one `(accession, SK)`. The correction asserts both at run time
rather than trusting this measurement.

**`source_document_id` is `NOT NULL`** (information_schema) with **0** NULLs in the breaching
population, so the scope predicate's `NOT LIKE` cannot go three-valued (Codex #35).

## Design

### 1. Gate — exempt on (submission type × line form type), not on the letter

`insider_transactions.evaluate_insider_date_validity` gains two keyword parameters,
`submission_form_type` and `txn_form_type`. The **upper bound only** is skipped when:

```
submission_ok  = submission_form_type is unknown, or starts with "4"
line_is_form5  = txn_form_type == "5"
                 or (txn_form_type is unknown and transaction_timeliness == "E")
exempt         = submission_ok and line_is_form5
```

- `E` survives **only as a fallback for an unknown line form type**, because EDGAR permits `E`
  solely on a form-type-5 line ("a value other than 'E' will cause a SUSPENSE error"). An explicit
  `4` + `E` is malformed and is **not** exempted — the old OR-form would have exempted a row the
  correction deletes, which is the self-contradiction the pre-push check "does my correction
  remove anything my gate keeps?" exists to catch (Codex #7). Measured: that combination does not
  occur in the breaching population.
- **Unknown submission type stays fail-open**, matching the gate's existing documented posture for
  a blank `DOCUMENT_TYPE` and preserving every current caller's behaviour.
- The **#2441 statutory floor is not inherited by either parameter**, per its own docstring: no
  form type makes a pre-1934 date possible.

Both writers hold the values at decision time and neither has to store them:

- XML — `_parse_transaction` reads `./transactionCoding/transactionFormType` (mandatory per
  §4.3.8.2) into `ParsedTransaction.txn_form_type`; the upsert call site already holds the
  `ParsedFiling.document_type` for the submission side. **Parsing it is inert unless the call site
  passes it** (Codex #21) — both halves land in this diff, and a test asserts the wiring.
- DERA — `_reject_reason_for_form` takes `trans_form_type` from `NONDERIV_TRANS.TRANS_FORM_TYPE`
  alongside the `TRANS_TIMELINESS` it already reads; `form_upper` is already the submission type.

⛔ **Not stored, deliberately.** A new `insider_transactions` column is a parse-shape change, which
`sql/057`'s own rule says bumps `parser_version` — a full Form 4 rewash (#2377 shape) paid for an
auditability nicety the gate does not need. The correction resolves from the archives instead,
which is where the field has always been. ⚠ Consequence accepted and stated: a future correction
of this class is again an archive join, not a SQL query.

⚠ **Holdings rows are untouched.** `NONDERIV_HOLDING` has no transaction form type; the DERA
holdings call site keeps passing `None` and stays governed by the submission-type gate alone
(Codex #18). It is also outside the correction's scope predicate.

⚠ **Rows already rejected by the too-narrow gate are not replayed** (Codex #22). `604e3c06`
shipped this morning; any form-type-5 line it dropped returns on the next ingest of the archive
that carries it. Recorded rather than fixed here — a replay is a re-ingest, not a correction.

### 2. Correction — 702 rows, soft-deleted, `_current` re-derived in the same transaction

`scripts/correct_2790_insider_future_period.py`, resolving every candidate against the archives at
run time rather than against a list baked into the file:

- **Soft-delete only** (`known_to = now()`), per I6 — never hard-delete. The #2441 precedent (38
  live → 0 live / 38 tombstoned) is the same treatment in the same layer.
- `refresh_insiders_current_batch` over the affected instruments runs **inside the same
  transaction**: the repair sweep cannot rescue a half-done correction, because its drift
  predicate compares `MAX(ingested_at)` and setting `known_to` does not move it.
- **A row that does not resolve is KEPT** — and the run prints resolved / exempt / unresolved and
  asserts they sum to the scope count, so an empty or partial archive cache cannot pass vacuously
  as "0 breaches remain" (Codex #24).
- **Conflict assertion**: if one accession yields two `DOCUMENT_TYPE`s, or one `(accession, SK)`
  two `TRANS_FORM_TYPE`s, the run aborts rather than letting iteration order decide (Codex #27).
- `--apply` writes a per-row ledger (`accession`, SK, resolved form types, archive filename) to a
  JSONL file before committing, so the change is reversible row-by-row (Codex #40).

⚠ **Do not predict the `_current` delta.** Removing a winner promotes an older observation rather
than deleting the key — when the reverted first attempt ran, 112 breaching `_current` rows cleared
and the table fell by **1**.

⚠ **Do not infer the intended year.** ACGL's own filing shows what was meant; a 2047 date does
not. Repair is a different ticket.

### 3. The residual this does NOT fix, stated rather than fitted

Of the ticket's 20 future-dated `ownership_insiders_current` rows, **19 resolve to form-type-4
lines on Form 4 submissions** and are corrected. The twentieth — instrument 8884,
`0001209191-21-009556`, `period_end` 2030-12-21, a form-type-5 line on a Form 4 filed in 2021 — is
exempt under the source rule and survives. Nine years is not plausibly a scheduled distribution,
but **no published rule bounds how far ahead a Form-5-eligible event may be volunteered**, and
#2441 refused exactly this shape of fix (a "plausible reporting window" that rejected 1,755 rows
to catch 20). A fitted constant is not available here either, so the row stays and is named.

⚠ Related and also not fixed: an exempt future-dated observation can **win `_current` before its
event occurs** (Codex #14). That is a projection-semantics question — "a balance dated in the
future is not a current balance" — which applies to every source, not just this one, and belongs
in its own ticket rather than inside a correction.

### 4. `sql/057`'s contradiction — fixed in a NEW migration

`:264` and `:338` disagree and `:264` is the one #1687 cited. An applied migration cannot be edited
(`migrations.py:188` raises `Migration content drift` on a sha256 move), so a new migration
re-states `COMMENT ON COLUMN insider_transactions.transaction_timeliness` with §3.6.8's vocabulary
and §4.3.8.2's form-type reading.

## Result (applied to dev, run `643dbd33-abe7-4dd6-8730-10aac4206c9c`)

702 soft-deleted, 304 instruments refreshed, one transaction, postcondition asserted before commit.

| | before | after |
| --- | ---: | ---: |
| future-dated `ownership_insiders_current` rows (the ticket's headline) | **20** | **1** |
| `_current` rows breaching `period_end > filed_at` | 90 | 4 — all form-type-5-on-Form-4, none correctable |
| `ownership_insiders_current` as-of anchor for `RMCF` | 2027-07-27 | **2026-09-01** |

The surviving 1 is the named residual (instrument 8884). The other 3 of the 4 are exempt lines
whose dates have since passed, so they no longer read as "future".

**Full-population A/B through the real read path**, control re-measured on this branch immediately
before `--apply` (`scripts/audit_2230_insider_oversubscription`, 4 shards): 12,825 instruments,
**0 harness errors both arms**, 4,430 with a usable denominator.

| | control | treatment |
| --- | ---: | ---: |
| insiders wedge > `shares_outstanding` | 388 | **389** |
| severity 1.0-1.5× / 1.5-5× / 5-100× / ≥100× | 126 / 169 / 89 / 4 | 127 / 169 / 89 / 4 |

11 instruments moved: 9 grew, 2 shrank, **1 newly over-subscribed and 0 cleared**.

⚠ **The one newly-over instrument is `RMCF`, and it is the correction working.** 0.867 → 1.075.
Seven `_current` rows — six co-filer CIKs of one joint Form 4 group — all carried the same
709,835-share balance at a typo'd `2027-07-27`, and that bogus balance was the winner for each
identity. Removing them promotes the group's real latest balance, **1,971,306 at 2026-01-16**
(`0001193125-26-019954`), which is 2.8× larger. The wedge grows because six identities now each
report the true joint block — which is **#2230's attribution-overlap axis**, a mechanism this
change does not touch and did not create. It replaced a wrong number with a right one; the right
one happens to sit above the threshold.

⚠ The two arms ran ~40 minutes apart with ordinary ingest running between them, so the non-insider
correction counters moved too (`suppressed_by_13f_nt` 777 → 778, `superseded_by_later_13f_hr`
15,709 → 15,722). Those are churn, not treatment; the per-instrument diff above is the controlled
comparison. `insider_section16_exit_declared` 338 → 345 is **not** churn — removing a holder's
future-dated tip can let #2788's exit-box release fire, which is the expected interaction.

**Operator-visible figures on the live endpoint** after the correction
(`GET /instruments/{symbol}/ownership-rollup`, dev API, HTTP 200 for all six):

| symbol | insiders slice | pct outstanding | filers |
| --- | ---: | ---: | ---: |
| AAPL | 9,811,301 | 0.00067 | 20 |
| GME | 48,022,385 | 0.09518 | 7 |
| MSFT | 2,511,214.65 | 0.00033 | 20 |
| JPM | 9,096,971.24 | 0.00342 | 28 |
| HD | 556,544.84 | 0.00055 | 19 |
| RMCF | 10,147,459 | **1.07498** | 23 |

`RMCF` renders exactly the A/B treatment figure, so the harness and the endpoint agree.

## Acceptance

1. A form-type-**4** line dated after its filing date is rejected (DERA) / flagged (XML), on any
   submission type.
2. A form-type-**5** line on a **Form 4** submission dated after its filing date is **kept**, with
   blank timeliness as well as `E`. This is the assertion that catches the over-narrow gate.
3. A form-type-**5** line on a **Form 5** submission dated after its filing date is **rejected**
   (the ACGL shape). This is the assertion that catches the over-wide gate.
4. A Form 3 row is untouched — PR #3145's assertion, retained.
5. The #2441 floor still rejects a pre-1934 date on a form-type-5 line marked `E`.
6. After `--apply`: 0 live breaching rows resolving to the correctable classes; the 76 exempt rows
   still live; `ownership_insiders_current` future-dated rows 20 → 1 (the named residual).
7. Full-population A/B over the real read path, control re-measured at the branch point rather
   than inherited. **Pass criteria, stated in advance** (Codex #44): every instrument whose
   insiders wedge GROWS is enumerated and explained individually — a soft-delete can promote an
   older, larger balance — and no instrument becomes newly over-subscribed without a named cause.
   `as_of_max` movement is reported per instrument, because `ownership_history.py:189` filters
   closed rows and the anchor moves retroactively.

## Tests

Pure-logic table test over `evaluate_insider_date_validity` and `_reject_reason_for_form`:
submission `4` / `4/A` / `5` / `3` / blank × line form type `4` / `5` / absent × timeliness blank /
`E` / `L` × lead ≤ 0 and > 0 × `filed_at` present and absent, plus a pre-1934 form-type-5 `E` row
(floor wins) and an explicit `4` + `E` row (malformed — not exempt). The two discriminating cases
did not exist before: **form type 5, blank timeliness, future date, Form 4 submission** (kept —
the live gate gets this wrong today) and **form type 5 on a Form 5 submission** (rejected). Plus a
wiring test that the XML upsert call site actually passes both fields through.
