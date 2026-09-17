# #2441 — a Section 16 date that predates Section 16

`insider_transactions.txn_date_invalid` (#1687) bounds the execution date from ABOVE only.
20 rows carry a two-digit year parsed literally (`0023-06-23`), and nothing flags them.

## The premise, re-measured on the full population (dev DB, 2026-09-17)

| | |
| --- | ---: |
| `insider_transactions` rows | 1,064,351 |
| `txn_date < 1934-06-06` | **20** |
| …of those, `txn_date_invalid` already TRUE | **0** |
| rows already flagged by the existing upper bound | **86** |

Every one of the 20 is `document_type = '4'` with `transaction_timeliness IS NULL`, and every
one carries a year equal to the last two digits of **the year it evidently meant** — `0023` in
a filing accepted 2024-06-07 reporting June 2023 trades, `0024` in a 2024 filing, `0025` in a
2025 one. Distinct `txn_date` years below 2003 across the whole table: **23 (6 rows), 24 (11),
25 (3), and 2002 (1 row, legitimate)**. Nothing at all falls between `1900-01-01` and the
Exchange Act, so the floor separates the defect class from real data with no overlap.

### The defect is in the source, not in our parser — 20/20 rows, not 15/15 accessions

A per-accession regex proves too little (any `<value>` anywhere in any retained document could
satisfy it). The per-ROW form, asserting each flagged row's own date string:

```sql
select count(*) as rows_total,
       count(*) filter (where r.payload is not null) as rows_with_payload,
       count(*) filter (where r.payload ~ ('<value>[[:space:]]*'
              || to_char(it.txn_date,'YYYY-MM-DD') || '[[:space:]]*</value>')) as exact_date_in_payload
  from insider_transactions it
  left join filing_raw_documents r
    on r.accession_number = it.accession_number and r.payload is not null
 where it.txn_date < '1934-06-06';
-- 20 | 20 | 20
```

Confirmed live at the source for `0001434728-24-000220`, whose filed document on sec.gov
carries `<transactionDate><value>0023-06-23</value></transactionDate>` verbatim. **EDGAR
accepted the value**, so there is no upstream range validation to lean on and re-ingest cannot
fix it — the conclusion #1687 reached for the future-dated class (there on 75 retained payloads
of 80 violators; here on 20 of 20).

## Source rule

**15 U.S.C. § 78p (Securities Exchange Act § 16)** — *"June 6, 1934, ch. 404, title I, § 16,
48 Stat. 896"*. § 16(a)(1) attaches the reporting duty to a beneficial owner, director or
officer **of a class of equity security registered pursuant to section 12**; § 16(a)(2) fixes
when the resulting statements are filed. The reported item on Table I / Table II of a Form 4
is a transaction *in that registered class, by that reporting person, in that capacity*.

**Neither the class, the capacity, nor the duty can exist before the statute that creates all
three**, so a reported transaction date before **1934-06-06** is impossible on its face. The
Act's enactment date is used rather than any later § 12 registration effective date precisely
because it is the weakest defensible anchor: it cannot reject a date that a tighter anchor
would allow.

⚠ **The narrow claim is deliberate.** § 16 reports do describe events that predate the
reporting *status* — a Form 3 discloses holdings acquired before the person became an insider,
and a Table II derivative can have been granted years earlier (Exchange Act § 16 C&DIs, e.g.
101.01). This floor makes no claim about those: it bounds only the dates a Form 3/4/5 reports
**as § 16 events**, and only against the existence of § 16 itself.

This is the same invariant #1687 cites (17 CFR 240.16a-3(a)/(g)) read from the other side —
one bound from the statute that creates the duty, one from the rule that times it.

### ⛔ The ticket's preferred anchor is refused — it requires an invented threshold

#2441 prefers *"bound it against the filing's own accession year — a transaction cannot predate
its filing by more than a plausible reporting window"*. The objection is not that some such
window is unmeasurable; it is that **no source publishes one**, so the number would be chosen
to fit the symptom — the #2231 and #2279 mistake. What it would cost, measured on the full
population (1,029,258 rows joined to `sec_filing_manifest`; 35,093 unjoined):

| gap (`filed_at::date − txn_date`) | rows | accessions |
| --- | ---: | ---: |
| 0–2 days | 639,684 | 293,483 |
| 3–45 days | 368,857 | 173,948 |
| 46–410 days | 16,214 | 7,873 |
| 1.1–3 years | **2,630** | 995 |
| 3–10 years | **1,573** | 444 |
| 10–50 years | **182** | 33 |
| future (existing flagged class) | 98 | 85 |
| >50 years (this defect) | **20** | 15 |

A 3-year window rejects **1,755** rows to catch 20; a 10-year window rejects 182; a 50-year
window rejects 0 of the long tail but is plainly arbitrary. Long lags are expected rather than
anomalous: 17 CFR 240.16a-3(f) puts a Form 5 up to 45 days *after* the fiscal year it reports
(and a first Form 5 obligation can reach earlier years), and a delinquent § 16 report is a
recognised event with its own disclosure regime (Reg S-K Item 405). **None of those 1,755 rows
has been shown to be wrong, and none has been shown to be right** — which is exactly the
position a fitted threshold would silently resolve. The statutory floor needs no threshold.

⚠ Consequence, stated rather than hidden: a two-digit-year typo that lands in **1935 or later**
is not detectable by this rule (`1900`–`1934-06-05` is caught; `1993` is not). The observed
mechanism does not produce those — a literal `23` parses to `0023`, never `1923` — but if one
ever appears, this fix will not see it.

## Design

One constant, one predicate, in `app/services/insider_transactions.py`:

```python
SECTION_16_ENACTED = date(1934, 6, 6)

def predates_section_16(value: date | None) -> bool: ...
```

### 1. `evaluate_insider_date_validity` — the floor is evaluated FIRST and takes no exemption

Both existing exemptions belong to the **upper** bound and must not be inherited:

- `filed_at is None` — the floor needs no filing anchor at all, so a row with no resolvable
  filing date is still bounded. (It is not claimed that such rows are currently unguarded:
  `lookup_sec_filed_at` falls back to `filing_events` when the manifest has no row.)
- `transaction_timeliness == 'E'` — whatever `E` means, it cannot make a pre-1934 date correct.
  ⚠ The spec deliberately does **not** restate a gloss for `E`: #2790 found `sql/057`
  self-contradictory and EDGAR Ownership XML Tech Spec §4.3.8.2 supporting neither reading, and
  that call is #1687's open question. The floor is independent of its outcome.

`deemed_execution_date` is the same kind of date (a § 16 execution date) and is nullable, so it
takes the #1687 treatment for an impossible nullable value — quarantine to `NULL`, never
invent. The two fields are evaluated **independently**: a floor hit on `txn_date` must not skip
the deemed evaluation, and a bad deemed date must not flag an otherwise valid transaction.
Measured today: 0 rows (`min(deemed_execution_date) = 2015-04-15`).

### 2. The bulk DERA writer inherits the floor — form-agnostically, with its own counter

`sec_insider_dataset_ingest._is_future_dated_for_form` delegates to the same decision function
(#2790), so the floor lands on both writers and cannot drift apart. Two changes there:

- **The floor is applied BEFORE the `form_upper.startswith(("4","5"))` early return.** That
  gate is #2790's deliberate bound on the *upper* rule, whose rationale is form-specific
  (§ 16(a)(2) sets only latest bounds for a Form 3, so a Form 3 ahead of its filing is
  correct). No such asymmetry exists below 1934: **no form's § 16 date can precede § 16.**
  This reaches the 1 `:NDH:` row that the form gate would otherwise exempt.
- **Telemetry is split.** A floor rejection must not increment `rows_skipped_future_dated`,
  which surfaces to the operator as `future_dated` in `sec_bulk_orchestrator_jobs.py:620`.
  The helper returns a reason instead of a bool, and a new `rows_skipped_pre_section16`
  counter is logged and surfaced alongside it.

### 3. ⛔ Table II milestones are measured and left alone — no source rule authorises the change

The first draft of this spec quarantined pre-1934 `exercise_date` / `expiration_date` to
`NULL`. **Withdrawn.** Those are not § 16 event dates: `exerciseDate` is the date a derivative
becomes exercisable and `expirationDate` its term — *characteristics of the security*, per Form
4 General Instruction 4(c). The statutory argument above does not reach them, nullability is a
storage fact and not a licence, and no field-level reconciliation of the 36 values against
their raw payloads and footnotes has been done. Nulling them would be a treatment decided from
first principles — the thing the instruction set forbids.

They are therefore **measured and recorded** (the ticket's acceptance asks for the sibling
columns to be checked "and the result recorded either way"), and the row is **not** flagged
either: its `txn_date` is valid, and flagging it would drop a real transaction out of
downstream consumers.

### 4. Migration `sql/390` — flag, then tombstone what the #1687 contract would never have written

Mirrors `sql/205`'s one-off shape, with two deliberate differences: **no manifest join** (the
floor needs no filing anchor) and **no `E` exemption**. It preserves existing `TRUE` flags
(`AND txn_date_invalid = FALSE`).

The observation layer needs its own statement, because **`sync_insiders` never retracts**: it
filters its input on `NOT it.txn_date_invalid` and upserts the survivors, so flagging a
transaction leaves any observation already written from it in place, live, forever. The 38
live pre-1934 observation rows are tombstoned under the same statutory rule —
**`known_to = NOW()`, never a hard delete** (invariant **I6**, `ownership_observations.py:455`)
— and `refresh_insiders_current` is then run for the affected instruments, as I6 requires of
any caller.

All 38 are tombstoned, not just the 5 from the XML path. The rule is a property of the date,
not of the writer, and an asymmetry here would leave 33 equally-impossible rows live with no
stated reason. This is reversible by construction (`known_to` can be re-nulled), which is why
I6 exists.

## What the change REJECTS (narrowing gate — enumerate, do not summarise)

| surface | rows | treatment |
| --- | ---: | --- |
| `insider_transactions.txn_date` < 1934-06-06 | **20** (15 accessions) | flagged; raw value retained; excluded from the ownership sync + both readers |
| `insider_transactions.deemed_execution_date` | **0** | would quarantine to `NULL` |
| `ownership_insiders_observations.period_end` < 1934-06-06, live | **38** (18 instruments) — 5 XML-path, 32 DERA `:NDT:`, 1 DERA `:NDH:` | `known_to = NOW()` |
| DERA drain, next run | **32** `:NDT:` + **1** `:NDH:` | not re-written; counted as `pre_section16`, not `future_dated` |
| `insider_transactions.exercise_date` / `expiration_date` | 12 / 24 | **unchanged** — §3 |
| `insider_initial_holdings` (Form 3 XML path) | 0 | checked, clean: `min` of `exercise_date`, `expiration_date`, `as_of_date` all post-1934 |
| `insider_filings.period_of_report` | 12 | **unchanged** — different table, not named by the ticket |

No legitimate row is in the rejected sets: the corpus holds **zero** `txn_date` values between
`1900-01-01` and `1934-06-06`, and besides the 20 defective rows exactly **one** `txn_date`
below 2003 — a 2002 row, untouched.

⚠ Scope of "full population": dev's already-ingested corpus. It bounds what the migration
changes today, not what a future archive contains.

## Operator-visible effect

**0** of the 38 observations reach `ownership_insiders_current` today, so no ownership-card
figure moves. The value of the tombstone is forward-looking and is the reason to do it rather
than to rely on that zero: `refresh_insiders_current` picks a winner per `(instrument, holder,
nature)` by date, and a live impossible row is eligible to become that winner whenever the rows
that currently outrank it are superseded or retention-dropped. It is also visible in ownership
*history* regardless.

## Tests (shape, not count — a count goes stale on the next harvest)

Pure-logic, no DB:

1. a pre-1934 `txn_date` is flagged **with `filed_at=None`**;
2. …and **with `transaction_timeliness='E'`**;
3. `1934-06-06` itself is accepted (the boundary is valid);
4. `1934-06-05` is refused;
5. a 3-year-old `txn_date` with a normal `filed_at` is NOT flagged — the delinquent-filing
   case the refused anchor would have broken;
6. the existing upper bound still flags a future-dated row, and still exempts `E`;
7. a pre-1934 `deemed_execution_date` quarantines to `None` **while its row's valid `txn_date`
   stays unflagged**, and the converse (floor hit on `txn_date`, valid deemed date retained);
8. the DERA helper reports `pre_section16` for a pre-1934 date on **form `3`** (where the
   upper bound is exempt) and `future` for a future-dated form `4`, so the counters cannot be
   conflated.

## Acceptance — executed on dev, 2026-09-17, migration applied

A "0 remaining" query passes just as well after flagging everything, so each is paired
with a control that a broader change would fail.

| check | before | after |
| --- | ---: | ---: |
| `txn_date < '1934-06-06' AND NOT txn_date_invalid` | 20 | **0** |
| `txn_date_invalid` total | 86 | **106** (= 86 + 20, nothing else moved) |
| `txn_date_invalid AND txn_date >= '1934-06-06'` — the pre-existing upper-bound set, which this change must not touch | 86 | **86** |
| `insider_transactions` row count | 1,064,351 | 1,064,351 |
| `ownership_insiders_observations` pre-1934 live (`known_to IS NULL`) | 38 | **0** |
| …tombstoned (`known_to IS NOT NULL`) | 0 | **38** |
| `ownership_insiders_observations` LIVE rows, whole table | 5,581,239 | **5,581,201** (−38 exactly: nothing outside the set was tombstoned) |
| `ownership_insiders_current` rows | 171,539 | 171,539 |
| `ownership_insiders_current` business-column hash (`instrument_id`, `holder_identity_key`, `ownership_nature`, `shares`) | `7fe8b12d…` | **`7fe8b12d…` — identical** |

The `_current` hash is identical **after** `refresh_insiders_current_batch` was run over all
18 affected instruments (I6 requires the caller to refresh; not running it would leave the
projection unverified rather than unchanged). That is the measurement behind "0 of the 38
reach `_current`" — asserted, not assumed.

Live endpoint (`/instruments/{symbol}/ownership-rollup`), golden panel plus three of the 18
affected names — every insiders wedge renders and every one sits under `shares_outstanding`:

| AAPL | GME | MSFT | JPM | HD | GS | UUUU | MDXG |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 9,811,301 | 48,022,385 | 2,526,081 | 9,096,971 | 556,545 | 2,485,483 | 4,766,985 | 64,521,120 |

Cross-source (clause 9), SEC EDGAR direct: `0001434728-24-000220` is served by sec.gov
carrying `<transactionDate><value>0023-06-23</value></transactionDate>` — the flagged value is
the filer's own, so flagging (not repairing, not re-ingesting) is the correct treatment.

## Out of scope, measured and recorded

- `insider_filings.period_of_report` — **12** rows pre-Act, same two-digit shape, different
  table and not named by the ticket.
- Table II milestone dates — §3.
- Correcting any typo'd date to its intended value. The intended year is inferable
  (`0023` → `2023`) but inferring it is a data-treatment decision with no source rule behind
  it, and #1687's finding — the value is the filer's, carried verbatim — argues for flagging
  rather than rewriting.
