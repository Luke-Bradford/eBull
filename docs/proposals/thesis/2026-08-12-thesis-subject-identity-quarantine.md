# Quarantine the stored wrong-company theses (#2436)

**Status:** proposal · **Refs:** #2431 (the write-side gate, merged `28b7bd03`), #2011, #2437.

## Problem

`#2431` added `_memo_names_subject` (`app/services/thesis.py:1645`) and wired it into
`_validate_writer_output` (`app/services/thesis.py:1840`), so a memo that never names its own
instrument is refused before insert. It does nothing about rows already stored, and the
deterministic layer reads those rows now.

Re-measured on the dev corpus at branch point (not inherited from the ticket — full population,
`_memo_names_subject` as shipped, latest-per-instrument by `created_at DESC` to match
`portfolio.py:459`):

```text
total stored theses                                    2,652
fail the subject-identity gate                         1,512
  ...of which are the LATEST for their instrument         178
     ...carrying stance = 'buy'                            47
     ...carrying >= 1 valuation band value                112
     ...currently held (positions.current_units > 0)        4
```

The harm is not prospective. Among those 178 instruments:

```text
trade_recommendations, action = EXIT,
  rationale LIKE 'Valuation target reached%'      14 rows across 2 instruments
```

Every one of those exits was decided by `portfolio.py:632-637` comparing a live price against a
`base_value` written in a memo about a different company.

## Source rule

There is no external regulator here; the governing rule is the repo's own, and it is already
settled:

- `docs/settled-decisions.md:147` — *"do not overwrite prior thesis rows"*. So the fix marks,
  it does not delete or repair. The rows are the evidence base for #2431's real fix.
- `docs/review-prevention-log.md:2820-2822` — a quarantined input relabelled as an ordinary
  empty result enters downstream statistics as an **observed miss** rather than **absent
  evidence**. So "skip" must carry a distinct reason, not reuse the no-thesis wording.
- `docs/review-prevention-log.md:2746` — *"a narrowing gate's denominator is the set it
  admitted"*. The census below reports both sides.
- `.claude/CLAUDE.md` narrowing-gate rule — enumerate what the change REJECTS.

The gate's own safety evidence is unchanged and re-runnable:
`scripts/verify_2431_subject_identity.py` (0 rejections across the 487 v1-v4 known-good memos).

## Design

### 1. Schema — store the verdict, do not re-derive at read time

`sql/332_thesis_subject_identity_verdict.sql`:

```sql
ALTER TABLE theses
    ADD COLUMN IF NOT EXISTS subject_identity_ok           boolean,
    ADD COLUMN IF NOT EXISTS subject_identity_rule_version text,
    ADD COLUMN IF NOT EXISTS subject_identity_checked_at   timestamptz;
```

Plus a CHECK that the triple moves together — all three NULL (never checked) or all three set.
The rule version is load-bearing: `_memo_names_subject` will change, and a row's verdict must
record *which rule* decided it, not the rule that happens to be current when it is read.

`NULL` means **not yet checked**, which is not the same as **passed**. Consumers therefore
quarantine on `subject_identity_ok IS NOT TRUE` — fail-closed, matching the repo's posture
("no silent bypass of failed checks").

### 2. Write-through at insert

`_insert_thesis_atomic` takes the `subject` dict and stores the verdict computed by the same
`_memo_names_subject`. It does **not** hard-code `true` on the strength of the validator having
passed: the validator returns `True` when the subject is not a dict ("nothing to check
against"), which is *unchecked*, not *passed*, and stamping `true` there would be a lie in the
one place a reader trusts most.

### 3. Backfill

`scripts/backfill_thesis_subject_identity.py` — re-verdicts **every** row (idempotent; a rule
change re-runs it and every verdict + rule version is rewritten), prints the census, writes
nothing else. Registered so a restored dump self-heals rather than sitting on NULLs.

### 4. Consumers

The rule is the ticket's: **skip, with a reason code. Never fall back to an older thesis** — a
months-old band presented as current is a second wrong number replacing the first.

| consumer | what it reads | treatment |
| --- | --- | --- |
| `app/services/portfolio.py:459` | latest thesis → stance, confidence, buy zone, `base_value`, break conditions → BUY/ADD/HOLD/EXIT | do not populate `details[iid]["thesis"]`; set `thesis_quarantined` so the reason strings at `:797` and `:829` say quarantined, not "no thesis" |
| `app/services/entry_timing.py:92` | latest thesis `base_value` → take-profit **order parameter** | band fields read as NULL → no TP; rationale records it |
| `app/services/scoring.py:1289,1636` | latest thesis → value + conviction families | treat as no thesis; existing `notes` mechanism carries the reason |
| `app/services/reporting.py:607` | as-of-entry thesis bear/base/bull → `target_hit` attribution | `target_hit = None`, bands null — an attribution against a fabricated band is a wrong label, and `None` is already its "cannot classify" state |
| `app/api/theses.py` | operator thesis surfaces | **expose** the verdict; do not hide the row (it is the evidence base) |
| `app/api/alerts.py:777` | 14-day thesis-change feed diffing bands | suppress quarantined rows from the feed and report the suppressed count — an alert saying "base 62.69 → 58.00" about the wrong company is a wrong notification |

Deliberately **not** changed, with reasons:

- `app/services/thesis_dq_audit.py` — it audits the rows themselves; excluding them would make
  the audit blind to the defect it should report.
- `app/services/thesis_outcomes.py` — a research statistic over what the writer produced.
  Changing its population changes a measurement, which is a separate decision with its own
  evidence bar.
- `app/services/thesis_diff.py` — renders version-to-version diffs of stored rows; the stored
  rows are a truthful record.
- `app/services/fair_value_band.py` — its `bear/base/bull` are the fundamentals-derived band on
  `instrument_valuation`, not `theses`.
- Frontend rendering of the quarantined state — needs FE-QA against a live stack, which the
  autonomy worktree cannot do (API :8000 / vite :5173 serve `~/Dev/eBull`). Follow-up ticket.
- Regenerating the 178 — that is an LLM spend decision and operator-gated.

### 5. Census (in the PR, per the narrowing-gate rule)

Both sides: rows admitted and rows rejected, per consumer, with the reproducing query.

## Acceptance

1. No consumer in the table above reads a band from a thesis whose stored verdict is not `TRUE`.
2. The count of skipped instruments is reported, not silent.
3. Re-running the backfill re-verdicts every row and is idempotent.
4. Nothing is deleted.
5. A revert-probe on each invariant test fails when the guard is removed.
