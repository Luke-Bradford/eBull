# Pre-PR fresh-agent review skill

**Purpose:** before pushing any non-trivial PR, run a fresh-agent Codex review with the perspectives a real fund / data team would apply. The review-bot keeps catching edges the generic pre-push prompt misses; this skill loads the right lenses up front so issues land before the bot finds them, not after.

## When to use

Mandatory before push for any PR touching:

- Filings ETL (Form 4 / Form 3 / 13D/G / 13F-HR / DEF 14A / N-PORT / N-CSR / 10-K / 10-Q / 8-K / SBR / S-1).
- Ownership rollup or any per-category observations / current table.
- Schema migrations on any ownership / fundamentals / share-count surface.
- Identity resolution (filer_cik, holder_name, CUSIP, ISIN).
- Anything that joins legacy + new tables.

Optional for:

- Pure UI / typing / docs PRs.
- Test-only changes.

## Roles to brief Codex on

Codex generic review is too neutral. Brief it with explicit role lenses so it applies the right scrutiny:

### 1. Financial-plumbing engineer

- **Concerns:** SEC filing semantics, identifier stability, accession-number contracts, period_of_report vs filed_at vs valid-time vs system-time, the specific shape of each form's XML / HTML / XBRL.
- **Edge cases to surface:** joint reporters on 13D/G; equity vs PUT vs CALL on 13F-HR; direct vs indirect holdings on Form 4; beneficial vs voting splits on DEF 14A; D vs I on insider_initial_holdings; restated financial periods; legacy `filer_cik IS NULL` rows on natural-person filers.

### 2. Data engineer

- **Concerns:** schema drift, partition floor / ceiling coverage, idempotency on natural keys, ON CONFLICT behaviour, transaction atomicity, advisory locks, refresh fan-out.
- **Edge cases to surface:** orphan FKs, NULL semantics in unique indexes (use partial UNIQUE + COALESCE sentinel), generated-column identity for nullable identifiers, refresh failures swallowed silently, partial-state regressions on retry, window cuts (`since=`) that exclude annual-cycle data.

### 3. Data scientist

- **Concerns:** dedup correctness, double-counting, cross-source priority chains, two-axis dedup (source × ownership_nature), denominator basis mixing, cardinality assumptions.
- **Edge cases to surface:** Cohen-on-GME case (direct + beneficial both render); Vanguard-13F vs Vanguard-13G case; treasury-on-top-of-outstanding accounting; restricted/RSU pre-issuance dilution; short interest as borrow artifact (NOT a separate ownership wedge).

### 4. Adversarial reviewer

- **Concerns:** what would the bot catch?
- **Edge cases to surface:** truthy checks on `Decimal("0")`; non-idempotent fallbacks to `date.today()`; aggregate counts hiding partial-state bugs; PK-but-not-DISTINCT-ON drift; cross-column invariants dropped on copy; comments contradicting code.

## Standard prompt template

```bash
codex.cmd exec --output-last-message /tmp/<branch>-codex.txt "Review <branch> in d:/Repos/eBull. Diff: <list files + one-line each>.

Acceptance: <issue criterion 1>; <criterion 2>; ...

Apply these review lenses:

1. Financial-plumbing engineer — SEC filing semantics for [forms touched]. Specifically: [form-specific edges, e.g. joint reporters on 13D/G; equity/PUT/CALL on 13F; direct/indirect on Form 4].
2. Data engineer — schema drift, partition coverage, idempotency under retry, refresh fan-out, NULL semantics, advisory-lock atomicity, generated-column identity for nullable identifiers.
3. Data scientist — dedup correctness, double-counting, denominator basis, cross-source priority chain integrity.
4. Adversarial — what edge case would the bot catch? Be specific about: truthy-check-drops-zero, non-idempotent fallbacks, aggregate counts hiding partial-state bugs, cross-column invariants from legacy tables, stale comments.

Reply terse. Real correctness bugs only. Skip style nits." < /dev/null
```

## Apply findings before push

Each Codex finding gets one of:

- **FIXED inline** — apply, re-test, commit.
- **DEFERRED #N** — file a tech-debt ticket if scope creeps; cite it in PR description.
- **REBUTTED** — explain why specifically; cite contradicting evidence (schema, spec, prior PR).

Push only after every finding reaches a terminal state. The bot will then have less to catch and the cycle closes faster.

## EdgarTools alignment

For ownership-related PRs, cross-reference what `dgunning/edgartools` (MIT, the canonical open-source SEC parser) extracts per form. If our schema captures fewer fields than EdgarTools exposes for the same form, surface the gap — operator wants every available field digested even when the rollup doesn't render it yet, so future expansions don't need re-wash.

Quick-reference of what EdgarTools provides per form (cross-check before extending observations tables):

- **Form 4:** every transaction row + footnote; D/I tag; transaction_type; is_derivative; security_title; conversion_or_exercise_price; expiration_date; underlying_security_title; underlying_shares.
- **Form 3:** initial holdings table (D/I tag, security_title, shares).
- **13D/G:** every reporter (CIK, name, type_of_reporting_person, citizenship, member_of_group, sole/shared voting + investment authority columns), aggregate_amount_owned, percent_of_class.
- **13F-HR:** every infotable row (cusip, name_of_issuer, title_of_class, value_usd, shares_or_principal, shares_or_principal_type, put_call, investment_discretion, voting_sole/shared/none).
- **DEF 14A:** beneficial-ownership table (holder_name, role, shares, percent_of_class, footnotes); ESOP plan totals; named-executive vesting tables.
- **N-PORT:** fund_series_id, advisor_id, custodian, shares per holding, fair_value, is_restricted.

If our observations table doesn't carry a field EdgarTools exposes for the same form, EITHER add it now (operator preference per "lets be sure we're maximising what we can get out of the data") OR file a tech-debt ticket to add it before the next form-class expansion.
