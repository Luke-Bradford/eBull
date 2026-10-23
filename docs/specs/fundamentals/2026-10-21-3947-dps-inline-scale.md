# #3947: dividend-per-share facts tagged with an inline `scale`

**Ticket:** #3947 (refs #3943, #3903, #3915). **Status:** spec v4, plus Amendment 1 (durable rows beyond the swept
raw layer, and one regime per difference-derived cell), which overrides the sections it names. Codex checkpoint 1
on v4 ran three rounds, the cap:
- round 1 raised 45 findings, applied in v2;
- round 2 accepted the declared-policy framing and raised 31 implementation findings, applied in v3;
- round 3 kept the framing accepted and raised 31 findings, applied in v4 without a fourth round, as #3910, #3915,
  #3903 and #3919 did ("Checkpoint record").

The dividends pane's TTM and latest DPS sum `financial_periods.dps_declared`, and some stored members are a
thousand or a million times the amount the filing displays. EFC shows a TTM DPS of 1,170.39 against a latest DPS
of 0.39. The research on #3947 (2026-10-21) found that SEC companyfacts equals our raw store for these facts: the
filer's inline XBRL produced the value, not our parser. This spec declares a policy for the class a published
data-quality rule flags, and states that a second class (FMBH) is left as filed.

## Problem

Figures marked [n] come from `PYTHONPATH=. uv run python -m scripts.measure_3947_dps_inline_scale` (dev DB and
EDGAR, 2026-10-21; the output is pasted on #3947). The dividends-paid, SEC-frames and cross-filing measurements are
from the #3947 research comments, which carry their own commands; this script does not reproduce them.

- **The scale class.** In EFC's `efc-20250930.htm` the displayed `0.39` carries `scale="3"`, so the instance value
  is 390. Companyfacts serves 390 and `financial_facts_raw` stores 390.
- **[1] Population:** 11,873 accessions filed on or after 2019-06-15 hold a `USD/shares`
  `CommonStockDividendsPerShareDeclared` fact. The same query without the unit restriction also returned 11,873
  (an earlier run of the script, 2026-10-21). 7,979 have an `.htm` primary document in `sec_filing_manifest`; the
  rest are `.txt` full submissions or have no URL.
- **[2] Pilot, primary documents only.** The accessions are selected by a `USD/shares` fact; the counts are
  regex-detected inline occurrences of the concept in the primary document, with unit, context and value
  unresolved. They are not counts of correctable stored facts, and a miss means no regex match.
  - All 68 `.htm` accessions holding a fact ≥ 10: 12 have an occurrence with `scale` > 0 (45 occurrences at
    scale 3, 1 at scale 6). They are EFC (5 accessions), SCI, HTB and TPVG (2 each) and WHR (1: `1.75` at
    scale 6, stored as 1,750,000). 2 of the 68 have no regex-detected occurrence in the primary document; one, WSO
    `0001193125-25-041730`, carries its statements in `d898605dex13.htm`.
  - A seeded random 150 of the 7,911 others: no regex-detected occurrence with a positive scale (726 detected).
  - This is a pilot. Nothing here rests on "only large values are scaled": the scan reads every accession in scope.
- **[3] The cents class.** FMBH's FY2025 10-K tags `98` with `decimals="INF"` and no `scale` (#3947 research).
- **Readers of the stored member** (a grep of `app/` and `sql/`, 2026-10-21):
  - `instrument_dividend_summary` and `dividend_history` (sql/050), which feed the dividends pane through
    `app/services/dividends.py`. `ttm_dps` sums the present members of the last four quarters (sql/050:96);
  - `financial_periods_ttm.dps_declared_ttm` (sql/508) and `instrument_valuation.dps_declared_ttm`, read by
    `ranking_pot_rebalance.py:706-709` for the pot, which is halted (#2842) but not removed;
  - `ranking_ablation_reader`'s diagnostic.

  The dividend yield stopped reading these members under #3903. No order or allocation path reads them today.

## Source rule

| decision | rule | source |
|---|---|---|
| which occurrences are flagged | an inline `us-gaap:CommonStockDividendsPerShareDeclared` fact whose `scale` attribute is an integer greater than 0 | XBRL US DQC.US.0179.10141 (rule "Dividend Event Disclosures", released 2024-06-27, effective 2024-08-31, https://xbrl.us/data-rule/dqc_0179/): the component "checks for an inline scale greater than 0" on this concept. Message: "Generally the amount per share are not scaled when reported." Example: "a value of 230 with an inline scale of 3" |
| how the instance value is formed | the transformed display, multiplied by 10^scale, negated when `sign="-"` | Inline XBRL 1.1 Part 1 §10.2.3; {scale} is ten raised to the attribute (§10.1) |
| which inline fact is the stored fact | same accession, entity, unit, period and instance value, on a context with no dimension | companyfacts serves only the non-dimensional member of a concept (`.claude/skills/data-sources/sec-edgar.md` §7.17) |
| which documents to read | every document of the accession's Inline XBRL Document Set | Inline XBRL 1.1 Part 1 (a document set's contexts and units may sit in any of its documents); WSO above shows the primary document alone is not enough |

**What the rule establishes, and what it does not.** DQC_0179.10141 is a warning ("generally"), not a proof that
a given occurrence is wrong. Dividing by 10^scale recovers the *unscaled transformed numeric value*: the number
the statement displays, after the format transformation and sign. That is the amount the filed statement shows a
reader. It is not independent evidence of the economic amount.

### Declared policy

1. **Restore, as a declared policy.** A stored fact whose matching occurrences all carry the same scale s > 0 is
   read by derivation as its value ÷ 10^s. The risk is stated: if a filer genuinely displays a per-share amount
   in thousands of dollars, the restored value understates it by 10^s. The acceptance below lists every
   restoration with its displayed text and the issuer's other evidence for the same period, so that risk is
   visible case by case. That evidence is reported beside the decision; it is not a guarantee.
   - Why not refuse: `ttm_dps` sums the *present* members (sql/050:96). When no other fact fills the period, a
     refused member can turn an overstated TTM into an understated one, and a refused FY member with no surviving
     alternative removes the derived Q4. In the measured snapshot, EFC's Q3 2025 has no other accession
     reporting it.
2. **Positive scale only.** A negative scale (a display in cents, `scale="-2"`) yields a correct instance value
   when the tagging is right. Negative-scale defects, if any, are outside this detector.
3. **Mixed repairs refuse.** One fact may be displayed several times. When the occurrences matching a stored
   fact carry different scales (`0.39` at scale 3 and `390` at scale 0 encode the same instance value, but imply
   different repairs), the repair is ambiguous and the fact is refused, as #3344 refuses.
4. **Doubt keeps.** A fact is read as stored today when its accession has no active scan, is not inline, holds an
   unresolved occurrence of the concept or an occurrence of another entity, or has no occurrence matching it
   (decision table rows 1–7).
5. **One concept.** The rule names `CommonStockDividendsPerShareDeclared` only. The other `dps_declared` aliases
   (`sec_fundamentals.py:281-287`, LP/LLC distributions per unit) are not scanned.
6. **Coverage cutoff.** 2019-06-15 is a deliberate cutoff at the first inline XBRL phase-in for large accelerated
   filers (sec-edgar skill, mandate table). Earlier facts are kept and counted as untested, not assumed non-inline.

## Construction

The correction is read only when `EBULL_DPS_INLINE_SCALE_ENABLED` is true (default false, `app/config.py`). The
scan can run and fill its tables with the flag off; the A/B evaluates decisions off and on without the flag; and
rollback starts by turning it off. The #2327 CUSIP bridge ships the same way.

### Document set

An accession's document set is every `.htm`, `.html` or `.xhtml` file in its `index.json` listing that contains
an `ix:header`. All of them are read as one Inline XBRL Document Set: contexts and units resolve across the set.
`FilingSummary.xml` is not used to find inline documents, because nothing guarantees that its `<InputFiles>` list is
complete. An accession whose listing holds no such file is `not_inline`.

*Build note (PR a).* The listing also holds the EDGAR Renderer's own pages (`R1.htm` …; WHR's
`0000106640-24-000125` lists 74). The scan skips exactly the files `FilingSummary.xml` names as
`<Report><HtmlFileName>`: SEC wrote those pages from the instance, so none can be a filer document. A name pattern was
rejected because filer document names can be upper-case (`SELECT count(*) FROM filing_documents WHERE document_name
~ '[A-Z]' AND document_name !~ '^R[0-9]+\.htm$'` was 1,090,470 on 2026-10-21). A missing or unparseable summary
excludes nothing, which costs requests but never coverage.

### Parser contract

- Elements and the concept QName are resolved by namespace, not by prefix spelling.
- **Defaults.** An absent `scale` is 0 and an absent `sign` is positive. §10.2.3 applies each "if present".
- **Scale lexical.** The attribute is `xs:integer`: after whitespace collapse it must match `[-+]?[0-9]+`. A
  separate implementation bound, −30..30, rejects other values with reason `scale_out_of_bound`.
- **Canonical key.**
  - Entity: the context identifier under scheme `http://www.sec.gov/CIK`, as a 10-digit zero-padded CIK.
  - Unit: units are compared as expanded QNames. Only a `divide` of `{http://www.xbrl.org/2003/iso4217}USD` over
    `{http://www.xbrl.org/2003/instance}shares` maps to companyfacts' `USD/shares`. Any other unit is
    `unsupported_unit`: it is excluded from matching, and it is not an unresolved occurrence.
  - Period: a duration's `startDate` and `endDate` as written; an instant has no start.
- **Two failure classes:**
  - **The attempt fails, and the active generation stays in force:** a fetch error or non-2xx response, a document
    that does not parse, a context or unit id defined twice in the set, or any element with a `target` attribute
    (a second target document is not handled).
  - **Unresolved, published on the occurrence:** a nil fact, a `contextRef` or `unitRef` that does not resolve, an
    invalid `sign`, a scale outside its lexical space or bound, or a format outside the Inline XBRL
    Transformation Registry (never guessed).
- **Reuse first.** edgartools 5.30.2 has an inline extractor (`edgar/documents/strategies/xbrl_extraction.py`,
  `XBRLExtractor`) that reads `scale`, `sign` and `format`; its transformation table holds five formats. The build
  adopts it only if it passes every parser fixture (acceptance 3). Otherwise the hand-rolled lxml reader cites that
  comparison. EFC, WSO and WHR are smoke tests on real documents, not the adoption test.

### Schema (one migration)

```sql
CREATE TABLE sec_inline_scale_scan (
    accession_number TEXT PRIMARY KEY,
    -- the active generation: NULL until a scan succeeds; a failed attempt never changes these
    outcome          TEXT CHECK (outcome IN ('inline', 'not_inline')),
    documents        INTEGER,
    parser_version   TEXT,
    scanned_at       TIMESTAMPTZ,
    -- work at attempt_version: reset once per version, when the first attempt at a new version starts
    attempt_version  TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ,
    last_error       TEXT
);

CREATE TABLE sec_inline_dps_occurrence (
    accession_number TEXT NOT NULL REFERENCES sec_inline_scale_scan ON DELETE CASCADE,
    document         TEXT NOT NULL,
    element_ordinal  INTEGER NOT NULL,   -- document order among the concept's elements
    fact_id          TEXT,
    context_ref_raw  TEXT,
    unit_ref_raw     TEXT,
    scale_raw        TEXT,
    sign_raw         TEXT,
    format_raw       TEXT,
    decimals_raw     TEXT,
    display_text     TEXT NOT NULL,
    entity_cik       TEXT,               -- the resolved fields below are NULL when unresolved
    period_start     DATE,
    period_end       DATE,
    dimensional      BOOLEAN,
    unit             TEXT,               -- 'USD/shares', or 'unsupported_unit'
    scale            INTEGER,
    instance_value   NUMERIC,
    unresolved       TEXT,               -- NULL when every field above resolved; otherwise the reason
    PRIMARY KEY (accession_number, document, element_ordinal)
);

CREATE SEQUENCE sec_inline_scale_renormalise_revision;
CREATE TABLE sec_inline_scale_renormalise (
    instrument_id    BIGINT PRIMARY KEY REFERENCES instruments,
    revision         BIGINT NOT NULL,    -- nextval of the sequence at each enqueue
    failures         INTEGER NOT NULL DEFAULT 0
);
```

Every occurrence is stored, scale 0 included, so each decision can be traced to its elements.

### The scan (`sec_dps_inline_scale_scan`, new job)

- **One scanner at a time.** The job runs under its `JobLock`, so two scans never overlap; a success publishes
  only if `attempt_version` still equals the version it started with.
- **Population:** accessions holding a `CommonStockDividendsPerShareDeclared` fact (any unit), filed on or after
  2019-06-15. An accession is **eligible** when `parser_version IS DISTINCT FROM current`, and either
  `attempt_version IS DISTINCT FROM current` or `attempts < 5 AND next_attempt_at <= now()`.
- **Attempt.** On the first attempt at a version, set `attempt_version = current` and `attempts = 0`. Each failure
  increments `attempts`, sets `next_attempt_at = now() + 2^attempts hours` and records `last_error`.
  - At 5 attempts the accession is **exhausted** at that version and is reported.
  - Re-admitting it means `attempts = 0, next_attempt_at = now()`.
- **Success** replaces the accession's occurrences and the active-generation columns in one transaction, and
  clears `attempts`, `next_attempt_at` and `last_error`.
- **Version bump.** Old generations stay active until their accession is rescanned at the new version.
- **Queue.** In the transaction of a success, the scan enqueues the accession's instruments with
  `revision = nextval(…)` (an upsert that always takes the new revision) when the old or new generation holds a
  positive-scale or an unresolved occurrence. A later scan never clears a queued row.
- **Draining.** The job takes queued rows in `revision` order and starts no new instrument after half its
  wall-clock cap. It normalises the instrument, then runs `DELETE … WHERE instrument_id = %s AND revision = %s`
  with the revision it read, so work queued meanwhile stays queued. A failure increments `failures`, and the row
  stays for the next run. Draining is skipped while the flag is off.
- **Normaliser writers are serialised.** `normalize_financial_periods` takes
  `pg_advisory_xact_lock(<namespace>, instrument_id)` inside its per-instrument transaction. Its callers (this
  job, `daily_financial_facts` at `app/workers/scheduler.py:5282`, `force_refresh.py:164`, bootstrap) can then
  never interleave writes for one instrument.
- **Order and budget:** draining as above, then filings from the last 7 days, then the backlog oldest first. All
  requests go through the SEC provider's shared 10 req/s limiter via `fetch_document_text` (its caller pin,
  `tests/test_fetch_document_text_callers.py`, gains this caller; the SQL normalisation path is these tables).
  - An illustrative size: 11,873 `index.json` requests plus each listed HTML document. At the pilot's mean
    primary document of 3.74–5.25 MB [2], the primary documents alone are roughly 45 GB, parsed and discarded.
  - Each run reports accessions by work state (done, pending, retrying, exhausted), documents fetched, and the
    queue length.

*Build note (PR b).* Draining admits no instrument at or after half of the scan's 45-minute start cap, measured from the
run's start, so the scan keeps the other half. The uncapped enablement run (acceptance 5.2) is
`drain_renormalise_queue(conn, admit_until=None)` in `app/services/dps_inline_scale_scan.py`, with the flag on and on a
connection with no open transaction; it refuses otherwise. A failed normalisation is read from
`NormalizationSummary.instruments_failed`, which the normaliser now counts, because it logs a per-instrument failure and
does not re-raise it. Rollback step 4 (`--verify-stored off`) compares against a fresh normalisation, so it is
meaningful only after step 3: stored rows drift as ingestion and code move, while two normalisations in one snapshot
agree exactly.

### The correction (normaliser)

Any normalisation applies the decisions, whatever triggered it. A raw-data import or an identifier change
therefore picks up existing occurrences through the existing normalisation paths; the queue only covers changes
to occurrences.

With the flag on, `normalize_financial_periods` step 1 (`app/services/fundamentals/__init__.py:2369-2397`) also
reads:
- the instrument's SEC CIKs (`external_identifiers`, provider `sec`, type `cik`, all rows, 10-digit zero-padded);
- the active scan row of each of its accessions;
- those accessions' occurrences.

One pure function, `_dps_fact_inline_scale_decisions(facts, scans, ciks, occurrences)`, returns a `Decision` for
every `CommonStockDividendsPerShareDeclared` fact: `action`, `reason`, `scale`, `value` and the matched
occurrence keys. The first matching row wins:

| # | condition | action | reason |
|---|---|---|---|
| 1 | filed before 2019-06-15 | keep | `out_of_scope` |
| 2 | the fact's unit is not `USD/shares` | keep | `unsupported_unit` |
| 3 | the accession has no active generation | keep | `untested` |
| 4 | the active generation is `not_inline` | keep | `not_inline` |
| 5 | an occurrence's entity CIK is not one of the instrument's SEC CIKs | keep | `entity_mismatch` |
| 6 | the accession holds an unresolved occurrence of the concept | keep | `unresolved` |
| 7 | no non-dimensional `USD/shares` occurrence has the fact's period and `instance_value = val` | keep | `no_match` |
| 8 | the matching occurrences carry more than one scale, one of them > 0 | refuse | `mixed_scale` |
| 9 | they all carry one scale s > 0 | restore, `value = val ÷ 10^s` | `scaled` |
| 10 | otherwise | keep | `unscaled` |

Rules 5 and 6 are accession-wide on purpose: an unresolved occurrence has no reliable key, so it may be any fact.

**Where decisions act: on DPS value selection only.**
- The existing structural filters (`_per_share_value_is_representable`, `_fact_period_ended_by_filing`) and
  period grouping and anchoring run on the raw values, exactly as today.
- After them, a restored fact's value is replaced, and a refused fact is skipped, only when a value is chosen for
  `dps_declared` and in that column's YTD pool.
- So no other column, no period row and no period date can change. A raw value the representability filter drops
  stays dropped and is reported as `dropped_before_decision`.
- `FactRow` is rebuilt from the raw value on every run, so a restore never applies twice, and
  `financial_facts_raw` is never changed.

The acceptance report applies the same function to every stored fact of the concept, including those the
normaliser's input filter (`:2375-2377`, fiscal year and period not NULL) excludes, and reports both populations.

## The cents class is left as filed

FMBH tags `98` for $0.98 with no scale. This investigation found no applicable published rule: it checked every
DQC_0179 component (10139–10144, 10151, 10153), none of which tests the magnitude of an unscaled amount.
Three discriminators were measured and none separates FMBH from genuine values:

1. **Dividends paid** (DPS × weighted-average shares ÷ dividends paid; #3947 research (a)): GBCI, XHR, DPZ and
   MSGS overlap FMBH.
2. **Dividends declared** on SEC frames, CY2010–CY2026 (#3947 research (b)): J, HL and MSGS overlap, and the band
   mixes scaled DPS, scaled share counts and scaled totals.
3. **FY ÷ nine-month YTD of the same issuer** [3]: of 2,217 eligible pairs (USD-per-share duration facts with a
   positive YTD; 0 selection keys hold two values), 11 have a ratio ≥ 10. FMBH sits at 133–134 and genuine special dividends at up to 46 (PGR) and
   39 (DDS). No observed genuine value reaches FMBH, but no rule supplies a threshold, and one placed between 46
   and 133 would be fitted to today's 11 pairs with no out-of-sample test.

A cross-filing ÷100 disagreement finds 5 instruments and cannot see a defect reported once (#3947, 21:13Z). Stock
splits also restate per-share history by exact ratios, so a ratio alone is not a defect.

**Decision:** FMBH's members are kept as filed, with no flag and no per-issuer exception, because an exception
list is a per-case patch with no rule to say where it ends. Effects: FMBH's `ttm_dps` (98.02 today) and its FY
members stay overstated on the pane and in `dps_declared_ttm`; its `latest_dps` (0.25) is a quarter and is
correct. The sec-edgar skill records the class, the three measured discriminators and FMBH. This change adds no
heuristic for it. Reopening needs a published rule or a structured source that marks the unit, tested on the
full population.

## Acceptance (corpus rung)

The flag stays off until 1–4 pass. Items 1, 2 and 4 run in one script (`scripts/ab_3947_dps_inline_scale.py`).
It uses one `REPEATABLE READ` transaction, opened with `SELECT 1` first, because `conn.transaction()` on an idle
psycopg connection commits. It runs the normaliser with decisions off and on through a function argument, not the
flag, reads every figure from that one snapshot, and rolls back.

1. **Scan coverage.** The script runs once every in-scope accession's work at the current version is done or
   exhausted, with no attempt in flight. It reports, for that snapshot:
   - accessions by active generation (`inline`, `not_inline`, none) and by work state (done, exhausted), with
     exhausted ones listed by `last_error`. 0 are pending or retrying;
   - every stored fact of the concept with one `Decision`, counted by reason for all facts and for
     derivation-eligible ones;
   - every `unresolved`, `entity_mismatch` and `no_match` accession, with its reasons. These keep today's value and
     do not block.
   - **Exhausted accessions are unknown coverage, not absence.** They keep today's value, are listed as a rollout
     exception, and no claim is made about their scale.
2. **Fact-level A/B.**
   - Every changed `dps_declared` cell is explained by its lineage: a restored fact, a refused fact (NULL or another
     fact filling it), or a derived Q4 whose inputs changed. Every other column, every period row's existence and
     its dates are unchanged. An unexplained change fails.
   - Every restored fact is listed with instrument, accession, period, display text, scale and restored value, plus
     the evidence from other accessions. That is every `CommonStockDividendsPerShareDeclared` fact of the same
     instrument, unit `USD/shares`, and the same `period_start` and `period_end` whose decision is `unscaled`, with
     all their values. Each restored fact is classified `agrees` (every one equals the restored value), `disagrees`
     (any differs) or `none`. This evidence sits beside the policy and does not gate it.
3. **Fixtures.**
   - **Parser**, one per contract clause: absent and present `scale` and `sign`; `+3` and ` 3 `; scale out of
     bound; supported and unsupported formats; nil; an unresolved `contextRef` and `unitRef`; a duplicate id; a
     `target` attribute; prefix variants for one namespace; an `unsupported_unit`; the CIK scheme; a
     multi-document set.
   - **Decisions**, with expected values written by hand: one per table row, plus scale 3 and 6, and a refused FY
     member with its derived Q4.
   - **DB tests for the job:**
     - a positive → unscaled rescan, which queues a reversal;
     - a failed rescan, which keeps the active generation;
     - a row re-queued during draining, which survives;
     - a version bump, with a once-per-version attempt reset;
     - exhaustion and re-admission;
     - two normalisations of one instrument, which serialise and give the same rows.
4. **Downstream**, decisions off against on in the same snapshot, for every instrument with a restore or a refusal:
   `instrument_dividend_summary.ttm_dps` and `latest_dps`, and `financial_periods_ttm.dps_declared_ttm`.
5. **Enablement.**
   1. Turn the flag on, then confirm that the jobs child PID and the API process have both restarted.
   2. Enqueue every instrument item 2 lists, and drain the queue to empty with an uncapped one-off run of the
      drain step.
   3. Only then run 6–8.
6. **Smoke panel:** AAPL, GME, MSFT, JPM and HD unchanged on `/instruments/<symbol>/dividends`.
7. **Source-display reconciliation:** EFC's Q3 2025 DPS against the figure `efc-20250930.htm` displays on EDGAR
   (0.39). This checks the implementation against the source display, not the policy.
8. **Live re-check:** EFC on `/instruments/EFC/dividends`. The four quarters `ttm_dps` sums are listed with their
   source accessions, and a derived Q4 shows its FY − Q1 − Q2 − Q3 arithmetic. `ttm_dps` must equal that sum.

## Rollback

1. Set `EBULL_DPS_INLINE_SCALE_ENABLED=false`. Confirm that the jobs child PID and the API process have restarted.
   Those are the processes that host every normaliser caller (`scheduler.py:5282`, `force_refresh.py:164`).
2. Pause `sec_dps_inline_scale_scan`. Wait until `job_runs` shows no run in progress of it or of
   `daily_financial_facts`. The per-instrument advisory lock orders any late writer before the rebuild's own write
   for that instrument.
3. Re-normalise every instrument holding a `CommonStockDividendsPerShareDeclared` fact. That set is wider than the
   affected one and needs no ledger, because the normaliser is deterministic over its inputs.
4. Verify in one snapshot: the A/B script in decisions-off mode against the stored rows, `dps_declared` and the
   item-4 downstream fields, gives 0 differences. An instrument that differs because ingestion moved its inputs is
   re-normalised and checked again.
5. Only then, if the change is abandoned, drop the three tables and the sequence with the migration's rollback file.

## Not in scope

- The cents class (above).
- The other `dps_declared` aliases and `dps_cash_paid` (policy 5).
- Inline scale on other per-share concepts. #3344 covers part of EPS: it catches the "in millions" class when
  earnings, shares and cover shares are present, and deliberately not the "in thousands" class.

## Amendment 1: durable rows beyond the swept raw layer

PR (b)'s A/B on EFC and WHR (#3947, 2026-10-22) listed stored `dps_declared` cells that no normalisation rewrote:
WHR Q2 and Q3 2023 at 1,750,000 each, and EFC's derived Q4 2022 and 2023 at 1,798.65 and 1,349.10. This amendment
explains why. It changes the population, the drain, how decisions reach derived cells, the A/B, enablement and
rollback. Figures marked [An] come from `PYTHONPATH=. uv run python -m scripts.measure_3947_unreachable` (dev DB and
EDGAR, 2026-10-22). Each subsection names the v4 text it overrides. v4 text it does not name stands.

### Finding

- **Raw is a retention window; canonical rows are durable history.**
  - `financial_facts_retention_sweep` runs daily at 02:45 UTC (`app/workers/scheduler.py:2016`). It keeps the latest
    3 annual-family (10-K, 10-K/A) and 8 quarterly-family (10-Q, 10-Q/A) accessions per instrument in
    `financial_facts_raw` (`app/services/financial_facts_retention.py:44-49`).
  - Canonical rows whose facts aged out are kept as history, not deleted
    (`app/services/fundamentals/__init__.py:2161-2170`).
- **Every companyfacts refresh refills raw with full history first.**
  - `daily_financial_facts` fetches the CIK's companyfacts with a 20-year cutoff, upserts every fact, then normalises
    the touched instruments (`app/workers/scheduler.py:5246-5300`). `run_force_refresh` does the same for a chosen
    set (`app/services/fundamentals/force_refresh.py:135-170`).
  - On 2026-10-22, NFLX was fetched at 02:32 UTC and held 18 10-K accessions back to 2010. After the 02:45 sweep, no
    instrument held more than 3 [A1].
  - The FY guard spec already names re-ingesting and then re-deriving as the way to correct an aged-out wrong-value
    row (`docs/specs/fundamentals/2026-06-29-fy-flow-duration-guard.md:133-138`).
- **v4 has three gaps.**
  - **G1, population.** The scan reads its population from raw at 00:30 UTC, when raw is swept. An accession that
    aged out before the scan began is never scanned.
  - **G2, drain.** The drain normalises over swept raw, and that cannot rewrite a cell whose facts are gone.
  - **G3, mixed derivations.** Under v4, a cell derived by difference can mix a restored input with a kept raw input
    that was never verified. Two derivations do this:
    - Q4 = FY − Q1 − Q2 − Q3 (`app/services/fundamentals/__init__.py:1824-1839`);
    - the YTD fill, which resolves a discrete quarter from the YTD pool (`_resolve_flow_value`, `:964`, applied at
      `:1741-1753`).

    A restored FY of 1.56 minus a kept quarter of 390 is negative. Nothing in v4 or the merged PR (b) prevents it.
    The flag is off, so it has had no live effect.
- **Size.** 4,625 stored `dps_declared` cells in 169 instruments have no `financial_periods_raw` row today [A2].
  That depends on when each instrument was last normalised, so it does not size the work. The population below does.

### A1. A durable, insert-only population (overrides "The scan → Population" and the ordering in "Order and budget")

```sql
CREATE TABLE sec_inline_scale_population (
    accession_number TEXT   NOT NULL,
    instrument_id    BIGINT NOT NULL REFERENCES instruments,
    filed_date       DATE,               -- NULL: undated
    PRIMARY KEY (accession_number, instrument_id)
);

CREATE TABLE sec_inline_scale_discovery (
    instrument_id    BIGINT PRIMARY KEY REFERENCES instruments,
    discovered_at    TIMESTAMPTZ,        -- set by any companyfacts upsert for the instrument after the migration
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ,
    last_error       TEXT
);
```

- **What writes rows.** Population rows are only ever inserted, and a NULL `filed_date` is filled when a date
  becomes known. Rows are written by:
  1. **Every write of companyfacts facts into raw.** In the same transaction, `upsert_facts_for_instrument`
     (`app/services/fundamentals/__init__.py:407`) does two things:
     - records the instrument's accessions holding the concept, with the facts' filed date;
     - sets the instrument's `discovered_at`, but only when the caller passes `complete_payload=True`.

     A caller passes `complete_payload=True` only after fetching and parsing the CIK's whole companyfacts payload
     successfully. Any other write records population rows but never marks discovery.
     - **New membership can enqueue.** When a population insert adds an (accession, instrument) pair whose
       accession already has an active generation, the A4 enqueue predicate is evaluated for that instrument in the
       same transaction. An already-scanned accession can then still reach a newly attached instrument.

     The build confirms by grep that the function is the only writer of these facts. The callers known today are
     the daily refresh (`:3420`), the per-instrument refresh (`:630`) and the bulk ingest
     (`app/services/sec_companyfacts_ingest.py:281`). So whenever raw holds full history, the population captures
     it.
  2. **The migration and each scan run, from durable records.** These rows come from the accessions holding the
     concept in raw and from the accessions named in the `source_ref` of `sec_edgar` rows with `dps_declared` not
     NULL. Their date resolves from raw (any concept), else `sec_filing_manifest.filed_at`, else the earliest
     `filing_events.filing_date`. A population instrument with no discovery row gets one.
  3. **Discovery.** Each scan run fetches companyfacts for discovery rows with no `discovered_at` and upserts it
     through (1). Raw is then trimmed by the next sweep, as after every refresh.
     - **Retries.** A failure follows v4's backoff, `attempts` and `next_attempt_at`. At 5 attempts the instrument's
       discovery is exhausted, and it is reported.
     - **Order.** Discovery runs after draining and before scanning.
- **Why both (2) and (3).** The `source_ref` seed alone is incomplete.
  - Of four issuers checked against SEC companyconcept [A4] (illustrative, not a completeness test), SCI and EFC each
    have 2 concept accessions absent from the seed.
  - A Q4 row copies only the FY row's `source_ref` (`:1816`), and YTD-pool facts are pooled separately (`:1738`).
    So `source_ref` does not trace every derivation input.
  - Discovery captures what companyfacts serves, whatever the provenance.
- **Membership is never removed** except by rollback step 5. So a parser-version bump rescans aged-out accessions
  too.
- **The population** is the table's rows dated on or after 2019-06-15. An accession's instruments are its rows.
- **Eligibility is unchanged.** v4's eligibility predicate on `parser_version`, `attempt_version`, `attempts` and
  `next_attempt_at` applies to every population accession, and so do its version and backoff rules.
- **Undated rows are not scanned.** An accession with no active generation then decides `untested`. One with an
  active generation from an earlier dated scan keeps using it. Work state never overrides an active generation.
- **Archive owner** follows the v4 query (`app/services/dps_inline_scale_scan.py:136-140`): the manifest CIK, else
  the primary SEC CIK of the lowest instrument id that has one. A wrong owner fails the attempt and ends
  `exhausted`, which is reported. It is not read as absence.
- **Each run note reports:**
  - work state for two classes, `raw` (the accession holds the concept in raw at that run) and `history`
    (everything else);
  - the undated count;
  - discovery state: discovered, pending, retrying and exhausted.
- **Order (replaces v4's).** Within each step, oldest first:
  1. draining;
  2. discovery;
  3. filings from the last 7 days;
  4. the `raw` backlog;
  5. `history` accessions of candidate instruments (A4);
  6. the remaining `history`.
- **Seed size [A3].**
  - 57,445 accessions over 1,731 instruments.
  - 11,878 are `raw` and 17,232 are `history`, all dated on or after 2019-06-15.
  - 9,331 are dated earlier.
  - 19,004 are undated. By the accession number's year field, 328 of those are from 2020 to 2025 and 348 from 2019.

  Discovery adds what the seed misses.

*Build note (PR c).*
- The A1 (2) seed runs at the start of every scan run, after the drain. Migration 515 creates the tables only;
  the first run after deploy seeds them before it selects anything. On the dev DB (2026-10-22, rolled back) the
  first seed took 42 s and a repeat took 6 s. It reproduced [A3]: `raw` 11,878, `history` 17,232, undated 19,004 and
  1,731 instruments.
- The date fill covers every undated member, not only the pairs in the current seed. A member whose `source_ref`
  was later reselected away still takes a date once raw, the manifest or a filing event knows one.
- Any `complete_payload=True` write sets `discovered_at`, inserting the row if needed, whether or not the instrument
  is in the population yet. That write already captured every concept accession companyfacts serves for it.
  Discovery fetches only population instruments. An empty payload (a 404 or no sections) counts as discovered.
- The run note adds `history_candidate`, the `history` accessions of candidate instruments, as a subset beside the
  two classes, because Stage 1 readiness (A5) reads it.
- The drain still normalises over swept raw. `refill_and_normalise`, the sweep lock, the A3 groups and the
  stage-scoped A/B gates arrive with PR (d). Until then the A/B's coverage gate requires both classes complete,
  which is the Stage 2 condition.

### A2. Refill, then normalise, as one locked unit (overrides "Draining", where it says "normalises the instrument")

The drain, enablement and rollback all use one per-instrument primitive, `refill_and_normalise`:

1. **Fetch** the instrument's companyfacts (primary CIK, as the daily path) outside any transaction. A fetch or
   parse failure increments `failures` and leaves the queue row.
2. **In one transaction:**
   1. `lock_instrument_for_normalize(iid)` (`:2473`);
   2. upsert the facts through `upsert_facts_for_instrument`, which also records the population (A1 (1));
   3. `normalize_financial_periods([iid])`, which nests in the transaction and holds its lock to the end
     (`:2486-2560`);
   4. mark the queue row drained at the revision it read (A4's ledger replaces v4's `DELETE … AND revision = %s`);
   5. commit.

   A normalisation failure (`instruments_failed`) rolls the transaction back and counts as a failure. Only a
   successful refill **and** normalisation dequeues.
3. **The sweep takes the same lock.** `financial_facts_retention_sweep` takes `lock_instrument_for_normalize(iid)`
   before its per-instrument DELETE. It therefore cannot remove facts between step 2's upsert and its normalisation,
   whatever the hour or caller. The sweep holds one instrument lock per transaction, so it cannot form a lock-order
   cycle with the normaliser's sorted order. The daily path keeps its existing commit between upsert and
   normalisation; it is unchanged here.

Further points:
- **Data treatment.** This is the daily path's data treatment (settled "Fundamentals provider posture", #532),
  triggered early. A refill also rewrites other columns of aged-out rows: the values the instrument's next filing
  would write anyway. The A/B reports that drift and does not attribute it to this policy.
- **Requests.** One companyfacts request per instrument, through the shared SEC limiter.
- **Rejected, option (b): repairing a cell from an occurrence.** That would be a second writer into
  `financial_periods`, re-implementing value selection and both derivations outside the normaliser.

*Build note (PR d2a).* (d2) is split again: (d2a) is this section; (d2b) is the A/B rework.
- `fetch_companyfacts` (step 1) and `refill_and_normalise` (step 2) live in `app/services/dps_inline_scale_scan.py`.
  `refill_and_normalise` runs inside the caller's transaction and refuses outside one. It raises `RefillFailed` on a
  failed normalisation so the caller rolls the upsert back. It upserts with `complete_payload=True`, because the
  payload is the whole companyfacts fetch.
- `drain_renormalise_queue` now takes `facts_provider`. The uncapped enablement run is
  `drain_renormalise_queue(conn, facts_provider=SecFundamentalsProvider(...), admit_until=None)`. A fetch failure, a
  write error or a failed normalisation each increment `failures` and keep the row; `DrainResult.fetch_failed` counts
  the first. Each drain with queued rows opens one `data_ingestion_runs` row, as discovery does.
- `sweep_retention_for_instrument` takes `lock_instrument_for_normalize` before its DELETE, so every caller of the
  sweep holds the lock, not only the scheduled orchestrator.

### A3. Decisions reach a derivation group only when every leaf is verified (overrides "Where decisions act" and decision row 8's action)

- **Two derivations, same selection.** With the flag on, the normaliser derives each instrument's periods twice over
  the same inputs: decisions off and decisions on.
  - The decisions-on derivation replaces a `scaled` fact's value with its restored value, and changes nothing else.
  - It skips no fact. A `mixed_scale` fact (v4 row 8) keeps its raw value in that derivation and is unverified, so
    any cell it reaches takes the decisions-off value below. The refusal therefore acts as "keep", which is v4's
    "doubt keeps".
  - The two derivations thus select the same facts, produce the same NULL cells, and take the same Q4 and YTD
    branches. Only values differ.
- **Leaves.** Every `dps_declared` cell carries its leaf facts:
  - a directly selected cell: its selected fact;
  - a YTD-filled cell: the facts the resolver combined. `_resolve_flow_value` (`:964`) gains a variant that returns
    them, on its direct and its subtraction branches;
  - a derived Q4: the leaves of its FY and Q1–Q3 cells.

  A NULL cell has no value to gate and is NULL in both derivations.
- **Verified decisions:**
  - `scaled` and `unscaled`;
  - `not_inline`, an instance value the filer authored directly.

  Every other decision is unverified: `untested`, `unresolved`, `no_match`, `entity_mismatch`, `mixed_scale`,
  `unsupported_unit` and `out_of_scope`. `out_of_scope` is included because v4 policy 6 counts earlier facts as
  untested, not as non-inline.
  - **A leaf of another `dps_declared` concept** (the aliases of v4 policy 5, which are not scanned) has no
    decision. It is unverified, with reason `other_concept`.
- **Derivation group.** For each `fiscal_year` label, the group is that label's FY, Q1, Q2, Q3 and Q4 cells,
  counting both the cells this normalisation produces and the label's stored `financial_periods` rows. These are
  the cells the Q4 derivation (`:1787-1839`) and the YTD fill (`:1741-1753`) link.
- **An incomplete group keeps its stored DPS.**
  - A group is complete when the normalisation produces a row for every member the stored rows hold.
  - Over swept raw, a label's FY can be produced while its aged-out quarters are not. That group is incomplete,
    and the produced members are written with their stored `dps_declared`, unchanged.
  - So a normalisation over swept raw never pairs a newly restored FY with stored quarters of the other regime.
    The only change is to `dps_declared` of the incomplete group's produced rows; every other column is written as
    today.
  - A complete group is gated by the rule below. `refill_and_normalise` always produces complete groups for the
    years companyfacts still serves.
- **Rule.** A group takes its decisions-on values only when two conditions hold:
  - every leaf of every cell in it is verified;
  - no cell in it is negative in the decisions-on derivation while the same cell is non-negative or NULL in the
    decisions-off derivation.

  Otherwise every cell of the group takes its decisions-off value, with reason `unverified_leaf`, `other_concept`
  or `negative_derived`. Each group therefore takes one of three branches: `on`, `off` or `kept_stored`. Gating the whole group keeps displayed arithmetic consistent: a derived Q4 always equals
  FY − Q1 − Q2 − Q3 over the cells shown beside it.
- **The fallback** is the decisions-off value over the same (possibly refilled) inputs. It is not necessarily
  today's stored value, because a refill can move inputs.
- **Negativity.** XBRL US DQC.US.0015 ("Negative Values") lists `CommonStockDividendsPerShareDeclared` as rule
  element 5748, an element that should not be negative (`dqc_us_rules/resources/DQC_US_0015/
  dqc_15_usgaap_2026_concepts.csv`). That rule tests filed facts. Applying it to derived cells is this spec's
  regression gate: a decision may not introduce a negative the decisions-off derivation does not have. The gate claims
  nothing about which value is right.
- **v4's guarantee, restated.**
  - v4 says no other column, period row or date can change. That now holds between the two derivations over
    identical inputs.
  - `ttm_dps` sums cells (sql/050:96). Every summed cell is consistent within its group, and the sum stays
    per-cell, as in v4.

*Build note (PR d1).* PR (d) is split: (d1) is A3 in the normaliser; (d2) is `refill_and_normalise`, the sweep lock
and the A/B rework.
- The gate is `_gate_dps_groups`, called through `derive_gated_dps_periods` with the flag on. It returns each
  group's `DpsGroupOutcome` (branch and reason), which (d2)'s A/B reads. `PeriodRow.dps_leaves` carries each cell's
  leaves; `_resolve_flow_leaves` is the resolver variant.
- **Same row, not same label.** An incomplete group's produced row takes the stored value of the same row
  (`fiscal_year`, `period_type`, `period_end_date`). Completeness stays per label and type. The derivation can emit
  several rows under one label (#1914/#541 collisions, which the canonical merge resolves), and a label-keyed copy put
  one stored value on rows with other period ends: 381 cells moved off the decisions-off value on the dev DB, against
  25 with the row key (2026-10-22, read-only census over 1,640 instruments).
- **A row that differs between the two derivations** cannot occur by construction. If it does, every group of the
  instrument takes decisions-off (`structure_mismatch`) and an error is logged.
- **Flag off is unchanged.** The decisions-off derivation was compared old code against new over every instrument
  holding a `dps_declared` fact: 1,588 instruments, 84,453 rows, every column, 0 differences.
- **Flag on, dev DB, over today's swept raw (read-only):** groups `on` 7,193, `off` 15,591 (`unverified_leaf`
  15,454, `other_concept` 137), `kept_stored` 958. Cells that differ from decisions-off: 8 `on` (the scanned EFC
  restores) and 25 `kept_stored`. The kept cells are stored history that the swept raw now derives differently: the
  stored value in place of a NULL or another value, and 4-dp storage against the unrounded derivation.

### A4. Candidates, the enqueue predicate, and the rollback set

- **Candidate instrument.** A population instrument with an accession whose active generation holds a
  positive-scale or unresolved occurrence.
- **The enqueue predicate** (this overrides only the "when" clause of v4's "Queue"). A successful scan enqueues the
  accession's population instruments when either:
  - its old or new generation holds a positive-scale or unresolved occurrence (as in v4); or
  - the instrument is a candidate, because a rescan can flip a leaf from unverified to verified and so unlock a
    group.

  v4's other queue guarantees stand: the enqueue happens in the generation-publication transaction, each enqueue
  takes a fresh sequence-backed revision, and a later scan never clears queued work.
- **The queue is also a durable ledger.** `sec_inline_scale_renormalise` gains `drained_revision BIGINT`. A
  successful drain sets `drained_revision = revision`, conditioned on the revision it read, and no longer deletes
  the row. An instrument is queued while `drained_revision IS DISTINCT FROM revision`.
  - The ledger lists every instrument whose cells a decision could have changed: a restored or refused leaf
    requires a positive-scale or unresolved occurrence, and publishing one enqueues.
  - **Verification scope.** Stage 2 convergence, the `unreachable` report and rollback verification all use the
    ledger's instruments, including former candidates.
- **The rollback set** (step 3's rebuild) is every population instrument, not a candidate set. A rescan can drop an instrument from
  the candidates before its reversal drains, so the broad set is used, and it needs no ledger. On [A3] that is
  about 1,731 companyfacts requests.

### A5. Enablement in two stages

- **Readiness.** The scan's run note decides when a stage may start. The stage's A/B then recomputes the
  population, classes, discovery state and coverage inside its own snapshot, as v4 requires, and fails if the
  stage's coverage condition does not hold there.
- **Stage 1** runs when two conditions hold in the snapshot, with 0 pending and 0 retrying in each:
  - the `raw` class and the `history` accessions of candidate instruments;
  - discovery of every candidate instrument.

  It is v4 acceptance 1–8, as amended below.
  - The A/B's refill can reveal concept accessions not in the table. They are listed as `undiscovered`, their
    groups fall back under A3, and they do not fail the gate.
  - A3 gates every group at runtime. Partial coverage therefore leaves each group either verified or at its
    decisions-off value, and a later scan can activate a group only under the same rule.
- **Stage 2** runs when two conditions hold:
  - full coverage of both classes;
  - discovery complete for every population instrument, with exhausted instruments listed as unknown coverage.

  Stage 2 then runs:
  1. an uncapped drain to empty;
  2. the A/B;
  3. **stored convergence:** in one snapshot, the stored rows of every ledger instrument (A4) equal a fresh refill
     and gated normalisation. Any difference is listed and fails, except `unreachable` cells (defined below), which are
     listed.

  A Stage 2 failure starts the rollback.
- **Convergence to the gated result.** A group's stored cells equal its gated result once every leaf's accession is
  dated and in the population, each was scanned at the current version, and the instrument drained after the last
  of those scans. The gated result is the decisions-on values only if every leaf is verified and the negative gate
  passes. `unresolved`, `no_match` and `entity_mismatch` leaves keep the group at decisions-off while those decisions
  remain unverified. The A/B
  lists every group by state. There is no fixed bound on the number of cycles.
- **Pace [A5], an optimistic extrapolation.** The 2026-10-22 01:54 UTC run, with draining off, scanned 628
  accessions in its 45-minute cap and fetched 6,227 documents (9.9 per accession). That projects about 18 more runs
  for the 11,437 pending `raw` accessions and about 28 for 17,232 `history` ones. After enablement, draining and
  discovery can take up to half of each run's admission window, and history documents may cost differently.
- **Cadence stays daily.** A run holds the `sec_rate` lane, and `sec_atom_fast_lane` skips its fires while one runs
  (`app/workers/scheduler.py:1731-1732`).

### Acceptance and rollback, as amended

- **The A/B (items 1, 2 and 4).**
  - **Quiescence and retry.** The script runs with `sec_dps_inline_scale_scan` paused and no
    `daily_financial_facts` run in progress.
    - Its first statements take `lock_instrument_for_normalize` for every instrument in scope, in sorted order,
      before any write. That matches A2's lock-before-upsert order, so it cannot deadlock with a drain.
    - A `REPEATABLE READ` serialization failure is an operational retry of the whole transaction, at most 3 times.
      It is reported, and it is not an acceptance failure.
  - **Scope.** Stage 1 covers the candidate instruments; Stage 2 and rollback verification cover the ledger's
    instruments (A4). Each refill writes one instrument's full history inside the snapshot.
  - **Prefetch.** It fetches and parses the companyfacts payload of every in-scope instrument before it opens the
    snapshot. No SEC request runs while it holds the locks or the snapshot, as in A2. A fetch failure is reported,
    and that instrument is listed as unrefilled.
  - **Refill.** Inside the snapshot, after the locks, it upserts each prefetched payload with
    `upsert_facts_for_instrument` in the snapshot's transaction. It does not call `refresh_financial_facts`, which commits
    (`app/services/fundamentals/__init__.py:580`).
  - **Both derivations** then run over the refilled raw. The script computes each group's expected branch (`on`,
    `off` or `kept_stored`) independently from A3's rule and records the branch each final cell took.
  - **Lineage** (v4 item 2) covers YTD-filled cells as well as derived Q4s, through their leaves.
  - **It reports:**
    - refill drift between the stored rows and the decisions-off result: cells by column, plus row additions and
      removals, date changes and `source_ref` changes. This is informational;
    - `unreachable`: a stored `dps_declared` cell of an in-scope instrument whose key (`period_type`,
      `period_end_date`) has no `dps_declared` value in the decisions-off result over the refilled inputs. That is
      the actual selection and derivation output.
      - A row the normaliser deletes under its structural rules (B2 sub-annual FY, `:2151-2179`; B3 future period)
        is a deletion. It is reported in refill drift and is not `unreachable`.
      - Unreachable cells are kept and listed;
    - work state per class, with the undated, undiscovered and discovery-exhausted counts;
    - groups by A3 reason.
  - **It fails on** any of these invariant violations:
    - a group member that did not take its group's expected branch;
    - a final value that differs from its branch's result: the decisions-on result, the decisions-off result, or
      the stored value;
    - an `on` group with an unverified leaf, or with a cell negative where the decisions-off cell is not.
- **Fixtures**, added to v4 acceptance 3. v4's "refused FY member with its derived Q4" fixture is replaced by the
  `mixed_scale` fixture below, because A3 keeps the fact:
  - A Q4 whose Q2 is YTD-filled with an unverified leaf. The whole fiscal-year group takes decisions-off values.
  - A `mixed_scale` fact. Its group takes decisions-off values, equal to the decisions-off derivation.
  - A group with all leaves verified but a negative YTD-filled quarter. The whole group falls back, and the
    displayed Q4 equals FY − ΣQ.
  - An `untested` → `unscaled` rescan on a candidate. It enqueues the instrument, and the group activates on drain.
  - The sweep blocked by the lock during `refill_and_normalise`. This is a DB test with a stubbed provider.
  - Refill, then the sweep, then an ordinary normalisation over swept raw. The incomplete group keeps its stored
    DPS.
  - A group with an alias-concept leaf takes decisions-off values (`other_concept`).
  - Discovery attaches an instrument to an already-scanned positive-scale accession, which enqueues it.
  - A partial write (`complete_payload=False`) records population rows but no `discovered_at`.
  - A drain that sets `drained_revision`, after which a re-enqueue makes the instrument queued again.
  - The A/B's sorted locks taken before its first write.
  - A fetch failure and a normalisation failure. Each leaves the queue row and increments `failures`.
  - Population and discovery writes:
    - from `upsert_facts_for_instrument`, which also sets `discovered_at`;
    - from `source_ref`;
    - insert-only across a reselection that removes a `source_ref`;
    - discovery retry and exhaustion;
    - an undated row, skipped, while an existing generation is still used.
*Build note (PR d2b).* `scripts/ab_3947_dps_inline_scale.py` takes `--stage 1|2` or `--verify-stored off|on`.
- **Scope** is read before the prefetch and again in the snapshot; the locks cover both. An instrument the in-snapshot
  seed adds to the scope fails the run ("rerun"), because it was neither prefetched nor locked. Prefetched payloads
  are spilled to a temporary directory, so a full companyfacts payload per instrument does not stay in memory.
- **Off before on.** The gated derivation reads the stored rows. The script runs the decisions-off normalisation in a
  savepoint and undoes it before the decisions-on one, so `kept_stored` copies the stored value, not the off arm's
  output.
- **The expected branch** is computed from this section's rule in the script, not imported from `_gate_dps_groups`.
  A changed cell (off against on, through the real normaliser) is explained only when it equals its gated value; the
  v4 per-cell lineage (`direct` / `ytd` / `derived_q4`) is removed.
- **`--verify-stored on`** is Stage 2's stored convergence; **`off`** is Rollback step 4. Both scope the ledger.
- **Dry run, dev DB, 2026-10-22** (`--stage 1 --allow-incomplete`, rolled back): 16 candidate instruments, groups
  `on` 69, `off`/`unverified_leaf` 153, `kept_stored` 1. 8 cells changed, all EFC and all explained. 1 unreachable
  cell: instrument 8604's stored Q4 2025 at 0.0000, a label its refilled derivation no longer produces. Refill drift
  touches one instrument (EFC: one row added, 15 cells). PASS apart from coverage. `--verify-stored off` fails on that
  same EFC drift, as expected before rollback step 3.

- **Enablement step 2** drains the candidate set with `refill_and_normalise`.
- **Item 9, Stage 2.** It also checks the live pane: EFC's dividend history shows Q4 2022 and Q4 2023 as
  FY − Q1 − Q2 − Q3 from restored inputs, and WHR's Q2 and Q3 2023 show 1.75.
- **Rollback.**
  - **Step 3:** run `refill_and_normalise` with the flag off for every population instrument. Normalising over
    swept raw cannot revert a restored aged-out cell.
  - **Step 4:** verify the ledger's instruments in decisions-off mode with the same locked, in-snapshot refill.
  - **Rollback exceptions.** A ledger instrument's stored cell that the decisions-off result over the refilled
    inputs does not produce (the `unreachable` definition, structural deletions excluded) cannot be reverted. Each one is listed with its stored value as a rollback
    exception, and the rollback is not complete until the list is empty or each exception is accepted on #3947.
  - **Step 5** also drops `sec_inline_scale_population` and `sec_inline_scale_discovery`.

## Amendment 2: the document set comes from the accession's XBRL archive

At the scan's v1 pace the `raw` backlog alone was about 42 daily runs (#3947, 2026-10-23: about 630 accessions per
run against 26,762 pending; fetch was about 98% of an attempt). Measuring a faster acquisition found a second defect:
v1 leaves members of the document set out of it. Figures marked [Z…] come from
`PYTHONPATH=. uv run python -m scripts.measure_3947_xbrl_zip` (dev DB and EDGAR, 2026-10-23, at `PARSER_VERSION`
v1). The script takes a seeded 30 accessions from each of five disjoint arms of the scan's own population
(`_POPULATION_SQL`): form 8-K, else scanned `inline`, else scanned `not_inline`, else unscanned with the concept in raw,
else history-only. That is 150 accessions; the seed selects from the population as it stands, so a rerun on a later
day may select others. Every [Z] figure is a sample figure, not a population claim. Each subsection names the text it
overrides; text it does not name stands.

### Finding

- **[Z1] The archive yields the same occurrences as the listing.** For each sampled accession the script reads the
  Inline XBRL documents of `{accession}-xbrl.zip` and of the `index.json` listing, and runs the v1 extraction
  (`parse_document_set`) over B1-classified, XML-decoded inputs from each.
  - The members have the same names in 150 of 150, and the extraction outcome and occurrences are equal in 150 of 150
    (449 occurrences). No candidate was `undecidable`.
  - Bytes are reported, not gated: the served copy is not the filer's bytes. Of the 117 accessions with Inline XBRL
    members, the raw bytes are equal in 0. Every served member (147) carries a `<script>` element that `www.sec.gov`
    appends before `</body>`, and no archive member does. The served copy also has LF line ends and no blanks before
    a line end where the archive's bytes have CRLF or blanks. With those three differences removed the bytes are equal
    in 116 of 117; MCO `0001059556-19-000004` still differs and has equal occurrences.
- **[Z2] Two requests instead of about ten.** v1's path makes 8.47 to 11.40 requests per accession by arm:
  `index.json`, `FilingSummary.xml` and every listed HTML file, EDGAR's own `-index.html` and `-index-headers.html`
  pages included. The proposed acquisition makes 2: the archive and the SGML header.
  - This is an acquisition and classification benchmark. Both paths run the v1 extraction and are timed the same way:
    requests at the script's 0.15 s spacing, unzip, classification and occurrence parsing, with the two paths in
    alternating order. v2's extraction is not built yet.
  - Mean seconds per accession by arm are 2.49 to 3.32 for v1's path and 0.57 to 0.73 for the proposed one
    (network-dependent; a rerun moves these, not the counts).
  - The script does not share the jobs process's limiter; production pace is B4.5.
- **[Z3] v1 leaves set members out.** v1 builds contexts, units and occurrences only from documents holding an
  `ix:header`.
  - 21 sampled Inline XBRL documents hold none, all in the 8-K arm; their `<TYPE>`s are EX-99 exhibits and, in three
    cases, `8-K`. An independent regex census (`_regex_census`, neither lxml nor the app parser; a lower bound, as it
    reads only one lexical form) finds the concept in 16 of them, 125 occurrences.
  - EDGAR's extracted instance (`*_htm.xml`, SEC's own reading of the whole set) of the 19 accessions holding such
    documents carries 148 non-nil facts of the concept. 120 are missing from v1's occurrences, and v1 has none the
    instance lacks; for example, 18 of 18 are missing for `0001020569-22-000169` and 17 of 20 for
    `0001130310-20-000049`.
  - Control: for EFC `0001628280-26-055148`, the stored v1 occurrences (4, read before any fetch), the census, the v1
    extraction and the instance all give 4.
  - A fact matched only by an omitted occurrence reads `no_match` under v1.
- **[Z4] Two header-bearing documents.** 8 of the 30 sampled 8-Ks have an `ix:header` in the primary document and in
  an EX-99.1; their `<TYPE>`s are `8-K` and `EX-99.1`, both of the primary set.
- **[Z5] Archive contents.** All 30 sampled `not_inline` archives hold an `.xml` file other than the linkbases and no
  HTML. The archives of the other arms held non-Inline HTML members in 29, 30, 30 and 25 of 30 (`inline`,
  `unscanned_raw`, `history`, 8-K). The largest unpacked archive was 41,505,764 bytes.

The run was on 2026-10-23 after that day's 02:45 UTC retention sweep, which moved accessions from the `raw` class to
`history` (A1); the arm sizes the script prints are from that run.

### Source rule

| decision | rule | source |
|---|---|---|
| what an Inline XBRL document is | an attachment whose root element is `html`, with no prefix, in the XHTML namespace `http://www.w3.org/1999/xhtml`, and that contains any element in the Inline XBRL namespace `http://www.xbrl.org/2013/inlineXBRL`. A file that is not well-formed XML has no such root, so it is not one | EDGAR XBRL Guide (August 2026, https://www.sec.gov/files/edgar/filer-information/specifications/xbrl-guide.pdf) §11, "(Previously EFM § 5.2.5)": "EDGAR validates a file attachment with suffix .htm as an Inline XBRL document when its root element is html in namespace … (its namespace prefix must be empty) and it contains any element in namespace …"; "An .htm attachment that does not meet these two criteria … is not an Inline XBRL document". The Guide's PDF prints both namespace names with `https://`. The names are those of XHTML and of Inline XBRL 1.1 Part 1, which this scan already resolves (`IX_NS`, `app/services/inline_xbrl_dps.py`); no `https://` spelling is accepted |
| which documents form the set | every Inline XBRL document of the submission except attachments EX-FILING FEES and EX-98, as one Inline XBRL Document Set. A document need not hold an `ix:header`; a set holds at least one | Guide §11: "Inline XBRL formatted attachments are processed and validated together as an Inline XBRL Document Set (IXDS)"; "Certain exhibit attachments are excluded from the primary IXDS … Attachment EX-FILING FEES … Attachment EX 98". Inline XBRL 1.1 Part 1 §3.1 (an Inline XBRL Document contains Inline XBRL Elements) and §8.1.3 ("There MUST be at least one ix:header element in the Inline XBRL Document Set") |
| which attachment type a document has | the `<TYPE>` (CONFORMED DOCUMENT TYPE) of its `<DOCUMENT>` in the submission's SGML header | EDGAR Public Dissemination Service Technical Specification (https://www.sec.gov/files/info/edgar/specifications/pds_dissemination_spec.pdf), Table 9 |
| where the documents are read from | `{accession}-xbrl.zip`, which holds every Inline XBRL document of the submission | PDS Technical Specification §2.3.7: the archive is in a dissemination with XBRL documents "Always", and "contains all filer-submitted XBRL, all Inline XBRL formatted documents, and all .jpg and .gif files referenced …" |

- **The `<XBRL>` indicator.** PDS §2.3.2 says the document header of an Inline XBRL `.htm` carries the XBRL
  indicator (Table 9, `<XBRL>`). It would be a structured membership field. `-index-headers.html` does not carry it:
  each `<DOCUMENT>`'s `<TEXT>` there is replaced by a link (checked on `0000884887-20-000014` and
  `0001411342-19-000100`, 2026-10-23). It sits only inside the full submission text, which holds every document's
  body. Classification therefore applies Guide §11, the criterion EDGAR itself applies.
- **The archive's other HTML.** Nothing documents that the archive also holds non-Inline HTML exhibits [Z5], and
  nothing here relies on it.

### B1. The document set (overrides "Construction → Document set", its PR (a) build note, and the parser's set rule)

1. **Candidates** are the source's files with suffix `.htm`, `.html` or `.xhtml`. EDGAR's rule names `.htm`; the
   other two are compatibility suffixes kept from v4.
2. **Classification.** This replaces v4's parser-contract clause that "a document that does not parse" fails the
   attempt, for candidates; a set member has parsed by definition. A candidate's bytes go intact to the XML parser,
   which detects the encoding (BOM, declaration, else UTF-8).

   | parse | evidence | result |
   |---|---|---|
   | succeeds | unprefixed XHTML `html` root and an element in the Inline XBRL namespace, by expanded names | Inline XBRL document |
   | succeeds | otherwise | not one |
   | fails (malformed or undecodable) | the text, decoded by its BOM (UTF-8, UTF-16 or UTF-32), else its declared encoding, else UTF-8 with replacement, contains `inlineXBRL`; or a lenient HTML parse finds the namespace name in an attribute value | the attempt fails, `undecidable:<name>` |
   | fails | neither | not one |

   - A file that is not well-formed XML has no root element, so EDGAR's rule already makes it a non-member. The
     `undecidable` row is stricter than the rule: it refuses to decide a damaged file that looks Inline.
   - There is no byte-search prefilter, so neither an encoding nor a character reference can make a well-formed
     member a non-member.
3. **Types.** Every attempt reads the SGML header (`{accession}-index-headers.html`). It must give exactly one
   non-empty `<TYPE>` for every Inline XBRL document. A failed request fails the attempt, and so does an Inline XBRL
   document whose filename is absent, repeated, or carries an empty type or more than one type. Records of other
   files are not checked.
4. **The set** is every Inline XBRL document whose `<TYPE>` is not `EX-FILING FEES` or `EX-98`.
   - **An empty set is `not_inline`.** That covers no Inline XBRL document at all, and only excluded ones.
     - The outcome means "no primary set". It follows only from classifying every candidate of a source that holds
       every Inline XBRL document.
     - No other evidence (a non-Inline primary document, an `.xml` file) is used, since the Guide allows an Inline
       XBRL exhibit beside a non-Inline primary.
   - Documents in the set but no `ix:header` among them: the attempt fails (`no_ix_header`).
   - **Separate sets are out of scope.**
     - What happens to them. EX-FILING FEES and EX-98 documents are classified like any candidate, so an
       `undecidable` one fails the attempt. They take no part in resolution or extraction.
     - Why. EDGAR processes each as its own instance (Guide §11), and nothing shows that companyfacts, the source of
       the stored facts this scan matches, reads those instances.
     - The consequence. A stored fact matched only by an occurrence in one of them reads `no_match`, so its group
       stays at decisions-off (A3).
     - B4 lists any v1 stored occurrence that lies in one, and each listed occurrence blocks until it is accepted
       on #3947.
5. **Resolution** runs within the set, as in v4:
   - contexts and units resolve across its documents;
   - a duplicate id fails the attempt;
   - so does an Inline XBRL element with a `target` attribute (an HTML `<a target>` does not).

   A header-less member cannot supply a context or a unit: they sit in `ix:resources`, which "MUST be a child of
   the ix:header element" (Inline XBRL 1.1 Part 1 §14.1.1, content model Table 15). So including such members cannot
   change how an existing occurrence resolves.

### B2. How documents are obtained (overrides only acquisition: in "The scan → Attempt", in "Order and budget", and the module's "Attempt" rule)

Attempt initialisation, failure accounting, backoff, exhaustion, conditional publication, the queue and the A4
enqueue predicate are unchanged.

1. **Requests.** Every request goes through the provider's shared SEC limiter.
   - `FilingSummary.xml` and the SGML header use `fetch_document_text`, as now.
   - The archive and the listing path's HTML files use a new provider method that returns bytes, `None` on 404 or
     410.
   - `tests/test_fetch_document_text_callers.py` pins the callers of both methods. This overrides "Order and
     budget"'s statement that all requests use `fetch_document_text`.
2. **Archive.** One GET of
   `https://www.sec.gov/Archives/edgar/data/{cik}/{accession without dashes}/{accession}-xbrl.zip`. Members are read
   in memory, never written to disk. Only HTML members are read, and the CRC of each one read is verified; other
   members are not read.
3. **Limits.** These are policy limits on what one attempt buffers, fixed by construction at about six times the
   largest unpacked archive measured [Z5]. They are not a memory bound on the parser; v1 also holds every document
   of an accession at once.
   - The response may be at most 256 MiB. The new method streams it and stops at the first byte over.
   - The HTML members may total at most 256 MiB, counted as bytes actually read and stopping at the first byte over.
   - On either, the archive's buffers are released before anything else is read.
4. **Listing path.** The scan takes v4's listing path when the archive does any of these:
   - returns 404 or 410;
   - exceeds a limit;
   - is not a readable zip, fails a CRC check, or holds no member;
   - holds two members with one name, or a member name with a directory part.

   The listing path is `index.json`; then `FilingSummary.xml`, where a missing or unparseable summary excludes
   nothing; then every listed HTML file except the rendered pages, as bytes. B1 then applies to the listed files.
   Each fallback is counted by reason, and each generation records which path produced it. Any other failure of the
   archive request fails the attempt. A failed attempt leaves the active generation in force, as in v4.
5. **`documents`** (sql/514) counts the candidate files classified on the path that produced the generation. It is
   not comparable with v1's counts, which included EDGAR's index pages and every listed HTML file. The run result
   adds archive and listing-path counts and the request count.

### B3. Version

`PARSER_VERSION` becomes `dps-inline-scale-v2`.
- **Eligibility.** Under v4's rule every accession in the population is eligible again: `raw`, `history`, prior
  failures and the exhausted.
- **Readiness.** `work_state` counts an accession done only at the current version, so the 1,690 accessions done at
  v1 count as pending again, and Stage 1 readiness waits for their v2 scans.
- **Until rescanned,** an accession's v1 generation stays the active one that decisions read (v4 "Version bump"). A v2
  attempt that fails or exhausts leaves it so.
- **On success,** a v2 scan enqueues by the unchanged rules, including A4's candidate predicate. The drain stays off
  while the flag is off.

### B4. Acceptance (corpus rung)

1. **Manifest, before any network work.** In one snapshot the replay script writes, with the git SHA:
   - every accession with an active generation, with its scan row and every stored occurrence field (the baseline);
   - the seeded supplement: 100 accessions each from unscanned `raw`, `history`, and those with a failed attempt at
     v1, disjoint from the baseline and each other, all of an arm when it has fewer.

   Results are written per accession, so a run resumes. Every manifest accession ends with a result or an
   `incomplete` mark.
2. **Path invariance, every manifest accession.** The v2 code reads both sources and runs B1. Each accession records
   the archive's status. It passes when both sources give the same set (names) and the same occurrences.
   - Only an accession whose archive was read counts as invariance evidence.
   - 404/410 accessions are listed apart, with their listing-path result.
   - An accession with an `undecidable` candidate, on either source, is not counted as passing; it is listed.
3. **Against v1, every baseline accession.**
   - **No loss:** every stored occurrence is reproduced with the same document, element ordinal and every stored
     field, and every outcome is unchanged.
   - **Additions are the header-less members.** Every occurrence v2 adds lies in a set member with no `ix:header`.
   - **SEC's instance is the completeness oracle.** EDGAR writes an extracted instance (`{primary document}_htm.xml`,
     PDS §2.3.7) of every Inline submission: SEC's own reading of the whole set.
     - **It passes when,** for every manifest accession with a set, two sets are equal. One is the (`contextRef`,
       `unitRef`, value) of v2's resolved non-nil occurrences, with the instance value. The other is the concept's
       non-nil facts in that instance.
     - **A difference blocks.** Unresolved occurrences are listed beside it.
     - **The oracle does not depend on the parser.** It fails an expansion that adds nothing where SEC's instance has
       facts. It also fails an occurrence read with wrong fields.
     - **The [Z3] accessions are frozen in the replay** with their instance counts (see [Z3]).
   - **Any other difference blocks** unless it is one of these, each listed with its evidence:
     - a document v1 read that B1 does not count as a member (a prefixed or non-XHTML root);
     - a stored occurrence in an EX-FILING FEES or EX-98 document (B1.4);
     - a stored field changed by decoding, listed with both values and the document's declared encoding. It passes
       only if the v2 value is the text decoded by the declared encoding or BOM (XML 1.0 §4.3.3, Appendix F) and
       the v1 value is not;
     - an attempt that fails at v2 because a newly included member carries an Inline XBRL `target`.
4. **Completeness.** A transport error (connection, timeout, 429 or 5xx) is retried up to 3 times. An accession
   still failing is `incomplete`, and the replay passes only with none.

   The replay reports:
   - accessions with added occurrences;
   - added occurrences with `scale` > 0 or unresolved;
   - archive fallbacks by reason;
   - end-to-end seconds per accession.
5. **Pace, after deploy (monitoring with a decision rule).**
   - **Post on #3947** the first two v2 runs' attempted, succeeded, failed and fallback counts and their duration
     (`job_runs`).
   - **Report the work state at v2.** `raw` clearance and Stage 1 readiness are reported separately, because
     readiness also needs `history_candidate` and discovery.
   - **Project runs to Stage 1** as (pending `raw` + pending `history_candidate`) ÷ succeeded per run, stating the
     share that draining and discovery take once the flag is on. This is a lower bound. It omits population and
     candidate growth, retry backoff, and exhaustion, and each run note re-measures it.
   - **Decision rule.** If a run with draining off succeeds on fewer than 1,884 accessions, three times the 628 of the
     2026-10-22 01:54 UTC v1 run, the cause is found and posted before any other pace lever (concurrency, a second
     window) is considered.
6. **Fixtures.**
   - **The header-less case.** A multi-document archive whose header-less member holds two occurrences with known
     fields. It must give exactly those, per-document ordinals, and the same result from the archive and from the
     listing.
   - **`not_inline`.** A `not_inline` archive.
   - **Fallbacks to the listing path:**
     - 404 and 410;
     - a response and an HTML total each at the limit (read) and one byte over (fallback);
     - a CRC failure and a response that is not a zip;
     - an empty archive, duplicate names, and a directory member.
   - **A 500 on the archive.** The attempt fails.
   - **Classification:**
     - a UTF-16 member with a BOM, which is a member;
     - a malformed UTF-32 file with a BOM that contains `inlineXBRL` (`undecidable`);
     - a namespace declared through a character reference, which is a member;
     - an Inline XBRL prefix other than `ix`, which is a member;
     - an `.xhtml` and an `.html` candidate;
     - a malformed file that declares the namespace (`undecidable`), and one that does not (non-member);
     - a prefixed root and a non-XHTML root (non-members);
     - the namespace name in text with no Inline XBRL element (non-member);
     - an HTML `<a target>` (allowed), and an Inline XBRL element with `target` (fails).
   - **Types and sets:**
     - a member typed `EX-FILING FEES` (dropped);
     - members typed `8-K` and `EX-99.1` (both kept);
     - a failed header request, a missing type, two types in one record, a repeated filename, and a member the
       header does not list (each fails);
     - an accession whose only Inline XBRL documents are typed `EX-98`, and one whose only ones are typed
       `EX-FILING FEES` (each `not_inline`);
     - members with no `ix:header` (`no_ix_header`).
   - **Generation.** Every failure branch above leaves the active generation intact.
   - **DB:**
     - a v2 rescan of a v1-done accession of a candidate instrument enqueues it under A4's candidate predicate when
       neither its old nor its new generation holds a positive-scale or unresolved occurrence;
     - the existing revision-preservation and enqueue fixtures pass at v2;
     - a v2 failure and a v2 exhaustion each keep the v1 generation active, `work_state` counts them retrying and
       exhausted, and a flag-on normalisation reads the v1 occurrences;
     - a v2 success: `work_state` counts it done, and a flag-on normalisation reads the v2 occurrences.
7. **Cross-source check.** One [Z3] document's added occurrences are compared by hand with the document on EDGAR,
   and the comparison is recorded with its figures.

## Checkpoint record

- **Round 1** (Codex, 45 findings), applied in v2:
  - The source: DQC as a warning; restore made a declared policy with its risk and a case-by-case listing
    (findings 1–4 and 7). The EFC claim is now "in the measured snapshot" (5). Negative-scale defects are out of
    scope (6). EPS coverage is stated as partial (8).
  - Matching: entity and unit added (9, 10); namespace-aware parsing, the document set, and unresolved outcomes
    (11, 12, 15); per-occurrence provenance (13, 14, 16).
  - Discovery and states: an `index.json` fallback, `not_inline` only when confirmed, and retryable states (17,
    18, 30). Per-form handling is uniform, and amendments are scanned under their own accession (19). The
    cutoff is stated as deliberate (20).
  - The measurement script: an integer scale test, `raise_for_status`, aligned `USD/shares` predicates, a
    deterministic FY/YTD tie-break, and occurrence labelling (21–24, 26). The spec states which figures the
    script reproduces (25) and labels the budget illustrative (27).
  - Design: coverage at three levels (28, 39); an executable schema (29); scan generations (31); durable,
    reversal-aware re-normalising (32, 33); fresh filings ahead of the backlog (34); correction before every
    filter (35).
  - Acceptance and rollback: lineage-explained A/B, fixtures and downstream checks (36–38), and an ordered
    rollback (40).
  - The cents class: the wording on rules (41), overlap and threshold separated (42), the retain decision made
    explicit (43), the reopening condition (44), and the reader audit dated (45).
- **Round 2** (Codex): the declared-policy framing for restore is accepted, with no further change needed to
  it. 31 implementation findings, applied in v3:
  - Decisions: an ordered `(action, reason)` table, with unresolved occurrences accession-wide (findings 1, 2).
    The CIK is passed in explicitly (6), and the canonical key is defined (7).
  - Schema: raw attributes kept, unresolved fields nullable, an `INTEGER` scale with a lexical and range check,
    and an element ordinal (3–5, 28).
  - Discovery and states: a fact-level completeness fallback (8); retry backoff and exhaustion (9); failed
    attempts leave the active generation in force (10); a version bump keeps old generations active until
    rescanned (11).
  - The queue: an instrument-level queue that later scans never clear, with a generation-safe delete and a
    half-budget cap on draining (12–15).
  - Acceptance: a frozen population with an accounting identity (16); decisions off against on, on one snapshot,
    behind the flag (17); all facts reported against derivation-eligible ones (18); refusal confined to DPS value
    selection, with every other column held unchanged (19); a defined comparator (20); DB tests for the job (21);
    downstream checks covering refusals (22); the EFC checks relabelled and pinned (23).
  - Rollback: re-normalise every DPS instrument, check against decisions off on current inputs, and quiesce the
    job first (24–26).
  - Measurement: labels match what was measured (27); the WHR example is removed from the ordering rationale
    (29); the refusal rationale says "can" (30); the parser adoption test is the whole contract, not three
    examples (31).
- **Round 3** (Codex): the declared-policy framing stays accepted. 31 findings, applied in v4 without a fourth
  round:
  - Discovery: the document set now comes from `index.json` alone, with every inline file read as one set; a
    `target` attribute fails the attempt (findings 1, 2).
  - State: an `attempt_version` with a once-per-version reset, a persisted `next_attempt_at`, a `JobLock` with
    version-conditional publication, and active generation reported separately from work state (3–7).
  - Snapshot: coverage, A/B and downstream run in one rolled-back `REPEATABLE READ` transaction (8, 24).
  - The queue: a sequence-backed revision, a per-instrument advisory lock on every normaliser writer, ordered
    draining with an admission deadline and a failure count (9–11). Decisions live in the normaliser, so every
    normalisation path applies them (12).
  - Where decisions act: on DPS value selection after the structural filters, so period structure cannot change
    (13). An all-facts report sits beside the derivation subset (14). The function contract takes scans, CIKs and
    occurrences and returns a `Decision` (15). CIK matching is against all of the instrument's SEC CIKs, with
    `entity_mismatch` (16).
  - Parsing: units compared as expanded QNames (17); absent `scale` and `sign` take their defaults (18); the
    `xs:integer` lexical space with a separate bound (19); parser fixtures per contract clause (20); the cutoff as
    decision row 1 (21); the two failure classes (22).
  - Acceptance and rollout: exhausted accessions are unknown coverage (23); an enablement barrier with a full drain
    (25).
  - Rollback: every normaliser host restarted and in-flight writers ordered by the lock (26); one-snapshot
    verification, including downstream (27).
  - Measurement: regex-detected labels (28); a tie report for the FY/YTD selection (29); an empty-arm guard and
    `min(150, n)` (30); the script's request pacing documented, since it runs outside the jobs limiter (31).
- **Amendment 1, round 1** (Codex, 25 findings, 12 blocking), applied in the amendment's second draft:
  - Derivations: the per-input A3 became a leaf rule over the whole expression, with a fallback to the
    decisions-off derivation's own value (findings 2, 3); the YTD citation was corrected (25).
  - Queue and rollback: candidate rescans enqueue (4); rollback refills every population instrument (8);
    unreachable cells are rollback exceptions (12).
  - Refill: one locked transaction, with the sweep taking the same lock (5, 6); a failed refill never dequeues (7).
  - Population: a durable insert-only table fed by every write into raw, so discovery is not limited to
    `source_ref` (1, 9, 14, 16, 21); undated rows do not override an active generation (20); the archive-owner
    fallback is stated (22).
  - Enablement: a runtime negative gate citing DQC.US.0015 element 5748 (10, 18); Stage 2 checks stored
    convergence (11); coverage is read before the A/B, with undiscovered accessions listed (13); convergence
    conditions replace the one-cycle claim (17); the pace is labelled optimistic (23).
  - Overrides are named per subsection, and v4's guarantee is restated between the arms (19). The script uses the
    population's own dating (15). The retention wording covers amendments (24).
- **Amendment 1, round 2** (Codex, 13 findings, 7 blocking; round-1 findings 3–8, 14, 15, 18, 20 and 23–25 judged
  resolved), applied in the third draft:
  - Gating moved to a derivation group (a fiscal-year label's FY and Q1–Q4), so a derived Q4 always equals the
    cells beside it (8). `mixed_scale` keeps its raw value and is unverified, so both derivations select the same
    facts and NULLs. Leaves are the selected facts, and the resolver returns its combined facts (2).
  - The A/B records which value each cell took and checks each branch's invariant (3). It takes sorted locks before
    any refill write (1). It recomputes coverage inside its snapshot (5). `unreachable` is defined by the
    decisions-off result over refilled inputs (7).
  - A4 overrides only v4's enqueue predicate, and the other queue guarantees stand (4).
  - Discovery has its own table, retries and completion gate for each stage (6).
  - Convergence is stated to the gated result (9), and the fallback baseline is decisions-off over the same inputs
    (10).
  - A1 names the ordering it replaces and keeps v4's eligibility predicate (11). The script's `[A2]` label was
    corrected (12), and so was the archive-owner wording (13).
- **Amendment 1, round 3** (Codex, the cap; 10 findings, 5 blocking; round-1 findings judged resolved apart from
  2, 11, 12, 19 and 21, and round-2 findings apart from 2, 3, 6, 7 and 8). Applied without a fourth round, as v4
  did:
  - A group includes the label's stored rows, and an incomplete group keeps its stored DPS, so a normalisation over
    swept raw cannot pair regimes (1).
  - An alias-concept leaf is unverified, `other_concept` (2).
  - The A/B checks each group's expected branch and that every value equals its branch's result (3).
  - The queue keeps rows with a `drained_revision`, giving a durable ledger that scopes Stage 2 and rollback
    verification (4).
  - Structural deletions are excluded from `unreachable` (5).
  - New population membership on a scanned accession enqueues (6).
  - `discovered_at` is set only from a complete payload (7).
  - The A/B runs quiesced and retries a serialization failure (8).
  - v4's refusal fixture is replaced (9), and "permanently" became conditional (10).
- **Amendment 2, round 1** (Codex, 27 findings, 3 blocking), applied in the second draft:
  - Separate sets: EX-FILING FEES and EX-98 are separated by attachment type, not by duplicate ids (1, 2).
  - Classification: exact namespace names (3), EDGAR's exact rule with compatibility suffixes named (4),
    `not_inline` from classification alone (5), and no byte prefilter, with `undecidable` failing (6, 7).
  - Measurement: classification errors kept apart (8); occurrences compared, not only names (9); unmeasured claims
    removed (10); disjoint arms including history (11); timing labelled (12).
  - Limits, fallbacks and archive validity (13–15). Overrides scoped to acquisition (16, 23).
  - Acceptance: two-source path invariance (17); intended differences enumerated (18); completeness (19); a frozen
    manifest (20); fixtures (21); `documents` defined (22); enqueue, readiness and projection (24–26). Wording (27).
- **Amendment 2, round 2** (Codex, 17 new findings, 3 blocking; 14 of 27 judged resolved), applied in the third draft:
  - Every attempt reads the SGML header and types every member; separation no longer depends on header count (29, 1).
  - An unambiguous type map (31).
  - `target` limited to Inline XBRL elements (32).
  - Byte equality reported, not gated (33, 39).
  - Archive provenance required (34).
  - Pace given a decision rule (36).
  - Supplement manifest (37).
  - Measurement labels and accounting (38, 40, 43, 44).
  - CRC scope (41).
  - Sample wording (42).
  - Finding 28, the `<XBRL>` indicator: `-index-headers.html` was checked and does not carry it, so classification
    applies Guide §11, recorded under the source rule.
- **Amendment 2, round 3** (Codex, the cap; 11 new findings, 1 blocking; round-1 and round-2 findings judged resolved
  apart from 6, 8, 10, 12, 17, 20, 21, 24–26, 28, 30, 31, 35, 38, 39 and 40). Applied without a fourth round, as v4
  and Amendment 1 did:
  - The completeness oracle is EDGAR's extracted instance, not the regex census (45, 17, 35). The census stays a
    lower-bound measurement. The script now reports the instance comparison and the EFC control with its stored
    baseline (51).
  - Classification is one table, replacing v4's parse-failure clause for candidates. A damaged file that looks Inline
    is `undecidable` (6, 28, 55). Membership itself stays EDGAR's rule.
  - An empty primary set is `not_inline`, with fixtures (49). Separate sets are classified but take no part in
    resolution or extraction, with the reason and the matching consequence stated (30, 54).
  - Measurement: undecidable accessions are excluded from equality (8, 40). Timing is symmetric and labelled an
    acquisition benchmark (12, 46, 47). `None` is kept apart from `[]` (48). Script counts are per member (52), and
    byte figures cover non-empty comparisons only (53). The summary's HTTP errors propagate (38). Every member must
    map to exactly one type (31).
  - Fixtures, enqueue, readiness and projection wording (21, 24, 25, 26).
  - **Rebutted: 50.** A header-less member cannot change an existing occurrence's resolution: contexts and units sit in
    `ix:resources`, which must be a child of `ix:header` (Inline XBRL 1.1 §14.1.1, Table 15). Recorded under B1.5. A
    v2 failure on a new member's `target` is listed as an intended difference.
  - **Not applied: 20 and 39, for the measurement script only.** Its figures are dated sample measurements,
    reproducible by command and seed with the caveat stated; the replay's manifest (B4.1) is the frozen one.
