# DEF 14A bene-table extension — ESOP tagging + observations write-through

**Issue:** #843 (`feat(#788 P4): DEF 14A consolidated beneficial-ownership table extraction`)
**Phase:** Phase 4 of #788 epic / #842
**Date:** 2026-05-06
**Author:** Luke + Claude + Codex (round 1 design)

## Why

Phase 4 of the #788 ownership decomposition closes the ESOP overlay
loop opened by Phase 3 (#919 funds slice) and unlocks #961 (ESOP
overlay tag for funds slice). Operator chart currently surfaces
funds slice but has no signal to tag rows where a fund family is
the issuer's plan trustee. This spec defines the parser delta +
schema + write-through that produces that signal.

## What ships (OPT-B per Codex round 1)

1. **ESOP role tagging in DEF 14A parser.** Conservative regex on
   `holder_name`; rows matching get `holder_role='esop'` (in
   addition to existing `'officer' | 'director' | 'principal' |
   'group'` set). Regex set locked below.
2. **New `ownership_esop_observations` + `ownership_esop_current`
   tables.** Mirrors the partitioned + materialised pattern of
   sibling `ownership_*_observations` tables. Identity:
   `(instrument_id, plan_name, period_end, source_document_id)`.
3. **Write-through from DEF 14A ingester.** When parser emits an
   `'esop'`-tagged row, additionally `record_esop_observation` +
   `refresh_esop_current`. Existing `def14a_beneficial_holdings`
   row is preserved as-is (audit trail).
4. **5+ golden-file fixtures.** AAPL, MSFT, JPM, GME, HD (panel)
   plus 1 ESOP-heavy small cap (issuer with explicit plan trustee
   row crossing 5%) — confirms the regex actually catches real
   rows.
5. **Silent-skip path** — already in. Confirmed via existing
   `def14a_ingest_log` tombstone semantics.

## What is explicitly OUT (deferred follow-ups)

- **Cross-source augment of `ownership_insiders_observations`** —
  using DEF 14A officer rows as backup insider source when Form 4
  is absent/stale. Codex round-1 confirmed this is the right move
  (DEF 14A as backup source ranked BELOW Form 4 in the priority
  chain, never additive math) but it's a separate write-through
  surface. New ticket: TBD post-merge.
- **Cross-source augment of `ownership_blockholders_observations`**
  — DEF 14A 5%-holder rows enriching the blockholders slice when
  the holder filed a 13G the ingester missed. Same defer rationale.
  New ticket: TBD post-merge.
- **DEF 14A vs Form 4 cumulative drift detector** — original #769
  PR3 plan. Genuine separate-feature scope. Already filed as the
  drift-detector path in `app/services/def14a_drift.py`; this PR
  doesn't extend it.

These three items together are the "Phase 4.B" follow-up ticket
filed alongside this PR's merge.

## Locked regex set (Codex round 1)

Case-insensitive matching on `Def14ABeneficialHolder.holder_name`:

```regex
\bESOP\b
\bemployee stock ownership plan\b
\b401\s*\(?k\)?\b
\bemployee savings plan\b
\bretirement savings plan\b
\bprofit[-\s]sharing plan\b
\bemployee benefit plan\b
\bcompany stock fund\b
\b(?:savings|retirement|profit[-\s]sharing)\s+plan\s+trust\b
```

**Explicitly NOT matched** (false-positive guard): generic `trust`,
`trustee`, `trustee for` alone — these surface on every Vanguard
Group / BlackRock / institutional row in the 5%-holders block and
would over-tag.

The regex set lands as a `frozenset[re.Pattern]` constant in
`app/providers/implementations/sec_def14a.py`; the parser walks
it once per row and tags the first match.

## Schema

`sql/127_ownership_esop.sql` (next slot after #963's `126_*`).

```sql
-- ownership_esop_observations — append-only fact log for ESOP /
-- employee benefit plan holdings extracted from DEF 14A bene tables.
CREATE TABLE ownership_esop_observations (
    instrument_id           INTEGER NOT NULL,
    plan_name               TEXT NOT NULL,
    plan_trustee_name       TEXT,            -- e.g. "Vanguard Fiduciary Trust" extracted from holder_name suffix
    plan_trustee_cik        TEXT,            -- resolved via holder_name_resolver when possible
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'beneficial'),

    -- Provenance block (uniform across every ownership_*_observations).
    source                  TEXT NOT NULL CHECK (source = 'def14a'),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_field            TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_start            DATE,
    period_end              DATE NOT NULL,
    known_from              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    known_to                TIMESTAMPTZ,
    ingest_run_id           UUID NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Fact payload.
    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    percent_of_class        NUMERIC(8, 4),

    PRIMARY KEY (instrument_id, plan_name, period_end, source_document_id)
) PARTITION BY RANGE (period_end);

-- Quarterly partitions 2010-2030 (mirrors 123_ownership_funds.sql).

CREATE TABLE ownership_esop_current (
    instrument_id           INTEGER NOT NULL,
    plan_name               TEXT NOT NULL,
    plan_trustee_name       TEXT,
    plan_trustee_cik        TEXT,
    ownership_nature        TEXT NOT NULL CHECK (ownership_nature = 'beneficial'),
    source                  TEXT NOT NULL CHECK (source = 'def14a'),
    source_document_id      TEXT NOT NULL,
    source_accession        TEXT,
    source_url              TEXT,
    filed_at                TIMESTAMPTZ NOT NULL,
    period_end              DATE NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shares                  NUMERIC(24, 4) NOT NULL CHECK (shares > 0),
    percent_of_class        NUMERIC(8, 4),
    PRIMARY KEY (instrument_id, plan_name)
);
```

`plan_name` is the canonicalised form (lowercased + trimmed)
extracted by stripping the trustee suffix. Example:
- `holder_name = "Apple Inc. 401(k) Plan, c/o Vanguard Fiduciary Trust as Trustee"`
- `plan_name = "Apple Inc. 401(k) Plan"`
- `plan_trustee_name = "Vanguard Fiduciary Trust"`
- `plan_trustee_cik = resolver lookup against trustee_name → e.g. "0000933478"`

## Service layer

New file `app/services/esop_observations.py` with:
- `record_esop_observation(conn, *, instrument_id, plan_name, ..., shares, percent_of_class, ...)` — append-only insert with the full provenance block + write-side guards.
- `refresh_esop_current(conn, *, instrument_id)` — deterministic
  rebuild via DELETE + INSERT pattern matching
  `refresh_funds_current` from `ownership_observations.py`.

DEF 14A ingester at `app/services/def14a_ingest.py` extends
`_ingest_single_accession`: when parsed row's `holder_role == 'esop'`,
also call `record_esop_observation` + `refresh_esop_current` (same
pattern as the existing `record_def14a_observation` write-through).

## Rollup integration

NOT in this spec. The funds-slice ESOP overlay tag (operator-visible
chart change) lands in #961 once this spec ships rows to
`ownership_esop_current`. #961 reads from this table, joins against
funds-slice rows by `plan_trustee_cik = fund_filer_cik`, tags
matches with `esop_plan=true`.

This separation respects the "fix-in-scope" rule (#843 is the
parser+observations layer; #961 is the rollup-consumer layer).

## Source priority chain (no change)

The existing chain stays:
```
Form 4 > Form 3 > 13D/G > DEF 14A > 13F-HR > N-PORT
```

ESOP holdings live in their own slice (`ownership_esop_observations`)
and don't compete in the cross-source dedup. The funds-slice overlay
in #961 is a memo overlay (denominator_basis="institution_subset"
per #919's pattern); ESOP doesn't change residual math.

## Test plan

### Unit tests (parser)

`tests/test_def14a_parser.py` extension. New test class
`TestEsopRoleInference`:
- Each of 9 regex patterns matches a representative `holder_name`.
- Generic `Trust`, `Trustee`, `Trustee for` alone do NOT match.
- `'Apple Inc. 401(k) Plan'` → `holder_role='esop'`.
- `'Vanguard 500 Index Fund'` → `holder_role` stays as parser-inferred (NOT `'esop'`).

### Golden-file fixtures

`tests/fixtures/sec/def14a/<accession>.html` — 6 real proxy
HTML files:
- `aapl_def14a_2026.html` — large-cap, no ESOP
- `msft_def14a_2025.html` — large-cap, no ESOP
- `jpm_def14a_2026.html` — large-cap, no ESOP
- `gme_def14a_2025.html` — meme-stock, no ESOP
- `hd_def14a_2026.html` — large-cap, no ESOP
- `<small_cap_with_esop>_def14a.html` — small cap with explicit
  plan trustee row crossing 5% (TBD: identify via SEC EDGAR
  full-text search for "401(k) Plan" in Item 12 disclosures)

`tests/test_def14a_parser_golden.py` — `parametrize` over the 6
fixtures; assert: parse succeeds, holder count > 0, panel issuers
have ≥5 expected holders, ESOP fixture surfaces at least one row
with `holder_role='esop'`.

### Integration tests (write-through)

`tests/test_esop_observations.py`:
- `record_esop_observation` round-trips through `_observations` +
  `_current` tables.
- Re-ingesting same accession is idempotent (`refresh_esop_current`
  picks the latest filing per plan).
- DEF 14A ingester end-to-end test: seed a `filing_events` row +
  fixture HTML; assert `ownership_esop_current` populates.

### Pre-push gates

- `uv run ruff check . + ruff format --check . + pyright`
- `uv run pytest tests/test_def14a_*.py tests/test_esop_observations.py -p no:testmon`
- `pnpm --dir frontend typecheck` (no FE changes expected; verify)

## ETL DoD clauses 8-12

- Smoke against panel: re-run `sec_def14a_ingest` for AAPL/MSFT/JPM/HD/GME after deploy. Confirm zero ESOP rows surface (expected — large caps don't cross threshold). Confirm small-cap ESOP fixture issuer (if SEC-discoverable) surfaces ≥1 row.
- Cross-source verify: spot-check 1 ESOP row against the source proxy filing on EDGAR.
- Backfill executed: `POST /jobs/sec_def14a_bootstrap/run` against panel issuers.
- Operator-visible verification: deferred to #961 (this spec ships data layer; #961 ships chart layer).

## Codex sign-off

- Round 1 (this spec, locked OPT-B): see `.claude/codex-843-r1-review.txt`.
- Round 2 (pre-push diff review): mandatory per CLAUDE.md checkpoint 2.

## Decisions explicitly settled

| | Decision | Rationale |
|---|---|---|
| Parser engine | Hand-rolled (existing `sec_def14a.py`) | EdgarTools' `ProxyStatement.beneficial_ownership` adds zero new categorization signal; would just add a dependency path with divergence surface |
| ESOP detection | holder_name regex (9-pattern conservative set) | False-positive guard against generic Trust/Trustee already surfacing for every institutional 5%-holder |
| New table vs reuse | New `ownership_esop_observations` + `_current` | Mirrors sibling-table pattern; ESOP plans have distinct identity (plan_name + plan_trustee) that don't fit the def14a_beneficial_holdings shape cleanly |
| Cross-source augment | DEFERRED | Genuine separate feature; would 3x this PR's scope per Codex |
| Drift detector | DEFERRED | Already exists in `def14a_drift.py`; not extending in this PR |
| Insider rank for DEF 14A | Backup source BELOW Form 4, never additive | Codex round 1 |

## Estimated session cost

~1 session (parser regex + new schema + service + 6 fixtures + tests + write-through). Smaller than the issue's original full-fat scope because cross-source augment + drift were genuinely separate features, not parts of the parser PR.
