# Institutional family identity — count each manager family once (#1644 + #1649)

**Status:** proposed (unshipped)
**Issues:** #1644 (proxy 5%-holder double-count) + #1649 (13F family undercount) —
collapsed into one fix per operator decision 2026-06-15 ("build family identity now").
Part of epic #788. Builds on #1639 (13F-NT supersession, merged `080f8107`) and
#1640 (one-owner-once, merged `f8321efb`).
**Area:** `app/services/ownership_rollup.py` + new `app/services/institutional_families.py`
(pure service; **no schema change**, read-path only).

## Problem

The DEF 14A "Security Ownership of Certain Beneficial Owners" 5%-holder rows for
institutional managers are added to the rollup as an **additive `def14a_unmatched`
pie wedge** on top of the 13F institutions slice. The committee framed this as a
pure double-count (#1644). Dev evidence (AAPL, instr 1001) shows it is **two
opposite bugs that share one root cause — missing institutional family identity**:

| Family | proxy (def14a) | 13F in rollup (post-#1639) | reality (~%) | today | correct |
|---|---|---|---|---|---|
| Vanguard | 1,415,826,462 | 1,436,449,741 (10 sub-CIKs summed) | ~9% ≈ 1.33B | 13F **+** proxy ≈ 2.85B counted (**~2× over**) | once @ 1.436B |
| BlackRock | 1,043,713,019 | 6,122,822 (lone shell CIK) | ~7% ≈ 1.04B | 13F 6.1M **+** proxy 1.04B (proxy is the *only* real figure) | once @ 1.043B |

Both proposed fixes in #1644 are falsified by this data:

- **Resolve proxy name → single CIK**: the proxy figure is a *family-consolidated
  13G beneficial total* (SEC Reg S-K Item 403 sources the 5%-owner table from the
  filer's consolidated Schedule 13G/13D). No single 13F CIK equals it — Vanguard
  files under 11 CIKs, BlackRock under ~50. Names do not even normalise-match
  (`"The\xa0Vanguard\xa0Group"` vs `"VANGUARD GROUP INC"`; 468 instruments carry
  proxy names with the street address concatenated, e.g.
  `"The Vanguard Group 100 Vanguard Blvd. Malvern, PA 19355"`).
- **Drop institutional-named proxy rows**: deletes BlackRock's 1.043B — the only
  complete BlackRock figure we have (13F captures 6.1M; the real iShares/BFA
  sub-books, historic CIK `1364742`, are absent from ingestion). The issue's
  52.4B blast-radius estimate over-counts: it scored BlackRock-type *gap-fills* as
  double-counts.

Whether a proxy institutional row is a double-count (drop) or a gap-fill (keep)
**cannot be decided per-CIK** — it depends on whether the family's true position
is already captured in the 13F slice. That is family identity.

### Why #1639 makes the 13F side already-correct

Post-#1639, Vanguard's overlapping *parent* CIK (`VANGUARD GROUP INC`, 1.426B,
which double-reported the sub-entities' books) is NT-suppressed. The 10 surviving
Vanguard sub-CIKs are **disjoint** and **sum** to 1.436B ≈ the true position. So
the 13F institutions contribution per family is already right; we do **not** touch
13F aggregation. The remaining bug is purely the proxy wedge added on top.

## The single rule (canonical)

> **An institutional manager family (Rule 13d-1(b) reporting group) is one owner,
> counted exactly once, at the MAX of its channel estimates. The channels are:
> (a) the SUM of the family's surviving 13F-HR holdings, (b) its consolidated DEF
> 14A proxy 5%-beneficial figure, (c) its consolidated Schedule 13G/13D figure.
> 13G/proxy are family-consolidated totals; per-CIK 13F figures are disjoint
> slices that sum within the family. The family beneficial total is the MAX across
> (a)/(b)/(c) — never their sum. Losing channels become `dropped_sources` +
> a `corrections_applied` entry.**

This is the family-level generalisation of #1640's one-owner-once rule (which
operates per-CIK). #1640 already collapses a single CIK across its filing
channels; this collapses the *set of CIKs + name-variants* that constitute one
manager family.

### Applied to the evidence

- Vanguard: MAX( ΣΣ13F=1.436B, proxy=1.415B ) = **1.436B**; proxy folded to a
  dropped source + `corrections_applied` kind `def14a_restates_institution`.
- BlackRock: MAX( Σ13F=6.1M, proxy=1.043B ) = **1.043B**; the 6.1M 13F holder
  folded to a dropped source + correction. Gap filled with no new ingestion.

## Family registry (the identity mechanism)

A **curated, versioned, in-code registry** — `app/services/institutional_families.py` —
not a table (no migration), not auto-detection (fragile; eBull prefers
deterministic + auditable, CLAUDE.md). The universe of managers that appear as
>5% proxy holders is small and stable (the Big Three dominate; ~10–20 families
cover the vast majority of the 855 affected instruments).

```python
@dataclass(frozen=True)
class InstitutionalFamily:
    family_id: str            # stable slug, e.g. "vanguard"
    display_name: str         # "The Vanguard Group"
    name_patterns: tuple[str, ...]   # lowercased substrings; address-robust
    ciks: frozenset[str]      # known 10-digit CIKs (convenience; name-pattern is the safety net)
    bucket: Literal["institutions", "etfs"]  # parent-entity nature (Codex F9)

FAMILIES: Final[tuple[InstitutionalFamily, ...]] = (...)
```

Membership resolution (pure function, table-tested) — applied to **both** 13F
holders and proxy/13G rows so a CIK and a name-variant land in the same family:

1. **By CIK** — a holder whose `filer_cik ∈ family.ciks`.
2. **By name pattern** — substring match of `normalise_name`-stripped lowercased
   name against `family.name_patterns` (robust to trailing addresses and `\xa0`).
   Patterns are specific (`"vanguard"`, `"blackrock"`, not `"van"`).
3. No match → the holder is its **own singleton family** (today's behaviour;
   zero regression for no-match rows).

**Name-pattern membership is load-bearing, not just a convenience** (Codex F5):
the curated `ciks` set is necessarily incomplete (Vanguard files under 11+ CIKs,
new sub-entities appear quarterly). If membership were CIK-only, an unlisted
`VANGUARD …` 13F CIK would stay a *separate* institutions holder while the proxy
is counted once inside the family → residual double-count. Resolving 13F holders
by name pattern as well closes that gap: any `VANGUARD …`-named 13F filer joins
the Vanguard family regardless of whether its CIK is curated.

**Registry validation invariant** (Codex F4/F6) — enforced at import time and by a
test, fail-closed:
- no CIK appears in two families;
- no two families share a name pattern, and no pattern is a substring of another
  family's pattern (no ambiguous overlap);
- a name that matches **>1** family resolves to **singleton** (not an arbitrary
  family) and logs a WARN — a false-positive must never silently relocate a holder.

The registry is the single source of truth for both the 13F-CIK side and the
proxy-name side, so they cannot drift (same lesson as `holder_name_resolver`).

## Read-path integration

Insertion point (Codex G1): **before** `_reconcile_owner_once`, operating on its
raw inputs — the per-(CIK, source) survivors from `_dedup_by_priority` /
`_dedup_within_source` plus the `unmatched_def14a` proxy candidates. Running it
*after* owner-once is too late: that pass has already MAX-folded each CIK across
its channels and buried the losing channel figures in `dropped_sources`, so the
raw `f_13f` / `f_proxy` / `f_13g` channel figures (and clean correction
provenance) are no longer recoverable. New pre-pass `_reconcile_institutional_families`:

1. Partition the survivors + blockholders + proxy candidates into
   **family-member** rows (resolve to a curated family by CIK or name pattern) and
   **the rest**. The rest flow into `_reconcile_owner_once` unchanged → zero
   regression for non-curated holders.
2. For each curated family, gather its member rows across all three channels
   (13F survivors, 13G/13D blockholders, proxy candidates), removing them from the
   inputs the downstream passes see.
3. Compute the three **channel figures** for the family, after dropping invalid
   rows (see sanity guard — reject, never cap):
   - `f_13f`  = **Σ** the family's surviving 13F holdings (disjoint sub-books;
     post-#1639 the overlapping parent is already NT-suppressed).
   - `f_proxy` = **MAX** of the family's proxy rows *for this instrument* (they
     restate one consolidated 13G figure; per-instrument there is normally one —
     MAX guards against a parser dup / amendment, Codex F7).
   - `f_13g`  = **MAX** of the family's 13G/13D rows for this instrument.
4. Family figure = `MAX(f_13f, f_proxy, f_13g)`. The argmax channel is the
   `winning_source`.
5. Emit **one** family holder at the family figure. Audit trail distinguishes two
   kinds (Codex F10 — aggregation is not suppression):
   - **Within-channel aggregation** (the family's N summed 13F sub-CIKs) → recorded
     as a non-removing **breakdown** (sub-CIK rows preserved for the L2 table); NOT
     a `dropped_source` (no shares were removed — they were summed *into* the figure).
   - **Cross-channel fold** (a losing channel, e.g. proxy folded under `f_13f`) →
     a `dropped_sources` entry at that channel's figure **and** a
     `corrections_applied` entry with `shares_removed = losing_channel_figure`.
6. The `corrections_applied` entry makes the shrunk proxy wedge / grown
   institutions figure explainable (the #1647 contract).

Proxy rows that do **not** resolve to a curated family stay in the
`def14a_unmatched` wedge exactly as today (named officers, small holders).

### Sanity guard (load-bearing, applies to EVERY row entering the family MAX)

Applies to every consolidated-beneficial channel row that feeds a family figure —
proxy **and** 13G/13D, and the 13F rows that feed the `f_13f` sum (Codex G3: a
garbage 13G or 13F row would otherwise win/inflate the figure and detonate the
rollup just as a garbage proxy row would).

`ownership_def14a_current` carries parser-garbage share values on 3 instruments
(LAMR `Kevin P. Reilly` = 48.3 *trillion* ≈ 300,000× outstanding; GEF/GEF.B
director-group rows in the hundreds-of-billions). A garbage value would **win the
family MAX and detonate the rollup** (massively oversubscribed).

**Reject (never cap)** an invalid channel row *before* the MAX (Codex F1 — capping
to `shares_outstanding` would fabricate a 100%-owner and still win the MAX). A
rejected row is dropped from its channel with a WARN-with-provenance log; the MAX
runs over the surviving valid rows.

Reject predicate (Codex F2 — denominator-aware, not blunt): reject when
`shares > shares_outstanding` using **the same denominator the rollup already
uses** for this instrument (`_read_shares_outstanding`, post `_denominator_too_stale`
gate). Rationale: no single beneficial owner can hold >100% of the class. The known
soft spot is **dual-class** issuers, where a holder's shares in one class can exceed
another class's count — precise per-class denominators are **#1646's scope**; here
the guard uses the instrument-level denominator the rest of the rollup trusts, and
the 300,000× garbage is caught by any sane threshold. Pure helper, table-tested.
This incidentally fixes the 3 broken instruments. (Root-cause DEF 14A name/value
parser cleanup is a **non-goal** — filed as follow-up N1; see below.)

### Known limitation: stale-channel overcount (Codex F3/G2, tied to #1648)

`MAX` defends against the dominant failure (undercount — BlackRock). The residual
risk is the opposite: a **stale** proxy/13G figure *larger* than the current 13F
because the manager sold between the proxy as-of date and the latest 13F quarter →
MAX keeps the stale-larger figure → mild overcount.

**v1 keeps pure `MAX(f_13f, f_proxy, f_13g)`** — no as-of tie-break heuristic
(Codex G2: a "prefer-newer within ~10%" rule would let the family figure fall
*below* the MAX, breaking the canonical invariant and inviting divergent
implementations). The as-of gap **is** recorded in the `corrections_applied`
entry (`detail` carries both channels' `as_of`) so a consumer can see the staleness
even though v1 does not act on it. Full per-figure quarter-coherence is **#1648's**
contract. The v1 posture deliberately favours "never undercount a known major
holder" — the BlackRock undercount reads far worse to a decision agent than a
one-quarter-stale MAX, and the invariant stays simple and testable.

## `corrections_applied` contract generalisation (#1647)

The `CorrectionApplied` dataclass (rollup.py:92) is currently NT-specific
(`superseded_period`, `winning_nt_period`, `winning_nt_accession` are all
required). A `def14a_restates_institution` correction has no NT quarters. Generalise:

- Keep `kind`, `filer_cik` (nullable now — a proxy-name-only fold has no CIK),
  `filer_name`, `shares_removed`.
- Make the NT-specific fields `Optional`. Add generic, non-lossy identifiers
  (Codex F8): `family_id: str | None`, `source_channel: SourceTag` (the *losing*
  channel removed), `winning_source: SourceTag`, `winning_accession: str | None`,
  `detail: str`.
- Closed `kind` vocab grows to: `suppressed_by_13f_nt`,
  `def14a_restates_institution` (proxy folded under a larger 13F family sum),
  `institutional_family_collapse` (13F shell folded under a larger proxy/13G — the
  BlackRock gap-fill).

API + FE consume the same generalised shape (the #1639 `corrections_applied[]`
plumbing already exists in `app/api/instruments.py`).

## Edge cases

- **Family with only 13F, no proxy/13G** → no proxy to fold; behaviour unchanged
  (sub-CIKs sum in institutions as today). Family-collapse fires only when a
  consolidated proxy/13G figure exists *or* the family has >1 curated CIK to
  collapse. (Codex ckpt-1: confirm we never MAX-collapse disjoint 13F sub-books
  into one — that would undercount; the SUM-then-MAX-against-proxy order prevents it.)
- **Category attribution** (Codex F9): a manager *family* parent (Vanguard Group,
  BlackRock Inc.) is an **institution**, not an ETF — the proxy/13G consolidated
  filer is the parent entity. Bucket the collapsed family holder by the **parent
  family's nature** (institutions for the Big Three; `filer_type` on the curated
  family), NOT by the argmax channel's `filer_type`. This prevents an
  institution-class proxy winning the MAX from erasing a constituent ETF slice.
  Constituent ETF sub-books (e.g. individual iShares funds) that were *separately*
  in the etfs slice and are NOT part of the curated parent family stay in etfs
  untouched. Curated families carry an explicit `bucket: 'institutions' | 'etfs'`.
- **Dirty proxy names** (468 instruments, address-concatenated): tolerated —
  substring family match still works; non-family dirty names render as today.
- **Proxy figure < 13F family sum** (Vanguard): 13F sum wins the MAX; proxy folds.
- **Proxy figure > 13F family sum** (BlackRock): proxy wins; 13F holder(s) fold.

## Non-goals (filed as follow-ups)

- **N1 — DEF 14A parser cleanup**: strip street addresses + footnote daggers
  (`† º`) from `holder_name`; sanity-bound `shares` at parse time. 468 dirty
  names + 3 garbage-value instruments. Read-path sanity guard covers the
  *correctness* risk now; the parser fix is cosmetic + upstream hygiene.
- **N2 — BlackRock CIK `1364742` ingestion gap**: the real iShares/BFA 13F is
  absent from ingestion. Family MAX fills the figure from the proxy, so the rollup
  is correct without it, but the 13F detail is incomplete. Ingestion is a separate
  coverage ticket.
- Family registry coverage beyond the curated ~10–20 mega-managers.

## L2 filer-table display (operator-decided 2026-06-15)

**RESOLVED → keep the sub-CIK breakdown.** The collapsed family renders as one
family row at the family figure, expandable to its constituent 13F sub-CIK rows
(the within-channel aggregation breakdown) plus the folded proxy/13G channels as
`dropped_sources`. The operator still sees which entities filed and which channels
were folded. Pie/residual/concentration math uses the single family figure.

## Definition of done

- Pure-logic tests on `institutional_families` resolution + the family-MAX-once
  pass (Vanguard fold, BlackRock gap-fill, garbage-value rejection, singleton
  passthrough, ETF bucketing).
- Dev-verify panel AAPL/GME/MSFT/JPM/HD: institutions figure for each Big-Three
  family renders ONCE; proxy wedge no longer double-adds; LAMR/GEF no longer
  oversubscribed.
- Cross-source: AAPL Vanguard ~1.43B and BlackRock ~1.04B vs an independent
  source (e.g. WSJ/Nasdaq institutional-holders page, gurufocus).
- `corrections_applied[]` carries a `def14a_restates_institution` /
  `institutional_family_collapse` entry per fold, verified on the live endpoint.
- No migration / no backfill (pure read-path) → no jobs-proc restart, no
  `sec_rebuild`.
