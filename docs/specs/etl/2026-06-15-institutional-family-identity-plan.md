# Implementation plan — institutional family identity (#1644 + #1649)

Spec: `docs/specs/etl/2026-06-15-institutional-family-identity.md` (signed off
2026-06-15; L2 display = keep sub-CIK breakdown). Pure read-path, no migration.

## Representation decision (drives every task)

**Uniform collapse.** Each curated family that has any member rows for an
instrument collapses to **one** `Holder` at the family figure
`MAX(f_13f, f_proxy, f_13g)`, classified by the family's `bucket`
(institutions/etfs). The constituent rows are carried on a new **display-only**
field `Holder.family_members: tuple[FamilyMember, ...]` for the L2 breakdown — they
are NOT re-summed into residual/concentration (the family holder's `shares` already
IS the figure). This matches the signed-off display (family row expandable to
sub-CIK children) for both the Vanguard (Σ13F wins) and BlackRock (proxy wins)
shapes uniformly.

- `f_13f` = Σ of the family's surviving (sanity-passed) 13F holder shares.
- `f_proxy` = MAX of the family's sanity-passed proxy candidate shares.
- `f_13g`  = MAX of the family's sanity-passed 13G/13D holder shares.
- The argmax channel is `winning_source`; every other present channel is a
  cross-channel **fold** → `dropped_sources` entry (its channel figure) + a
  `corrections_applied` entry (`shares_removed = folded channel figure`).
- `family_members` = the constituent 13F sub-CIK rows (the within-channel
  aggregation breakdown — NOT folds, no shares removed).

## Tasks

### T1 — `app/services/institutional_families.py` (new, pure)

- `InstitutionalFamily` frozen dataclass: `family_id, display_name, name_patterns,
  ciks, bucket`.
- `FAMILIES` tuple — seed the mega-managers observed in dev proxy data:
  vanguard, blackrock, state_street, fidelity (FMR), geode, t_rowe_price,
  capital_group, morgan_stanley, jpmorgan, goldman_sachs, northern_trust,
  bank_of_america, wellington, invesco, charles_schwab. (Curated from the
  `ownership_def14a_current` >5% holder distribution; extendable.)
- `resolve_family(filer_cik, filer_name) -> InstitutionalFamily | None` — CIK
  membership first, then name-pattern (`normalise_name`-stripped, lowercased,
  substring), fail-closed on >1 match (return None + WARN).
- `_validate_registry()` run at import: no CIK in two families; no shared /
  substring-overlapping name pattern. Raises on violation (fail-closed).
- Reuse `normalise_name` from `holder_name_resolver` (single source of truth — do
  NOT re-implement the strip).

### T2 — rollup model (`ownership_rollup.py`)

- New frozen `FamilyMember(filer_cik, filer_name, shares, source, accession,
  edgar_url, as_of_date)` — display-only breakdown row.
- Add `family_members: tuple[FamilyMember, ...] = ()` to `Holder`.
- Generalise `CorrectionApplied` (Codex F8): `filer_cik: str | None`; NT fields →
  `Optional`; add `family_id: str | None`, `source_channel: SourceTag | None`
  (the folded/losing channel), `winning_source: SourceTag | None`,
  `winning_accession: str | None`, `detail: str = ""`. Existing `suppressed_by_13f_nt`
  producer (`_read_notice_suppressions`) updated to set the new fields (winning =
  none-folded; source_channel='13f'; family_id=None) — keep its behaviour identical.

### T3 — sanity guard + family pre-pass (`ownership_rollup.py`)

- `_proxy_value_is_sane(shares, outstanding) -> bool` — `shares <= outstanding`
  (pure, table-tested). Apply to every row entering a family channel figure (Codex
  G3) AND to the residual `unmatched_def14a` wedge rows (kills LAMR/GEF garbage).
- `_reconcile_institutional_families(survivors, blockholders, unmatched_def14a,
  outstanding) -> (family_holders, rest_survivors, rest_blockholders,
  rest_unmatched, corrections)`:
  1. Resolve every survivor/blockholder/proxy row to a family via `resolve_family`.
  2. Group family-member rows by `family_id`; everything else passes through to
     `rest_*` unchanged (zero regression for non-curated holders).
  3. Per family: drop sanity-failing rows; compute `f_13f/f_proxy/f_13g`.
     **Skip-collapse guard (Codex G-fix):** collapse UNLESS the family has
     *exactly one surviving row total in exactly one channel* (nothing to
     reconcile — leave that lone row in `rest_*`). NB "member" = any surviving row
     in any channel, not just 13F — two duplicate proxy rows with no 13F still
     collapse (else they'd stay additive in `def14a_unmatched`).
     Else emit one family `Holder`:
       - `shares` = family figure; `winning_source` = argmax channel.
       - `filer_type` from family `bucket`.
       - `winning_accession` (Codex contract gap): the accession of the **largest
         single row in the winning channel** (representative link; a 13F-sum winner
         has no single accession). `winning_edgar_url` from that accession.
       - `as_of_date` (Codex Q5): the winning channel's as-of. For a 13F-sum
         winner, `min(member.as_of_date)` when members span quarters (conservative;
         never `max` — that overstates freshness), with the range in the
         correction `detail`.
       - `family_members` = the constituent 13F sub-CIK rows (display breakdown).
       - `dropped_sources` = one per *folded* (losing) channel at its channel figure.
     plus a `corrections_applied` entry per folded channel.
- Wire into `compute` (rollup.py:1598): run the pre-pass, then
  `_reconcile_owner_once(rest_survivors + rest_blockholders)`, inject
  `family_holders` into `by_category[family.bucket]`, pass `rest_unmatched` (also
  sanity-filtered) to `_bucket_into_slices`, append family corrections to
  `corrections_applied`.
- `filer_count` honesty: `_build_slice` counts a family holder as
  `1 + len(family_members)` so coverage % is not deflated by collapse.
- **`_build_slice` must propagate `family_members`** (Codex integration gap):
  `_build_slice` rebuilds every `Holder` (rollup.py:1121) — the rebuild currently
  drops any field not explicitly copied. Add `family_members=h.family_members` to
  the reconstruction or the API/FE never see the breakdown.
- **Do NOT run the family pre-pass over `funds_holders`** (Codex Q4): N-PORT funds
  are collected separately and appended as the `institution_subset` memo overlay
  (rollup.py:1093); family reconciliation operates only on survivors + blockholders
  + `unmatched_def14a`.

### T3b — CSV export (Codex CSV gap, `ownership_rollup.py` CSV builder ~1757)

The CSV currently emits top-level holders + dropped_sources + corrections memo
rows. A collapsed family's `family_members` (13F sub-CIK breakdown) would vanish
from CSV. Emit each `family_member` as a memo row under its family parent
(`__family_member:<family_id>__` prefix, mirroring the `__suppressed_by_13f_nt:__`
memo convention) so the raw export keeps the breakdown the L2 table shows.

### T4 — API (`app/api/instruments.py`)

- Add `family_members` to the holder response model; generalise the
  `_CorrectionAppliedModel` (nullable NT fields + new generic fields; Literal vocab
  grows to `def14a_restates_institution`, `institutional_family_collapse`).
- Keep `suppressed_by_notice` count working.

### T5 — Frontend

- Holder type + render: a holder with `family_members` renders as an expandable
  family parent row (subtotal = holder.shares) with the sub-CIK children; folded
  channels shown via `dropped_sources` as today.
- Corrections panel: handle the two new `kind`s (copy strings).

### T6 — tests (pure-logic first)

- `tests/test_institutional_families.py`: resolution (CIK, name-pattern, dirty
  name w/ address + `\xa0`, no-match singleton, >1-match fail-closed), registry
  validation (dup CIK / overlapping pattern raise).
- `tests/test_ownership_rollup.py` additions: Vanguard fold (Σ13F wins, proxy →
  correction, members preserved), BlackRock gap-fill (proxy wins, 13F folded),
  garbage-value rejection (LAMR-shape), singleton passthrough (non-curated holder
  unchanged), ETF bucket, filer_count honesty.
- API shape test for the generalised corrections + family_members.

### T7 — dev-verify + follow-ups

- DoD 8–12: AAPL/GME/MSFT/JPM/HD panel via `/instruments/{symbol}/ownership-rollup`;
  Vanguard once ~1.436B, BlackRock once ~1.043B; LAMR/GEF no longer oversubscribed;
  cross-source AAPL Vanguard/BlackRock vs Nasdaq/WSJ holders; corrections present.
- File follow-ups: **N1** DEF 14A name/value parser cleanup, **N2** BlackRock 13F
  CIK 1364742 ingestion gap.
- No jobs-proc restart / no `sec_rebuild` (pure read-path).

## Open implementation questions for Codex ckpt-1

1. **Skip-collapse threshold**: is "single channel AND ≤1 member → leave in rest"
   the right no-op guard, or should ANY family with a member always collapse for
   display consistency? (Risk: needless churn vs inconsistent display.)
2. **filer_count**: counting `1 + len(family_members)` — does any downstream
   consumer assume `filer_count == len(slice.holders)`? (Grep `filer_count`.)
3. **family_members double-count safety**: confirm no residual/concentration path
   sums `family_members` (they live only on the holder for display).
4. **etfs bucket + funds overlay**: a BlackRock-family collapse lands in
   institutions; confirm it does not perturb the N-PORT `funds` memo overlay
   (institution_subset) accounting.
5. **as_of of the collapsed holder**: which channel's `as_of` represents the family
   (winning channel's? max across members?) for the freshness/coherence display.
