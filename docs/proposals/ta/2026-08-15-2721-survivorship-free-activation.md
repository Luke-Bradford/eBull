# #2721 — activate the survivorship-free evidence identity

Status: historical activation gate accepted on 2026-08-15; full evidence is
recorded in issue #2721 comment `5300786612`.

## Decision

`BACKTEST_UNIVERSE` becomes `survivorship_free`. This is the identity used by
the backtester, Strategies API, recent-evidence scheduler and paper runtime.
`survivor_only` remains available only as an explicit diagnostic arm; it is not
capital evidence.

The change is deliberately separate from the termination implementation. PRs
#2731/#2732 wired the universe and termination rule while the historical Form 25
expansion was still running. Activating the default before that census would
have made an implementation claim into an accepted data claim.

## Corpus identity must move with the universe

The survivor archive and Intrader archive have different vendors and frozen
frontiers. A survivorship-free row is stamped
`icyDenev/Intrader@2024-09-27`; it must never be queried as the legacy
`CORPUS_VERSION`. `corpus_version_for(universe)` is therefore the shared source
used by the writer, API current-result filter and scheduler completion check.
Otherwise a correct run would be stored and then rendered as missing.

## The archive boundary changes the historical denominator

Intrader's measured and load-time-asserted capture date is 2024-09-27. #2721's
hard bound says a window ending later can never earn `survivorship_free` on
this archive. The pinned historical windows are consequently:

| id | start | end |
| --- | --- | --- |
| `primary-2022-plus` | 2022-01-01 | 2024-09-27 |
| `rolling-36m` | 2021-09-28 | 2024-09-27 |
| `rolling-24m` | 2022-09-28 | 2024-09-27 |
| `year-2022` | 2022-01-01 | 2022-12-31 |
| `year-2023` | 2023-01-01 | 2023-12-31 |
| `year-2024` | 2024-01-01 | 2024-09-27 |

The former 2025 and 2026 windows are removed, not marked incomplete. Those
dates require prospective shadow evidence from the live signal/outcome ledgers;
calling them an unfinished historical backtest would create a denominator the
archive can never complete and would permanently block allocation for the wrong
reason.

This does not weaken the forward gate. Historical evidence ends at archive
capture; promotion and demo execution separately require a frozen
preregistration, prospective decision dates/calendar span, forecast assessment,
allocation controls and the broker preflight.

## Activation gate

Merge only after all of the following succeed on the same database state:

1. 2013–2021 Form 25 harvest completes with zero unfetched filings and a
   per-year filing/resolution census.
2. Intrader and PapersWithBacktest delisting links are rebuilt from the expanded
   register.
3. `scripts.verify_2721_survivorship_universe --smoke-run 100` reports exact
   universe reconciliation and rolls its limited smoke rows back.
4. The final provision/termination-class census is posted to #2721.

No preregistration is frozen before this activation: it would bind immutable
terms to the old `survivor_only` strategy versions.

## Accepted census

The 2013–2021 expansion harvested 7,999 filings. The equity/common cohort has
2,669 filings; symbol resolution is effectively absent before inline XBRL and
must not be described as unbiased. In 2021, the acquisition/failure mix differs
by 9.0 percentage points between unresolved and resolved issuers (z=2.45).
Absent linkage therefore remains unchecked, never checked-clean.

After both-vendor relink, full-population verification measured:

- exact `survivor_only` restoration: 5,265 series and zero Intrader;
- `survivorship_free`: 22,879 vendor series, 17,290 admitted, 5,589
  unlinked-alive excluded, zero unharvested;
- 12,641 terminating series: 11,388 unknown, 699 operation-of-law, 342
  q-suffix OTC unverified, 211 exchange failure and 1 exchange failure (a)(4);
- exact reconciliation of every selection stratum;
- a 100-series rollback-only smoke wrote four in-sample rows and their
  termination censuses, then persisted nothing.

This accepts the data identity and its stated limitations. It is not evidence
that any strategy is profitable, and it does not authorise preregistration,
promotion or allocation by itself.
