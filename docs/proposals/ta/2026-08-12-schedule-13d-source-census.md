# Initial Schedule 13D catalyst source census

Date: 2026-08-12
Issue: #2582
Status: source-feasible for preregistration; outcomes unopened

The exact follow-on contract is now frozen in
`2026-08-12-schedule-13d-preregistration.md` and
`contracts/schedule13d-public-catalyst-v1.json`. This census remains source
evidence and does not inherit any return result.

## Decision

An initial Schedule 13D is the first untested short-horizon event family in the
current repository with both an independent economic mechanism and enough
recent causal source coverage to justify an exact preregistration. It is not a
strategy, promotion or promise of positive returns. The next question is
specifically whether any return remains available **after** public filing and a
causal retail fill.

The family is preferable to adding another indicator vote. A Schedule 13D
reports an active/control stake; an initial Schedule 13G is a passive or
qualified-investor disclosure and can act as a stratified challenger. The
candidate observes the disclosure, not the activist's earlier private purchase.

Primary prior and falsifier:

- Brav, Jiang, Partnoy and Thomas, [Hedge Fund Activism, Corporate Governance,
  and Firm Performance](https://onlinelibrary.wiley.com/doi/10.1111/j.1540-6261.2008.01373.x),
  report positive older-sample announcement-window abnormal returns, with
  material variation across events and objectives.
- The SEC's [2023 final-rule economic
  analysis](https://www.sec.gov/files/rules/final/2023/33-11253.pdf) finds that,
  in one analysed corporate-action subset, most market reaction occurred near
  the ownership trigger rather than after the later filing. That is direct
  evidence against assuming the public filing leaves a tradable edge.
- The SEC's [current rule
  summary](https://www.sec.gov/newsroom/press-releases/2023-219) records the
  shortened initial-13D deadline and structured-data requirement. The modern
  2024-2026 corpus is a different disclosure regime from the older studies.

These publications supply the mechanism and the falsifier, not current eBull
performance evidence.

## Read-only corpus result

The census grouped `blockholder_filings` by accession so joint reporters did
not multiply events, but takes the public filing date from
`sec_filing_manifest.filed_at`. It joined coverage metadata only; no return,
target, stop, winner/loser or outcome path was calculated.

| filing year | initial 13D accessions | instrument mapped | research-series mapped | 60 prior + 20 later calendar days |
|---|---:|---:|---:|---:|
| 2024 from 18 December | 25 | 24 | 24 | 24 |
| 2025 | 757 | 727 | 691 | 575 |
| 2026 through 12 August | 503 | 457 | 445 | 296 |
| **total** | **1,285** | **1,208** | **1,160** | **895** |

The research archive ends on 2026-07-08; 89 mapped 2026 events therefore lack
20 later calendar days. They are incomplete, not zero-return observations.
Across all retained 13D/G initial and amendment forms, the ingest audit has
41,166 successful accessions, 3,566 partials and 26 failures. A trial census
must carry those source refusals.

Chain shape is material:

- 1,228 accessions have no strictly earlier active public filing date for any
  reporting person on
  the issuer in the retained chain;
- 57 have an earlier active filing for at least one reporting person;
- 49 have an earlier passive 13G-family filing for at least one reporting
  person;
- 44 have another filing for the same reporter and issuer on the same public
  filing date, so their within-date chain order is ambiguous and is not
  invented from accession-number order;
- the mapped accessions cover 855 distinct instruments.

Those figures do not yet prove the chain identity is economically correct.
They prove that blindly treating 1,285 rows as independent new campaigns would
be wrong. First 13D, repeat campaign and 13G-to-13D conversion require explicit
causal identities and separate attribution.

## Timestamp and Item 4 result

All 1,285 initial-13D accessions have their existing
`primary_doc_13dg` raw XML. Every retained document contains structured
`item4` / `transactionPurpose` text, but the current typed blockholder row does
not persist that purpose. The evaluator can parse it from the one canonical
document on demand. It must not duplicate the narrative in another table.

All 1,285 accessions map to a canonical SEC-manifest public filing date; 155
also retain an SEC acceptance timestamp. The typed blockholder `filed_at` is
not that field: it may be an XML signature timestamp and differs from the
manifest public date on eight accessions. It is therefore forbidden as the
decision clock. All rows use the fail-closed next regular-session open after
the SEC-manifest filing date; optional acceptance times do not receive a more
favourable fill in this daily-bar trial.

No typed `date_of_event` is present for these initial rows. That field is the
private ownership-trigger date, not the public action time in any case; it may
measure disclosure lag but can never authorise a pre-filing trade.

## Initial 13G challenger feasibility

The same outcome-free coverage join and structured-document rule labels found
the following initial-13G challenger ceiling. `Both` and `unknown` remain
separate; they are never pooled into a known regulatory rule.

| filing year | Rule 13d-1(b) | Rule 13d-1(c) | both | unknown | total with 60 prior + 20 later days |
|---|---:|---:|---:|---:|---:|
| 2024 from 18 December | 3 | 49 | 1 | 4 | 57 |
| 2025 | 3,350 | 1,419 | 20 | 486 | 5,275 |
| 2026 through complete coverage | 4,076 | 795 | 11 | 205 | 5,087 |
| **total** | **7,429** | **2,263** | **32** | **695** | **10,419** |

Rule identity is read only from the structured
`designateRulePursuantThisScheduleFiled` XML element. A broad text search had
previously misclassified accession `0002042569-25-000002` as `both`: its
structured designation is Rule 13d-1(c), while a signature comment says the
prior filing had incorrectly selected Rule 13d-1(b). No price outcome was used
to make this correction.

Initial 13Gs cluster heavily in February and at quarter/annual reporting waves.
That source shape is why the frozen challenger matches within filing month and
reports Rule 13d-1(b) and Rule 13d-1(c) separately. It does not establish that a
matched passive filing is an economically perfect counterfactual.

## Storage impact

The canonical 13D/G raw store already contains 44,732 deduplicated documents,
about 409 MB of uncompressed payload bytes. The candidate adds no raw document,
price, indicator or per-poll history. During research it reads the existing
document by accession and emits one bounded report. If later admitted to
prospective shadowing, only a compact purpose category/version/hash belongs on
a fired or refused decision; routine non-firings remain aggregate counts.

## Required preregistration before outcomes

1. Freeze the first-campaign/conversion/repeat-chain identity and fixture-test
   amendment collapse. Do not allow joint reporters or amendments to multiply
   independent observations.
2. Freeze an Item 4 purpose taxonomy from the source schema and a
   deterministic `unknown` refusal. Purpose categories are attribution unless
   one primary category is independently selected before outcomes.
3. Freeze the public-decision clock. Date-only history enters no earlier than
   the next regular-session open. Same-close and trigger-date fills are
   forbidden.
4. Freeze one published-effect comparison horizon before measurement. A
   capital bracket is a separate executable adaptation: it needs an
   independently declared risk objective and later untouched evidence, not a
   search over targets and stops on the same outcomes.
5. Use initial 13G, matched random timing and an unfiltered eligible-event arm
   as same-population challengers. Different 13G filer types and reporting
   deadlines must be stratified; the annual institutional filing wave is not a
   naive placebo for an immediate 13D.
6. Predeclare market/sector residual return, liquidity, price, volatility and
   filing-time regime as attribution/risk controls. They do not become a grid
   of confirming indicators.
7. Charge adverse spread/slippage historically and observed eToro bid/ask in
   prospective shadowing. Unknown product, eligibility or cost remains a
   refusal.
8. Reserve a terminal recent interval, declare the complete trial denominator
   and calculate the sample/power requirement from a minimum worthwhile net
   effect—not from older published means or the unopened local outcomes.

Promotion still requires a positive adverse-cost clustered lower bound,
acceptable tails and concentration, calibrated EV ranking, improved
mandate-level portfolio contribution, prospective shadow evidence and demo
execution. A high hit rate or positive mean cannot pass by itself.
