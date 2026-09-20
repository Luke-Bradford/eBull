# #2840 — where price provenance lives in the strategy identity

Status: **recommendation with a measured cost. No code, no migration, nothing frozen,
`TRIAL_REGISTER_VERSION` unmoved at `r9`.** Doc-only on purpose — §5 states when the change may
land and §6 states the prerequisite it does **not** satisfy.

⚠ An earlier draft of this document was refused at Codex checkpoint 1 (31 findings). Two were
inverting: the sequencing advice was **backwards**, and the cost table measured inventory rather
than what a rotation actually detaches. Both are corrected below and §8 records what changed.

## The question

`docs/proposals/ta/2026-09-20-2840-forward-daily-provenance.md` §6 found that a forward S-12
reading a composed nominal daily series has nowhere to sit:

- `S12_PARAMS` carries `as_traded_universes`, so S-12 keys its **price basis** on the **universe
  label**;
- the universe label also names the **selection** (survivor-only vs survivorship-free);
- a composed forward series is survivor-only *by selection* and nominal *by basis* — a combination
  the label cannot express.

## 1. The recommendation — a sixth `INPUT_RULE_SETS` entry

`app/services/strategy_registry.py:56-142` holds a registry-wide mapping of *"Every versioned rule
set whose OUTPUT a strategy reads"*, hashed into `StrategyIdentity.version`. It has five entries,
four of them hand-maintained because strategies do not import the module — the engine does:

| entry | what it versions |
| --- | --- |
| `indicator_series` | how every series is computed (import-detected) |
| `market_regime_provider` | the benchmark **SOURCE** — *"live `price_daily` vs the backtest's `spy_chain_v1` research chain"* |
| `series_termination` | what a held position realises when its series stops |
| `universe_selection` | *"the vendor pins, admission rule, alive cut and capture date"* |
| `price_quarantine` | which bars every strategy sees, via `price_masked_bars`' loader |

**Two things make this the recommended home rather than merely a possible one.**

⚠ **`universe_selection`'s own comment says the label is insufficient**, and it says so as a
checkpoint-1 finding: *"The bare `universe` label on the identity does not version any of those
(ckpt-1), and a changed admission is a changed universe under criterion 11."* The overloading this
ticket hit is the same overloading that entry was created to relieve.

⚠ **`market_regime_provider` already versions a live-versus-research price-source distinction** —
for the benchmark series — with the rationale that transfers verbatim: *"switching the backtest
source flips every pre-2023 bar of a regime-gated strategy from `not_evaluable` to a real verdict,
which is a changed input under an unchanged `strategy_version` unless it is hashed here."*

⚠⚠ **And #3031 is the operational precedent for the whole move**: it added `price_quarantine` to
this mapping, accepting the manifest-wide rotation, and shipped
`scripts/census_3031_identity_rotation.py` to measure the cost. That census is reused below rather
than re-derived.

## 2. The three alternatives, and why each loses on balance rather than on principle

⚠ An earlier draft dismissed these. They are closer than that, and the reasons matter because a
future session may face a case that tips differently.

**A new global `StrategyIdentity` field (`price_source_id`).** The mapping header rejects a
*per-strategy, author-maintained* `inputs=[...]` field — *"a field they must remember to fill is
the same omission with a nicer name"* — and that argument does **not** transfer to a required,
loader-derived global field. ⚠ Nor is the `cost_model_id` analogy a clean distinction: `cost_model`
defines frozen rules with a hand-bumped identifier, so it *is* a rule-set identifier that happens
to sit in a field. **Why the mapping still wins:** a sixth mapping entry needs no change to
`StrategyIdentity`'s shape, no migration of the ledger's stored columns, and inherits the existing
pinning test. A new field is the better choice only if the provenance must be *selected per run*
rather than *installed globally*, and a composed-series rule set is installed.

**A new `Universe` literal.** Rotates everything anyway (`Universe` is a `Literal` in the
source-hashed `indicator_series`), so its rotation cost is **not** a comparative disadvantage. It
loses because it puts a data-basis fact inside a selection label — precisely the conflation that
produced this question — and because `AS_TRADED_UNIVERSES` would then admit a name whose
nominality is a property of the loader, not of the label.

**A rule version inside strategy `params`.** Rule versions already appear in `params` today, so
this is an existing arrangement rather than a novel one. It loses for the mapping header's stated
reason — per-strategy carriage reintroduces the omission — and gains nothing, since the value would
be identical across every strategy.

**Not available: the declaration.** `PreregDeclaration` carries no corpus-version field, recorded
as a gap in the arm-2 spec. ⚠ That is a statement about today's capability, not about design: the
declaration could be extended. Adding a rule hash does **not** close that gap, and the two are
independent.

## 3. Version style — hash of the owning module's source

All five existing entries are `f"{_RULE_ID}+{sha256(module source)[:12]}"`
(`indicator_series.py:64-71`, `universe_selection.py:55-61`, `series_termination.py:66-72`, same
idiom for the other two). The over-invalidation is accepted in terms: *"a comment edit in
`indicator_series.py` moves every strategy's identity … Both make stored signals visibly stale
instead of silently mixed."*

⚠ The counter-style exists and sits **outside** the mapping, so it is not a licence to deviate:
`strategy_entry_liquidity.ENTRY_LIQUIDITY_RULE_VERSION` is a hand-bumped literal because *"a
version change stop[s] meaning anything — the reader cannot tell a re-worded docstring from a
changed estimator"*. Matching the mapping's own style keeps a reader from needing to know which
discipline each entry follows.

## 4. Coverage — and what the tests do not prove

- `tests/test_strategy_registry.py::TestInputRuleSetsAreComplete` walks
  `app.services.strategies` and fails when a strategy imports a versioned rule set missing from
  the mapping. It cannot catch an engine-only dependency.
- `::TestTheEngineIsWalkedTooNotJustTheStrategies` (line 400) traverses the **scan's
  service-import closure** and checks version constants — so engine-only dependencies are **not**
  left solely to hand-maintained assertions, as an earlier draft claimed. ⚠ Whether a composed
  forward loader falls inside that closure is unspecified and must be established, not assumed.
- `::test_the_stored_mapping_is_the_hashed_one` (line 247) pins the hand-maintained entries. ⚠ It
  proves **registration**, not that the loader executes those rules — the quarantine tests
  additionally check loader binding, and a sixth entry needs the same.

## 5. When it may land — corrected, because the first draft had this backwards

Reproduce the cost with the tool #3031 already built:
`PYTHONPATH=. uv run python scripts/census_3031_identity_rotation.py` (read-only; it **recomputes**
current identities from `STRATEGY_MANIFEST` rather than importing a version constant, so it cannot
silently print a healthy-looking zero).

Measured 2026-09-20. The column that matters is the second — rows on an identity the manifest
**still produces**; the first is inventory and an earlier draft mistook it for impact:

| table | rows | **on a current identity** |
| --- | ---: | ---: |
| `strategy_signals` | 59,230 | **95** |
| `strategy_signals` (fired, unresolved) | 58,506 | **93** |
| `strategy_scan_watermark` | 39 | **4** |
| `strategy_results_store` | 580 | **0** |
| `strategy_preregistration_declarations` | 7 | **0** |
| `strategy_holdout_accesses` | 560 | **0** |
| `strategy_promotions` | 0 | 0 |
| `strategy_deployments` | 0 | 0 |

⇒ **A rotation today detaches 93 unresolved fired signals and 4 watermarks. Nothing else.**

⚠⚠ **Every stored declaration is ALREADY stranded — 0 of 7 sit on a current identity.** An earlier
draft said "4 pinned, one already stranded"; both halves were wrong. There are three
manifest-pinned declarations (mt1 ×2, s11) among seven total, and *all seven* are detached, because
`cost_model_id` is a hashed field and #3238's `COST_MODEL_ID` v3 → v4 rotated every version. S-12's
own current identity is `b18d5869092b`, already past the `f6100a890599` the arm-2 spec recorded.

⚠ **The watermark count is not a replay queue.** An earlier draft called 39 watermarks "frontiers
to re-walk". `write_window_indices` cold-starts an unseen identity with one pre-frontier bar, so an
old watermark row is simply no longer addressed.

### ⚠⚠ The sequencing advice, inverted from the first draft

A declaration pins whatever `strategy_version` is current **at freeze time**. Therefore:

- **Add the entry BEFORE arm 2's freeze** → arm 2 pins the new identity and nothing is stranded.
- **Add it AFTER arm 2's run completes** → the run's evidence is already attached.
- **Add it BETWEEN the freeze and the run** → that is the case that strands the pin.

The first draft said *"freeze and run arm 2 in-sample first, or accept the strand"*, which presents
the safe option as a cost and omits the genuinely safe one. ⚠ Note also that `r9` is the **trial
register** version, a different mechanism from the strategy identity; arm 2 needs its trial entry
before its freeze, and describing the work as "a declaration held at `r9`" obscures that ordering.

### "At the commit that first wires it" — an operational definition

`series_termination`'s entry records the rule (*"joined the hashed set at the SAME commit that
first wired it into the backtest"*), but "wires" needs pinning down. ⚠ Take it as: **the commit at
which a strategy identity is first CONSTRUCTED over the composed source** — not a census, not a
test harness, not an exploratory script, all of which can read the series without minting an
identity. Registration and first identity-bearing consumption in one commit; earlier registration
is not forbidden (the termination precedent is historical, not a prohibition) but buys nothing.

⚠ And the rotation is not this entry's to schedule alone: the identity hashes `strategy_registry.py`
itself, so any interface edit there — or a provenance field added to the source-hashed
`indicator_series` — rotates everything first regardless.

## 6. ⚠⚠ What this does NOT give the forward instrument

**A rule-set hash versions the RULE, not the DELIVERED SERIES.** The same composer can hand a
strategy a series whose constituents are contemporaneous, backfilled, incomplete, or
unverifiable — the merged findings measure exactly that spread (16/29 usable post-activation). So
"the nominal rule set is installed" cannot be the admission gate, and an identity entry cannot
carry per-series provenance.

⇒ **A separate carrier is a prerequisite, and nothing in the repo provides it today.** `BarSeries`
has no provenance field, `s12_signals` receives none, and the manifest's signal interface passes
none — so a next session must decide how trusted loader metadata reaches both identity
construction and per-bar evaluation, including what refuses on a mismatch. **That is the real
remaining design work; this document only settles where the RULE's version lives.**

⚠ And "only the first of the three guards changes" — which the first draft claimed — is false for a
composed source. The config guard resolves a research archive through `vendor_for`; the run-time
guard reads `load_corpus`'s archive-derived cost basis. A composed intraday source routes through
neither, so **both** need to cover its actual routing and basis.

⚠ The paired control is in scope too: S-4's control book must consume the **same** resolved series
with compatible provenance, or the paired difference measures sourcing rather than the gate.

⚠ Still open from the merged findings and not addressed here: split-boundary policy, the effect of
omitted sessions on ATR/lookback/holding clocks, the float→`int` volume narrowing, and mixed-source
warm-up. A basis check does not complete the instrument.

## 7. One question flagged rather than asserted

`strategy_entry_liquidity` owns `ELIGIBLE_ADJUSTMENT_BASES = {"unadjusted"}` and an
`ENTRY_LIQUIDITY_RULE_VERSION` that is **not** in `INPUT_RULE_SETS`. ⚠ The module has **more than
one consumer** and they differ in kind: its liquidity eligibility excludes diagnostic observations
after returns are recorded (`backtest_run.py:1960-1975`) and does not exclude strategy legs, while
`archive_policy_for` supplies basis metadata used in cost selection. So "verdict input" versus
"diagnostic only" is the wrong dichotomy for the module as a whole, and whether any of its outputs
meets the mapping's criterion needs a caller trace per consumer. **Not claimed either way.**

## 8. What the refused draft got wrong

| claim | what replaced it |
| --- | --- |
| "freeze and run arm 2 first, or accept the strand" | **inverted** — add BEFORE the freeze, or after the run; between them is the only stranding case |
| "4 declarations pinned, one already stranded" | 7 declarations, **0** on a current identity; three are manifest-pinned |
| cost table of total rows | rows **on a current identity** — 93 unresolved fired signals + 4 watermarks |
| "39 frontiers to re-walk" | a watermark is not a replay queue; an unseen identity cold-starts |
| hand-rolled cost queries | `scripts/census_3031_identity_rotation.py`, which already exists for exactly this operation |
| "the repo already decided this" | the mapping is a **recommendation on balance**; three alternatives are closer than the draft allowed |
| `cost_model_id` is a per-run config, not a rule set | false — it identifies frozen rules with a hand-bumped id |
| engine-only deps rest on hand assertions | `TestTheEngineIsWalkedTooNotJustTheStrategies` walks the scan's import closure |
| "only the first guard changes" | both the config and run-time guards must cover a composed source's routing |
| implied that a basis check completes the instrument | a rule hash versions the RULE, not the delivered series; the per-series carrier does not exist |

Refs #2840. Refs #2437. Refs #3031.
