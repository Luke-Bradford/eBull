# #2840 §8 obligation (a) — REFUSED as framed: the route rides on the argument, not the callee

Ticket: #2840 (S-H regime/band-gated reversion, phase 2 research seat). Queue: #2437.
Predecessors: `07f62f2f` (the carrier), `b9860cf2` (§8 recorded both obligations open),
`ac107806` (the source-selection tripwire; obligation (a) framed), `b9b2a1e5` (obligation
(b) CLOSED; (a) reopened when three of my impossibility arguments were refused),
`688d5175` (`BarSeries` rows and caches frozen).

**Outcome: obligation (a) is not closable by any construction that leaves the route
caller-supplied.** Three constructions were built and refused this round, each by a
reproduction or by an in-repo settled decision — not by argument. The obligation is
re-framed on that evidence in §6.

## 0. Why this round, and why it is a refusal

Obligation (a) has been carried open across four sessions as *"three candidate
constructions, none decided"*. The two wall-clock heads (read the first real RTH session,
then two pre-open re-observation fires) are gated for the **sixth** consecutive session —
00:1x ET Monday, open ~9 h out — so (a) was the only unblocked head and the deliverable was
a decided construction.

It is decided. The decision is that the framing is wrong, and §6 says what replaces it.

## 1. The defect, REPRODUCED on the real file rather than inherited

Working-order 3c: an inherited root cause is a premise. `ac107806` measured 61/175 verdict
labels moving under a call-site swap; the identity half is re-measured here directly.

Probe — swap `strategy_signal_scan.py:987` from `from_undeclared_source(series=series)` to
`from_archive_basis("unadjusted", series=series)` (import line widened), recompute in a
fresh interpreter, restore from a byte copy taken first:

```
baseline  survivor_only  strategy-registry-v1+4d8b3b6536a1
swapped   survivor_only  strategy-registry-v1+4d8b3b6536a1   <- UNMOVED
```

⇒ a run that certifies `price_daily` — a series the provider **back-adjusts** at fetch time
(#2840's own finding), fed to a `>= $100` NOMINAL gate — is stored under the byte-identical
`strategy_version` as the run that refuses it. Control re-run after restore matches baseline.

## 2. Construction 1 — route keyed on the UNIVERSE. Refused by this ticket's own history.

`{"survivor_only": "undeclared", "survivorship_free": "archive"}` in the already-hashed
module, with `s12_signals` (which receives `universe`) raising on disagreement. No protocol
change, no new kwarg, S-12-only blast radius. It looked like the best answer.

**Refused.** `strategy_price_basis.py`'s own header says this module exists *because* S-12
used to refuse on `universe not in AS_TRADED_UNIVERSES` — *"a universe token standing in for
a price-basis fact, wrong in both directions"* — and #2840 §6 item 3 removed it. Keying the
route on the universe reinstates exactly that defect one layer up.

⚠ Independently false on the facts: `scripts/ab_2840_barseries_row_immutability.py:250-258`
runs the 2x2 grid {undeclared, certified} x {survivor_only, survivorship_free} deliberately,
so the correspondence this construction assumes is not 1:1 even in today's harness.

## 3. Construction 2 — declare the routes, digest them, pin with an AST walk. Refused by reproduction.

The design, and it is worth recording because the first two thirds of it are sound.

**Source rule.** This is not an SEC/EDGAR matter, so per the repo rule it would be fixed by
construction — except it does not need inventing, because this repo already fixed the same
defect class once. `market_regime_provider.py:72`:

```python
RULE_SET_VERSION: Final[str] = "benchmark-source-v1:live=price_daily_spy;research=spy_chain_v1"
```

whose docstring (`:66-71`) states obligation (a)'s rationale verbatim — *"switching the
backtest benchmark from `price_daily` to the research chain flips every pre-2023 bar … Same
signals table, same strategy code, different verdicts — the exact 'changed input under an
unchanged version' defect"* — and fixes the SHAPE of the answer (`:59-64`): **a declared
string naming the source in each context, deliberately NOT a module hash**, so fetch
*mechanics* stay outside the identity while source *semantics* stay inside.

Its one weakness is that the string is hand-bumped. The proposal closed that by deriving the
string from a structured declaration and pinning the declaration against an AST walk of what
the code actually calls — reusing `tests/test_job_registry.py:315`'s idiom
(`_extract_adapter_job_names`, which walks a module's AST *"to avoid the test going stale
relative to hand-maintained lists"*).

**Measured working, as far as it goes.** The identity plumbing verifies: an isolated probe
rotated S-12's two universe identities and left the other 22 unchanged. The extractor was
attacked with seven evasions and caught five (direct swap · alias import · module-attribute
access · local-variable indirection · inline `PriceBasisSeries`), missing two (`getattr`,
re-export hop).

### ⛔⛔ And it is REFUSED, by an eighth evasion that needs no trick at all

At `backtest_run.py:3254`, replace `archive_adjustment_basis` with the literal
`"unadjusted"`. The **constructor set is unchanged**, so the declaration still matches and
the pin passes. With provenance withheld, `from_archive_basis` flips from refuse-all to
certify-all — the same 61 verdict labels / 174 verdict-reason pairs `ac107806` measured,
including one new firing. Identity unmoved.

⇒ **the route rides on the ARGUMENT, not on the callee.** An inventory of which function is
called cannot constrain what it is passed. The same hole admits a whole class: reversing the
`PRICE_BASIS_CONSUMERS` condition, swapping the branches, or changing
`_resolve_liquidity_policy` upstream all move certification with every constructor name
intact.

⚠ The pin is also **redundant for the attacks it does catch**: `ac107806` already catches the
scan-swap class behaviourally in `tests/test_2840_carrier_source_selection.py`. A second gate
on the same class, which misses the class that matters, is worse than none — it reads as
coverage.

⚠ Two further defects in the design as written, both real: constructor *sets* retain no call
count, condition, argument or destination, so `backtest_run`'s two declared names already
give it redundant cover; and the walk's `app/**` scope excludes the `scripts/` harnesses that
actually run certified strategies.

## 4. Construction 3 — make the certifying ARGUMENT unforgeable. Refused by #3031.

The direct answer to §3's refutation: stop `from_archive_basis` accepting a `str` at all.
Take `ArchivePolicy | None` — a type minted **only** by
`strategy_entry_liquidity.archive_policy_for(vendor)` reading the pinned `RESEARCH_ARCHIVES`
register. Then `from_archive_basis("unadjusted", …)` is a **pyright error at the pre-push
gate**, and the dangerous edit stops being a token swap and becomes an import of the pinned
archive register.

**Refused, and the refusal is proven rather than argued.** `strategy_price_basis.py:68-78`
records that this module had to *stop* reaching `strategy_decision_context`, because doing so
pulls `market_calendar.RULE_SET_VERSION` into the scan's reachable closure and a calendar
change would then reuse every stored `strategy_version` (#3031). `strategy_entry_liquidity`
imports `strategy_decision_context` **and** carries `ENTRY_LIQUIDITY_RULE_VERSION`.

⚠ The `if TYPE_CHECKING:` dodge does not work, and that was checked rather than assumed: the
walk at `tests/test_strategy_registry.py:473-486` is **AST-based**, so a type-only import is
an `ast.ImportFrom` like any other. Probe — add the TYPE_CHECKING-only import, run the test,
restore:

```
AssertionError: these versioned rule sets are reachable from the signal scan but are hashed
into no strategy identity, so a change to them would reuse the old strategy_version (#3031).
['app.services.market_calendar.RULE_SET_VERSION = nyse-market-calendar-v1+546b962183b5',
 'app.services.strategy_entry_liquidity.ENTRY_LIQUIDITY_RULE_VERSION = entry-liquidity-2026-09-16-v1']
```

Unmodified control re-run at exit 0. The two honest fixes are the two that file already
enumerated — hash both rule sets into every identity (rotating all 12 to carry rules this
path does not read), or do not reach it. It chose the second, and re-reaching it now would
reverse an in-file settled decision to close a weaker hole than the one it closed.

## 5. What the three refusals have in common

Every construction moves **which token you edit**, and none removes the edit:

| construction | the dangerous edit becomes | why it still works |
| --- | --- | --- |
| universe-keyed route | edit the universe→route table | and reinstates the removed defect |
| declared routes + AST pin | edit the *argument*, not the callee | §3, reproduced |
| policy-object argument | import the archive register | blocked by #3031 first |

⇒ **obligation (a) cannot be closed by a static hash, because the route is a run-time fact
and the identity is a static artefact.** Everything above is an accident control wearing a
closure's clothes, which is the failure shape this ticket has now recorded at seven
consecutive rounds.

⚠ And the third refusal inverts one of MY OWN claims from `b9b2a1e5` §4.4, which said runtime
enforcement needs per-bar provenance. It does not, for the scan: the live runner knows it is
the live runner and can enforce the *negative* policy ("this path consumes nothing
certifying") with no provenance field at all. That is true, and it is still not a closure —
removing the enforcement is the same unhashed call-site edit — but the impossibility claim was
too broad and is withdrawn. Seventh safety-shaped claim of mine inverted on this ticket.

## 6. What obligation (a) actually requires — re-framed on the evidence

The harm §1 reproduces is **indistinguishability**: two evidence sets produced from different
sources share a `strategy_version`, so any consumer grouping by version pools them. Prevention
is not available (§5). **Detection is**, and there are exactly two constructions for it:

1. **Record the route with the evidence.** The carrier reports which route produced it and
   that route is stamped on `strategy_signal_observations` / `strategy_signals`. A swap then
   writes rows that say `archive` where every prior row says `undeclared` — queryable, and it
   costs no identity rotation. ⚠ Needs a schema migration, so it is the corpus rung with
   Definition-of-Done clauses 8-12, not a spare half-round.
   ⚠ Codex checkpoint 1 (finding 15) names the prerequisite: stored rows carry
   `input_rule_set_versions`, **not** `S12_PARAMS`, so a params-only digest is not readable
   from the ledger even when it is inside the hash. Any recording design must state its
   resolution path.
2. **Per-series bar provenance** — the route derived from the bars rather than supplied by
   the caller. This is #2840's already-deferred per-bar source, and it is the only
   construction that makes the route a fact about the data instead of a claim about the code.

(1) is the cheaper of the two and does not block on (2).

## 7. What this round SHIPS

Documentation and lessons only. No code: every construction that would have shipped is
refused above, and shipping a gate that misses the class that matters is worse than shipping
nothing (§3).

- This spec.
- Two prevention-log entries (§8).
- The measurements, so the next session does not re-derive them: the identity-unmoved
  reproduction (§1), the 57-vs-3 rotation cost of the whole-file digest (below), the eighth
  evasion (§3), and the #3031 assertion text (§4).

⚠ **The whole-file-digest candidate (§8 Option 1) is also refused, on measured cost.** It
works (`e9d0b1304427 → 1718e47d3091`, measured last session), but over 60 days
`backtest_run.py` took 44 commits and `strategy_signal_scan.py` 13, against **3** commits ever
that changed a `price_basis=` line in either. ⚠ Codex checkpoint 1 (finding 19) is right that
this is an upper bound and not a clean ratio — per-file commit counts can double-count a
commit touching both, commits are not deployments, and the denominator counts textual edits
rather than semantic routing changes. It is stated as the order of magnitude it is, and it
points the same way as §3's refutation: the digest is both over-sensitive *and* blind to the
argument.

## 8. Evidence disposition — nothing rotates, so nothing detaches

This round ships no code, so no identity moves. The census was still run, because the next
round's construction will rotate and the figure is the one it needs.

⚠ Last session's census called `entry.identity(universe=u)` inside a `try`, which raised
`TypeError` (`cost_model_id` is required) and stored a sentinel, so its intersection was empty
**by construction**. This one passes `cost_model_id` and uses **no `try`** — a raise aborts
loudly. It did not raise: 24 distinct identities over 24 (strategy, universe) pairs.

```
strategy_signal_observations     total=  922,821  attached_any_current=0
strategy_signals                 total=   59,230  attached_any_current=0
strategy_scan_watermark          total=       39  attached_any_current=0
```

**Non-vacuity control** — the thing the broken census lacked: stored versions are
`strategy-registry-v1+<12 hex>`, the same domain as the computed ones (`s11 +dbe90ab931e5`
87,027 · `s8 +77a336b109e7` 86,905 · `s4 +8c4babd582d9` 85,577), so the zero is a real
anti-join and not a type mismatch. S-12 specifically: **5,791 observations + 1 watermark, all
on `b18d5869092b`**, which is neither current identity (`4d8b3b6536a1` / `ab938f417093`) —
`688d5175` detached it yesterday.

⚠ Codex checkpoint 1 (findings 20-22) is right that this is weaker than a positive control
would be: a format match cannot detect a wrong database, a wrong checkout or a wrong join, and
three tables do not establish the absence of attached backtest results or outcomes. Stated as
the bound it is. The next round that actually rotates owes a positive control — reconstruct a
known stored historical identity and demonstrate attachment — and a wider table sweep.

## 9. What stays OPEN on #2840

- **Obligation (a)**, re-framed per §6 — record the route with the evidence (corpus rung,
  migration), or per-series bar provenance. ⚠ Do NOT reopen it as "hash the source selection":
  §5 is the measured refusal of that whole family.
- `binding_mismatch` non-guarantees 3 and 4 (equal payload transfers the claim; instrument id
  is not bound) — untouched.
- Read the first real RTH session; then two or more pre-open re-observation fires.
- ⚠ Still not breadth; still do NOT re-open "which 8-K item carries forward notice" from form
  text.

## 10. Checkpoint-1 disposition (24 findings)

| # | finding | disposition |
|---|---|---|
| 1 | precedent is relevant, not governing | **ACCEPTED** §3 — recast as the shape of the answer, not a proof of sufficiency |
| 2 | eighth evasion: substitute the argument at `backtest_run:3254` | **ACCEPTED — construction refused** §3 |
| 3 | constructor sets erase routing (count, condition, argument, destination) | **ACCEPTED** §3 |
| 4 | "the scan is the dangerous direction" does not discharge an obligation covering both routers | **ACCEPTED** §3 |
| 5 | upstream argument selection escapes entirely | **ACCEPTED** §3 — same class as 2 |
| 6 | `dataclasses.replace` / `type(carrier)(...)` escape | **PARTLY REBUTTED** — `replace` onto an uncertified carrier RAISES on `b9b2a1e5`'s missing-bindings check; the surviving forge must also pass `bindings_for(series)`, which is evasion 5's shape. Finding 10 half-notices this. Moot either way: construction refused |
| 7 | extractor grammar unspecified (`partial`, dispatch dicts, star/relative imports, shadowing) | **MOOT** — construction refused |
| 8 | `app/**` scope excludes the `scripts/` harnesses | **ACCEPTED** §3 |
| 9 | the walk includes `strategy_price_basis` itself; exemption implicit | **MOOT** — construction refused |
| 10 | evasion 5 not executable as written (needs `bar_bindings`) | **ACCEPTED** — see 6 |
| 11 | "seven evasions" misleading; closure claim contradicts acknowledged holes | **ACCEPTED** — the closure claim is withdrawn entirely, §5 |
| 12 | job-registry analogy omits its companion syntax guard | **ACCEPTED** §3 |
| 13 | digest serialization underspecified | **MOOT** — construction refused |
| 14 | the one-consumer guard is a text search, not a consumer detector | **ACCEPTED** — recorded here; pre-existing in `S12_PARAMS`, not introduced by this round, and not re-scoped into it |
| 15 | stored rows carry `input_rule_set_versions`, not `S12_PARAMS` — a params digest is not ledger-readable | **ACCEPTED and PROMOTED** §6, where it becomes a prerequisite of the replacement construction |
| 16 | §4.1 rejects too much; universe cannot distinguish live vs research of the SAME universe | **ACCEPTED in part** §2 — the refusal stands on the in-file header, and the sharper objection is recorded |
| 17 | §4.4's impossibility claim is unsupported: the live runner can enforce a negative policy | **ACCEPTED — my claim withdrawn** §5 |
| 18 | alternative: hashed routing module + run context + boundary validation | **REBUTTED** §5 — it relocates the edit rather than removing it, and its live-scan half is §4's import, which #3031 blocks |
| 19 | 57-vs-3 is not an established ratio | **ACCEPTED** §7 — restated as an order of magnitude with its three defects named |
| 20 | non-vacuity control inadequate; needs a positive control | **ACCEPTED** §8 — bound stated, owed by the next rotating round |
| 21 | "costs nothing additional" is narrowly conditional; three tables is not the population | **ACCEPTED** §8 |
| 22 | cold-start cost understated (watermark reset can discard catch-up backlog) | **ACCEPTED** §8 |
| 23 | acceptance 5 confuses expected equality with nothing to compare | **MOOT** — no code ships, so there is no A/B to justify |
| 24 | acceptance misses the invariant being claimed | **MOOT** — construction refused |

Refs #2840. Refs #2437.
