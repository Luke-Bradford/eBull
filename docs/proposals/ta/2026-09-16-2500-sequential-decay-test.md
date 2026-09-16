# #2500 slice 2 — the declared repeated-look method

Slice 1 (`b22295fd`) shipped the pointer to the approved envelope and stopped, stating in
place that *"the comparison that will read it is [a data-treatment decision], and that
comparison needs a declared repeated-look method before it can be written."*
`strategy_monitoring_baseline.MISSING_ENVELOPE_COMPONENTS` names the gap:
`checkpoint_plan_and_error_budget`.

This slice declares and freezes that method. **Pure rule module only** — no schema, no
write, no caller, no gate touched, same shape as slice 1.

## Source rule

#2500's evaluation contract, clause 4: *"Repeated peeking must not reuse an ordinary 95%
interval; use a declared alpha-spending or anytime-valid method."*

**Why anytime-valid rather than alpha-spending.** Alpha-spending (Lan–DeMets / O'Brien–
Fleming) allocates the error budget along an information fraction, which needs either a
declared maximum sample `N` or a declared infinite summable allocation. A strategy's firing
cadence is `firing_interarrival_range` — one of the four components slice 1 recorded as
**not stored anywhere** — so any `N` we wrote down would be invented, and the allocation
shape would be a second invention. An anytime-valid boundary needs neither. This is a
sourcing argument, not an impossibility claim: alpha-spending is available in principle and
is rejected because its inputs are not.

### The boundary

Howard, Ramdas, McAuliffe & Sekhon (2021), *Time-uniform, nonparametric, nonasymptotic
confidence sequences*, Ann. Statist. 49(2) 1055–1080 (arXiv:1810.08240), **equation (14)**,
the two-sided normal-mixture uniform boundary:

```
u(v) := sqrt( (v + rho) * log( l0^2 * (v + rho) / (alpha^2 * rho) ) )
```

`l0 = 1` in the scalar case (Definition 1), giving

```
u(v) = sqrt( (v + rho) * log( (v + rho) / (alpha^2 * rho) ) )
```

Lemma 2 makes this a sub-psi uniform boundary with crossing probability `alpha`, and
Proposition 5 gives the two-sided (normal-mixture) form:
`P( exists t : |S_t| >= u(V_t) ) <= alpha`, **simultaneously over all `t`** — which is
exactly the repeated-look property #2500 asks for.

**Intrinsic time is defined here as** `V_n = n * sigma_proxy^2`, `sigma_proxy^2 = (b-a)^2/4`
(below). ⚠ It has squared-return units, not trade units. A tuning target expressed in trades
must be converted: `m = n_target * sigma_proxy^2`. Trade count and empirical variance are
not interchangeable.

### Tuning `rho`

Proposition 3(a): for a target intrinsic time `m`, `v -> u(v)/sqrt(v)` is uniquely minimised
at `v = m` when `m/rho = -W_{-1}(-alpha^2 / (e * l0^2)) - 1`.

⚠ **No Lambert-W implementation and no new dependency** (no scipy in this project).
Substituting `w = -(1+k)` into `w * e^w = -alpha^2/e` reduces the tuning rule to

```
(1 + k) * e^{-k} = alpha^2,        rho = m / k
```

strictly decreasing on `k > 0` (derivative `-k e^{-k}`), so it has a unique root for every
`alpha` in (0,1) and is solved by bracketed bisection. The test asserts the **identity in
relative terms**, not a hand-copied constant — cf. the 2026-09-16 prevention entry *a
hand-copied predicate has no compiler*. Absolute tolerance would accept useless roots at
small `alpha`, where `alpha^2` underflows the comparison scale.

⚠⚠ `rho` is a function of `(alpha, n_target)` ONLY, both of which are caller-declared and
frozen before the first look. **Re-deriving `rho` after seeing outcomes voids the
guarantee** — the boundary must be fixed in advance of the data it judges.

### ⚠⚠ The sub-Gaussian precondition is derived from the barrier contract, not assumed

Eq (14) holds for a sub-psi process with a known variance proxy. We have no measured proxy
for per-trade net returns and cannot assume a small one: the 2026-08-22 cut-and-reset
records skew 36 and **kurtosis 1,976** on s8's per-trade returns. (Those moments do not by
themselves prove non-sub-Gaussianity — a bounded variable can have arbitrarily large
standardised kurtosis — but they remove any basis for picking a proxy by inspection.)

The proxy comes instead from **Hoeffding's lemma** (Hoeffding 1963; Boucheron–Lugosi–Massart
Lemma 2.2): if `X in [a, b]` almost surely then `X - E[X]` is sub-Gaussian with variance
proxy `(b - a)^2 / 4`. The same bound applied conditionally makes each martingale increment
conditionally sub-Gaussian with that proxy, which is what a uniform boundary needs.

`[a, b]` is supplied by the caller from the strategy's frozen TP/SL/timeout contract
**widened by the pinned `worst_gap_pct`** already carried in #2505's envelope — i.e. the
declared lower bound is `-(stop_barrier + worst_gap)`, not `-stop_barrier`. Widening is
deliberate: a stop that gaps is the ordinary breach of the naive bound, and absorbing it
conservatively is cheaper than refusing on it.

### ⚠⚠ What a bounds breach does and does NOT buy

A trade outside the declared `[a, b]` **refuses**. It is #2500 evaluation-contract class 1
(implementation/data drift: "outcome-resolution mismatch"), which suspends immediately.
Admitting it by widening the bound would silently convert a broken barrier contract into a
wider tolerance.

⚠⚠ **The refusal does not repair validity, and the spec previously claimed it did.** Codex
checkpoint 1 supplied the counterexample: `X = -1` with probability 0.99 and `X = +99`
otherwise has mean 0 and violates a declared `[-1, 1]`; under this tuning, **17 consecutive
losses cross the lower boundary before the `+99` ever arrives, which happens with
probability `0.99^17 ~ 84%`**. So a false "deteriorated" verdict is reached long before the
policing refusal fires.

The guarantee therefore rests on `[a, b]` being a **true almost-sure bound**, not on a
declared-and-policed one. The gap-widened barrier contract is the best available
approximation to that and it is an approximation; the module states this rather than
implying the refusal closes the hole. The monitor slice that consumes this must route a
bounds refusal to class 1 and never to "deteriorated".

### The null is conditional, and that is an assumption about the data

`S_n = sum(X_i - m0)`. The null is

```
H0:  E[X_i | F_{i-1}] >= m0   almost surely, for every i
```

— **conditional on the filtration, not marginal**. Codex checkpoint 1 killed the marginal
version with a one-line counterexample: if `X_i = Z` for every trade with `Z` a fair sign,
every marginal mean is zero yet the lower boundary is crossed with probability 1/2.

Under the conditional null, write `X_i - m0 = (X_i - E[X_i|F_{i-1}]) + (E[X_i|F_{i-1}] - m0)`.
The first term is a martingale difference, bounded, hence conditionally sub-Gaussian with
proxy `(b-a)^2/4`; the second is non-negative. So `S_n` dominates the pure martingale
`M_n`, and

```
P( exists n : S_n <= -u(V_n) )  <=  P( exists n : M_n <= -u(V_n) )  <=  alpha
```

⚠ **The two-sided bound applies to `M_n`, not to `S_n`.** Under a composite null with
strictly positive drift the UPWARD crossing of `S_n` can be certain, so `P(either crossing
of S_n) <= alpha` is false. Only the lower-tail domination above is claimed. No extra factor
of two is needed — the two-sided boundary already covers each tail at level `alpha`.

⚠ **Named violations of the conditional null**, not hidden: overlapping positions exposed to
one market shock, outcome-dependent resolution ordering, and selective inclusion of resolved
trades all break it. The monitor slice must define the stream so that increments are
adapted and one-per-resolution, or the guarantee does not hold.

### One-sided reading

Decay is a fall in expectancy. The verdict reads the **lower** crossing only; an upward
crossing is not a verdict. The conservative direction is the right one because `paused` is
reachable from every pre-terminal stage and its only successor is `retired`
(`strategy_control_plane.py:80-85`), so an automatic pause is not undoable without minting a
new strategy version.

### A crossing latches

The verdict is evaluated over **every prefix** `k = 1..n`, not only at `n`. Without that, a
sum that crosses at `k` and recovers by `n` reports not-rejected, which would silently
convert the time-uniform guarantee back into a single-look one.

## What this test does NOT claim

- **It tests cumulative expectancy against a fixed pinned benchmark, not recent change.** A
  long favourable history can mask later deterioration, and this rule will not flag it.
- **`m0` is a pinned number, not the true historical mean.** Baseline uncertainty, selection
  effects and train/monitor separation are not addressed here.
- **Non-rejection is not health.** The verdict is named `not_rejected`, deliberately — it
  does not establish unchanged expectancy and says nothing about the other envelope
  components.
- **Multiplicity is per-test only.** Anytime validity covers repeated looks *within one*
  `(strategy_version, baseline, alpha, rho, bounds)` test. Running the test across many
  strategies is a separate multiplicity problem this module does not solve.
- **`m0` selection is not decided here.** `PromotionEvidence` carries an expectancy *lower
  confidence bound* plus per-year means, and a baseline may pin several evidence records, so
  "the approved expectancy" is ambiguous. `m0` is a required caller input; choosing it is the
  next slice's work and is named as such.

## Empirical verification

Shipped as `scripts/verify_2500_sequential_boundary.py` (seeded, output retained in the PR)
plus a fast seeded arm in the test suite. `alpha = 0.05`, `n_target = 200`, observations on
`[-1, 1]` so `sigma_proxy^2 = 1`; 200,000 paths x 2,000 steps.

| arm | two-sided crossing rate | bar |
| --- | --- | --- |
| Rademacher | 0.0333 | 0.05 |
| Gaussian(0,1) | 0.0336 | 0.05 |

Both under `alpha`. ⚠ This is a finite-horizon *consistency* check, not a proof of the
worst case: the theorem bounds the infinite-horizon crossing probability for every sub-psi
process, and no simulation can establish that. Tuning identity checked at
`alpha in {0.01, 0.05, 0.10}`: `(1+k)e^{-k}` reproduces `alpha^2` to a relative 1e-12.

**Power — the number that must not be omitted.** ⚠ The alternative keeps the SAME support
`[-1, 1]` (a biased sign, `P(+1) = (1-d)/2`), because a shifted Rademacher leaves the
declared bounds and the production rule would refuse it. Median trades to a one-sided lower
crossing, and detection rate within 2,000 trades — Monte Carlo standard error on each rate
is below 0.4 points at 40,000 paths:

| true mean shift | detected within 2,000 trades | Monte Carlo s.e. | median trades to detect |
| --- | --- | --- | --- |
| -0.05 | 30.1% | 0.23 pt | not reached within the horizon |
| -0.10 | 93.8% | 0.12 pt | 740 |
| -0.20 | 100.0% (0 of 40,000 missed) | 0 | 186 |
| -0.40 | 100.0% (0 of 40,000 missed) | 0 | 57 |

⚠ The median is taken over **all** paths with undetected ones held at the horizon sentinel,
not over the detected subset. Codex checkpoint 2 caught the conditional form: at -0.10 it
reported 692, by which point only ~47% of all paths had detected. Conditioning on detection
flatters exactly the arm whose latency matters.

⚠ `100.0%` is "no path of 40,000 failed to detect within the horizon", not a guarantee.
Measured crossing rates: Rademacher 0.0333, Gaussian 0.0336, both `<= alpha = 0.05`. Tuning
identity relative error `2.2e-16` (machine epsilon) at `alpha in {0.01, 0.05, 0.10}`.

⚠ **A small decay costs many hundreds of trades to detect.** At any cadence this engine
currently fires, that is not a fast alarm, and the monitor slice must report the detectable
effect size rather than implying it catches decay promptly. This is the cost of *this*
boundary at *this* tuning against *this* proxy — a tighter proxy or an empirical-Bernstein
boundary would do better, and neither is available without a measured variance.

## What ships

`app/services/strategy_decay_sequential_test.py` — pure, no I/O, no defaults:

- `SEQUENTIAL_TEST_ID = "anytime-valid-normal-mixture-v1"`, frozen.
- `mixture_tuning_k(alpha)` — bracketed bisection on `(1+k)e^{-k} = alpha^2`.
- `mixture_rho(*, target_intrinsic_time, alpha)` — `m / k`.
- `uniform_boundary(intrinsic_time, *, rho, alpha)` — eq (14) at `l0 = 1`.
- `hoeffding_variance_proxy(*, lower_bound, upper_bound)` — `(b - a)^2 / 4`.
- `evaluate_sequential_look(...) -> SequentialLookVerdict`.

⚠ **Every parameter is required; nothing has a production default.** `alpha`, `n_target`,
`m0` and the bounds are declarations, and a default here would become an invented constant
in the one place the whole slice exists to avoid. The simulation's `0.05` / `200` are
simulation settings and are not exported as defaults.

`SequentialLookVerdict` carries the full provenance needed to reproduce the claim — test id,
`alpha`, `rho`, `n_target`, bounds, `m0`, observation count, intrinsic time, boundary value,
worst prefix sum and the prefix index where the worst occurred — because a method id alone
does not identify a statistical claim.

Outcome is one of `not_rejected` / `deteriorated` / `refused`. Closed refusal vocabulary,
evaluated in this order so the first entry is the most specific cause:

`alpha_out_of_range`, `invalid_declared_bounds` (non-finite, reversed, or equal — an equal
pair gives a zero proxy and a degenerate zero intrinsic time), `invalid_tuning_target`,
`baseline_outside_declared_bounds`, `no_observations`, `non_finite_observation`,
`observation_outside_declared_bounds`, `non_finite_boundary` (arithmetic that overflows or
underflows despite finite inputs).

⚠ A refusal is never `not_rejected`. Configuration faults raise nothing and return a
refusal; the module performs no I/O, so there is no database-error case to distinguish
(slice 1's fail-closed-but-do-not-swallow rule applies to the reader that will call it).

## Not in this slice

Health states, storage, the transition to `paused`, the UI, `m0` selection, the stream
definition, cross-strategy multiplicity, and the other three missing envelope components.
This module chooses the method and stops.
