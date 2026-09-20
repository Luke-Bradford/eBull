# #2840 — the certifier: it already existed, in a census script

**Status:** findings + a promotion + one schema migration. Nothing frozen, `r9` unmoved, no
strategy behaviour changed, no broker call.

⚠ **NOT zero dev-DB mutations.** `sql/402` was applied to the dev database (§2a) — a
`SET DEFAULT` on one column, no rows written, no backfill. Every measurement in this
document ran read-only.

`3c6bb73f` established that the carrier question is blocked on a certifier and named the
next item: *define what evidence certifies that a delivered bar's level is the level it
traded at.* **It was already defined.** `nominality_bucket` in
`scripts/census_2840_forward_daily_provenance.py` implements exactly that rule, with tests.
The gap was its HOME: a rule in a census script is reachable by a census and by nothing on a
production path, and #2840's carrier needs it as an admission input.

This PR promotes it to `app/services/bar_capture_certificate.py` and points the census at it.

## 1. The rule, and why it is a rule rather than a threshold

A US corporate action takes effect at a US session **open**. A bar observed with no session
open between its completion and its capture cannot have been retroactively re-based — at
capture time there had been no opportunity. So:

- `before_next_open` — the only bucket in which no opportunity existed.
- `after_N_opens` (saturating at 3) — **not a defect label**. It is the number of
  opportunities a split check would have to rule out.
- `impossible` — captured before `bar_time + minutes_per_bar`, i.e. a corrupt row. The
  census arm prints this bucket only when it is non-empty, and it does not appear in §3's
  output; the count is left to the command rather than frozen here.

No constant is chosen anywhere in it, which is the point (`.claude/CLAUDE.md`'s #2279
BandWidth precedent forbids the percentile-threshold version).

## 2. The certificate is only worth what the WRITER guarantees

`strategy_observation_storage.store_intraday_bars` writes each bar once:
`_INSERT_INTRADAY_BAR` is a plain INSERT with **no `ON CONFLICT`** against PK
`(timeframe, bar_time, instrument_id)`; `captured_at` is a **SQL literal** in that statement
rather than a bound parameter, so no caller can supply it (it was a bare `DEFAULT now()`
before §2a); and a bar at or behind the stored watermark **raises** rather than overwriting.

⚠ Inherited verbatim from the census's own narrower claim, and not widened: that is an
**application convention, not a schema guarantee** — `sql/276` carries no UPDATE/DELETE
prohibition — and it makes the stored value the first observation *this writer accepted*, not
the first that ever existed. The new module's docstring states it beside the rule, because an
upsert added there would silently turn `captured_at` from "first seen" into "last touched"
while every consumer kept reading the old meaning.

## 2a. ⚠ The stamp was not an upper bound on the observation — `sql/402`

Found at Codex checkpoint 2, and it is the one real defect in the promotion. `sql/276`
defaulted `captured_at` to **`now()`**, which is `transaction_timestamp()` — the start of
the *enclosing* transaction. `store_intraday_bars` opens `conn.transaction()`, which is only
a **savepoint** when the caller already holds one, so a caller whose transaction began
before the fetch stamps a time **earlier than the observation**.

The direction is the unsafe one: this rule certifies when no open falls between the bar and
its capture, so a stamp that is too early places a post-open observation *before* the open
and certifies a bar it must refuse. A stamp that is too late can only refuse.

**Fix:** `sql/402` moves the default to `clock_timestamp()` — the statement time, which
necessarily follows the fetch — and `_INSERT_INTRADAY_BAR` now names the column with that
SQL literal so the writer does not depend on a default a later migration could move back.
Keeping it a literal rather than a bound parameter preserves the "no caller can supply it"
property.

⚠ `sql/402` carries `SET LOCAL lock_timeout = '5s'` before the `ALTER`. `SET DEFAULT` takes
`ACCESS EXCLUSIVE`, a *pending* `AccessExclusiveLock` queues **ahead of new readers**, and
the FastAPI lifespan runs migrations — so an unbounded wait on this relation hangs app boot
rather than failing it. Prevention-log precedent: *"A migration that is WAITING for a lock is
not passive"* (#2363). ⚠ Added after 402 had already been applied to dev, so the edited file
was replayed and its ledger row reset via the drift guard's own prescribed procedure
(`UPDATE schema_migrations SET content_sha256 = NULL WHERE filename = …`, quoted verbatim in
`app/db/migrations.py`'s error text). The GUC is session-local and changes no schema, so the
replay is a no-op on state — verified: the default still reads `clock_timestamp()`, the
marker row is unchanged, and `migrate.py` now reports `No pending migrations`.

**Applied and verified on dev** (`PYTHONPATH=. uv run python scripts/migrate.py` →
`Applied 1 migration(s): ['402_intraday_capture_clock_timestamp.sql']`):
`information_schema.columns.column_default` for `strategy_intraday_bars.captured_at` reads
`clock_timestamp()`, and the `ALTER` propagated to every existing partition (checked, not
assumed — `strategy_intraday_bars_1m`, `…_1m_y2026m08d21`, … all read `clock_timestamp()`).

⚠ **No backfill, and none is possible.** The true observation instant of an already-stored
bar is not recoverable. Measured before the change: 29,234 rows over 7,183 distinct
`captured_at`, batch writes sharing one timestamp across up to 1,000 rows and 122 bar-days
(the shape a transaction timestamp makes), and **zero** rows whose `captured_at` precedes
their own bar's completion. That last query is the *only* detectable form of the defect and
it is clean; the undetectable form is why the fix is structural. Both queries are in
`sql/402`'s header.

## 2b. ⚠⚠ The consequence, and it is the headline: the certificate admits ZERO stored bars

Codex found the second half of the same defect — `sql/402`'s header says pre-migration rows
cannot be repaired, and the first draft then let the rule certify them anyway. The calendar
arithmetic is identical for a row whose stamp is trustworthy and one whose stamp may precede
its observation, so arithmetic alone cannot separate them.

The module now splits into two layers:

- **`nominality_bucket`** — the arithmetic. Did a session open fall between the bar and its
  capture? Unchanged, and still what the census's proxy comparison is measured against.
- **`capture_certificate`** — the ADMISSION verdict. Refuses
  `unverifiable_capture_semantics` for any bar stamped before this database applied
  `sql/402`, and `capture_semantics_from` is a required keyword with no default (passing
  `None` means "not applied here" and refuses everything — the fail-closed direction).

The cutover is read from `intraday_capture_semantics.effective_from` (`sql/403`), **not
typed as an instant and not `schema_migrations.applied_at`**. It is per-environment, so a
literal would be right in exactly one database — and `applied_at` is `now()`, the migration
transaction's START, recorded *before* the `ALTER` takes `ACCESS EXCLUSIVE`. A writer
inserting inside that window carries old semantics with a stamp at or after it, and the gate
would admit it. `sql/403` records `clock_timestamp()` evaluated after the DDL instead. ⚠ It
is a separate migration because `sql/402` is already applied and the runner RAISES on a
content-hash mismatch (#1333) — the recorded instant is therefore 403's, which is later and
so stricter.

⇒ **Every one of the 29,234 stored bars is `unverifiable_capture_semantics` today**, because
all of them predate the migration. That is the truthful state and it is worth stating
plainly: the forward panel starts accruing certifiable bars from the migration forward, and
any sizing argument that assumed the existing store was usable as certified evidence is
wrong by the whole store.

## 2c. ⚠⚠ And the open bounds ECONOMIC effect, not the provider's rewrite — so nothing certifies at all

The deepest finding of the three checkpoint-2 rounds, and it holds. A session open bounds
when a corporate action becomes *economically* effective. It does **not** bound when eToro
rewrites its own history. If the provider pre-adjusts a Friday candle ahead of a Monday
split, `captured_at` is still before the next open and the delivered level is already
re-based.

⇒ **The open test is NECESSARY and not SUFFICIENT.** Treating it as sufficient converts
`91267518`'s deliberately-unverified premise into an admission — the exact move that ticket
refused, because the strong form would license reversing an assumed factor and could
*manufacture* ≥$100 gate eligibility.

So the module carries the precondition explicitly: `PROVIDER_REWRITE_TIMING_VERIFIED = False`,
and a clean post-cutover bar returns `unverified_provider_rewrite_timing` rather than
`before_next_open`. Only the *certifying* verdict is downgraded — an `after_n_opens` bar keeps
its count, because that count is how many opportunities a later split check must rule out.
The two refusals are deliberately distinct tokens: one is about **our stamp**, the other
about **the provider's behaviour**, and conflating them would hide which evidence is still
missing when one arrives.

⚠ Flipping that flag is a RULE change and must bump `CAPTURE_CERTIFICATE_RULE_ID` in the same
commit. The evidence that would flip it is already on the queue: **a confirmed split inside a
reachable window**, via one of `91267518`'s three named routes.

## 3. Measured — the whole store under the ARITHMETIC half

⚠ This is `nominality_bucket`, not the admission verdict — see §2b for why every one of
these is currently refused at admission. The distribution is what the store WILL look like
once the stamps are trustworthy, and it is the thing the proxy comparison in §4 is against.

`PYTHONPATH=. uv run python -m scripts.census_2840_forward_daily_provenance --capture-certificate`
(new arm, read-only, `REPEATABLE READ`), dev DB, 2026-09-20, every stored bar:

| timeframe | `before_next_open` | `after_1` | `after_2` | `after_3+` | total |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1m` | 6,226 | 389 | 0 | 0 | 6,615 |
| `5m` | 10,550 | 1,126 | 1,150 | 3,257 | 16,083 |
| `30m` | 1,880 | 198 | 198 | 4,260 | 6,536 |
| **all** | **18,656** | 1,713 | 1,348 | 7,517 | 29,234 |

⚠ **Not the 16/29 figure and not reconcilable with it** (`ff8f2311` measured sessions,
post-activation, seven liquid members, composition ∩ nominality ∩ timeliness). ⚠ Also **not**
the 74.1%-backfilled figure in `3c6bb73f`'s doc, which bucketed by raw capture LAG: on 30m,
lag-based counting gives 1,686 contemporaneous where the session rule gives **1,880**,
because a Friday-afternoon bar captured on the Saturday has a >2 h lag and no intervening
open. The session rule is the correct one and the lag figure was a first approximation.

## 4. I reinvented it, and the reinvention was worse

Before finding `nominality_bucket` I drafted a *"captured on the same New York calendar
date"* predicate, justified by not wanting the session calendar as a dependency. Measured
against the real rule over the whole store: **1,067 bars certified by the mechanism are
refused by the proxy**, zero the other way. Every one is the Friday/Saturday shape.

The lesson is the transferable part and it is now in the prevention log: **a proxy for a
mechanism reads as conservative and silently discards evidence**, and the check that catches
it is running both against the population rather than reasoning about which is stricter.
Pinned by `tests/test_2840_bar_capture_certificate.py::TestTheCalendarDateProxyIsWrongAndInWhichDirection`.

## 5. What changed in the move, and what did not

**Unchanged:** the rule, the bucket names, the saturation at 3, the `impossible` branch, the
30-day horizon.

**Changed, and it closes a fail-open branch:** exhausting the calendar search used to fall
through to `before_next_open` — the one branch where the rule has NO information was also
its most permissive. It now returns `undeterminable_next_open`, a refusal. I had written a
test *asserting* the old behaviour as "safe here", defended by an unverified claim about the
longest US market closure; an argument is not a guarantee, and it was standing in for one on
a safety branch. The refusal also demotes `horizon_days` to a plain **compute bound**: the
constant can now be wrong without any verdict being wrong, which is why it needs no
source-rule basis. No stored bar's verdict moves (the arithmetic table in §3 is unchanged). `tests/test_2840_forward_daily_provenance.py`
exercises all of it through the new module without an edit — that suite is the regression
evidence.

**Changed, and it is a real behaviour change:** `captured_at`'s default and the writer's
statement — §2a.

**Changed, harmlessly:** `next_session_open_utc` now tests `us_market_status(day) != "closed"`
instead of using `expected_bars(day)`'s bar COUNT as a truthiness test. The two are
equivalent (`expected_bars` returns 0 exactly when `session_close` is `None`, which is
exactly `closed`), and the equivalence is **pinned over all 365 days of 2026** rather than
asserted — plus a second test that the year is not vacuous (it contains `open`, `half_day`
and `closed`). Dropping the hop keeps a 13-bars-per-session *admission* rule out of a
calendar question it has nothing to do with.

## 6. Next

1. 🎯 **A confirmed split**, via one of `91267518`'s three named routes. It is now the
   binding item, not a nice-to-have: without it `PROVIDER_REWRITE_TIMING_VERIFIED` stays
   `False` and the certificate refuses every bar however clean its capture.
2. **The carrier** — the shape is settled even while the certificate refuses. It delivers a per-bar certificate on the uniform
   `PerSeriesSignals` call (`3c6bb73f` §3.1), as a parallel structure, never a mask
   (`3c6bb73f` §3.3).
3. Then the `AS_TRADED_UNIVERSES` / `SCAN_UNIVERSE` overloading, which still refuses a
   perfectly-certified composed series before any basis is read.

## 7. Not claimed

- That the provider serves nominal prices. `91267518` left eToro's back-adjustment behaviour
  UNVERIFIED; this does not move it.
- That an `after_N_opens` bar **is** re-based — only that nothing here can say it is not. The
  strong form would license reversing an assumed factor, which could manufacture ≥$100 gate
  eligibility.
- That a contemporaneous capture is unrevisable **at source**. It is unrevisable in our
  store, subject to §2's convention-not-guarantee caveat.
