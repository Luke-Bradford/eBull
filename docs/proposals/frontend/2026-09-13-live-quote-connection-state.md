# #2944 — a disconnected quote stream must stop claiming to be live

Status: spec for implementation (2026-09-13).

## Defect (re-verified from source)

Three distinct bugs, all in the two SSE quote implementations.

**1. `connected` stays true while the browser is reconnecting.**
`frontend/src/lib/useLiveQuote.ts:129` and
`frontend/src/components/quotes/LiveQuoteProvider.tsx:184` both handle only
`readyState === CLOSED` in `onerror`. EventSource fires `onerror` on *every*
reconnect attempt with `readyState === CONNECTING`, and neither handler touches
`connected` on that branch. `SummaryStrip.tsx:218` drives the pulsing "live"
dot straight off `connected`, so the operator sees a pulsing LIVE badge over a
frozen number while the transport is down.

**2. A null instrument id does not clear the previous instrument's state.**
`useLiveQuote.ts` returns early on `instrumentId == null` **before** the
`setTick(null) / setConnected(false) / setUnavailable(false)` reset, so
unsubscribing leaves the previous instrument's tick, its `connected` and its
`unavailable` on screen. The operator's own repro records exactly this:
`instrument=null: connected=false, unavailable=true, previous instrument tick
retained`.

**3. A retained tick outranks the REST fallback unconditionally.**
`SummaryStrip.tsx:143` (`liveNative?.value ?? price?.current`) and
`LivePriceCell.tsx:45` both prefer the tick with no reference to connection
state. A tick frozen at the moment the stream died therefore keeps winning over
every later REST snapshot, indefinitely.

And the existing test cannot catch (1): `useLiveQuote.test.ts:114` never opens
the stream before firing the transient error, and asserts only `unavailable`.

## Source rule

`.claude/skills/frontend/safety-state-ui.md` § "Stale marker is required, not
optional" governs the display half directly, and it is a settled repo rule
rather than a judgement call:

> When the displayed snapshot comes from cache rather than a live response,
> mark it visibly. … **A silent cache that looks live is worse than no cache at
> all.**

A tick retained across a dropped stream IS a cache. So the rule is not "hide
it" — hiding a recent number the operator can still use is its own harm — it is
**keep it and mark it**, exactly as the kill-switch pill does
(`DemoLivePill.tsx`, the cache-and-OR pattern that skill names as canonical).

The connection-state machine itself follows the WHATWG HTML EventSource
definition of `readyState` (`CONNECTING = 0`, `OPEN = 1`, `CLOSED = 2`) —
`CONNECTING` after a successful open is the spec's own name for "reconnecting",
so it is read from the source and not invented.

## What the data does NOT support, and the design consequence

The ticket asks for precedence against "a newer snapshot". **No newer snapshot
can arrive, and there is no timestamp to compare against.** Both halves were
checked:

- `InstrumentPrice` (`frontend/src/api/types.ts:381-398`) carries `current`,
  `currency` and `display_current`, plus `day_change_as_of` — which is the date
  of the latest `price_daily` **close** the day-change is computed from
  (#1924), not an as-of for `current`. None of `LivePriceCell`'s four call sites
  (`PositionsTable.tsx:121`, `CopyTradingPage.tsx:325` + `:371`,
  `PortfolioPage.tsx:595`) has a per-price timestamp either; broker positions
  expose `current_price` with no as-of. A `fallbackAsOf` prop would therefore be
  a **writer with no reader**.
- ⚠ These pages load their snapshot through `useAsync`
  (`frontend/src/lib/useAsync.ts`), which has **no background refetch**. The
  instrument summary loads on symbol change and then stands still. So the
  `fallback` prop does not move while the page is mounted, and a first draft of
  this spec that proposed "prefer the fallback once its value changes since the
  tick arrived" was building on a cadence that does not exist. Caught by Codex
  at checkpoint 1 (finding 26). That draft was also unsound independently — it
  required writing a ref during render (unsafe under concurrent rendering and
  StrictMode), made the outcome depend on when a cell happened to mount, and had
  no defined equality for `string` vs `number` price shapes.

**The hazard the acceptance bullet is really pointing at is therefore not
"fallback newer than tick" — it is a stale tick PRESENTING AS LIVE.** Two
things make that happen and both are fixed here:

1. the badge claims live while the transport is down (bug 1), and
2. **on reconnect, the retained tick is silently re-blessed as live** even
   though no new frame has arrived. A stream that drops at 09:00, reconnects at
   09:05 and receives nothing (quiet book) would show the 09:00 price pulsing
   LIVE forever. Codex ckpt-1 finding 12.

Timestamp-based precedence is left as a **named gap**, not silently dropped: it
needs either an as-of on the REST price or a refetch cadence, neither of which
exists today.

## Decision

### One shared connection-state policy

New `frontend/src/lib/liveQuoteConnection.ts`, imported by both the hook and the
provider (acceptance: "reuse one connection-state policy across the
single-instrument hook and page provider"):

```ts
export type LiveConnectionStatus =
  | "idle"          // no subscription (no id / empty set / no EventSource)
  | "connecting"    // stream constructed, no successful open yet
  | "live"          // open
  | "reconnecting"  // errored with readyState CONNECTING AFTER a successful
                    // open — transport is down and the browser is retrying
  | "unavailable";  // errored with readyState CLOSED — terminal for this
                    // EventSource; the browser will not retry

/** The HTML spec's own readyState is the discriminator; nothing is inferred.
 *  ``hasOpened`` separates "never got up" (connecting) from "fell over"
 *  (reconnecting) — CONNECTING alone does not say which. */
export function statusOnStreamError(
  readyState: number,
  hasOpened: boolean,
): LiveConnectionStatus;

/** Legacy booleans, derived so no consumer has to be rewritten at once. */
export function isConnected(status: LiveConnectionStatus): boolean;   // === "live"
export function isUnavailable(status: LiveConnectionStatus): boolean; // === "unavailable"

/** Is the retained tick the price of RIGHT NOW, or a cache?
 *  Both halves are required: the stream must be up, AND the tick must have
 *  arrived on the connection that is currently up. */
export function tickIsAuthoritative(
  status: LiveConnectionStatus,
  tickArrivedOnThisConnection: boolean,
): boolean;
```

`connected` and `unavailable` stay on both public shapes with unchanged
meaning, derived from `status`. This is deliberate: eight files consume them,
and widening the contract without breaking it keeps the diff to the surfaces
that actually render a lie.

### Precedence, stated explicitly

| state | outcome |
| --- | --- |
| tick present, `status === "live"`, tick arrived on this connection | the tick wins, rendered normally, pulse shown |
| tick present, anything else | the tick still wins the *number* (it is the most recent price anyone has) but is rendered **stale-marked**, and the pulse is replaced by an amber marker |
| no tick | the fallback wins, exactly as today |

"Arrived on this connection" is ordinary hook/reducer state, not a
render-written ref: a `tickFresh` flag set **true** on `message` and **false**
on both `error` and `open`.

⚠ **Attribution, measured rather than assumed:** it is the `error` clear that
makes the reconnect case correct, because a browser always fires `error` before
it retries. A revert-probe removing the `open` clear left every user-visible
test green. The `open` clear is kept as a redundant local guard — so the
invariant lives at the handler that opens the connection rather than resting on
that ordering — and is pinned by a test that fires `open` twice with no
intervening error, labelled as pinning a defensive invariant rather than a
reachable bug.

⚠ The badge is driven by the **provenance of the price on screen**, not by the
socket: an open stream that has never delivered a frame for this instrument
(quiet book, halted name) shows the REST fallback and must not pulse.

### Clearing

- `useLiveQuote`: the state reset moves **above** the null-id early return and
  above the `EventSource === undefined` guard, so unsubscribing clears tick and
  status. In addition the returned `tick` is **scoped to the requested id** —
  `tick.instrument_id === instrumentId` — so even the render that happens
  before the effect runs cannot show the previous instrument's price.
- `LiveQuoteProvider`: reset unconditionally on the empty-set and
  no-EventSource branches (today the reset is inside an `if (prior !== null)`
  and below the support guard). On a canonical-set change the effect dispatches
  `suspend` **immediately** — status to `connecting`, `tickFresh` false — rather
  than waiting for the 300 ms debounce timer, so nothing claims live during the
  gap while the old source is already closed. Ticks are deliberately NOT wiped
  at that point; the debounce exists to avoid flicker and the stale marker now
  carries the honesty.
- `LiveQuoteProvider` also gains a membership check: a frame whose
  `instrument_id` is not in the subscribed canonical set is dropped, matching
  the single-instrument hook's existing defensive filter.

### Rendering

- `SummaryStrip`: the pulse renders only when the tick is authoritative. A
  retained tick renders with a static amber dot plus a `title` and an `sr-only`
  label naming the state ("Price stream reconnecting — showing last received
  price" / "Price stream unavailable — showing last received price"), per the
  safety-state-ui rule. Source coupling is unchanged, because the primary never
  switches back to REST — only its freshness marking changes.
- `LivePriceCell`: a retained tick renders in the same muted class the fallback
  already uses, with a `title`. No extra glyph — the muted/normal distinction
  already exists in this component and a table cell gains nothing from one.

## Tests

`useLiveQuote.test.ts`, `LiveQuoteProvider.test.tsx`, `LivePriceCell.test.tsx`,
`SummaryStrip.test.tsx`, plus a new `liveQuoteConnection.test.ts` for the pure
policy.

Acceptance sequence, asserted on **rendered price and badge**, not just flags:

1. open → tick → transient failure (`readyState CONNECTING`) → reopen with **no
   new frame**. Asserts: `connected` false and `unavailable` false while
   reconnecting; after the reopen `connected` is true again but the price is
   still stale-marked and the pulse is still absent; and only once a new frame
   lands does the pulse return. This is the reconnect-resurrection case.
2. permanent failure (`readyState CLOSED`) → `unavailable`, no pulse, tick
   stale-marked, price still readable.
3. `id → null` → tick cleared, status `idle`, previous instrument's price gone
   on the render itself, not only after the effect.
4. `id A → id B` → no render shows A's price under B's id.
5. open with no frame at all → REST fallback on screen, **no pulse**.
6. error with `CONNECTING` *before* any successful open → status `connecting`,
   not `reconnecting`.
7. Provider: canonical-set change → status leaves `live` immediately, before
   the debounce fires; empty set → `idle` with ticks cleared; a frame for an
   id outside the subscribed set is dropped.
8. The existing transient-error test is fixed to **open the stream first** —
   without an `open` it could never have caught bug (1), which is why it did
   not.

## Known, accepted, and out of scope (Codex ckpt-1)

- **SSE health is not upstream quote health** (`app/api/sse_quotes.py:287`
  keeps heartbeating when the eToro subscriber is absent). A frozen upstream
  feed on a healthy stream still reads as live. That needs a tick-age or
  backend-supplied feed-health signal and is a separate ticket, not a rename of
  this one.
- **A union change resets every instrument's tick**, including quiet members
  that will not re-tick soon. Preserving the intersection is a behaviour change
  with its own tradeoffs; not bundled here.
- **`StrategyPositions.tsx:34` prefers retained ticks for value/P&L with no
  stale marking**, and **`InstrumentPage.tsx:690` runs a provider for the chart
  while `SummaryStrip` opens its own `useLiveQuote`**, so that page holds two
  streams and two refcounts. Both pre-existing, both outside the two files this
  ticket names.
- **Timestamp-based fallback precedence** — needs an as-of on the REST price or
  a refetch cadence; see above.

## Blast radius

`connected` / `unavailable` keep their current meaning and shape, so the other
six consumers are untouched. `status` and the freshness flag are additive.
