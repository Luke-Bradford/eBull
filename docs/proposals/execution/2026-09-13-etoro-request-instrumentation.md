# #2946 step 3 item 3 — instrument the eToro request path

Status: SPEC (2026-09-13). Scope: step 3 item 3 only. Items 1 (lane-G history floor),
2 (lane split) and 4 (openapi content hash) are explicitly NOT in this unit.

## The gap this closes

Step 2 (`2026-09-13-etoro-quota-load-census.md`) could not report an observed request
rate for any eToro lane, because **there is no per-request artefact for any eToro lane**.
Every number in that census is CONFIGURED (a constant read by import), DERIVED (a bound
from a constant) or UNCOUNTED (named, never zero-filled). Its item-4 verdict is
`insufficient_evidence` *in both directions* — 0 observed 429s is a lower bound taken in
one process, and the largest traffic sources are uncounted by construction.

Step 3's own close-out names this as the remaining work:

> **Instrument the eToro path** — still the smallest change that turns every DERIVED
> number into an OBSERVED one: `ResilientClient` already accepts `on_429`
> (`resilient_client.py:74`) and **only SEC providers wire it**, so eToro 429s increment
> no counter anywhere. This is also the evidence that would move clause 2 off
> `insufficient_evidence`.

## Source rule

**Lane assignment is fixed by `app/providers/implementations/etoro_quota_lanes.py`**, the
portal census taken in step 1 (`2026-09-13-etoro-quota-lane-map.md`). This spec adds no
lane knowledge of its own:

- `CALL_SITES` supplies `(verb, demo_path, lane)` for every throttled endpoint.
- `KNOWN_UNTHROTTLED` supplies the same for the four raw-`httpx` sites.
- Nothing is hand-listed. A new endpoint that is not in the table classifies as
  `unclassified` **carrying its path verbatim** — never guessed into a neighbouring lane,
  and never prettified (#2844's refusal-label lesson).

That is the whole of the data-treatment decision here: a classifier over a request path
into a closed vocabulary, resolved from the source's own documented table rather than
reasoned out.

## Design

### 1. `ResilientClient` gains one optional observer hook

```python
on_attempt: Callable[[RequestAttempt], None] | None = None
```

Called **exactly once per HTTP attempt**, including every retry, **immediately around the
`send()` call** — not at the loop tail. `_request` leaves the loop three ways (`return` on
a non-retryable status, `continue` on a retry, and `raise_for_status()` on the final
429/5xx attempt), so a loop-tail hook would miss the exhausted-retry attempt, which is the
one that matters most for a quota question.

`RequestAttempt` carries `ts` (UTC), `method`, `url`, `status` (`int | None`), `error`
(`str | None`), `pre_request_wait_s`, `attempt` (0-based).

- Per ATTEMPT, not per logical call, because a rolling quota counts stamps and a retry
  places one. Step 2's census asked for "with retries" for the same reason.
  ⚠ An attempt is **HTTP traffic we issued**, which is evidence of quota spend and not
  proof of it: a connect-pool failure may never reach eToro, and how the broker accounts
  for a rejected request is unobservable from here.
- `status=None, error=<exception class>` on a transport failure: step 2's FINDING that a
  transport failure "writes no row and still spends quota" is exactly this case.
  ⚠ `httpx.Client.send` reads the body before returning, so a body-read failure on an
  already-received response records `status=None`. A 429 can hide there. Stated, not fixed.
- The hook records nothing when `build_request` or the gate raises — nothing was sent.
- `pre_request_wait_s` is measured around the whole `_throttle_and_stamp` call, so the
  `RateGate` branch is covered. ⚠ It is **wait before send, lock contention included**;
  it is not sleep time and it excludes retry backoff. Named for what it measures.
- `ts` is **issuance time**, captured immediately before `send()` and carried through both
  outcomes. Stamping on completion would make the rolling-window count a function of
  response latency: two requests dispatched at 0 s and 59 s but completing at 0 s and 61 s
  would report a maximum of one per minute when the quota saw two. A quota counts
  dispatches. (Caught at Codex checkpoint 2; the first draft stamped on completion.)
- `ts` is embedded in the record and in the log line. It is **not** taken from the log
  handler: the API and jobs processes format logs differently and the API's format carries
  no timestamp at all, so a reader that parses the handler's prefix cannot merge them.
- `on_429` is untouched. SEC keeps its counter; eToro does not wire `on_429` at all,
  because `on_attempt` already sees every 429 and the two would double-count. With
  `on_attempt=None` the code path is byte-for-byte the old one.

The hook is wrapped so an observer exception can never fail a request, and it is never
invoked while the throttle lock is held.

### 2. `app/providers/implementations/etoro_request_log.py` (new)

- `lane_for_request(verb, url) -> LaneMatch` — regexes compiled at import from
  `CALL_SITES` (`.verb` / `.demo_path`) **and** `KNOWN_UNTHROTTLED` (`.verb` / `.path` —
  a different accessor on each table, and getting that wrong silently empties one of
  them). Rules, in full:
  - Every literal is `re.escape`d and the regex is **fully anchored** (`fullmatch`).
  - `{param}` → `[^/]+`, except `{env}`, which is `(?:demo|real)` — the raw pnl entry is
    written `/api/v1/trading/info/{env}/pnl` and an unrestricted wildcard there would
    classify `/api/v1/trading/info/staging/pnl` as lane E.
  - A literal `demo` segment expands to `(?:/(?:demo|real))?` **including its leading
    slash**. Writing the optionality as `/(?:demo|real)?/` requires a literal `//` when
    the segment is absent and would fail every real-environment path. The three shapes
    (demo / real / absent) are the three `KNOWN_PATH_DRIFT` kinds.
  - A template ending in `...` is a **prefix template** (`KNOWN_UNTHROTTLED` uses one for
    the debug candle probe): everything before it must match and any remaining segments
    are accepted. Escaping the `...` makes the entry dead; leaving it unescaped makes it
    match any three characters. Neither is acceptable, so the marker is explicit.
  - Query string and fragment are dropped, an absolute URL is reduced to its path,
    repeated slashes collapse, a trailing slash is dropped, and the verb is upper-cased.
- Several table paths legitimately match more than one template (raw pnl vs the broker's
  pnl call site; the two `/api/v2/trading/execution/{env}/orders` submitters). Resolution
  is **first match in table order**, which is deterministic, and a request is recorded
  **once**. Acceptance 2 is the guard that no path matches two *different* lanes.
- Unknown → `LaneMatch(lane="unclassified", template=None)`. The record keeps both the
  normalised match path and the **verbatim input URL**, because normalising and "byte
  identical" cannot both be one field.
- `record_etoro_request(...)` — bumps a per-lane counter and emits **one structured log
  line** on logger `app.etoro.requests`, schema `v=1`, carrying `ts`, `lane`, `verb`,
  `path`, `status`, `err`, `wait_s`, `attempt`, `src` and `pid`.
  - `src` names the client (`broker_read` / `broker_write` / `market` / the raw site) and
    `env` names the broker environment, because **an eToro quota is per user key and per
    environment** and a lane total that merges them is not a budget comparison. No
    credential material is logged, ever — `src`/`env` are labels, not identity.
- `etoro_lane_counters()` / `reset_etoro_lane_counters()` — the in-process reader,
  guarded by one lock, snapshot-consistent.

### 3. Wiring

`etoro_broker.py` `_http_read` + `_http_write` and `etoro.py` `_http` pass
`on_attempt=` **at construction**. Wrapping `.get()` / `.post()` instead would count
logical calls and miss every retry, which is the quantity the whole unit exists for.

The four `KNOWN_UNTHROTTLED` raw-`httpx` sites (`broker_credentials.py` ×2,
`_debug_ws.py` ×2) go through one shared helper, `issue_raw_request`. It exists rather
than a per-site recipe because recording correctly at a raw site means two things that
are easy to get half-right one site at a time: **before** the caller's non-200 early
return, **and** on a raised request. The first draft did only the former, so a read
timeout arriving after eToro received the request vanished — undercounting exactly the
failure mode step 2 named. After this,
**every eToro REST request this repo issues is counted**; `KNOWN_UNTHROTTLED` stays a
throttle statement, not a visibility one. ⚠ WebSocket traffic is out of scope and stays
uncounted — it is a different transport and not a REST quota lane.

`RAW_HTTPX_EXPRESSION_COUNTS`'s existing AST drift test is extended so a new raw eToro
client in a named module fails the build until it is recorded and instrumented.

### 4. Reader — `scripts/measure_2946_quota_load.py --request-log <path>`

Accepts the flag more than once and merges by `ts`, because the API and the jobs daemon
log to different files and clause 2 is a cross-process question. Tabulates per
`(lane, src, env)`: attempts, 429s, transport failures, **max attempts in any rolling 60 s
window** — the OBSERVED counterpart of the census's DERIVED `floor(60/i) + 1` — and total
`wait_s` as a separate, differently-dimensioned column (seconds, not requests).

Malformed or unparseable lines are **counted and reported**, never silently skipped: a
reader that drops evidence turns missing data into "no traffic", which is the one error
that would falsely close this ticket. A line is therefore claimed by its **marker** first
and validated second — matching on the whole well-formed shape would let a truncated
record fall into the "unrelated log line" bucket and vanish.

`--out` works in this mode too, through the same tee as every other arm, so an automated
capture cannot silently keep a stale report.

A log line is the artefact and not a counter because the counters are **process-local**
and the two processes that matter do not share memory — which is precisely the shape of
clause 2 (two concurrent lane-B callers in separate processes).

## What this does NOT do

- No floor moves, no lane moves, no lane split, no coordinator, no cross-process gate.
- It does not close step 2 clause 2 (`test_two_concurrent_lane_b_callers_still_exceed_the_budget`
  keeps recording the shortfall). It produces the evidence that would decide whether to.
- It does not claim lane B is isolated from lanes A/C. Unchanged.
- It does not make the census a pinned artefact — that is item 4.
- It changes **no historical number**. Step 2's census stays DERIVED; this makes the
  *next* census observable.
- It does not observe offered demand, other consumers of the same key, or reconciliation
  latency (`strategy_order_reconciliation_state` still holds 0 rows).

## Acceptance

1. Every `CALL_SITES` entry's demo path classifies to its own lane; so does every
   `KNOWN_UNTHROTTLED` path and the real-env variant of each `KNOWN_PATH_DRIFT` method.
   Asserted by iterating the tables, never by a literal list.
2. No path in either table matches two different lanes.
3. An unknown path classifies `unclassified`, and the record's verbatim field is
   byte-identical to the input URL.
4. A 429 followed by a success records **two** attempts and one 429 on the right lane.
5. Retries exhausted by 429 record `max_retries + 1` attempts — including the final one,
   which raises.
6. `max_retries=0` records exactly one attempt.
7. A transport exception records one attempt with `status=None` and re-raises the
   original exception with its traceback intact.
8. A `build_request` failure records **nothing**.
9. SEC's `on_429` count, callback order and retry behaviour are unchanged, and the
   `on_attempt=None` path is identical to today's.
10. `record_etoro_request` raising cannot fail a request, and an import-time classifier
    failure is not silently swallowed (it is a test failure, not a runtime fallback).
11. Every raw `KNOWN_UNTHROTTLED` site records, including on its non-200 early return
    **and on a raised request**, asserted both directly on `issue_raw_request` and by an
    AST guard over `RAW_HTTPX_EXPRESSION_COUNTS`.
12. The tabulator's rolling-window count is boundary-explicit (a window is half-open,
    `(t - 60s, t]`) and malformed lines appear in the output as their own count,
    including truncated records that still carry the marker.
13. The recorded timestamp precedes `send()` entering, not returning.
14. `--request-log` honours `--out`.
