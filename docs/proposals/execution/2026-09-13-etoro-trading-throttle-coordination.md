# eToro trading-quota coordination (#2946)

Status: proposal — **narrowed after Codex ckpt-1 killed the first design.** Refs #2946.
Related #168 (resilience), #726 (the race this restores the fix for), #1484 (rate gates),
#2602 (depends on timely reconciliation), #2277 (portal drift — not this).

## Question

Does per-provider throttling let API / jobs / research readers exhaust the same eToro
user-key quota and delay order reconciliation, and what is the smallest correction?

## What is verified at `6ce7dcf0`

The ticket records three facts. All three hold:

1. **`etoro_broker.py:204` creates a fresh `shared_ts` per provider instance**, so two
   providers on the same user key do not share a budget at all.
2. **The read and write `ResilientClient`s share that timestamp but NOT a lock** —
   `shared_throttle_lock` is never passed (`etoro_broker.py:205-214`). This contradicts
   `ResilientClient`'s own documented contract (`resilient_client.py:86-95`): *"providers
   sharing a `shared_last_request` list also need to share this lock to keep the throttle
   atomic across instances. We surface that via the `shared_throttle_lock` parameter —
   callers that share a clock pass the same lock object."*
3. **No `RateGate` is injected**, so nothing coordinates across processes.

Floors are `_ETORO_READ_INTERVAL_S = 1.1` and `_ETORO_WRITE_INTERVAL_S = 3.5`
(`etoro_broker.py:101-102`).

### Item 2 is a reproducible burst, not a latent risk

Four concurrent `_throttle_and_stamp()` calls across a read/write pair sharing one clock,
fake httpx client, no broker request, floor set to 0.5 s for both so the gaps are readable:

| configuration | fire times (s) | gaps (s) | gaps below floor |
| --- | --- | --- | --- |
| unshared lock (today) | 0.000, 0.623, 0.623, 1.249 | 0.623, **0.000**, 0.626 | **1 of 3** |
| shared lock | 0.000, 0.641, 1.291, 1.941 | 0.641, 0.650, 0.650 | 0 of 3 |

Two requests fired at the same measured instant against a 0.5 s floor. This is the #726
check-and-write race reintroduced across the two instances that share the clock. The
reproducer is committed as a test rather than left as a transcript, so the timing claim is
re-runnable (see Tests).

**Five sibling call sites already pass the lock** — `etoro.py:129-130`,
`sec_edgar.py:315,323`, `sec_fundamentals.py:674`, `finra_regsho.py:87`,
`finra_short_interest.py:99`. ⚠ The SEC ones also inject a gate, and
`_throttle_and_stamp` short-circuits on a gate (`resilient_client.py:156-158`), so their
clock/lock pair is bypassed and they are not live examples of the mechanism. The live
examples are the two FINRA providers and `etoro.py`. `etoro_broker.py` is the only eToro
provider that shares a clock without the lock.

## The fix, and the two designs rejected on the way

**Ship: pass the same `threading.Lock` to both clients, per provider instance.** One line
plus the lock. It restores the documented contract and closes the burst.

Cost, stated rather than glossed: `_throttle_and_stamp` **sleeps inside the lock**
(`resilient_client.py:163-167`), so a write holding the lock across its 3.5 s sleep blocks
a read that owed only 1.1 s — head-of-line blocking of up to **2.4 s per write**. That is
acceptable here and it is a deliberate trade:

- it is **over**-restriction, the safe direction for a rate limit;
- writes are rare (an order submission), reads are the frequent path, so the expected cost
  is 2.4 s per submission, not per poll;
- the alternative — bursting past the floor — risks a 429 whose `Retry-After` can exceed
  2.4 s by a wide margin, and an unshared lock does not bound the burst at all.

### Rejected: reserve-under-lock, sleep-outside (a pooled `RateGate`)

`InProcessFloorGate` (`rate_gate.py:42-79`) reserves under the lock and sleeps outside, and
a pooled variant carrying a per-caller floor would avoid the 2.4 s blocking. **Codex ckpt-1
killed it and was right.** A reservation model lets each queued caller advance the shared
horizon before it has sent anything, so **a research burst can book the next-free time
arbitrarily far ahead and reconciliation lands behind the whole reserved queue.** That is
precisely the failure #2946 step 3 forbids — *"avoid starving reconciliation behind
research"* — and it is strictly worse than 2.4 s of blocking, because it is unbounded. The
legacy sleep-inside-lock form cannot express that failure: each caller waits only from the
last *actual* fire, so there is no queue to get stuck behind.

Two further problems with the same design: `compute_wait` ignores its `floor` argument
(`rate_gate.py:26-33`), so a naive integration would omit the floor entirely; and
`last_fire_at` would silently come to mean "last reserved", changing the semantics for
callers that reserve and then never send.

### Rejected: a per-user-key registry shared across provider instances

This looked like the answer to item 1 and it is not safe as a drop-in. **Eligibility and
what-if costs ride the WRITE client** (`etoro_broker.py:857`, `:895` — both
`self._http_write.post`), and the portal gives those endpoints their **own dedicated
quotas**, separate from the order-write pool. Keying one pool per credential would
therefore make a research instance's eligibility census delay every other same-key
instance's order writes, on endpoints the portal documents as independent. Fixing item 1
requires splitting the lanes by quota family first; doing it before that trades a bounded
intra-instance burst for cross-instance contention the source rule says should not exist.

## Source rule, and where it is unsettled

`.claude/skills/data-sources/etoro-api.md` § rate limits: quotas are **per user key over a
rolling minute**; order writes are 20/min pooled with related trading endpoints;
eligibility and what-if costs have **dedicated** quotas; market data is a separate shared
family.

⚠ **The "trading family" is not the clean partition this design initially assumed, and that
is why item 1 is deferred rather than fixed.** Raised by Codex ckpt-1 and not yet settled
against the live portal (the skill's protocol requires per-endpoint verification with a
recorded spec version, which this audit has not done):

- the TP/SL modify **PATCH** is documented on the default shared quota, not the 20/min
  write pool, yet rides `_http_write`;
- **trading history** uses the default shared quota, which it shares with endpoints outside
  trading entirely, so no broker-scoped coordinator can bound that family;
- the general rate-limit page and the per-endpoint index **disagree** on the market-data
  number, so precedence between them has to be established before either is cited.

Settling these is the prerequisite for lane-splitting and for any cross-process gate, and
it is the recommended next step on #2946 rather than part of this change.

## Out of scope, with reasons

- **Cross-process coordination.** `PostgresFloorGate` exists but is not ready-made here: it
  needs seeded budget rows and only `sec` is seeded, and its DB-failure fallback is a
  globally shared 0.11 s floor (`postgres_rate_gate.py`), which applied to eToro would be a
  fail-*open* floor 30× below the read interval. Adopting it needs its own ticket.
- **The same-process bypass.** `app/api/broker_credentials.py:634` calls trading PnL through
  a raw `httpx.Client`, outside any provider throttle, so it is invisible to this and to any
  future gate. Reported on the issue; not fixed here because it is a different call path.
- **A shared 429 cooldown.** `Retry-After` currently pauses only the failing request while
  siblings keep consuming the affected quota. Real, and out of scope.
- **No real broker request, no trading write, no measurement against a live quota.**

## Tests (pure, no DB, no network)

- **The race regression**, as an executable reproducer with a synchronisation barrier so it
  is not timing-luck: N threads across a read/write pair sharing one clock and one lock;
  assert no two acquisitions are stamped closer than the floor. The same test with
  independent locks must show a violation, so the test proves the *lock* is what closes it
  rather than passing for an unrelated reason.
- **The provider wires it**: both `_http_read` and `_http_write` on one
  `EtoroBrokerProvider` hold the *same* lock object and the *same* clock list — identity
  assertions, so a future refactor cannot silently return to per-client locks.
- **Different instances stay independent**: two providers hold different lock objects. This
  documents item 1 as still open rather than leaving its absence untested.
