# Nasdaq halt feed: live validation and safety wiring

Date: 2026-08-10  
Issue: #2507

## Outcome

The existing Nasdaq Trader halt parser and bounded tables were not an operating
data source: no scheduler/runtime invoker referenced the service and both dev
tables were empty. The source is now a five-minute, operator-visible job on an
independent `nasdaq` lane during 09:00 ET through regular/early close plus 15
minutes. The demo execution cycle also refreshes the same primary source
immediately before entry evaluation, so job ordering cannot turn an old halt
snapshot into trading authority.

A feed snapshot can only refuse an entry. It cannot create a signal, promote a
candidate or authorise an order.

## Live-source findings

Fixture tests had missed two provider-valid shapes. The first real request
failed closed because active halts may have `ResumptionDate` populated while
`ResumptionTradeTime` remains empty. After correcting that contract, the next
request failed closed because live resumption times may omit fractional
seconds. Both variants are now explicit fixtures; a trade time without a date
still refuses.

The first successful tracked live run recorded:

```text
source_pub_at=2026-08-10T13:54:51Z
items=38
active halts=19
job status=success
```

The preceding malformed-shape run remains durably visible as `failure` rather
than being overwritten by the success.

## Freshness, connections and storage

- `source_pub_at` older than five minutes is refused; a new HTTP fetch cannot
  make a cached provider payload look fresh.
- Feed state is row-locked and a publication timestamp may never regress, so
  concurrent scheduled/pre-execution refreshes cannot replace newer state.
- HTTP fetch and parse complete before a database connection is acquired.
- One feed-state row is updated in place.
- Halt rows use the provider identity `(source, symbol, halt_at)` and are
  upserted as resumption information arrives.
- Resumed halt rows expire after 90 days; active halts remain until the source
  supplies a resumption.
- No polling payload history, heartbeat heap or derived indicator series is
  retained.

This closes the empty-source execution-safety defect. It does not validate a
trading strategy or solve historical event coverage for the mechanism
classifier.
