# Hook notes

Suggested hooks:

## pre-trade
Run before any write-capable trading action.
Checks:
- live mode enabled
- account mode allowed
- thesis freshness
- concentration limits
- stale-data flags
- kill switch

## post-trade
Run after order success/failure.
Actions:
- write audit row
- update ledger
- emit alert
- refresh positions

## post-refresh
Run after ingestion jobs.
Checks:
- row counts
- null spikes
- stale timestamps
- missing critical fields
