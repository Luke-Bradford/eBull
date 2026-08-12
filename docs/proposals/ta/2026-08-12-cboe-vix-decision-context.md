# Cboe VIX decision-context result

Issues: #2577, #2523, #2507. Source ingestion: #2574 / PR #2576.

## Result

The official bounded Cboe daily series is now a causally loadable regime input, not merely an available dataset. A strategy decision receives exactly one of:

- a complete close, as-known bar date and frozen source version for the prior NYSE session; or
- a named `missing_source` / `stale_source` refusal.

Date D is excluded on all decisions made on New York date D, including after the close. Weekends and NYSE holidays resolve to the previous open session. A context identity bump to `decision-context-v3` makes the new availability semantics explicit. Database constraints require eligible v3 rows to carry the complete VIX triplet and preserve v2/v3 point-in-time sector completeness.

## Storage and scope

No rolling VIX indicators, raw-history versions, new index, routine not-fired rows or per-instrument VIX copies are added. Only fired/refused candidate contexts may store the two new provenance scalars beside the existing numeric value. On the development database the pair measured 28 marginal bytes in a composite fixture; the empty table remained 49,152 bytes after migration.

This closes a context-integrity gap. It does not turn VIX into a directional signal, repair a rejected candidate, add a capital candidate, or authorise the four harness controls to trade.
