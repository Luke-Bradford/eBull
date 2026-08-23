# #2914 R6 operational rules plan

1. Freeze and hash the declaration before the reference-series census.
2. Add a pure turn-of-month window constructor over caller-supplied venue sessions.
3. Add a typed factor-valuation record that distinguishes a genuine spread from unavailable data and
   rejects return units.
4. Add a read-only full-population verifier over accepted #2912 reference snapshots and emit one
   canonical JSON/Markdown result.
5. Test calendar boundaries, malformed inputs, record invariants, and the database census.
6. Run the repository gates, review, merge, and post the non-return verdict to #2914 and Tier 1
   completion to #2899.
