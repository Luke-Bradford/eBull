# R6 cost-by-size-band implementation plan (#2907)

1. Freeze and hash the declaration before any value query.
2. Add a typed read-only verifier in `scripts/verify_2907_cost_by_size_band.py`.
   Reuse the validated-universe loader and frozen cost-model API; do not copy
   either definition.
3. Add focused unit tests for band boundaries, nearest-rank percentiles, exact
   unchanged-price arithmetic, coverage conservation, both haircut thresholds,
   and the p95 tail-warning suffix.
4. Add one database test that asserts every current validated-universe ID is
   conserved into the Cartesian cap-band/price-status census, without writing.
5. Commit cleanly, run the verifier once, and have that one evidence object emit
   both JSON and Markdown under `docs/proposals/ta/` for the result commit.
6. Run the normal PR review, resolve every comment with prevention, run the
   gates selected by the repository's review-intensity ladder, push and merge.
7. Post #2907's declaration hash, live measurement timestamp, population and
   coverage, size-band numbers, both haircut comparisons and verdict. Then
   continue to #2914; do not start a Tier 2 arm.
