-- 099_unresolved_13f_cusips.sql
--
-- Issue #781 — capture CUSIPs observed in 13F-HR holdings (#730)
-- that didn't resolve to an ``instruments`` row at ingest time, so
-- a downstream resolver can fuzzy-match by issuer name and populate
-- ``external_identifiers`` without re-fetching the SEC archive.
--
-- This table is the practical alternative to parsing the SEC's
-- quarterly Official List of Section 13(f) Securities (PDF-only,
-- no machine-readable feed). Every CUSIP that appears in any 13F-HR
-- filing must by definition be a 13F-eligible security, and we
-- already fetch each filing's holdings during the #730 ingest —
-- recording the unresolved (CUSIP, issuer name) pairs gives us the
-- same coverage as the official list with one notable exception:
-- securities that no curated 13F filer holds will never surface.
-- That gap is acceptable for v1 because the ownership card cares
-- about *held* positions; we'll never need to render an
-- instrument that nobody holds.
--
-- The resolver service (#781 PR 2 follow-up) walks this table and:
--   1. Fuzzy-matches ``name_of_issuer`` against
--      ``instruments.company_name`` using a similarity threshold.
--   2. Promotes confident matches into ``external_identifiers``
--      with ``provider='sec', identifier_type='cusip'``.
--   3. Drops the row from this table on successful promotion.
--
-- Schema decisions:
--
--   * Identity / dedupe = ``cusip`` only — same CUSIP in multiple
--     filings is the same unresolved row. ``observation_count`` is
--     incremented on each re-encounter so the operator can prioritise
--     resolution by frequency (CUSIPs held by many filers are more
--     valuable to map first).
--   * ``name_of_issuer`` carries the latest filer-supplied label.
--     Filers spell issuer names slightly differently across filings
--     (``"BERKSHIRE HATHAWAY INC"`` vs ``"BERKSHIRE HATHAWAY"`` vs
--     ``"BERKSHIRE HATHAWAY INC. CL B"``) — keeping the latest is a
--     pragmatic v1 choice; a future enhancement could store every
--     observed variant in a JSONB column for broader fuzzy matching.
--   * ``last_accession_number`` is informational — the operator can
--     trace back to the source filing without joining
--     ``institutional_holdings`` (whose unresolved rows aren't
--     persisted today).
--   * ``resolution_status`` tombstones rejection: when a resolver
--     run flags a CUSIP as unresolvable (no fuzzy match above
--     threshold), set to ``'unresolvable'`` so the next run skips
--     it. Operator can clear the row to force a retry. NULL = not
--     yet attempted.

CREATE TABLE IF NOT EXISTS unresolved_13f_cusips (
    cusip                  TEXT PRIMARY KEY,
    name_of_issuer         TEXT NOT NULL,
    last_accession_number  TEXT NOT NULL,
    observation_count      INTEGER NOT NULL DEFAULT 1,
    resolution_status      TEXT
        CHECK (resolution_status IS NULL OR resolution_status IN (
            'unresolvable',   -- no candidate met the similarity threshold
            'ambiguous',      -- two or more candidates tied at the top
                              -- score; needs manual disambiguation (e.g.
                              -- Alphabet Inc CL A vs CL C — same
                              -- normalised name, different instrument_id)
            'conflict',       -- external_identifiers already maps this
                              -- CUSIP to a DIFFERENT instrument_id; the
                              -- resolver refuses to silently overwrite
            'manual_review'   -- operator-set flag for cases the
                              -- automated path can't handle
        )),
    first_observed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hot path for the resolver: unresolved (status IS NULL) ordered
-- by frequency DESC so the operator sees the highest-leverage
-- CUSIPs first.
CREATE INDEX IF NOT EXISTS idx_unresolved_13f_cusips_pending
    ON unresolved_13f_cusips (observation_count DESC, last_observed_at DESC)
    WHERE resolution_status IS NULL;

-- Hot path for "what did this filing's resolver pass leave open":
-- per-accession scan for the operator-facing tooling.
CREATE INDEX IF NOT EXISTS idx_unresolved_13f_cusips_accession
    ON unresolved_13f_cusips (last_accession_number);
