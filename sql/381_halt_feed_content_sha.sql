-- #3049 — record WHAT the halt feed published, separately from WHEN it says it did.
--
-- WHY THIS COLUMN EXISTS
--
-- ``strategy_halt_feed_state`` already stores ``payload_sha256``, and #3049's scope
-- proposed reusing it to tell "same content, older stamp" from a real content
-- rollback. It cannot: ``payload_sha256`` hashes the raw RSS bytes and ``<pubDate>``
-- is IN those bytes, so a re-serve carrying an older stamp has a different
-- ``payload_sha256`` BY CONSTRUCTION. Measured over 32 single-client polls on
-- 2026-09-14 (``scripts/probe_3049_halt_pubdate_skew.py``): 12 distinct pubDate
-- values, 12 distinct ``payload_sha256`` -- exactly one per stamp -- and only
-- **2** distinct halt-content fingerprints, one of which was served under 8
-- different stamps spanning 421 seconds.
--
-- ``content_sha256`` hashes the parsed halt identities only (symbol, halt_at,
-- market, reason_code, resumed_at), which is the content the feed exists to
-- publish and the only content ``strategy_market_halts`` stores. Two payloads
-- with the same fingerprint upsert byte-identical rows, so accepting the older
-- of the two overwrites nothing -- which is what makes #3049's fix a
-- by-construction one with no invented time tolerance (#2795's lesson).
--
-- NULLABLE ON PURPOSE. The existing row's content cannot be reconstructed:
-- ``strategy_market_halts`` accumulates across days under a 90-day retention and
-- is a superset of any single snapshot. A NULL therefore means "content unknown",
-- which compares unequal to every fingerprint and so refuses exactly as today --
-- fail-closed through the one poll it takes to populate.

ALTER TABLE strategy_halt_feed_state
    ADD COLUMN IF NOT EXISTS content_sha256 TEXT
        CHECK (content_sha256 IS NULL OR content_sha256 ~ '^[0-9a-f]{64}$');
