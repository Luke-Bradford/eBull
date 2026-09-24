-- 421_def14a_recipient_other_cover.sql
--
-- #2351 slice 2b — a warrant / preferred sibling's class, proven by one proxy's
-- point-in-time cover, also suppresses the DEF 14A rows fanned to it by the issuer's
-- other proxies (including ones whose own cover does not list it yet). Those rows
-- carry reason 'non_common_sibling_other_cover'; their cover_* / witness_* columns name
-- the cover that proved the class, which may post-date the proxy.
--
-- Spec: docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md (slice 2b).

ALTER TABLE def14a_recipient_suppressions
    DROP CONSTRAINT IF EXISTS def14a_recipient_suppressions_reason_check;
ALTER TABLE def14a_recipient_suppressions
    ADD CONSTRAINT def14a_recipient_suppressions_reason_check
    CHECK (reason IN ('non_common_sibling', 'non_common_sibling_other_cover'));
