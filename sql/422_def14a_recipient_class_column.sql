-- 422_def14a_recipient_class_column.sql
--
-- #2351 slice 3 — a COMMON sibling of a dual-class issuer (GOOG beside GOOGL) is
-- suppressed for a proxy when every Item 403 row it holds for that proxy sits, in the
-- parser's own tables, under a caption naming a different class. Those rows carry
-- reason 'other_common_class_column'; cover_* name the sibling's own point-in-time
-- cover title, witness_* the sibling whose class the column is.
--
-- Spec: docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md (slice 3).

ALTER TABLE def14a_recipient_suppressions
    DROP CONSTRAINT IF EXISTS def14a_recipient_suppressions_reason_check;
ALTER TABLE def14a_recipient_suppressions
    ADD CONSTRAINT def14a_recipient_suppressions_reason_check
    CHECK (reason IN ('non_common_sibling', 'non_common_sibling_other_cover', 'other_common_class_column'));
