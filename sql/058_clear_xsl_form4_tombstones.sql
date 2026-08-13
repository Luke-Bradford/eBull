-- 058_clear_xsl_form4_tombstones.sql
--
-- One-off recovery for #454. Before the XSL-URL normalisation fix,
-- every Form 4 ingest attempt fetched the XSL-rendered HTML path
-- (``/xslF345X06/form4.xml``) instead of the raw XML. The parser
-- rejected the HTML (root was ``<html>`` not ``<ownershipDocument>``)
-- and the ingester wrote a tombstone for the filing. Result on dev:
-- 500 tombstones, zero real transactions.
--
-- With the URL-normalisation fix in place, these filings are
-- parseable — but the ingester's candidate selector skips accessions
-- that already have an ``insider_filings`` row, so the tombstones
-- lock the filings out permanently.
--
-- Recovery: delete every tombstone row where ``primary_document_url``
-- contains the XSL-rendering segment. The ingester's next pass picks
-- them up as fresh candidates and re-parses against the canonical
-- XML URL. Real 404/410 tombstones (no XSL segment in the URL) are
-- preserved so we don't re-fetch genuinely-dead URLs.
--
-- ON DELETE CASCADE on the child tables means any filers / footnotes
-- / transactions under these accessions are also cleared — but
-- tombstones by definition carry no child rows, so this is a no-op
-- beyond the parent filings.

DELETE FROM insider_filings
WHERE is_tombstone = TRUE
  AND primary_document_url IS NOT NULL
  AND primary_document_url ~ '/xslF345(?:X0[56])?/';
