-- 059_instrument_business_summary_sections.sql
--
-- 10-K Item 1 "Business" subsection normalisation (#449). The 055
-- migration captured the whole Item 1 narrative as a single TEXT
-- blob on ``instrument_business_summary.body``. That blob is useful
-- for a fallback render but violates the operator's "every
-- structured field lands in SQL" rule: every subsection of Item 1
-- (Segments, Products, Competition, Human Capital, Regulatory, R&D,
-- IP, Properties, etc.) is queryable in the source HTML as a
-- headed block — we must store each as its own row, not grep-search
-- a glued blob.
--
-- Storage model:
--
--   instrument_business_summary (unchanged)
--     └── instrument_business_summary_sections
--           — one row per (instrument, source_accession, section_order)
--           — preserves source ordering so a renderer can walk the
--             10-K's own layout
--           — section_key canonicalises the heading to a stable
--             identifier ("human_capital", "competition") for filter /
--             compare logic; unmapped headings land as
--             section_key='other' with section_label preserving the
--             original text (no silent drop)
--           — cross_references JSONB captures every "see Item 7",
--             "see Exhibit 21", "refer to Note 15" pointer as a
--             structured list so downstream features (thesis engine,
--             link extraction) don't have to re-scan body text
--
-- The old blob ``instrument_business_summary.body`` remains as the
-- compatibility surface for the current UI and a safety net while
-- the section-aware UI ships. Future migrations may drop it once
-- every caller reads from the sections table.

CREATE TABLE IF NOT EXISTS instrument_business_summary_sections (
    id                  BIGSERIAL   PRIMARY KEY,
    instrument_id       BIGINT      NOT NULL
                            REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- SEC accession this section was extracted from. Pairs with the
    -- parent ``instrument_business_summary.source_accession`` — we
    -- don't FK across because a section row may exist for an
    -- accession whose parent blob was truncated / rejected.
    source_accession    TEXT        NOT NULL,
    -- Source-order within Item 1. The ``General / Overview`` block
    -- (when present) is order 0 by convention; numbered subsections
    -- that follow get 1, 2, 3... in the filing's own layout.
    section_order       INT         NOT NULL,
    -- Canonical identifier for the section. Normalised from the
    -- heading text to a fixed vocabulary so cross-issuer queries
    -- ("how many companies disclose Human Capital") don't depend on
    -- case / punctuation / wording. Known values include:
    --   general, overview, segments, products, services, customers,
    --   markets, competition, seasonality, backlog, raw_materials,
    --   manufacturing, ip, r_and_d, sales_marketing, regulatory,
    --   environmental, climate, human_capital, properties,
    --   corporate_info, available_information, history, strategy,
    --   other.
    -- "other" is the explicit catch-all for headings that don't
    -- match the canonical vocabulary — with the original heading
    -- preserved in ``section_label`` so no information is lost.
    section_key         TEXT        NOT NULL,
    -- The heading as it appeared in the filing, e.g. "Human Capital
    -- Resources", "Our People and Culture", "Government Regulation".
    -- Stored verbatim so the UI renders the issuer's own wording.
    section_label       TEXT        NOT NULL,
    body                TEXT        NOT NULL,
    -- Cross-references extracted from this section's body. Shape:
    --   [{"reference_type": "item",     "target": "Item 1A",  "context": "see Item 1A Risk Factors"},
    --    {"reference_type": "exhibit",  "target": "Exhibit 21", "context": "see Exhibit 21 for our list of subsidiaries"},
    --    {"reference_type": "note",     "target": "Note 15",  "context": "described in Note 15 to the consolidated financial statements"}]
    -- Empty list when no cross-references are present. Stored as
    -- JSONB rather than a child table because we only ever render /
    -- filter these next to the parent section — no join semantics.
    cross_references    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, source_accession, section_order)
);

CREATE INDEX IF NOT EXISTS idx_business_summary_sections_key
    ON instrument_business_summary_sections (section_key);

CREATE INDEX IF NOT EXISTS idx_business_summary_sections_instrument
    ON instrument_business_summary_sections (instrument_id, source_accession);

COMMENT ON TABLE instrument_business_summary_sections IS
    'Normalised 10-K Item 1 subsections per instrument per filing. '
    'Complements instrument_business_summary.body (whole-Item-1 blob) '
    'with per-section granularity so downstream consumers can query '
    '/ render individual subsections instead of grepping the blob.';

COMMENT ON COLUMN instrument_business_summary_sections.section_key IS
    'Canonical lower-snake-case identifier mapped from the filing '
    'heading. Known values: general, overview, segments, products, '
    'services, customers, markets, competition, seasonality, backlog, '
    'raw_materials, manufacturing, ip, r_and_d, sales_marketing, '
    'regulatory, environmental, climate, human_capital, properties, '
    'corporate_info, available_information, history, strategy, other. '
    '"other" means the heading did not match any known key — the '
    'original heading is still preserved verbatim in section_label.';

COMMENT ON COLUMN instrument_business_summary_sections.section_label IS
    'Heading as it appeared in the filing, verbatim. The UI renders '
    'this so the operator sees the issuer''s own wording, not the '
    'canonical key.';

COMMENT ON COLUMN instrument_business_summary_sections.cross_references IS
    'Inline list of cross-references found in this section''s body. '
    'Each entry is {reference_type, target, context}. reference_type '
    'is one of "item" (within same filing), "exhibit", "note", '
    '"filing" (other SEC filing), "part". target is the short '
    'canonical pointer ("Item 1A", "Exhibit 21.1", "Note 15"). '
    'context is the sentence-sized phrase the reference appeared in, '
    'for audit / linkification.';
