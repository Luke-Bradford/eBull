-- 053_filing_events_items.sql
--
-- 8-K ``items[]`` code typing (#431). ``submissions.json`` carries an
-- ``items`` array per filing (e.g. "1.01,2.03,9.01" for a material
-- agreement + off-balance-sheet arrangement + exhibit). We already
-- pull the submissions dict daily but discard this field — richer
-- than the bare ``form_type='8-K'`` signal we store today.
--
-- Adds:
--   1. ``filing_events.items`` — text array of raw item codes.
--   2. ``sec_8k_item_codes``   — lookup: code → human label +
--                                severity (informational / material /
--                                critical). Seeded at migration time
--                                with the SEC's published item set.
--
-- The cascade-trigger plumbing (fire thesis refresh on material 8-K)
-- is a follow-up; this migration just captures the codes.

ALTER TABLE filing_events
    ADD COLUMN IF NOT EXISTS items TEXT[];

CREATE INDEX IF NOT EXISTS idx_filing_events_items_gin
    ON filing_events USING GIN (items);


-- ---------------------------------------------------------------------------
-- sec_8k_item_codes lookup
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sec_8k_item_codes (
    code      TEXT PRIMARY KEY,
    label     TEXT NOT NULL,
    severity  TEXT NOT NULL CHECK (severity IN ('informational', 'material', 'critical'))
);

-- Seed the standard SEC 8-K item codes (Form 8-K General Instructions).
-- Severity is our editorial call — "material" items are the ones we want
-- the thesis cascade (#276) to react to; "critical" escalates further
-- (bankruptcy, delistings, auditor changes).
INSERT INTO sec_8k_item_codes (code, label, severity) VALUES
    ('1.01', 'Entry into a Material Definitive Agreement',        'material'),
    ('1.02', 'Termination of a Material Definitive Agreement',    'material'),
    ('1.03', 'Bankruptcy or Receivership',                        'critical'),
    ('1.04', 'Mine Safety — Reporting of Shutdowns and Patterns', 'informational'),
    ('1.05', 'Material Cybersecurity Incidents',                  'critical'),
    ('2.01', 'Completion of Acquisition or Disposition of Assets', 'material'),
    ('2.02', 'Results of Operations and Financial Condition',      'material'),
    ('2.03', 'Creation of a Direct Financial Obligation',          'material'),
    ('2.04', 'Triggering Events That Accelerate a Financial Obligation', 'material'),
    ('2.05', 'Costs Associated with Exit or Disposal Activities',  'material'),
    ('2.06', 'Material Impairments',                               'critical'),
    ('3.01', 'Notice of Delisting or Failure to Satisfy a Continued Listing Rule', 'critical'),
    ('3.02', 'Unregistered Sales of Equity Securities',            'material'),
    ('3.03', 'Material Modification to Rights of Security Holders', 'material'),
    ('4.01', 'Changes in Registrant''s Certifying Accountant',     'critical'),
    ('4.02', 'Non-Reliance on Previously Issued Financial Statements', 'critical'),
    ('5.01', 'Changes in Control of Registrant',                   'critical'),
    ('5.02', 'Departure/Election of Directors or Principal Officers', 'material'),
    ('5.03', 'Amendments to Articles of Incorporation or Bylaws',  'informational'),
    ('5.04', 'Temporary Suspension of Trading Under Employee Benefit Plans', 'informational'),
    ('5.05', 'Amendments to Code of Ethics',                       'informational'),
    ('5.06', 'Change in Shell Company Status',                     'material'),
    ('5.07', 'Submission of Matters to a Vote of Security Holders', 'informational'),
    ('5.08', 'Shareholder Director Nominations',                   'informational'),
    ('6.01', 'ABS Informational and Computational Material',       'informational'),
    ('6.02', 'Change of Servicer or Trustee',                      'material'),
    ('6.03', 'Change in Credit Enhancement or Other External Support', 'material'),
    ('6.04', 'Failure to Make a Required Distribution',            'critical'),
    ('6.05', 'Securities Act Updating Disclosure',                 'informational'),
    ('7.01', 'Regulation FD Disclosure',                           'informational'),
    ('8.01', 'Other Events',                                       'informational'),
    ('9.01', 'Financial Statements and Exhibits',                  'informational')
ON CONFLICT (code) DO UPDATE SET
    label    = EXCLUDED.label,
    severity = EXCLUDED.severity;


COMMENT ON COLUMN filing_events.items IS
    'SEC 8-K item codes (e.g. ARRAY[''1.01'',''9.01'']). Populated from '
    'submissions.json filings.recent[].items — comma-separated source '
    'split into an array. NULL for non-8-K or pre-#431 filings.';
COMMENT ON TABLE sec_8k_item_codes IS
    'Reference lookup for SEC 8-K item codes with editorial severity tier.';
