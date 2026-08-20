-- 252_sec_form25_register.sql
--
-- #2282 stage 2c — the SEC Form 25 / 25-NSE delisting register.
--
-- Source rule: 17 CFR 240.12d2-2. Recipe and measured traps in
-- `.claude/skills/data-sources/sec-edgar.md` §2.6; the parser is
-- app/services/sec_form25_register.py.
--
--
-- WHY A REGISTER AND NOT A `delisted` FLAG
-- ---------------------------------------------------------------------------
-- ⚠ (b) and (a)(3) are NOT the same event for a backtest. (b) is an
-- exchange-initiated delisting for non-compliance — a failure, where holders
-- were left with something close to nothing. (a)(3) is a merger or holdco
-- reorganisation, where the instruments now evidence OTHER securities by
-- operation of law and holders received cash or stock. A survivorship study
-- that treats those alike will report the acquisition premium as a loss.
--
-- No vendor's flat "delisted" flag can distinguish them. The rule provision
-- can, and it is free. That distinction is most of what this register is for,
-- which is why the provision is stored rather than collapsed into a boolean.
--
--
-- GRAIN: ONE ROW PER FILING, NOT PER ISSUER
-- ---------------------------------------------------------------------------
-- A Form 25 is per-SECURITY (§2.6 trap 2). Berkshire Hathaway filed two in
-- 2023 and both were bonds. Keying this table on issuer would assert that
-- Berkshire delisted in January 2023 — which is exactly the failure the
-- register exists to avoid, so `accession_number` is the key and
-- `description_class_security` is carried alongside so a consumer can see WHAT
-- came off the tape.
--
-- `accession_number` is also the de-duplication key for EDGAR's own indexing:
-- a 25-NSE appears in form.idx under BOTH the exchange CIK and the issuer CIK,
-- so 2023's 2,437 index rows are 1,282 filings.

CREATE TABLE IF NOT EXISTS sec_form25_register (
    accession_number TEXT PRIMARY KEY,
    form             TEXT NOT NULL
        CHECK (form IN ('25', '25-NSE', '25/A', '25-NSE/A')),
    filed_date       DATE NOT NULL,

    -- The filer, when an exchange filed it. NULL on issuer-filed Form 25.
    exchange_cik     TEXT,
    exchange_name    TEXT,

    -- The subject. NULL only when the submission carried no parseable issuer
    -- block, which is itself worth being able to count.
    issuer_cik       TEXT,
    issuer_name      TEXT,
    file_number      TEXT,

    -- WHAT was struck from the tape. Very often a note, warrant, unit or
    -- preferred rather than the common stock.
    description_class_security TEXT,

    -- Normalised paragraph, e.g. '(a)(3)'. NULL is a REAL and expected state:
    -- an issuer-filed Form 25 is paragraph (c) — voluntary withdrawal — and is
    -- a different document shape carrying no <ruleProvision> at all. 128 of
    -- 2023's filings are that case and every one of them is a delisting, so a
    -- NULL here must never be read as "unparseable".
    rule_provision   TEXT,
    provision_class  TEXT NOT NULL
        CHECK (provision_class IN ('equity_delisting', 'debt_lifecycle', 'unknown')),

    -- ⚠ SECOND, ORTHOGONAL AXIS — and the reason a cohort built on the
    -- provision alone is roughly twice the size it should be.
    --
    -- `provision_class` classifies the EVENT. It says nothing about WHAT came
    -- off the tape, because a Form 25 is per-SECURITY. Provision-filtering
    -- 2023 gives 842 "delisting-meaning" filings, of which (measured):
    --
    --     common_equity 317     warrant  155     unknown (issuer-filed) 128
    --     fund          111     unit      62     preferred              56
    --     debt           10     right      3
    --
    -- The `fund` class is its own category, not noise: #2289 established that
    -- the validated universe is US stocks EX-ETF (6,733 of 7,288, because
    -- `us_equity` is an exchange class carrying 555 ETFs). ETFs and closed-end
    -- funds name the PRODUCT rather than the security class -- "Invesco DB
    -- Gold Fund", "The Cannabis ETF" -- so they carry none of the words the
    -- other classes look for. A fund closing is not a company delisting.
    --
    -- A warrant expiring worthless alongside its SPAC is not a company
    -- delisting, and 15 of those (a)(3)/(b) filings describe NOTES — the
    -- provision is a delisting provision but the security is debt.
    --
    -- 'unknown' is a real, unavoidable value: an issuer-filed Form 25
    -- (paragraph (c)) carries no <descriptionClassSecurity> element at all, so
    -- all 128 of 2023's land here. Assuming common stock for them would add
    -- 128 unverified names to a register whose entire value is being
    -- trustworthy about what delisted.
    security_class   TEXT NOT NULL
        CHECK (security_class IN ('common_equity', 'warrant', 'unit',
                                  'preferred', 'debt', 'right', 'fund',
                                  'unknown')),

    signature_date   DATE,

    -- ⚠ The LAST TRADABLE DAY, and only where the filing actually states one.
    -- §2.6 trap 5 is right that a Form 25 can carry three distinct dates —
    -- filing, removal-effective and suspension — but the sentence separating
    -- them lives in the EX-99 rule-provision exhibit, and most exchange
    -- filings attach a stub. NULL therefore means "this filing did not say",
    -- NOT "same as filed_date". Back-filling it from filed_date would
    -- mistruncate every series it touched, which is why
    -- research_price_series.delisting_date is CHECK-paired to its source.
    suspension_date  DATE,

    -- Ticker resolution (§2.6 trap 4). SEC drops `tickers` to [] on delisting
    -- and companyconcept/…/dei/TradingSymbol.json 404s, so the symbol comes
    -- from the cover-page inline XBRL of the last periodic report filed BEFORE
    -- the delisting. The accession it came from is stored: a resolution whose
    -- evidence cannot be re-fetched is not auditable.
    resolved_symbol         TEXT,
    symbol_source           TEXT
        CHECK (symbol_source IN ('cover_page_xbrl', 'manual')),
    symbol_source_accession TEXT,

    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sec_form25_register_issuer_cik_padded
        CHECK (issuer_cik IS NULL OR issuer_cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_form25_register_exchange_cik_padded
        CHECK (exchange_cik IS NULL OR exchange_cik ~ '^[0-9]{10}$'),
    CONSTRAINT sec_form25_register_symbol_evidenced
        CHECK ((resolved_symbol IS NULL) = (symbol_source IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_sec_form25_register_issuer
    ON sec_form25_register (issuer_cik)
    WHERE issuer_cik IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sec_form25_register_class
    ON sec_form25_register (provision_class, security_class, filed_date);

CREATE INDEX IF NOT EXISTS idx_sec_form25_register_symbol
    ON sec_form25_register (resolved_symbol)
    WHERE resolved_symbol IS NOT NULL;

COMMENT ON TABLE sec_form25_register IS
    'Rule 12d2-2 delisting notifications, ONE ROW PER FILING (a Form 25 is '
    'per-security, not per-issuer — Berkshire filed two in 2023 and both were '
    'bonds). Keeps the rule provision because (b) is a failure and (a)(3) is '
    'an acquisition, a distinction no vendor delisted flag carries.';

COMMENT ON COLUMN sec_form25_register.suspension_date IS
    'Last tradable day, where the filing states one. NULL means the filing did '
    'not say — NOT "same as filed_date". A Form 25 carries up to three dates '
    'and only this one truncates a price series correctly.';

-- The census #2282 acceptance item 2 asks for: how many filings, of which
-- provision class, for how many distinct issuers, and how many resolved to a
-- ticker. Reports the UNRESOLVED side too — §2.6's known cohort bias is that
-- resolution drops closed-end funds and foreign private issuers, and a
-- coverage figure that hides its own failures is the thing #2282 exists to
-- prevent.
CREATE OR REPLACE VIEW sec_form25_register_census AS
SELECT
    date_trunc('year', filed_date)::date            AS filed_year,
    provision_class,
    security_class,
    rule_provision,
    count(*)                                        AS filings,
    count(DISTINCT issuer_cik)                      AS issuers,
    count(*) FILTER (WHERE resolved_symbol IS NOT NULL)  AS filings_with_symbol,
    count(*) FILTER (WHERE suspension_date IS NOT NULL)  AS filings_with_suspension_date
FROM sec_form25_register
GROUP BY 1, 2, 3, 4;

COMMENT ON VIEW sec_form25_register_census IS
    'Form 25 register coverage by year, rule provision AND security class. '
    'Both axes are needed: (a)(1)+(a)(2) are debt lifecycle and are 34.3% of '
    'filings, and of the remainder a further 296 are warrants, units, '
    'preferred or notes rather than common equity. A census on either axis '
    'alone overstates delistings.';

-- The common-equity delisting cohort — what the vendor acceptance test is
-- built from, and what "382 US common-equity delistings of 2023" means.
-- BOTH filters, and the residue is stated rather than absorbed.
CREATE OR REPLACE VIEW sec_form25_common_equity_delistings AS
SELECT *
FROM sec_form25_register
WHERE provision_class = 'equity_delisting'
  AND security_class = 'common_equity';

COMMENT ON VIEW sec_form25_common_equity_delistings IS
    'Provision-filtered AND security-filtered. Excludes the 128 issuer-filed '
    'Form 25s, whose security class is unknown by construction (paragraph (c) '
    'filings carry no descriptionClassSecurity) — they are delistings, they '
    'are simply not verifiable as COMMON-EQUITY delistings, and inflating the '
    'cohort with unverified names would defeat its purpose as an acceptance '
    'test.';
