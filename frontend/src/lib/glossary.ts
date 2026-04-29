/**
 * Operator-facing glossary of abbreviations used across the eBull
 * UI (#684).
 *
 * Single source of truth so:
 *   - the in-flow `<Term>` component renders consistent tooltips for
 *     every wrapped abbreviation
 *   - the standalone `/glossary` page renders the same data as a
 *     scannable table
 *   - SEC filing-type chips can fall back to the friendly short name
 *     when the extracted-summary field is empty (replaces the
 *     duplicate-chip rendering noticed by the operator on
 *     /instrument/IEP — `8-K  8-K`, `10-K  10-K` is now
 *     `8-K Material event`, `10-K Annual report`)
 *
 * Entries split into two namespaces:
 *   - SEC + financial-statement / accounting terms (CIK, SIC, ROE,
 *     P/E, etc.) — operator may not know without context
 *   - eBull-internal UI labels (Tier, NET 90d, PM/AH, Held: Nu) —
 *     short by design but inscrutable on first read
 *
 * Entry shape:
 *   - `term`: the rendered abbreviation/short string the operator sees
 *   - `shortName`: a human-readable expansion (≤6 words). Doubles as
 *     the friendly fallback for filing chips.
 *   - `what`: 1-sentence definition. Tooltip body.
 *   - `why`: 1-sentence "why an operator should care". Tooltip
 *     secondary line + glossary "Why it matters" column.
 *   - `learnMoreUrl` (optional): canonical reference for the curious.
 *
 * Sorted alphabetically by `term` so the glossary page renders
 * deterministically and a future contributor doesn't have to guess
 * where a new entry belongs.
 */

export interface GlossaryEntry {
  readonly term: string;
  readonly shortName: string;
  readonly what: string;
  readonly why: string;
  readonly learnMoreUrl?: string;
}

export const GLOSSARY: ReadonlyArray<GlossaryEntry> = [
  // -------------------------------------------------------------------
  // SEC filing form types
  // -------------------------------------------------------------------
  {
    term: "8-K",
    shortName: "Material event",
    what: "SEC filing reporting a material corporate event between regular periodic filings — earnings, M&A, executive changes, dividend declarations, bankruptcies.",
    why: "Fastest-cadence US-issuer disclosure (within 4 business days of the event). Each 8-K's `items[]` codes (1.01, 2.02, 5.02, etc.) classify the event type so you can spot earnings vs leadership change at a glance.",
    learnMoreUrl: "https://www.sec.gov/forms",
  },
  {
    term: "8-K/A",
    shortName: "Material event amendment",
    what: "Amendment to a previously filed 8-K — corrections, retractions, or supplementary detail.",
    why: "Rare but important: a 8-K/A on a recent earnings or M&A 8-K usually means the original numbers/narrative changed.",
  },
  {
    term: "10-K",
    shortName: "Annual report",
    what: "SEC annual report — full audited financial statements, MD&A, risk factors, business description, executive compensation. Filed within 60-90 days of fiscal year end.",
    why: "Most-comprehensive single document on the issuer. Item 1 (business) + Item 1A (risks) + Item 7 (MD&A) drive thesis quality more than any other source.",
    learnMoreUrl: "https://www.sec.gov/forms",
  },
  {
    term: "10-K/A",
    shortName: "Annual report amendment",
    what: "Amendment to a previously filed 10-K. Common when the original missed the proxy section (Part III incorporated by reference) or restates financial numbers.",
    why: "A 10-K/A that restates financials is a yellow-flag signal — accounting discipline or audit issues.",
  },
  {
    term: "10-Q",
    shortName: "Quarterly report",
    what: "SEC quarterly report (filed for Q1/Q2/Q3 only — Q4 is rolled into the 10-K). Unaudited statements + MD&A.",
    why: "The cadence-driver of quarterly fundamentals. XBRL facts here populate the Fundamentals drill page's quarterly rows.",
    learnMoreUrl: "https://www.sec.gov/forms",
  },
  {
    term: "10-Q/A",
    shortName: "Quarterly report amendment",
    what: "Amendment to a previously filed 10-Q.",
    why: "Same yellow-flag reading as 10-K/A — a re-stated quarter is rare and worth investigating.",
  },
  {
    term: "20-F",
    shortName: "Foreign annual report",
    what: "Annual report for a foreign private issuer (FPI). Equivalent to a 10-K for non-US issuers listed on US exchanges (e.g. via ADRs).",
    why: "Where to look for ASML, TSM, NVO etc. — the FPI equivalent of the 10-K, with similar audit + risk-factor structure.",
  },
  {
    term: "20-F/A",
    shortName: "Foreign annual amendment",
    what: "Amendment to a previously filed 20-F.",
    why: "Same restatement signal as 10-K/A but on the FPI track.",
  },
  {
    term: "40-F",
    shortName: "Canadian annual report",
    what: "Annual report for a Canadian issuer using the multijurisdictional disclosure system (MJDS) to file on US exchanges.",
    why: "Smaller cohort (Shopify, BAM, etc.). Reads like a 10-K for thesis purposes.",
  },
  {
    term: "6-K",
    shortName: "Foreign interim report",
    what: "Interim report for a foreign private issuer — covers material events filed between annual 20-F filings (rough analog of an 8-K + 10-Q rolled together).",
    why: "Where FPI quarterly numbers actually land (not 10-Q). Cadence is less regular than US issuers.",
  },
  {
    term: "Form 4",
    shortName: "Insider transaction",
    what: "Statement of changes in beneficial ownership filed by directors, officers, and >10% shareholders within 2 business days of any trade.",
    why: "Drives the insider-activity drill page. Open-market buys are the strongest insider sentiment signal; tax-withholding (F) and grants (A) are mechanical.",
  },
  {
    term: "13D",
    shortName: "5%+ active holder",
    what: "Schedule 13D — disclosure required when an investor crosses 5% beneficial ownership AND intends to influence control (vs passive 13G).",
    why: "13D is loud — activist filings, takeover stake-builds. 13D/A amendments often flag stake increases or letters to the board.",
  },
  {
    term: "13G",
    shortName: "5%+ passive holder",
    what: "Schedule 13G — passive >5% disclosure (mutual funds, index providers). Less reporting overhead than 13D.",
    why: "Routine. A 13D-to-13G or 13G-to-13D conversion is the interesting signal.",
  },

  // -------------------------------------------------------------------
  // SEC entity / regulatory
  // -------------------------------------------------------------------
  {
    term: "CIK",
    shortName: "SEC entity ID",
    what: "Central Index Key — the SEC's stable 10-digit identifier for any entity that has ever filed with the agency.",
    why: "Primary join key for every SEC dataset (filings, XBRL, ownership). Survives ticker changes, spin-offs, mergers; ticker-based lookups don't.",
  },
  {
    term: "SIC",
    shortName: "Industry code",
    what: "Standard Industrial Classification — 4-digit US government industry code (e.g. 2911 = Petroleum Refining).",
    why: "Coarser than GICS but baked into SEC filings. Used as the peer-set anchor when richer industry data is missing.",
  },
  {
    term: "EDGAR",
    shortName: "SEC filing repository",
    what: "Electronic Data Gathering, Analysis, and Retrieval — the SEC's public filings database (since 1993).",
    why: "Where every primary-source document on this page comes from. The `SEC EDGAR` chip on the chart / company profile means we read this filing direct, not via a redistributor.",
    learnMoreUrl: "https://www.sec.gov/edgar",
  },
  {
    term: "XBRL",
    shortName: "Tagged financial data",
    what: "eXtensible Business Reporting Language — structured tagging system the SEC mandates for financial statements (since 2009).",
    why: "What lets us extract revenue / margins / debt as numbers (not PDFs). Tagged facts feed the Fundamentals drill page.",
  },
  {
    term: "Filer category",
    shortName: "SEC reporting size tier",
    what: "SEC's size classification — Large accelerated (>$700M public float), Accelerated ($75M-$700M), Non-accelerated, or Smaller reporting company.",
    why: "Drives filing deadlines (Large accelerated must file 10-K within 60 days of FY end; smaller filers get 90 days) and the level of audit scrutiny.",
  },
  {
    term: "FPI",
    shortName: "Foreign private issuer",
    what: "Non-US issuer using SEC's foreign-issuer track — files 20-F annually + 6-K interim, exempt from proxy and Reg FD.",
    why: "Different filing cadence and disclosure standards than US-domiciled issuers. Affects what data we can extract.",
  },

  // -------------------------------------------------------------------
  // Valuation / fundamentals ratios
  // -------------------------------------------------------------------
  {
    term: "P/E ratio",
    shortName: "Price / Earnings",
    what: "Current share price divided by trailing-twelve-month earnings per share.",
    why: "Most-cited valuation multiple. Negative when the issuer is loss-making (the chart shows -3.38 for BBBY today — meaning P / negative-E).",
  },
  {
    term: "P/B ratio",
    shortName: "Price / Book value",
    what: "Share price divided by book value per share (shareholders' equity / shares outstanding).",
    why: "Asset-heavy issuers (banks, REITs) trade closer to book than asset-light tech. P/B<1 with positive earnings can indicate undervaluation or hidden distress.",
  },
  {
    term: "ROE",
    shortName: "Return on equity",
    what: "Net income divided by shareholders' equity. The DuPont decomposition splits it into Net Margin × Asset Turnover × Equity Multiplier.",
    why: "Measures how efficiently the business converts each pound of operator capital into profit. >15% sustained is high quality; negative means losses are eating equity.",
  },
  {
    term: "ROA",
    shortName: "Return on assets",
    what: "Net income divided by total assets. Independent of capital structure (vs ROE).",
    why: "Compares profitability across companies with different leverage. Low ROA + high ROE = leverage doing the work.",
  },
  {
    term: "ROIC",
    shortName: "Return on invested capital",
    what: "After-tax operating profit (NOPAT) divided by invested capital (debt + equity). Strips out tax effects + capital structure.",
    why: "The cleanest profitability gauge for cross-company comparison. ROIC > weighted cost of capital = real value creation.",
  },
  {
    term: "Debt / Equity",
    shortName: "Leverage ratio",
    what: "Total debt divided by shareholders' equity.",
    why: "Quick read on capital structure. >1 means more debt than equity; sustained high values amplify both upside and bankruptcy risk.",
  },
  {
    term: "DuPont",
    shortName: "ROE decomposition",
    what: "Three-way breakdown of Return on Equity = Net Margin × Asset Turnover × Equity Multiplier.",
    why: "Tells you WHERE the ROE comes from — operational efficiency (margin), asset utilisation (turnover), or leverage (multiplier). Same ROE can hide very different businesses.",
  },
  {
    term: "FCF",
    shortName: "Free cash flow",
    what: "Operating cash flow minus capital expenditure. The cash actually available to investors after the business reinvests in itself.",
    why: "Less manipulable than earnings (working-capital-aware). Negative FCF over multiple years is the canonical capital-burn signal.",
  },
  {
    term: "EPS",
    shortName: "Earnings per share",
    what: "Net income divided by weighted-average shares outstanding for the period. Diluted EPS includes the dilutive effect of options and convertibles.",
    why: "The denominator on P/E and the line items most analysts and journalists track. We use diluted EPS in YoY-growth and DuPont charts.",
  },
  {
    term: "TTM",
    shortName: "Trailing twelve months",
    what: "The latest 4 quarters summed (for flow items like revenue) or the latest available value (for balance items like book value).",
    why: "Smoother than any single quarter; the standard window for yield, P/E, and dividend-payout calculations.",
  },
  {
    term: "DPS",
    shortName: "Dividend per share",
    what: "Per-share dividend (or LP per-unit distribution) declared or paid in a period.",
    why: "Multiplied by your held units = your dividend income. Driving metric on the Dividends drill page.",
  },
  {
    term: "Yield-on-cost",
    shortName: "Dividend / your entry",
    what: "Annual dividend divided by YOUR per-share cost basis (not current price).",
    why: "Shows how a long-held position's effective yield grows as dividends rise — current dividend yield doesn't tell that story.",
  },
  {
    term: "Payout ratio",
    shortName: "Dividends / FCF",
    what: "Dividends paid divided by free cash flow.",
    why: ">100% sustained = the dividend is being funded by debt or asset sales, not the business itself. Yellow flag for sustainability.",
  },

  // -------------------------------------------------------------------
  // eBull internal labels
  // -------------------------------------------------------------------
  {
    term: "Tier 1",
    shortName: "High-coverage instrument",
    what: "eBull coverage tier — Tier 1 instruments have full SEC + market data + thesis pipeline coverage. Tier 2 has market data only.",
    why: "Drives which charts and panels render. A Tier 2 instrument has no fundamentals chart because we don't ingest XBRL for it.",
  },
  {
    term: "Tier 2",
    shortName: "Market-data only",
    what: "eBull coverage tier — Tier 2 instruments have market data (prices, quotes) but no fundamentals or filings ingest.",
    why: "Useful for watchlists / index members; not deep-research targets.",
  },
  {
    term: "PM",
    shortName: "Pre-market",
    what: "Pre-market trading session (04:00-09:30 ET). Lower liquidity than the regular session.",
    why: "Toggle on the chart shows pre-market candles tinted differently. Useful for spotting overnight news reaction before the open.",
  },
  {
    term: "AH",
    shortName: "After-hours",
    what: "After-hours trading session (16:00-20:00 ET).",
    why: "Earnings often print after the close — AH candles show the immediate market reaction before the regular session re-opens.",
  },
  {
    term: "NET 90d",
    shortName: "Net insider activity (90 days)",
    what: "Acquired shares minus disposed shares from Form 4 filings over the trailing 90 days.",
    why: "The headline insider-activity number on the L1 pane. Positive = net buying; negative = net selling.",
  },
  {
    term: "TXNS",
    shortName: "Transaction count",
    what: "Number of Form 4 transaction rows in the window.",
    why: "Volume signal. Many small grants ≠ one big open-market buy — TXNS + NET together tell the cadence.",
  },
];

// Indexed-by-term map for O(1) tooltip lookup. Build once at module
// load; the glossary array stays the canonical sortable source.
const GLOSSARY_INDEX: Record<string, GlossaryEntry> = (() => {
  const out: Record<string, GlossaryEntry> = {};
  for (const entry of GLOSSARY) {
    out[entry.term] = entry;
  }
  return out;
})();

export function lookupTerm(term: string): GlossaryEntry | null {
  return GLOSSARY_INDEX[term] ?? null;
}

/** Friendly-name lookup for SEC filing form types — used as the
 *  fallback string in the Recent Filings list when an issuer's
 *  XBRL-extracted summary field is null (#684 — operator-reported
 *  duplicate `8-K  8-K` rendering). Returns the glossary entry's
 *  ``shortName`` when present; falls back to the raw type for
 *  unknown forms (e.g. an exotic Schedule X) so the row still
 *  renders something readable. */
export function filingTypeFriendlyName(filingType: string | null): string {
  if (filingType === null) return "filing";
  const entry = lookupTerm(filingType);
  return entry !== null ? entry.shortName : filingType;
}
