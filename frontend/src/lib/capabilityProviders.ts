/**
 * Provider tag → human-readable label for the per-instrument
 * capability summary (#515 PR 3b).
 *
 * The backend's ``CAPABILITY_PROVIDERS`` enum
 * (``app/services/capabilities.py``) is the source of truth for the
 * tag set. This map gives each tag a short label the operator sees in
 * panel chrome ("SEC 8-K", "Companies House"). Unknown tags fall
 * through to the raw string — adding a new provider on the backend
 * does not require a frontend release; the operator sees the tag
 * verbatim until the label lands here.
 */

const PROVIDER_LABEL: Record<string, string> = {
  // US — SEC family
  sec_edgar: "SEC EDGAR",
  sec_xbrl: "SEC XBRL",
  sec_dividend_summary: "SEC dividends",
  sec_8k_events: "SEC 8-K",
  sec_10k_item1: "SEC 10-K Item 1",
  sec_form4: "SEC Form 4",
  sec_13f: "SEC 13F",
  sec_13d_13g: "SEC 13D/G",
  // UK
  companies_house: "Companies House",
  lse_rns: "LSE RNS",
  // EU
  esma: "ESMA",
  bafin: "BaFin",
  amf: "AMF",
  consob: "Consob",
  // Asia
  hkex: "HKEX",
  tdnet: "TDnet",
  edinet: "EDINET",
  asx: "ASX",
  krx: "KRX",
  kind: "KIND",
  twse: "TWSE",
  mops: "MOPS",
  sse: "SSE",
  szse: "SZSE",
  nse_india: "NSE India",
  bse_india: "BSE India",
  sgx: "SGX",
  // MENA
  tadawul: "Tadawul",
  adx: "ADX",
  dfm: "DFM",
  // Crypto
  coingecko: "CoinGecko",
  glassnode: "Glassnode",
  // Commodity / FX
  cme: "CME",
  lme: "LME",
  ecb: "ECB",
  fed: "Fed",
  boe: "BoE",
  // Canada
  tmx_group: "TMX",
  sedar_plus: "SEDAR+",
};

export function providerLabel(tag: string): string {
  return PROVIDER_LABEL[tag] ?? tag;
}

/** Active providers for one capability cell — providers where
 *  ``data_present[provider]`` is true, in the operator-decided
 *  order. Frontend renders one shell+hook pair per active provider.
 */
export function activeProviders(cell: {
  providers: string[];
  data_present: Record<string, boolean>;
}): string[] {
  return cell.providers.filter((p) => cell.data_present[p] === true);
}
