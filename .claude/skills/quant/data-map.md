# quant/data-map: what we hold, what is usable for backtests, what to add

## When to use

Use this before choosing data for a signal or a backtest, or proposing a new data source. It answers "is this point-in-time, how far back, and does anything use it?". The column-level detail lives in `data-engineer`, `metrics-analyst` and `data-sources/*`. Re-measure figures before quoting them; the queries are in those skills.

## Backtestable today

- **Prices: `research_price_daily`.** Survivorship-free US, 1962 onward. Bar count, series count and date range: run the queries at the top of `data-sources/research-price-corpus.md` rather than quoting a figure.
  - The Intrader vendor ends 2024-09-27. Its OHLC are raw, but `adj_close` carries split and dividend adjustment (`research_corpus_ingest.py`), so total return is computable to 2024-09. PWB ends 2026-07-08; check its adjustment basis in `data-sources/research-price-corpus.md` before splicing.
  - It includes cross-asset ETFs (e.g. SPY, QQQ, IWM, EFA, EEM, sector SPDRs, TLT, IEF, SHY, AGG, LQD, HYG, TIP, GLD, IAU, SLV, DBC, USO, VNQ, VGK, EWJ, VTI, BND). Start dates vary by fund (XLRE 2015, XLC 2018); measure the list and each inception before quoting. Picking today's familiar funds is survivor selection: an ETF study must include closed funds, inception eligibility and mandate changes, or say it does not.
  - eToro `price_daily` covers 2019-12 onward for splicing to the present.
- **Fundamentals: `financial_facts_raw`.** Per accession with `filed_date`, from 2009, plus the frozen point-in-time bundle (`app/services/pit_fundamentals.py`, acceptance-time correct).
  - `financial_periods` and `fundamentals_snapshot` are restated in place. They are fine for display, **not admissible for backtests**.
- **Insiders: Form 4.** Full schema, but dense only from 2023.
- **Short interest: FINRA**, 2021-07 onward. About five years: enough for a filter, thin for an alpha test. ⚠ Not point-in-time as stored: the ingest stamps `filed_at` at settlement-date midnight (`finra_short_interest_ingest.py`), but FINRA publishes days later. Lag to the publication calendar before using it in a backtest.
- **Macro and factor references: `reference_data_observations`.** FRED, Ken French FF5 + momentum, AQR. Snapshot-versioned.

## Forward-only (accumulating; not backtestable yet)

- 13F institutions, N-PORT funds, 13D/G, DEF 14A: 2024 onward. Today they feed only the UI. They become signals (institutional breadth, crowding, ownership change) once about 24 months accrue.
- News sentiment (2026-06+), house scores (2026-06+), theses (2026-07 → 2026-08, writer parked).
- eToro crowd, top-investor positions, spreads, eligibility and what-if costs (2026-09-25+).
- Reg SHO short volume (2026-05+), intraday bars (8 instruments).

## Add next

| source | why | point-in-time | cost |
|---|---|---|---|
| Borrow fees (IBKR `shortstock` FTP `usa.txt`) | An indicative borrow-cost proxy and a long-side filter (Drechsler & Drechsler 2014). It does not price eToro CFDs: those are charged by eToro's own overnight and hard-to-borrow terms, so validate the proxy against them | No history, so **archive daily from now** | free |
| Total-return series past 2024-09 | Intrader `adj_close` already carries dividends to 2024-09-27; the gap is extending it to the present and validating it against an independent reference (#3619) | vendor-dependent | free to ~$30/month (e.g. Tiingo) |
| Factor libraries: JKP, global-q, AQR (QMJ, BAB, TSMOM), Open Source Asset Pricing portfolio returns | Validate our constructions; adoption-track priors | historical | free |
| VIX term structure (VIX3M, VIX9D), Cboe CSV | Risk overlay. These are spot-tenor indices, not the VIX futures curve, so they cannot price SVXY's roll; that needs the futures benchmark | yes | free |
| Fed Excess Bond Premium, ALFRED vintages | Credit-stress and regime overlays | archive releases / vintages | free |
| 10-K/10-Q section text (our `periodic_report_sections` + Notre Dame SRAF parses) | Filing-change features (Cohen, Malloy & Nguyen 2020) | filing date | free |
| Options-implied skew | Xing, Zhang & Zhao 2010 (JFQA): high-skew stocks underperform; the paper reports ~10.9%/year for its long-short, a published in-sample figure, not ours | no free history | ~$29–99/month; only if a sleeve needs it |

## Data-quality hazards

Some insider `txn_date` values are out of range (0023…2036). Fund and DEF 14A period ends extend into the future. `financial_periods` holds one row per period with no restatement history. Never feed these unfiltered into a signal.
