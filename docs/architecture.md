# Architecture

## Goal

Build a long-horizon AI-assisted investment operating system for eToro that:
- syncs the actual tradable universe
- scores and ranks opportunities
- builds and revises stock theses
- suggests or places long-only trades
- tracks positions, P&L, and UK tax treatment
- records why every decision was made

## Core principle

The system has two brains:

### 1. Research brain
AI-heavy.
Allowed to:
- read filings
- summarize news
- interpret sentiment
- build a thesis
- argue a counter-thesis
- estimate valuation bands
- rank opportunities

### 2. Execution brain
Deliberately constrained.
Allowed to:
- validate hard rules
- approve or reject a trade
- place orders only if every rule passes
- update the ledger

## Module map

### Universe service
Responsible for:
- pulling tradable instruments from eToro
- resolving and caching `instrument_id`
- maintaining a clean symbol / exchange / sector map

### Market data service
Responsible for:
- quotes
- candles
- rolling returns
- volatility
- liquidity / spread checks where available

### Fundamentals and filings service
Responsible for:
- ingesting official company data
- capturing change over time
- surfacing red flags such as dilution, debt stress, and going-concern language

### News and sentiment service
Responsible for:
- ingesting current events
- clustering duplicates
- scoring importance
- separating signal from noise

### Thesis engine
Responsible for:
- current investment case
- what must go right
- what breaks the thesis
- risk summary
- buy zone
- valuation range

### Ranking engine
Responsible for:
- converting mixed evidence into a stable ranked list
- exposing changes by score family
- penalising stale or low-confidence setups

### Portfolio manager
Responsible for:
- deciding buy / add / hold / trim / exit candidates
- assigning suggested position sizes
- keeping diversification sane

### Execution guard
Responsible for:
- enforcing hard risk rules before any order is sent
- refusing to bypass stale data, concentration breaches, or other failures

### Ledger and tax engine
Responsible for:
- positions
- fills
- realized / unrealized P&L
- dividends
- same-day, 30-day, and Section 104 handling for UK tax

## Coverage tiers

### Tier 1 - active coverage
25-50 names.
- full thesis
- daily monitoring
- trade eligible

### Tier 2 - watchlist coverage
100-200 names.
- lighter research
- weekly refresh
- promotable to Tier 1

### Tier 3 - universe only
Everything else.
- basic metadata only
- not trade eligible

## Workflow

1. Sync eToro tradable universe
2. Refresh prices and relevant external data
3. Update coverage tiers
4. Update thesis
5. Run counter-thesis
6. Re-score
7. Produce candidate actions
8. Run execution guard
9. Stage or place order
10. Log the full rationale

## Deployment stance

Start with:
- research only
- then staged orders
- then demo execution
- then live with small capital sleeve
- never start with hands-off live trading
